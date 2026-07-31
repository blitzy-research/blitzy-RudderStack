package sendgridbulkupload

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// SendGrid Marketing Contacts endpoints.
//
// These are compile-time constants rather than configurable values on purpose: SendGrid
// publishes exactly one global API host for the v3 Web API, so making the origin settable
// would create nothing but a way to misconfigure a destination.
const (
	// sendGridAPIHost is the only host this adapter builds a request against itself, and the
	// only host the bearer credential may ever be sent to. sendGridBaseURL is derived from it
	// so the two can never drift apart.
	sendGridAPIHost = "api.sendgrid.com"

	// sendGridBaseURL is the single origin every call in this adapter is made against.
	sendGridBaseURL = "https://" + sendGridAPIHost

	// sendGridContactsEndpoint is the Marketing Contacts upsert endpoint, driven with PUT.
	// It is corroborated in-repo by the read-only SendGrid parity fixture, which records
	// the endpoint as "PUT https://api.sendgrid.com/v3/marketing/contacts".
	sendGridContactsEndpoint = sendGridBaseURL + "/v3/marketing/contacts"

	// sendGridImportsEndpoint is the import-status endpoint less its trailing identifier.
	// The job_id handed back by the upsert is appended to address one import; the official
	// specification names that path parameter "id" while the value to pass is the job_id,
	// so only the parameter's name differs from how the endpoint is usually written down.
	sendGridImportsEndpoint = sendGridContactsEndpoint + "/imports/"
)

// Rate-limit response headers.
//
// SendGrid documents the three X-RateLimit-* headers for the v3 Web API. Retry-After is NOT
// documented for it anywhere and is therefore read purely opportunistically, because an
// edge or proxy sitting in front of the API may still inject it.
const (
	headerRetryAfter         = "Retry-After"
	headerRateLimitLimit     = "X-RateLimit-Limit"
	headerRateLimitRemaining = "X-RateLimit-Remaining"
	headerRateLimitReset     = "X-RateLimit-Reset"
)

// Operation names.
//
// Each is carried on APIError.Operation and logged alongside every failure, so that a
// single reason string tells an operator both which SendGrid call failed and why, without
// any caller having to supply that context itself.
const (
	opUploadContacts  = "upload contacts"
	opGetImportStatus = "get import status"
	opGetImportErrors = "get import errors"
)

// maxResponseBodyExcerptRunes bounds how much of an unexpected response body is quoted into
// an error or a log line. A SendGrid error envelope is tiny, but an edge proxy can answer
// with a full HTML page, and that must not be allowed to bloat a job's recorded failure
// reason in the jobs database.
const maxResponseBodyExcerptRunes = 512

// Tuned transport settings, matching the convention already established for the
// bulk-upload connectors in this tree.
const (
	defaultTimeout             = 30 * time.Second
	defaultIdleConnTimeout     = 90 * time.Second
	defaultMaxIdleConnsPerHost = 50
	defaultMaxConnsPerHost     = 100
)

// Response-size budgets.
//
// Every response this adapter reads is read through a budget, because an unbounded read of a
// remote body lets the remote side decide how much memory this process allocates. Exceeding a
// budget is reported as an error rather than truncated silently: a truncated document parses
// as a shorter, different document, which for the errors document would mean reporting failed
// contacts as delivered.
const (
	// maxAPIResponseBytes bounds the two small JSON documents the API itself returns. An
	// upsert acknowledgement and an import status are a few hundred bytes; a megabyte leaves
	// generous room for an unexpected error page while still bounding the allocation.
	maxAPIResponseBytes int64 = 1 * 1024 * 1024

	// defaultMaxErrorsDocumentBytes bounds the errors document, which is the only response
	// whose size is driven by data volume rather than by a fixed schema. It is overridable
	// through configuration because the document's format is not published, so its size per
	// errored row cannot be predicted.
	defaultMaxErrorsDocumentBytes int64 = 32 * 1024 * 1024
)

// Settings for the one request whose target is chosen by the remote side.
const (
	// errorsDocumentTimeout is more generous than defaultTimeout because the errors document
	// can be substantially larger than an API response.
	errorsDocumentTimeout = 60 * time.Second

	// errorsDocumentDialTimeout bounds a single connection attempt to the errors document's
	// host, which is not necessarily the API host.
	errorsDocumentDialTimeout = 10 * time.Second

	// maxErrorsDocumentRedirects bounds the redirect chain the errors document fetch follows.
	maxErrorsDocumentRedirects = 3

	// maxErrorsURLLength bounds the length of the provider-supplied URL before it is parsed.
	maxErrorsURLLength = 2048
)

// sendGridHostSuffixes are the domains SendGrid itself serves. They are the ONLY hosts the
// bearer credential may be attached to, so that a URL published by the provider can never
// carry this destination's API key anywhere else.
var sendGridHostSuffixes = []string{"sendgrid.com", "sendgrid.net"}

// defaultErrorsURLAllowedHosts is the default allow list applied to the errors document URL.
//
// It holds SendGrid's own domains only. When SendGrid serves the document from object storage
// instead, the operator widens the list through
// BatchRouter.SENDGRID_BULK_UPLOAD.errorsURLAllowedHosts - and the rejection error names that
// key, so the required action is self-evident from the failure. Widening the list does NOT
// widen the credential: the key is still attached only for the hosts above.
var defaultErrorsURLAllowedHosts = []string{"sendgrid.com", "sendgrid.net"}

// configKeyErrorsURLAllowedHosts and configKeyMaxErrorsDocumentBytes resolve
// BatchRouter.SENDGRID_BULK_UPLOAD.<key> and fall back to BatchRouter.<key>.
const (
	configKeyErrorsURLAllowedHosts  = "errorsURLAllowedHosts"
	configKeyMaxErrorsDocumentBytes = "maxErrorsDocumentBytes"
)

// redactedCredential replaces any credential recognized in provider-supplied text.
const redactedCredential = "<redacted-credential>"

// bearerCredentialPattern and sendGridAPIKeyPattern recognize a credential inside text this
// connector did not write.
//
// This connector never logs its own key. A provider or an intermediary can nonetheless echo the
// Authorization header back inside its own response body - a debugging proxy is the usual
// culprit - and that body is quoted into the error this adapter returns, which becomes a job's
// recorded failure reason and a log field. Belt and braces: the credential is stripped out of
// such text before it can be kept anywhere.
//
// bearerCredentialPattern deliberately stops at the first character that cannot appear in a
// token, so it never swallows the JSON punctuation around the value it redacts, and the
// replacement keeps the scheme so the diagnostic ("something sent an Authorization header back")
// survives. sendGridAPIKeyPattern covers a key echoed WITHOUT the scheme, since SendGrid keys
// have a recognizable SG.<id>.<secret> shape.
var (
	bearerCredentialPattern = regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=\-]+`)
	sendGridAPIKeyPattern   = regexp.MustCompile(`SG\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}`)
)

// redactCredentials removes any credential recognizable in provider-supplied text.
//
// apiKey is redacted by exact match as well as by pattern, which is what catches a key echoed
// back bare - under a field name this connector cannot anticipate, for instance - and it is
// optional so that callers without access to the credential can still apply the patterns. The
// placeholder contains no quote and no backslash, so a JSON body stays well-formed through the
// substitution and can still be decoded as an error envelope afterwards.
func redactCredentials(text, apiKey string) string {
	if text == "" {
		return ""
	}
	redacted := bearerCredentialPattern.ReplaceAllString(text, "Bearer "+redactedCredential)
	redacted = sendGridAPIKeyPattern.ReplaceAllString(redacted, redactedCredential)
	if apiKey != "" {
		redacted = strings.ReplaceAll(redacted, apiKey, redactedCredential)
	}
	return redacted
}

// getDefaultHTTPClient returns an http.Client with standard configuration
func getDefaultHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        defaultMaxConnsPerHost,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		IdleConnTimeout:     defaultIdleConnTimeout,
		// Disable compression to prevent BREACH attacks
		DisableCompression: true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   defaultTimeout,
	}
}

// newErrorsDocumentHTTPClient builds the client used for the one request whose target is
// chosen by the remote side rather than by this adapter.
//
// A URL that arrives inside a provider response is untrusted input even when the provider is
// trusted, so the fetch is hardened in three independent ways:
//   - the dial control hook refuses to connect to any address that is not publicly routable,
//     which also defeats a DNS answer that resolves an allowed host to an internal address;
//   - the redirect chain is capped, every hop is re-validated, and a hop may not leave the
//     origin of the URL that was validated first;
//   - the Authorization header is removed from every redirected request, whatever its target,
//     which is strictly stronger than Go's own same-domain-or-subdomain rule.
func newErrorsDocumentHTTPClient(allowedHosts []string) *http.Client {
	dialer := &net.Dialer{
		Timeout:   errorsDocumentDialTimeout,
		KeepAlive: defaultIdleConnTimeout,
		Control:   controlPubliclyRoutableAddress,
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConns:        defaultMaxConnsPerHost,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		IdleConnTimeout:     defaultIdleConnTimeout,
		// Disable compression to prevent BREACH attacks, and to keep the transferred size
		// equal to the size the read budget is applied to.
		DisableCompression: true,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   errorsDocumentTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// The credential never crosses a redirect, whatever the target is.
			req.Header.Del("Authorization")
			if len(via) >= maxErrorsDocumentRedirects {
				return fmt.Errorf("the errors document redirected more than %d times", maxErrorsDocumentRedirects)
			}
			if _, err := validateErrorsURL(req.URL.String(), allowedHosts); err != nil {
				return fmt.Errorf("the errors document redirect was rejected: %w", err)
			}
			if len(via) > 0 && !sameOrigin(req.URL, via[0].URL) {
				return fmt.Errorf("the errors document redirected off its origin, to host %q", req.URL.Hostname())
			}
			return nil
		},
	}
}

// controlPubliclyRoutableAddress refuses any connection to an address that is not publicly
// routable. It runs after DNS resolution and immediately before the socket is connected, so it
// sees the address that will actually be dialled - which is what makes it effective against
// DNS rebinding and against a redirect aimed at a cloud metadata endpoint.
func controlPubliclyRoutableAddress(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("the dial address %q cannot be parsed: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("the dial address %q is not an ip address", host)
	}
	if !isPubliclyRoutableIP(ip) {
		return fmt.Errorf("refusing to connect to the non publicly routable address %s", ip)
	}
	return nil
}

// isPubliclyRoutableIP reports whether ip sits outside every range a server-side request
// forgery would aim at: loopback, private, link-local (which covers 169.254.169.254),
// carrier-grade NAT, multicast, unspecified and "this network".
//
// The address is unmapped first, so that an IPv4-mapped IPv6 form such as ::ffff:127.0.0.1
// cannot slip past the IPv4 checks.
func isPubliclyRoutableIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if unmapped := ip.To4(); unmapped != nil {
		ip = unmapped
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 0: // 0.0.0.0/8 - "this network".
			return false
		case ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127: // 100.64.0.0/10 - carrier-grade NAT.
			return false
		case ip4[0] == 192 && ip4[1] == 0 && ip4[2] == 0: // 192.0.0.0/24 - IETF assignments.
			return false
		}
	}
	return true
}

// sameOrigin reports whether two URLs share a scheme, a host and an effective port.
func sameOrigin(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	effectivePort := func(u *url.URL) string {
		if port := u.Port(); port != "" {
			return port
		}
		return "443"
	}
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		normalizeHost(a.Hostname()) == normalizeHost(b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

// validateErrorsURL parses and vets a provider-supplied errors document URL before any
// connection is attempted, and reports whether its host is served by SendGrid itself.
//
// The URL is canonicalized first, then everything that could turn the fetch into a request
// against infrastructure this connector must not reach is rejected: a non-HTTPS scheme, an
// opaque reference, embedded credentials, a fragment, a missing host, an IP literal, a
// punycode host, a port other than 443, and any host outside the allow list. Only after all of
// that does the caller decide whether to attach the credential.
func validateErrorsURL(rawURL string, allowedHosts []string) (*url.URL, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil, errors.New("the errors document url is empty")
	}
	if len(trimmed) > maxErrorsURLLength {
		return nil, fmt.Errorf("the errors document url is longer than %d characters", maxErrorsURLLength)
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, errors.New("the errors document url is not a valid url")
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return nil, fmt.Errorf("the errors document url scheme %q is not allowed, https is required", parsed.Scheme)
	}
	if parsed.Opaque != "" {
		return nil, errors.New("the errors document url must not be opaque")
	}
	if parsed.User != nil {
		return nil, errors.New("the errors document url must not carry user information")
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, errors.New("the errors document url must not carry a fragment")
	}
	host := normalizeHost(parsed.Hostname())
	if host == "" {
		return nil, errors.New("the errors document url has no host")
	}
	if net.ParseIP(host) != nil {
		return nil, errors.New("the errors document url host must be a domain name, not an ip literal")
	}
	if strings.Contains(host, "xn--") {
		return nil, fmt.Errorf("the errors document url host %q is an internationalised name and is not allowed", host)
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return nil, fmt.Errorf("the errors document url port %q is not allowed, 443 is required", port)
	}
	if !hostAllowed(host, allowedHosts) {
		return nil, fmt.Errorf("the errors document url host %q is not allowed; add it to BatchRouter.%s.%s to allow it",
			host, destName, configKeyErrorsURLAllowedHosts)
	}
	return parsed, nil
}

// hostAllowed reports whether host equals, or is a subdomain of, one of the allowed entries.
//
// Matching on an exact value or on a dot-prefixed suffix is what keeps a lookalike host out:
// neither "api.sendgrid.com.attacker.example" nor "notsendgrid.com" can match "sendgrid.com".
func hostAllowed(host string, allowed []string) bool {
	host = normalizeHost(host)
	for _, entry := range allowed {
		entry = strings.TrimPrefix(normalizeHost(entry), ".")
		if entry == "" {
			continue
		}
		if host == entry || strings.HasSuffix(host, "."+entry) {
			return true
		}
	}
	return false
}

// isSendGridHost reports whether host is served by SendGrid itself, which is the only case in
// which the API key may be attached to an errors document request. A document served from
// object storage is reached through a pre-signed URL that authenticates through its own query
// parameters, so it neither needs nor receives the credential.
func isSendGridHost(host string) bool {
	return hostAllowed(host, sendGridHostSuffixes)
}

// normalizeHost lower-cases a host name and strips the optional root label dot, so that
// "API.SendGrid.com." and "api.sendgrid.com" compare equal.
func normalizeHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

// setAuthorizationHeader attaches the static bearer credential.
//
// SendGrid authenticates the v3 Web API with a static key, so there is no token exchange, no
// refresh and no OAuth subsystem involved. It is a separate function from setRequestHeaders so
// that the one request whose target is provider-supplied can decide whether to call it at all.
func setAuthorizationHeader(req *http.Request, apiKey string) {
	req.Header.Set("Authorization", "Bearer "+apiKey)
}

// setRequestHeaders applies the headers the two fixed SendGrid endpoints need. Content-Type is
// strictly required only on the upsert, which is the one call that carries a body, but it is
// set unconditionally so the header logic lives in one place and cannot drift between the two
// operations; it is inert on a GET.
func setRequestHeaders(req *http.Request, apiKey string) {
	req.Header.Set("Content-Type", "application/json")
	setAuthorizationHeader(req, apiKey)
}

// sendGridAPIServiceImpl is the production SendGridAPIService: a thin, stateless HTTP
// adapter over the three SendGrid REST operations this connector needs.
//
// "Thin" is a deliberate constraint. The adapter owns transport concerns only - the bearer
// credential, the endpoints, status-code classification and the typed rate-limit error -
// and owns no lifecycle logic, no chunking, no reconciliation and no retry loop. The batch
// router already owns retry, backoff and circuit breaking, so a second retry loop here
// would compound delays and obscure the framework's own abort accounting.
type sendGridAPIServiceImpl struct {
	// client is the tuned client built by getDefaultHTTPClient. It is deliberately NOT
	// wrapped in any client-side rate limiter: SendGrid publishes no per-endpoint request
	// ceiling for the Marketing Contacts endpoints, so a pre-emptive throttle would be a
	// guess that either throttles a healthy destination for no reason or fails to prevent
	// anything. This adapter is reactive to HTTP 429 instead, through *RateLimitError.
	client *http.Client

	// apiKey is the static bearer credential, validated once at construction so that a
	// misconfigured destination can never reach the network.
	apiKey string

	// logger carries this destination's log context. Only the non-sugared, type-specific
	// field API is used with it.
	logger logger.Logger

	// statsFactory builds this adapter's measurements.
	statsFactory stats.Stats

	// statLabels are the tags every measurement is emitted with. They are fixed at
	// construction because none of them can change over the adapter's lifetime.
	statLabels stats.Tags

	// errorsClient is a separate, hardened client used ONLY for the errors document, whose
	// target is supplied by the provider rather than built here. It is not the client above
	// because the two have genuinely different threat models: the fixed endpoints are
	// compile-time constants, while this one is remote input.
	errorsClient *http.Client

	// errorsURLAllowedHosts is the allow list an errors document URL's host must satisfy.
	errorsURLAllowedHosts []string

	// maxErrorsDocumentBytes bounds how many bytes of the errors document are read.
	maxErrorsDocumentBytes int64
}

// Compile-time proof that the adapter really satisfies the seam declared in types.go, so
// that any drift in the interface breaks the build here instead of surfacing at runtime.
var _ SendGridAPIService = (*sendGridAPIServiceImpl)(nil)

// NewSendGridAPIService builds the SendGrid HTTP adapter for one destination.
//
// It reads the bearer credential out of the destination's configuration and FAILS OUTRIGHT
// when that credential is absent, is not a string, or is blank. Validating here rather than
// on the first upload means a misconfigured destination is reported once, clearly, at
// construction time, instead of producing an opaque 401 once per batch for as long as it
// stays misconfigured.
//
// The returned value is the SendGridAPIService interface rather than the concrete type, so
// callers depend on the seam and tests can substitute the generated mock.
func NewSendGridAPIService(destination *backendconfig.DestinationT, log logger.Logger, statsFactory stats.Stats) (SendGridAPIService, error) {
	if destination == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	// Indexing a nil Config map is safe in Go and simply yields an untyped nil, which fails
	// the type assertion below, so no separate nil-map guard is needed.
	apiKey, _ := destination.Config["apiKey"].(string)
	// Trimmed before the emptiness test because a credential pasted into a configuration
	// form very often carries surrounding whitespace: a bearer header built from such a
	// value is rejected by SendGrid with an opaque 401 on every single request, and a value
	// consisting only of whitespace is no credential at all.
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("apiKey not found or not a string")
	}
	// Defaulted rather than trusted, so that a caller which has not wired up observability
	// yet cannot turn a delivery failure into a nil-pointer panic inside a router worker.
	if log == nil {
		log = logger.NOP
	}
	if statsFactory == nil {
		statsFactory = stats.NOP
	}
	// Both resolve BatchRouter.SENDGRID_BULK_UPLOAD.<key> and fall back to BatchRouter.<key>,
	// defaulting to SendGrid's own domains and to a generous document budget.
	allowedHosts := common.GetBatchRouterConfigStringMap(configKeyErrorsURLAllowedHosts, destName, defaultErrorsURLAllowedHosts)
	maxErrorsDocumentBytes := common.GetBatchRouterConfigInt64(configKeyMaxErrorsDocumentBytes, destName, defaultMaxErrorsDocumentBytes)
	if maxErrorsDocumentBytes <= 0 {
		maxErrorsDocumentBytes = defaultMaxErrorsDocumentBytes
	}
	return &sendGridAPIServiceImpl{
		client:                 getDefaultHTTPClient(),
		apiKey:                 apiKey,
		logger:                 log,
		statsFactory:           statsFactory,
		errorsClient:           newErrorsDocumentHTTPClient(allowedHosts),
		errorsURLAllowedHosts:  allowedHosts,
		maxErrorsDocumentBytes: maxErrorsDocumentBytes,
		statLabels: stats.Tags{
			"module": "batch_router",
			// Sourced from the destName constant rather than from the destination
			// definition, so the tag can never drift from the registered
			// destination-definition name even if an operator renames the destination.
			"destType": destName,
			"destID":   destination.ID,
		},
	}, nil
}

// UploadContacts issues the Marketing Contacts upsert and returns the job_id SendGrid
// assigned to the queued import.
//
// Only HTTP 202 Accepted counts as success, and a 202 means the batch has been QUEUED
// rather than applied - which is precisely why the connector polls afterwards. Every other
// status yields an error carrying the status code, and 429 specifically yields a
// *RateLimitError so the caller can keep the affected jobs retryable.
func (s *sendGridAPIServiceImpl) UploadContacts(request UpsertRequest) (*UpsertResponse, error) {
	payload, err := jsonrs.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: marshalling request body: %w", opUploadContacts, err)
	}
	// Observed before the request is made, so that the size of a payload SendGrid goes on
	// to reject is recorded just as faithfully as the size of one it accepts.
	s.statsFactory.NewTaggedStat("payload_size", stats.HistogramType, s.statLabels).Observe(float64(len(payload)))

	startTime := time.Now()
	req, err := s.newRequest(opUploadContacts, http.MethodPut, sendGridContactsEndpoint, payload)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		// A transport failure - DNS, TLS, a reset connection, or this client's own timeout
		// - is not a verdict from SendGrid, so it is reported as a plain wrapped error and
		// the batch router's retry budget decides what happens next.
		return nil, fmt.Errorf("sendgrid %s: %w", opUploadContacts, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The body is read through a budget, and the STATUS is classified from the response
	// regardless of whether that read succeeded. A failed or oversized body therefore never
	// masks a 403 or a 429: readLimitedBody yields a nil body on failure, classifyResponse
	// still sees the real status code, and only a genuine success falls through to the read
	// error below.
	body, readErr := readLimitedBody(resp, maxAPIResponseBytes)
	if err := s.classifyResponse(opUploadContacts, http.StatusAccepted, resp, body); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opUploadContacts, resp.StatusCode, readErr)
	}

	var upsertResp UpsertResponse
	if err := jsonrs.Unmarshal(body, &upsertResp); err != nil {
		return nil, fmt.Errorf("sendgrid %s: decoding response body: %w", opUploadContacts, err)
	}
	if upsertResp.JobID == "" {
		// A 202 carrying no job_id cannot be polled, so accepting it would strand the
		// import in the importing state forever with nothing able to resolve it. Reporting
		// it as a failed call keeps the affected jobs retryable instead.
		s.logger.Errorn("[sendgrid bulk upload] upload accepted without a job id",
			logger.NewStringField("operation", opUploadContacts),
			logger.NewStringField("responseBody", excerptResponseBody(body)))
		return nil, fmt.Errorf("sendgrid %s: response carried no job_id", opUploadContacts)
	}

	s.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, s.statLabels).Since(startTime)
	return &upsertResp, nil
}

// GetImportStatus reads the status of a queued import.
//
// The job_id returned by UploadContacts is passed as the path parameter the official
// specification names "id"; only the parameter's name differs from the way the endpoint is
// usually written down. The decoded response keeps its nested "results" object: SendGrid
// reports the per-row counters and the errors-document URL inside that object and never at
// the top level, so a flattened decode would succeed without complaint and then read
// ErroredCount as 0 forever, reporting every partially failed import as a clean success.
func (s *sendGridAPIServiceImpl) GetImportStatus(jobID string) (*ImportStatusResponse, error) {
	if jobID == "" {
		return nil, fmt.Errorf("sendgrid %s: jobID is empty", opGetImportStatus)
	}
	// Path-escaped rather than concatenated verbatim, so that an unexpected identifier can
	// never reshape the request path.
	endpoint := sendGridImportsEndpoint + url.PathEscape(jobID)
	req, err := s.newRequest(opGetImportStatus, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: %w", opGetImportStatus, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := readLimitedBody(resp, maxAPIResponseBytes)
	if err := s.classifyResponse(opGetImportStatus, http.StatusOK, resp, body); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opGetImportStatus, resp.StatusCode, readErr)
	}

	var status ImportStatusResponse
	if err := jsonrs.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("sendgrid %s: decoding response body: %w", opGetImportStatus, err)
	}
	return &status, nil
}

// GetImportErrors fetches the URL SendGrid published in ImportResults.ErrorsURL and returns
// that document exactly as received.
//
// It stays raw on purpose. The document's schema is genuinely undocumented - the official
// specification mentions errors_url only as a bare string URL, with no media type and no
// schema - so interpreting it belongs to the tolerant parser in the manager rather than to
// this transport adapter, which must not encode a guess about a shape it cannot verify.
// Nothing is unmarshalled here, no content type is sniffed and no decompression is
// attempted.
//
// This is the ONE request in the connector whose target is chosen by the remote side, so it is
// the one place where the credential must not simply be attached. The URL is validated and
// canonicalized BEFORE any request is built, and the bearer key is then attached only when the
// host is served by SendGrid itself - a document served from object storage is reached through
// a pre-signed URL that authenticates through its own query parameters, so sending the key
// there would disclose it for no purpose. The request also goes through the hardened
// errorsClient, which caps and re-validates redirects, strips the credential from every hop and
// refuses to dial a non-publicly-routable address.
func (s *sendGridAPIServiceImpl) GetImportErrors(errorsURL string) ([]byte, error) {
	if strings.TrimSpace(errorsURL) == "" {
		return nil, fmt.Errorf("sendgrid %s: errorsURL is empty", opGetImportErrors)
	}
	parsed, err := validateErrorsURL(errorsURL, s.errorsURLAllowedHosts)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: %w", opGetImportErrors, err)
	}
	// The originally supplied string is used verbatim rather than re-serialized, so that a
	// pre-signed URL's signature can never be invalidated by canonicalization. Validation ran
	// against the parsed form, so nothing is trusted that was not checked.
	req, err := http.NewRequest(http.MethodGet, strings.TrimSpace(errorsURL), nil)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: building request: %w", opGetImportErrors, err)
	}
	req.Header.Set("Accept", "application/json")
	credentialed := isSendGridHost(parsed.Hostname())
	if credentialed {
		setAuthorizationHeader(req, s.apiKey)
	}

	resp, err := s.errorsClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: %w", opGetImportErrors, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The read is bounded and its failure is propagated rather than discarded: a truncated
	// errors document handed to the parser as though it were complete would leave the rows lost
	// to the truncation reported as delivered, which is exactly the silent data loss the whole
	// reconciliation path exists to prevent. The status is classified from the response either
	// way, so an oversized body can never mask a 403 or a 429.
	body, readErr := readLimitedBody(resp, s.maxErrorsDocumentBytes)
	if err := s.classifyResponse(opGetImportErrors, http.StatusOK, resp, body); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opGetImportErrors, resp.StatusCode, readErr)
	}
	return body, nil
}

// readLimitedBody reads at most limit bytes of a response body.
//
// An advertised Content-Length above the budget is rejected WITHOUT reading anything, and the
// read itself goes through a limit reader so that a chunked or mis-advertised body cannot
// exhaust memory either. One byte beyond the budget is read deliberately, so that overflow can
// be detected and reported as an error instead of being truncated silently - a truncated
// document would otherwise be parsed as a shorter, different one.
func readLimitedBody(resp *http.Response, limit int64) ([]byte, error) {
	if limit <= 0 {
		limit = maxAPIResponseBytes
	}
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("the response advertises %d bytes, which exceeds the %d byte limit", resp.ContentLength, limit)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("reading the response body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("the response body exceeds the %d byte limit", limit)
	}
	return body, nil
}

// newRequest builds one authenticated SendGrid request.
//
// Every call in this adapter goes through it, so the bearer credential and the JSON content
// type can never be present on two operations and forgotten on the third. A nil payload
// produces a request with no body, which is what the two GETs need.
func (s *sendGridAPIServiceImpl) newRequest(operation, method, endpoint string, payload []byte) (*http.Request, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewBuffer(payload)
	}
	// The interface this adapter implements carries no context, matching every sibling
	// bulk-upload connector, so cancellation is bounded by the client's own timeout.
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: building request: %w", operation, err)
	}
	setRequestHeaders(req, s.apiKey)
	return req, nil
}

// classifyResponse is the single place in this package where an HTTP status code is
// interpreted. It returns nil when the call succeeded, and otherwise the typed error the
// rest of the connector branches on with errors.As.
//
// The 429 branch is the load-bearing one. Returning the dedicated *RateLimitError - rather
// than the generic *APIError every other status yields - is what lets the manager route the
// affected jobs to the batch router's RETRYABLE channel (FailedJobIDs) instead of its
// terminal one (AbortJobIDs). A rate limit is never a permanent condition, and the batch
// router alone owns the decision to give up, so this connector must never abort because of
// one. Because detection lives here, nothing else in the package may re-sniff a status code
// for 429.
//
// It is also the single place a failing body is turned into text that gets kept, so it is where
// any credential a provider or an intermediary echoed back is stripped out - once, for every
// operation and both error types. The redaction produces a NEW slice and never touches the
// caller's bytes, so the errors document GetImportErrors returns on success stays byte-identical.
func (s *sendGridAPIServiceImpl) classifyResponse(operation string, successCode int, resp *http.Response, body []byte) error {
	if resp.StatusCode != successCode || resp.StatusCode == http.StatusTooManyRequests {
		body = []byte(redactCredentials(string(body), s.apiKey))
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		rateLimitErr := newRateLimitError(resp, body)
		s.logger.Warnn("[sendgrid bulk upload] rate limited by sendgrid",
			logger.NewStringField("operation", operation),
			logger.NewStringField("retryAfter", rateLimitErr.RetryAfter),
			logger.NewIntField("rateLimitResetAt", rateLimitErr.ResetAt),
			logger.NewIntField("rateLimitLimit", int64(rateLimitErr.Limit)),
			logger.NewIntField("rateLimitRemaining", int64(rateLimitErr.Remaining)),
			obskit.Error(rateLimitErr))
		return rateLimitErr
	}
	if resp.StatusCode != successCode {
		apiErr := newAPIError(operation, resp, body)
		s.logger.Errorn("[sendgrid bulk upload] sendgrid rejected the request",
			logger.NewStringField("operation", operation),
			logger.NewIntField("statusCode", int64(apiErr.StatusCode)),
			obskit.Error(apiErr))
		return apiErr
	}
	return nil
}

// newRateLimitError builds the typed error for an HTTP 429 response.
//
// Header handling is deliberately tolerant. Retry-After is read FIRST because, when an edge
// or proxy in front of SendGrid injects it, it is the most direct statement of how long to
// wait - but it is not documented for the SendGrid v3 Web API, so it is never required to
// be present. X-RateLimit-Reset is the documented fallback and is a UNIX timestamp in
// SECONDS at which the window resets, NOT a delay: reading it as a duration would produce a
// wait of decades, so it is kept as the absolute instant it is and rendered as one. Limit
// and Remaining are captured purely so an operator can see how tight the window was.
//
// A header that is absent, empty or unparseable yields a zero value rather than an error:
// the response was still a rate limit and must still be reported as one, and
// RateLimitError.Error reports an absent window explicitly instead of rendering it as a
// misleading instant.
func newRateLimitError(resp *http.Response, body []byte) *RateLimitError {
	return &RateLimitError{
		StatusCode: resp.StatusCode,
		RetryAfter: strings.TrimSpace(resp.Header.Get(headerRetryAfter)),
		ResetAt:    parseRateLimitHeaderValue(resp.Header, headerRateLimitReset),
		Limit:      int(parseRateLimitHeaderValue(resp.Header, headerRateLimitLimit)),
		Remaining:  int(parseRateLimitHeaderValue(resp.Header, headerRateLimitRemaining)),
		Message:    describeResponseBody(body),
	}
}

// newAPIError builds the error for a SendGrid response that is neither the expected success
// nor a rate limit. The status code is carried explicitly so that the manager can map the
// outcome onto the batch router's retryable or terminal channel without re-parsing anything.
func newAPIError(operation string, resp *http.Response, body []byte) *APIError {
	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Operation:  operation,
		Errors:     decodeAPIErrorItems(body),
	}
	if len(apiErr.Errors) == 0 {
		// The body was absent, empty, or not a SendGrid error envelope at all - an HTML
		// page from an edge proxy, for instance - so a bounded excerpt of it is the only
		// diagnostic an operator is going to get.
		apiErr.Message = excerptResponseBody(body)
		if apiErr.Message == "" {
			apiErr.Message = "sendgrid returned an empty response body"
		}
	}
	return apiErr
}

// describeResponseBody renders a response body as a single-line summary fit for an error
// message or a log field.
//
// A well-formed SendGrid error envelope is rendered through its entries, which handles the
// documented null "field" safely - the rate-limit body is exactly
// {"errors":[{"field":null,"message":"too many requests"}]}. Anything else falls back to a
// bounded excerpt of the raw bytes, so an operator is never left holding nothing but a bare
// status code.
func describeResponseBody(body []byte) string {
	items := decodeAPIErrorItems(body)
	rendered := make([]string, 0, len(items))
	for _, item := range items {
		if text := strings.TrimSpace(item.String()); text != "" {
			rendered = append(rendered, text)
		}
	}
	if len(rendered) > 0 {
		return strings.Join(rendered, "; ")
	}
	return excerptResponseBody(body)
}

// decodeAPIErrorItems decodes a SendGrid error envelope on a best-effort basis.
//
// It never reports a decoding failure. Receiving something that is not an envelope is a
// perfectly normal outcome when an edge proxy answers instead of the API, and the caller
// always has a bounded excerpt of the raw body to fall back on, so failing here would only
// replace useful diagnostics with a parser complaint.
func decodeAPIErrorItems(body []byte) []APIErrorItem {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var envelope APIErrorResponse
	if err := jsonrs.Unmarshal(body, &envelope); err != nil {
		return nil
	}
	return envelope.Errors
}

// excerptResponseBody renders a bounded, single-line excerpt of a raw response body.
//
// Runs of whitespace - newlines included - are collapsed to single spaces so the excerpt
// cannot break up a structured log line, and truncation happens on a rune boundary so the
// result is always valid UTF-8. The bound matters because this text can end up stored as a
// job's failure reason.
func excerptResponseBody(body []byte) string {
	collapsed := strings.Join(strings.Fields(string(body)), " ")
	if collapsed == "" {
		return ""
	}
	runes := []rune(collapsed)
	if len(runes) <= maxResponseBodyExcerptRunes {
		return collapsed
	}
	return string(runes[:maxResponseBodyExcerptRunes]) + " ... (truncated)"
}

// parseRateLimitHeaderValue reads one advisory rate-limit header as a base-10 integer,
// yielding 0 when the header is absent, empty, unparseable or negative.
//
// Rate-limit headers are diagnostics, not control flow: a malformed one must never turn an
// otherwise perfectly clear rate-limit response into an unrelated parsing failure, and it
// must never panic. Zero is a safe sentinel because RateLimitError.Error reports an absent
// window explicitly. Negative values are discarded too, since a window cannot reset in the
// distant past and a quota cannot be negative.
func parseRateLimitHeaderValue(header http.Header, name string) int64 {
	raw := strings.TrimSpace(header.Get(name))
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0
	}
	return value
}
