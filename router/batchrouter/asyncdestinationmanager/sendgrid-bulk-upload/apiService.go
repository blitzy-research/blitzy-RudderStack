package sendgridbulkupload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	sendGridBaseURL    = "https://api.sendgrid.com"
	contactsPath       = "/v3/marketing/contacts"
	contactImportsPath = "/v3/marketing/contacts/imports/"
)

const (
	defaultTimeout             = 30 * time.Second
	defaultIdleConnTimeout     = 90 * time.Second
	defaultMaxIdleConnsPerHost = 50
	defaultMaxConnsPerHost     = 100
	defaultDialTimeout         = 10 * time.Second
	defaultKeepAlive           = 30 * time.Second
)

// Operation names. They are this connector's own vocabulary, deliberately not the provider's, and
// they are the only description of a failed call that reaches a log line or a persisted reason.
const (
	operationUploadContacts  = "marketing contacts upsert"
	operationGetImportStatus = "contacts import status read"
	operationGetImportErrors = "contacts import errors document fetch"
)

// Bounds this adapter applies to everything it reads or accepts. They are internal constants rather
// than configuration on purpose: they exist to keep a hostile or broken response from exhausting a
// shared batch router process, and a limit an operator can widen without a release is not a limit.
const (
	// maxAPIResponseBytes bounds the two small JSON responses of the canonical API. Both are a
	// handful of fields, so a megabyte is already several orders of magnitude of headroom.
	maxAPIResponseBytes int64 = 1 << 20

	// maxErrorsDocumentBytes bounds the errors document. Its schema is undocumented and its size is
	// driven by how many contacts an import rejected, so it is budgeted generously - an import may
	// carry 30,000 contacts - while still being bounded.
	maxErrorsDocumentBytes int64 = 32 << 20

	// maxErrorsURLLength bounds the URL this adapter is willing to parse and fetch. Pre-signed
	// object-storage URLs are long, so the bound is generous; it exists to stop an absurd value
	// rather than to enforce a shape.
	maxErrorsURLLength = 2048

	maxErrorsURLRedirects = 5

	// maxImportJobIDLength bounds untrusted path input while leaving ample room for provider-issued
	// identifiers.
	maxImportJobIDLength = 256

	maxAPIErrorItems = 64

	maxRetryAfterSeconds = 86400
)

// Reasons an errors document URL is refused. They are stable tokens rather than prose so that they
// can be logged, counted and persisted without echoing a provider-controlled URL - which may be
// pre-signed and therefore carry a credential in its query string.
const (
	rejectionBlankURL       = "blank_url"
	rejectionURLTooLong     = "url_too_long"
	rejectionUnparseableURL = "unparseable_url"
	rejectionScheme         = "unsupported_scheme"
	rejectionCredentials    = "credentials_in_url"
	rejectionBlankHost      = "blank_host"
	rejectionPort           = "unsupported_port"
	rejectionAddress        = "address_not_publicly_routable"
	rejectionRedirect       = "redirect_not_permitted"
)

// errorsURLRejection reports that this connector declined to fetch an errors document URL. It
// carries a stable reason token and never the URL itself.
type errorsURLRejection struct {
	reason string
}

func (e *errorsURLRejection) Error() string {
	return fmt.Sprintf("sendgrid published an errors document URL this connector will not fetch (reason: %s)", e.reason)
}

// sendGridAPIServiceImpl is the HTTP adapter over the three SendGrid operations.
//
// It NEVER logs: every failure is returned as a typed value, so the manager - which is the only
// component that knows the destination and the jobs involved - stays the single owner of diagnostics
// and there is no second, unattributed copy of every failure in the logs. It emits metrics only.
type sendGridAPIServiceImpl struct {
	apiKey       string
	client       *http.Client
	errorsClient *http.Client
	statsFactory stats.Stats
	statLabels   stats.Tags
}

var _ SendGridAPIService = (*sendGridAPIServiceImpl)(nil)

// NewSendGridAPIService builds the adapter from the ALREADY PARSED destination configuration, so the
// typed view stays the single reading of what the control plane sent.
//
// It fails when the API key is missing or blank. SendGrid authenticates the Marketing Contacts API
// with that key alone, so reporting the problem once, at construction, is strictly better than
// letting every batch fail later with an opaque 401.
func NewSendGridAPIService(destinationID string, destinationConfig DestinationConfig, statsFactory stats.Stats) (SendGridAPIService, error) {
	apiKey := strings.TrimSpace(destinationConfig.APIKey)
	if apiKey == "" {
		return nil, errors.New("apiKey is missing from the sendgrid destination configuration")
	}
	if statsFactory == nil {
		statsFactory = stats.NOP
	}
	return &sendGridAPIServiceImpl{
		apiKey:       apiKey,
		client:       newAPIHTTPClient(),
		errorsClient: newErrorsDocumentHTTPClient(),
		statsFactory: statsFactory,
		statLabels: stats.Tags{
			"module":   batchRouterModule,
			"destType": destName,
			"destID":   destinationID,
		},
	}, nil
}

func newTransport() *http.Transport {
	return &http.Transport{
		MaxIdleConns:        defaultMaxConnsPerHost,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		IdleConnTimeout:     defaultIdleConnTimeout,
		// Do not advertise automatic gzip support.
		DisableCompression: true,
	}
}

// newAPIHTTPClient builds the client for the two canonical API calls.
//
// It refuses every redirect. The canonical API does not redirect, and following one would replay the
// bearer credential - and, for the upsert, the whole contact payload - to whatever host the response
// nominated.
func newAPIHTTPClient() *http.Client {
	return &http.Client{
		Transport: newTransport(),
		Timeout:   defaultTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return &errorsURLRejection{reason: rejectionRedirect}
		},
	}
}

// newErrorsDocumentHTTPClient builds the client for the errors document fetch.
//
// The errors document URL is chosen by the provider and may legitimately point at object storage
// rather than at api.sendgrid.com, so no host policy is applied - a host allow-list would strand a
// perfectly valid provider URL and leave the import unreconcilable until an operator intervened. The
// request is instead contained by controls that do not depend on knowing the host in advance:
// HTTPS only, connections only to publicly routable addresses, a bounded number of redirects with
// the credential and the referrer stripped on every hop, and a bounded read.
func newErrorsDocumentHTTPClient() *http.Client {
	transport := newTransport()
	transport.DialContext = (&net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: defaultKeepAlive,
		Control:   controlPubliclyRoutableAddress,
	}).DialContext
	return &http.Client{
		Transport:     transport,
		Timeout:       defaultTimeout,
		CheckRedirect: checkErrorsDocumentRedirect,
	}
}

// checkErrorsDocumentRedirect keeps a redirected errors document fetch as safe as the first hop.
//
// Go strips sensitive headers only when a redirect changes domain, and it copies a Referer forward;
// a pre-signed errors URL carries its authorization in the query string, so both the Authorization
// header and the Referer are removed on EVERY hop. The dialer control still applies to each new
// connection, so a redirect cannot reach a private address either.
func checkErrorsDocumentRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxErrorsURLRedirects {
		return &errorsURLRejection{reason: rejectionRedirect}
	}
	if !strings.EqualFold(req.URL.Scheme, "https") {
		return &errorsURLRejection{reason: rejectionScheme}
	}
	if req.URL.User != nil {
		return &errorsURLRejection{reason: rejectionCredentials}
	}
	if port := req.URL.Port(); port != "" && port != "443" {
		return &errorsURLRejection{reason: rejectionPort}
	}
	req.Header.Del("Authorization")
	req.Header.Del("Referer")
	return nil
}

// controlPubliclyRoutableAddress refuses to open a connection to anything that is not a public
// address, which is what keeps a provider-supplied URL from being turned into a request against
// loopback, link-local metadata services or private infrastructure.
func controlPubliclyRoutableAddress(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return &errorsURLRejection{reason: rejectionAddress}
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return &errorsURLRejection{reason: rejectionAddress}
	}
	if !isPubliclyRoutableAddr(addr) {
		return &errorsURLRejection{reason: rejectionAddress}
	}
	return nil
}

// nonGlobalPrefixes lists the ranges netip's own predicates do not cover but which are still not
// reachable public destinations: carrier-grade NAT, IETF protocol assignments, benchmarking,
// documentation, NAT64 and reserved space.
var nonGlobalPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func isPubliclyRoutableAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() ||
		addr.IsUnspecified() ||
		addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() ||
		addr.IsMulticast() {
		return false
	}
	for _, prefix := range nonGlobalPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func setRequestHeaders(req *http.Request, apiKey string, hasBody bool) {
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	if hasBody {
		req.Header.Set("Content-Type", "application/json")
	}
}

func (s *sendGridAPIServiceImpl) newAPIRequest(operation, method, endpoint string, payload []byte) (*http.Request, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewBuffer(payload)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("building the sendgrid %s request: %w", operation, err)
	}
	setRequestHeaders(req, s.apiKey, payload != nil)
	return req, nil
}

// UploadContacts upserts one batch of contacts and returns the import job identifier SendGrid
// issued for it. Only 202 Accepted is success: the endpoint queues the work, so any other status
// means nothing was accepted.
func (s *sendGridAPIServiceImpl) UploadContacts(request UpsertRequest) (*UpsertResponse, error) {
	if len(request.Contacts) == 0 {
		return nil, errors.New("refusing to send a sendgrid marketing contacts upsert carrying no contacts")
	}
	payload, err := jsonrs.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshalling the sendgrid %s request: %w", operationUploadContacts, err)
	}
	s.statsFactory.NewTaggedStat("sendgrid_upload_payload_size", stats.HistogramType, s.statLabels).
		Observe(float64(len(payload)))

	req, err := s.newAPIRequest(operationUploadContacts, http.MethodPut, sendGridBaseURL+contactsPath, payload)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s request failed: %w", operationUploadContacts, err)
	}
	defer func() { _ = resp.Body.Close() }()
	s.statsFactory.NewTaggedStat("sendgrid_upload_time", stats.TimerType, s.statLabels).Since(startTime)

	body, err := readLimitedBody(resp.Body, maxAPIResponseBytes)
	if err != nil {
		return nil, err
	}
	if err := classifyResponse(operationUploadContacts, http.StatusAccepted, resp, body); err != nil {
		return nil, err
	}
	var upsert UpsertResponse
	if err := jsonrs.Unmarshal(body, &upsert); err != nil {
		return nil, fmt.Errorf("decoding the sendgrid %s response: %w", operationUploadContacts, err)
	}
	upsert.JobID = strings.TrimSpace(upsert.JobID)
	if upsert.JobID == "" {
		return nil, fmt.Errorf("sendgrid accepted the %s without returning a job id", operationUploadContacts)
	}
	if len(upsert.JobID) > maxImportJobIDLength {
		return nil, fmt.Errorf("sendgrid returned a %s job id longer than the %d characters this connector accepts",
			operationUploadContacts, maxImportJobIDLength)
	}
	return &upsert, nil
}

// GetImportStatus reads one import's status. The value SendGrid returned as job_id is passed into
// the path parameter the specification names id.
func (s *sendGridAPIServiceImpl) GetImportStatus(jobID string) (*ImportStatusResponse, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, errors.New("refusing to read a sendgrid contacts import status without a job id")
	}
	if len(jobID) > maxImportJobIDLength {
		return nil, fmt.Errorf("the sendgrid contacts import job id is longer than the %d characters this connector accepts",
			maxImportJobIDLength)
	}
	req, err := s.newAPIRequest(operationGetImportStatus, http.MethodGet,
		sendGridBaseURL+contactImportsPath+url.PathEscape(jobID), nil)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s request failed: %w", operationGetImportStatus, err)
	}
	defer func() { _ = resp.Body.Close() }()
	s.statsFactory.NewTaggedStat("sendgrid_poll_time", stats.TimerType, s.statLabels).Since(startTime)

	body, err := readLimitedBody(resp.Body, maxAPIResponseBytes)
	if err != nil {
		return nil, err
	}
	if err := classifyResponse(operationGetImportStatus, http.StatusOK, resp, body); err != nil {
		return nil, err
	}
	var status ImportStatusResponse
	if err := jsonrs.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("decoding the sendgrid %s response: %w", operationGetImportStatus, err)
	}
	return &status, nil
}

// GetImportErrors fetches the document an import published at results.errors_url and returns it
// verbatim - no unmarshalling, no content sniffing, no decompression - because that document's
// schema is undocumented and interpreting it is the tolerant parser's job, not the adapter's.
//
// The bearer credential is attached only for hosts SendGrid itself serves. A provider-issued
// object-storage URL authenticates through its own signature, and presenting this destination's API
// key to a third-party host would disclose the credential for no benefit.
func (s *sendGridAPIServiceImpl) GetImportErrors(errorsURL string) ([]byte, error) {
	target, err := validateErrorsURL(errorsURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building the sendgrid %s request: %w", operationGetImportErrors, err)
	}
	req.Header.Set("Accept", "*/*")
	if isSendGridHost(target.Hostname()) {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}
	startTime := time.Now()
	resp, err := s.errorsClient.Do(req)
	if err != nil {
		// The transport error is NOT wrapped: for this one call the URL inside it is
		// provider-controlled and may be pre-signed, so only a classified summary is returned.
		return nil, fmt.Errorf("sendgrid %s request failed (transport error class: %s)",
			operationGetImportErrors, transportErrorClass(err))
	}
	defer func() { _ = resp.Body.Close() }()
	s.statsFactory.NewTaggedStat("sendgrid_errors_document_time", stats.TimerType, s.statLabels).Since(startTime)

	body, err := readLimitedBody(resp.Body, maxErrorsDocumentBytes)
	if err != nil {
		return nil, err
	}
	if err := classifyResponse(operationGetImportErrors, http.StatusOK, resp, body); err != nil {
		return nil, err
	}
	s.statsFactory.NewTaggedStat("sendgrid_errors_document_size", stats.HistogramType, s.statLabels).
		Observe(float64(len(body)))
	return body, nil
}

// validateErrorsURL accepts any public HTTPS URL, which is what the provider contract requires, and
// refuses only shapes that are unusable or unsafe. Deliberately NO host policy: see
// newErrorsDocumentHTTPClient for how the fetch is contained instead.
func validateErrorsURL(rawURL string) (*url.URL, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil, &errorsURLRejection{reason: rejectionBlankURL}
	}
	if len(trimmed) > maxErrorsURLLength {
		return nil, &errorsURLRejection{reason: rejectionURLTooLong}
	}
	target, err := url.Parse(trimmed)
	if err != nil {
		return nil, &errorsURLRejection{reason: rejectionUnparseableURL}
	}
	if !strings.EqualFold(target.Scheme, "https") {
		return nil, &errorsURLRejection{reason: rejectionScheme}
	}
	if target.User != nil {
		return nil, &errorsURLRejection{reason: rejectionCredentials}
	}
	if target.Hostname() == "" {
		return nil, &errorsURLRejection{reason: rejectionBlankHost}
	}
	if port := target.Port(); port != "" && port != "443" {
		return nil, &errorsURLRejection{reason: rejectionPort}
	}
	return target, nil
}

func isSendGridHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	for _, domain := range []string{"sendgrid.com", "sendgrid.net"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

// readLimitedBody reads at most limit bytes and treats anything longer as a failure rather than
// silently truncating, because a truncated document cannot be interpreted safely.
func readLimitedBody(body io.Reader, limit int64) ([]byte, error) {
	read, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("reading the sendgrid response body: %w", err)
	}
	if int64(len(read)) > limit {
		return nil, fmt.Errorf("the sendgrid response exceeded the %d byte limit this connector reads", limit)
	}
	return read, nil
}

// classifyResponse is the single place a SendGrid status code is interpreted. A 429 becomes the
// typed rate-limit error, so no other part of the package has to re-sniff status codes to keep
// rate-limited jobs on the retryable channel.
func classifyResponse(operation string, successCode int, resp *http.Response, body []byte) error {
	if resp.StatusCode == successCode {
		return nil
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return newRateLimitError(resp.Header)
	}
	return &APIError{
		Operation:  operation,
		StatusCode: resp.StatusCode,
		Items:      decodeAPIErrorItems(body),
	}
}

// newRateLimitError reads the rate-limit window out of the response headers.
//
// Retry-After is read first because it is the standard HTTP header and an edge or proxy may inject
// it, but it is never required: it is not documented for the SendGrid v3 API. The documented header
// is X-RateLimit-Reset, whose value is a UNIX epoch-SECONDS timestamp rather than a delta.
func newRateLimitError(header http.Header) *RateLimitError {
	return &RateLimitError{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: parseRetryAfterHeader(header.Get("Retry-After")),
		ResetEpoch: parseRateLimitHeaderValue(header, "X-RateLimit-Reset", 0),
		Limit:      parseRateLimitHeaderValue(header, "X-RateLimit-Limit", -1),
		Remaining:  parseRateLimitHeaderValue(header, "X-RateLimit-Remaining", -1),
	}
}

func parseRateLimitHeaderValue(header http.Header, name string, absent int64) int64 {
	raw := strings.TrimSpace(header.Get(name))
	if raw == "" {
		return absent
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return absent
	}
	return value
}

// parseRetryAfterHeader normalizes Retry-After into a canonical duration string, accepting only the
// two forms HTTP defines - delta seconds and an HTTP date. Anything else is discarded rather than
// carried through, so an arbitrary provider-controlled string can never reach a log or a job status.
func parseRetryAfterHeader(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds <= 0 || seconds > maxRetryAfterSeconds {
			return ""
		}
		return (time.Duration(seconds) * time.Second).String()
	}
	if at, err := http.ParseTime(raw); err == nil {
		delay := time.Until(at).Truncate(time.Second)
		if delay <= 0 || delay > maxRetryAfterSeconds*time.Second {
			return ""
		}
		return delay.String()
	}
	return ""
}

// decodeAPIErrorItems decodes the documented error body so that the number of error items can be
// reported. The items' own text is never rendered anywhere - see APIErrorItem.
func decodeAPIErrorItems(body []byte) []APIErrorItem {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var decoded struct {
		Errors []APIErrorItem `json:"errors"`
	}
	if err := jsonrs.Unmarshal(body, &decoded); err != nil {
		return nil
	}
	if len(decoded.Errors) > maxAPIErrorItems {
		return decoded.Errors[:maxAPIErrorItems]
	}
	return decoded.Errors
}

// transportErrorClass reduces a transport failure to a stable token, so a diagnosis can be reported
// without repeating a provider-controlled URL that a wrapped net/http error would carry.
func transportErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	var rejection *errorsURLRejection
	if errors.As(err, &rejection) {
		return rejection.reason
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_failure"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "connection_closed"
	}
	return "request_failed"
}
