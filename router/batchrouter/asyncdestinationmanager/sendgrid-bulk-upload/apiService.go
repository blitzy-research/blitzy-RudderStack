package sendgridbulkupload

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// SendGrid Marketing Contacts endpoints.
//
// These are compile-time constants rather than configurable values on purpose: SendGrid
// publishes exactly one global API host for the v3 Web API, so making the origin settable
// would create nothing but a way to misconfigure a destination.
const (
	// sendGridBaseURL is the single origin every call in this adapter is made against.
	sendGridBaseURL = "https://api.sendgrid.com"

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

// setRequestHeaders applies the two headers every SendGrid Marketing Contacts call needs.
//
// SendGrid authenticates the v3 Web API with a static bearer credential, so there is no
// token exchange, no refresh and no OAuth subsystem involved here at all. Content-Type is
// strictly required only on the upsert, which is the one call that carries a body, but it
// is set unconditionally so the header logic lives in exactly one place and cannot drift
// between the three operations; it is inert on a GET.
func setRequestHeaders(req *http.Request, apiKey string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
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
	return &sendGridAPIServiceImpl{
		client:       getDefaultHTTPClient(),
		apiKey:       apiKey,
		logger:       log,
		statsFactory: statsFactory,
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opUploadContacts, resp.StatusCode, err)
	}
	if err := s.classifyResponse(opUploadContacts, http.StatusAccepted, resp, body); err != nil {
		return nil, err
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opGetImportStatus, resp.StatusCode, err)
	}
	if err := s.classifyResponse(opGetImportStatus, http.StatusOK, resp, body); err != nil {
		return nil, err
	}

	var status ImportStatusResponse
	if err := jsonrs.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("sendgrid %s: decoding response body: %w", opGetImportStatus, err)
	}
	return &status, nil
}

// GetImportErrors performs an authenticated GET of the URL SendGrid published in
// ImportResults.ErrorsURL and returns that document exactly as received.
//
// It stays raw on purpose. The document's schema is genuinely undocumented - the official
// specification mentions errors_url only as a bare string URL, with no media type and no
// schema - so interpreting it belongs to the tolerant parser in the manager rather than to
// this transport adapter, which must not encode a guess about a shape it cannot verify.
// Nothing is unmarshalled here, no content type is sniffed and no decompression is
// attempted.
func (s *sendGridAPIServiceImpl) GetImportErrors(errorsURL string) ([]byte, error) {
	if errorsURL == "" {
		return nil, fmt.Errorf("sendgrid %s: errorsURL is empty", opGetImportErrors)
	}
	// The URL is supplied by SendGrid rather than built here, and it still requires the
	// bearer credential, which is why it goes through the same authenticated request path
	// as the two fixed endpoints.
	req, err := s.newRequest(opGetImportErrors, http.MethodGet, errorsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: %w", opGetImportErrors, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The read error is propagated rather than discarded: a truncated errors document
	// handed to the parser as though it were complete would leave the rows lost to the
	// truncation reported as delivered, which is exactly the silent data loss the whole
	// reconciliation path exists to prevent.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sendgrid %s: reading response body (status %d): %w", opGetImportErrors, resp.StatusCode, err)
	}
	if err := s.classifyResponse(opGetImportErrors, http.StatusOK, resp, body); err != nil {
		return nil, err
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
func (s *sendGridAPIServiceImpl) classifyResponse(operation string, successCode int, resp *http.Response, body []byte) error {
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
