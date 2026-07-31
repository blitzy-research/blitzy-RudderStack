package sendgridbulkupload

//go:generate mockgen -destination=../../../../mocks/router/sendgridbulkupload/sendgridbulkupload_mock.go -package=mocks github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sendgrid-bulk-upload SendGridAPIService

import (
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	// destName is the single source of truth for this connector's destination-definition
	// name, and it must be used verbatim as the "destType" stats tag and as log context
	// everywhere in this package so that the tag can never drift from the registered name.
	//
	// The very same literal is registered in three places outside this package - the batch
	// destination catalog the processor consults, the async destination list the batch
	// router classifies against, and the async destination manager factory switch - and
	// every one of them compares it as a plain string. A typo therefore produces neither a
	// compile error nor a test failure: the destination would simply never be routed
	// anywhere, which is why the value lives in exactly one place.
	//
	// Note that this is deliberately NOT the pre-existing "SENDGRID" cloud destination,
	// which is a different, synchronously delivered destination handled by the regular
	// router and is untouched by this connector.
	destName = "SENDGRID_BULK_UPLOAD"
)

// DestinationConfig is the typed view of this connector's destination configuration, as
// delivered by the control plane in the destination's untyped config map.
//
// It is populated by round-tripping that untyped map through jsonrs.Marshal followed by
// jsonrs.Unmarshal, so it carries plain JSON tags only and needs no custom unmarshaller.
// The tags are the keys the control plane sends and follow the lowerCamelCase convention
// used by every other bulk-upload connector in this tree.
type DestinationConfig struct {
	// APIKey is the SendGrid API key presented as a static bearer credential on every
	// request. It is mandatory: SendGrid authenticates the Marketing Contacts API with this
	// key alone, so a destination without it cannot do any useful work and construction of
	// the manager must fail outright rather than hand back a half-configured uploader that
	// would only fail later, once per batch, with an opaque 401.
	APIKey string `json:"apiKey"`

	// ListIDs are the SendGrid Marketing Campaigns list IDs that every contact in an upload
	// is added to.
	//
	// It is optional. An upsert carrying no list IDs still creates or updates the contacts,
	// it simply does not associate them with any list. It is also the lowest-priority
	// source of list IDs: a per-event context.externalId entry of type "listIds" takes
	// precedence over it, so that a single destination can target different lists per event.
	ListIDs []string `json:"listIds"`

	// CustomFieldsMapping maps a RudderStack trait name (the key) onto a SendGrid custom
	// field ID (the value) - an opaque identifier such as "w1" or "w2".
	//
	// The mapping is explicit and operator-supplied because SendGrid requires a custom
	// field to exist before any value can be written to it and addresses it by ID rather
	// than by name. Inventing field names would therefore produce nothing but rejected
	// requests, so a trait with no entry here is not sent as a custom field at all.
	CustomFieldsMapping map[string]string `json:"customFieldsMapping"`
}

// Contact is one contact object of the SendGrid Marketing Contacts upsert request body.
//
// SendGrid upserts contacts: a field omitted from the request is left exactly as it was on
// the existing contact, whereas a field sent with an empty value OVERWRITES whatever was
// stored before. Every field therefore carries omitempty, so that a trait missing from a
// RudderStack event can never silently erase data already held in SendGrid.
//
// A contact must carry at least one of Email, PhoneNumberID, ExternalID or AnonymousID;
// SendGrid rejects a contact that has none of the four. Email is the primary identifier and
// SendGrid lower-cases it on ingestion, which is precisely what lets an import's errors be
// reconciled against the jobs that produced it without keeping any state between calls.
type Contact struct {
	// Email is the primary contact identifier. SendGrid lower-cases it automatically, so it
	// is also lower-cased locally before it is used as a reconciliation key.
	Email string `json:"email,omitempty"`

	// PhoneNumberID is an alternative unique identifier and must be a valid phone number.
	PhoneNumberID string `json:"phone_number_id,omitempty"`

	// ExternalID is an alternative unique identifier. RudderStack maps an event's userId
	// onto it by default.
	ExternalID string `json:"external_id,omitempty"`

	// AnonymousID is an alternative unique identifier. RudderStack maps an event's
	// anonymousId onto it.
	AnonymousID string `json:"anonymous_id,omitempty"`

	// FirstName is a reserved SendGrid contact field.
	FirstName string `json:"first_name,omitempty"`

	// LastName is a reserved SendGrid contact field.
	LastName string `json:"last_name,omitempty"`

	// AddressLine1 is a reserved SendGrid contact field.
	AddressLine1 string `json:"address_line_1,omitempty"`

	// AddressLine2 is a reserved SendGrid contact field.
	AddressLine2 string `json:"address_line_2,omitempty"`

	// City is a reserved SendGrid contact field.
	City string `json:"city,omitempty"`

	// StateProvinceRegion is a reserved SendGrid contact field.
	StateProvinceRegion string `json:"state_province_region,omitempty"`

	// PostalCode is a reserved SendGrid contact field.
	PostalCode string `json:"postal_code,omitempty"`

	// Country is a reserved SendGrid contact field and accepts either a full country name
	// or an abbreviation.
	Country string `json:"country,omitempty"`

	// AlternateEmails holds additional email addresses belonging to the same contact.
	AlternateEmails []string `json:"alternate_emails,omitempty"`

	// CustomFields holds values for custom fields that already exist in SendGrid, keyed by
	// their SendGrid custom field ID as resolved through DestinationConfig.CustomFieldsMapping.
	CustomFields map[string]any `json:"custom_fields,omitempty"`
}

// UpsertRequest is the body of the SendGrid Marketing Contacts upsert call.
//
// The endpoint accepts at most 30,000 contacts or 6MB of data per request, whichever limit
// is reached first, so a caller must chunk its contacts against both caps - and must budget
// the byte cap against the whole serialized body, envelope and list IDs included, not just
// against the contacts.
type UpsertRequest struct {
	// ListIDs is optional: when it is empty the contacts are upserted without being added
	// to any list, so the key is omitted from the body entirely rather than being sent as
	// null or as an empty array.
	ListIDs []string `json:"list_ids,omitempty"`

	// Contacts is required and is always serialized, even when the slice is empty, because
	// the endpoint expects the key to be present.
	Contacts []Contact `json:"contacts"`
}

// UpsertResponse is the body SendGrid returns with HTTP 202 Accepted once it has queued an
// upsert for asynchronous processing. A 202 means "accepted for processing", never
// "applied", which is why the connector polls.
type UpsertResponse struct {
	// JobID identifies the queued import. It is the value that has to be persisted in the
	// batch router's importing parameters and later handed back to
	// SendGridAPIService.GetImportStatus.
	JobID string `json:"job_id"`
}

// ImportStatusResponse is the body of the SendGrid import status call, which is addressed
// with the job_id returned by the upsert.
//
// Status is one of exactly four documented values:
//
//	pending   - the import has not finished; this is the only non-terminal state
//	completed - the import finished without any errors
//	errored   - the import finished with some errors, described by Results.ErrorsURL
//	failed    - the import finished with all errors, or was entirely unprocessable
//
// There is no "processing" or "in_progress" value, and "completed" carries the promise that
// nothing errored - SendGrid signals a partial failure with "errored".
type ImportStatusResponse struct {
	// ID echoes the import's job_id.
	ID string `json:"id"`

	// Status is the import state; see the four documented values above.
	Status string `json:"status"`

	// JobType describes the kind of import SendGrid ran, for example an upsert.
	JobType string `json:"job_type"`

	// Results MUST stay nested. SendGrid reports the per-row counters and the errors
	// document URL inside a "results" object, never at the top level. A flattened struct
	// would still unmarshal without any error and would then read ErroredCount as 0
	// forever, so every partially failed import would be reported as a clean success and
	// the failing rows would be marked delivered. That makes this nesting the single
	// highest-risk detail of the whole connector.
	Results ImportResults `json:"results"`

	// StartedAt is when SendGrid began the import. It is kept as an opaque string on
	// purpose: nothing in this connector reasons about the instant, so parsing it would add
	// a failure mode and buy nothing.
	StartedAt string `json:"started_at"`

	// FinishedAt is when SendGrid finished the import, kept as an opaque string for the
	// same reason as StartedAt.
	FinishedAt string `json:"finished_at"`
}

// ImportResults is the nested "results" object of an import status response. It is a
// distinct type rather than an inline struct so that tests and the poll mapping can build
// and assert on it directly.
type ImportResults struct {
	// RequestedCount is how many contacts the upsert asked SendGrid to process.
	RequestedCount int `json:"requested_count"`

	// CreatedCount is how many contacts SendGrid created.
	CreatedCount int `json:"created_count"`

	// UpdatedCount is how many existing contacts SendGrid updated.
	UpdatedCount int `json:"updated_count"`

	// DeletedCount is how many contacts SendGrid deleted, which stays zero for an upsert.
	DeletedCount int `json:"deleted_count"`

	// ErroredCount is how many rows SendGrid could not process. Any value above zero means
	// the import must be reconciled row by row through the errors document, no matter which
	// status the import reports.
	ErroredCount int `json:"errored_count"`

	// ErrorsURL is an authenticated URL serving a document that describes the errored rows.
	// It is empty when nothing errored.
	ErrorsURL string `json:"errors_url"`
}

// ImportErrorRow is one row of the document SendGrid publishes at ImportResults.ErrorsURL,
// reduced to the only two things reconciliation actually needs.
//
// The model is deliberately loose - two plain strings and no JSON tags - because that
// document's schema is genuinely undocumented. The official specification mentions
// errors_url exactly twice and both times only as a bare string URL, with no media type,
// no schema and no stated retention, and the reference pages describe no format at all.
// Committing a rigid shape here would turn any difference between the guess and reality
// into silent data loss, because a row that fails to match leaves a job that really failed
// reported as delivered.
//
// The tolerant parser in the manager therefore decodes the document generically and fills
// these fields from whichever candidate key each row actually carries: message,
// error_message, reason or detail for Message, and email, contact.email, identifier,
// external_id or anonymous_id for Identifier.
type ImportErrorRow struct {
	// Identifier is the contact identifier the row refers to, normally the email address.
	// It is compared case-insensitively against the identifiers re-derived from the
	// importing jobs, so that a row can be resolved back to the job that produced it
	// without any state having been carried over from the upload.
	Identifier string

	// Message is the human-readable reason SendGrid could not process the row. It becomes
	// the recorded failure reason of the job the row resolves to.
	Message string
}

// APIErrorItem is one entry of the standard SendGrid error envelope.
type APIErrorItem struct {
	// Field names the request field the error refers to.
	//
	// It is a pointer because SendGrid legitimately sends a null field for errors that are
	// not tied to any particular field - the documented rate-limit body is exactly
	// {"errors":[{"field":null,"message":"too many requests"}]} - and a plain string could
	// not tell that null apart from an empty field name.
	Field *string `json:"field"`

	// Message is the human-readable reason SendGrid rejected the request.
	Message string `json:"message"`
}

// String renders one error entry, tolerating the null field described on Field.
func (i APIErrorItem) String() string {
	if i.Field == nil {
		return i.Message
	}
	return "field=" + *i.Field + ": " + i.Message
}

// APIErrorResponse is the wire shape of a SendGrid error body, which wraps one or more
// entries under an "errors" key. It is decoded on a best-effort basis: a response that is
// not a SendGrid error envelope at all, such as an HTML page returned by an edge proxy,
// simply yields no entries and is reported through APIError.Message instead.
type APIErrorResponse struct {
	// Errors holds the individual error entries SendGrid reported.
	Errors []APIErrorItem `json:"errors"`
}

// APIError is the error a SendGridAPIService implementation returns for any SendGrid
// response that is not a success, other than a rate limit - which is reported as the more
// specific *RateLimitError so that callers can treat it as retryable.
//
// It is returned as a pointer, so a caller can recover the concrete value with
// errors.As and branch on the status code rather than on the message text.
type APIError struct {
	// StatusCode is the HTTP status code SendGrid answered with. It is carried explicitly
	// so that a caller can map the outcome onto the batch router's retryable or terminal
	// channel without re-parsing anything.
	StatusCode int

	// Operation names the SendGrid call that failed, for example "upload contacts", so a
	// single reason string tells an operator both what failed and why.
	Operation string

	// Message carries a summary of the failure. It is used on its own whenever the response
	// body was absent, empty or not a SendGrid error envelope, so that an operator is never
	// left holding nothing but a bare status code.
	Message string

	// Errors holds the decoded envelope entries, when the body contained any.
	Errors []APIErrorItem
}

// Error implements error. It is defined on the pointer receiver so that the concrete type
// survives being wrapped and can be recovered with errors.As.
func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	operation := e.Operation
	if operation == "" {
		operation = "request"
	}
	parts := []string{fmt.Sprintf("sendgrid %s failed with status %d", operation, e.StatusCode)}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	if len(e.Errors) > 0 {
		parts = append(parts, strings.Join(lo.Map(e.Errors, func(item APIErrorItem, _ int) string {
			return item.String()
		}), "; "))
	}
	return strings.Join(parts, ": ")
}

// RateLimitError is the typed error a SendGridAPIService implementation returns when
// SendGrid answers with HTTP 429.
//
// It exists so that a caller can tell a rate limit apart from every other failure with
// errors.As and route the affected jobs to the batch router's RETRYABLE channel rather
// than its terminal one. A rate limit is never a permanent condition: the batch router
// already owns retry, backoff and the decision to give up, so this connector must never
// abort a job because of a 429, and it deliberately installs no client-side limiter of its
// own either - SendGrid publishes no per-endpoint figure for the Marketing Contacts
// endpoints, so any pre-emptive throttle would be a guess. The connector is reactive to
// 429 instead, which is exactly what this type makes possible.
//
// Error renders the advertised reset window and is intentionally deterministic: it never
// reads the wall clock, so its output can be embedded verbatim in an upload's failure
// reason and asserted on in tests.
type RateLimitError struct {
	// StatusCode is the HTTP status SendGrid answered with, which is 429 for a rate limit.
	// It is carried explicitly so that callers branch on the value and not on the type
	// alone, exactly as they do for APIError.
	StatusCode int

	// RetryAfter is the raw Retry-After header value, or the empty string when the header is
	// absent - which is the normal case.
	//
	// Retry-After is NOT documented anywhere for the SendGrid v3 Web API, so it must always
	// be treated as optional. It is still read first when present, because an edge or proxy
	// in front of the API may inject it and it is then the most direct statement of how long
	// to wait. It is kept raw because the header may hold either a delay in seconds or an
	// HTTP date, and normalizing it would discard information for no benefit.
	RetryAfter string

	// ResetAt is the parsed X-RateLimit-Reset header: a UNIX timestamp in SECONDS at which
	// the current rate-limit window resets.
	//
	// It is an absolute instant, NOT a delay - reading it as a duration would produce a wait
	// of decades - and it is zero when the header was absent or could not be parsed.
	ResetAt int64

	// Limit mirrors the X-RateLimit-Limit header: how many requests the window allows. It is
	// zero when the header was absent or unparseable.
	Limit int

	// Remaining mirrors the X-RateLimit-Remaining header: how many requests are left in the
	// window. It is zero when the header was absent or unparseable, and SendGrid reports no
	// remaining quota on a 429 in any case, so zero is safely read as exhausted or unknown.
	Remaining int

	// Message carries whatever detail SendGrid supplied in the response body, typically
	// "too many requests".
	Message string
}

// Error implements error, rendering the rate-limit window in a form an operator can act on.
//
// It is defined on the pointer receiver so that the concrete type survives being wrapped
// and can be recovered with errors.As, and it is safe on a zero value: an absent
// Retry-After, an absent or unparseable reset timestamp and absent limit headers are each
// reported as such instead of being rendered as a misleading zero.
func (e *RateLimitError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("sendgrid rate limited the request with status %d", e.StatusCode)}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	if e.RetryAfter != "" {
		parts = append(parts, "Retry-After: "+e.RetryAfter)
	}
	if e.ResetAt > 0 {
		// Rendered as an absolute UTC instant rather than as a remaining duration, so the
		// message stays deterministic for a given error value.
		parts = append(parts, "rate limit window resets at "+time.Unix(e.ResetAt, 0).UTC().Format(time.RFC3339))
	}
	if e.RetryAfter == "" && e.ResetAt <= 0 {
		parts = append(parts, "no reset window was advertised by sendgrid")
	}
	if e.Limit > 0 || e.Remaining > 0 {
		parts = append(parts, fmt.Sprintf("X-RateLimit-Limit: %d, X-RateLimit-Remaining: %d", e.Limit, e.Remaining))
	}
	return strings.Join(parts, ", ")
}

// SendGridAPIService is the seam between this connector and the three SendGrid REST
// operations it needs: upserting a batch of contacts, reading an import's status, and
// fetching an import's errors document. There is exactly one method per operation and no
// other outbound integration exists, which is what lets every test scenario be expressed
// through mock expectations with no network access whatsoever.
//
// Implementations must never return a nil value together with a nil error: every method
// either yields a usable result or a non-nil error, so callers may branch on the error
// alone. A failure that describes a SendGrid response should be reported as *APIError, and
// a 429 must be reported as *RateLimitError so that the caller can detect it with
// errors.As and keep the affected jobs retryable.
type SendGridAPIService interface {
	// UploadContacts issues the Marketing Contacts upsert and returns the job_id SendGrid
	// assigned to the queued import. Only HTTP 202 counts as success; every other status is
	// an error, and 429 specifically is a *RateLimitError.
	UploadContacts(request UpsertRequest) (*UpsertResponse, error)

	// GetImportStatus reads the status of a queued import, passing the job_id returned by
	// UploadContacts as the path parameter the API names id.
	GetImportStatus(jobID string) (*ImportStatusResponse, error)

	// GetImportErrors performs an authenticated GET of the URL SendGrid published in
	// ImportResults.ErrorsURL and returns the document unmodified.
	//
	// It stays raw on purpose. The document's schema is undocumented, so interpreting it
	// belongs to the tolerant parser in the manager rather than to the transport adapter,
	// which must not encode a guess about a shape it cannot verify.
	GetImportErrors(errorsURL string) ([]byte, error)
}

// SendGridBulkUploader is this connector's async destination manager, registered under the
// destName destination-definition name: it transforms events into SendGrid contacts, upserts
// them in capped batches, polls the resulting import and reconciles its per-row errors back
// onto the originating jobs.
//
// Every field is exported for two concrete reasons. The package's tests live in an external
// test package and build the uploader as a literal, and the two caps have to be shrinkable
// so that chunking boundaries can be exercised without materializing tens of thousands of
// contacts.
//
// The struct deliberately holds NO per-upload or cross-invocation state. Upload statistics
// may be reconciled in a different process invocation, or on a different pod, from the
// upload that produced the import, so anything cached at upload time would simply be
// missing when reconciliation needs it - and a connector that depended on it would report
// failed rows as delivered after a restart. Reconciliation instead re-derives everything it
// needs from the importing jobs it is handed. Staying stateless also keeps the package
// correct under the repository's shuffled test execution.
type SendGridBulkUploader struct {
	// Logger is the logger for this destination. Only the non-sugared, type-specific field
	// API may be used with it.
	Logger logger.Logger

	// StatsFactory builds this connector's counters. Every stat must be tagged with
	// {module: batch_router, destType: destName, destID: DestinationID}.
	StatsFactory stats.Stats

	// DestinationID is the destination's backend-config ID. It is both the destID stats tag
	// and the destination ID that every upload outcome has to carry back to the batch
	// router, which keys its bookkeeping on it.
	DestinationID string

	// DestinationName is the destination's human-readable name, used only as log context.
	//
	// The destType stats tag must come from the destName constant instead of from this
	// field, so that the tag can never drift from the registered destination-definition
	// name even if an operator renames the destination.
	DestinationName string

	// DestinationConfig is the parsed destination configuration: the API key, the target
	// list IDs and the trait-to-custom-field mapping.
	DestinationConfig DestinationConfig

	// SendGridAPIService performs the three SendGrid calls. It is held as the interface
	// rather than as the concrete adapter so that tests can substitute the generated mock
	// and exercise every response, including a rate limit, without touching the network.
	SendGridAPIService SendGridAPIService

	// MaxContactsPerRequest caps how many contacts one upsert may carry, mirroring the
	// endpoint's documented ceiling of 30,000.
	MaxContactsPerRequest int // Override for testing (0 = use default)

	// MaxRequestBytes caps the serialized size of one upsert body, budgeted below the
	// endpoint's documented 6MB ceiling so that the envelope and the list IDs fit inside it
	// too.
	MaxRequestBytes int // Override for testing (0 = use default)
}
