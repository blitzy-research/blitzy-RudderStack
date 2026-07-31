package sendgridbulkupload

//go:generate mockgen -destination=../../../../mocks/router/sendgridbulkupload/sendgridbulkupload_mock.go -package=mocks github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sendgrid-bulk-upload SendGridAPIService

import (
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	// destName identifies this connector in logs, metrics, and destination-scoped configuration; it
	// is distinct from the synchronous SENDGRID destination.
	destName = "SENDGRID_BULK_UPLOAD"
)

type DestinationConfig struct {
	// APIKey is the SendGrid API key presented as a static bearer credential on every request. It
	// is mandatory: a destination without it cannot do useful work, so manager construction fails
	// outright rather than returning an uploader that would produce an opaque 401 on every batch.
	APIKey string `json:"apiKey"`

	// ListIDs are the SendGrid Marketing Campaigns lists every contact of an upload is added to.
	// Optional, and the lowest-priority source of list IDs: a per-event context.externalId entry of
	// type "listIds" takes precedence, so one destination can target different lists per event.
	ListIDs []string `json:"listIds"`

	// CustomFieldsMapping maps a RudderStack trait name (key) onto a SendGrid custom field ID
	// (value) - an opaque identifier such as "w1". The mapping is explicit and operator-supplied
	// because SendGrid requires a custom field to exist before a value can be written to it and
	// addresses it by ID rather than by name, so inventing names would only produce rejected
	// requests. It is normalized and validated during construction: keys and values are trimmed and
	// non-blank, and no two traits may claim the same field ID.
	CustomFieldsMapping map[string]string `json:"customFieldsMapping"`
}

// Contact is one Marketing Contacts upsert item. Omitted fields must stay omitted because SendGrid
// preserves them, while fields sent empty overwrite stored values. At least one of Email,
// PhoneNumberID, ExternalID, or AnonymousID must be present. Email is normalized to lower case so
// provider error rows and importing jobs compare consistently.
type Contact struct {
	Email               string         `json:"email,omitempty"`
	PhoneNumberID       string         `json:"phone_number_id,omitempty"`
	ExternalID          string         `json:"external_id,omitempty"`
	AnonymousID         string         `json:"anonymous_id,omitempty"`
	FirstName           string         `json:"first_name,omitempty"`
	LastName            string         `json:"last_name,omitempty"`
	AddressLine1        string         `json:"address_line_1,omitempty"`
	AddressLine2        string         `json:"address_line_2,omitempty"`
	City                string         `json:"city,omitempty"`
	StateProvinceRegion string         `json:"state_province_region,omitempty"`
	PostalCode          string         `json:"postal_code,omitempty"`
	Country             string         `json:"country,omitempty"`
	AlternateEmails     []string       `json:"alternate_emails,omitempty"`
	CustomFields        map[string]any `json:"custom_fields,omitempty"`
}

// UpsertRequest is the body of PUT /v3/marketing/contacts. The endpoint accepts at most 30,000
// contacts or 6 MB per request, whichever is reached first, and the byte cap applies to this whole
// serialized body - the list IDs and the envelope included - not just to the contacts.
type UpsertRequest struct {
	ListIDs  []string  `json:"list_ids,omitempty"`
	Contacts []Contact `json:"contacts"`
}

// UpsertResponse is the 202 Accepted body of the upsert call. The job ID it carries is the handle
// for every later status and error lookup, and it is persisted verbatim so that Poll receives
// exactly the string SendGrid issued.
type UpsertResponse struct {
	JobID string `json:"job_id"`
}

// ImportStatusResponse models GET /v3/marketing/contacts/imports/{id}. Results must remain nested;
// flattening it silently leaves errored_count at zero and can turn partial failures into success.
type ImportStatusResponse struct {
	ID         string        `json:"id"`
	Status     string        `json:"status"`
	JobType    string        `json:"job_type"`
	Results    ImportResults `json:"results"`
	StartedAt  string        `json:"started_at"`
	FinishedAt string        `json:"finished_at"`
}

type ImportResults struct {
	RequestedCount int    `json:"requested_count"`
	CreatedCount   int    `json:"created_count"`
	UpdatedCount   int    `json:"updated_count"`
	DeletedCount   int    `json:"deleted_count"`
	ErroredCount   int    `json:"errored_count"`
	ErrorsURL      string `json:"errors_url"`
}

// ImportErrorRow is one row of the document published at ImportResults.ErrorsURL, reduced to the
// only two things reconciliation needs.
//
// The model is deliberately loose and carries no JSON tags: that document's schema is genuinely
// undocumented - the provider's OpenAPI specification mentions errors_url twice, both times as a
// bare string URL with no media type and no schema - so the tolerant parser fills these fields in
// from whichever candidate key a row happens to use rather than binding to one guessed shape.
type ImportErrorRow struct {
	// Identifier is the contact identifier the row refers to, used to resolve the row back to the
	// job that produced the contact. It never leaves reconciliation: it is not logged and not
	// written into any persisted failure reason, because it is contact PII.
	Identifier string

	// Message is the provider's own description of the rejection. It is used ONLY to derive a
	// connector-owned error class; it is never logged and never persisted, because provider prose
	// can restate any contact field it likes.
	Message string
}

// APIErrorItem is one entry of the documented SendGrid error body,
// {"errors":[{"field":null,"message":"…"}]}.
//
// Field is a pointer because the documented 429 body sends field: null, which a plain string cannot
// represent. Message is decoded to model the documented body faithfully and to let the connector
// count the items an error response carried; it is deliberately never rendered into an error string,
// a log line or a persisted failure reason, because provider prose is untrusted third-party text
// that may echo contact data.
type APIErrorItem struct {
	Field   *string `json:"field"`
	Message string  `json:"message"`
}

// APIError reports a SendGrid response this connector cannot use.
//
// Everything it renders is connector-owned: the operation that failed, the HTTP status code, and how
// many error items the documented body carried. That is enough for an operator to act on and safe to
// place in a JobsDB failure reason, which is where these strings ultimately land.
type APIError struct {
	Operation  string
	StatusCode int
	// Items are the decoded entries of the documented error body, retained for their count and for
	// shape fidelity only.
	Items []APIErrorItem
}

func (e *APIError) Error() string {
	return fmt.Sprintf("sendgrid %s responded with status %d carrying %d error item(s)",
		e.Operation, e.StatusCode, len(e.Items))
}

// RateLimitError reports an HTTP 429 from any SendGrid call.
//
// It exists as a distinct type so that callers detect a rate limit with errors.As instead of
// re-sniffing status codes, which is what keeps rate-limited jobs on the retryable channel and out
// of the terminal one. Everything it renders is connector-owned and numeric, so it is safe to
// persist as a failure reason.
type RateLimitError struct {
	StatusCode int
	// RetryAfter is the Retry-After hint, normalized to a canonical duration such as "30s", or
	// empty. The header is not documented for the SendGrid v3 API - it may be injected by an edge or
	// a proxy - so it is optional, and a value that is neither delta-seconds nor an HTTP date is
	// discarded rather than carried through as free-form provider text.
	RetryAfter string
	// ResetEpoch is X-RateLimit-Reset, which the provider documents as a UNIX epoch-SECONDS
	// timestamp rather than a delta. Zero when the header is absent or unparseable.
	ResetEpoch int64
	// Limit is X-RateLimit-Limit, or -1 when the header is absent or unparseable.
	Limit int64
	// Remaining is X-RateLimit-Remaining, or -1 when the header is absent or unparseable.
	Remaining int64
}

func (e *RateLimitError) Error() string {
	details := make([]string, 0, 4)
	if e.RetryAfter != "" {
		details = append(details, "retryAfter="+e.RetryAfter)
	}
	if e.ResetEpoch > 0 {
		// Rendered as an absolute UTC instant: the header is epoch seconds, not a delta, and an
		// absolute timestamp stays meaningful however long the reason survives in JobsDB.
		details = append(details, "reset="+time.Unix(e.ResetEpoch, 0).UTC().Format(time.RFC3339))
	}
	if e.Limit >= 0 {
		details = append(details, fmt.Sprintf("limit=%d", e.Limit))
	}
	if e.Remaining >= 0 {
		details = append(details, fmt.Sprintf("remaining=%d", e.Remaining))
	}
	if len(details) == 0 {
		return fmt.Sprintf("sendgrid rate limited the request with status %d and published no rate limit window", e.StatusCode)
	}
	return fmt.Sprintf("sendgrid rate limited the request with status %d (%s)",
		e.StatusCode, strings.Join(details, ", "))
}

// SendGridAPIService abstracts the three provider calls used by the manager. Pointer-returning
// methods return either a non-nil response or a non-nil error.
type SendGridAPIService interface {
	// UploadContacts issues PUT /v3/marketing/contacts and treats only 202 Accepted as success.
	UploadContacts(request UpsertRequest) (*UpsertResponse, error)
	// GetImportStatus issues GET /v3/marketing/contacts/imports/{id} for one import job ID.
	GetImportStatus(jobID string) (*ImportStatusResponse, error)
	// GetImportErrors fetches the document an import published at results.errors_url and returns it
	// verbatim, because its schema is undocumented and the adapter must not encode a guess about it.
	GetImportErrors(errorsURL string) ([]byte, error)
}

// SendGridBulkUploader is this connector's implementation of the four-method async destination
// manager contract.
//
// Its fields are exported because the test suite lives in an external package and assembles the
// uploader directly to inject the generated API-service mock. It holds no per-upload or
// cross-invocation mutable state: reconciliation re-derives everything it needs from the importing
// jobs, so GetUploadStats stays correct when it runs in a different process or on a different pod
// from Upload.
type SendGridBulkUploader struct {
	Logger             logger.Logger
	StatsFactory       stats.Stats
	DestinationID      string
	DestinationConfig  DestinationConfig
	SendGridAPIService SendGridAPIService

	MaxContactsPerRequest int // Override for testing (0 = use default)
	MaxRequestBytes       int // Override for testing (0 = use default)
}
