package sendgridbulkupload

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

// Request caps for the SendGrid Marketing Contacts upsert endpoint.
//
// SendGrid caps one upsert at 30,000 contacts OR 6MB of data, WHICHEVER IS LOWER, so both
// caps have to be honored simultaneously - which is what makes the chunker in this file a
// dual-cap chunker rather than a simple batcher.
const (
	// sendGridMaxContactsPerRequest is the endpoint's documented ceiling on the number of
	// contacts one upsert may carry. It is corroborated in-repo by the read-only SendGrid
	// parity fixture, which records a batch limit of 30,000 contacts per API call.
	sendGridMaxContactsPerRequest = 30000

	// sendGridMaxRequestBytes is the endpoint's documented 6MB ceiling on one request.
	//
	// "6MB" is read DECIMALLY here (6,000,000 bytes) rather than as 6 mebibytes
	// (6,291,456 bytes) because the documentation does not say which it means and the
	// decimal reading is the stricter of the two: budgeting against the smaller number can
	// only ever produce a request SendGrid accepts, whereas guessing the larger number
	// would produce requests it rejects whenever the truth is the smaller one.
	sendGridMaxRequestBytes = 6 * 1000 * 1000

	// requestEnvelopeReserveBytes is the headroom held back from sendGridMaxRequestBytes and
	// never handed to the chunker.
	//
	// This reserve is REQUIRED FOR CORRECTNESS, and it is the one place where this connector
	// deliberately diverges from the Klaviyo chunker it is otherwise modeled on. That
	// chunker measures only the elements it is packing, whereas SendGrid's ceiling applies
	// to the WHOLE serialized request body: the {"list_ids":[...],"contacts":[...]} envelope
	// and every list ID inside it count against the same 6MB. A chunker budgeted at a flat
	// 6MB would therefore build bodies that are individually within the element budget yet
	// over the wire limit once the envelope is added, and SendGrid would reject the request.
	//
	// The reserve is sized generously rather than calculated, because the number of list IDs a
	// destination targets is operator-controlled and therefore not knowable here.
	requestEnvelopeReserveBytes = 250 * 1000

	// defaultMaxContactsPerRequest is the element cap used when nothing overrides it.
	defaultMaxContactsPerRequest = sendGridMaxContactsPerRequest

	// defaultMaxRequestBytes is the byte cap used when nothing overrides it: the documented
	// ceiling less the envelope reserve described above.
	defaultMaxRequestBytes = sendGridMaxRequestBytes - requestEnvelopeReserveBytes
)

// Configuration keys, resolved through the batch router's two-level configuration helpers so
// that every value can be tuned per destination type as
// BatchRouter.SENDGRID_BULK_UPLOAD.<key> and falls back to BatchRouter.<key>.
const (
	// configKeyMaxContactsPerRequest overrides the element cap. It exists for operational
	// tuning only; the default is already the API's documented ceiling.
	configKeyMaxContactsPerRequest = "maxContactsPerRequest"

	// configKeyMaxRequestBytes overrides the byte cap, envelope reserve included.
	configKeyMaxRequestBytes = "maxRequestBytes"

	// configKeyMaxBufferCapacity overrides the largest staging-file line that can be read.
	configKeyMaxBufferCapacity = "maxBufferCapacity"
)

// defaultMaxBufferCapacity is the largest single staging-file line this connector will read,
// matching the 512KB default the sibling connectors use.
//
// bufio.Scanner defaults to a 64KB token limit and reports a line longer than its buffer as
// an error rather than truncating it silently. That error is handled as a retryable upload
// failure, so an oversized line can never be dropped unnoticed - but a contact carrying a
// long trait list is legitimate, so the limit is raised well past the default and made
// configurable rather than left to trip on ordinary data.
const defaultMaxBufferCapacity = 512 * bytesize.KB

// Separators used to carry several values through the single-string fields the batch router
// hands back to Poll and GetUploadStats.
//
// One upload can legitimately produce SEVERAL SendGrid imports - the batch is chunked
// against both request caps, and each accepted chunk gets its own job_id - while
// common.ImportParameters carries one import identifier and common.PollStatusResponse
// carries one failure-document reference. Both are therefore joined, and split back apart on
// the way in.
const (
	// importIDSeparator joins the accepted chunks' job_ids into the single import identifier
	// the batch router persists. SendGrid job_ids are UUID-like and contain no colon, and
	// the same separator is the established precedent in this tree.
	importIDSeparator = ":"

	// errorsURLSeparator joins several errors-document URLs into
	// PollStatusResponse.FailedJobParameters.
	//
	// A newline is used because it cannot legally appear inside a URL, so the join is always
	// reversible. The field is passed straight from Poll to GetUploadStats in memory and is
	// never persisted, so nothing downstream is sensitive to the choice.
	errorsURLSeparator = "\n"
)

// The four documented SendGrid import states.
//
// This enumeration is exhaustive: there is no "processing" or "in_progress" value, and
// pending is the ONLY non-terminal state. Note in particular that SendGrid signals a PARTIAL
// failure with errored, not with completed - completed carries the promise that nothing
// errored - which is why the poll mapping treats errored as the reconciliation branch.
const (
	// importStatusPending means the import has not finished yet.
	importStatusPending = "pending"

	// importStatusCompleted means the import finished without any errors.
	importStatusCompleted = "completed"

	// importStatusErrored means the import finished with SOME errors, described by the
	// errors document.
	importStatusErrored = "errored"

	// importStatusFailed means the import finished with ALL errors, or was entirely
	// unprocessable.
	importStatusFailed = "failed"
)

// batchRouterModule is the module stats tag every measurement in this connector carries,
// matching the batch router's own module name.
const batchRouterModule = "batch_router"

// externalIDTypeListIDs is the context.externalId entry type that carries per-event SendGrid
// list IDs, which take precedence over the destination configuration.
const externalIDTypeListIDs = "listIds"

// Reasons recorded against jobs this connector rejects locally, before any request is made.
//
// Both conditions are PERMANENT: the batch router would rebuild an identical payload on a
// retry, so retrying could only ever fail again and would burn the retry budget for nothing.
// They are therefore the only outcomes in this connector that use the terminal abort channel
// - a rate limit never does.
const (
	// reasonMissingIdentifier is recorded when an event yields a contact carrying none of
	// the four identifiers SendGrid accepts.
	reasonMissingIdentifier = "sendgrid requires each contact to carry at least one of email, phone_number_id, external_id or anonymous_id"

	// reasonContactTooLarge is recorded when a single contact is larger than an entire
	// request's byte budget, so no chunk could ever hold it.
	reasonContactTooLarge = "the contact is larger than the maximum sendgrid request size and cannot be uploaded in any batch"

	// reasonMalformedRecord is recorded when a staging-file line is malformed yet still names
	// the job that produced it.
	reasonMalformedRecord = "the staging file record for this job is malformed and cannot be turned into a sendgrid contact"
)

// defaultFailureReason is recorded for an errored row whose document carried no message, so
// that a job is never marked failed with an empty explanation.
const defaultFailureReason = "sendgrid reported an error for this contact without a message"

// maxReasonRunes bounds every provider-supplied message this connector records against a job or
// writes to a log, so that a verbose or hostile response cannot bloat the jobs database.
const maxReasonRunes = 512

// Bounds applied to the errors document while it is parsed.
//
// The transport adapter already bounds how many BYTES are read; these bound what the parser is
// prepared to build out of them, so that a document within the byte budget still cannot turn
// into an unbounded number of rows or an unbounded nesting depth.
const (
	// maxErrorRows bounds how many rows are taken from the document. It sits far above the
	// 30,000 contacts one request can carry, so it can only be reached by a document that does
	// not describe a single import.
	maxErrorRows = 100000

	// maxErrorsDocumentDepth bounds the document's JSON nesting depth. Every shape the parser
	// accepts is at most three levels deep, so this leaves ample room while still rejecting a
	// document built to exhaust the decoder.
	maxErrorsDocumentDepth = 32
)

// Compile-time proof that this manager satisfies the full four-method async destination
// manager contract, so that any drift in the shared interface breaks the build here rather
// than surfacing as a runtime type error inside a batch router worker.
//
// common.SimpleAsyncDestinationManager is deliberately NOT used: its Poll unconditionally
// reports completion, whereas a SendGrid upsert only returns 202 Accepted and has to be
// polled before anything is known about its outcome.
var _ common.AsyncDestinationManager = (*SendGridBulkUploader)(nil)

// NewManager builds the SendGrid bulk-upload manager for one destination.
//
// The signature is fixed by the async destination manager factory, which calls it with
// exactly these three arguments and expects exactly two results. SendGrid needs neither
// application configuration nor the backend-config client, and it needs no OAuth subsystem
// either: the Marketing Contacts API authenticates with a single static bearer credential
// taken from the destination's own configuration.
//
// Construction FAILS rather than returning a half-configured manager. In particular a
// destination with no API key is rejected here, once and clearly, instead of being allowed
// to produce an opaque 401 on every batch for as long as it stays misconfigured.
func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*SendGridBulkUploader, error) {
	if destination == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	destinationConfig, err := parseDestinationConfig(destination)
	if err != nil {
		return nil, err
	}

	sendGridLogger := logger.Child("SendGridBulkUpload").Child("SendGridBulkUploader")

	// The API key guard lives in the API service constructor, which is the single place that
	// owns the bearer credential; its error is propagated verbatim so the reason a
	// destination could not be constructed is reported exactly once.
	apiService, err := NewSendGridAPIService(destination, sendGridLogger, statsFactory)
	if err != nil {
		return nil, err
	}

	return &SendGridBulkUploader{
		Logger:            sendGridLogger,
		StatsFactory:      statsFactory,
		DestinationID:     destination.ID,
		DestinationName:   destination.Name,
		DestinationConfig: destinationConfig,

		SendGridAPIService: apiService,

		// Both caps resolve BatchRouter.SENDGRID_BULK_UPLOAD.<key> and fall back to
		// BatchRouter.<key>, defaulting to the API's own documented limits. They are
		// overridable so that chunking boundaries can be exercised without materializing
		// tens of thousands of contacts, and so that an operator can tighten them without a
		// release if SendGrid ever narrows the endpoint.
		MaxContactsPerRequest: int(common.GetBatchRouterConfigInt64(configKeyMaxContactsPerRequest, destName, defaultMaxContactsPerRequest)),
		MaxRequestBytes:       int(common.GetBatchRouterConfigInt64(configKeyMaxRequestBytes, destName, defaultMaxRequestBytes)),
	}, nil
}

// parseDestinationConfig converts the control plane's untyped destination configuration map
// into this connector's typed view.
//
// The round trip through the repository's mandated JSON codec - marshal the untyped map,
// unmarshal it into the typed struct - is the convention every bulk-upload connector in this
// tree follows. It keeps the typed view honest, because the JSON tags on DestinationConfig
// stay the single description of what the control plane sends, and it costs one small
// allocation once per destination construction.
func parseDestinationConfig(destination *backendconfig.DestinationT) (DestinationConfig, error) {
	var destinationConfig DestinationConfig
	jsonConfig, err := jsonrs.Marshal(destination.Config)
	if err != nil {
		return destinationConfig, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	if err := jsonrs.Unmarshal(jsonConfig, &destinationConfig); err != nil {
		return destinationConfig, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}
	return destinationConfig, nil
}

// Transform reduces one RudderStack event to one staging-file line.
//
// It is deliberately EVENT-TYPE AGNOSTIC: both track and identify events are accepted and
// each becomes exactly one SendGrid contact, because the contact is derived from the event's
// traits and identifiers rather than from its type.
//
// The line's shape - {"message":{...},"metadata":{"job_id":N}} - is what tags each contact
// with the job that produced it, and it is produced by the shared helper rather than by hand
// so that the metadata key can never drift from the key Upload reads back.
//
// The receiver is unnamed because the method uses no state; that is both the convention in
// this tree and what keeps the unparam linter satisfied.
func (*SendGridBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

// maxContactsPerRequest is the effective element cap for one upsert.
//
// A zero or negative field falls back to the documented default, so an uploader built as a
// struct literal - which is how the tests construct it - is always usable and can shrink the
// cap simply by setting it. An override is CLAMPED to the endpoint's documented ceiling: a
// larger value could only produce requests SendGrid rejects, so honouring it would turn a
// configuration mistake into a delivery failure.
func (b *SendGridBulkUploader) maxContactsPerRequest() int {
	if b.MaxContactsPerRequest <= 0 {
		return defaultMaxContactsPerRequest
	}
	return min(b.MaxContactsPerRequest, sendGridMaxContactsPerRequest)
}

// maxRequestBytes is the effective byte cap for one upsert's contacts, envelope reserve
// already deducted. A zero or negative field falls back to the documented default, and an
// override is clamped to the documented ceiling less the reserve for the same reason the
// element cap is.
func (b *SendGridBulkUploader) maxRequestBytes() int {
	if b.MaxRequestBytes <= 0 {
		return defaultMaxRequestBytes
	}
	return min(b.MaxRequestBytes, defaultMaxRequestBytes)
}

// statLabels are the tags every measurement this connector emits carries.
//
// destType comes from the destName constant rather than from the destination definition or
// from an inline literal, so the tag can never drift from the registered
// destination-definition name even if an operator renames the destination.
func (b *SendGridBulkUploader) statLabels(destinationID string) stats.Tags {
	return stats.Tags{
		"module":   batchRouterModule,
		"destType": destName,
		"destID":   destinationID,
	}
}

// destinationIDOf resolves the destination ID an upload's outcome must be reported against.
//
// The batch router keys its own bookkeeping on this value, so it is taken from the upload
// request when present and falls back to the ID captured at construction. The fallback is
// what makes the manager safe against an async destination struct assembled without a
// destination attached.
func (b *SendGridBulkUploader) destinationIDOf(asyncDestStruct *common.AsyncDestinationStruct) string {
	if asyncDestStruct != nil && asyncDestStruct.Destination != nil && asyncDestStruct.Destination.ID != "" {
		return asyncDestStruct.Destination.ID
	}
	return b.DestinationID
}

// buildContact maps one staged event message onto one SendGrid contact.
//
// The mapping is the one this repository already documents for SendGrid, and every
// alternative spelling accepted here is one the mapping records: a trait may arrive
// lowerCamelCase or snake_case, and the address traits may arrive either nested under
// traits.address or flattened onto traits. Reading both spellings costs a map lookup and
// avoids silently dropping data that an upstream transformation happened to spell the other
// way.
//
// Traits are read from traits first and from context.traits second, because an identify
// event carries them in either place depending on which SDK and which transformation
// produced the event.
//
// The email is lower-cased locally even though SendGrid lower-cases it on ingestion: that is
// what makes the address usable as the reconciliation key, and lower-casing both sides means
// the match never depends on SendGrid's normalization having happened first.
//
// It returns an error - never a partially built contact - when the event carries none of the
// four identifiers SendGrid accepts, so that one unusable record is rejected on its own
// instead of poisoning the batch it happens to share a request with.
func (b *SendGridBulkUploader) buildContact(message gjson.Result) (Contact, error) {
	traits := firstResult(message, "traits", "context.traits")
	address := firstResult(traits, "address")

	contact := Contact{
		Email:         strings.ToLower(coalesce(firstString(traits, "email"), firstString(message, "email"))),
		PhoneNumberID: firstString(traits, "phone", "phone_number_id", "phoneNumber"),
		// userId maps onto external_id by default: that is this repository's documented
		// SendGrid convention, not an invention of this connector.
		ExternalID:  firstString(message, "userId", "user_id"),
		AnonymousID: firstString(message, "anonymousId", "anonymous_id"),
		FirstName:   firstString(traits, "firstName", "first_name"),
		LastName:    firstString(traits, "lastName", "last_name"),
		AddressLine1: coalesce(
			firstString(address, "street", "line1"),
			firstString(traits, "address_line_1", "addressLine1"),
		),
		AddressLine2: coalesce(
			firstString(address, "street2", "line2"),
			firstString(traits, "address_line_2", "addressLine2"),
		),
		City: coalesce(firstString(address, "city"), firstString(traits, "city")),
		StateProvinceRegion: coalesce(
			firstString(address, "state", "region"),
			firstString(traits, "state", "state_province_region"),
		),
		PostalCode: coalesce(
			firstString(address, "postalCode", "postal_code", "zip"),
			firstString(traits, "postalCode", "postal_code"),
		),
		Country:         coalesce(firstString(address, "country"), firstString(traits, "country")),
		AlternateEmails: stringSliceOf(firstResult(traits, "alternateEmails", "alternate_emails")),
		CustomFields:    b.buildCustomFields(message, traits),
	}

	if contact.Email == "" && contact.PhoneNumberID == "" && contact.ExternalID == "" && contact.AnonymousID == "" {
		return Contact{}, errors.New(reasonMissingIdentifier)
	}
	return contact, nil
}

// buildCustomFields resolves the operator-supplied trait-to-custom-field mapping against one
// event.
//
// SendGrid requires a custom field to exist before a value can be written to it and
// addresses it by an opaque ID such as "w1", so the mapping has to be explicit: a trait with
// no entry in it is not sent as a custom field at all, because inventing a field name would
// produce nothing but a rejected request.
//
// A mapped trait is looked up by its exact key first - which is what makes a trait name
// containing a dot work, since a path lookup would read such a name as a nested path - and
// only then as a path, which is what makes a mapping such as "address.city" work. A trait
// that is absent or explicitly null is skipped rather than sent as an empty value, because
// SendGrid leaves an omitted field untouched but OVERWRITES one sent empty.
//
// It returns nil rather than an empty map when nothing maps, so that the custom_fields key is
// omitted from the request body entirely.
func (b *SendGridBulkUploader) buildCustomFields(message, traits gjson.Result) map[string]any {
	if len(b.DestinationConfig.CustomFieldsMapping) == 0 {
		return nil
	}
	customFields := make(map[string]any, len(b.DestinationConfig.CustomFieldsMapping))
	for traitName, fieldID := range b.DestinationConfig.CustomFieldsMapping {
		if traitName == "" || fieldID == "" {
			continue
		}
		value := lookupTrait(message, traits, traitName)
		if !value.Exists() || value.Type == gjson.Null {
			continue
		}
		customFields[fieldID] = value.Value()
	}
	if len(customFields) == 0 {
		return nil
	}
	return customFields
}

// resolveListIDs decides which SendGrid lists one event's contact is added to.
//
// The precedence this repository documents is honored: a per-event context.externalId entry
// of type "listIds" wins, so that a single destination can target different lists per event,
// and the destination configuration is the fallback. The dashboard's own list setting and the
// destination configuration are the same thing on the wire - both reach this connector as the
// destination configuration's list IDs - so they collapse into that single fallback.
//
// An entry's id is accepted both as an array of list IDs and as a single string, because both
// shapes occur in the wild, and the entry type is matched case-insensitively.
func (b *SendGridBulkUploader) resolveListIDs(message gjson.Result) []string {
	if externalIDs := message.Get("context.externalId"); externalIDs.IsArray() {
		listIDs := make([]string, 0, len(externalIDs.Array()))
		for _, entry := range externalIDs.Array() {
			if !strings.EqualFold(strings.TrimSpace(entry.Get("type").String()), externalIDTypeListIDs) {
				continue
			}
			id := entry.Get("id")
			if id.IsArray() {
				listIDs = append(listIDs, stringSliceOf(id)...)
				continue
			}
			if value := strings.TrimSpace(id.String()); value != "" {
				listIDs = append(listIDs, value)
			}
		}
		if normalized := normalizeListIDs(listIDs); len(normalized) > 0 {
			return normalized
		}
	}
	return normalizeListIDs(b.DestinationConfig.ListIDs)
}

// contactBatch is a set of contacts that share the same target list IDs and can therefore
// travel in the same request body.
//
// Grouping is not an optimization, it is a correctness requirement: one upsert carries a
// single list_ids array that applies to every contact in it, so two contacts resolving to
// different lists cannot share a request no matter how small they are.
type contactBatch struct {
	// listIDs are the list IDs this batch's request body carries.
	listIDs []string

	// contacts and jobIDs are index-aligned: contacts[i] was produced by jobIDs[i]. The
	// alignment is what lets a single rejected chunk's jobs be identified exactly.
	contacts []Contact
	jobIDs   []int64
}

// stagedContacts is the outcome of reading one staging file.
type stagedContacts struct {
	// batches holds the readable contacts, grouped by their target list IDs in
	// first-appearance order so that the requests an upload issues are deterministic.
	batches []*contactBatch

	// rejectedJobIDs are jobs whose event could not yield a usable contact. The condition is
	// permanent, so these are reported on the terminal channel.
	rejectedJobIDs []int64

	// malformedJobIDs are jobs whose staging line was malformed but still carried a usable job
	// ID. The same bytes would fail identically on every retry, so these are reported on the
	// terminal channel, exactly like a contact that carries no identifier.
	malformedJobIDs []int64
}

// parseStagingLine validates one staging-file line and returns the originating job ID together
// with the message object the contact is built from.
//
// Validation happens BEFORE either field is consumed. gjson is a deliberately tolerant reader:
// asked for metadata.job_id on a line that is not JSON, or that carries no metadata at all, it
// answers with a zero value that is indistinguishable from legitimately absent data - most
// damagingly job ID 0, which belongs to no job and must never reach the batch router.
//
// The returned job ID carries the recoverability of the failure, which is what lets the caller
// choose the right outcome without a second error type: 0 alongside an error means the line
// could not be attributed to any job, so the batch must be retried rather than an attribution
// invented; a non-zero job ID alongside an error means the line was malformed but attributable,
// so only that one record need be rejected.
func parseStagingLine(line []byte) (int64, gjson.Result, error) {
	if !gjson.ValidBytes(line) {
		return 0, gjson.Result{}, errors.New("the staging file line is not valid JSON")
	}
	record := gjson.ParseBytes(line)
	if !record.IsObject() {
		return 0, gjson.Result{}, errors.New("the staging file line is not a JSON object")
	}

	// job_id, not jobId: that is the key the shared marshalling helper actually writes.
	jobIDResult := record.Get("metadata.job_id")
	if jobIDResult.Type != gjson.Number && jobIDResult.Type != gjson.String {
		// A numeric string is accepted alongside a number purely defensively: it is still an
		// attributable job ID, and recovering the attribution always beats failing the batch.
		return 0, gjson.Result{}, fmt.Errorf("the staging file line carries no numeric metadata.job_id (found %s)", jobIDResult.Type)
	}
	jobID := jobIDResult.Int()
	if jobID <= 0 {
		// Zero is unattributable, and so is a negative value: job IDs are positive, so a
		// negative one would name a job that cannot exist.
		return 0, gjson.Result{}, errors.New("the staging file line carries an unusable metadata.job_id")
	}

	message := record.Get("message")
	if !message.IsObject() {
		// The job ID is known, so this record - and only this record - is rejected.
		return jobID, gjson.Result{}, errors.New("the staging file line carries no message object")
	}
	return jobID, message, nil
}

// readStagedContacts reads a staging file into list-ID-grouped batches of contacts.
//
// Each line is one event, written by Transform, shaped {"message":{...},"metadata":{...}}.
// The job ID is read from metadata.job_id - the key the shared marshalling helper actually
// writes - and the contact is derived from message.
//
// Every line is VALIDATED BEFORE either field is consumed, because gjson is a deliberately
// tolerant reader: on a line that is not JSON at all, or that carries no metadata, it yields
// zero values indistinguishable from legitimately absent data. A line whose event yields no
// usable contact is skipped INDIVIDUALLY, because one unusable record must never prevent the
// rest of the batch from being delivered.
//
// A line that cannot be attributed to ANY job is different in kind: nothing can be reported
// against it, and continuing would leave the job that produced it silently unaccounted for. It
// therefore aborts the whole read and is returned as an error, which the caller reports as a
// retryable batch failure - the same treatment a read or scan failure gets, because a partially
// read file would likewise drop every line after the failure.
func (b *SendGridBulkUploader) readStagedContacts(filePath string, statLabels stats.Tags) (*stagedContacts, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening staging file: %w", err)
	}
	defer func() { _ = file.Close() }()

	staged := &stagedContacts{batches: make([]*contactBatch, 0, 1)}
	// Indexes into staged.batches by the canonical form of a batch's list IDs, so that
	// grouping stays O(1) per line while the batches themselves keep first-appearance order.
	batchIndex := make(map[string]int)
	contactSizeStat := b.StatsFactory.NewTaggedStat("contact_size", stats.HistogramType, statLabels)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(common.GetBatchRouterConfigInt64(configKeyMaxBufferCapacity, destName, defaultMaxBufferCapacity)))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			// A blank line carries no record at all - a trailing newline is entirely normal -
			// so it is skipped rather than charged against any job.
			continue
		}

		jobID, message, err := parseStagingLine(line)
		if err != nil {
			if jobID == 0 {
				// The line belongs to no job this connector can name. Reporting it against job
				// ID 0 would name a job that does not exist while leaving the real one
				// unaccounted for, so the whole read fails and the caller retries the batch.
				b.Logger.Errorn("[sendgrid bulk upload] staging file line could not be attributed to a job",
					obskit.Error(err))
				return nil, fmt.Errorf("reading staging file: %w", err)
			}
			// Malformed but attributable: the same bytes would fail identically on every
			// retry, so this single record is rejected permanently and the batch proceeds.
			staged.malformedJobIDs = append(staged.malformedJobIDs, jobID)
			b.Logger.Errorn("[sendgrid bulk upload] skipping malformed staging file line",
				logger.NewIntField("jobID", jobID),
				obskit.Error(err))
			continue
		}

		contact, err := b.buildContact(message)
		if err != nil {
			staged.rejectedJobIDs = append(staged.rejectedJobIDs, jobID)
			b.Logger.Warnn("[sendgrid bulk upload] skipping event that cannot be upserted",
				logger.NewIntField("jobID", jobID),
				obskit.Error(err))
			continue
		}
		if contactJSON, err := jsonrs.Marshal(contact); err == nil {
			contactSizeStat.Observe(float64(len(contactJSON)))
		}

		listIDs := b.resolveListIDs(message)
		key := listIDsKey(listIDs)
		index, ok := batchIndex[key]
		if !ok {
			staged.batches = append(staged.batches, &contactBatch{
				listIDs:  listIDs,
				contacts: make([]Contact, 0, 1),
				jobIDs:   make([]int64, 0, 1),
			})
			index = len(staged.batches) - 1
			batchIndex[key] = index
		}
		staged.batches[index].contacts = append(staged.batches[index].contacts, contact)
		staged.batches[index].jobIDs = append(staged.batches[index].jobIDs, jobID)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading staging file: %w", err)
	}
	return staged, nil
}

// chunkBySizeAndElements splits one batch of contacts against BOTH request caps at once.
//
// It measures every contact with the same codec that serializes the request body and adds one
// byte for the comma that will separate it from its neighbor, then flushes the current chunk
// as soon as adding the next contact would reach the byte cap or the chunk already holds the
// maximum number of elements. The flush is guarded so that an empty chunk is never emitted.
//
// The returned contact and job-ID chunk slices are INDEX-ALIGNED - they are flushed together
// and reset together - which is what allows exactly one rejected chunk's jobs to be reported
// while every accepted chunk keeps its importing state.
//
// A contact that is on its own larger than a whole request's budget can never fit in any
// chunk. Rather than being dropped, or being allowed to wedge the chunker by forming a chunk
// that is over the cap, its job ID is returned separately so the caller can report that one
// job and deliver the rest.
//
// The caps are supplied by the caller's accessors, which guarantee positive values.
func chunkBySizeAndElements(contacts []Contact, jobIDs []int64, maxBytes, maxElements int) ([][]Contact, [][]int64, []int64, error) {
	// Every contact is paired with the job that produced it, so the two inputs are read in
	// lockstep below. The pairing is reported as an error rather than being assumed, because
	// indexing one slice with the other's offset would panic inside a batch router worker and
	// take down far more than the one upload that was actually malformed.
	if len(contacts) != len(jobIDs) {
		return nil, nil, nil, fmt.Errorf("%d contacts cannot be paired with %d job ids", len(contacts), len(jobIDs))
	}

	var (
		contactChunks   [][]Contact
		jobIDChunks     [][]int64
		oversizedJobIDs []int64
	)
	contactChunk := make([]Contact, 0)
	jobIDChunk := make([]int64, 0)
	chunkSize := 0

	for idx, contact := range contacts {
		contactJSON, err := jsonrs.Marshal(contact)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to marshal contact: %w", err)
		}

		contactSize := len(contactJSON) + 1 // +1 for comma character

		if contactSize >= maxBytes {
			oversizedJobIDs = append(oversizedJobIDs, jobIDs[idx])
			continue
		}

		if (chunkSize+contactSize >= maxBytes || len(contactChunk) == maxElements) && len(contactChunk) > 0 {
			contactChunks = append(contactChunks, contactChunk)
			jobIDChunks = append(jobIDChunks, jobIDChunk)
			contactChunk = make([]Contact, 0)
			jobIDChunk = make([]int64, 0)
			chunkSize = 0
		}

		contactChunk = append(contactChunk, contact)
		jobIDChunk = append(jobIDChunk, jobIDs[idx])
		chunkSize += contactSize
	}

	if len(contactChunk) > 0 {
		contactChunks = append(contactChunks, contactChunk)
		jobIDChunks = append(jobIDChunks, jobIDChunk)
	}

	return contactChunks, jobIDChunks, oversizedJobIDs, nil
}

// Upload upserts one staging file's contacts into SendGrid.
//
// The staging file is read, the contacts are grouped by target list and chunked against both
// request caps, and one PUT is issued per chunk. Every accepted chunk contributes its job_id
// to the import identifier the batch router persists and its job IDs to the importing set;
// every rejected chunk contributes its job IDs to the retryable or the terminal set,
// according to why it was rejected.
//
// The three outcome sets are kept DISJOINT, so a job is only ever reported once.
//
// A rate limit is never terminal. When SendGrid answers 429 the affected jobs are returned as
// retryable failures with the advertised reset window in the reason, and never as aborts: the
// batch router owns retry, backoff and the decision to give up, and it escalates to an abort
// on its own once its retry budget is exhausted. Because the router only keeps an upload in
// the importing state when both an import identifier and importing job IDs come back, a
// rate-limited upload that accepted no chunk at all deliberately returns neither, which is
// exactly what releases those jobs to be re-queued.
func (b *SendGridBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destinationID := b.destinationIDOf(asyncDestStruct)
	statLabels := b.statLabels(destinationID)
	if asyncDestStruct == nil {
		// There is no batch, and therefore no job, to report anything against. Returning an
		// empty output is the only truthful answer; panicking would take down a worker that is
		// shared with every other destination on this partition.
		b.Logger.Errorn("[sendgrid bulk upload] upload called without a batch",
			obskit.DestinationID(destinationID))
		return common.AsyncUploadOutput{DestinationID: destinationID}
	}
	// Copied rather than aliased: jobs that already failed before this upload have to be
	// reported again, and appending to the caller's slice would mutate the caller's state.
	failedJobIDs := append([]int64{}, asyncDestStruct.FailedJobIDs...)

	staged, err := b.readStagedContacts(asyncDestStruct.FileName, statLabels)
	if err != nil {
		// The file could not be read in full, so nothing can be said about any individual
		// job. Every job is reported as retryable so the batch router can rebuild the file
		// and try again rather than losing the batch.
		b.Logger.Errorn("[sendgrid bulk upload] unable to read the staging file",
			obskit.Error(err),
			obskit.DestinationID(destinationID),
			logger.NewStringField("fileName", asyncDestStruct.FileName))
		allJobIDs := lo.Union(failedJobIDs, asyncDestStruct.ImportingJobIDs)
		b.StatsFactory.NewTaggedStat("failed_job_count", stats.CountType, statLabels).Count(len(allJobIDs))
		return common.AsyncUploadOutput{
			FailedJobIDs:  allJobIDs,
			FailedReason:  fmt.Sprintf("BRT: Error in reading staging file: %v", err),
			FailedCount:   len(allJobIDs),
			DestinationID: destinationID,
		}
	}

	var (
		importIDs      []string
		importingJobs  []int64
		abortedJobIDs  []int64
		failureReasons []string
		abortReasons   []string
	)
	if len(staged.rejectedJobIDs) > 0 {
		abortedJobIDs = append(abortedJobIDs, staged.rejectedJobIDs...)
		abortReasons = append(abortReasons, reasonMissingIdentifier)
	}
	if len(staged.malformedJobIDs) > 0 {
		abortedJobIDs = append(abortedJobIDs, staged.malformedJobIDs...)
		abortReasons = append(abortReasons, reasonMalformedRecord)
		// Surfaced as a metric as well, so that a staging-file shape change is visible without
		// having to read logs.
		b.StatsFactory.NewTaggedStat("invalid_record_count", stats.CountType, statLabels).Count(len(staged.malformedJobIDs))
	}

	for _, batch := range staged.batches {
		contactChunks, jobIDChunks, oversizedJobIDs, err := chunkBySizeAndElements(
			batch.contacts, batch.jobIDs, b.maxRequestBytes(), b.maxContactsPerRequest(),
		)
		if err != nil {
			// Serializing a contact failed, which says nothing about SendGrid's availability,
			// so the batch is reported as retryable rather than discarded.
			b.Logger.Errorn("[sendgrid bulk upload] unable to chunk contacts",
				obskit.Error(err),
				obskit.DestinationID(destinationID))
			failedJobIDs = append(failedJobIDs, batch.jobIDs...)
			failureReasons = append(failureReasons, fmt.Sprintf("BRT: Error in preparing contacts: %v", err))
			continue
		}
		if len(oversizedJobIDs) > 0 {
			abortedJobIDs = append(abortedJobIDs, oversizedJobIDs...)
			abortReasons = append(abortReasons, reasonContactTooLarge)
			b.Logger.Errorn("[sendgrid bulk upload] contacts too large to upload in any batch",
				obskit.DestinationID(destinationID),
				logger.NewIntField("contactCount", int64(len(oversizedJobIDs))))
		}

		for idx, contactChunk := range contactChunks {
			uploadResp, err := b.SendGridAPIService.UploadContacts(UpsertRequest{
				ListIDs:  batch.listIDs,
				Contacts: contactChunk,
			})
			if err != nil {
				terminal, reason := b.classifyUploadError(err, destinationID, len(jobIDChunks[idx]))
				if terminal {
					abortedJobIDs = append(abortedJobIDs, jobIDChunks[idx]...)
					abortReasons = append(abortReasons, reason)
				} else {
					failedJobIDs = append(failedJobIDs, jobIDChunks[idx]...)
					failureReasons = append(failureReasons, reason)
				}
				continue
			}
			if uploadResp == nil || strings.TrimSpace(uploadResp.JobID) == "" {
				// An accepted upsert that yields no job_id cannot be polled, so recording it as
				// importing would strand these jobs in the importing state forever. They are
				// reported as retryable instead: the endpoint upserts, so a repeated attempt is
				// harmless.
				b.Logger.Errorn("[sendgrid bulk upload] upload accepted without an import job id",
					obskit.DestinationID(destinationID),
					logger.NewIntField("contactCount", int64(len(jobIDChunks[idx]))))
				failedJobIDs = append(failedJobIDs, jobIDChunks[idx]...)
				failureReasons = append(failureReasons, "BRT: SendGrid accepted the upload without returning an import job id")
				continue
			}
			importIDs = append(importIDs, uploadResp.JobID)
			importingJobs = append(importingJobs, jobIDChunks[idx]...)
		}
	}

	output := common.AsyncUploadOutput{DestinationID: destinationID}
	importingJobs = lo.Uniq(importingJobs)
	if len(importingJobs) > 0 {
		// The count is computed BEFORE the import parameters are marshalled, so that what is
		// persisted is the number of jobs actually accepted rather than a value derived later.
		importCount := len(importingJobs)
		importParameters, err := jsonrs.Marshal(common.ImportParameters{
			ImportId:    strings.Join(importIDs, importIDSeparator),
			ImportCount: importCount,
		})
		if err != nil {
			// Without persisted import parameters the batch router could never poll this
			// import, and the jobs would sit in the importing state forever. Reporting them as
			// retryable trades a duplicate upsert - which is harmless, because the endpoint
			// upserts - for jobs that can never be resolved.
			b.Logger.Errorn("[sendgrid bulk upload] unable to marshal import parameters",
				obskit.Error(err),
				obskit.DestinationID(destinationID))
			failedJobIDs = append(failedJobIDs, importingJobs...)
			failureReasons = append(failureReasons, fmt.Sprintf("BRT: Error in marshalling import parameters: %v", err))
			// The output's importing fields are deliberately left unset, which is what makes
			// the batch router release these jobs instead of waiting on an import it could
			// never poll.
		} else {
			output.ImportingJobIDs = importingJobs
			output.ImportingParameters = importParameters
			output.ImportingCount = importCount
		}
	}

	// Every job belongs to exactly one chunk, so the three sets are disjoint by construction;
	// the set arithmetic makes that an invariant rather than an assumption, and guarantees it
	// still holds after the import-parameter fallback above has moved jobs between sets.
	failedJobIDs, _ = lo.Difference(lo.Uniq(failedJobIDs), output.ImportingJobIDs)
	abortedJobIDs, _ = lo.Difference(lo.Uniq(abortedJobIDs), lo.Union(output.ImportingJobIDs, failedJobIDs))
	// An outcome nothing landed in is reported as absent rather than as a present-but-empty
	// slice, so that "no job was rate limited" and "no job was aborted" read identically to a
	// caller however it inspects the result.
	if len(failedJobIDs) == 0 {
		failedJobIDs = nil
	}
	if len(abortedJobIDs) == 0 {
		abortedJobIDs = nil
	}

	output.FailedJobIDs = failedJobIDs
	output.FailedCount = len(failedJobIDs)
	output.FailedReason = joinReasons(failureReasons)
	output.AbortJobIDs = abortedJobIDs
	output.AbortCount = len(abortedJobIDs)
	output.AbortReason = joinReasons(abortReasons)

	b.StatsFactory.NewTaggedStat("success_job_count", stats.CountType, statLabels).Count(len(output.ImportingJobIDs))
	if len(failedJobIDs) > 0 {
		b.StatsFactory.NewTaggedStat("failed_job_count", stats.CountType, statLabels).Count(len(failedJobIDs))
	}
	b.Logger.Infon("[sendgrid bulk upload] upload finished",
		obskit.DestinationID(destinationID),
		logger.NewIntField("importCount", int64(len(output.ImportingJobIDs))),
		logger.NewIntField("failedCount", int64(len(failedJobIDs))),
		logger.NewIntField("abortedCount", int64(len(abortedJobIDs))),
		logger.NewIntField("requestCount", int64(len(importIDs))))

	return output
}

// classifyUploadError decides whether one rejected chunk's jobs are retryable or terminal, and
// renders the reason recorded against them.
//
// A rate limit is ALWAYS retryable - that is the whole point of the API service returning a
// dedicated type for it - and is deliberately never reported as terminal, no matter how
// exhausted the window is.
//
// A 400 Bad Request or a 413 Payload Too Large is terminal, because the batch router would
// rebuild a byte-identical body on a retry: SendGrid has already judged that body
// unacceptable, so retrying it could only fail again while consuming the retry budget. Every
// other status - an authorization failure an operator can fix, a 5xx, a transport error, a
// timeout - is retryable, and the batch router decides when to give up.
func (b *SendGridBulkUploader) classifyUploadError(err error, destinationID string, chunkJobCount int) (bool, string) {
	var rateLimitErr *RateLimitError
	if errors.As(err, &rateLimitErr) {
		// The reset window, the limit and the remaining quota are all rendered by the error
		// itself, so the reason an operator reads is the same text the adapter logged.
		reason := fmt.Sprintf("BRT: SendGrid rate limited the upload, jobs will be retried: %s", rateLimitErr.Error())
		b.Logger.Warnn("[sendgrid bulk upload] rate limited while uploading contacts",
			obskit.Error(rateLimitErr),
			obskit.DestinationID(destinationID),
			logger.NewStringField("retryAfter", rateLimitErr.RetryAfter),
			logger.NewIntField("rateLimitResetAt", rateLimitErr.ResetAt),
			logger.NewIntField("rateLimitLimit", int64(rateLimitErr.Limit)),
			logger.NewIntField("rateLimitRemaining", int64(rateLimitErr.Remaining)),
			logger.NewIntField("chunkJobCount", int64(chunkJobCount)))
		return false, reason
	}

	b.Logger.Errorn("[sendgrid bulk upload] unable to upload contacts",
		obskit.Error(err),
		obskit.DestinationID(destinationID),
		logger.NewIntField("chunkJobCount", int64(chunkJobCount)))

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			return true, fmt.Sprintf("BRT: Error in Uploading contacts (Aborted): %v", apiErr.Error())
		}
	}
	return false, fmt.Sprintf("BRT: Error in Uploading contacts: %v", err)
}

// Poll reads the state of the import an upload produced and maps it onto the batch router's
// poll contract.
//
// It is a single status read per import - the batch router owns the polling cadence, so this
// method never waits, never sleeps and never loops until an import settles.
//
// The mapping is:
//
//	pending                             -> {200, InProgress}          keep polling, no state change
//	completed, errored_count == 0        -> {200, Complete}            every job succeeded
//	completed, errored_count >  0        -> {200, Complete, HasFailed}  reconcile row by row
//	errored                              -> {200, Complete, HasFailed}  reconcile row by row
//	failed                               -> {400, Complete, HasFailed}  every job aborted, terminally
//	unrecognized status                  -> {500}                       every job retried
//	transport or API error               -> {500}                       every job retried
//	rate limited while polling           -> {429}                       every job retried
//
// Two rows deserve their reasons stated. The completed-with-errors row exists because
// SendGrid signals a partial failure with errored, so completed is expected to carry no
// errors - but the batch router marks EVERY importing job succeeded wholesale unless
// HasFailed is set, so a completed import that nonetheless reports errored rows must take the
// reconciliation branch too. Reading errored_count wrongly here would report failed contacts
// as delivered, which is the single worst outcome this connector can produce.
//
// And failed maps to 400 rather than to the generic reconciliation branch because SendGrid
// defines it as finished with all errors or entirely unprocessable: a permanent condition for
// which 400 is the framework's designated terminal path, and for which retrying would only
// burn the retry budget. Its errors document URL is surfaced in the reason for diagnosis
// rather than routed through reconciliation, which would classify every row as retryable.
//
// HasWarning and WarningJobParameters are never set: SendGrid has no warning tier.
func (b *SendGridBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	importIDs := splitImportIDs(pollInput.ImportId)
	if len(importIDs) == 0 {
		// Retryable rather than terminal: the batch router escalates to an abort by itself
		// once its retry budget is exhausted, so this cannot strand the jobs, and treating a
		// missing identifier as terminal here would abort jobs SendGrid may well have accepted.
		b.Logger.Errorn("[sendgrid bulk upload] poll requested without an import id",
			obskit.DestinationID(b.DestinationID))
		return common.PollStatusResponse{
			StatusCode: http.StatusInternalServerError,
			Complete:   false,
			Error:      "no sendgrid import id was persisted for this upload",
		}
	}

	var (
		errorsURLs   []string
		details      []string
		anyPending   bool
		failedTotal  int
		erroredTotal int
	)
	for _, importID := range importIDs {
		importStatus, err := b.SendGridAPIService.GetImportStatus(importID)
		if err != nil {
			return b.pollErrorResponse(err, importID)
		}
		if importStatus == nil {
			// Nothing is known about this import, which is exactly the situation the retryable
			// branch exists for: the next poll may well answer. Guarding here keeps a shared
			// batch router worker alive instead of dereferencing a nil response.
			return b.pollErrorResponse(fmt.Errorf("sendgrid returned no status for import %s", importID), importID)
		}
		if b.Logger.IsDebugLevel() { // rendering the whole response is not a cheap operation
			b.Logger.Debugn("[sendgrid bulk upload] import status",
				logger.NewStringField("importId", importID),
				logger.NewStringField("importStatus", fmt.Sprintf("%+v", *importStatus))) // nolint:forbidigo
		}

		switch strings.ToLower(strings.TrimSpace(importStatus.Status)) {
		case importStatusPending:
			anyPending = true
		case importStatusCompleted:
			if importStatus.Results.ErroredCount > 0 {
				erroredTotal++
				errorsURLs = appendErrorsURL(errorsURLs, importStatus)
				details = append(details, describeImport(importID, importStatus))
			}
		case importStatusErrored:
			erroredTotal++
			errorsURLs = appendErrorsURL(errorsURLs, importStatus)
			details = append(details, describeImport(importID, importStatus))
		case importStatusFailed:
			failedTotal++
			errorsURLs = appendErrorsURL(errorsURLs, importStatus)
			details = append(details, describeImport(importID, importStatus))
		default:
			// An unrecognized state is retryable, not terminal: SendGrid may have introduced
			// a value this connector has not been taught, and guessing at its meaning could
			// mark live contacts delivered or abort them.
			b.Logger.Errorn("[sendgrid bulk upload] unrecognized import status",
				logger.NewStringField("importId", importID),
				logger.NewStringField("importStatus", importStatus.Status))
			return common.PollStatusResponse{
				StatusCode: http.StatusInternalServerError,
				Complete:   false,
				Error:      sanitizeReason(fmt.Sprintf("Unknown status: %s", importStatus.Status)),
			}
		}
	}

	if anyPending {
		// Not complete and not failed: the batch router simply polls again, leaving every job
		// exactly as it is.
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			InProgress: true,
		}
	}

	if failedTotal > 0 && failedTotal == len(importIDs) {
		// Every import was rejected outright, so the whole batch is terminally lost and the
		// framework's abort path is the honest outcome. When only SOME imports failed the
		// mapping deliberately falls through to reconciliation instead, because aborting here
		// would also abort the jobs of the imports that succeeded.
		return common.PollStatusResponse{
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      sanitizeReason(fmt.Sprintf("SendGrid Bulk Upload Failed: %s", strings.Join(details, "; "))),
		}
	}

	if erroredTotal > 0 || failedTotal > 0 {
		return common.PollStatusResponse{
			StatusCode: http.StatusOK,
			Complete:   true,
			HasFailed:  true,
			// Forwarded verbatim, because SendGrid supplies the URL itself and reconstructing
			// one would be a guess. GetUploadStats falls back to re-reading the import status
			// when this is empty.
			FailedJobParameters: strings.Join(errorsURLs, errorsURLSeparator),
			Error:               sanitizeReason(fmt.Sprintf("SendGrid Bulk Upload partially failed: %s", strings.Join(details, "; "))),
		}
	}

	return common.PollStatusResponse{
		StatusCode: http.StatusOK,
		Complete:   true,
	}
}

// pollErrorResponse renders a failure to read an import's status.
//
// A rate limit is reported with its own status code so the reset window reaches the operator,
// and everything else is reported as a server-side failure. Both are retryable: the batch
// router retries every non-200, non-400 poll response, which is exactly the behavior a
// transient status-endpoint failure calls for.
func (b *SendGridBulkUploader) pollErrorResponse(err error, importID string) common.PollStatusResponse {
	var rateLimitErr *RateLimitError
	if errors.As(err, &rateLimitErr) {
		b.Logger.Warnn("[sendgrid bulk upload] rate limited while polling import status",
			obskit.Error(rateLimitErr),
			obskit.DestinationID(b.DestinationID),
			logger.NewStringField("importId", importID))
		return common.PollStatusResponse{
			StatusCode: http.StatusTooManyRequests,
			Complete:   false,
			Error:      sanitizeReason(rateLimitErr.Error()),
		}
	}
	b.Logger.Errorn("[sendgrid bulk upload] unable to read import status",
		logger.NewStringField("error", sanitizeReason(err.Error())),
		obskit.DestinationID(b.DestinationID),
		logger.NewStringField("importId", importID))
	return common.PollStatusResponse{
		StatusCode: http.StatusInternalServerError,
		Complete:   false,
		Error:      sanitizeReason(err.Error()),
	}
}

// GetUploadStats reconciles a partially errored import row by row, so that the contacts
// SendGrid could not process are retried while the rest are marked delivered. One import can
// therefore yield both failed and succeeded jobs, which is the whole reason this method exists.
//
// The errored rows go to the FAILED channel, not the aborted one. A per-row error - a rejected
// address, a value SendGrid would not take - is recoverable, and the failed channel is the
// batch router's retryable one, whereas the aborted channel is terminal.
//
// The reconciliation is STATELESS: it re-derives every contact identifier from the importing
// jobs it is handed rather than from anything cached during the upload. That is deliberate and
// load-bearing, because this method may well run in a different process invocation, or on a
// different pod, from the Upload that produced the import - any cached lookup would simply be
// missing after a restart, and a connector that depended on one would then report failed
// contacts as delivered.
//
// Every path that cannot establish which rows failed returns a non-200 status so the batch
// router retries, because returning 200 with an empty failed set would mark every job in the
// import succeeded - the exact silent data loss this method exists to prevent. That includes an
// errored row whose identifier resolves to no importing job: counting and logging such a row
// makes a change in the undocumented document shape observable, but it cannot attribute the row,
// so only the non-200 keeps the affected contact from being delivered by exclusion.
func (b *SendGridBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	errorsURLs, response := b.resolveErrorsURLs(input)
	if len(errorsURLs) == 0 {
		return response
	}

	rows := make([]ImportErrorRow, 0)
	for _, errorsURL := range errorsURLs {
		document, err := b.SendGridAPIService.GetImportErrors(errorsURL)
		if err != nil {
			b.Logger.Errorn("[sendgrid bulk upload] unable to fetch the errors document",
				logger.NewStringField("error", sanitizeReason(err.Error())),
				obskit.DestinationID(b.DestinationID))
			return common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to fetch the sendgrid errors document: " + err.Error()),
			}
		}
		parsedRows, err := parseImportErrors(document)
		if err != nil {
			// The document could not be understood at all. Retrying is the safe failure mode:
			// reporting success here would silently deliver contacts SendGrid rejected.
			b.Logger.Errorn("[sendgrid bulk upload] unable to parse the errors document",
				logger.NewStringField("error", sanitizeReason(err.Error())),
				obskit.DestinationID(b.DestinationID))
			return common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to parse the sendgrid errors document: " + err.Error()),
			}
		}
		rows = append(rows, parsedRows...)
	}

	metadata := common.EventStatMeta{
		FailedKeys:     make([]int64, 0),
		AbortedKeys:    make([]int64, 0),
		WarningKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0),
		FailedReasons:  make(map[int64]string),
		AbortedReasons: make(map[int64]string),
		WarningReasons: make(map[int64]string),
	}

	lookup := b.buildIdentifierLookup(input.ImportingList)
	unmatchedRows, ambiguousRows := 0, 0
	for _, row := range rows {
		jobIDs, ok := lookup[strings.ToLower(strings.TrimSpace(row.Identifier))]
		if !ok || len(jobIDs) == 0 {
			// Counted and logged so that a change in the undocumented document shape becomes
			// visible in metrics. The identifier is fingerprinted rather than logged verbatim,
			// because it is personal data, and the row's own message is sanitized because it can
			// echo the rejected contact's address back.
			//
			// Observability alone would NOT make this safe: an unattributed row's job would be
			// marked succeeded by exclusion, so the reconciliation is failed closed below
			// instead, and it is that non-200 - not this log line - that prevents the loss.
			unmatchedRows++
			b.Logger.Warnn("[sendgrid bulk upload] errored row could not be matched to an importing job",
				obskit.DestinationID(b.DestinationID),
				logger.NewStringField("identifierFingerprint", redactIdentifier(row.Identifier)),
				logger.NewStringField("reason", sanitizeReason(row.Message)))
			continue
		}
		reason := coalesce(sanitizeReason(row.Message), defaultFailureReason)
		if len(jobIDs) > 1 {
			// The identifier is ambiguous - two staged events carried it, so SendGrid upserted
			// them onto one contact - and the failure cannot be attributed to just one of them.
			// EVERY candidate is failed: failing a job that in fact succeeded costs one
			// idempotent re-upsert, whereas reporting a failed contact as delivered loses it.
			ambiguousRows++
			reason = fmt.Sprintf("%s (this contact identifier matches %d jobs, so all of them are retried)", reason, len(jobIDs))
			b.Logger.Warnn("[sendgrid bulk upload] errored row matches more than one importing job",
				obskit.DestinationID(b.DestinationID),
				logger.NewIntField("jobCount", int64(len(jobIDs))),
				logger.NewStringField("identifierFingerprint", redactIdentifier(row.Identifier)))
		}
		for _, jobID := range jobIDs {
			if _, seen := metadata.FailedReasons[jobID]; !seen {
				metadata.FailedKeys = append(metadata.FailedKeys, jobID)
			}
			metadata.FailedReasons[jobID] = reason
		}
	}
	if unmatchedRows > 0 {
		b.StatsFactory.NewTaggedStat("unmatched_error_row_count", stats.CountType, b.statLabels(b.DestinationID)).Count(unmatchedRows)
	}
	if ambiguousRows > 0 {
		b.StatsFactory.NewTaggedStat("ambiguous_error_row_count", stats.CountType, b.statLabels(b.DestinationID)).Count(ambiguousRows)
	}
	if unmatchedRows > 0 {
		// FAIL CLOSED. The reconciliation is incomplete, so no job may be marked succeeded from
		// it: succeeded-by-exclusion would deliver exactly the contacts whose rows could not be
		// attributed. A retryable status makes the batch router poll again, and abort only once
		// its retry budget is spent.
		b.Logger.Errorn("[sendgrid bulk upload] refusing to reconcile an import with unattributable errored rows",
			obskit.DestinationID(b.DestinationID),
			logger.NewIntField("unmatchedRowCount", int64(unmatchedRows)),
			logger.NewIntField("rowCount", int64(len(rows))))
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error: fmt.Sprintf("%d of %d sendgrid errored rows could not be attributed to an importing job",
				unmatchedRows, len(rows)),
		}
	}

	// Succeeded by exclusion: every importing job that no errored row resolved to was
	// delivered. The set difference keeps the two key sets disjoint and stays linear even for
	// an import holding tens of thousands of jobs.
	//
	// FilterMap rather than Map because a nil entry in the importing list must be skipped, not
	// dereferenced: this method runs inside a shared batch router worker, where a panic would
	// take down every destination the worker is serving, not just this one. The identifier
	// lookup guards the same way, so the two passes agree on which entries are usable.
	importingJobIDs := lo.FilterMap(input.ImportingList, func(job *jobsdb.JobT, _ int) (int64, bool) {
		if job == nil {
			return 0, false
		}
		return job.JobID, true
	})
	succeededKeys, _ := lo.Difference(importingJobIDs, metadata.FailedKeys)
	metadata.SucceededKeys = append(metadata.SucceededKeys, succeededKeys...)

	b.Logger.Infon("[sendgrid bulk upload] reconciled a partially failed import",
		obskit.DestinationID(b.DestinationID),
		logger.NewIntField("failedCount", int64(len(metadata.FailedKeys))),
		logger.NewIntField("succeededCount", int64(len(metadata.SucceededKeys))),
		logger.NewIntField("unmatchedRowCount", int64(unmatchedRows)))

	// 200 is mandatory on success: the batch router discards the entire reconciliation, and
	// returns an error, for any other status.
	return common.GetUploadStatsResponse{
		StatusCode: http.StatusOK,
		Metadata:   metadata,
	}
}

// resolveErrorsURLs finds the errors documents to reconcile against.
//
// The URLs Poll forwarded are preferred, because SendGrid supplied them directly. When they are
// absent - which happens when the process that polled is not the process that reconciles - the
// import identifier is recovered from the persisted parameters and the import status is read
// again to obtain them.
//
// The second return value is only meaningful when no URL could be resolved, in which case it
// carries the non-200 response the caller must return: never a 200 with nothing failed, which
// would mark the whole import delivered.
func (b *SendGridBulkUploader) resolveErrorsURLs(input common.GetUploadStatsInput) ([]string, common.GetUploadStatsResponse) {
	if errorsURLs := splitErrorsURLs(input.FailedJobParameters); len(errorsURLs) > 0 {
		return errorsURLs, common.GetUploadStatsResponse{StatusCode: http.StatusOK}
	}

	var params struct {
		ImportId string `json:"importId"`
	}
	if err := jsonrs.Unmarshal(input.Parameters, &params); err != nil {
		b.Logger.Errorn("[sendgrid bulk upload] unable to parse the persisted import parameters",
			obskit.Error(err),
			obskit.DestinationID(b.DestinationID))
		return nil, common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      sanitizeReason("Failed to parse parameters: " + err.Error()),
		}
	}

	errorsURLs := make([]string, 0, 1)
	for _, importID := range splitImportIDs(params.ImportId) {
		importStatus, err := b.SendGridAPIService.GetImportStatus(importID)
		if err != nil {
			b.Logger.Errorn("[sendgrid bulk upload] unable to re-read import status while reconciling",
				logger.NewStringField("error", sanitizeReason(err.Error())),
				obskit.DestinationID(b.DestinationID),
				logger.NewStringField("importId", importID))
			return nil, common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to fetch the sendgrid import status: " + err.Error()),
			}
		}
		errorsURLs = appendErrorsURL(errorsURLs, importStatus)
	}
	if len(errorsURLs) == 0 {
		b.Logger.Errorn("[sendgrid bulk upload] no errors document is available for a failed import",
			obskit.DestinationID(b.DestinationID))
		return nil, common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      "SendGrid reported errored contacts without publishing an errors document",
		}
	}
	return errorsURLs, common.GetUploadStatsResponse{StatusCode: http.StatusOK}
}

// buildIdentifierLookup indexes the importing jobs by every contact identifier their events
// carry, so that an errored row can be resolved back to the job that produced it with no state
// retained from the upload.
//
// All four identifiers SendGrid accepts are indexed - the email first and foremost, but the
// external, anonymous and phone identifiers too - because the errors document is free to report
// whichever of them it likes and this connector cannot dictate the choice. All keys are
// lower-cased, which is what makes matching insensitive to SendGrid's own normalization of the
// email address.
//
// An identifier maps to EVERY job that claimed it, not just the first. Two staged events can
// legitimately carry the same address - SendGrid upserts them onto one contact - and in that
// case the errored row genuinely refers to both, so keeping only one would let the other be
// marked succeeded by exclusion even though its contact failed.
func (b *SendGridBulkUploader) buildIdentifierLookup(importingList []*jobsdb.JobT) map[string][]int64 {
	lookup := make(map[string][]int64, len(importingList))
	for _, job := range importingList {
		if job == nil {
			continue
		}
		contact, err := b.buildContact(stagedMessage(job.EventPayload))
		if err != nil {
			// A job whose event carries no identifier was never uploaded, so it cannot appear
			// in the errors document either; it is logged for completeness and skipped.
			b.Logger.Warnn("[sendgrid bulk upload] importing job carries no contact identifier",
				obskit.DestinationID(b.DestinationID),
				logger.NewIntField("jobID", job.JobID),
				logger.NewStringField("error", sanitizeReason(err.Error())))
			continue
		}
		for _, identifier := range identifiersOf(contact) {
			if !lo.Contains(lookup[identifier], job.JobID) {
				lookup[identifier] = append(lookup[identifier], job.JobID)
			}
		}
	}
	return lookup
}

// jsonDepth reports the deepest nesting of a JSON document, ignoring braces and brackets that
// appear inside string literals. It works on raw bytes so the depth can be bounded BEFORE the
// document is handed to a decoder.
func jsonDepth(document []byte) int {
	var depth, maxDepth int
	inString, escaped := false, false
	for _, character := range document {
		if inString {
			switch {
			case escaped:
				escaped = false
			case character == '\\':
				escaped = true
			case character == '"':
				inString = false
			}
			continue
		}
		switch character {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case '}', ']':
			depth--
		}
	}
	return maxDepth
}

// parseImportErrors decodes the document SendGrid publishes for an import's errored rows.
//
// The parser is deliberately TOLERANT of several shapes because the document's schema is
// genuinely undocumented: the official specification mentions the errors URL only as a bare
// string, with no media type and no schema, and the reference pages describe no format at all.
// Committing to one guessed shape would turn any difference between the guess and reality into
// silent data loss, so a bare array, an object wrapping the rows under errors or under results,
// a single row object, and newline-delimited JSON are all accepted.
//
// Per row, the message is taken from the first of message, error_message, reason or detail that
// is present, and the identifier from the first of email, contact.email, identifier,
// external_id or anonymous_id.
//
// A document that matches NONE of the accepted shapes, or that matches one but yields no usable
// row, is reported as an error rather than as an empty result. That distinction matters: an
// empty result would be reconciled as "nothing failed" and would mark every job in a partially
// errored import delivered, whereas an error makes the batch router retry.
//
// The repository's mandated codec decides whether the bytes are one well-formed JSON value at
// all; gjson is then used purely for read-only field extraction, which is what handles the
// nested contact.email candidate without a bespoke type per shape.
func parseImportErrors(document []byte) ([]ImportErrorRow, error) {
	trimmed := bytes.TrimSpace(document)
	if len(trimmed) == 0 {
		return nil, errors.New("the errors document is empty")
	}
	// Depth is checked before the document is decoded, because decoding a deeply nested value
	// is what costs the memory and the stack: the check has to happen while the input is still
	// just bytes. The document's schema is not published, so no legitimate shape is anywhere
	// near this bound.
	if depth := jsonDepth(trimmed); depth > maxErrorsDocumentDepth {
		return nil, fmt.Errorf("the errors document is nested %d levels deep, more than the %d allowed", depth, maxErrorsDocumentDepth)
	}

	var probe any
	if err := jsonrs.Unmarshal(trimmed, &probe); err == nil {
		root := gjson.ParseBytes(trimmed)
		if root.IsArray() {
			entries := root.Array()
			if len(entries) > maxErrorRows {
				return nil, fmt.Errorf("the errors document holds %d rows, more than the %d allowed", len(entries), maxErrorRows)
			}
			if rows := importErrorRowsFrom(entries); len(rows) > 0 {
				return rows, nil
			}
			return nil, errors.New("the errors document is an array carrying no recognizable rows")
		}
		if root.IsObject() {
			for _, key := range []string{"errors", "results"} {
				if wrapped := root.Get(key); wrapped.IsArray() {
					entries := wrapped.Array()
					if len(entries) > maxErrorRows {
						return nil, fmt.Errorf("the errors document's %q array holds %d rows, more than the %d allowed", key, len(entries), maxErrorRows)
					}
					if rows := importErrorRowsFrom(entries); len(rows) > 0 {
						return rows, nil
					}
					return nil, fmt.Errorf("the errors document's %q array carries no recognizable rows", key)
				}
			}
			if row, ok := importErrorRowFrom(root); ok {
				return []ImportErrorRow{row}, nil
			}
			return nil, errors.New("the errors document is an object carrying no recognizable rows")
		}
		return nil, errors.New("the errors document is neither an array nor an object")
	}

	// Not one JSON value, so the document may be newline-delimited JSON - the last shape this
	// parser accepts.
	return parseNewlineDelimitedImportErrors(trimmed)
}

// parseNewlineDelimitedImportErrors decodes an errors document written as one JSON object per
// line.
//
// A single unreadable line is skipped rather than failing the whole document, because one
// malformed line must not cost the reconciliation of every other row; a document yielding no
// row at all is still reported as an error, so it can never be mistaken for "nothing failed".
func parseNewlineDelimitedImportErrors(document []byte) ([]ImportErrorRow, error) {
	rows := make([]ImportErrorRow, 0)
	scanner := bufio.NewScanner(bytes.NewReader(document))
	scanner.Buffer(nil, int(defaultMaxBufferCapacity))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var probe any
		if err := jsonrs.Unmarshal(line, &probe); err != nil {
			continue
		}
		parsed := gjson.ParseBytes(line)
		if !parsed.IsObject() {
			continue
		}
		if row, ok := importErrorRowFrom(parsed); ok {
			if len(rows) >= maxErrorRows {
				return nil, fmt.Errorf("the errors document holds more than the %d allowed rows", maxErrorRows)
			}
			rows = append(rows, row)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading the newline delimited errors document: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("the errors document matches none of the shapes this parser understands")
	}
	return rows, nil
}

// importErrorRowsFrom reduces the rows of an errors document to the identifier and the message
// reconciliation needs, skipping entries that carry neither.
func importErrorRowsFrom(entries []gjson.Result) []ImportErrorRow {
	rows := make([]ImportErrorRow, 0, len(entries))
	for _, entry := range entries {
		if row, ok := importErrorRowFrom(entry); ok {
			rows = append(rows, row)
		}
	}
	return rows
}

// importErrorRowFrom reduces one row of an errors document to its identifier and its message.
//
// A row is usable as soon as it carries either, and it reports false only when it carries
// neither - a row with a message but no resolvable identifier is still worth keeping, because it
// is then counted and logged as unmatched instead of vanishing.
//
// The identifier is lower-cased here so that everything downstream compares like with like.
func importErrorRowFrom(entry gjson.Result) (ImportErrorRow, bool) {
	if !entry.IsObject() {
		return ImportErrorRow{}, false
	}
	identifier := firstString(entry, "email", "contact.email", "identifier", "external_id", "anonymous_id")
	message := firstString(entry, "message", "error_message", "reason", "detail")
	if identifier == "" && message == "" {
		return ImportErrorRow{}, false
	}
	return ImportErrorRow{
		Identifier: strings.ToLower(identifier),
		Message:    message,
	}, true
}

// identifiersOf lists every identifier a contact can be recognized by in an errors document,
// lower-cased and with empties dropped.
func identifiersOf(contact Contact) []string {
	identifiers := make([]string, 0, 4)
	for _, identifier := range []string{contact.Email, contact.ExternalID, contact.AnonymousID, contact.PhoneNumberID} {
		if trimmed := strings.ToLower(strings.TrimSpace(identifier)); trimmed != "" {
			identifiers = append(identifiers, trimmed)
		}
	}
	return lo.Uniq(identifiers)
}

// emailPattern and longNumberPattern recognize the two personally identifiable shapes that
// realistically appear inside a provider-supplied message, which is text this connector does not
// control yet has to record against a job.
//
// longNumberPattern deliberately requires at least nine digits: long enough to cover a phone
// number or a numeric account identifier, and short enough to leave the values an operator needs
// untouched - a date, an RFC3339 rate-limit reset instant, a quota or a byte count.
var (
	emailPattern      = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	longNumberPattern = regexp.MustCompile(`\+?\d(?:[\s().-]?\d){8,}`)
)

// redactIdentifier turns a contact identifier into a short, stable, non-reversible fingerprint.
//
// An identifier is personal data - an email address, a phone number, an account ID - so it is
// never written to a log verbatim. The fingerprint still lets the same identifier be correlated
// across log lines during an investigation, which is the only thing an operator needs it for.
func redactIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.ToLower(trimmed)))
	return hex.EncodeToString(digest[:])[:16]
}

// sanitizeReason prepares provider-supplied text for somewhere it is KEPT: a job status that is
// persisted in the jobs database, or a log line.
//
// The connector is required to record SendGrid's explanation for a failed contact, and that text
// is outside its control: it can echo the rejected contact's own email or phone number, it can
// carry control characters that would corrupt a structured log stream, and it has no length
// bound. Recognizable personal shapes are therefore redacted, control and non-printable
// characters are dropped, whitespace is collapsed to single spaces, and the result is length
// capped.
func sanitizeReason(reason string) string {
	if reason == "" {
		return ""
	}
	redacted := emailPattern.ReplaceAllString(reason, "<redacted-email>")
	redacted = longNumberPattern.ReplaceAllString(redacted, "<redacted-number>")

	var builder strings.Builder
	builder.Grow(len(redacted))
	pendingSpace := false
	for _, character := range redacted {
		switch {
		case unicode.IsSpace(character):
			pendingSpace = builder.Len() > 0
		case unicode.IsControl(character) || !unicode.IsPrint(character):
			// Dropped entirely: it carries no diagnostic value and can corrupt a log stream.
		default:
			if pendingSpace {
				builder.WriteRune(' ')
				pendingSpace = false
			}
			builder.WriteRune(character)
		}
	}
	sanitized := strings.TrimSpace(builder.String())
	runes := []rune(sanitized)
	if len(runes) <= maxReasonRunes {
		return sanitized
	}
	return string(runes[:maxReasonRunes]) + " ... (truncated)"
}

// stagedMessage extracts the event message from an importing job's payload.
//
// The batch router hands reconciliation the jobs exactly as they were queued, so the message is
// read from the same place Transform reads it. The two fallbacks make the derivation resilient
// to a job whose payload is a staging-file line, or is the message itself, rather than failing
// to resolve an identifier and reporting a failed contact as delivered.
func stagedMessage(payload []byte) gjson.Result {
	if message := gjson.GetBytes(payload, "body.JSON"); message.Exists() {
		return message
	}
	if message := gjson.GetBytes(payload, "message"); message.Exists() {
		return message
	}
	return gjson.ParseBytes(payload)
}

// appendErrorsURL adds an import's errors document URL to the accumulated list, ignoring an
// absent one and never adding the same URL twice.
func appendErrorsURL(errorsURLs []string, importStatus *ImportStatusResponse) []string {
	errorsURL := strings.TrimSpace(importStatus.Results.ErrorsURL)
	if errorsURL == "" || lo.Contains(errorsURLs, errorsURL) {
		return errorsURLs
	}
	return append(errorsURLs, errorsURL)
}

// describeImport renders one import's outcome for an operator: the state, the row counters and
// the errors document URL, which together explain why a batch was not delivered cleanly.
func describeImport(importID string, importStatus *ImportStatusResponse) string {
	description := fmt.Sprintf(
		"import %s status %s (requested %d, created %d, updated %d, errored %d)",
		importID, importStatus.Status,
		importStatus.Results.RequestedCount, importStatus.Results.CreatedCount,
		importStatus.Results.UpdatedCount, importStatus.Results.ErroredCount,
	)
	if errorsURL := strings.TrimSpace(importStatus.Results.ErrorsURL); errorsURL != "" {
		description += ", errors document " + errorsURL
	}
	return description
}

// splitImportIDs recovers the individual SendGrid job_ids from the single import identifier the
// batch router persisted.
func splitImportIDs(importID string) []string {
	return splitAndClean(importID, importIDSeparator)
}

// splitErrorsURLs recovers the individual errors document URLs Poll joined into the poll
// response's failure parameters.
func splitErrorsURLs(failedJobParameters string) []string {
	return splitAndClean(failedJobParameters, errorsURLSeparator)
}

// splitAndClean splits a joined field back into its parts, dropping blanks and duplicates so
// that a trailing separator or a repeated value cannot produce a bogus request.
func splitAndClean(value, separator string) []string {
	parts := strings.Split(value, separator)
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return lo.Uniq(cleaned)
}

// normalizeListIDs trims, de-duplicates and drops blank list IDs, returning nil when nothing is
// left so that the list_ids key is omitted from the request body entirely.
func normalizeListIDs(listIDs []string) []string {
	normalized := make([]string, 0, len(listIDs))
	for _, listID := range listIDs {
		if trimmed := strings.TrimSpace(listID); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return lo.Uniq(normalized)
}

// listIDsKey is the canonical grouping key for a set of list IDs. It is used only as a map key,
// never sent anywhere, and the separator is a character that cannot appear in a SendGrid list ID.
func listIDsKey(listIDs []string) string {
	return strings.Join(listIDs, ",")
}

// joinReasons renders several failure reasons as one, dropping blanks and repetitions so that a
// reason recorded once per rejected chunk does not become an unreadable wall of duplicates in a
// job's error response.
//
// It is also the single place the two durable upload reasons are sanitized. A reason can quote
// SendGrid's own response, which is text this connector does not control and which can echo a
// rejected contact's address; these strings are persisted with the job, so they are cleaned of
// personal shapes and control characters and length capped before they get there. The rate-limit
// reset window survives verbatim, which is what keeps the 429 reason actionable.
func joinReasons(reasons []string) string {
	sanitized := lo.FilterMap(reasons, func(reason string, _ int) (string, bool) {
		clean := sanitizeReason(reason)
		return clean, clean != ""
	})
	return strings.Join(lo.Uniq(sanitized), "; ")
}

// firstResult returns the first of the given paths that is present, or the zero result when none
// is. The zero result is safe to keep reading from, which is what lets the callers chain lookups
// without a guard at every step.
func firstResult(source gjson.Result, paths ...string) gjson.Result {
	for _, path := range paths {
		if value := source.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

// firstString returns the trimmed string value of the first of the given paths that holds a
// non-blank value, which is how one contact field accepts several accepted trait spellings.
func firstString(source gjson.Result, paths ...string) string {
	for _, path := range paths {
		if value := strings.TrimSpace(source.Get(path).String()); value != "" {
			return value
		}
	}
	return ""
}

// stringSliceOf renders a JSON array as a slice of trimmed, non-blank strings, and a lone scalar
// as a single-element slice. It returns nil when nothing is left, so that an omitempty field
// stays omitted rather than overwriting stored data with an empty array.
func stringSliceOf(source gjson.Result) []string {
	if !source.Exists() {
		return nil
	}
	values := make([]string, 0, 1)
	if source.IsArray() {
		for _, element := range source.Array() {
			if value := strings.TrimSpace(element.String()); value != "" {
				values = append(values, value)
			}
		}
	} else if value := strings.TrimSpace(source.String()); value != "" {
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

// lookupTrait resolves one mapped trait name against an event.
//
// The exact key is tried first, on the traits object and then on the message, which is what makes
// a trait whose own name contains a dot resolvable at all - a path lookup would read such a name
// as a nested path. Only then is the name treated as a path, which is what makes a mapping such
// as "address.city" work.
func lookupTrait(message, traits gjson.Result, traitName string) gjson.Result {
	for _, source := range []gjson.Result{traits, message} {
		if !source.IsObject() {
			continue
		}
		if value, ok := source.Map()[traitName]; ok {
			return value
		}
	}
	if value := firstResult(traits, traitName); value.Exists() {
		return value
	}
	return firstResult(message, traitName)
}

// coalesce returns the first non-blank value, which keeps the contact mapping readable where a
// field has more than one accepted source.
func coalesce(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
