package sendgridbulkupload

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

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

	// minContactBudgetBytes is the smallest contact budget a request envelope may leave behind
	// before the envelope itself - rather than any contact - is treated as the problem.
	//
	// SendGrid's ceiling applies to the WHOLE serialized request body, so the
	// {"list_ids":[...],"contacts":[...]} envelope and every list ID inside it are charged
	// against the same 6MB. This connector therefore MEASURES that envelope exactly, once per
	// list group, and hands the chunker only what is left (see requestEnvelopeBytes); it is the
	// one place where it deliberately diverges from the Klaviyo chunker it is otherwise modeled
	// on, which measures only the elements it packs.
	//
	// A group whose list IDs leave less than this behind could not carry even the smallest
	// possible contact - {"email":"a@b.co"} serializes to eighteen bytes - so its jobs are
	// rejected against the LIST IDS instead of being reported one by one as contacts that are
	// individually too large, which would name the wrong cause entirely.
	minContactBudgetBytes = 32

	// defaultMaxContactsPerRequest is the element cap used when nothing overrides it.
	defaultMaxContactsPerRequest = sendGridMaxContactsPerRequest

	// defaultMaxRequestBytes is the byte cap used when nothing overrides it: the documented
	// ceiling itself, applying to the WHOLE request body.
	//
	// No constant reserve is deducted from it. A flat reserve is a guess in both directions: too
	// small and SendGrid rejects a request that was within the element budget but over the wire
	// limit once the envelope was added, too large and the connector splits batches it could have
	// sent whole. The envelope is measured per request instead, which is exact.
	defaultMaxRequestBytes = sendGridMaxRequestBytes
)

// Configuration keys, resolved through the batch router's two-level configuration helpers so
// that every value can be tuned per destination type as
// BatchRouter.SENDGRID_BULK_UPLOAD.<key> and falls back to BatchRouter.<key>.
const (
	// configKeyMaxContactsPerRequest overrides the element cap. It exists for operational
	// tuning only; the default is already the API's documented ceiling.
	configKeyMaxContactsPerRequest = "maxContactsPerRequest"

	// configKeyMaxRequestBytes overrides the byte cap applied to a whole request body, envelope
	// and list IDs included.
	configKeyMaxRequestBytes = "maxRequestBytes"

	// configKeyMaxBufferCapacity overrides the largest staging-file line that can be read. Its
	// default is derived from the byte cap above, so lowering it below that cap trades the ability
	// to classify a large record for a smaller per-line allocation.
	configKeyMaxBufferCapacity = "maxBufferCapacity"

	// configKeyMaxImportsPerUpload overrides how many SendGrid imports one upload may create, and
	// therefore how many status requests a single poll of it can cost.
	configKeyMaxImportsPerUpload = "maxImportsPerUpload"
)

// configFieldCustomFieldsMapping names the destination-configuration field the trait-to-custom-
// field mapping arrives in. It is used in the validation errors that reject a malformed mapping,
// so the message names the exact field an operator has to correct; it is declared here so the
// name in those messages can never drift from the JSON tag on DestinationConfig.
const configFieldCustomFieldsMapping = "customFieldsMapping"

// Bounds on how many SendGrid imports one upload may create.
//
// This is a POLL budget expressed at the only place that can control it. One upload is chunked
// against both request caps, each accepted chunk becomes its own SendGrid import, and every one of
// those imports has to be asked about by status identifier - so the number of chunks an upload
// produces IS the worst-case number of provider requests each later poll of it costs. Poll cannot
// bound that itself: it may not persist a cursor, so it could not resume where a previous poll
// stopped, and calling an import settled without asking would risk reporting rejected contacts as
// delivered.
//
// Three things keep the realistic cost far below the worst case: a poll stops at the FIRST import
// still pending, an error or a rate limit on any status request returns immediately, and the batch
// router's own per-batch event cap means an ordinary batch produces a single chunk. The bound exists
// for the pathological batch - thousands of unusually large contacts - where none of that helps.
//
// Chunks beyond the bound are not sent, and their jobs are reported on the RETRYABLE channel. The
// batch router re-batches them on the next pass, so an oversized batch drains in bounded steps
// rather than either issuing an unbounded number of requests or losing anything.
const (
	// defaultMaxImportsPerUpload is the number of imports one upload may create when nothing
	// overrides it.
	defaultMaxImportsPerUpload = 64

	// maxImportsPerUploadCeiling bounds what an override may raise the budget to, so that the
	// number of requests a poll can cost stays explicitly bounded however the destination is
	// configured.
	maxImportsPerUploadCeiling = 512
)

// Staging-file line limits.
//
// bufio.Scanner reports a line longer than its token limit as an error rather than truncating it
// silently, and this connector handles that error as a retryable failure of the WHOLE batch - so
// the limit has to be large enough to read every line the rest of the pipeline could still
// classify on its own.
//
// The largest such line is the one carrying a contact at, or just past, an entire request's byte
// budget: the chunker rejects that ONE contact and delivers the rest of the batch, but only if
// the line could be read in the first place. A limit below the request cap would turn a single
// oversized record into a whole-batch failure that repeats identically on every retry until the
// batch router's retry budget is spent - the record would never even reach the per-record
// isolation that exists precisely to contain it. The default is therefore DERIVED from the
// request cap rather than picked, and the sibling connectors' 512KB default is deliberately not
// copied: none of them budgets a 6MB request.
//
// bufio.Scanner allocates lazily, starting at 4KB and doubling only as a line demands it, so a
// large ceiling costs nothing for the ordinary lines that make up almost every staging file.
const (
	// stagingRecordHeadroomBytes covers everything a staging line carries that the contact
	// derived from it does not: the {"message":{...},"metadata":{"job_id":N}} envelope, the event
	// fields that map onto no contact field at all - type, event, the four timestamps, messageId,
	// rudderId, context - and JSON escaping.
	stagingRecordHeadroomBytes = 2 * 1000 * 1000

	// defaultMaxBufferCapacity is the largest single staging-file line this connector reads when
	// nothing overrides it: enough for a record whose contact fills a whole request, plus the
	// headroom above.
	defaultMaxBufferCapacity = sendGridMaxRequestBytes + stagingRecordHeadroomBytes

	// minMaxBufferCapacity is the floor an override is raised to.
	//
	// It is bufio's OWN default token size. A buffer below it is strictly worse than not
	// configuring the buffer at all: every line longer than the configured value fails the
	// scan, and because a scan failure aborts the whole read, one small number would turn
	// every batch for that destination into a permanent retry loop. Raising to the floor keeps
	// a mistyped value at least as capable as the unconfigured scanner.
	minMaxBufferCapacity = bufio.MaxScanTokenSize

	// maxBufferCapacityCeiling bounds what an override may raise the limit to, so that a
	// configuration mistake - or a deliberately hostile entry - cannot turn a bounded per-line
	// read into an effectively unbounded allocation inside a batch router worker shared by every
	// destination in the process. bufio allocates the buffer lazily but grows it to whatever it is
	// told to allow, so the ceiling is the only thing that bounds it.
	maxBufferCapacityCeiling = 4 * defaultMaxBufferCapacity
)

// The encoding that carries every import one upload produced - together with the jobs each of
// them accepted - through the single string common.ImportParameters gives the import identifier.
//
// One upload legitimately produces SEVERAL SendGrid imports: the batch is chunked against both
// request caps and against the resolved list IDs, and every accepted chunk gets its own job_id.
// Joining those identifiers is not sufficient, because SendGrid reports a state PER IMPORT. A
// batch in which one import is rejected outright while another completes can only be resolved
// correctly if it is known WHICH jobs belonged to the rejected one - otherwise the rejected
// import's contacts are either retried blindly or, far worse, marked delivered by exclusion
// against the other import's errors document. Reconciliation is stateless and may run in another
// process, so that mapping has to be PERSISTED alongside the identifiers rather than remembered.
//
// The rendered form is `<importID>=<jobs>;<importID>=<jobs>`, where <jobs> is an ascending,
// range-collapsed list such as `1-500,507`. It remains a plain string, so the batch router's
// gjson read of "importId" and the string-typed common.AsyncPoll.ImportId are both unaffected,
// and collapsing runs keeps the ordinary case - a chunk holding a contiguous run of job IDs -
// down to a few dozen bytes.
//
// All four delimiters are absent from a SendGrid import job_id, which is UUID-shaped. That is
// not left to trust: Upload refuses to record an import whose identifier contains one of them,
// so an unreadable manifest cannot be produced even if the provider changes the shape.
const (
	// importManifestSeparator separates one import's entry from the next.
	importManifestSeparator = ";"

	// importManifestMembershipMark separates an import's identifier from its job IDs. An entry
	// that lacks it names an import whose membership is UNKNOWN, which is what both the legacy
	// form and the degraded form produce, and which reconciliation handles explicitly.
	importManifestMembershipMark = "="

	// importManifestJobSeparator separates the job IDs and ranges inside one entry.
	importManifestJobSeparator = ","

	// importManifestRangeMark joins the two ends of a collapsed run of consecutive job IDs.
	importManifestRangeMark = "-"

	// importIDSeparator is accepted when splitting an import identifier and is never produced.
	// It is the separator this connector joined identifiers with before the manifest existed,
	// so a value persisted by an earlier build still resolves to its import identifiers - with
	// membership reported as unknown, which is exactly what such a value carries.
	importIDSeparator = ":"

	// errorsURLSeparator joins several errors-document URLs into
	// PollStatusResponse.FailedJobParameters when the structured outcome document cannot be
	// rendered.
	//
	// A newline is used because it cannot legally appear inside a URL, so the join is always
	// reversible. The field is passed straight from Poll to GetUploadStats in memory and is
	// never persisted, so nothing downstream is sensitive to the choice.
	errorsURLSeparator = "\n"
)

// maxImportManifestBytes bounds the rendered manifest, and the bound is load-bearing rather
// than defensive.
//
// The batch router copies the import parameters into the status row of EVERY importing job, so a
// manifest of length N is stored N times over: membership rendered without a cap would turn a
// large batch into quadratic storage. Range collapsing already keeps the ordinary case far below
// this - one import over a contiguous run of job IDs renders in about fifty bytes, whatever the
// run's length - and the cap only ever binds when the job IDs of one import are heavily
// interleaved with another's, which happens when a batch resolves to several different list ID
// sets. Exceeding it degrades to the identifier-only form: membership is then reported as
// unknown and reconciliation refuses to succeed anything by exclusion in the presence of a
// rejected import, which is a correct if pessimistic outcome, whereas an unbounded manifest
// would be a storage fault.
const maxImportManifestBytes = 1024

// Bounds and the character set applied to one import identifier before it is persisted.
const (
	// maxImportIDLength bounds a single SendGrid import job_id. A UUID is thirty-six characters,
	// so this leaves generous room while keeping one entry's contribution to the manifest small
	// and bounded.
	maxImportIDLength = 128

	// importIDForbiddenCharacters are the characters an identifier must not contain if the
	// manifest is to be decoded unambiguously: every delimiter the encoding uses, the legacy
	// separator it still accepts, and whitespace, which the decoder trims and would therefore
	// alter. A SendGrid job_id contains none of them.
	importIDForbiddenCharacters = importManifestSeparator +
		importManifestMembershipMark +
		importManifestJobSeparator +
		importIDSeparator +
		" \t\r\n"

	// maxImportMembershipJobs bounds how many job IDs one import's membership may expand to.
	//
	// It exists because a range is a COMPRESSED form: twenty characters can ask for a billion
	// elements, so the decoder has to know when to refuse rather than allocate. The ceiling sits
	// far above any batch the router assembles - two orders of magnitude above the default
	// maximum events per batch - so it can only ever be reached by a value this connector did not
	// write, which is exactly the case that must be refused.
	maxImportMembershipJobs = 200000
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

	// importStatusUnrecognized is NOT a SendGrid value. It is this connector's own label for
	// any state outside the four above, and it is what normalizeImportStatus collapses an
	// unknown value to so that the state can be logged and tagged without putting
	// provider-controlled text into telemetry or giving a metric tag unbounded cardinality.
	importStatusUnrecognized = "unrecognized"
)

// batchRouterModule is the module stats tag every measurement in this connector carries,
// matching the batch router's own module name.
const batchRouterModule = "batch_router"

// externalIDTypeListIDs is the context.externalId entry type that carries per-event SendGrid
// list IDs, which take precedence over the destination configuration.
const externalIDTypeListIDs = "listIds"

// Reasons recorded against jobs this connector rejects locally, before any request is made.
//
// All four conditions are PERMANENT: the batch router would rebuild an identical payload on a
// retry, so retrying could only ever fail again and would burn the retry budget for nothing.
// They are therefore the only LOCAL outcomes in this connector that use the terminal abort
// channel. Three of them abandon exactly one job; the fourth - list IDs that consume a whole
// request's budget - abandons the group of jobs that target those lists, because the condition
// belongs to the destination's configuration rather than to any single contact.
//
// Exactly one other terminal outcome exists anywhere in this connector, and it is not local: an
// import SendGrid itself declared failed, which the provider defines as finished with all errors
// or entirely unprocessable (see reasonImportFailed). Every other provider response is retryable
// - not a rate limit, and not a rejected request either: the batch router owns the decision to
// give up, and escalates to an abort once its own retry budget is spent.
const (
	// reasonMissingIdentifier is recorded when an event yields a contact carrying none of
	// the four identifiers SendGrid accepts.
	reasonMissingIdentifier = "sendgrid requires each contact to carry at least one of email, phone_number_id, external_id or anonymous_id"

	// reasonContactTooLarge is recorded when a single contact is larger than an entire
	// request's byte budget, so no chunk could ever hold it.
	reasonContactTooLarge = "the contact is larger than the maximum sendgrid request size and cannot be uploaded in any batch"

	// reasonFieldTooLong is recorded when a reserved contact field carries a value longer than
	// this connector will put on the wire.
	//
	// Terminal, like the other local rejections: the same bytes produce the same oversized field
	// on every retry. Rejecting the ONE record is what protects the batch - a field the provider
	// refuses would otherwise fail the whole request that carried it, taking tens of thousands of
	// valid contacts down with it.
	reasonFieldTooLong = "a field of this contact is longer than the maximum length this connector will send to sendgrid"

	// reasonTooManyFieldValues is recorded when a multi-valued contact field carries more entries
	// than this connector will send.
	//
	// Deliberately NOT silent truncation: dropping entries would change what the operator asked to
	// be delivered while reporting success, whereas rejecting the record says plainly that it was
	// not sent.
	reasonTooManyFieldValues = "a field of this contact carries more values than this connector will send to sendgrid"

	// reasonMalformedRecord is recorded when a staging-file line is malformed yet still names
	// the job that produced it.
	reasonMalformedRecord = "the staging file record for this job is malformed and cannot be turned into a sendgrid contact"

	// reasonListIDsTooLarge is recorded when the list IDs a group of contacts targets are
	// themselves large enough to consume a whole request's byte budget, leaving no room for any
	// contact to travel with them. The list IDs come from the destination's configuration or from
	// the events' own external IDs, so every retry would rebuild the same oversized envelope.
	reasonListIDsTooLarge = "the sendgrid list ids these contacts target consume the entire request budget, so no contact can be uploaded alongside them"
)

// defaultFailureReason is recorded for an errored row whose document carried no message, so
// that a job is never marked failed with an empty explanation.
const defaultFailureReason = "sendgrid reported an error for this contact without a message"

// Reasons recorded when reconciliation cannot PROVE a contact was delivered.
//
// Both are retryable, and both exist because of the same asymmetry: SendGrid upserts contacts, so
// re-sending one that in fact succeeded costs a single idempotent request, whereas recording a
// contact SendGrid explicitly rejected as delivered loses it permanently and silently. Whenever
// the evidence is incomplete, the connector therefore fails rather than assumes.
const (
	// reasonUnattributableImport is recorded against an importing job that reconciliation could
	// not attribute to any import while SendGrid had rejected one of that upload's imports
	// outright. The rejected import's contacts are lost, the manifest does not say which jobs
	// they were, and so this job cannot be shown to belong to one of the imports that survived.
	// It is retried rather than aborted, because it may well have been delivered.
	reasonUnattributableImport = "sendgrid rejected an entire import of this upload and the affected contacts could not be identified, so this job's delivery could not be confirmed"

	// reasonMissingErrorsDocument is recorded against every job of an import that reported errored
	// contacts and then published no errors document to identify them with. Delivery can be shown
	// for none of that import's contacts and rejection for none of them either, so all of them are
	// re-upserted rather than half of them being guessed at.
	reasonMissingErrorsDocument = "sendgrid reported errored contacts for this import without publishing an errors document, so the affected contacts could not be identified"

	// reasonUnresolvableContact is recorded against an importing job whose own payload no longer
	// yields a contact identifier. Its identifier cannot be computed, so it cannot be compared
	// against the errors document in either direction and it can never be shown to be absent
	// from it.
	reasonUnresolvableContact = "this job's contact identifier could not be re-derived during reconciliation, so its delivery could not be confirmed"

	// reasonUnattributableRows is recorded against every still-unresolved job of an import whose
	// errors document carried entries this connector could not attribute to a specific job: an
	// entry naming an identifier no importing job claimed, or an entry the tolerant parser could
	// not reduce to an identifier or a message at all.
	//
	// Such an entry is proof that SendGrid rejected a contact of this import WITHOUT saying which,
	// so the import can no longer be reconciled by exclusion: "no row named this job" stops
	// meaning "this job was delivered" the moment a row exists whose subject is unknown. The
	// affected import's remaining contacts are therefore re-upserted, at the cost of one
	// idempotent request each, rather than being reported as delivered on evidence that is known
	// to be incomplete. The jobs a row DID name keep their own specific reason, because failJob
	// never overwrites the first reason recorded for a job.
	reasonUnattributableRows = "sendgrid reported contact errors this connector could not attribute to specific jobs, so every contact of the affected import whose delivery was not confirmed is retried"
)

// reasonImportFailed is recorded against every job of an import SendGrid reported as failed, and
// it is the only reason in this connector that abandons a job on the strength of a provider
// response rather than of a local validation failure.
//
// It is terminal because SendGrid's own definition of the state is terminal - the import finished
// with all errors, or was entirely unprocessable - so an identical retry has nothing left to
// achieve. It is also exactly what the framework does when an upload produced a single import in
// that state: the poll response maps it to 400 and the batch router aborts the whole batch. This
// reason simply extends the same verdict to the correct SUBSET when one import of several failed
// while the others did not, which is the only reason per-import membership is persisted at all.
//
// This is deliberately NOT the per-row abort another connector in this tree applies to individual
// errored contacts: a single rejected contact is recoverable and stays in the retryable channel.
const reasonImportFailed = "sendgrid rejected this contact's entire import, reporting that it finished with all errors or was entirely unprocessable"

// maxReasonRunes bounds every provider-supplied message this connector records against a job or
// writes to a log, so that a verbose or hostile response cannot bloat the jobs database.
const maxReasonRunes = 512

// maxRowReasonRunes bounds the per-contact reason a provider errors-document row contributes to
// durable job metadata, and is deliberately TIGHTER than maxReasonRunes.
//
// maxReasonRunes bounds a whole-upload or whole-import diagnosis, of which there is one. This bounds
// text that is written once PER FAILED CONTACT, so a single errored import can persist it tens of
// thousands of times. A provider message long enough to be useful is short; one long enough to need
// this cap is carrying something other than an explanation, and the less of it that is copied into a
// database row the better.
const maxRowReasonRunes = 256

// The stable, connector-owned codes every provider-derived per-contact reason is prefixed with.
//
// The code, not the prose, is the part a downstream consumer should match on. Provider text is
// outside this connector's control - its wording can change without notice and it is the part that
// may carry customer data - so pinning a classification in front of it means alerting and triage
// never have to parse the message, and a reason whose provider half is redacted to nothing is still
// actionable.
const (
	reasonCodeRowRejected    = "SENDGRID_CONTACT_REJECTED"
	reasonCodeRowUnexplained = "SENDGRID_CONTACT_REJECTED_WITHOUT_MESSAGE"
	reasonCodeRowAmbiguous   = "SENDGRID_CONTACT_AMBIGUOUS"
)

// identifierRedactionPlaceholder replaces a contact identifier removed from provider text.
const identifierRedactionPlaceholder = "[redacted-identifier]"

// The sentinels a local contact rejection carries, so the caller can route the record to the right
// terminal reason without matching on message text.
//
// Each wraps the durable reason constant, so the reason an operator reads and the error this package
// passes around can never drift apart.
var (
	errFieldTooLong       = errors.New(reasonFieldTooLong)
	errTooManyFieldValues = errors.New(reasonTooManyFieldValues)
)

// The bounds this connector applies to the reserved contact fields before putting them on the wire.
//
// THESE ARE THIS CONNECTOR'S OWN BOUNDS, NOT A TRANSCRIPTION OF THE PROVIDER'S SCHEMA. SendGrid
// does publish a maxLength for each contact field, but this connector cannot verify those numbers
// at build time, and guessing them exactly would be worse than useless: a bound set one character
// tighter than the provider's would reject deliverable contacts terminally, and one set looser
// would not prevent the request rejection it exists to prevent.
//
// So each is set GENEROUSLY - at or beyond the widest value any real contact field plausibly holds
// - and the purpose is narrow: stop a value that is obviously not a contact field at all, such as
// a whole serialized document that arrived where a city name belonged, from being sent in a request
// carrying up to 30,000 other contacts that the provider would then refuse wholesale.
//
// maxEmailRunes is the one bound taken from a specification rather than chosen: 254 is the longest
// address an SMTP path may carry (RFC 5321), so a longer value is not a deliverable email under any
// provider's rules.
const (
	maxEmailRunes             = 254
	maxPhoneNumberIDRunes     = 100
	maxContactIdentifierRunes = 255
	maxContactNameRunes       = 255
	maxAddressLineRunes       = 255
	maxLocalityRunes          = 255
	maxPostalCodeRunes        = 100
	maxAlternateEmails        = 50
	maxCustomFieldsPerContact = 128
	maxCustomFieldValueRunes  = 2000
	maxListIDsPerContact      = 64
)

// Bounds applied to the errors document while it is parsed.
//
// The transport adapter already bounds how many BYTES are read; these bound what the parser is
// prepared to build out of them, so that a document within the byte budget still cannot turn
// into an unbounded number of rows or an unbounded nesting depth.
const (
	// maxErrorRows bounds how many rows are taken from an import IN TOTAL, not from each of its
	// documents: one upload can produce several SendGrid imports and therefore several errors
	// documents, and a per-document bound would let their number multiply the total. Each document
	// is parsed against what the earlier ones left of this allowance. It sits far above the 30,000
	// contacts one request can carry, so it can only be reached by documents that do not describe
	// a single import.
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
//
// Observability, by contrast, is DEFAULTED rather than demanded: a nil logger or a nil stats
// factory falls back to the no-op implementation, so an absent observability dependency cannot
// become a nil-pointer dereference on the first upload.
func NewManager(log logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*SendGridBulkUploader, error) {
	if destination == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	destinationConfig, err := parseDestinationConfig(destination)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = logger.NOP
	}
	if statsFactory == nil {
		statsFactory = stats.NOP
	}

	// The destination's identity is bound to the logger ONCE, here, rather than being passed
	// as a field at each of the several dozen call sites that log.
	//
	// Binding is what makes attribution total instead of best-effort: every line this connector
	// or its HTTP adapter emits carries destinationId and destinationType, including the ones
	// written from helpers that never receive a destination, and a line can no longer be missing
	// them because somebody forgot an argument. The typed obskit constructors are used so the
	// field NAMES match every other component in the process and the values stay queryable
	// rather than becoming free-form strings.
	//
	// destinationType comes from the destName constant, not from destination.Name or from the
	// destination definition, so the tag can never drift from the registered name even if an
	// operator renames the destination - the same rule the stat labels follow.
	sendGridLogger := log.Child("SendGridBulkUpload").Child("SendGridBulkUploader").
		Withn(obskit.DestinationID(destination.ID), obskit.DestinationType(destName))

	// The API key guard lives in the API service constructor, which is the single place that
	// owns the bearer credential; its error is propagated verbatim so the reason a
	// destination could not be constructed is reported exactly once. It is handed the PARSED
	// configuration rather than the destination, so the typed view stays the only reading of
	// the control plane's values.
	apiService, err := NewSendGridAPIService(destination.ID, destinationConfig, sendGridLogger, statsFactory)
	if err != nil {
		return nil, err
	}

	return &SendGridBulkUploader{
		Logger:            sendGridLogger,
		StatsFactory:      statsFactory,
		DestinationID:     destination.ID,
		DestinationConfig: destinationConfig,

		SendGridAPIService: apiService,

		// Every limit resolves BatchRouter.SENDGRID_BULK_UPLOAD.<key> and falls back to
		// BatchRouter.<key>, defaulting to the API's own documented limits and to the staging-line
		// limit derived from them. They are overridable so that chunking boundaries can be
		// exercised without materializing tens of thousands of contacts, and so that an operator
		// can tighten them without a release if SendGrid ever narrows the endpoint. Each accessor
		// normalizes and clamps whatever arrives here, so a nonsensical override cannot disable a
		// limit.
		MaxContactsPerRequest: int(common.GetBatchRouterConfigInt64(configKeyMaxContactsPerRequest, destName, defaultMaxContactsPerRequest)),
		MaxRequestBytes:       int(common.GetBatchRouterConfigInt64(configKeyMaxRequestBytes, destName, defaultMaxRequestBytes)),
		MaxBufferCapacity:     int(common.GetBatchRouterConfigInt64(configKeyMaxBufferCapacity, destName, defaultMaxBufferCapacity)),
		MaxImportsPerUpload:   int(common.GetBatchRouterConfigInt64(configKeyMaxImportsPerUpload, destName, defaultMaxImportsPerUpload)),
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
// Both codec failures are WRAPPED with %w rather than flattened with %v, so the codec's own
// error stays reachable through errors.Is and errors.As for a caller that needs to tell a
// malformed configuration value apart from an unsupported type. The two messages name the
// direction of the conversion separately, because "the control plane sent something this codec
// cannot encode" and "the control plane sent something that does not fit this struct" call for
// different corrective action.
//
// The trait-to-custom-field mapping is validated HERE rather than at first use, so a
// destination carrying an ambiguous or unusable mapping is rejected once, at construction,
// instead of silently dropping or non-deterministically overwriting custom field values on
// every batch for as long as it stays misconfigured.
func parseDestinationConfig(destination *backendconfig.DestinationT) (DestinationConfig, error) {
	var destinationConfig DestinationConfig
	jsonConfig, err := jsonrs.Marshal(destination.Config)
	if err != nil {
		// Wrapped with %w rather than rendered with %v, so that the codec's own error stays
		// inspectable with errors.Is and errors.As all the way up to the factory instead of
		// being flattened into a string at the first hop.
		return destinationConfig, fmt.Errorf("error in marshalling destination config: %w", err)
	}
	if err := jsonrs.Unmarshal(jsonConfig, &destinationConfig); err != nil {
		return destinationConfig, fmt.Errorf("error in unmarshalling destination config: %w", err)
	}
	normalizedMapping, err := validateCustomFieldsMapping(destinationConfig.CustomFieldsMapping)
	if err != nil {
		return DestinationConfig{}, err
	}
	destinationConfig.CustomFieldsMapping = normalizedMapping
	return destinationConfig, nil
}

// validateCustomFieldsMapping normalizes the operator-supplied trait-to-custom-field mapping
// and rejects every shape that cannot be applied deterministically.
//
// Three shapes are rejected outright rather than skipped:
//
//   - A BLANK TRAIT NAME names no trait, so there is nothing to read a value from. Skipping it
//     silently would hide a mapping the operator believes is in force.
//   - A BLANK FIELD ID names no SendGrid custom field. Sending one would put an empty key in
//     the custom_fields object, which SendGrid rejects for the whole contact, so a single blank
//     entry would fail every record in every batch.
//   - TWO TRAITS MAPPED TO THE SAME FIELD ID is ambiguous by construction. Both values are
//     candidates for one field and Go's map iteration order is deliberately randomized, so
//     whichever value won would differ between processes, between restarts, and even between
//     two batches in the same process. That is the worst available failure mode: it still
//     delivers data, so nothing looks broken, and it delivers DIFFERENT data every time.
//
// Keys are visited in sorted order so that a configuration with several problems always
// reports the same one first. Without that, the reported key would be drawn from a randomized
// iteration order and two runs against identical configuration could disagree about what is
// wrong with it.
//
// Both sides are trimmed, because a trait name or field ID pasted into a configuration form
// very often carries surrounding whitespace, and a normalized copy is returned so that every
// later read works from validated values and needs no per-entry defensiveness.
//
// A nil or empty mapping is VALID - custom fields are optional - and yields an empty map rather
// than nil, so that the returned value is always safe to read and range over without a further
// nil test at every call site.
func validateCustomFieldsMapping(mapping map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(mapping))
	if len(mapping) == 0 {
		return normalized, nil
	}
	// fieldIDOwners records which trait already claimed each field ID, so a duplicate can be
	// reported naming BOTH traits involved - which is the only form of the message an operator
	// can act on without going back to the configuration to work out what collided.
	fieldIDOwners := make(map[string]string, len(mapping))
	for _, traitName := range slices.Sorted(maps.Keys(mapping)) {
		normalizedTrait := strings.TrimSpace(traitName)
		if normalizedTrait == "" {
			return nil, fmt.Errorf("invalid %s: a trait name is empty", configFieldCustomFieldsMapping)
		}
		normalizedFieldID := strings.TrimSpace(mapping[traitName])
		if normalizedFieldID == "" {
			return nil, fmt.Errorf("invalid %s: trait %q is mapped to an empty custom field id",
				configFieldCustomFieldsMapping, normalizedTrait)
		}
		if owner, duplicated := fieldIDOwners[normalizedFieldID]; duplicated {
			return nil, fmt.Errorf("invalid %s: traits %q and %q are both mapped to custom field id %q, which is ambiguous",
				configFieldCustomFieldsMapping, owner, normalizedTrait, normalizedFieldID)
		}
		if _, duplicated := normalized[normalizedTrait]; duplicated {
			// Reachable only through whitespace: "plan" and " plan" are distinct map keys but
			// normalize to the same trait, so one of the two values would be discarded.
			return nil, fmt.Errorf("invalid %s: trait %q is mapped more than once",
				configFieldCustomFieldsMapping, normalizedTrait)
		}
		fieldIDOwners[normalizedFieldID] = normalizedTrait
		normalized[normalizedTrait] = normalizedFieldID
	}
	return normalized, nil
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

// maxRequestBytes is the effective byte cap for one upsert's WHOLE serialized body: the
// {"list_ids":[...],"contacts":[...]} envelope, every list ID inside it, every contact and every
// separating comma. The envelope's exact cost is measured per list group and deducted from this
// budget before anything is packed, so no part of the body is charged twice and none is missed.
//
// A zero or negative field falls back to the documented default, and an override is CLAMPED to
// the documented ceiling for the same reason the element cap is: a larger value could only
// produce requests SendGrid rejects.
func (b *SendGridBulkUploader) maxRequestBytes() int {
	if b.MaxRequestBytes <= 0 {
		return defaultMaxRequestBytes
	}
	return min(b.MaxRequestBytes, sendGridMaxRequestBytes)
}

// maxBufferCapacity is the effective limit on one staging-file line, validated ONCE per read
// before it is handed to bufio.
//
// The configured value is remote input as far as this code is concerned: it arrives from
// BatchRouter.SENDGRID_BULK_UPLOAD.maxBufferCapacity, falls back to BatchRouter.maxBufferCapacity,
// and nothing upstream constrains it. bufio.Scanner.Buffer takes what it is given verbatim, so all
// three degenerate cases are closed here rather than left to the scanner:
//
//   - A NON-POSITIVE value (an unset-but-present key, a typo, an explicit 0 or a negative
//     number) is not a small buffer, it is a nonsensical one: EVERY line would report "token too
//     long" and turn every batch into a retryable failure. It falls back to the derived default.
//   - A POSITIVE BUT TINY value is worse than no configuration at all, because every line longer
//     than it fails the scan and a scan failure aborts the whole read - a permanent retry loop
//     from one mistyped number. It is raised to bufio's own default token size.
//   - An ENORMOUS value is an allocation this connector must not be able to request inside a
//     shared batch router worker. It is lowered to the documented ceiling.
func (b *SendGridBulkUploader) maxBufferCapacity() int {
	if b.MaxBufferCapacity <= 0 {
		return defaultMaxBufferCapacity
	}
	return min(max(b.MaxBufferCapacity, minMaxBufferCapacity), maxBufferCapacityCeiling)
}

// maxImportsPerUpload is the effective number of SendGrid imports one upload may create, and
// therefore the worst-case number of status requests each later poll of that upload can cost.
//
// A zero or negative field falls back to the default, so an uploader built as a struct literal
// always has a usable budget, and an override is clamped to the ceiling so the poll cost stays
// bounded however the destination is configured.
func (b *SendGridBulkUploader) maxImportsPerUpload() int {
	if b.MaxImportsPerUpload <= 0 {
		return defaultMaxImportsPerUpload
	}
	return min(b.MaxImportsPerUpload, maxImportsPerUploadCeiling)
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
		Country: coalesce(firstString(address, "country"), firstString(traits, "country")),
		// Collected one past the limit on purpose, so a field carrying too many entries is still
		// recognisable as too many by validateContactFields instead of arriving silently shortened.
		AlternateEmails: stringSliceOf(firstResult(traits, "alternateEmails", "alternate_emails"), maxAlternateEmails+1),
	}
	customFields, skippedCustomFields := b.buildCustomFields(message, traits)
	contact.CustomFields = customFields
	if skippedCustomFields > 0 {
		// Counted and surfaced rather than dropped in silence: a mapped trait that never reaches
		// SendGrid is a configuration mistake an operator can only fix if they can see it.
		b.Logger.Warnn("[sendgrid bulk upload] skipping custom field values sendgrid cannot store",
			logger.NewIntField("skippedCustomFieldCount", int64(skippedCustomFields)))
	}

	if contact.Email == "" && contact.PhoneNumberID == "" && contact.ExternalID == "" && contact.AnonymousID == "" {
		return Contact{}, errors.New(reasonMissingIdentifier)
	}
	if err := validateContactFields(contact); err != nil {
		return Contact{}, err
	}
	return contact, nil
}

// contactRejectionCause names why buildContact refused a contact, as a fixed token.
//
// Derived from the wrapped sentinel rather than from the error's text, so the value is drawn from a
// closed set this package controls and can never carry a fragment of the event that produced it. It
// exists so a log line can say WHICH kind of rejection occurred without rendering the error.
func contactRejectionCause(err error) string {
	switch {
	case errors.Is(err, errFieldTooLong):
		return "fieldTooLong"
	case errors.Is(err, errTooManyFieldValues):
		return "tooManyFieldValues"
	default:
		return "noUsableIdentifier"
	}
}

// validateContactFields refuses a contact whose field values are outside what this connector sends.
//
// Applied AFTER the identifier check so the more specific diagnosis wins: a contact with no usable
// identifier is reported as exactly that rather than as whichever field happened also to be too
// long. The error wraps a sentinel so the caller can route the record to the right terminal reason,
// and it names the FIELD and the LIMIT but never the value - the value is customer data, and this
// text is written into durable job metadata.
func validateContactFields(contact Contact) error {
	for _, bound := range []struct {
		field string
		value string
		limit int
	}{
		{"email", contact.Email, maxEmailRunes},
		{"phone_number_id", contact.PhoneNumberID, maxPhoneNumberIDRunes},
		{"external_id", contact.ExternalID, maxContactIdentifierRunes},
		{"anonymous_id", contact.AnonymousID, maxContactIdentifierRunes},
		{"first_name", contact.FirstName, maxContactNameRunes},
		{"last_name", contact.LastName, maxContactNameRunes},
		{"address_line_1", contact.AddressLine1, maxAddressLineRunes},
		{"address_line_2", contact.AddressLine2, maxAddressLineRunes},
		{"city", contact.City, maxLocalityRunes},
		{"state_province_region", contact.StateProvinceRegion, maxLocalityRunes},
		{"postal_code", contact.PostalCode, maxPostalCodeRunes},
		{"country", contact.Country, maxLocalityRunes},
	} {
		if utf8.RuneCountInString(bound.value) > bound.limit {
			return fmt.Errorf("%w: %s is longer than %d characters", errFieldTooLong, bound.field, bound.limit)
		}
	}
	for _, alternate := range contact.AlternateEmails {
		if utf8.RuneCountInString(alternate) > maxEmailRunes {
			return fmt.Errorf("%w: an alternate_emails entry is longer than %d characters", errFieldTooLong, maxEmailRunes)
		}
	}
	if len(contact.AlternateEmails) > maxAlternateEmails {
		return fmt.Errorf("%w: alternate_emails carries more than %d entries", errTooManyFieldValues, maxAlternateEmails)
	}
	if len(contact.CustomFields) > maxCustomFieldsPerContact {
		return fmt.Errorf("%w: custom_fields carries more than %d entries", errTooManyFieldValues, maxCustomFieldsPerContact)
	}
	return nil
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
// The mapping itself needs NO per-entry defensiveness here. validateCustomFieldsMapping has
// already trimmed both sides, rejected blank trait names and blank field IDs, and rejected two
// traits claiming one field ID, so every entry reached below is non-blank and every field ID is
// written exactly once. That is deliberate: the previous shape skipped blank entries silently
// and let a duplicated field ID be resolved by Go's randomized map iteration order, which meant
// a misconfigured destination still delivered - just different data on each run. Failing at
// construction instead makes this loop's output a pure function of the event.
//
// It returns nil rather than an empty map when nothing maps, so that the custom_fields key is
// omitted from the request body entirely.
// buildCustomFields resolves the configured trait-to-custom-field mapping for one contact, and
// reports how many mapped values it refused.
//
// SendGrid custom fields hold a single scalar - text, a number or a date - so an object or an array
// is not a value the provider can store. Passing one through put raw JSON in the request, which the
// provider rejects for the WHOLE request rather than for the one field, so a single event with a
// nested trait failed up to 30,000 unrelated contacts. Refusing the value keeps the batch and costs
// only the field, and the count is returned so the refusal is visible instead of silent.
func (b *SendGridBulkUploader) buildCustomFields(message, traits gjson.Result) (map[string]any, int) {
	if len(b.DestinationConfig.CustomFieldsMapping) == 0 {
		return nil, 0
	}
	// One resolver per contact, so each source object is indexed AT MOST ONCE however many custom
	// fields are configured. Resolving each mapping independently re-indexed the same objects once
	// per mapping, which is work proportional to the mapping count times the event size for a
	// result that cannot change between mappings.
	resolver := newTraitResolver(message, traits)
	customFields := make(map[string]any, len(b.DestinationConfig.CustomFieldsMapping))
	skipped := 0
	for traitName, fieldID := range b.DestinationConfig.CustomFieldsMapping {
		if traitName == "" || fieldID == "" {
			continue
		}
		value := resolver.lookup(traitName)
		if !value.Exists() || value.Type == gjson.Null {
			continue
		}
		switch value.Type {
		case gjson.String:
			// Bounded here rather than by validateContactFields, because a custom field's target is
			// an opaque pre-created field ID and there is no per-field limit this connector could
			// know. One implausibly long value is refused; the rest of the contact still goes.
			if utf8.RuneCountInString(value.String()) > maxCustomFieldValueRunes {
				skipped++
				continue
			}
			customFields[fieldID] = value.String()
		case gjson.Number:
			customFields[fieldID] = value.Value()
		case gjson.True, gjson.False:
			// Rendered as text, because SendGrid custom fields have no boolean kind and a bare
			// JSON boolean is not a value any of the three kinds accepts.
			customFields[fieldID] = strconv.FormatBool(value.Bool())
		default:
			// gjson.JSON: an object or an array. Refused, never flattened - inventing a rendering
			// for it would guess at what the operator meant and put a guess on the wire.
			skipped++
		}
	}
	if len(customFields) == 0 {
		return nil, skipped
	}
	return customFields, skipped
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
		// Streamed and bounded rather than materialized: the array is provider-of-record only in
		// the sense that the event author controls it, so its length is not this connector's to
		// trust. maxListIDsPerContact is far beyond any real targeting - a contact addressed to
		// more than 64 lists is a mistake, not a campaign - and stopping there keeps one record's
		// declared targeting from sizing the work this function does.
		listIDs := make([]string, 0, 1)
		externalIDs.ForEach(func(_, entry gjson.Result) bool {
			if !strings.EqualFold(scalarString(entry.Get("type")), externalIDTypeListIDs) {
				return true
			}
			id := entry.Get("id")
			if id.IsArray() {
				listIDs = append(listIDs, stringSliceOf(id, maxListIDsPerContact-len(listIDs))...)
			} else if value := scalarString(id); value != "" {
				listIDs = append(listIDs, value)
			}
			return len(listIDs) < maxListIDsPerContact
		})
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

	// contacts, jobIDs and sizes are index-aligned: contacts[i] was produced by jobIDs[i] and
	// serializes to sizes[i] bytes. The alignment is what lets a single rejected chunk's jobs be
	// identified exactly.
	//
	// sizes is measured ONCE, while the staging file is read, with the same codec that serializes
	// the request body. Carrying it here is what lets the chunker pack against the byte cap without
	// serializing every contact a second time, and it is the same measurement the contact_size
	// histogram reports, so the metric and the packing can never disagree.
	contacts []Contact
	jobIDs   []int64
	sizes    []int
}

// stagedContacts is the outcome of reading one staging file.
type stagedContacts struct {
	// batches holds the readable contacts, grouped by their target list IDs in
	// first-appearance order so that the requests an upload issues are deterministic.
	batches []*contactBatch

	// rejectedJobIDs are jobs whose event could not yield a usable contact. The condition is
	// permanent, so these are reported on the terminal channel.
	rejectedJobIDs []int64

	// unsendableJobIDs are jobs whose contact was well formed and identifiable but carried a field
	// value outside what this connector will send. Kept apart from rejectedJobIDs so the durable
	// reason states the actual cause: "no usable identifier" and "a field is too long" are different
	// problems with different fixes, and reporting one as the other sends an operator looking in the
	// wrong place.
	unsendableJobIDs []int64

	// unsendableReason is the terminal reason the unsendable records carry, taken from the first
	// such record's sentinel so a length rejection is never reported as a count rejection.
	unsendableReason string

	// malformedJobIDs are jobs whose staging line was malformed but still carried a usable job
	// ID. The same bytes would fail identically on every retry, so these are reported on the
	// terminal channel, exactly like a contact that carries no identifier.
	malformedJobIDs []int64

	// unserializableJobIDs are jobs whose contact could not be serialized, so it could neither be
	// sized nor sent. These are reported on the RETRYABLE channel rather than the terminal one:
	// unlike a malformed record or a missing identifier, nothing about the record itself is known
	// to be invalid, and the batch router escalates to an abort on its own if the failure repeats.
	unserializableJobIDs []int64
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

	// Snake case, because "metadata.job_id" is the exact key the shared marshalling helper
	// writes - no camel-cased spelling of it is ever produced or accepted.
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
// pathFreeError renders a filesystem failure without disclosing the path it happened to.
//
// The staging file lives at an absolute path inside the pod - it names the deployment's directory
// layout and the batch router's internal file naming - and that path was reaching two places it does
// not belong: an operator-facing log line, and the FAILURE REASON persisted against every job of the
// batch, which is durable and far more widely readable than a log. A filesystem error carries the
// path inside itself (*os.PathError renders as "open /var/.../file: permission denied"), so wrapping
// with %w propagated it no matter how carefully the surrounding sentence was written.
//
// Error() returns the path-free summary, and Unwrap() keeps the original error reachable, so
// errors.Is and errors.As still classify the failure exactly as before. Nothing about the connector's
// error handling changes; only what the text says.
type pathFreeError struct {
	summary string
	err     error
}

func (e *pathFreeError) Error() string { return e.summary }

func (e *pathFreeError) Unwrap() error { return e.err }

// stagingFileError describes a staging-file failure by its CAUSE rather than by its path.
//
// The cause is what an operator acts on - a missing file means the batch router's own write failed, a
// permission error means the deployment's mount is wrong, an over-long line means the read buffer is
// configured too small - and each of those is diagnosable without knowing where the file sat. A
// basename token is offered separately, at the log sites only, for anyone who does need to correlate.
func stagingFileError(operation string, err error) error {
	cause := "an i/o error occurred"
	switch {
	case errors.Is(err, os.ErrNotExist):
		cause = "the file does not exist"
	case errors.Is(err, os.ErrPermission):
		cause = "permission was denied"
	case errors.Is(err, bufio.ErrTooLong):
		cause = "a line was longer than the configured read buffer"
	}
	return &pathFreeError{summary: operation + " the staging file: " + cause, err: err}
}

// stagingFileToken names a staging file without disclosing where it lives.
//
// The base name alone: it is what the batch router itself generates, it is enough to correlate a log
// line with a specific batch, and it carries none of the directory layout that the full path does.
func stagingFileToken(filePath string) string {
	if filePath == "" {
		return "(unnamed)"
	}
	return filepath.Base(filePath)
}

func (b *SendGridBulkUploader) readStagedContacts(filePath string, statLabels stats.Tags) (*stagedContacts, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, stagingFileError("opening", err)
	}
	defer func() { _ = file.Close() }()

	staged := &stagedContacts{batches: make([]*contactBatch, 0, 1)}
	// Indexes into staged.batches by the canonical form of a batch's list IDs, so that
	// grouping stays O(1) per line while the batches themselves keep first-appearance order.
	batchIndex := make(map[string]int)
	contactSizeStat := b.StatsFactory.NewTaggedStat("contact_size", stats.HistogramType, statLabels)

	scanner := bufio.NewScanner(file)
	// Resolved through the validated accessor rather than read inline, so a non-positive,
	// absurdly small or absurdly large configured value cannot reach bufio unchecked, and large
	// enough to read any record the per-record isolation below could still classify on its own.
	// The effective size is published because the accessor clamps, so the value in force can
	// legitimately differ from the value an operator configured.
	bufferCapacity := b.maxBufferCapacity()
	b.StatsFactory.NewTaggedStat("staging_file_buffer_capacity_bytes", stats.GaugeType, statLabels).
		Gauge(float64(bufferCapacity))
	scanner.Buffer(nil, bufferCapacity)
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
				return nil, stagingFileError("reading", err)
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
			switch {
			case errors.Is(err, errFieldTooLong), errors.Is(err, errTooManyFieldValues):
				staged.unsendableJobIDs = append(staged.unsendableJobIDs, jobID)
				if staged.unsendableReason == "" {
					// The sentinel's own text, so the reason names the cause that actually fired.
					staged.unsendableReason = reasonFieldTooLong
					if errors.Is(err, errTooManyFieldValues) {
						staged.unsendableReason = reasonTooManyFieldValues
					}
				}
			default:
				staged.rejectedJobIDs = append(staged.rejectedJobIDs, jobID)
			}
			b.Logger.Warnn("[sendgrid bulk upload] skipping event that cannot be upserted",
				logger.NewIntField("jobID", jobID),
				obskit.Error(err))
			continue
		}
		// Serialized exactly ONCE per contact, here. The length is both the value the contact_size
		// histogram reports and the size the chunker packs against, so the metric and the byte
		// budgeting are the same measurement rather than two independent ones - and the contact is
		// not serialized again until the request that carries it is built.
		contactJSON, err := jsonrs.Marshal(contact)
		if err != nil {
			// A contact built entirely from values decoded out of JSON should not fail to
			// serialize, and if it does it can be neither sized nor sent. Only this record is
			// affected, and it is reported as retryable rather than terminal: nothing proves the
			// record itself invalid, and the batch router gives up on its own if it keeps failing.
			staged.unserializableJobIDs = append(staged.unserializableJobIDs, jobID)
			b.Logger.Errorn("[sendgrid bulk upload] unable to serialize a contact",
				logger.NewIntField("jobID", jobID),
				obskit.Error(err))
			continue
		}
		contactSizeStat.Observe(float64(len(contactJSON)))

		listIDs := b.resolveListIDs(message)
		key := listIDsKey(listIDs)
		index, ok := batchIndex[key]
		if !ok {
			staged.batches = append(staged.batches, &contactBatch{
				listIDs:  listIDs,
				contacts: make([]Contact, 0, 1),
				jobIDs:   make([]int64, 0, 1),
				sizes:    make([]int, 0, 1),
			})
			index = len(staged.batches) - 1
			batchIndex[key] = index
		}
		staged.batches[index].contacts = append(staged.batches[index].contacts, contact)
		staged.batches[index].jobIDs = append(staged.batches[index].jobIDs, jobID)
		staged.batches[index].sizes = append(staged.batches[index].sizes, len(contactJSON))
	}
	if err := scanner.Err(); err != nil {
		return nil, stagingFileError("reading", err)
	}
	return staged, nil
}

// requestEnvelopeBytes measures the fixed cost of one upsert request body: everything SendGrid's
// byte ceiling is charged for that is not a contact.
//
// The ceiling applies to the WHOLE serialized body, and the list IDs are part of it. Their cost is
// MEASURED rather than reserved for by a constant because the number and length of the list IDs a
// destination targets is operator-controlled and unbounded by anything this connector knows: a flat
// reserve is either too small - a body within the element budget but over the wire limit, which
// SendGrid rejects - or too large, which splits batches that could have been sent whole. Measuring
// costs one marshal of an empty request per list group and is exact.
//
// It is measured with the same codec that serializes the request actually sent, so
// envelope + every contact + one comma between neighbours is exactly the number of bytes that go on
// the wire.
func requestEnvelopeBytes(listIDs []string) (int, error) {
	// A non-nil empty contact slice is deliberate: it serializes as "contacts":[], which is the
	// envelope with no elements in it, so the measurement includes both brackets and neither a
	// phantom element nor a phantom comma.
	envelope, err := jsonrs.Marshal(UpsertRequest{ListIDs: listIDs, Contacts: []Contact{}})
	if err != nil {
		return 0, fmt.Errorf("measuring the request envelope: %w", err)
	}
	return len(envelope), nil
}

// chunkBySizeAndElements splits one batch of contacts against BOTH request caps at once.
//
// maxBytes is the budget for the CONTACTS ALONE: the caller measures the request envelope exactly
// and deducts it first, so this function packs against what is genuinely left of the 6MB ceiling.
// Each contact is charged its serialized size - measured once, while the staging file was read -
// plus one byte for the comma that will separate it from its neighbour, and the current chunk is
// flushed as soon as adding the next contact would reach the budget or the chunk already holds the
// maximum number of elements. Charging a comma for the first element too, and comparing with >=,
// makes the packing strictly conservative: a chunk is always at least two bytes short of its
// budget, so envelope + contents can never reach the ceiling.
//
// The returned contact and job-ID chunk slices are INDEX-ALIGNED - they are cut at the same
// offsets - which is what allows exactly one rejected chunk's jobs to be reported while every
// accepted chunk keeps its importing state. They are SUBSLICES of the batch's own slices rather
// than copies, because the only thing done with a chunk is to serialize it into a request body;
// each is capped to its own length so that an accidental append could not reach into the next
// chunk's elements. An empty chunk is never emitted.
//
// A contact that is on its own larger than the whole budget can never fit in any chunk. Rather than
// being dropped, or being allowed to wedge the chunker by forming a chunk that is over the cap, it
// is removed and its job ID is returned separately so the caller can report that one job and
// deliver the rest.
//
// The caps are supplied by the caller, which guarantees both are positive.
func chunkBySizeAndElements(contacts []Contact, jobIDs []int64, sizes []int, maxBytes, maxElements int) ([][]Contact, [][]int64, []int64, error) {
	// Every contact is paired with the job that produced it and with its own measured size, so all
	// three inputs are read in lockstep below. The pairing is reported as an error rather than being
	// assumed, because indexing one slice with another's offset would panic inside a batch router
	// worker and take down far more than the one upload that was actually malformed.
	if len(contacts) != len(jobIDs) || len(contacts) != len(sizes) {
		return nil, nil, nil, fmt.Errorf("%d contacts cannot be paired with %d job ids and %d sizes",
			len(contacts), len(jobIDs), len(sizes))
	}

	// Contacts too large for any chunk are removed IN PLACE first, so that everything that remains
	// is packable and every chunk can then be handed out as a contiguous subslice. Compaction is a
	// single linear pass over slices this batch owns exclusively - they are built once while the
	// staging file is read and consumed once here - so it allocates nothing at all.
	var oversizedJobIDs []int64
	kept := 0
	for idx := range contacts {
		if sizes[idx]+1 >= maxBytes {
			oversizedJobIDs = append(oversizedJobIDs, jobIDs[idx])
			continue
		}
		if kept != idx {
			contacts[kept], jobIDs[kept], sizes[kept] = contacts[idx], jobIDs[idx], sizes[idx]
		}
		kept++
	}
	contacts, jobIDs, sizes = contacts[:kept], jobIDs[:kept], sizes[:kept]

	var (
		contactChunks [][]Contact
		jobIDChunks   [][]int64
	)
	start, chunkSize := 0, 0
	for idx := range contacts {
		contactSize := sizes[idx] + 1 // +1 for the comma that separates it from its neighbour
		if length := idx - start; length > 0 && (chunkSize+contactSize >= maxBytes || length == maxElements) {
			contactChunks = append(contactChunks, contacts[start:idx:idx])
			jobIDChunks = append(jobIDChunks, jobIDs[start:idx:idx])
			start, chunkSize = idx, 0
		}
		chunkSize += contactSize
	}
	if start < len(contacts) {
		contactChunks = append(contactChunks, contacts[start:len(contacts):len(contacts)])
		jobIDChunks = append(jobIDChunks, jobIDs[start:len(jobIDs):len(jobIDs)])
	}

	return contactChunks, jobIDChunks, oversizedJobIDs, nil
}

// Upload upserts one staging file's contacts into SendGrid.
//
// The staging file is read, the contacts are grouped by target list and chunked against both
// request caps, and one PUT is issued per chunk. Every accepted chunk contributes its job_id
// to the import identifier the batch router persists and its job IDs to the importing set;
// a chunk SendGrid rejected contributes its job IDs to the retryable set, whatever the status
// code, because the framework owns the decision to give up. Only the local per-record
// rejections - a contact with none of SendGrid's identifiers, a malformed staging record, a
// contact too large for any request - are terminal, and each aborts just its own job.
//
// The three outcome sets are kept DISJOINT, so a job is only ever reported once, and together
// they account for EVERY job in the batch: a job the staging file turned out not to contain is
// swept into the retryable set rather than left with no outcome at all, because the batch router
// writes a status only for the jobs an upload names.
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
		b.Logger.Errorn("[sendgrid bulk upload] upload called without a batch")
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
			logger.NewStringField("stagingFile", stagingFileToken(asyncDestStruct.FileName)))
		allJobIDs := lo.Union(failedJobIDs, asyncDestStruct.ImportingJobIDs)
		b.StatsFactory.NewTaggedStat("failed_job_count", stats.CountType, statLabels).Count(len(allJobIDs))
		return common.AsyncUploadOutput{
			FailedJobIDs: allJobIDs,
			// %v renders pathFreeError's summary, so the absolute staging path cannot reach this
			// value - and this value is the one persisted against every job of the batch, which is
			// why it, rather than the log line, was the disclosure that mattered. Sanitized as well,
			// because the reason is durable and every other durable reason passes the same gate.
			FailedReason:  sanitizeReason(fmt.Sprintf("BRT: Error in reading staging file: %v", err)),
			FailedCount:   len(allJobIDs),
			DestinationID: destinationID,
		}
	}

	var (
		importIDs      []string
		importingJobs  []int64
		abortedJobIDs  []int64
		deferredJobIDs []int64
		failureReasons []string
		abortReasons   []string
		// attemptedRequests counts upsert requests ISSUED, which is not the same as the number
		// of imports accepted: a chunk rejected with a rate limit or a 5xx is work this upload
		// performed but produces no import, so the two are tracked separately.
		attemptedRequests int
		// The two reasons a chunk can be deferred, counted apart so the durable failure reason
		// names the limit that actually bound rather than guessing at one of them.
		deferredForImportBudget   int
		deferredForManifestBudget int
		// The running size of the manifest's IDENTIFIER-ONLY form: the sum of the accepted import
		// identifiers plus the separators between them.
		//
		// This is what makes maxImportManifestBytes a HARD ceiling on what is persisted rather than
		// a preference. renderImportManifest degrades from full membership to identifiers alone when
		// the former will not fit, so bounding the identifier form here bounds every form it can
		// return. Bounding it only at render time was the defect: the fallback itself was unbounded,
		// so an upload creating many imports persisted a manifest many times the ceiling into the
		// status row of EVERY importing job.
		manifestIdentifierBytes int
		// Latched once an accepted identifier no longer fits the manifest budget, so that every
		// later chunk is deferred WITHOUT being sent.
		//
		// An identifier's length is only known once SendGrid answers, so the chunk that discovers
		// the budget is spent has already been upserted and is re-sent on the next batch - one
		// harmless duplicate against an upserting endpoint. Latching keeps it at exactly one per
		// upload instead of one per surplus chunk. A later, shorter identifier might have fitted,
		// but stopping at the first that does not is monotone and cannot oscillate, and the only
		// cost of stopping early is that those jobs are re-batched.
		manifestBudgetExhausted bool
	)
	// Which jobs each accepted import carries. This is the mapping SendGrid's per-import states
	// are useless without, and it is persisted with the identifiers because reconciliation runs
	// statelessly and cannot ask this process anything.
	importMembership := make(map[string][]int64)
	if len(staged.rejectedJobIDs) > 0 {
		abortedJobIDs = append(abortedJobIDs, staged.rejectedJobIDs...)
		abortReasons = append(abortReasons, reasonMissingIdentifier)
	}
	if len(staged.unsendableJobIDs) > 0 {
		abortedJobIDs = append(abortedJobIDs, staged.unsendableJobIDs...)
		abortReasons = append(abortReasons, staged.unsendableReason)
		b.StatsFactory.NewTaggedStat("unsendable_contact_count", stats.CountType, statLabels).Count(len(staged.unsendableJobIDs))
	}
	if len(staged.malformedJobIDs) > 0 {
		abortedJobIDs = append(abortedJobIDs, staged.malformedJobIDs...)
		abortReasons = append(abortReasons, reasonMalformedRecord)
		// Surfaced as a metric as well, so that a staging-file shape change is visible without
		// having to read logs.
		b.StatsFactory.NewTaggedStat("invalid_record_count", stats.CountType, statLabels).Count(len(staged.malformedJobIDs))
	}
	if len(staged.unserializableJobIDs) > 0 {
		// Retryable, not terminal: the record could not be sized or sent, but nothing proves it
		// invalid, so the batch router is left to decide when to give up on it.
		failedJobIDs = append(failedJobIDs, staged.unserializableJobIDs...)
		failureReasons = append(failureReasons, "BRT: Error in preparing contacts: the contact could not be serialized")
	}

	for _, batch := range staged.batches {
		// The envelope is measured before anything is packed, because SendGrid's 6MB ceiling is
		// charged for the whole body: the chunker is then given only what the list IDs and the
		// surrounding {"list_ids":[...],"contacts":[...]} braces leave behind, which is what keeps
		// the request that finally goes out inside the documented limit.
		envelopeBytes, err := requestEnvelopeBytes(batch.listIDs)
		if err != nil {
			// Measuring the envelope failed, which says nothing about SendGrid's availability, so
			// this group is reported as retryable rather than discarded.
			b.Logger.Errorn("[sendgrid bulk upload] unable to measure the request envelope",
				obskit.Error(err))
			failedJobIDs = append(failedJobIDs, batch.jobIDs...)
			failureReasons = append(failureReasons, fmt.Sprintf("BRT: Error in preparing contacts: %v", err))
			continue
		}
		contactBudget := b.maxRequestBytes() - envelopeBytes
		if contactBudget < minContactBudgetBytes {
			// The list IDs alone consume the request. Reporting this as a batch of contacts that
			// are each individually too large would name the wrong cause, and retrying would
			// rebuild exactly the same envelope, so the group is abandoned with the real reason.
			abortedJobIDs = append(abortedJobIDs, batch.jobIDs...)
			abortReasons = append(abortReasons, reasonListIDsTooLarge)
			b.Logger.Errorn("[sendgrid bulk upload] list ids consume the whole request budget",
				logger.NewIntField("listIDCount", int64(len(batch.listIDs))),
				logger.NewIntField("envelopeBytes", int64(envelopeBytes)),
				logger.NewIntField("maxRequestBytes", int64(b.maxRequestBytes())),
				logger.NewIntField("contactCount", int64(len(batch.jobIDs))))
			continue
		}

		contactChunks, jobIDChunks, oversizedJobIDs, err := chunkBySizeAndElements(
			batch.contacts, batch.jobIDs, batch.sizes, contactBudget, b.maxContactsPerRequest(),
		)
		if err != nil {
			// The batch's own slices are inconsistent, which says nothing about SendGrid's
			// availability, so the batch is reported as retryable rather than discarded.
			b.Logger.Errorn("[sendgrid bulk upload] unable to chunk contacts",
				obskit.Error(err))
			failedJobIDs = append(failedJobIDs, batch.jobIDs...)
			failureReasons = append(failureReasons, fmt.Sprintf("BRT: Error in preparing contacts: %v", err))
			continue
		}
		if len(oversizedJobIDs) > 0 {
			abortedJobIDs = append(abortedJobIDs, oversizedJobIDs...)
			abortReasons = append(abortReasons, reasonContactTooLarge)
			b.Logger.Errorn("[sendgrid bulk upload] contacts too large to upload in any batch",
				logger.NewIntField("contactCount", int64(len(oversizedJobIDs))))
		}

		for idx, contactChunk := range contactChunks {
			if manifestBudgetExhausted {
				// The manifest budget was spent by an earlier chunk of this upload, so this one is
				// NOT sent: an import whose identifier cannot be persisted could never be polled.
				// Deferred, never dropped, exactly as an over-budget import count is.
				deferredJobIDs = append(deferredJobIDs, jobIDChunks[idx]...)
				deferredForManifestBudget += len(jobIDChunks[idx])
				continue
			}
			if len(importIDs) >= b.maxImportsPerUpload() {
				// The import budget is spent, so this chunk is NOT sent. Every later poll of this
				// upload has to ask about each import it created, so accepting an unbounded number
				// of them here would hand that unbounded cost to the poll loop, which every
				// destination of this type shares.
				//
				// Deferred, never dropped: these jobs go on the retryable channel and the batch
				// router re-batches them on its next pass, which drains an oversized batch in
				// bounded steps.
				deferredJobIDs = append(deferredJobIDs, jobIDChunks[idx]...)
				deferredForImportBudget += len(jobIDChunks[idx])
				continue
			}
			// Counted before the call, not after, and separately from the imports that were
			// accepted. The two numbers answer different questions - "how many upsert requests
			// did this upload issue?" versus "how many of them SendGrid queued as imports" - and
			// conflating them hides exactly the case an operator most needs to see, because a
			// chunk rejected with 429 or 5xx never becomes an import and would simply vanish
			// from a count derived from the accepted ones.
			attemptedRequests++
			uploadResp, err := b.SendGridAPIService.UploadContacts(UpsertRequest{
				ListIDs:  batch.listIDs,
				Contacts: contactChunk,
			})
			if err != nil {
				// Retryable whatever SendGrid answered: the batch router owns the retry budget
				// and escalates to an abort itself once it is spent, so one rejected request
				// can never terminally discard a whole chunk of jobs here.
				failedJobIDs = append(failedJobIDs, jobIDChunks[idx]...)
				failureReasons = append(failureReasons, b.classifyUploadError(err, len(jobIDChunks[idx])))
				continue
			}
			importID := ""
			if uploadResp != nil {
				importID = strings.TrimSpace(uploadResp.JobID)
			}
			if importID == "" {
				// An accepted upsert that yields no job_id cannot be polled, so recording it as
				// importing would strand these jobs in the importing state forever. They are
				// reported as retryable instead: the endpoint upserts, so a repeated attempt is
				// harmless.
				b.Logger.Errorn("[sendgrid bulk upload] upload accepted without an import job id",
					logger.NewIntField("contactCount", int64(len(jobIDChunks[idx]))))
				failedJobIDs = append(failedJobIDs, jobIDChunks[idx]...)
				failureReasons = append(failureReasons, "BRT: SendGrid accepted the upload without returning an import job id")
				continue
			}
			if !isPersistableImportID(importID) {
				// The identifier cannot be written into the import manifest without making it
				// ambiguous, so neither this import nor any other in the same upload could be
				// polled or reconciled reliably afterwards. Refusing it here - rather than
				// persisting a manifest that cannot be read back - keeps the encoding's one
				// assumption enforced instead of merely documented. Retryable, for the same
				// reason as an absent identifier: the endpoint upserts.
				b.Logger.Errorn("[sendgrid bulk upload] upload accepted with an import job id that cannot be persisted",
					logger.NewIntField("importIdLength", int64(len(importID))),
					logger.NewIntField("contactCount", int64(len(jobIDChunks[idx]))))
				failedJobIDs = append(failedJobIDs, jobIDChunks[idx]...)
				failureReasons = append(failureReasons, "BRT: SendGrid returned an import job id that cannot be persisted for polling")
				continue
			}
			// De-duplicated, because SendGrid is free to answer two chunks with the same import
			// job_id; the membership of both chunks then belongs to that one import. A repeat costs
			// no manifest bytes, which is why the budget is charged only for a NEW identifier.
			if !lo.Contains(importIDs, importID) {
				entryBytes := len(importID)
				if len(importIDs) > 0 {
					entryBytes += len(importManifestSeparator)
				}
				if manifestIdentifierBytes+entryBytes > maxImportManifestBytes {
					// The manifest budget is spent. These contacts HAVE been upserted, but recording
					// the import would push the persisted parameter past the ceiling it is
					// guaranteed to respect, and dropping the identifier instead would be far worse:
					// the import would never be polled, and if every import this upload did record
					// completed cleanly the batch router would mark these jobs delivered on the
					// strength of imports that never carried them.
					//
					// So the chunk is deferred exactly as an over-budget import count is deferred:
					// its jobs go on the retryable channel and the batch router re-batches them.
					// The cost is one duplicate upsert per deferred chunk, which is harmless because
					// the endpoint upserts, and it buys a hard bound on what every importing job's
					// status row has to store.
					deferredJobIDs = append(deferredJobIDs, jobIDChunks[idx]...)
					deferredForManifestBudget += len(jobIDChunks[idx])
					manifestBudgetExhausted = true
					continue
				}
				manifestIdentifierBytes += entryBytes
				importIDs = append(importIDs, importID)
			}
			importMembership[importID] = append(importMembership[importID], jobIDChunks[idx]...)
			importingJobs = append(importingJobs, jobIDChunks[idx]...)
		}
	}

	if len(deferredJobIDs) > 0 {
		// Reported once, after every list group has been packed, so the reason states the whole
		// upload's shortfall rather than one group's - and it names the limit that actually bound,
		// because the two call for different operator responses.
		failedJobIDs = append(failedJobIDs, deferredJobIDs...)
		if deferredForImportBudget > 0 {
			failureReasons = append(failureReasons, fmt.Sprintf(
				"BRT: this batch needed more than the %d sendgrid imports one upload may create, so %d job(s) were deferred to the next batch",
				b.maxImportsPerUpload(), deferredForImportBudget))
		}
		if deferredForManifestBudget > 0 {
			failureReasons = append(failureReasons, fmt.Sprintf(
				"BRT: this batch's sendgrid imports no longer fit the %d byte import manifest persisted against every job, so %d job(s) were deferred to the next batch",
				maxImportManifestBytes, deferredForManifestBudget))
		}
		b.StatsFactory.NewTaggedStat("deferred_job_count", stats.CountType, statLabels).Count(len(deferredJobIDs))
		b.Logger.Errorn("[sendgrid bulk upload] batch needed more imports than one upload may record",
			logger.NewIntField("maxImportsPerUpload", int64(b.maxImportsPerUpload())),
			logger.NewIntField("maxImportManifestBytes", int64(maxImportManifestBytes)),
			logger.NewIntField("importCount", int64(len(importIDs))),
			logger.NewIntField("manifestIdentifierBytes", int64(manifestIdentifierBytes)),
			logger.NewIntField("deferredForImportBudget", int64(deferredForImportBudget)),
			logger.NewIntField("deferredForManifestBudget", int64(deferredForManifestBudget)),
			logger.NewIntField("deferredCount", int64(len(deferredJobIDs))))
	}

	output := common.AsyncUploadOutput{DestinationID: destinationID}
	importingJobs = lo.Uniq(importingJobs)
	if len(importingJobs) > 0 {
		// The count is computed BEFORE the import parameters are marshalled, so that what is
		// persisted is the number of jobs actually accepted rather than a value derived later.
		importCount := len(importingJobs)
		// Every accepted import together with the jobs it carries, rendered as one string so
		// that the batch router's gjson read of "importId" and the string-typed
		// common.AsyncPoll.ImportId both keep working unchanged. The renderer degrades to the
		// identifier-only form rather than growing without bound; see maxImportManifestBytes.
		importManifest := renderImportManifest(importIDs, importMembership)
		if b.Logger.IsDebugLevel() {
			b.Logger.Debugn("[sendgrid bulk upload] persisting the import manifest",
				logger.NewIntField("importCount", int64(len(importIDs))),
				logger.NewIntField("manifestBytes", int64(len(importManifest))))
		}
		var (
			importParameters []byte
			err              error
		)
		if importManifest == "" {
			// Unreachable while Upload charges every accepted identifier against
			// maxImportManifestBytes, and asserted rather than assumed: the renderer returns the
			// empty string only when not even the identifier-only form fits the ceiling, and
			// persisting past the ceiling or dropping identifiers are both worse outcomes than
			// retrying. Handled on the same path as a marshal failure below.
			err = fmt.Errorf("the manifest for %d import(s) does not fit the %d byte budget persisted against every job",
				len(importIDs), maxImportManifestBytes)
		} else {
			importParameters, err = jsonrs.Marshal(common.ImportParameters{
				ImportId:    importManifest,
				ImportCount: importCount,
			})
		}
		if err != nil {
			// Without persisted import parameters the batch router could never poll this
			// import, and the jobs would sit in the importing state forever. Reporting them as
			// retryable trades a duplicate upsert - which is harmless, because the endpoint
			// upserts - for jobs that can never be resolved.
			b.Logger.Errorn("[sendgrid bulk upload] unable to persist import parameters",
				obskit.Error(err))
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

	// Completeness sweep: the batch router writes a status ONLY for the jobs this output names,
	// so a job in the batch that landed in none of the three sets would silently receive no
	// status at all and never be resolved. That should not happen - the router derives the batch
	// from the very lines it wrote - but "should not happen" is exactly the kind of assumption
	// that leaves jobs stranded when a staging file is truncated, or holds nothing but blank
	// lines, so the gap is closed explicitly rather than assumed away. The sweep is retryable,
	// because a missing line says nothing permanent about the job that produced it, and it is
	// disjoint from the other three sets by construction.
	if unaccountedJobIDs, _ := lo.Difference(
		lo.Uniq(asyncDestStruct.ImportingJobIDs),
		lo.Union(output.ImportingJobIDs, failedJobIDs, abortedJobIDs),
	); len(unaccountedJobIDs) > 0 {
		b.Logger.Errorn("[sendgrid bulk upload] jobs in the batch were not present in the staging file",
			logger.NewStringField("stagingFile", stagingFileToken(asyncDestStruct.FileName)),
			logger.NewIntField("unaccountedCount", int64(len(unaccountedJobIDs))))
		failedJobIDs = append(failedJobIDs, unaccountedJobIDs...)
		failureReasons = append(failureReasons, fmt.Sprintf(
			"BRT: %d job(s) in this batch were not present in the staging file and could not be uploaded",
			len(unaccountedJobIDs)))
	}
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
	// requestCount is the number of upsert requests ATTEMPTED, and acceptedRequestCount the
	// number SendGrid answered with a job_id. They differ by exactly the chunks that were
	// rejected - a rate limit, a 5xx, a transport failure - so reporting both is what makes the
	// summary self-consistent with failedCount instead of silently under-counting the work done.
	b.Logger.Infon("[sendgrid bulk upload] upload finished",
		logger.NewIntField("importCount", int64(len(output.ImportingJobIDs))),
		logger.NewIntField("failedCount", int64(len(failedJobIDs))),
		logger.NewIntField("abortedCount", int64(len(abortedJobIDs))),
		logger.NewIntField("requestCount", int64(attemptedRequests)),
		logger.NewIntField("acceptedRequestCount", int64(len(importIDs))))

	return output
}

// classifyUploadError logs one rejected chunk's failure and renders the retryable reason
// recorded against that chunk's jobs.
//
// Every provider failure uses the RETRYABLE channel - a rate limit, a rejected request, an
// authorization failure, a 5xx, a transport error, a timeout - and the batch router alone decides
// when to give up, escalating to an abort once its retry budget is exhausted. Only this
// connector's LOCAL per-record validation failures are terminal, and the caller records those one
// job at a time. This method therefore returns a reason and nothing else: there is no terminal
// branch for a caller to act on.
//
// A rate limit is told apart from the rest so that the advertised reset window reaches the
// operator-facing reason and the log line is a warning rather than an error. Every other failure
// carries its status code as its own log field, so an authorization failure can be told from a
// provider outage without parsing the message.
//
// This method is the SINGLE OWNER of failure logging for the upload path. The HTTP adapter that
// produced err records the same conditions at debug level with transport metadata only, and
// deliberately does not warn or error, so one rejected chunk yields exactly one warning or error
// line - the one written here, which is also the only one that knows the destination and how many
// jobs the chunk carried.
//
// The provider's own words appear ONLY in the returned reason, which the batch router persists
// against the affected jobs. The log lines carry this connector's own fixed classification and
// numbers instead, because err may render a SendGrid message that quotes the contacts that were
// submitted - their email addresses, phone numbers and mapped custom-field values - and the log
// stream is neither scoped to those deliveries nor retained on their terms.
//
// The destination is not passed in: it is bound to the logger at construction, so it is already
// on every line this method writes.
func (b *SendGridBulkUploader) classifyUploadError(err error, chunkJobCount int) string {
	var rateLimitErr *RateLimitError
	if errors.As(err, &rateLimitErr) {
		// The reset window, the limit and the remaining quota are all rendered by the error
		// itself, so the reason an operator reads carries the advertised window verbatim.
		//
		// Routed through the gate even though every field the error renders was already
		// validated or sanitized when the adapter built it. This reason is PERSISTED as the
		// job's failure reason, and gating the final string is what makes that safety a
		// property of this sink rather than an inherited assumption about how the error was
		// constructed - so a field added to RateLimitError later cannot quietly become the leak.
		// It also bounds the length, which the error's own rendering does not.
		reason := sanitizeReason(fmt.Sprintf("BRT: SendGrid rate limited the upload, jobs will be retried: %s", rateLimitErr.Error()))
		// Every field is this connector's own: the classification is a constant from this
		// package, the three rate-limit numbers were parsed as integers, and RetryAfter was
		// re-rendered by parseRetryAfterHeader as plain seconds or a canonical HTTP date. The
		// error object itself is NOT logged, because its message is SendGrid's.
		b.Logger.Warnn("[sendgrid bulk upload] rate limited while uploading contacts",
			logger.NewStringField("errorClass", errorClassRateLimited),
			logger.NewIntField("statusCode", int64(rateLimitErr.StatusCode)),
			logger.NewStringField("retryAfter", rateLimitErr.RetryAfter),
			logger.NewIntField("rateLimitResetAt", rateLimitErr.ResetAt),
			logger.NewIntField("rateLimitLimit", int64(rateLimitErr.Limit)),
			logger.NewIntField("rateLimitRemaining", int64(rateLimitErr.Remaining)),
			logger.NewIntField("chunkJobCount", int64(chunkJobCount)))
		return reason
	}

	// errorClass and statusCodeOf are what replace a rendered error here. Between them they
	// separate every failure mode an operator needs to tell apart - a rejected request and its
	// exact HTTP status, a timeout, a shutdown, a transport or decode fault - without a single
	// character of provider text. statusCodeOf reports zero when the failure never reached
	// SendGrid at all, which is itself diagnostic.
	b.Logger.Errorn("[sendgrid bulk upload] unable to upload contacts",
		logger.NewStringField("errorClass", errorClass(err)),
		logger.NewIntField("statusCode", statusCodeOf(err)),
		logger.NewIntField("chunkJobCount", int64(chunkJobCount)))

	// Gated for the same reason, and here it matters more: this is the CATCH-ALL branch, so the
	// error it renders may be an *APIError sanitized at construction, a transport failure, a
	// decode failure, or something a future change introduces. Sanitizing the rendered reason
	// rather than trusting each possible error type is what keeps the guarantee true for all of
	// them at once.
	return sanitizeReason(fmt.Sprintf("BRT: Error in Uploading contacts: %v", err))
}

// Poll reads the state of the import an upload produced and maps it onto the batch router's
// poll contract.
//
// It is a single status read per import - the batch router owns the polling cadence, so this
// method never waits, never sleeps and never loops until an import settles.
//
// The number of provider requests one poll costs is bounded, and small in the common case. It stops
// at the FIRST import still pending, because pending decides the whole poll on its own; it returns
// on the first status request that errors or is rate limited; and the number of imports an upload
// may create is capped when the upload is made, which is the only place that can control it. Those
// three together matter because the batch router polls the destinations of one type SERIALLY in a
// single goroutine, so a poll that lingers delays every other destination of this type.
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
// One upload can hold SEVERAL imports, and the states above are per import, so the mapping is
// applied to the batch as a whole in exactly one way: the table's rows are evaluated for every
// import, and the STRONGEST outcome wins - any import still pending keeps the whole batch
// polling, every import failed is the terminal 400, and any mixture that includes a rejected or
// an errored import goes to reconciliation. The mixture is the case that makes per-import
// membership necessary: aborting the batch because one import of three was rejected would
// discard the other two imports' delivered contacts, while succeeding the batch by exclusion
// against only the errored import's document would silently deliver the rejected import's
// contacts. Reconciliation is therefore handed every import's own state, errors document and job
// membership through FailedJobParameters, and settles each of them on its own terms.
//
// HasWarning and WarningJobParameters are never set: SendGrid has no warning tier.
func (b *SendGridBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	manifest := parseImportManifest(pollInput.ImportId)
	importIDs := manifest.importIDs
	if len(importIDs) == 0 {
		// Retryable rather than terminal: the batch router escalates to an abort by itself
		// once its retry budget is exhausted, so this cannot strand the jobs, and treating a
		// missing identifier as terminal here would abort jobs SendGrid may well have accepted.
		b.Logger.Errorn("[sendgrid bulk upload] poll requested without an import id")
		return common.PollStatusResponse{
			StatusCode: http.StatusInternalServerError,
			Complete:   false,
			Error:      "no sendgrid import id was persisted for this upload",
		}
	}

	var (
		errorsURLs   errorsURLSet
		details      []string
		outcomes     []importOutcome
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
		// Logged as named fields rather than as a rendering of the whole response.
		//
		// A "%+v" dump was the wrong shape twice over. It prints EVERY field the struct has, so
		// the errors document URL went to the log in full - and SendGrid may serve that document
		// from object storage through a PRE-SIGNED URL whose query string IS the credential that
		// authorizes the download, which would put a live secret into the log stream. It also
		// grows silently as fields are added to the response type, so no future field could be
		// reviewed before it started being logged. Naming each field fixes both: the counters an
		// operator actually needs are here explicitly, and the URL appears only as a query-free
		// reference. The raw URL still reaches reconciliation, but only through
		// FailedJobParameters.
		if b.Logger.IsDebugLevel() {
			b.Logger.Debugn("[sendgrid bulk upload] import status",
				logger.NewStringField("importId", importID),
				// Normalized rather than logged verbatim: the value arrives inside a SendGrid
				// response, so it is provider-controlled text. Mapping it through this
				// connector's closed set of states keeps the field meaningful while making it
				// impossible for the log stream to carry anything SendGrid chose.
				logger.NewStringField("status", normalizeImportStatus(importStatus.Status)),
				logger.NewIntField("requestedCount", int64(importStatus.Results.RequestedCount)),
				logger.NewIntField("createdCount", int64(importStatus.Results.CreatedCount)),
				logger.NewIntField("updatedCount", int64(importStatus.Results.UpdatedCount)),
				logger.NewIntField("erroredCount", int64(importStatus.Results.ErroredCount)),
				logger.NewStringField("errorsDocument", redactedURLReference(importStatus.Results.ErrorsURL)))
		}

		switch strings.ToLower(strings.TrimSpace(importStatus.Status)) {
		case importStatusPending:
			// RETURNED IMMEDIATELY, without asking about the remaining imports. Pending already
			// decides the whole poll - an upload is in progress until every one of its imports has
			// settled, and this response writes no job status at all - so the later imports could
			// only be asked about to have their answers discarded. Stopping here is what makes the
			// common case, an upload still being processed, cost ONE provider request however many
			// imports it created; the poll loop is shared by every destination of this type, and it
			// runs them one after another, so a poll that lingers delays all of them.
			//
			// Nothing is lost by not looking: no state is written, and the very next poll asks
			// again, by which time this import may have settled and the rest can be classified.
			return common.PollStatusResponse{
				StatusCode: http.StatusOK,
				InProgress: true,
			}
		case importStatusCompleted:
			if importStatus.Results.ErroredCount > 0 {
				erroredTotal++
				errorsURLs.add(importStatus)
				details = append(details, describeImport(importID, importStatus))
				// Recorded as errored rather than as completed, so that reconciliation acts on
				// the state's MEANING - some contacts were rejected - without having to re-apply
				// the defensive reading of errored_count that this branch exists for.
				outcomes = append(outcomes, manifest.outcomeOf(importID, importStatusErrored, importStatus))
				break
			}
			// A clean import is recorded too, even though it contributes nothing to the poll
			// verdict. Reconciliation only reaches this far when some OTHER import of the same
			// upload was rejected or errored, and it can only clear this one's jobs if it is
			// told they belong to an import that finished cleanly.
			outcomes = append(outcomes, manifest.outcomeOf(importID, importStatusCompleted, importStatus))
		case importStatusErrored:
			erroredTotal++
			errorsURLs.add(importStatus)
			details = append(details, describeImport(importID, importStatus))
			outcomes = append(outcomes, manifest.outcomeOf(importID, importStatusErrored, importStatus))
		case importStatusFailed:
			failedTotal++
			errorsURLs.add(importStatus)
			details = append(details, describeImport(importID, importStatus))
			outcomes = append(outcomes, manifest.outcomeOf(importID, importStatusFailed, importStatus))
		default:
			// An unrecognized state is retryable, not terminal: SendGrid may have introduced
			// a value this connector has not been taught, and guessing at its meaning could
			// mark live contacts delivered or abort them.
			// The unrecognized VALUE is not logged - it is provider-controlled text - but it is
			// not lost either: it reaches the operator through PollStatusResponse.Error below,
			// which the batch router persists against every job of this import, and the counter
			// makes the condition alertable without depending on log inspection at all. A metric
			// tag is deliberately not derived from the value: its cardinality would then be
			// SendGrid's to choose.
			b.StatsFactory.NewTaggedStat("unrecognized_import_status_count", stats.CountType, b.statLabels(b.DestinationID)).Increment()
			b.Logger.Errorn("[sendgrid bulk upload] unrecognized import status",
				logger.NewStringField("importId", importID),
				logger.NewStringField("status", importStatusUnrecognized))
			return common.PollStatusResponse{
				StatusCode: http.StatusInternalServerError,
				Complete:   false,
				Error:      sanitizeReason(fmt.Sprintf("Unknown status: %s", importStatus.Status)),
			}
		}
	}

	if failedTotal > 0 && failedTotal == len(importIDs) {
		// Every import was rejected outright, so the whole batch is terminally lost and the
		// framework's abort path is the honest outcome. When only SOME imports failed the
		// mapping deliberately falls through to reconciliation instead, which aborts precisely
		// the rejected imports' jobs rather than the whole batch.
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
			// Every import's own state, its errors document URL verbatim - SendGrid supplies the
			// URL itself, and reconstructing one would be a guess - and the jobs it carried, so
			// that reconciliation settles each import on its own terms instead of pooling states
			// that mean different things. The URLs are de-duplicated by the ordered set they were
			// collected in. The field is handed straight to GetUploadStats in memory and is never
			// persisted, so it can carry the raw URLs; GetUploadStats falls back to re-reading the
			// import statuses when it arrives empty.
			FailedJobParameters: b.renderImportOutcomes(outcomes, errorsURLs.values()),
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
		// Connector-owned fields only. *RateLimitError renders SendGrid's own message, so the
		// error object is not logged; the message reaches the jobs through the Error field of
		// the response this method returns.
		b.Logger.Warnn("[sendgrid bulk upload] rate limited while polling import status",
			logger.NewStringField("errorClass", errorClassRateLimited),
			logger.NewIntField("statusCode", int64(rateLimitErr.StatusCode)),
			logger.NewStringField("retryAfter", rateLimitErr.RetryAfter),
			logger.NewIntField("rateLimitResetAt", rateLimitErr.ResetAt),
			logger.NewStringField("importId", importID))
		return common.PollStatusResponse{
			StatusCode: http.StatusTooManyRequests,
			Complete:   false,
			Error:      sanitizeReason(rateLimitErr.Error()),
		}
	}
	// The rendered error is replaced by this connector's own classification and the HTTP status,
	// which together separate an expired key from a provider outage from a timeout without
	// carrying a word of SendGrid's text. The text itself reaches the jobs through the Error
	// field of the response this method returns.
	b.Logger.Errorn("[sendgrid bulk upload] unable to read import status",
		logger.NewStringField("errorClass", errorClass(err)),
		logger.NewIntField("statusCode", statusCodeOf(err)),
		logger.NewStringField("importId", importID))
	return common.PollStatusResponse{
		StatusCode: http.StatusInternalServerError,
		Complete:   false,
		Error:      sanitizeReason(err.Error()),
	}
}

// GetUploadStats reconciles a partially errored upload, so that the contacts SendGrid could not
// process are retried while the rest are marked delivered. One upload can therefore yield failed,
// aborted and succeeded jobs at once, which is the whole reason this method exists.
//
// Reconciliation is PER IMPORT, because SendGrid's states are per import and they do not mean the
// same thing:
//
//	failed    -> that import's own jobs are ABORTED terminally. SendGrid defines the state as
//	             finished with all errors or entirely unprocessable, which is precisely what the
//	             framework aborts a whole batch for when an upload produced a single import; the
//	             same verdict simply has to reach the right SUBSET when other imports survived.
//	errored   -> that import's errored ROWS are failed, and its remaining jobs are cleared.
//	completed -> that import's jobs are cleared on the strength of the import's own state.
//
// Pooling those states would be wrong in both directions: aborting the batch because one import
// of three was rejected discards two imports' delivered contacts, and clearing the batch by
// exclusion against only the errored import's document silently delivers the rejected import's
// contacts. Membership is what makes the distinction possible, and it is read from the import
// manifest Upload persisted.
//
// The errored ROWS go to the FAILED channel, not the aborted one. A per-row error - a rejected
// address, a value SendGrid would not take - is recoverable, and the failed channel is the
// batch router's retryable one, whereas the aborted channel is terminal. The only terminal
// outcome here is an import SendGrid itself declared permanently failed.
//
// The reconciliation is STATELESS: it re-derives every contact identifier from the importing
// jobs it is handed rather than from anything cached during the upload. That is deliberate and
// load-bearing, because this method may well run in a different process invocation, or on a
// different pod, from the Upload that produced the import - any cached lookup would simply be
// missing after a restart, and a connector that depended on one would then report failed
// contacts as delivered.
//
// A non-200 status is reserved for the cases in which the errors document could not be
// OBTAINED OR UNDERSTOOD AT ALL: the import parameters cannot be parsed, the document cannot be
// fetched, or the fetched bytes match none of the shapes the parser accepts. Those are the only
// situations in which no reconciliation whatsoever is possible, and the batch router retries them.
//
// A row the document holds but this method cannot attribute - one the parser did not recognize,
// or one whose identifier resolves to no importing job - is COUNTED AND LOGGED, and nothing else.
// It is emphatically not turned into a verdict on jobs it never named: an unattributable row is
// evidence about the DOCUMENT, not about any particular contact, and the one thing that must
// never follow from it is a blanket failure of an entire import's jobs. That was the previous
// behavior and it was wrong twice over - it retried thousands of contacts SendGrid had accepted
// because one row was unreadable, and it left SucceededKeys empty for a batch that had almost
// entirely succeeded. The counters and warnings are what make such a row visible, and they turn a
// change in the undocumented document shape into a metric rather than into churn.
//
// Where evidence genuinely IS missing, the response is scoped to the jobs it is missing about and
// is always retryable. SendGrid upserts contacts, so re-sending one that in fact succeeded costs a
// single idempotent request, whereas recording a contact SendGrid rejected as delivered loses it
// permanently and silently; the ambiguous-identifier branch below makes the same trade, failing
// every candidate rather than guessing between them.
//
// Note the status code every one of those branches uses: 200, not 500. A conservative verdict must
// NOT be expressed as a non-200, because the batch router writes NO job status at all when this
// method does that (handle_async.go) and its poll route has no retry budget that could escalate -
// every job would stay importing indefinitely and the destination would stop accepting new work.
// Returning 200 with the unresolved jobs in FailedKeys is what makes the safe verdict also a
// terminating one: the router records them as retryable failures, retries them, and aborts them
// itself if the retries keep failing.
//
// Every importing job is guaranteed to leave this method in exactly one of FailedKeys,
// AbortedKeys or SucceededKeys. That is not a nicety: a job the response does not name receives no
// status from the router at all and stays importing forever.
func (b *SendGridBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	outcomes, response := b.resolveImportOutcomes(input)
	if len(outcomes) == 0 {
		return response
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

	// The importing jobs are indexed BEFORE the first document is fetched, because each document
	// is attributed as soon as it is parsed rather than being accumulated: the index is what
	// attribution needs, so it has to exist first. That ordering is what keeps peak memory at ONE
	// document's rows however many documents an import published, and it costs one linear pass
	// that the common case - a document that fetches and parses - performs anyway.
	//
	// The index is also the single population every branch below works from, which is what lets
	// the completeness guarantee be stated over it: it recognises each importing job exactly once
	// and skips a nil entry rather than dereferencing it, because this method runs inside a batch
	// router worker shared by every destination in the process.
	index := b.buildImportingIndex(input.ImportingList)

	// The per-import index: which import carried a job, and what became of that import. Built
	// once, so that every later decision about a job is a map lookup rather than a scan.
	//
	// unattributableImports counts the imports whose jobs cannot be settled from the import alone
	// AND whose membership the manifest does not carry - a rejected import, or an errored one that
	// published no document, in either case with no way to say which jobs it held. A single such
	// import makes succeeding by exclusion unsound for the whole upload, because any remaining job
	// might be one of the ones it lost.
	outcomeOfImport := make(map[string]importOutcome, len(outcomes))
	importOfJob := make(map[int64]string)
	unattributableImports, undocumentedImports := 0, 0
	for _, outcome := range outcomes {
		outcomeOfImport[outcome.ImportID] = outcome
		members := outcome.jobIDs()
		if !outcome.hasUsableEvidence() {
			if outcome.Status != importStatusFailed {
				undocumentedImports++
			}
			if len(members) == 0 {
				unattributableImports++
			}
		}
		for _, jobID := range members {
			importOfJob[jobID] = outcome.ImportID
		}
	}

	// PER-IMPORT PASS. Every job whose own import already decides its fate, and only those.
	//
	// A rejected import ABORTS its jobs terminally. This is the one place in the connector where a
	// provider response abandons a job, and it is exactly the verdict the framework itself applies
	// through the poll response's 400 when an upload produced a single failed import - narrowed to
	// the right subset when other imports of the same upload survived.
	//
	// An errored import that published no document FAILS its jobs retryably. SendGrid said some of
	// its contacts were rejected and then gave nothing to identify them with, so neither delivery
	// nor rejection can be shown for any of them; a retry re-upserts them all idempotently, which
	// is the only answer that neither loses a contact nor abandons one.
	//
	// Running this pass FIRST is what makes the outcome deterministic: the row pass below refuses to
	// overwrite an abort, so a contact whose whole import was rejected is never downgraded to a
	// retry by a row that merely shares its address.
	rejectedJobCount := 0
	for _, jobID := range index.jobIDs {
		importID, known := importOfJob[jobID]
		if !known {
			continue
		}
		switch outcome := outcomeOfImport[importID]; {
		case outcome.Status == importStatusFailed:
			b.abortJob(&metadata, jobID, reasonImportFailed)
			rejectedJobCount++
		case !outcome.hasUsableEvidence():
			b.failJob(&metadata, jobID, reasonMissingErrorsDocument)
		}
	}
	if undocumentedImports > 0 {
		b.StatsFactory.NewTaggedStat("missing_errors_document_count", stats.CountType, b.statLabels(b.DestinationID)).Count(undocumentedImports)
		b.Logger.Errorn("[sendgrid bulk upload] an import reported errored contacts without publishing an errors document",
			logger.NewIntField("importCount", int64(len(outcomes))),
			logger.NewIntField("undocumentedImportCount", int64(undocumentedImports)))
	}

	// Every errors document worth reading, fetched, parsed and ATTRIBUTED one at a time against a
	// single aggregate row budget. Nothing is accumulated across documents except the counts.
	counts, unattributableByURL, response := b.readErrorRows(&metadata, index, outcomes)
	if response.StatusCode != http.StatusOK {
		return response
	}

	// An importing job whose own identifier could not be re-derived is failed, because it is not
	// comparable against the rows in either direction and "not shown to have failed" is not
	// evidence of delivery.
	//
	// Unless its own import settles it. A job carried by an import SendGrid reported as completed
	// needs no identifier comparison at all: the provider stated that the import finished with no
	// errors whatsoever, which is direct evidence about every contact in it, and re-uploading such
	// a contact because this process could no longer parse its payload would be churn with nothing
	// to show for it. Membership is what makes that distinction available.
	confirmedByImport := 0
	for _, jobID := range index.unresolvedJobIDs {
		if importID, known := importOfJob[jobID]; known && outcomeOfImport[importID].Status == importStatusCompleted {
			confirmedByImport++
			continue
		}
		b.failJob(&metadata, jobID, reasonUnresolvableContact)
	}

	// FAIL-CLOSED PASS over what the documents could not explain.
	//
	// A row this connector could not attribute - one naming an identifier no importing job claimed,
	// or one the tolerant parser could not reduce at all - is positive evidence that SendGrid
	// rejected a contact of that import WITHOUT saying which. From that moment succeeding by
	// exclusion is unsound for that import: "no row named this job" no longer distinguishes a
	// delivered contact from the one the unattributable row was about. So every still-unresolved
	// job of the affected import is retried instead.
	//
	// The blast radius is deliberately the IMPORT and not the upload. Per-import membership is
	// persisted precisely so that one import's unreadable evidence does not retry the contacts of
	// its siblings, and it is what lets a single reconciliation still report both failures and
	// successes - the guarantee this connector's contract rests on. When the affected import's
	// membership is NOT recoverable, that narrowing is impossible and the whole upload's remaining
	// jobs are retried instead, which is the same conservative fallback a rejected import of
	// unknown membership already triggers.
	//
	// Jobs a row did name keep their own specific reason: failJob preserves the FIRST reason
	// recorded for a job, so the provider's explanation is never overwritten by this general one.
	unattributableRowImports := make(map[string]int, len(unattributableByURL))
	unattributableRowCount := 0
	unattributableRowsUploadWide := false
	if len(unattributableByURL) > 0 {
		for _, outcome := range outcomes {
			rows := unattributableByURL[strings.TrimSpace(outcome.ErrorsURL)]
			if rows == 0 {
				continue
			}
			unattributableRowImports[outcome.ImportID] = rows
			unattributableRowCount += rows
			if len(outcome.jobIDs()) == 0 {
				// Membership unknown, so the affected population cannot be narrowed to this
				// import: the whole upload's remaining jobs are retried below. Tracked apart from
				// unattributableImports so the reason recorded against those jobs names the cause
				// they actually had - unreadable rows, not a rejected import.
				unattributableRowsUploadWide = true
			}
		}
	}
	retriedForUnattributableRows := 0
	for _, jobID := range index.jobIDs {
		importID, known := importOfJob[jobID]
		if !known {
			continue
		}
		if _, affected := unattributableRowImports[importID]; !affected {
			continue
		}
		if _, aborted := metadata.AbortedReasons[jobID]; aborted {
			continue
		}
		if _, failed := metadata.FailedReasons[jobID]; failed {
			continue
		}
		b.failJob(&metadata, jobID, reasonUnattributableRows)
		retriedForUnattributableRows++
	}
	if len(unattributableRowImports) > 0 {
		b.StatsFactory.NewTaggedStat("unattributable_row_import_count", stats.CountType, b.statLabels(b.DestinationID)).Count(len(unattributableRowImports))
		// Error level, because contacts SendGrid rejected could not be pinpointed and a batch is
		// being retried on that account. It must be impossible to miss, and it is the signal that
		// the undocumented errors-document shape may have changed.
		b.Logger.Errorn("[sendgrid bulk upload] an import reported contact errors that could not be attributed, retrying its unresolved jobs",
			logger.NewIntField("unattributableRowImportCount", int64(len(unattributableRowImports))),
			logger.NewIntField("unattributableRowCount", int64(unattributableRowCount)),
			logger.NewIntField("retriedJobCount", int64(retriedForUnattributableRows)),
			logger.NewIntField("importCount", int64(len(outcomes))))
	}

	if rejectedJobCount > 0 {
		b.StatsFactory.NewTaggedStat("import_rejected_job_count", stats.CountType, b.statLabels(b.DestinationID)).Count(rejectedJobCount)
	}
	if confirmedByImport > 0 {
		b.StatsFactory.NewTaggedStat("import_confirmed_job_count", stats.CountType, b.statLabels(b.DestinationID)).Count(confirmedByImport)
	}
	if counts.unmatched > 0 {
		b.StatsFactory.NewTaggedStat("unmatched_error_row_count", stats.CountType, b.statLabels(b.DestinationID)).Count(counts.unmatched)
	}
	if counts.unrecognized > 0 {
		b.StatsFactory.NewTaggedStat("unrecognized_error_row_count", stats.CountType, b.statLabels(b.DestinationID)).Count(counts.unrecognized)
	}
	if counts.ambiguous > 0 {
		b.StatsFactory.NewTaggedStat("ambiguous_error_row_count", stats.CountType, b.statLabels(b.DestinationID)).Count(counts.ambiguous)
	}
	if len(index.unresolvedJobIDs) > 0 {
		b.StatsFactory.NewTaggedStat("unresolvable_importing_job_count", stats.CountType, b.statLabels(b.DestinationID)).Count(len(index.unresolvedJobIDs))
	}

	// Whatever is still unresolved is settled here, and every importing job leaves with a status.
	//
	// resolved is what the two passes above established: contacts an import lost terminally, and
	// contacts a row named. Everything else is decided by ONE question - can a job be shown to
	// belong to an import that was not rejected?
	resolved := lo.Union(metadata.AbortedKeys, metadata.FailedKeys)
	unresolved, _ := lo.Difference(index.jobIDs, resolved)
	if unattributableImports > 0 || unattributableRowsUploadWide {
		// It cannot. An import of this upload lost contacts - SendGrid rejected it outright,
		// reported errors and published no document, or published a document carrying errors that
		// could not be attributed - and the manifest does not say which jobs it carried, because
		// the manifest degraded past its size budget or was written before membership was
		// recorded. Every remaining job might therefore be one of the lost ones.
		// They are RETRIED, not aborted: an abort would terminally discard contacts that were very
		// probably delivered, whereas a retry costs one idempotent re-upsert each and the batch
		// router escalates on its own if it keeps failing.
		//
		// The reason names the cause that actually applies. Both causes lead here, and an operator
		// reading a job's reason has to be able to tell "an entire import was lost" from "the
		// errors document named a contact I could not identify" - they call for different
		// investigations. A rejected or undocumented import takes precedence when both hold, since
		// it is the stronger statement about what was lost.
		unresolvedReason := reasonUnattributableImport
		if unattributableImports == 0 {
			unresolvedReason = reasonUnattributableRows
		}
		// Error level, because a whole batch is being retried on account of contacts that cannot be
		// pinpointed. It must be impossible to miss.
		if unattributableImports > 0 {
			b.StatsFactory.NewTaggedStat("unattributable_import_count", stats.CountType, b.statLabels(b.DestinationID)).Count(unattributableImports)
		}
		b.Logger.Errorn("[sendgrid bulk upload] an import lost contacts without recoverable job membership, retrying every unresolved job",
			logger.NewIntField("unattributableImportCount", int64(unattributableImports)),
			logger.NewBoolField("unattributableRowsWithoutMembership", unattributableRowsUploadWide),
			logger.NewIntField("importCount", int64(len(outcomes))),
			logger.NewIntField("unresolvedCount", int64(len(unresolved))))
		for _, jobID := range unresolved {
			b.failJob(&metadata, jobID, unresolvedReason)
		}
	} else {
		// It can. Every rejected import's jobs are already aborted, every import whose document
		// carried an unattributable row has had its remaining jobs retried above, and for the
		// imports that remain the errors documents were fetched and parsed in full with every row
		// attributed - so "no row named this job and no rejected import claimed it" genuinely does
		// mean the contact was accepted. Exclusion is therefore applied only to imports whose
		// evidence is COMPLETE, which is the property that makes it sound at all. This
		// is the succeeded-by-exclusion step, and the set difference keeps the three key sets
		// disjoint while staying linear over tens of thousands of jobs.
		metadata.SucceededKeys = append(metadata.SucceededKeys, unresolved...)
	}

	b.Logger.Infon("[sendgrid bulk upload] reconciled a partially failed upload",
		logger.NewIntField("importCount", int64(len(outcomes))),
		logger.NewIntField("rejectedJobCount", int64(rejectedJobCount)),
		logger.NewIntField("failedCount", int64(len(metadata.FailedKeys))),
		logger.NewIntField("abortedCount", int64(len(metadata.AbortedKeys))),
		logger.NewIntField("succeededCount", int64(len(metadata.SucceededKeys))),
		logger.NewIntField("unmatchedRowCount", int64(counts.unmatched)),
		logger.NewIntField("unrecognizedRowCount", int64(counts.unrecognized)),
		logger.NewIntField("unattributableRowImportCount", int64(len(unattributableRowImports))),
		logger.NewIntField("retriedForUnattributableRowCount", int64(retriedForUnattributableRows)),
		logger.NewIntField("rowCount", int64(counts.total)))

	// 200 is mandatory on success: the batch router discards the entire reconciliation, and
	// returns an error, for any other status.
	return common.GetUploadStatsResponse{
		StatusCode: http.StatusOK,
		Metadata:   metadata,
	}
}

// attributeErrorRows resolves ONE errors document's rows back to the jobs that produced them and
// records each resolved job as a retryable failure.
//
// It is called per document, as soon as that document is parsed, which is what lets the rows be
// released before the next document is fetched. Working on one document at a time is also why the
// counts are RETURNED rather than accumulated here: the caller pools them across every document the
// import published, because the fail-closed decision is a property of the whole import.
//
// The returned counts are the rows this document could not attribute to exactly one job: unmatched
// rows, whose identifier resolved to no importing job, and ambiguous rows, whose identifier resolved
// to several. Neither is an error - both are recorded and both are visible in metrics - but an
// unmatched row is what makes the import unsafe to reconcile by exclusion.
func (b *SendGridBulkUploader) attributeErrorRows(
	metadata *common.EventStatMeta,
	lookup map[string][]int64,
	rows []ImportErrorRow,
	documentIndex int,
) (unmatched, ambiguous int) {
	for rowIndex, row := range rows {
		jobIDs, ok := lookup[strings.ToLower(strings.TrimSpace(row.Identifier))]
		if !ok || len(jobIDs) == 0 {
			// Counted and logged so that a change in the undocumented document shape becomes
			// visible in metrics rather than only in a job's reason.
			//
			// The row's POSITION is logged, not its identifier and not a fingerprint of it. A
			// contact identifier is personal data, and a hash of one is still personal data: the
			// value space of email addresses and phone numbers is small enough to enumerate, so
			// an unsalted digest is recoverable by dictionary attack and stable enough to
			// correlate the same person across unrelated log lines indefinitely. The position -
			// which document, which row within it, out of how many - is all an operator needs to
			// find the entry in the document itself, and it identifies nobody.
			//
			// The row is not attributed - this method cannot invent an attribution - and it is
			// deliberately not escalated either. It says a contact was rejected without saying
			// which, which is evidence about the document and not about any job the document
			// never named, so it is counted and warned about and the jobs it says nothing about
			// are left to be settled by their own import's state.
			unmatched++
			b.Logger.Warnn("[sendgrid bulk upload] errored row could not be matched to an importing job",
				logger.NewIntField("documentIndex", int64(documentIndex)),
				logger.NewIntField("rowIndex", int64(rowIndex)),
				logger.NewIntField("rowCount", int64(len(rows))))
			continue
		}
		reason := rowFailureReason(row.Message, row.Identifier)
		if len(jobIDs) > 1 {
			// The identifier is ambiguous - two staged events carried it, so SendGrid upserted
			// them onto one contact - and the failure cannot be attributed to just one of them.
			// EVERY candidate is failed: failing a job that in fact succeeded costs one
			// idempotent re-upsert, whereas reporting a failed contact as delivered loses it.
			ambiguous++
			reason = fmt.Sprintf("%s [%s: this contact identifier matches %d jobs, so all of them are retried]",
				reason, reasonCodeRowAmbiguous, len(jobIDs))
			// The matched jobIDs are the identifying detail worth logging here, and they are
			// this connector's own opaque keys rather than anybody's personal data. The
			// identifier itself is deliberately absent, for the reason given above.
			b.Logger.Warnn("[sendgrid bulk upload] errored row matches more than one importing job",
				logger.NewIntField("documentIndex", int64(documentIndex)),
				logger.NewIntField("rowIndex", int64(rowIndex)),
				logger.NewIntField("jobCount", int64(len(jobIDs))),
				logger.NewStringField("jobIDs", renderJobIDs(jobIDs)))
		}
		for _, jobID := range jobIDs {
			b.failJob(metadata, jobID, reason)
		}
	}
	return unmatched, ambiguous
}

// failJob records one job as a RETRYABLE failure, keeping FailedKeys free of duplicates and
// refusing to contradict an abort already recorded for the same job.
//
// FailedReasons doubles as the membership set, which is what lets every call site - an attributed
// row, a job whose identifier could not be re-derived, and the unattributable-rejection sweep -
// run in any order and overlap freely without ever appending the same job twice. The FIRST reason
// recorded for a job wins, so the specific explanation SendGrid gave for a contact is never
// overwritten by a general one.
//
// The abort check is what keeps the three key sets disjoint no matter what the errors document
// says. A contact whose entire import SendGrid rejected is settled terminally before any row is
// examined, and a row that happens to name the same identifier - the same address can legitimately
// appear in two imports of one upload - must not quietly downgrade that verdict to a retry, which
// would leave the same job in both FailedKeys and AbortedKeys and let the router write two
// conflicting statuses for it.
func (b *SendGridBulkUploader) failJob(metadata *common.EventStatMeta, jobID int64, reason string) {
	if _, aborted := metadata.AbortedReasons[jobID]; aborted {
		return
	}
	if _, seen := metadata.FailedReasons[jobID]; seen {
		return
	}
	metadata.FailedKeys = append(metadata.FailedKeys, jobID)
	metadata.FailedReasons[jobID] = reason
}

// abortJob records one job as a TERMINAL failure, keeping AbortedKeys free of duplicates.
//
// It has exactly one caller, and deliberately so: a job is abandoned on the strength of a provider
// response only when SendGrid reported that the job's entire import failed, which the provider
// itself defines as finished with all errors or entirely unprocessable. Every other conclusion
// this connector draws from a response goes through failJob and stays retryable.
func (b *SendGridBulkUploader) abortJob(metadata *common.EventStatMeta, jobID int64, reason string) {
	if _, seen := metadata.AbortedReasons[jobID]; seen {
		return
	}
	metadata.AbortedKeys = append(metadata.AbortedKeys, jobID)
	metadata.AbortedReasons[jobID] = reason
}

// renderJobIDs renders a small set of job IDs for a log field.
//
// Job IDs are this connector's own opaque keys, so unlike a contact identifier they are safe to
// log verbatim. The rendering is bounded anyway, because an ambiguous identifier could in
// principle match a great many staged events and a log field is not the place to discover that.
func renderJobIDs(jobIDs []int64) string {
	const maxRendered = 16
	rendered := lo.Map(jobIDs[:min(len(jobIDs), maxRendered)], func(jobID int64, _ int) string {
		return strconv.FormatInt(jobID, 10)
	})
	if len(jobIDs) > maxRendered {
		rendered = append(rendered, fmt.Sprintf("and %d more", len(jobIDs)-maxRendered))
	}
	return strings.Join(rendered, ",")
}

// resolveImportOutcomes establishes what happened to every import of the upload being reconciled:
// its state, its errors document and the jobs it carried.
//
// Three sources are tried, in descending order of fidelity:
//
//   - the structured outcome document Poll forwarded, which is authoritative because Poll read
//     each import's state directly and paired it with the membership the manifest carries;
//   - a bare list of errors document URLs, which an older poll response or a degraded render can
//     produce. The states are then unknown, so no import can be treated as rejected and every
//     document is read;
//   - nothing at all, which happens when the process that reconciles is not the process that
//     polled. The manifest is then recovered from the persisted parameters and each import's
//     status is read again.
//
// The second return value is only meaningful when no outcome could be established, in which case
// it carries the non-200 response the caller must return - never a 200 with nothing failed, which
// would mark the whole upload delivered.
func (b *SendGridBulkUploader) resolveImportOutcomes(input common.GetUploadStatsInput) ([]importOutcome, common.GetUploadStatsResponse) {
	manifest := parseImportManifest(b.persistedImportID(input.Parameters))

	if outcomes, ok := parseImportOutcomes(input.FailedJobParameters); ok && len(outcomes) > 0 {
		return outcomes, common.GetUploadStatsResponse{StatusCode: http.StatusOK}
	}

	// A bare URL list carries no state and no membership, so each URL becomes an import of unknown
	// identity whose document has to be read. Nothing here can be treated as a rejection, which is
	// exactly right: an unknown state must never abort a job.
	if errorsURLs := splitErrorsURLs(input.FailedJobParameters); len(errorsURLs) > 0 {
		return lo.Map(errorsURLs, func(errorsURL string, index int) importOutcome {
			return importOutcome{
				ImportID:  fmt.Sprintf("unknown-%d", index),
				ErrorsURL: errorsURL,
			}
		}), common.GetUploadStatsResponse{StatusCode: http.StatusOK}
	}

	if len(manifest.importIDs) == 0 {
		b.Logger.Errorn("[sendgrid bulk upload] no import id is available for reconciliation")
		return nil, common.GetUploadStatsResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      "No sendgrid import id was persisted for this upload",
		}
	}

	outcomes := make([]importOutcome, 0, len(manifest.importIDs))
	for _, importID := range manifest.importIDs {
		importStatus, err := b.SendGridAPIService.GetImportStatus(importID)
		if err != nil {
			b.Logger.Errorn("[sendgrid bulk upload] unable to re-read import status while reconciling",
				logger.NewStringField("errorClass", errorClass(err)),
				logger.NewIntField("statusCode", statusCodeOf(err)),
				logger.NewStringField("importId", importID))
			return nil, common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to fetch the sendgrid import status: " + err.Error()),
			}
		}
		if importStatus == nil {
			b.Logger.Errorn("[sendgrid bulk upload] no import status was returned while reconciling",
				logger.NewStringField("importId", importID))
			return nil, common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      "SendGrid returned no import status while reconciling",
			}
		}
		status := strings.ToLower(strings.TrimSpace(importStatus.Status))
		if status == importStatusCompleted && importStatus.Results.ErroredCount > 0 {
			// The same defensive reading Poll applies: completed promises no errors, so a
			// non-zero errored count means the import behaves as errored whatever it is called.
			status = importStatusErrored
		}
		outcomes = append(outcomes, manifest.outcomeOf(importID, status, importStatus))
	}
	return outcomes, common.GetUploadStatsResponse{StatusCode: http.StatusOK}
}

// persistedImportID recovers the import manifest string the batch router persisted, returning an
// empty string when the parameters cannot be read.
//
// An unreadable value is not an error here. The manifest only ever refines reconciliation - it says
// which jobs an import carried - and the callers already handle its absence, whereas failing the
// whole reconciliation over it would strand jobs the forwarded outcome document could have settled
// perfectly well. It is logged, because a value this connector wrote and cannot read back is worth
// knowing about.
//
// The parameter is []byte rather than the json.RawMessage the shared input struct declares, so
// that this package needs no import of the standard library's JSON package - which the repository
// forbids in favour of jsonrs. The named slice type converts implicitly at the call site.
func (b *SendGridBulkUploader) persistedImportID(parameters []byte) string {
	if len(parameters) == 0 {
		return ""
	}
	var params struct {
		ImportId string `json:"importId"`
	}
	if err := jsonrs.Unmarshal(parameters, &params); err != nil {
		b.Logger.Warnn("[sendgrid bulk upload] unable to parse the persisted import parameters",
			logger.NewStringField("errorClass", errorClass(err)))
		return ""
	}
	return params.ImportId
}

// errorRowCounts is what reading an import's errors documents yields besides the failures it
// records: the volume it read, and the three ways a row can fail to name exactly one job.
//
// They are counts rather than retained rows because the rows themselves are released as soon as
// they are attributed - see readErrorRows - and because every consumer of them is a metric, a log
// field or the one decision that needs them: whether succeeding by exclusion is still sound.
type errorRowCounts struct {
	// total is how many entries every document of the import presented, recognized or not, which
	// is what the aggregate row budget is spent against.
	total int

	// unrecognized is how many entries the tolerant parser could not read as a row at all.
	unrecognized int

	// unmatched is how many rows named an identifier no importing job claimed.
	unmatched int

	// ambiguous is how many rows named an identifier that several importing jobs claimed.
	ambiguous int
}

// readErrorRows fetches, parses and attributes the errors document of every import that has one
// worth reading, and returns what it read.
//
// Attribution happens HERE, one document at a time, rather than in the caller after every document
// has been accumulated. That is what keeps peak memory at ONE document's rows however many
// documents an import published: the rows of a document are never retained past the iteration that
// read them, so rows from two documents never coexist. It is also why the counts are returned
// rather than the rows.
//
// The row budget is what REMAINS of the import's allowance at each step, not a fresh per-document
// one: an import that published several documents must not be able to multiply the bound by
// publishing more of them.
//
// A rejected import's document is skipped deliberately: its jobs are already settled terminally by
// the import's own state, so reading it could only produce rows naming contacts that are already
// aborted - or, worse, rows naming an address that also appears in a surviving import, which would
// retry a contact that was delivered. A cleanly completed import publishes no document at all, and
// the same URL published by two imports is read once.
//
// An import that IS errored but published no document is skipped here too; the caller recognises
// that case from the outcome itself and settles those jobs conservatively. Returning a non-200 for
// it would be the worse answer, because the router writes no status at all for a non-200 and would
// then poll the same import forever.
//
// The response is non-200 only when a document could not be fetched or could not be understood at
// all. Both are the safe failure mode - reporting success would silently deliver contacts SendGrid
// rejected - and both are retried by the batch router.
func (b *SendGridBulkUploader) readErrorRows(
	metadata *common.EventStatMeta,
	index importingIndex,
	outcomes []importOutcome,
) (errorRowCounts, map[string]int, common.GetUploadStatsResponse) {
	var counts errorRowCounts
	// Keyed by the document's URL rather than by import, because one URL can be published by
	// several imports of an upload and is read exactly once: keying it this way lets the caller
	// attribute what a document could not explain back to EVERY import that pointed at it.
	unattributableByURL := make(map[string]int)
	// An ordered set rather than a scanned slice, so that de-duplicating the documents of an upload
	// that produced many imports stays linear in the number of imports.
	var readURLs errorsURLSet
	documentIndex := 0
	for _, outcome := range outcomes {
		if outcome.Status == importStatusFailed || outcome.Status == importStatusCompleted {
			continue
		}
		errorsURL := strings.TrimSpace(outcome.ErrorsURL)
		if errorsURL == "" || !readURLs.addURL(errorsURL) {
			continue
		}

		document, err := b.SendGridAPIService.GetImportErrors(errorsURL)
		if err != nil {
			// Classification instead of rendered text. errorClass reports errorsURLRejected
			// when the document's host was refused, which is the one failure here an operator
			// resolves rather than investigates - the errors-document host contract in
			// apiService.go explains it, and the returned Error field carries the offending
			// host and the configuration key that governs it.
			b.Logger.Errorn("[sendgrid bulk upload] unable to fetch the errors document",
				logger.NewStringField("errorClass", errorClass(err)),
				logger.NewIntField("statusCode", statusCodeOf(err)),
				logger.NewIntField("documentIndex", int64(documentIndex)))
			return counts, unattributableByURL, common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to fetch the sendgrid errors document: " + err.Error()),
			}
		}
		parsedDocument, err := parseImportErrors(document, maxErrorRows-counts.total)
		if err != nil {
			// The document could not be understood at all. Retrying is the safe failure mode:
			// reporting success here would silently deliver contacts SendGrid rejected.
			//
			// A parse failure is also the one case where rendering the error is most tempting and
			// least safe: a decoder reports the fragment it choked on, and that fragment is part
			// of a document quoting rejected contacts. The classification is fixed here instead,
			// and the decoder's own message travels only to the returned Error field.
			b.Logger.Errorn("[sendgrid bulk upload] unable to parse the errors document",
				logger.NewStringField("errorClass", errorClassDocumentUnparseable),
				logger.NewIntField("documentIndex", int64(documentIndex)))
			return counts, unattributableByURL, common.GetUploadStatsResponse{
				StatusCode: http.StatusInternalServerError,
				Error:      sanitizeReason("Failed to parse the sendgrid errors document: " + err.Error()),
			}
		}
		// Attributed immediately and then released, which is what bounds the memory this method
		// holds to one document's worth of rows.
		unmatched, ambiguous := b.attributeErrorRows(metadata, index.lookup, parsedDocument.Rows, documentIndex)
		// The SCANNED count, not the reduced one: an entry the parser declined still consumed a
		// read, and charging only the rows it understood would leave a document of unreadable
		// entries costing nothing against the import's allowance.
		counts.total += parsedDocument.ScannedRows
		counts.unrecognized += parsedDocument.UnrecognizedRows
		counts.unmatched += unmatched
		counts.ambiguous += ambiguous
		// What this document said about contacts it did not identify. An unmatched row named an
		// identifier no importing job claimed; an unrecognized row could not be reduced to an
		// identifier at all. Either way an errored contact of this document went unattributed, and
		// the caller has to stop inferring delivery for the imports that published it. Ambiguous
		// rows are deliberately excluded: they WERE attributed, to every candidate job.
		if unattributed := unmatched + parsedDocument.UnrecognizedRows; unattributed > 0 {
			unattributableByURL[errorsURL] += unattributed
		}
		documentIndex++
	}
	return counts, unattributableByURL, common.GetUploadStatsResponse{StatusCode: http.StatusOK}
}

// importingIndex is everything one pass over an import's importing list yields.
//
// The three results are held together deliberately, because they must describe exactly the same
// population: the identifier lookup an errored row is resolved against, the job set the two
// reconciliation branches work from, and the jobs whose identifier could not be re-derived. Deriving
// the job set in a SECOND, independent traversal of the same list left open the possibility of the
// two disagreeing about which entries were usable, and a job present in one but not the other is
// either failed twice or cleared without ever having been compared to anything.
type importingIndex struct {
	// lookup maps a lower-cased contact identifier to every distinct job that claimed it.
	lookup map[string][]int64

	// jobIDs lists every DISTINCT non-nil importing job in first-appearance order. It is the
	// population both the fail-closed sweep and the succeeded-by-exclusion difference work from,
	// so listing a job once is what keeps each of them reporting that job once.
	jobIDs []int64

	// unresolvedJobIDs lists the importing jobs whose own contact identifier could not be
	// re-derived at all. They are reported rather than merely skipped: an errored row can only be
	// compared against identifiers that were computed, so a job missing from the lookup can be
	// shown neither to be named by the document nor to be absent from it. Treating such a job as
	// delivered would be an assumption, and the caller fails it instead.
	unresolvedJobIDs []int64
}

// buildImportingIndex indexes the importing jobs by every contact identifier their events carry, so
// that an errored row can be resolved back to the job that produced it with no state retained from
// the upload.
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
//
// A job is indexed AT MOST ONCE, recognized by its ID. That single map lookup per job is what lets
// each identifier's job list be appended to directly: the only way one job could reach the same
// identifier twice is by appearing twice in the importing list, because identifiersOf already
// de-duplicates within a single contact. Scanning the growing list instead - a linear membership
// check per identifier per job - costs work proportional to the SQUARE of the import's size, which
// at the 30,000 contacts a single request can carry is hundreds of millions of comparisons for a
// question one map answers outright.
func (b *SendGridBulkUploader) buildImportingIndex(importingList []*jobsdb.JobT) importingIndex {
	index := importingIndex{
		lookup:           make(map[string][]int64, len(importingList)),
		jobIDs:           make([]int64, 0, len(importingList)),
		unresolvedJobIDs: make([]int64, 0),
	}
	seenJobIDs := make(map[int64]struct{}, len(importingList))
	for _, job := range importingList {
		// A nil entry is skipped rather than dereferenced: this runs inside a shared batch router
		// worker, where a panic would take down every destination the worker is serving.
		if job == nil {
			continue
		}
		if _, seen := seenJobIDs[job.JobID]; seen {
			continue
		}
		seenJobIDs[job.JobID] = struct{}{}
		index.jobIDs = append(index.jobIDs, job.JobID)

		contact, err := b.buildContact(stagedMessage(job.EventPayload))
		if err != nil {
			// Reported, not silently skipped. A job in this state is inconsistent with the
			// upload that produced the import - Upload rejects an identifier-less contact
			// individually and never sends it - so reaching here means the payload or the
			// trait mapping changed underneath the import. Retrying resolves it either way:
			// the next Upload either succeeds or aborts this one job with a precise reason.
			//
			// STILL no rendered error field, and the reason this comment gives for that has since
			// been vindicated: buildContact no longer has a single failure mode - it also refuses a
			// field longer, or a multi-valued field carrying more entries, than this connector will
			// send - so a rendered error here WOULD now be a sink that a newer failure mode started
			// filling. Instead the failure is reported by a fixed classification taken from the
			// sentinel the error wraps, which names the category and never the value, and the job ID
			// is enough to find the record.
			b.Logger.Warnn("[sendgrid bulk upload] importing job could not be reduced to a contact",
				logger.NewIntField("jobID", job.JobID),
				logger.NewStringField("cause", contactRejectionCause(err)))
			index.unresolvedJobIDs = append(index.unresolvedJobIDs, job.JobID)
			continue
		}
		for _, identifier := range identifiersOf(contact) {
			index.lookup[identifier] = append(index.lookup[identifier], job.JobID)
		}
	}
	return index
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
// Tolerance is NOT the same as discarding. Every entry the parser declines to understand is
// COUNTED on the returned document's UnrecognizedRows, because a declined entry is an errored
// contact whose identity is unknown - not an absent one. Reporting the count is what lets
// reconciliation refuse to infer success for the rest of the import, and it is the difference
// between a tolerant parser and a lossy one.
//
// rowBudget is how many rows this document is still allowed to contribute: the caller passes what
// REMAINS of the import's allowance, so an import cannot multiply the bound by publishing several
// documents. A document that would exceed it is reported as an error rather than truncated, because
// a truncated errors document reconciles as though the rows it dropped had never failed.
//
// Whether the bytes are one well-formed JSON value at all is decided WITHOUT materializing them:
// the check walks the bytes and answers yes or no, where decoding into an untyped value built the
// entire document as maps, slices and strings purely to have it thrown away and read again. gjson is
// then used for read-only field extraction, which is what handles the nested contact.email candidate
// without a bespoke type per shape, and each byte range is handed to it exactly once.
func parseImportErrors(document []byte, rowBudget int) (importErrorsDocument, error) {
	trimmed := bytes.TrimSpace(document)
	if len(trimmed) == 0 {
		return importErrorsDocument{}, errors.New("the errors document is empty")
	}
	// Depth is checked before the document is decoded, because decoding a deeply nested value
	// is what costs the memory and the stack: the check has to happen while the input is still
	// just bytes. The document's schema is not published, so no legitimate shape is anywhere
	// near this bound.
	if depth := jsonDepth(trimmed); depth > maxErrorsDocumentDepth {
		return importErrorsDocument{}, fmt.Errorf("the errors document is nested %d levels deep, more than the %d allowed", depth, maxErrorsDocumentDepth)
	}

	// Not one JSON value - which is what several JSON values one per line looks like - so the
	// document may be newline-delimited JSON, the last shape this parser accepts.
	if !gjson.ValidBytes(trimmed) {
		return parseNewlineDelimitedImportErrors(trimmed, rowBudget)
	}

	root := gjson.ParseBytes(trimmed)
	if root.IsArray() {
		scan := streamImportErrorRows(root, rowBudget)
		if scan.overBudget {
			return importErrorsDocument{}, fmt.Errorf("the errors document holds more than the %d rows still allowed for this import", rowBudget)
		}
		if len(scan.rows) > 0 {
			return importErrorsDocument{Rows: scan.rows, UnrecognizedRows: scan.unrecognized, ScannedRows: scan.scanned}, nil
		}
		return importErrorsDocument{}, errors.New("the errors document is an array carrying no recognizable rows")
	}
	if root.IsObject() {
		for _, key := range []string{"errors", "results"} {
			if wrapped := root.Get(key); wrapped.IsArray() {
				scan := streamImportErrorRows(wrapped, rowBudget)
				if scan.overBudget {
					return importErrorsDocument{}, fmt.Errorf("the errors document's %q array holds more than the %d rows still allowed for this import", key, rowBudget)
				}
				if len(scan.rows) > 0 {
					return importErrorsDocument{Rows: scan.rows, UnrecognizedRows: scan.unrecognized, ScannedRows: scan.scanned}, nil
				}
				return importErrorsDocument{}, fmt.Errorf("the errors document's %q array carries no recognizable rows", key)
			}
		}
		if rowBudget < 1 {
			return importErrorsDocument{}, fmt.Errorf("the errors document holds a row, more than the %d still allowed for this import", rowBudget)
		}
		if row, ok := importErrorRowFrom(root); ok {
			return importErrorsDocument{Rows: []ImportErrorRow{row}, ScannedRows: 1}, nil
		}
		return importErrorsDocument{}, errors.New("the errors document is an object carrying no recognizable rows")
	}
	return importErrorsDocument{}, errors.New("the errors document is neither an array nor an object")
}

// importErrorRowScan is what one streamed pass over an errors-document array yields.
type importErrorRowScan struct {
	// rows are the entries reduced to an identifier or a message. Nil when the scan stopped on
	// the budget, because a partial reading of a document must not be reconciled against.
	rows []ImportErrorRow

	// unrecognized counts the entries that could not be reduced to either.
	unrecognized int

	// scanned counts every entry the array presented, recognized or not, and is what the
	// import's aggregate row budget is spent against.
	scanned int

	// overBudget reports that the array presented more entries than the remaining budget allows,
	// in which case the scan stopped at the first entry past it.
	overBudget bool
}

// streamImportErrorRows reduces the entries of an errors-document array WITHOUT materializing the
// array first, and stops at the first entry past the remaining row budget.
//
// gjson.Result.Array() was the obvious way to write this and is the wrong one: it decodes and
// retains EVERY element before a single element has been looked at, so the row cap was applied to a
// slice the document had already been allowed to allocate. A provider response - or a compromised
// object-storage document - carrying millions of tiny entries could therefore exhaust a shared batch
// router worker while the bound that exists to prevent exactly that sat one line below the
// allocation. ForEach parses one element at a time and is told to stop, which makes the cap
// effective rather than advisory: peak memory is now the entries actually accepted plus one.
//
// Every entry counts against the budget, not merely the ones that reduced to a row. An entry the
// parser declines still had to be read, and letting a document of unreadable entries pass the cap
// for free would leave the one shape most likely to be hostile as the one shape that is unbounded.
func streamImportErrorRows(array gjson.Result, rowBudget int) importErrorRowScan {
	scan := importErrorRowScan{rows: make([]ImportErrorRow, 0)}
	array.ForEach(func(_, entry gjson.Result) bool {
		scan.scanned++
		if scan.scanned > rowBudget {
			scan.overBudget = true
			// Stops the iteration, so nothing past the cap is ever decoded.
			return false
		}
		row, ok := importErrorRowFrom(entry)
		if !ok {
			scan.unrecognized++
			return true
		}
		scan.rows = append(scan.rows, row)
		return true
	})
	if scan.overBudget {
		// Released deliberately: a document that broke the cap is refused outright by the caller,
		// and holding a partial reading of it would only invite a partial reconciliation.
		scan.rows = nil
		scan.unrecognized = 0
	}
	return scan
}

// importErrorsDocument is what the parser understood of one errors document.
//
// It is a struct rather than a bare row slice specifically so that UnrecognizedRows travels
// WITH the rows. The rows alone cannot express "there were also errors here I could not read",
// and reconciliation needs exactly that fact: without it, every entry the parser declined would
// be indistinguishable from an entry that never existed, and the jobs those entries referred to
// would be marked delivered by exclusion.
type importErrorsDocument struct {
	// Rows are the entries the parser reduced to an identifier or a message.
	Rows []ImportErrorRow

	// UnrecognizedRows counts the entries the parser could not reduce to either: a malformed
	// line, an entry that is not an object, or an object carrying neither a recognized
	// identifier field nor a recognized message field. Each one is an errored contact of
	// unknown identity, so a non-zero count means the import cannot be reconciled by exclusion.
	UnrecognizedRows int

	// ScannedRows counts every entry the document presented, recognized or not, and is what the
	// import's aggregate row budget is spent against.
	//
	// It is deliberately NOT len(Rows). Charging only the reduced rows would leave a document
	// made of entries the parser declines - a malformed line, an object with no field this
	// connector knows, an entry that is not an object at all - costing nothing at all, so the
	// one shape most likely to be hostile would be the one shape the cap did not bound. Blank
	// lines are excluded, because a blank line presents no entry.
	ScannedRows int
}

// parseNewlineDelimitedImportErrors decodes an errors document written as one JSON object per
// line.
//
// A single unreadable line does not fail the whole document, because one malformed line must not
// cost the reconciliation of every other row - but it is COUNTED on UnrecognizedRows rather than
// discarded, so the caller still knows an errored contact went unread and can refuse to infer
// success for the rest of the import. A document yielding no row at all is reported as an error,
// so it can never be mistaken for "nothing failed".
//
// A blank line is not counted: it carries no entry, whereas every non-blank line the parser
// cannot reduce to a row does.
//
// rowBudget is what remains of the whole import's row allowance, exactly as it is for the shapes
// that arrive as one JSON value, and a line's validity is decided without materializing it. Every
// non-blank line is charged against that budget as it is read, recognized or not, so a document of
// unreadable lines is bounded exactly as tightly as a document of readable ones.
func parseNewlineDelimitedImportErrors(document []byte, rowBudget int) (importErrorsDocument, error) {
	parsedDocument := importErrorsDocument{Rows: make([]ImportErrorRow, 0)}
	scanner := bufio.NewScanner(bytes.NewReader(document))
	// The compile-time constant, deliberately, and NOT the operator-tunable staging-file capacity.
	// The two scanners read entirely different inputs - a staging file this process wrote, and a
	// document SendGrid served - so a setting raised to accommodate an unusually large staged event
	// must not silently change how much of a provider-supplied line is buffered. The constant is
	// always positive and always within the validated bounds, so this site has no configuration
	// path to validate in the first place; the document as a whole is already bounded by the
	// errors-document read budget before it ever reaches this parser.
	scanner.Buffer(nil, defaultMaxBufferCapacity)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		// Charged BEFORE the line is judged, and charged whatever the judgement is. A line the
		// parser declines still had to be read, so letting it cost nothing would leave a document
		// of unreadable lines unbounded - the one shape most likely to be hostile escaping the one
		// bound meant to hold it. Checking here also stops the scan at the first line past the
		// budget rather than reading the rest of the document to discover it was too long.
		parsedDocument.ScannedRows++
		if parsedDocument.ScannedRows > rowBudget {
			return importErrorsDocument{}, fmt.Errorf("the errors document holds more than the %d rows still allowed for this import", rowBudget)
		}
		if !gjson.ValidBytes(line) {
			parsedDocument.UnrecognizedRows++
			continue
		}
		parsed := gjson.ParseBytes(line)
		if !parsed.IsObject() {
			parsedDocument.UnrecognizedRows++
			continue
		}
		row, ok := importErrorRowFrom(parsed)
		if !ok {
			parsedDocument.UnrecognizedRows++
			continue
		}
		parsedDocument.Rows = append(parsedDocument.Rows, row)
	}
	if err := scanner.Err(); err != nil {
		return importErrorsDocument{}, fmt.Errorf("reading the newline delimited errors document: %w", err)
	}
	if len(parsedDocument.Rows) == 0 {
		return importErrorsDocument{}, errors.New("the errors document matches none of the shapes this parser understands")
	}
	return parsedDocument, nil
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
// longNumberPattern requires at least nine digits: long enough to cover a phone number or a
// numeric account identifier, short enough to leave a quota, a byte count or an RFC3339 instant
// untouched - the "T" of an RFC3339 timestamp is not an accepted separator, so a rendered
// rate-limit reset window survives intact.
//
// It tolerates up to TWO separator characters between digits rather than one, because that is
// what the punctuated international phone format needs: "+1 (555) 867-5309" has both " (" and
// ") " in it, and a single-separator pattern walks straight past the whole number. Over-redacting
// is the safe direction here - a long space-separated date may be caught as well, which costs a
// little diagnostic detail and leaks nothing.
var (
	emailPattern      = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	longNumberPattern = regexp.MustCompile(`\+?\d(?:[\s().\-]{0,2}\d){8,}`)
)

// No contact identifier - and no fingerprint of one - is ever written to a log by this package.
//
// A truncated unsalted digest was tried and deliberately removed. It was not the pseudonym it
// looked like: email addresses and phone numbers occupy a value space small enough to enumerate
// exhaustively, so a digest of one is recovered by dictionary attack rather than broken, and
// because the same input always produced the same output it also served as a permanent
// correlation key linking one person's activity across otherwise unrelated log lines. Both
// properties are exactly what a log must not carry about a data subject, and calling the result
// "non-reversible" was simply wrong.
//
// Nothing of diagnostic value was lost. What an operator needs in order to investigate an
// unattributable row is the row's POSITION in the document, the aggregate counts, and the jobIDs
// involved - all of which are logged, all of which are this connector's own opaque keys, and none
// of which identifies a person.

// sanitizeReason prepares provider-supplied text for somewhere it is KEPT: a job status that is
// persisted in the jobs database, or a log line.
//
// It is the manager's entry point into the package's single sanitization gate,
// sanitizeProviderText, and exists as a named function purely because the manager never holds the
// API key: every caller here is handling text that arrived from SendGrid indirectly - an error
// value, an errors-document message - rather than a response body read with the credential in
// hand. Passing an empty key applies every pattern-based protection (credential shapes, URL
// query strings and fragments, email addresses, long digit runs, control characters, length cap)
// while leaving the exact-match redaction to the adapter, which does hold the key.
//
// Having exactly one implementation is the point: the connector is required to record SendGrid's
// explanation for a failed contact, that text is entirely outside its control, and the next sink
// added must not be the one that leaks.
func sanitizeReason(reason string) string {
	return sanitizeProviderText(reason, "")
}

// rowFailureReason renders the durable reason one errored provider row contributes to a job.
//
// WHY THE PROVIDER MESSAGE IS KEPT AT ALL. It is required: the AAP mandates recording the provider
// row's message in FailedReasons (AAP R6, and the reconciliation step in AAP section 0.4.2.4), and
// it is the only explanation of WHY a specific contact was refused. Dropping it would resolve the
// disclosure concern by destroying the feature, so it is retained and defended in depth instead.
//
// THE FOUR DEFENCES, applied in this order because the order is itself a property:
//
//  1. A stable connector-owned code is placed FIRST, so the reason is classifiable without parsing
//     provider prose and stays actionable even if every other step removes everything else.
//  2. sanitizeReason applies the package's single pattern gate - control characters, credential
//     shapes, URLs, email addresses, long digit runs.
//  3. The affected contact's OWN identifier is then redacted by exact, case-insensitive match. This
//     is the one redaction that is precise rather than heuristic: the identifier is known here, so
//     removing it needs no pattern and cannot miss. It catches what step 2 structurally cannot - an
//     identifier that is not email-shaped and not a long digit run, such as an external ID echoed
//     back inside a sentence.
//  4. The result is capped LAST, on rune boundaries, at maxRowReasonRunes. Capping last is what
//     keeps the cap from ever truncating a value that steps 2 and 3 were about to redact.
//
// What none of this can do is recognise ordinary prose that happens to carry personal data - a
// message naming a custom field's value, say. That residual exposure is inherent in recording a
// third party's explanation verbatim, which is why it is bounded in length, prefixed with a code
// that makes the provider half safe to drop downstream, and documented here rather than assumed
// away.
func rowFailureReason(message, identifier string) string {
	sanitized := sanitizeReason(message)
	if sanitized == "" {
		return reasonCodeRowUnexplained + ": " + defaultFailureReason
	}
	return capRunes(reasonCodeRowRejected+": "+redactIdentifier(sanitized, identifier), maxRowReasonRunes)
}

// redactIdentifier removes one contact identifier from text by exact, case-insensitive match.
//
// Case-insensitive because SendGrid lower-cases the email it stores while the event may have carried
// any casing, so the value echoed in a message need not match the value this connector holds byte
// for byte. Short values are left alone: a one or two character identifier would match half the
// words in a sentence, and blanking those would destroy the explanation while protecting nothing
// that is identifying in the first place.
func redactIdentifier(text, identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if len(identifier) < 3 || text == "" {
		return text
	}
	if !strings.Contains(strings.ToLower(text), strings.ToLower(identifier)) {
		return text
	}
	// Rebuilt case-insensitively: strings.ReplaceAll is case-sensitive, and the whole point of this
	// function is the case that does not match exactly.
	var (
		redacted strings.Builder
		lowered  = strings.ToLower(text)
		target   = strings.ToLower(identifier)
	)
	for {
		at := strings.Index(lowered, target)
		if at < 0 {
			redacted.WriteString(text)
			break
		}
		redacted.WriteString(text[:at])
		redacted.WriteString(identifierRedactionPlaceholder)
		text = text[at+len(target):]
		lowered = lowered[at+len(target):]
	}
	return redacted.String()
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

// errorsURLSet accumulates the errors-document URLs of an upload's imports: absent URLs ignored,
// duplicates collapsed, first-appearance order kept.
//
// Order is kept because the URLs are joined into a single field and are then fetched in that order,
// so a stable order makes a reconciliation's request sequence reproducible. Membership is a map
// rather than a scan of what has been collected so far: an upload can create as many imports as its
// budget allows, and re-scanning the collected URLs for each one is work proportional to the square
// of that number for a question a lookup answers outright.
//
// The zero value is ready to use, and the map is allocated only when a URL actually arrives - which
// for the overwhelmingly common single-import upload means it is never allocated at all.
type errorsURLSet struct {
	urls []string
	seen map[string]struct{}
}

// add records one import's errors-document URL, if it published one.
func (s *errorsURLSet) add(importStatus *ImportStatusResponse) {
	if importStatus == nil {
		return
	}
	s.addURL(importStatus.Results.ErrorsURL)
}

// addURL records one already-extracted URL and reports whether it was new.
//
// The boolean is what lets a caller iterating imports skip a document it has already read without
// scanning what it has read so far, which is the difference between linear and quadratic work for
// an upload that produced many imports sharing one document.
func (s *errorsURLSet) addURL(errorsURL string) bool {
	errorsURL = strings.TrimSpace(errorsURL)
	if errorsURL == "" {
		return false
	}
	if _, duplicate := s.seen[errorsURL]; duplicate {
		return false
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{}, 1)
	}
	s.seen[errorsURL] = struct{}{}
	s.urls = append(s.urls, errorsURL)
	return true
}

// values are the distinct URLs, in the order they were first seen.
func (s *errorsURLSet) values() []string {
	return s.urls
}

// describeImport renders one import's outcome for an operator: the state, the row counters and a
// reference to the errors document, which together explain why a batch was not delivered cleanly.
//
// The errors document appears as an ORIGIN-ONLY reference, never as the URL SendGrid published.
// This text is not merely logged - it becomes PollStatusResponse.Error, which the batch router
// persists against every job of the batch on the terminal branch - and SendGrid may serve the
// document from object storage through a pre-signed URL whose query string is the credential
// authorizing the download, behind a path that can embed a contact identifier or a signed prefix.
// Persisting either would write a live secret, or personal data, into the jobs database, where it
// would outlive the log retention that at least bounds a leak to a log. The scheme and host are
// kept because they are what tells an operator whether the document is served by SendGrid itself
// or from object storage, and which host to pin; everything after them has to go. Reconciliation
// is unaffected: it receives the raw URL through FailedJobParameters.
func describeImport(importID string, importStatus *ImportStatusResponse) string {
	description := fmt.Sprintf(
		"import %s status %s (requested %d, created %d, updated %d, errored %d)",
		importID, importStatus.Status,
		importStatus.Results.RequestedCount, importStatus.Results.CreatedCount,
		importStatus.Results.UpdatedCount, importStatus.Results.ErroredCount,
	)
	if reference := redactedURLReference(importStatus.Results.ErrorsURL); reference != "" {
		description += ", errors document " + reference
	}
	return description
}

// importOutcome is one SendGrid import's observed state, paired with the errors document it
// published and the jobs it carried.
//
// It is the unit reconciliation works in, because the imports of one upload do not share a fate:
// one can be rejected outright while another completes cleanly and a third reports individual
// errored contacts.
//
// The JSON tags describe what Poll hands to GetUploadStats through
// common.PollStatusResponse.FailedJobParameters. That field travels between the two calls IN MEMORY
// and is never persisted or logged by the batch router, which is what makes it safe to carry the
// errors document URL verbatim: reconciliation needs the real URL in order to fetch the document,
// while everything durable records only the redacted reference.
type importOutcome struct {
	// ImportID is the SendGrid job_id the upsert returned for this import.
	ImportID string `json:"id"`

	// Status is the import state, already normalized: a completed import that nonetheless
	// reported errored contacts is recorded as errored, so that no reader has to re-apply that
	// defensive interpretation. An EMPTY value means the state is unknown - which is all a bare
	// list of errors document URLs can convey - and is treated as errored with a document, never
	// as a rejection, because an unknown state must not abort a job.
	Status string `json:"status,omitempty"`

	// ErrorsURL is the document describing this import's errored rows, exactly as SendGrid
	// published it, and empty when the import published none.
	ErrorsURL string `json:"errorsUrl,omitempty"`

	// Jobs is this import's membership in the manifest's range-collapsed form, such as
	// `1-500,507`. Empty means the membership is unknown, which callers must handle explicitly
	// rather than read as "this import carried no jobs".
	Jobs string `json:"jobs,omitempty"`

	// ErroredCount is the number of errored contacts SendGrid reported for this import. It is
	// carried for diagnosis only; no decision is ever taken from it, because the document itself
	// is the authority on which contacts failed.
	ErroredCount int `json:"erroredCount,omitempty"`
}

// jobIDs expands this import's membership back into job IDs, returning an empty slice when the
// membership is unknown.
func (o importOutcome) jobIDs() []int64 {
	return parseJobIDRanges(o.Jobs)
}

// hasUsableEvidence reports whether this import's INDIVIDUAL contacts can be settled from evidence
// the connector actually holds.
//
// A completed import settles them by itself: SendGrid stated that it finished with no errors at all,
// which is direct evidence about every contact in it. An errored import settles them through its
// errors document, provided it published one. A rejected import is deliberately NOT usable evidence
// in this sense - its jobs are settled by the import's own terminal state rather than by anything
// said about individual contacts - and neither is an errored import that published nothing.
func (o importOutcome) hasUsableEvidence() bool {
	switch o.Status {
	case importStatusCompleted:
		return true
	case importStatusFailed:
		return false
	default:
		return strings.TrimSpace(o.ErrorsURL) != ""
	}
}

// importOutcomeDocument is the envelope the outcomes travel in. An object rather than a bare array,
// so that the forwarded value is distinguishable at a glance from the newline-joined URL list the
// fallback produces.
type importOutcomeDocument struct {
	Imports []importOutcome `json:"imports"`
}

// renderImportOutcomes renders the per-import outcomes for
// common.PollStatusResponse.FailedJobParameters, falling back to a newline-joined list of errors
// document URLs.
//
// The fallback is not decoration. Reconciliation has to be able to proceed even if this render
// fails, and a URL list is sufficient for the ordinary single-import case; it is strictly less
// informative - states and membership are both lost, so no import can be treated as rejected - which
// is exactly why it is only a fallback.
func (b *SendGridBulkUploader) renderImportOutcomes(outcomes []importOutcome, errorsURLs []string) string {
	if len(outcomes) > 0 {
		rendered, err := jsonrs.Marshal(importOutcomeDocument{Imports: outcomes})
		if err == nil {
			return string(rendered)
		}
		b.Logger.Warnn("[sendgrid bulk upload] unable to render the import outcomes, falling back to the errors document urls",
			logger.NewStringField("errorClass", errorClass(err)))
	}
	return strings.Join(errorsURLs, errorsURLSeparator)
}

// parseImportOutcomes recovers the per-import outcomes Poll forwarded.
//
// The second return value distinguishes "this value is not an outcome document" from "it is one and
// it holds nothing", so that the caller falls through to the URL-list interpretation instead of
// reading an unparseable value as an upload with no imports.
func parseImportOutcomes(failedJobParameters string) ([]importOutcome, bool) {
	trimmed := strings.TrimSpace(failedJobParameters)
	// A URL list can never begin with a brace, and the document always does, so this one check
	// separates the two forms before either is decoded.
	if !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}
	var document importOutcomeDocument
	if err := jsonrs.Unmarshal([]byte(trimmed), &document); err != nil {
		return nil, false
	}
	outcomes := make([]importOutcome, 0, len(document.Imports))
	for _, outcome := range document.Imports {
		outcome.ImportID = strings.TrimSpace(outcome.ImportID)
		if outcome.ImportID == "" {
			continue
		}
		// Normalized on the way in as well as on the way out, so that a value written by any
		// build compares correctly against the state constants.
		outcome.Status = strings.ToLower(strings.TrimSpace(outcome.Status))
		outcomes = append(outcomes, outcome)
	}
	return outcomes, true
}

// importManifest is the decoded form of the import identifier the batch router persisted: every
// import one upload produced, in the order they were accepted, with the jobs each of them carried.
type importManifest struct {
	// importIDs are the SendGrid job_ids, de-duplicated and in their original order.
	importIDs []string

	// membership maps an import identifier to the jobs it carried. An identifier ABSENT from this
	// map has unknown membership, which is what both the degraded and the legacy encodings
	// produce and what every consumer has to handle explicitly.
	membership map[string][]int64
}

// outcomeOf pairs one import's observed status and status response with the membership this manifest
// carries for it.
func (m importManifest) outcomeOf(importID, status string, importStatus *ImportStatusResponse) importOutcome {
	outcome := importOutcome{
		ImportID: importID,
		Status:   status,
		Jobs:     renderJobIDRanges(m.membership[importID]),
	}
	if importStatus != nil {
		outcome.ErrorsURL = strings.TrimSpace(importStatus.Results.ErrorsURL)
		outcome.ErroredCount = importStatus.Results.ErroredCount
	}
	return outcome
}

// renderImportManifest renders every accepted import together with the jobs it carried, for
// common.ImportParameters.ImportId.
//
// Exceeding the size budget degrades to the identifier-only form rather than truncating the
// membership: a manifest that lists some imports' jobs and silently omits others would be read as
// authoritative, whereas "no membership at all" is a state reconciliation already handles
// explicitly and conservatively. See maxImportManifestBytes for why the budget exists.
//
// maxImportManifestBytes is a HARD ceiling here: this function never returns a longer string. Both
// degradation steps are bounded, and if even the identifier-only form would not fit, the empty
// string is returned rather than an oversized manifest. Upload treats that empty result as a
// failure to persist and releases the jobs as retryable, because persisting a manifest that
// exceeds the ceiling into the status row of every importing job is not an acceptable alternative
// and silently dropping identifiers is worse still - an import nobody polls resolves as a clean
// success for jobs it may never have delivered.
//
// Upload also charges each accepted identifier against the same ceiling as it packs, so the
// identifier-only form is guaranteed to fit and the empty return is unreachable in practice; it
// exists so that this function's bound holds for every caller regardless.
func renderImportManifest(importIDs []string, membership map[string][]int64) string {
	entries := make([]string, 0, len(importIDs))
	for _, importID := range importIDs {
		entry := importID
		if jobs := renderJobIDRanges(membership[importID]); jobs != "" {
			entry += importManifestMembershipMark + jobs
		}
		entries = append(entries, entry)
	}
	if rendered := strings.Join(entries, importManifestSeparator); len(rendered) <= maxImportManifestBytes {
		return rendered
	}
	if identifiersOnly := strings.Join(importIDs, importManifestSeparator); len(identifiersOnly) <= maxImportManifestBytes {
		return identifiersOnly
	}
	return ""
}

// parseImportManifest decodes the persisted import identifier back into imports and membership.
//
// Three encodings are accepted, and all of them decode through one path: the current
// `id=jobs;id=jobs` form, the degraded `id;id` form the size budget produces, and the `id:id` form
// an earlier build of this connector persisted. Nothing is rejected outright - an entry this
// function cannot read contributes what it can, because the alternative is discarding an import
// identifier without which its jobs could never be resolved at all.
func parseImportManifest(importID string) importManifest {
	manifest := importManifest{
		importIDs:  make([]string, 0, 1),
		membership: make(map[string][]int64),
	}
	// The legacy separator is folded into the current one first, so a colon-joined list of
	// identifiers decodes as a set of imports with unknown membership - which is precisely what
	// such a value carries. Neither character can occur inside a SendGrid job_id, and Upload
	// refuses to record one that contains either, so the fold cannot split an identifier.
	normalized := strings.ReplaceAll(importID, importIDSeparator, importManifestSeparator)
	for _, entry := range strings.Split(normalized, importManifestSeparator) {
		id, jobs, hasMembership := strings.Cut(strings.TrimSpace(entry), importManifestMembershipMark)
		if id = strings.TrimSpace(id); id == "" {
			continue
		}
		if !lo.Contains(manifest.importIDs, id) {
			manifest.importIDs = append(manifest.importIDs, id)
		}
		if !hasMembership {
			continue
		}
		if jobIDs := parseJobIDRanges(jobs); len(jobIDs) > 0 {
			// Merged rather than assigned, because SendGrid may answer two chunks of one upload
			// with the same import job_id, in which case both chunks' jobs belong to it.
			manifest.membership[id] = lo.Uniq(append(manifest.membership[id], jobIDs...))
		}
	}
	return manifest
}

// renderJobIDRanges renders an ascending, duplicate-free list of job IDs, collapsing every run of
// consecutive values into `first-last`.
//
// Collapsing is what makes the manifest small enough to persist at all. A chunk holds the jobs of
// consecutive staging-file lines, whose job IDs are consecutive integers, so the ordinary case
// renders in a couple of dozen bytes however many jobs the chunk carried - and the batch router
// stores this string once per importing job, so its length is multiplied by the size of the batch.
func renderJobIDRanges(jobIDs []int64) string {
	if len(jobIDs) == 0 {
		return ""
	}
	// Uniq copies, so the caller's slice is never reordered underneath it.
	sorted := lo.Uniq(jobIDs)
	slices.Sort(sorted)
	parts := make([]string, 0, len(sorted))
	for index := 0; index < len(sorted); {
		end := index
		for end+1 < len(sorted) && sorted[end+1] == sorted[end]+1 {
			end++
		}
		if end == index {
			parts = append(parts, strconv.FormatInt(sorted[index], 10))
		} else {
			parts = append(parts,
				strconv.FormatInt(sorted[index], 10)+importManifestRangeMark+strconv.FormatInt(sorted[end], 10))
		}
		index = end + 1
	}
	return strings.Join(parts, importManifestJobSeparator)
}

// parseJobIDRanges expands the rendered form back into job IDs, and it is deliberately STRICT: any
// element it cannot read, and any total it will not materialize, makes the WHOLE membership
// unreadable and yields nil.
//
// Strictness is the safety property here, and tolerance would be the bug. A partially recovered
// membership is worse than none at all, because every consumer would take the recovered part for the
// whole: the jobs quietly missing from it would be attributed to the wrong import and could then be
// marked delivered by exclusion against another import's errors document - which is precisely the
// silent loss the manifest exists to prevent. Nil, by contrast, means "membership unknown", a state
// every consumer already handles by refusing to succeed anything it cannot account for.
func parseJobIDRanges(value string) []int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	jobIDs := make([]int64, 0, 8)
	for _, part := range strings.Split(trimmed, importManifestJobSeparator) {
		first, last, isRange := strings.Cut(strings.TrimSpace(part), importManifestRangeMark)
		// A job ID is a positive integer, so anything else did not come from renderJobIDRanges.
		start, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
		if err != nil || start <= 0 {
			return nil
		}
		if !isRange {
			jobIDs = append(jobIDs, start)
			continue
		}
		end, err := strconv.ParseInt(strings.TrimSpace(last), 10, 64)
		if err != nil || end < start {
			return nil
		}
		// Bounded BEFORE the range is expanded, because a corrupted or hostile twenty-byte range
		// such as `1-999999999` would otherwise ask this process to materialize a slice of a
		// billion elements inside a shared batch router worker. The ceiling sits far above any
		// batch the router assembles, so no genuine membership is ever refused by it.
		if end-start >= maxImportMembershipJobs || len(jobIDs) >= maxImportMembershipJobs {
			return nil
		}
		// Appended THEN tested, so the terminal value is never incremented.
		//
		// The obvious `for jobID := start; jobID <= end; jobID++` does not terminate when end is
		// math.MaxInt64: incrementing it wraps to math.MinInt64, which is still <= end, so the loop
		// runs forever appending to a slice that grows until the process dies. The range-width guard
		// above does not catch it, because `end - start` for a single-value range such as
		// `9223372036854775807-9223372036854775807` is 0, well inside the ceiling. This value can
		// arrive here: the manifest is read back out of persisted job parameters, and a corrupted or
		// hostile parameter is exactly the input this parser exists to survive. Signed overflow is
		// not a theoretical concern in a loop whose bound is attacker-influenced - it is the bug.
		for jobID := start; ; jobID++ {
			jobIDs = append(jobIDs, jobID)
			if jobID == end {
				break
			}
		}
	}
	if len(jobIDs) > maxImportMembershipJobs {
		return nil
	}
	return lo.Uniq(jobIDs)
}

// isPersistableImportID reports whether an import identifier can be written into the manifest and
// read back unambiguously.
//
// Upload refuses an identifier that fails this test rather than persisting a manifest nothing can
// decode, which is what turns the encoding's one assumption - that a SendGrid job_id contains none
// of the delimiters - into something enforced rather than merely documented.
func isPersistableImportID(importID string) bool {
	if importID == "" || len(importID) > maxImportIDLength {
		return false
	}
	return !strings.ContainsAny(importID, importIDForbiddenCharacters)
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
// listIDsKey builds the batching key for a set of target list IDs.
//
// Length-prefixed rather than joined by a separator, because the key decides which contacts share a
// request and a collision therefore sends contacts to the WRONG SendGrid lists. A comma join has
// collisions: ["a,b"] and ["a","b"] both render "a,b", so a contact targeting one list whose ID
// happens to contain a comma was batched with - and delivered to - the two lists of an unrelated
// contact. Prefixing each ID with its own length makes the encoding injective, so distinct inputs
// cannot produce the same key whatever characters a list ID contains.
func listIDsKey(listIDs []string) string {
	var key strings.Builder
	for _, listID := range listIDs {
		key.WriteString(strconv.Itoa(len(listID)))
		key.WriteByte(':')
		key.WriteString(listID)
	}
	return key.String()
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
// scalarString renders a gjson value as a contact field value, or returns "" if it is not one.
//
// THIS GUARD IS THE POINT OF THE FUNCTION. gjson.Result.String() does not fail on a container: for
// an object or an array it returns the value's RAW JSON TEXT. So without this check a trait that
// arrived as `{"email":{"work":"a@b.com"}}` yielded the non-blank string `{"work":"a@b.com"}`, which
// satisfied every "is it set?" test in this package and was then sent to SendGrid AS the contact's
// email address - and, because an object is never blank, it also satisfied the identifier check
// that is supposed to reject a contact SendGrid cannot key on.
//
// Booleans are refused for the same reason: `true` is a well-formed string and a nonsense address,
// postal code or identifier, and a trait mapped to one is a mapping mistake rather than a value.
// Numbers ARE accepted, because numeric phone numbers, postal codes and external IDs are ordinary.
func scalarString(value gjson.Result) string {
	switch value.Type {
	case gjson.String, gjson.Number:
		return strings.TrimSpace(value.String())
	default:
		// gjson.JSON covers both objects and arrays; Null, True and False are not field values.
		return ""
	}
}

func firstString(source gjson.Result, paths ...string) string {
	for _, path := range paths {
		// A non-scalar at one path does not end the search: the next spelling of the same trait may
		// still carry a usable value, which is exactly what the alternative paths are for.
		if value := scalarString(source.Get(path)); value != "" {
			return value
		}
	}
	return ""
}

// stringSliceOf renders a JSON array as a slice of trimmed, non-blank strings, and a lone scalar
// as a single-element slice. It returns nil when nothing is left, so that an omitempty field
// stays omitted rather than overwriting stored data with an empty array.
// stringSliceOf collects the scalar values of a gjson value, at most limit of them.
//
// Every element passes the same scalar guard a single-valued field does, so a nested object inside
// an alternate-emails array cannot become an alternate email. The limit is a stop, not a truncation
// policy: callers pass one MORE than they will accept, so an over-long field is still visible as
// over-long to validation and the record is rejected rather than quietly shortened. It exists so a
// single record cannot make this function allocate in proportion to how many elements it declares.
func stringSliceOf(source gjson.Result, limit int) []string {
	if !source.Exists() || limit <= 0 {
		return nil
	}
	values := make([]string, 0, 1)
	if source.IsArray() {
		source.ForEach(func(_, element gjson.Result) bool {
			if value := scalarString(element); value != "" {
				values = append(values, value)
			}
			return len(values) < limit
		})
	} else if value := scalarString(source); value != "" {
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

// traitResolver resolves mapped trait names against ONE event, indexing each source object at most
// once however many trait names are looked up.
//
// gjson's Result.Map() re-scans the object and allocates a fresh map on EVERY call, so calling it
// per mapping cost one complete pass over the event for each configured custom field - work
// proportional to the number of mappings times the size of the event, for a key set that cannot
// change between lookups. Indexing once and reusing the index makes that work proportional to the
// event alone.
//
// The indexes are built LAZILY, and the message's index only when a name is not found among the
// traits, so the common case - every mapped trait present in traits - never indexes the message at
// all. The resolver is per-contact scratch state with no lifetime beyond one call, so it introduces
// no shared or cross-invocation state.
type traitResolver struct {
	// message and traits are the two source objects, in lookup order.
	message gjson.Result
	traits  gjson.Result

	// traitKeys and messageKeys are the materialized top-level key indexes, nil for a source that
	// is not an object.
	traitKeys   map[string]gjson.Result
	messageKeys map[string]gjson.Result

	// traitsIndexed and messageIndexed record that the corresponding index has been built, so that
	// a source with no keys - or one that is not an object at all - is not indexed again on every
	// lookup.
	traitsIndexed  bool
	messageIndexed bool
}

// newTraitResolver builds a resolver over one event's message and traits objects. Nothing is
// indexed until the first lookup asks for it.
func newTraitResolver(message, traits gjson.Result) *traitResolver {
	return &traitResolver{message: message, traits: traits}
}

// lookup resolves one mapped trait name against the event.
//
// The exact key is tried first, on the traits object and then on the message, which is what makes a
// trait whose own name contains a dot resolvable at all - a path lookup would read such a name as a
// nested path. Only then is the name treated as a path, which is what makes a mapping such as
// "address.city" work. The precedence is exactly the one the mapping documents, and it is unchanged
// by the indexing above.
func (r *traitResolver) lookup(traitName string) gjson.Result {
	if !r.traitsIndexed {
		r.traitKeys, r.traitsIndexed = topLevelKeysOf(r.traits), true
	}
	// Reading a nil map is legal in Go and simply misses, so a non-object source needs no guard.
	if value, ok := r.traitKeys[traitName]; ok {
		return value
	}
	if !r.messageIndexed {
		r.messageKeys, r.messageIndexed = topLevelKeysOf(r.message), true
	}
	if value, ok := r.messageKeys[traitName]; ok {
		return value
	}
	if value := firstResult(r.traits, traitName); value.Exists() {
		return value
	}
	return firstResult(r.message, traitName)
}

// topLevelKeysOf indexes an object's immediate keys, and returns nil for anything that is not an
// object - a scalar, an array, or an absent value - so that callers can read the result without a
// type check.
func topLevelKeysOf(source gjson.Result) map[string]gjson.Result {
	if !source.IsObject() {
		return nil
	}
	return source.Map()
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
