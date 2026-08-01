package sendgridbulkupload

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
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

const batchRouterModule = "batch_router"

// Request caps for PUT /v3/marketing/contacts. SendGrid accepts at most 30,000 contacts OR 6 MB per
// request, whichever is reached first, and the byte ceiling applies to the WHOLE serialized body.
// The envelope - the list IDs and the surrounding object - is therefore measured per request and
// subtracted from this budget rather than being covered by a guessed reserve.
const (
	defaultMaxContactsPerRequest = 30000
	defaultMaxRequestBytes       = 6_000_000

	configKeyMaxContactsPerRequest = "maxContactsPerRequest"
	configKeyMaxRequestBytes       = "maxRequestBytes"

	// Floors apply only to configured values; explicit instance overrides are clamped only to
	// provider maxima.
	minConfiguredContactsPerRequest = 1
	minConfiguredRequestBytes       = 1024
)

// maxStagingLineBytes bounds scanner memory for a single staged record.
const maxStagingLineBytes = 8 << 20

// Connector-local field bounds prevent one malformed value from causing SendGrid to reject a large
// batch; they are not provider schema limits.
const (
	maxEmailRunes             = 254
	maxPhoneNumberRunes       = 100
	maxIdentifierRunes        = 255
	maxContactFieldRunes      = 255
	maxAlternateEmails        = 50
	maxCustomFieldsPerContact = 128
	maxCustomFieldValueRunes  = 2000
	maxListIDsPerContact      = 64
	maxListIDRunes            = 128
)

// Bounds applied while the errors document is parsed. The adapter already bounds how many BYTES are
// read; these bound what the parser is willing to build out of them.
const (
	// maxErrorRowsPerDocument bounds parser work independently of the errors document's byte limit.
	maxErrorRowsPerDocument = 2 * defaultMaxContactsPerRequest
	// maxErrorRowMessageRunes bounds how much of a row's message is examined when deriving its error
	// class. The message itself is never stored.
	maxErrorRowMessageRunes = 512
)

// The SendGrid import states. The documented enumeration is exactly these four: pending is the only
// non-terminal one, completed means finished without any errors, errored means finished with some
// errors, and failed means finished with all errors or entirely unprocessable.
const (
	importStatusPending   = "pending"
	importStatusCompleted = "completed"
	importStatusErrored   = "errored"
	importStatusFailed    = "failed"
)

// externalIDTypeListIDs is the context.externalId entry type that carries per-event list targeting,
// which takes precedence over the destination configuration.
const externalIDTypeListIDs = "listIds"

// Error classes. This is a CLOSED vocabulary owned by this connector, and it is the only thing
// derived from a provider row message that is ever persisted.
//
// The reason is a privacy one, and it is deliberate: an errors document row is free-form third-party
// prose that may restate any contact field - a name, a street address, a phone number, a custom
// value - and a per-job failure reason is written into JobsDB, where it long outlives the delivery
// attempt. Classifying the message and storing only the class keeps the diagnosis useful while
// making it impossible for provider text to carry contact data into durable storage.
const (
	errorClassInvalidEmail        = "invalid_email"
	errorClassInvalidPhoneNumber  = "invalid_phone_number"
	errorClassCustomFieldRejected = "custom_field_rejected"
	errorClassListRejected        = "list_rejected"
	errorClassDuplicateContact    = "duplicate_contact"
	errorClassNotPermitted        = "not_permitted"
	errorClassUnspecified         = "unspecified"
)

// Every reason this connector reports for a job. All of them are written here, in this connector's
// own words, so that nothing a provider or a proxy chose can reach a log line or a JobsDB status.
const (
	reasonNoContactIdentifier  = "the event carries none of the contact identifiers sendgrid accepts (email, phone_number_id, external_id, anonymous_id)"
	reasonNonScalarField       = "the event maps an object or an array onto a sendgrid contact field, which can only carry a single value"
	reasonFieldTooLong         = "the event carries a contact field longer than this connector will send to sendgrid"
	reasonTooManyFieldValues   = "the event carries more values for a contact field than this connector will send to sendgrid"
	reasonTooManyListIDs       = "the event targets more sendgrid lists, or longer list identifiers, than this connector will send"
	reasonMalformedStagedEvent = "the staged event could not be read as a sendgrid contact"
	reasonContactTooLarge      = "the contact is larger than one sendgrid marketing contacts request can carry"

	reasonStagingFileUnreadable   = "the staging file for this batch could not be read; the affected jobs will be retried"
	reasonJobNotStaged            = "this job was not present in the staging file for its batch, so no contact could be built for it; the affected jobs will be retried"
	reasonContactUnserializable   = "the contact could not be serialized for sendgrid; the affected jobs will be retried"
	reasonEnvelopeTooLarge        = "the destination's sendgrid list identifiers alone exceed one request's byte budget, so no contact fits; the affected jobs will be retried once the destination is reconfigured"
	reasonUploadNotPlanned        = "the sendgrid marketing contacts upsert could not be prepared; the affected jobs will be retried"
	reasonUploadNotUsable         = "the sendgrid marketing contacts upsert did not produce a usable response; the affected jobs will be retried"
	reasonImportNotRecorded       = "the accepted sendgrid import could not be recorded for polling; the affected jobs will be retried"
	reasonMissingImportID         = "the importing jobs carry no sendgrid import identifier, so the import cannot be polled"
	reasonImportStatusUnavailable = "the sendgrid contacts import status could not be read; the affected jobs will be retried"
	reasonImportFailed            = "sendgrid reported that this import finished with all errors or was entirely unprocessable"
	reasonUnknownImportStatus     = "sendgrid reported an import status this connector does not recognize"
	reasonErrorsDocumentMissing   = "the sendgrid import published no errors document, so its rejected contacts cannot be identified"
	reasonErrorsDocumentFetch     = "the sendgrid import's errors document could not be fetched"
	reasonErrorsDocumentUnusable  = "the sendgrid import's errors document could not be read in any shape this connector understands"
	reasonJobUnreconcilable       = "this job's contact could not be rebuilt from its payload, so the import's outcome for it could not be established; the job will be retried"
)

// Local, per-record rejection causes. Each is permanent: the same event would fail identically on
// every retry, so the record is reported terminally instead of consuming the framework's retry
// budget, and the rest of the batch is unaffected.
var (
	errNoContactIdentifier      = errors.New(reasonNoContactIdentifier)
	errNonScalarField           = errors.New(reasonNonScalarField)
	errFieldTooLong             = errors.New(reasonFieldTooLong)
	errTooManyFieldValues       = errors.New(reasonTooManyFieldValues)
	errTooManyListIDs           = errors.New(reasonTooManyListIDs)
	errMalformedStagedEvent     = errors.New(reasonMalformedStagedEvent)
	errUnattributableStagedLine = errors.New("a staging file line carries no job id")
	errUnusableErrorsDocument   = errors.New(reasonErrorsDocumentUnusable)
)

// safeImportStatusToken bounds what an unrecognized status value may contribute to a reported reason:
// a short lower-case token and nothing else, so an unexpected response body cannot inject prose.
var safeImportStatusToken = regexp.MustCompile(`^[a-z0-9_]{1,32}$`)

// Compile-time proof that this manager satisfies the whole four-method async destination manager
// contract, so drift in the shared interface breaks the build here instead of surfacing as a runtime
// type error inside a batch router worker.
//
// common.SimpleAsyncDestinationManager is deliberately not used: its Poll reports completion
// unconditionally, whereas a SendGrid upsert only returns 202 Accepted and must be polled before
// anything is known about its outcome.
var _ common.AsyncDestinationManager = (*SendGridBulkUploader)(nil)

// NewManager validates destination configuration before constructing the adapter. Missing
// credentials or ambiguous custom-field mappings fail fast; nil observability dependencies use
// no-op implementations.
func NewManager(log logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*SendGridBulkUploader, error) {
	if destination == nil {
		return nil, errors.New("the sendgrid destination is nil")
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
	// The destination's identity is bound to the logger once, here, so that every line this
	// connector emits carries destinationId and destinationType - including lines written by helpers
	// that never receive a destination. destinationType comes from the destName constant rather than
	// from the destination definition, so the value can never drift from the registered name.
	sendGridLogger := log.Child("SendGridBulkUpload").Child("SendGridBulkUploader").
		Withn(obskit.DestinationID(destination.ID), obskit.DestinationType(destName))

	apiService, err := NewSendGridAPIService(destination.ID, destinationConfig, statsFactory)
	if err != nil {
		return nil, err
	}
	return &SendGridBulkUploader{
		Logger:             sendGridLogger,
		StatsFactory:       statsFactory,
		DestinationID:      destination.ID,
		DestinationConfig:  destinationConfig,
		SendGridAPIService: apiService,
	}, nil
}

// parseDestinationConfig converts the control plane's untyped configuration map into the typed view,
// through the mandated marshal-then-unmarshal round trip that every bulk-upload connector in this
// tree uses. Both codec failures are wrapped with %w, so the codec's own error stays reachable
// through errors.Is and errors.As instead of being flattened into a string at the first hop.
func parseDestinationConfig(destination *backendconfig.DestinationT) (DestinationConfig, error) {
	var destinationConfig DestinationConfig
	jsonConfig, err := jsonrs.Marshal(destination.Config)
	if err != nil {
		return destinationConfig, fmt.Errorf("error in marshalling destination config: %w", err)
	}
	if err := jsonrs.Unmarshal(jsonConfig, &destinationConfig); err != nil {
		return destinationConfig, fmt.Errorf("error in unmarshalling destination config: %w", err)
	}
	destinationConfig.APIKey = strings.TrimSpace(destinationConfig.APIKey)
	listIDs, err := normalizeListIDs(destinationConfig.ListIDs)
	if err != nil {
		return destinationConfig, fmt.Errorf("error in the destination's sendgrid list ids: %w", err)
	}
	destinationConfig.ListIDs = listIDs

	mapping, err := validateCustomFieldsMapping(destinationConfig.CustomFieldsMapping)
	if err != nil {
		return destinationConfig, err
	}
	destinationConfig.CustomFieldsMapping = mapping
	return destinationConfig, nil
}

// validateCustomFieldsMapping trims entries and rejects blank names/IDs or duplicate field IDs.
// Duplicate IDs would make the selected value depend on map iteration order.
func validateCustomFieldsMapping(mapping map[string]string) (map[string]string, error) {
	if len(mapping) == 0 {
		return map[string]string{}, nil
	}
	if len(mapping) > maxCustomFieldsPerContact {
		return nil, fmt.Errorf("the destination maps %d traits onto sendgrid custom fields, more than the %d this connector sends",
			len(mapping), maxCustomFieldsPerContact)
	}
	normalized := make(map[string]string, len(mapping))
	claimedBy := make(map[string]string, len(mapping))
	for trait, fieldID := range mapping {
		normalizedTrait := strings.TrimSpace(trait)
		normalizedFieldID := strings.TrimSpace(fieldID)
		if normalizedTrait == "" {
			return nil, fmt.Errorf("the destination maps a blank trait name onto sendgrid custom field %q", normalizedFieldID)
		}
		if normalizedFieldID == "" {
			return nil, fmt.Errorf("the destination maps trait %q onto a blank sendgrid custom field id", normalizedTrait)
		}
		if owner, taken := claimedBy[normalizedFieldID]; taken {
			return nil, fmt.Errorf("the destination maps both trait %q and trait %q onto sendgrid custom field %q",
				owner, normalizedTrait, normalizedFieldID)
		}
		claimedBy[normalizedFieldID] = normalizedTrait
		normalized[normalizedTrait] = normalizedFieldID
	}
	return normalized, nil
}

// normalizeListIDs returns a non-nil slice; an empty set is valid because SendGrid accepts upserts
// without list targeting.
func normalizeListIDs(listIDs []string) ([]string, error) {
	if len(listIDs) == 0 {
		return []string{}, nil
	}
	if len(listIDs) > maxListIDsPerContact {
		return nil, errTooManyListIDs
	}
	normalized := make([]string, 0, len(listIDs))
	for _, listID := range listIDs {
		trimmed := strings.TrimSpace(listID)
		if trimmed == "" {
			continue
		}
		if utf8.RuneCountInString(trimmed) > maxListIDRunes {
			return nil, errTooManyListIDs
		}
		normalized = append(normalized, trimmed)
	}
	if len(normalized) == 0 {
		return []string{}, nil
	}
	return lo.Uniq(normalized), nil
}

// Transform reduces one job to one staged line, delegating to the shared helper so the staged shape
// stays exactly the one the framework defines: {"message":{…},"metadata":{"job_id":N}}.
//
// It is event-type agnostic by construction: it copies the event body through untouched, so a track
// and an identify are staged identically and both reduce to a contact when the batch is uploaded.
func (*SendGridBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}

func (b *SendGridBulkUploader) log() logger.Logger {
	if b.Logger == nil {
		return logger.NOP
	}
	return b.Logger
}

func (b *SendGridBulkUploader) metrics() stats.Stats {
	if b.StatsFactory == nil {
		return stats.NOP
	}
	return b.StatsFactory
}

// maxContactsPerRequest resolves the element cap. The instance field is honoured as given, bounded
// only by the provider's own maximum; a configured value outside its accepted range falls back to
// the documented default.
func (b *SendGridBulkUploader) maxContactsPerRequest() int {
	if b.MaxContactsPerRequest > 0 {
		return min(b.MaxContactsPerRequest, defaultMaxContactsPerRequest)
	}
	configured := int(common.GetBatchRouterConfigInt64(configKeyMaxContactsPerRequest, destName, defaultMaxContactsPerRequest))
	if configured < minConfiguredContactsPerRequest || configured > defaultMaxContactsPerRequest {
		return defaultMaxContactsPerRequest
	}
	return configured
}

func (b *SendGridBulkUploader) maxRequestBytes() int {
	if b.MaxRequestBytes > 0 {
		return min(b.MaxRequestBytes, defaultMaxRequestBytes)
	}
	configured := int(common.GetBatchRouterConfigInt64(configKeyMaxRequestBytes, destName, defaultMaxRequestBytes))
	if configured < minConfiguredRequestBytes || configured > defaultMaxRequestBytes {
		return defaultMaxRequestBytes
	}
	return configured
}

func (b *SendGridBulkUploader) statLabels(destinationID string) stats.Tags {
	if destinationID == "" {
		destinationID = b.DestinationID
	}
	return stats.Tags{
		"module":   batchRouterModule,
		"destType": destName,
		"destID":   destinationID,
	}
}

func (b *SendGridBulkUploader) destinationIDOf(asyncDestStruct *common.AsyncDestinationStruct) string {
	if asyncDestStruct != nil && asyncDestStruct.Destination != nil && asyncDestStruct.Destination.ID != "" {
		return asyncDestStruct.Destination.ID
	}
	return b.DestinationID
}

// contactFields carries the reserved contact fields off an event, remembering the FIRST rejection it
// meets. It exists so the mapping below reads as one flat list of trait-to-field correspondences -
// which is what a reader compares against the destination's documented mapping - instead of a dozen
// error checks between the lines that do the mapping.
type contactFields struct {
	err error
}

// scalar resolves one field, and turns into a no-op once a field has been rejected, so the recorded
// error is always the first cause rather than the last field examined.
func (f *contactFields) scalar(value gjson.Result, maxRunes int) string {
	if f.err != nil {
		return ""
	}
	text, err := scalarField(value, maxRunes)
	if err != nil {
		f.err = err
		return ""
	}
	return text
}

// buildContact applies the documented reserved-field aliases. Nested address values take precedence,
// email is lowercased for provider/reconciliation consistency, and mapping is event-type agnostic.
func (b *SendGridBulkUploader) buildContact(message gjson.Result) (Contact, error) {
	traits := message.Get("traits")
	address := traits.Get("address")

	fields := &contactFields{}
	contact := Contact{
		Email:               strings.ToLower(fields.scalar(traits.Get("email"), maxEmailRunes)),
		PhoneNumberID:       fields.scalar(traits.Get("phone"), maxPhoneNumberRunes),
		ExternalID:          fields.scalar(message.Get("userId"), maxIdentifierRunes),
		AnonymousID:         fields.scalar(message.Get("anonymousId"), maxIdentifierRunes),
		FirstName:           fields.scalar(firstPresent(traits, "firstName", "first_name"), maxContactFieldRunes),
		LastName:            fields.scalar(firstPresent(traits, "lastName", "last_name"), maxContactFieldRunes),
		AddressLine1:        fields.scalar(coalesce(address.Get("street"), traits.Get("address_line_1")), maxContactFieldRunes),
		AddressLine2:        fields.scalar(traits.Get("address_line_2"), maxContactFieldRunes),
		City:                fields.scalar(coalesce(address.Get("city"), traits.Get("city")), maxContactFieldRunes),
		StateProvinceRegion: fields.scalar(address.Get("state"), maxContactFieldRunes),
		PostalCode:          fields.scalar(address.Get("postalCode"), maxContactFieldRunes),
		Country:             fields.scalar(address.Get("country"), maxContactFieldRunes),
		AlternateEmails:     fields.alternateEmails(firstPresent(traits, "alternateEmails", "alternate_emails")),
		CustomFields:        fields.customFields(b.DestinationConfig.CustomFieldsMapping, traits),
	}
	if fields.err != nil {
		return Contact{}, fields.err
	}

	// SendGrid requires at least one of these four on every contact, and a contact carrying none of
	// them would be rejected together with everything else in its request. It is therefore refused
	// locally, so one unusable event cannot poison a batch of up to 30,000.
	if contact.Email == "" && contact.PhoneNumberID == "" && contact.ExternalID == "" && contact.AnonymousID == "" {
		return Contact{}, errNoContactIdentifier
	}
	return contact, nil
}

// customFields resolves the configured trait-to-custom-field mapping against one event.
//
// SendGrid addresses custom fields by pre-created opaque IDs such as "w1", so only traits the
// operator mapped explicitly are sent; nothing is invented. The traits object is materialized once
// and then consulted for each mapping, so the work stays linear in the event rather than re-parsing
// per mapped trait.
func (f *contactFields) customFields(mapping map[string]string, traits gjson.Result) map[string]any {
	if f.err != nil || len(mapping) == 0 || !traits.IsObject() {
		return nil
	}
	values := traits.Map()
	customFields := make(map[string]any, len(mapping))
	for trait, fieldID := range mapping {
		value, present := values[trait]
		if !present || value.Type == gjson.Null {
			continue
		}
		if value.IsObject() || value.IsArray() {
			f.err = errNonScalarField
			return nil
		}
		switch value.Type {
		case gjson.Number:
			customFields[fieldID] = value.Num
		case gjson.True, gjson.False:
			customFields[fieldID] = value.Bool()
		default:
			text := strings.TrimSpace(value.String())
			if text == "" {
				continue
			}
			if utf8.RuneCountInString(text) > maxCustomFieldValueRunes {
				f.err = errFieldTooLong
				return nil
			}
			customFields[fieldID] = text
		}
	}
	if len(customFields) == 0 {
		return nil
	}
	return customFields
}

// resolveListIDs applies the documented list-targeting precedence: a per-event context.externalId
// entry of type "listIds" wins over the destination configuration, so one destination can target
// different lists per event.
func (b *SendGridBulkUploader) resolveListIDs(message gjson.Result) ([]string, error) {
	for _, externalID := range message.Get("context.externalId").Array() {
		if !strings.EqualFold(strings.TrimSpace(externalID.Get("type").String()), externalIDTypeListIDs) {
			continue
		}
		listIDs, err := normalizeListIDs(stringsOf(externalID.Get("id")))
		if err != nil {
			return nil, err
		}
		if len(listIDs) > 0 {
			return listIDs, nil
		}
	}
	return b.DestinationConfig.ListIDs, nil
}

func firstPresent(parent gjson.Result, keys ...string) gjson.Result {
	for _, key := range keys {
		if value := parent.Get(key); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

func coalesce(values ...gjson.Result) gjson.Result {
	for _, value := range values {
		if value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

// scalarField reduces one JSON value to the string a SendGrid contact field can carry. An object or
// an array is refused rather than flattened, and an over-long value is refused rather than truncated,
// because a silently altered contact field is worse than a reported rejection.
func scalarField(value gjson.Result, maxRunes int) (string, error) {
	if !value.Exists() || value.Type == gjson.Null {
		return "", nil
	}
	if value.IsObject() || value.IsArray() {
		return "", errNonScalarField
	}
	trimmed := strings.TrimSpace(value.String())
	if trimmed == "" {
		return "", nil
	}
	if utf8.RuneCountInString(trimmed) > maxRunes {
		return "", errFieldTooLong
	}
	return trimmed, nil
}

// alternateEmails resolves the secondary addresses a contact may carry, accepting either the
// documented array or a single value, and lower-casing each for the same reason the primary email is
// lower-cased.
func (f *contactFields) alternateEmails(value gjson.Result) []string {
	if f.err != nil || !value.Exists() || value.Type == gjson.Null {
		return nil
	}
	if !value.IsArray() {
		single := f.scalar(value, maxEmailRunes)
		if single == "" {
			return nil
		}
		return []string{strings.ToLower(single)}
	}
	entries := value.Array()
	if len(entries) > maxAlternateEmails {
		f.err = errTooManyFieldValues
		return nil
	}
	emails := make([]string, 0, len(entries))
	for _, entry := range entries {
		if email := f.scalar(entry, maxEmailRunes); email != "" {
			emails = append(emails, strings.ToLower(email))
		}
	}
	if f.err != nil || len(emails) == 0 {
		return nil
	}
	return lo.Uniq(emails)
}

func stringsOf(value gjson.Result) []string {
	if !value.Exists() {
		return nil
	}
	if !value.IsArray() {
		return []string{value.String()}
	}
	entries := value.Array()
	values := make([]string, 0, len(entries))
	for _, entry := range entries {
		values = append(values, entry.String())
	}
	return values
}

// contactGroup collects the contacts of one staging file that share a list-ID target, together with
// the jobs that produced them and each contact's serialized size. Grouping is required because
// list_ids applies to a whole request, so contacts targeting different lists cannot share one.
type contactGroup struct {
	listIDs  []string
	contacts []Contact
	jobIDs   []int64
	sizes    []int
}

type contactChunk struct {
	listIDs  []string
	contacts []Contact
	jobIDs   []int64
}

type stagedContacts struct {
	groups []*contactGroup
	index  map[string]int

	// abortedJobIDs are records that can never succeed, so they are reported terminally.
	abortedJobIDs []int64
	abortReasons  []string
	// failedJobIDs are records that failed for a reason that may not recur, so they are retried.
	failedJobIDs  []int64
	failedReasons []string
}

func (s *stagedContacts) abort(jobID int64, reason string) {
	s.abortedJobIDs = append(s.abortedJobIDs, jobID)
	s.abortReasons = appendUniqueReason(s.abortReasons, reason)
}

func (s *stagedContacts) fail(jobID int64, reason string) {
	s.failedJobIDs = append(s.failedJobIDs, jobID)
	s.failedReasons = appendUniqueReason(s.failedReasons, reason)
}

func (s *stagedContacts) add(listIDs []string, contact Contact, jobID int64, size int) {
	key := listIDsKey(listIDs)
	position, found := s.index[key]
	if !found {
		s.groups = append(s.groups, &contactGroup{listIDs: listIDs})
		position = len(s.groups) - 1
		s.index[key] = position
	}
	group := s.groups[position]
	group.contacts = append(group.contacts, contact)
	group.jobIDs = append(group.jobIDs, jobID)
	group.sizes = append(group.sizes, size)
}

func (s *stagedContacts) acceptedJobIDs() []int64 {
	jobIDs := make([]int64, 0, len(s.groups))
	for _, group := range s.groups {
		jobIDs = append(jobIDs, group.jobIDs...)
	}
	return jobIDs
}

// listIDsKey is the grouping key for a set of list IDs. It is the serialized form rather than a
// joined string, so two different targets can never collide through a separator that happens to
// appear inside a list identifier.
func listIDsKey(listIDs []string) string {
	if len(listIDs) == 0 {
		return "[]"
	}
	key, err := jsonrs.Marshal(listIDs)
	if err != nil {
		return strings.Join(listIDs, "\x00")
	}
	return string(key)
}

func appendUniqueReason(reasons []string, reason string) []string {
	if reason == "" || slices.Contains(reasons, reason) {
		return reasons
	}
	return append(reasons, reason)
}

// parseStagingLine splits one staged line into the job it belongs to and the event it carries.
//
// A line that cannot even be attributed to a job is a different kind of problem from a line that is
// merely unusable: reporting the former against job ID 0 would name a job that does not exist while
// leaving the real one unaccounted for, so the caller fails the whole batch instead.
func parseStagingLine(line []byte) (int64, gjson.Result, error) {
	if !gjson.ValidBytes(line) {
		return 0, gjson.Result{}, errUnattributableStagedLine
	}
	staged := gjson.ParseBytes(line)
	jobID := staged.Get("metadata.job_id")
	if jobID.Type != gjson.Number || jobID.Int() <= 0 {
		return 0, gjson.Result{}, errUnattributableStagedLine
	}
	message := staged.Get("message")
	if !message.IsObject() {
		return jobID.Int(), gjson.Result{}, errMalformedStagedEvent
	}
	return jobID.Int(), message, nil
}

// readStagedContacts isolates attributable record errors; file/scan failures and lines without a job
// ID fail the batch because no real job can be named safely.
func (b *SendGridBulkUploader) readStagedContacts(filePath string, statLabels stats.Tags) (*stagedContacts, error) {
	file, err := os.Open(filePath)
	if err != nil {
		// The error is NOT wrapped: it carries the absolute staging path, which is internal
		// filesystem detail that has no place in a log line or a job status.
		return nil, fmt.Errorf("opening the sendgrid staging file failed (error class: %s)", stagingErrorClass(err))
	}
	defer func() { _ = file.Close() }()

	staged := &stagedContacts{index: make(map[string]int, 1)}
	contactSize := b.metrics().NewTaggedStat("sendgrid_contact_size", stats.HistogramType, statLabels)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, maxStagingLineBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		jobID, message, err := parseStagingLine(line)
		if err != nil {
			if jobID == 0 {
				return nil, errUnattributableStagedLine
			}
			staged.abort(jobID, reasonMalformedStagedEvent)
			continue
		}
		contact, err := b.buildContact(message)
		if err != nil {
			staged.abort(jobID, contactRejectionReason(err))
			continue
		}
		listIDs, err := b.resolveListIDs(message)
		if err != nil {
			staged.abort(jobID, contactRejectionReason(err))
			continue
		}
		// Serialized exactly once per contact. The length is both what the size histogram reports and
		// what the chunker packs against, so the metric and the byte budget are one measurement.
		contactJSON, err := jsonrs.Marshal(contact)
		if err != nil {
			staged.fail(jobID, reasonContactUnserializable)
			continue
		}
		contactSize.Observe(float64(len(contactJSON)))
		staged.add(listIDs, contact, jobID, len(contactJSON))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading the sendgrid staging file failed (error class: %s)", stagingErrorClass(err))
	}
	return staged, nil
}

func stagingErrorClass(err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "missing_file"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	case errors.Is(err, bufio.ErrTooLong):
		return "line_too_long"
	default:
		return "read_failed"
	}
}

func contactRejectionReason(err error) string {
	for _, cause := range []error{
		errNoContactIdentifier, errNonScalarField, errFieldTooLong, errTooManyFieldValues, errTooManyListIDs,
	} {
		if errors.Is(err, cause) {
			return cause.Error()
		}
	}
	return reasonMalformedStagedEvent
}

type chunkPlan struct {
	chunks []contactChunk
	// oversizedJobIDs are contacts larger than one whole request's budget: permanent.
	oversizedJobIDs []int64
	// untargetableJobIDs are contacts whose group's list IDs alone exhaust the request budget, which
	// only a configuration change can fix, so they are retried rather than discarded.
	untargetableJobIDs []int64
}

// planChunks splits every group into request-sized chunks against both caps.
//
// The byte budget handed to the chunker is the request cap MINUS the measured envelope, because
// SendGrid charges its 6 MB ceiling for the whole body: the list_ids array and the surrounding
// object are part of it. Measuring costs one marshal of an empty request per group and is exact,
// whereas a fixed reserve is either too small - producing a body within the element cap but over the
// wire limit, which SendGrid rejects wholesale - or too large, splitting batches that would have fit.
func (b *SendGridBulkUploader) planChunks(groups []*contactGroup) (chunkPlan, error) {
	var plan chunkPlan
	maxBytes := b.maxRequestBytes()
	maxContacts := b.maxContactsPerRequest()
	for _, group := range groups {
		envelope, err := requestEnvelopeBytes(group.listIDs)
		if err != nil {
			return chunkPlan{}, err
		}
		budget := maxBytes - envelope
		if budget <= 0 {
			plan.untargetableJobIDs = append(plan.untargetableJobIDs, group.jobIDs...)
			continue
		}
		contactChunks, jobIDChunks, oversized := chunkBySizeAndElements(
			group.contacts, group.jobIDs, group.sizes, budget, maxContacts)
		plan.oversizedJobIDs = append(plan.oversizedJobIDs, oversized...)
		for index := range contactChunks {
			plan.chunks = append(plan.chunks, contactChunk{
				listIDs:  group.listIDs,
				contacts: contactChunks[index],
				jobIDs:   jobIDChunks[index],
			})
		}
	}
	// Largest chunk first. Exactly one chunk is uploaded per invocation, because the framework
	// persists exactly one import identifier per upload, so sending the biggest one first makes the
	// most progress per attempt. The sort is stable, so the order stays deterministic.
	slices.SortStableFunc(plan.chunks, func(left, right contactChunk) int {
		return len(right.contacts) - len(left.contacts)
	})
	return plan, nil
}

// requestEnvelopeBytes measures the fixed cost of one upsert body: everything the byte ceiling is
// charged for that is not a contact. The empty-but-non-nil contact slice is deliberate - it
// serializes as "contacts":[], so the measurement includes both brackets and no phantom element.
func requestEnvelopeBytes(listIDs []string) (int, error) {
	envelope, err := jsonrs.Marshal(UpsertRequest{ListIDs: listIDs, Contacts: []Contact{}})
	if err != nil {
		return 0, fmt.Errorf("measuring the sendgrid request envelope: %w", err)
	}
	return len(envelope), nil
}

// chunkBySizeAndElements packs contacts into chunks that respect both caps, returning index-aligned
// contact and jobID chunks so a single request's jobs can always be named exactly.
//
// A contact too large for any chunk is isolated rather than allowed to wedge the loop, and its jobID
// is retained so the record is accounted for instead of silently disappearing.
func chunkBySizeAndElements(contacts []Contact, jobIDs []int64, sizes []int, maxBytes, maxElements int) ([][]Contact, [][]int64, []int64) {
	var (
		contactChunks [][]Contact
		jobIDChunks   [][]int64
		oversized     []int64
		chunkContacts = make([]Contact, 0, 1)
		chunkJobIDs   = make([]int64, 0, 1)
		chunkSize     int
	)
	flush := func() {
		if len(chunkContacts) == 0 {
			return
		}
		contactChunks = append(contactChunks, chunkContacts)
		jobIDChunks = append(jobIDChunks, chunkJobIDs)
		chunkContacts = make([]Contact, 0, 1)
		chunkJobIDs = make([]int64, 0, 1)
		chunkSize = 0
	}
	for index := range contacts {
		// Reserve one separator byte per contact; the first contact in each chunk is conservatively
		// over-counted by one byte.
		contactSize := sizes[index] + 1
		if contactSize > maxBytes {
			oversized = append(oversized, jobIDs[index])
			continue
		}
		if chunkSize+contactSize >= maxBytes || len(chunkContacts) == maxElements {
			flush()
		}
		chunkContacts = append(chunkContacts, contacts[index])
		chunkJobIDs = append(chunkJobIDs, jobIDs[index])
		chunkSize += contactSize
	}
	flush()
	return contactChunks, jobIDChunks, oversized
}

type uploadOutcome struct {
	importingJobIDs  []int64
	importParameters []byte
	failedJobIDs     []int64
	failedReasons    []string
	abortedJobIDs    []int64
	abortReasons     []string
	// batchJobIDs is every job the batch router claims for this upload. It is the reference the
	// completeness sweep in uploadOutput measures the three outcome sets against, so it has to be
	// set on every path that produces an outcome.
	batchJobIDs []int64
}

// Upload sends one request's worth of contacts to SendGrid and reports what happened to every job in
// the batch.
//
// EXACTLY ONE import per invocation. The framework persists one import identifier per upload -
// common.ImportParameters.ImportId, which the router reads back with gjson to poll - so a second
// accepted import would have nowhere to live and its jobs could never be polled. Any further chunk
// is therefore deferred on the retryable channel and picked up by a later batch, which keeps the
// persisted identifier exactly the string SendGrid issued.
func (b *SendGridBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	destinationID := b.destinationIDOf(asyncDestStruct)
	statLabels := b.statLabels(destinationID)

	staged, err := b.readStagedContacts(asyncDestStruct.FileName, statLabels)
	if err != nil {
		b.log().Errorn("[sendgrid bulk upload] the staging file for this batch could not be read", obskit.Error(err))
		return b.uploadOutput(destinationID, statLabels, uploadOutcome{
			batchJobIDs:   asyncDestStruct.ImportingJobIDs,
			failedJobIDs:  asyncDestStruct.ImportingJobIDs,
			failedReasons: []string{reasonStagingFileUnreadable},
		})
	}
	outcome := uploadOutcome{
		batchJobIDs:   asyncDestStruct.ImportingJobIDs,
		failedJobIDs:  staged.failedJobIDs,
		failedReasons: staged.failedReasons,
		abortedJobIDs: staged.abortedJobIDs,
		abortReasons:  staged.abortReasons,
	}

	plan, err := b.planChunks(staged.groups)
	if err != nil {
		b.log().Errorn("[sendgrid bulk upload] the contacts upsert could not be planned", obskit.Error(err))
		outcome.failedJobIDs = append(outcome.failedJobIDs, staged.acceptedJobIDs()...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, reasonUploadNotPlanned)
		return b.uploadOutput(destinationID, statLabels, outcome)
	}
	if len(plan.oversizedJobIDs) > 0 {
		outcome.abortedJobIDs = append(outcome.abortedJobIDs, plan.oversizedJobIDs...)
		outcome.abortReasons = appendUniqueReason(outcome.abortReasons, reasonContactTooLarge)
	}
	if len(plan.untargetableJobIDs) > 0 {
		outcome.failedJobIDs = append(outcome.failedJobIDs, plan.untargetableJobIDs...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, reasonEnvelopeTooLarge)
	}
	if len(plan.chunks) == 0 {
		return b.uploadOutput(destinationID, statLabels, outcome)
	}

	chunk := plan.chunks[0]
	deferred := deferredJobIDs(plan.chunks[1:])

	// Counted before the call, so the metric reports requests ATTEMPTED rather than requests
	// accepted: a rate-limited or rejected request is still a request that happened.
	b.metrics().NewTaggedStat("sendgrid_upload_request_count", stats.CountType, statLabels).Count(1)
	upsert, err := b.SendGridAPIService.UploadContacts(UpsertRequest{ListIDs: chunk.listIDs, Contacts: chunk.contacts})
	if err != nil {
		var rateLimit *RateLimitError
		if errors.As(err, &rateLimit) {
			b.metrics().NewTaggedStat("sendgrid_rate_limited_request_count", stats.CountType, statLabels).Count(1)
		}
		reason := uploadFailureReason(err)
		// Logged as the connector's own reason rather than as the raw error, so no provider or proxy
		// text - which may restate contact data - reaches the logs.
		b.log().Warnn("[sendgrid bulk upload] sendgrid did not accept the contacts upsert",
			logger.NewIntField("contactCount", int64(len(chunk.contacts))),
			logger.NewStringField("reason", reason))
		outcome.failedJobIDs = append(outcome.failedJobIDs, chunk.jobIDs...)
		outcome.failedJobIDs = append(outcome.failedJobIDs, deferred...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, reason)
		return b.uploadOutput(destinationID, statLabels, outcome)
	}

	// The chunk's job count is computed BEFORE the parameters are marshalled, so importCount is the
	// number of jobs this import actually covers - which is what the router uses to fetch exactly
	// that many importing jobs when it polls.
	importParameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId:    upsert.JobID,
		ImportCount: len(chunk.jobIDs),
	})
	if err != nil {
		b.log().Errorn("[sendgrid bulk upload] the accepted import could not be recorded", obskit.Error(err))
		outcome.failedJobIDs = append(outcome.failedJobIDs, chunk.jobIDs...)
		outcome.failedJobIDs = append(outcome.failedJobIDs, deferred...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, reasonImportNotRecorded)
		return b.uploadOutput(destinationID, statLabels, outcome)
	}
	outcome.importingJobIDs = chunk.jobIDs
	outcome.importParameters = importParameters
	if len(deferred) > 0 {
		outcome.failedJobIDs = append(outcome.failedJobIDs, deferred...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, b.deferralReason(len(plan.chunks)))
	}
	b.log().Infon("[sendgrid bulk upload] sendgrid accepted a contacts import",
		logger.NewIntField("contactCount", int64(len(chunk.contacts))),
		logger.NewIntField("deferredJobCount", int64(len(deferred))),
		logger.NewIntField("requestCount", int64(len(plan.chunks))))
	return b.uploadOutput(destinationID, statLabels, outcome)
}

func deferredJobIDs(chunks []contactChunk) []int64 {
	jobIDs := make([]int64, 0, len(chunks))
	for _, chunk := range chunks {
		jobIDs = append(jobIDs, chunk.jobIDs...)
	}
	return jobIDs
}

func (b *SendGridBulkUploader) deferralReason(requestCount int) string {
	return fmt.Sprintf("deferred to a later upload: sendgrid accepts at most %d contacts and %d bytes per request, so this batch needs %d requests and the batch router records one import per upload",
		b.maxContactsPerRequest(), b.maxRequestBytes(), requestCount)
}

// uploadFailureReason renders why an upsert did not succeed, in this connector's own words. Nothing a
// provider chose is repeated: a rate limit contributes its numeric window, any other rejection
// contributes its HTTP status, and anything else is reported generically.
func uploadFailureReason(err error) string {
	var rateLimit *RateLimitError
	if errors.As(err, &rateLimit) {
		return rateLimit.Error() + "; the affected jobs will be retried"
	}
	var apiError *APIError
	if errors.As(err, &apiError) {
		return apiError.Error() + "; the affected jobs will be retried"
	}
	return reasonUploadNotUsable
}

// uploadOutput reduces an outcome to the framework's output shape, keeping the three job sets
// disjoint and leaving no importing state behind when nothing was accepted - which is what releases
// rate-limited jobs for a later attempt instead of stranding the destination in "importing".
func (b *SendGridBulkUploader) uploadOutput(destinationID string, statLabels stats.Tags, outcome uploadOutcome) common.AsyncUploadOutput {
	importing := lo.Uniq(outcome.importingJobIDs)
	failed, _ := lo.Difference(lo.Uniq(outcome.failedJobIDs), importing)
	aborted, _ := lo.Difference(lo.Uniq(outcome.abortedJobIDs), append(slices.Clone(importing), failed...))

	// Completeness sweep. The batch router writes a status ONLY for the jobs this output names, so a
	// job of the batch that landed in none of the three sets would receive no status at all and stay
	// unresolved - invisibly, because nothing ever failed. The router builds the batch from the very
	// lines it wrote, so the two should always agree; the sweep is here for when they do not, which
	// is what a truncated or partially written staging file looks like. Such a job is RETRYABLE and
	// never aborted, because a line missing from the file says nothing permanent about the job that
	// produced it, and it is disjoint from the other sets by construction.
	settled := append(append(slices.Clone(importing), failed...), aborted...)
	if unaccounted, _ := lo.Difference(lo.Uniq(outcome.batchJobIDs), settled); len(unaccounted) > 0 {
		b.log().Errorn("[sendgrid bulk upload] jobs in this batch were not present in the staging file",
			logger.NewIntField("unaccountedJobCount", int64(len(unaccounted))))
		b.metrics().NewTaggedStat("sendgrid_unaccounted_job_count", stats.CountType, statLabels).Count(len(unaccounted))
		failed = append(failed, unaccounted...)
		outcome.failedReasons = appendUniqueReason(outcome.failedReasons, reasonJobNotStaged)
	}

	if len(importing) > 0 {
		b.metrics().NewTaggedStat("sendgrid_importing_job_count", stats.CountType, statLabels).Count(len(importing))
	}
	if len(failed) > 0 {
		b.metrics().NewTaggedStat("sendgrid_failed_job_count", stats.CountType, statLabels).Count(len(failed))
	}
	if len(aborted) > 0 {
		b.metrics().NewTaggedStat("sendgrid_aborted_job_count", stats.CountType, statLabels).Count(len(aborted))
	}

	output := common.AsyncUploadOutput{
		DestinationID:   destinationID,
		ImportingJobIDs: importing,
		ImportingCount:  len(importing),
		FailedJobIDs:    failed,
		FailedReason:    strings.Join(outcome.failedReasons, "; "),
		FailedCount:     len(failed),
		AbortJobIDs:     aborted,
		AbortReason:     strings.Join(outcome.abortReasons, "; "),
		AbortCount:      len(aborted),
	}
	// Importing parameters exist only when something is actually importing: the router treats them
	// plus a non-empty importing set as "upload in progress", and anything else as "release the
	// batch", which is exactly the behaviour a rate-limited upload needs.
	if len(importing) > 0 {
		output.ImportingParameters = outcome.importParameters
	}
	return output
}

// Poll reads one import's status and maps it onto the framework's poll response. It performs exactly
// one provider request, so a single poll can never monopolize the router's polling loop, and it never
// retries: the batch router owns retry, backoff and circuit-breaking.
func (b *SendGridBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	importID := strings.TrimSpace(pollInput.ImportId)
	if importID == "" {
		return common.PollStatusResponse{StatusCode: http.StatusInternalServerError, Error: reasonMissingImportID}
	}
	importStatus, err := b.SendGridAPIService.GetImportStatus(importID)
	if err != nil {
		response := pollErrorResponse(err)
		b.log().Warnn("[sendgrid bulk upload] the contacts import status could not be read",
			logger.NewIntField("statusCode", int64(response.StatusCode)),
			logger.NewStringField("reason", response.Error))
		return response
	}
	switch normalizeImportStatus(importStatus.Status) {
	case importStatusPending:
		return common.PollStatusResponse{StatusCode: http.StatusOK, InProgress: true}
	case importStatusCompleted:
		if importStatus.Results.ErroredCount <= 0 {
			return common.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}
		}
		// completed is documented as "finished without any errors", so a non-zero errored count means
		// the status and the counts disagree. Reconciling is the only safe reading: the alternative
		// branch has the router mark EVERY importing job succeeded wholesale.
		b.log().Warnn("[sendgrid bulk upload] a completed contacts import reported errored rows",
			logger.NewIntField("erroredCount", int64(importStatus.Results.ErroredCount)))
		return partialFailureResponse(importStatus)
	case importStatusErrored:
		return partialFailureResponse(importStatus)
	case importStatusFailed:
		// Terminal on purpose: SendGrid defines failed as finished with all errors or entirely
		// unprocessable, which retrying cannot change, and 400 is the framework's terminal path.
		return common.PollStatusResponse{
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      reasonImportFailed,
		}
	default:
		return common.PollStatusResponse{
			StatusCode: http.StatusInternalServerError,
			Error:      unknownImportStatusReason(importStatus.Status),
		}
	}
}

// partialFailureResponse routes an import that finished with some errors into reconciliation.
//
// HasFailed is what makes the router call GetUploadStats instead of marking every importing job
// succeeded, and the errors document URL is forwarded verbatim because SendGrid publishes it. That
// URL never reaches JobsDB: the router hands it straight to GetUploadStats.
func partialFailureResponse(importStatus *ImportStatusResponse) common.PollStatusResponse {
	return common.PollStatusResponse{
		StatusCode:          http.StatusOK,
		Complete:            true,
		HasFailed:           true,
		FailedJobParameters: strings.TrimSpace(importStatus.Results.ErrorsURL),
	}
}

func pollErrorResponse(err error) common.PollStatusResponse {
	var rateLimit *RateLimitError
	if errors.As(err, &rateLimit) {
		return common.PollStatusResponse{StatusCode: http.StatusTooManyRequests, Error: rateLimit.Error()}
	}
	var apiError *APIError
	if errors.As(err, &apiError) {
		return common.PollStatusResponse{StatusCode: http.StatusInternalServerError, Error: apiError.Error()}
	}
	return common.PollStatusResponse{StatusCode: http.StatusInternalServerError, Error: reasonImportStatusUnavailable}
}

func normalizeImportStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

// unknownImportStatusReason names an unrecognized status only when the value is a short token, so an
// unexpected response cannot inject arbitrary text into a reported reason.
func unknownImportStatusReason(status string) string {
	normalized := normalizeImportStatus(status)
	if !safeImportStatusToken.MatchString(normalized) {
		return reasonUnknownImportStatus
	}
	return reasonUnknownImportStatus + ": " + normalized
}

// GetUploadStats reconciles one finished import against the jobs that fed it.
//
// It is STATELESS: everything it needs is re-derived from the importing jobs the router hands it, so
// it stays correct when it runs in a different process invocation, or on a different pod, from the
// Upload that created the import.
//
// The reconciliation rule is exact. Rows this connector can attribute fail their jobs, retryably.
// Rows it cannot attribute are counted and reported but change NO other job's outcome - an
// unattributable row is not evidence that some other contact failed. Every importing job that no row
// named succeeded, which is what lets one import yield both outcomes at once - except for a job no
// identifier could be derived from, which no row could have named and which is therefore retried
// rather than counted as delivered.
func (b *SendGridBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	statLabels := b.statLabels(b.DestinationID)

	errorsURL, reason := b.resolveErrorsURL(input)
	if reason != "" {
		b.log().Errorn("[sendgrid bulk upload] the import's errors document could not be located",
			logger.NewStringField("reason", reason))
		return common.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: reason}
	}
	document, err := b.SendGridAPIService.GetImportErrors(errorsURL)
	if err != nil {
		b.log().Errorn("[sendgrid bulk upload] the import's errors document could not be fetched",
			logger.NewStringField("reason", reasonErrorsDocumentFetch))
		return common.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: reasonErrorsDocumentFetch}
	}
	rows, err := parseImportErrorRows(document)
	if err != nil {
		// Deliberately 500 rather than 200 with an empty failed set: a document nothing could be read
		// out of is not evidence that every contact was delivered, so the framework retries instead.
		b.log().Errorn("[sendgrid bulk upload] the import's errors document could not be parsed",
			logger.NewStringField("reason", reasonErrorsDocumentUnusable),
			logger.NewIntField("documentBytes", int64(len(document))))
		return common.GetUploadStatsResponse{StatusCode: http.StatusInternalServerError, Error: reasonErrorsDocumentUnusable}
	}

	index, unreconcilable := b.buildImportingIndex(input.ImportingList)
	if len(unreconcilable) > 0 {
		// A job whose contact cannot be rebuilt cannot be looked up in the index, so no row can ever
		// name it. Retrying it is the only honest outcome: leaving it out of the failed set would
		// hand it to the succeeded-by-exclusion loop below and report a delivery this connector
		// cannot establish.
		b.metrics().NewTaggedStat("sendgrid_unreconcilable_importing_job_count", stats.CountType, statLabels).
			Count(len(unreconcilable))
		b.log().Warnn("[sendgrid bulk upload] importing jobs could not be indexed for reconciliation",
			logger.NewIntField("unreconcilableJobCount", int64(len(unreconcilable))),
			logger.NewIntField("importingJobCount", int64(len(input.ImportingList))))
	}
	failedClasses := make(map[int64]string, len(rows))
	unmatched := 0
	for _, row := range rows {
		jobIDs := index[reconciliationKey(row.Identifier)]
		if len(jobIDs) == 0 {
			unmatched++
			continue
		}
		class := classifyRowMessage(row.Message)
		for _, jobID := range jobIDs {
			if existing, known := failedClasses[jobID]; !known || existing == errorClassUnspecified {
				failedClasses[jobID] = class
			}
		}
	}
	if unmatched > 0 {
		// Counted and reported, never silently dropped, so a change in the document's undocumented
		// shape becomes visible in metrics rather than as quiet data loss. The identifiers themselves
		// are not logged: they are contact PII.
		b.metrics().NewTaggedStat("sendgrid_unmatched_error_row_count", stats.CountType, statLabels).Count(unmatched)
		b.log().Warnn("[sendgrid bulk upload] errors document rows could not be attributed to an importing job",
			logger.NewIntField("unmatchedRowCount", int64(unmatched)),
			logger.NewIntField("rowCount", int64(len(rows))),
			logger.NewIntField("importingJobCount", int64(len(input.ImportingList))))
	}

	failedCount := len(failedClasses) + len(unreconcilable)
	metadata := common.EventStatMeta{
		FailedKeys:     make([]int64, 0, failedCount),
		AbortedKeys:    make([]int64, 0),
		WarningKeys:    make([]int64, 0),
		SucceededKeys:  make([]int64, 0, len(input.ImportingList)),
		FailedReasons:  make(map[int64]string, failedCount),
		AbortedReasons: make(map[int64]string),
		WarningReasons: make(map[int64]string),
	}
	for _, job := range input.ImportingList {
		if job == nil {
			continue
		}
		if class, failed := failedClasses[job.JobID]; failed {
			metadata.FailedKeys = append(metadata.FailedKeys, job.JobID)
			metadata.FailedReasons[job.JobID] = rowFailureReason(class)
			continue
		}
		if _, unindexable := unreconcilable[job.JobID]; unindexable {
			metadata.FailedKeys = append(metadata.FailedKeys, job.JobID)
			metadata.FailedReasons[job.JobID] = reasonJobUnreconcilable
			continue
		}
		metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
	}
	b.metrics().NewTaggedStat("sendgrid_reconciled_failed_job_count", stats.CountType, statLabels).
		Count(len(metadata.FailedKeys))
	b.metrics().NewTaggedStat("sendgrid_reconciled_succeeded_job_count", stats.CountType, statLabels).
		Count(len(metadata.SucceededKeys))

	// 200 is required: any other status makes the router discard the whole reconciliation.
	return common.GetUploadStatsResponse{StatusCode: http.StatusOK, Metadata: metadata}
}

// resolveErrorsURL finds the document to reconcile against, falling back to a status read when the
// poll response did not carry one. It returns a connector-owned reason instead of an error, because
// that reason is what the caller reports.
func (b *SendGridBulkUploader) resolveErrorsURL(input common.GetUploadStatsInput) (string, string) {
	if errorsURL := strings.TrimSpace(input.FailedJobParameters); errorsURL != "" {
		return errorsURL, ""
	}
	var parameters struct {
		ImportID string `json:"importId"`
	}
	if err := jsonrs.Unmarshal(input.Parameters, &parameters); err != nil {
		return "", reasonMissingImportID
	}
	importID := strings.TrimSpace(parameters.ImportID)
	if importID == "" {
		return "", reasonMissingImportID
	}
	importStatus, err := b.SendGridAPIService.GetImportStatus(importID)
	if err != nil {
		return "", reasonImportStatusUnavailable
	}
	errorsURL := strings.TrimSpace(importStatus.Results.ErrorsURL)
	if errorsURL == "" {
		return "", reasonErrorsDocumentMissing
	}
	return errorsURL, ""
}

// buildImportingIndex maps every identifier an importing job's contact carries onto that job, and
// names separately the jobs no identifier could be derived for.
//
// Construction is linear: an identifier's jobIDs are appended without scanning what is already
// there, so an import in which many jobs share an identifier cannot turn this into quadratic work.
// An identifier claimed by several jobs fails all of them, because each of them did send the contact
// SendGrid rejected.
//
// A job that yields no identifier is returned rather than dropped. It cannot be looked up, so no
// error row can ever name it, and reporting it succeeded because nothing named it would claim a
// delivery this connector never established.
func (b *SendGridBulkUploader) buildImportingIndex(importingList []*jobsdb.JobT) (map[string][]int64, map[int64]struct{}) {
	index := make(map[string][]int64, len(importingList))
	unreconcilable := make(map[int64]struct{})
	for _, job := range importingList {
		if job == nil {
			continue
		}
		contact, err := b.buildContact(stagedMessage(job.EventPayload))
		if err != nil {
			unreconcilable[job.JobID] = struct{}{}
			continue
		}
		indexed := false
		for _, identifier := range identifiersOf(contact) {
			key := reconciliationKey(identifier)
			if key == "" {
				continue
			}
			index[key] = append(index[key], job.JobID)
			indexed = true
		}
		if !indexed {
			unreconcilable[job.JobID] = struct{}{}
		}
	}
	return index, unreconcilable
}

func identifiersOf(contact Contact) []string {
	identifiers := make([]string, 0, 4)
	for _, identifier := range []string{contact.Email, contact.ExternalID, contact.AnonymousID, contact.PhoneNumberID} {
		if strings.TrimSpace(identifier) != "" {
			identifiers = append(identifiers, identifier)
		}
	}
	return lo.Uniq(identifiers)
}

// reconciliationKey normalizes an identifier for matching. Both sides are lower-cased regardless of
// SendGrid's own lower-casing of the email field, so a difference in case can never split a match.
func reconciliationKey(identifier string) string {
	key := strings.ToLower(strings.TrimSpace(identifier))
	if key == "" || utf8.RuneCountInString(key) > maxIdentifierRunes {
		return ""
	}
	return key
}

// stagedMessage extracts the event from an importing job's payload, reading it from the same place
// Transform does. The fallbacks keep reconciliation working if the job hands over an already staged
// line or a bare event instead.
func stagedMessage(payload []byte) gjson.Result {
	if message := gjson.GetBytes(payload, "body.JSON"); message.IsObject() {
		return message
	}
	if message := gjson.GetBytes(payload, "message"); message.IsObject() {
		return message
	}
	return gjson.ParseBytes(payload)
}

// parseImportErrorRows accepts a bare array, an errors/results wrapper, a lone row object, or NDJSON
// because the provider does not publish the document schema. The adapter already bounds total bytes;
// this function also bounds how many entries it is willing to examine.
func parseImportErrorRows(document []byte) ([]ImportErrorRow, error) {
	trimmed := bytes.TrimSpace(document)
	if len(trimmed) == 0 {
		return nil, errUnusableErrorsDocument
	}
	switch trimmed[0] {
	case '[':
		if !gjson.ValidBytes(trimmed) {
			return nil, errUnusableErrorsDocument
		}
		return rowsOf(gjson.ParseBytes(trimmed))
	case '{':
		if !gjson.ValidBytes(trimmed) {
			// A stream of objects, one per line, is not valid JSON as a whole document.
			return newlineDelimitedRows(trimmed)
		}
		parsed := gjson.ParseBytes(trimmed)
		for _, key := range []string{"errors", "results"} {
			if wrapped := parsed.Get(key); wrapped.IsArray() {
				return rowsOf(wrapped)
			}
		}
		// An import that rejected a single contact can publish that one row on its own, which is
		// valid JSON as a whole document and therefore never reaches the NDJSON branch. Reading it
		// as a one-row document is what keeps such an import reconcilable: the alternative reports
		// the document unusable, and this route has no retry budget to escalate, so the import would
		// be polled and rejected for as long as it exists.
		if row, usable := importErrorRowFrom(parsed); usable {
			return []ImportErrorRow{row}, nil
		}
		return nil, errUnusableErrorsDocument
	default:
		return nil, errUnusableErrorsDocument
	}
}

// rowsOf reads the rows of one array. The row budget is charged per entry SCANNED rather than per row
// materialized, so a document made almost entirely of entries this connector cannot use costs the
// same bounded work as one made of usable rows.
func rowsOf(array gjson.Result) ([]ImportErrorRow, error) {
	rows := make([]ImportErrorRow, 0, 8)
	scanned := 0
	array.ForEach(func(_, entry gjson.Result) bool {
		scanned++
		if row, usable := importErrorRowFrom(entry); usable {
			rows = append(rows, row)
		}
		return scanned < maxErrorRowsPerDocument
	})
	if len(rows) == 0 {
		return nil, errUnusableErrorsDocument
	}
	return rows, nil
}

// newlineDelimitedRows reads one row per line, charging the same per-scanned-entry budget: every
// non-blank line counts, whether or not it yields a usable row.
func newlineDelimitedRows(document []byte) ([]ImportErrorRow, error) {
	rows := make([]ImportErrorRow, 0, 8)
	scanner := bufio.NewScanner(bytes.NewReader(document))
	scanner.Buffer(nil, maxStagingLineBytes)
	scanned := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if !gjson.ValidBytes(line) {
			return nil, errUnusableErrorsDocument
		}
		scanned++
		if row, usable := importErrorRowFrom(gjson.ParseBytes(line)); usable {
			rows = append(rows, row)
		}
		if scanned >= maxErrorRowsPerDocument {
			break
		}
	}
	if err := scanner.Err(); err != nil || len(rows) == 0 {
		return nil, errUnusableErrorsDocument
	}
	return rows, nil
}

func importErrorRowFrom(entry gjson.Result) (ImportErrorRow, bool) {
	if !entry.IsObject() {
		return ImportErrorRow{}, false
	}
	row := ImportErrorRow{
		Identifier: boundedString(firstPresent(entry, "email", "contact.email", "identifier", "external_id", "anonymous_id"), maxIdentifierRunes),
		Message:    boundedString(firstPresent(entry, "message", "error_message", "reason", "detail"), maxErrorRowMessageRunes),
	}
	if row.Identifier == "" && row.Message == "" {
		return ImportErrorRow{}, false
	}
	return row, true
}

// boundedString reads a scalar and truncates it to a bounded number of runes. Truncation is safe here
// - unlike in a contact field - because these values are only matched and classified, never sent.
func boundedString(value gjson.Result, maxRunes int) string {
	if !value.Exists() || value.IsObject() || value.IsArray() {
		return ""
	}
	text := strings.TrimSpace(value.String())
	if utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	return string([]rune(text)[:maxRunes])
}

// errorClassKeywords is the precedence this connector classifies a row message by, most specific
// first. The order is part of the contract rather than an accident of where a case was written,
// because a provider message frequently names more than one thing: "invalid email address for list
// 123" mentions both an email and a list, and it describes the email.
//
// Specificity is what orders the table. A multi-word phrase ("custom field") can only be about that
// subject. An authorization or duplication verdict describes the whole operation and is unambiguous
// whichever field it names. A field word ("phone", "email") identifies the rejected value. And
// "list" ranks last because it is the weakest signal of the set: it is a short, common word that
// appears in prose about failures of every other kind.
var errorClassKeywords = []struct {
	class    string
	keywords []string
}{
	{errorClassCustomFieldRejected, []string{"custom field", "custom_field"}},
	{errorClassNotPermitted, []string{"permission", "not authorized", "unauthorized", "forbidden"}},
	{errorClassDuplicateContact, []string{"duplicate", "already exists"}},
	{errorClassInvalidPhoneNumber, []string{"phone"}},
	{errorClassInvalidEmail, []string{"email"}},
	{errorClassListRejected, []string{"list"}},
}

// classifyRowMessage reduces a provider row message to one of this connector's error classes,
// scanning errorClassKeywords in order so a message matching several classes always resolves to the
// most specific one.
//
// This is the ONLY use ever made of that message: the class is what a job's failure reason carries,
// so free-form provider prose - which may restate a name, an address or any custom value - never
// reaches durable storage. An unrecognized message is reported as unspecified rather than repeated.
func classifyRowMessage(message string) string {
	lowered := strings.ToLower(message)
	if lowered == "" {
		return errorClassUnspecified
	}
	for _, candidate := range errorClassKeywords {
		for _, keyword := range candidate.keywords {
			if strings.Contains(lowered, keyword) {
				return candidate.class
			}
		}
	}
	return errorClassUnspecified
}

// rowFailureReason is the per-job reason a reconciled failure carries. It is retryable by design: the
// router maps FailedKeys onto the non-terminal failed state and gives up on its own schedule.
func rowFailureReason(class string) string {
	return fmt.Sprintf("sendgrid rejected this contact during the marketing contacts import (error class: %s)", class)
}
