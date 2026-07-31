// Package sendgridbulkupload_test exercises the SENDGRID_BULK_UPLOAD connector through the
// four-method async destination manager contract the batch router actually calls, with the
// SendGrid Marketing Contacts API replaced by the generated mock of the SendGridAPIService
// seam.
//
// It is an EXTERNAL test package on purpose. Reaching the connector only through its exported
// surface is what keeps these tests a description of the contract the batch router depends on
// rather than of the implementation that happens to satisfy it today, and it is why the
// uploader's fields and both request caps are exported in the first place.
//
// Three properties of the suite are deliberate and load-bearing:
//
//   - NOTHING TOUCHES THE NETWORK. Every SendGrid response - an accepted upsert, a rate limit,
//     a partially errored import, an unparseable errors document - is expressed as a mock
//     expectation on the three-method seam. No local HTTP server is stood up and no socket is
//     opened, because needing one would mean the seam was drawn in the wrong place.
//   - NO PACKAGE-LEVEL MUTABLE STATE EXISTS. The repository runs its suites with -shuffle=on,
//     so every case builds its own destination, its own uploader and its own mock through the
//     helpers below, and the two fixtures under testdata are only ever read.
//   - THE ASSERTIONS PIN THE CONTRACT, NOT THE PROSE. Where the batch router reads a value with
//     gjson, the test reads it back the same way; where SendGrid nests a counter inside a
//     results object, the test feeds the connector a verbatim wire body and lets the decoder
//     prove it reads the nesting.
package sendgridbulkupload_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockAPIService "github.com/rudderlabs/rudder-server/mocks/router/sendgridbulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	sendgridbulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sendgrid-bulk-upload"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	// destinationDefinitionName is the registered destination-definition name, and it is
	// byte-critical: the processor's batch-destination catalog, the batch router's async
	// destination list and the manager factory all compare it as a plain string, so a typo
	// would produce neither a compile error nor a failing assertion anywhere else - the
	// destination would simply never be routed.
	//
	// It is emphatically NOT the pre-existing "SENDGRID" cloud destination, which is a
	// different, synchronously delivered destination and is untouched by this connector.
	destinationDefinitionName = "SENDGRID_BULK_UPLOAD"

	// testDestinationID is the destination ID every outcome has to be reported against,
	// because the batch router keys its own bookkeeping on it.
	testDestinationID = "1"

	// testAPIKey is an obviously fake credential. It matches none of the shapes a real
	// SendGrid key has - it is not an "SG.<id>.<secret>" value - so it can never be mistaken
	// for a live secret and cannot be redacted out of a reason by the sanitization gate.
	testAPIKey = "test-api-key"

	// testEventListID is the list ID the staging fixture's own events target through
	// context.externalId, and testConfigListID is a different one used to prove that the
	// per-event value takes precedence over the destination configuration.
	testEventListID  = "037ae8d4-25b4-496e-adff-2fded15fd0c5"
	testConfigListID = "9d1a5f0e-3c47-4b28-8f61-7bd0c2a4e913"

	// testImportID is the job_id SendGrid returns for an accepted upsert, and testErrorsURL
	// is the errors document it publishes for a partially errored import.
	testImportID  = "sendgrid-import-0001"
	testErrorsURL = "https://api.sendgrid.com/v3/marketing/contacts/imports/sendgrid-import-0001/errors"

	// stagingFixture and errorsFixture are the two read-only documents under testdata. They
	// are never written to: a test needing a mutable staging file builds one under
	// t.TempDir() instead.
	stagingFixture = "uploadData.jsonl"
	errorsFixture  = "errors.json"

	// matchedErrorsDocument is the errors document of an import whose every errored row
	// resolves to an importing job: the staging fixture's blake@example.com (job 2) and
	// devon@example.com (job 4), the latter reported under the nested contact.email spelling.
	//
	// It is built here rather than read from testdata because the committed fixture also
	// carries a deliberately UNMATCHED row, which drives the fail-closed branch instead. Both
	// behaviours have to be pinned, so each gets the document that exhibits it.
	matchedErrorsDocument = `{"errors":[` +
		`{"message":"Invalid email address provided for contact.","email":"blake@example.com"},` +
		`{"error_message":"Contact rejected: custom field value exceeds the maximum allowed length.","contact":{"email":"devon@example.com"}}` +
		`]}`
)

// newDestination builds the destination the control plane would deliver, with the supplied
// configuration map.
//
// It is a function and not a package-level variable precisely so that no case can mutate the
// destination another case is reading, which is what makes the suite safe under -shuffle=on.
func newDestination(destinationConfig map[string]any) *backendconfig.DestinationT {
	return &backendconfig.DestinationT{
		ID:   testDestinationID,
		Name: destinationDefinitionName,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: destinationDefinitionName,
		},
		Config:      destinationConfig,
		Enabled:     true,
		WorkspaceID: "1",
	}
}

// newDestinationConfig is the configuration a correctly configured destination carries: the
// bearer credential, the fallback list IDs and the explicit trait-to-custom-field mapping
// SendGrid requires because it addresses custom fields by pre-created opaque IDs such as "w1".
//
// A fresh map is returned on every call so that a case may adjust its own copy freely.
func newDestinationConfig() map[string]any {
	return map[string]any{
		"apiKey":  testAPIKey,
		"listIds": []any{testEventListID},
		"customFieldsMapping": map[string]any{
			"plan":       "w1",
			"signedUpAt": "w2",
		},
	}
}

// newMockAPIService builds the generated SendGridAPIService mock with a controller bound to the
// running test.
//
// Cleanup is registered with t.Cleanup rather than deferred, per the repository's testing
// guidelines. go.uber.org/mock also registers Finish itself, so this is belt and braces - and
// it is what makes the expectation counts in these tests, "exactly one upload call" above all,
// actually enforced rather than merely stated.
func newMockAPIService(t *testing.T) *mockAPIService.MockSendGridAPIService {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mockAPIService.NewMockSendGridAPIService(ctrl)
}

// newUploader builds the manager through its real constructor and then substitutes the mocked
// API service for the HTTP adapter the constructor built.
//
// Going through NewManager rather than assembling a struct literal is deliberate: every case
// then also exercises the real configuration parsing, the real logger and stats defaulting and
// the real cap resolution, so a regression in any of them cannot hide behind a hand-built
// uploader.
func newUploader(
	t *testing.T,
	apiService sendgridbulkupload.SendGridAPIService,
	destinationConfig map[string]any,
) *sendgridbulkupload.SendGridBulkUploader {
	t.Helper()
	return newUploaderWithStats(t, apiService, destinationConfig, stats.NOP)
}

// newUploaderWithStats is newUploader with an explicit stats factory, for the cases that have
// to prove a measurement was actually emitted rather than merely that a value was returned.
func newUploaderWithStats(
	t *testing.T,
	apiService sendgridbulkupload.SendGridAPIService,
	destinationConfig map[string]any,
	statsFactory stats.Stats,
) *sendgridbulkupload.SendGridBulkUploader {
	t.Helper()
	uploader, err := sendgridbulkupload.NewManager(logger.NOP, statsFactory, newDestination(destinationConfig))
	require.NoError(t, err)
	require.NotNil(t, uploader)
	uploader.SendGridAPIService = apiService
	return uploader
}

// statLabels are the tags every measurement this connector emits carries. destType comes from
// the registered destination-definition name rather than from the destination's own name, so
// renaming a destination cannot move its metrics.
func statLabels() stats.Tags {
	return stats.Tags{
		"module":   "batch_router",
		"destType": destinationDefinitionName,
		"destID":   testDestinationID,
	}
}

// stagingFixtureLines reads the read-only staging fixture and returns its non-blank lines, which
// are the very lines Upload consumes when it is pointed at fixtureStagingFile.
func stagingFixtureLines(t *testing.T) []string {
	t.Helper()
	content, err := os.ReadFile(fixtureStagingFile())
	require.NoError(t, err)
	lines := make([]string, 0, 8)
	for _, line := range strings.Split(string(content), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	require.NotEmpty(t, lines, "fixture %s is empty", stagingFixture)
	return lines
}

// errorsFixtureDocument reads the read-only errors-document fixture verbatim, so the tolerant
// parser is exercised against the exact bytes the repository commits rather than a paraphrase.
func errorsFixtureDocument(t *testing.T) []byte {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", errorsFixture))
	require.NoError(t, err)
	require.NotEmpty(t, content)
	return content
}

// fixtureStagingFile is the staging fixture's path, which Upload reads exactly as the batch
// router's own staging file. It is opened read-only and never modified.
func fixtureStagingFile() string {
	return filepath.Join("testdata", stagingFixture)
}

// writeStagingFile materializes a staging file from the given lines under the test's own
// temporary directory, which the framework removes for us.
//
// testdata is never written to: the fixtures there are shared by every case in the package and
// a test that mutated one would make the suite order-dependent.
func writeStagingFile(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "staging.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	return path
}

// stagingLine renders one staging-file line the way Transform does, through the very same
// shared helper.
//
// Building the line with the helper rather than by hand is what guarantees these tests cannot
// drift from the metadata key the connector reads back: the key is job_id, and it is written in
// exactly one place in the repository.
func stagingLine(t *testing.T, jobID int64, message string) string {
	t.Helper()
	line, err := common.GetMarshalledData(message, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, gjson.Get(line, "metadata.job_id").Int())
	return line
}

// requestRecorder captures the upsert requests the mocked API service received, in call order.
//
// The chunker is unexported, which is exactly right - it is an implementation detail - so the
// only honest way to assert its behaviour is through the requests it causes. Recording them is
// what makes chunk sizes, chunk counts and the index alignment between contacts and job IDs
// observable from outside the package.
//
// It is guarded by a mutex even though Upload issues its requests sequentially, so that the
// suite stays correct under -race if the connector ever parallelizes them.
type requestRecorder struct {
	mu       sync.Mutex
	requests []sendgridbulkupload.UpsertRequest
}

// record captures one request.
func (r *requestRecorder) record(request sendgridbulkupload.UpsertRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, request)
}

// all returns a copy of the captured requests, so a caller can never mutate the recording.
func (r *requestRecorder) all() []sendgridbulkupload.UpsertRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sendgridbulkupload.UpsertRequest{}, r.requests...)
}

// contactsIn renders the number of contacts each captured request carried, in call order. It is
// the chunk-size sequence the chunker produced, and asserting on it is also how these tests
// prove an empty chunk is never emitted.
func (r *requestRecorder) contactsIn() []int {
	requests := r.all()
	sizes := make([]int, 0, len(requests))
	for _, request := range requests {
		sizes = append(sizes, len(request.Contacts))
	}
	return sizes
}

// contactSize is the number of bytes one contact costs a request body as the chunker accounts
// for it: its serialized length plus the byte for the comma that separates it from its
// neighbour.
//
// Measuring it here with the repository's mandated codec, rather than hard-coding a number, is
// what lets the byte-cap boundary cases be expressed exactly - one below, exactly at, one above
// - without materializing megabytes of fixture data.
func contactSize(t *testing.T, contact sendgridbulkupload.Contact) int {
	t.Helper()
	encoded, err := jsonrs.Marshal(contact)
	require.NoError(t, err)
	return len(encoded) + 1
}

// uniformContact is the contact the nth uniform staging line produces. Every one of them
// serializes to the same number of bytes, which is what makes the byte-cap arithmetic in the
// chunker tests exact.
func uniformContact(index int) sendgridbulkupload.Contact {
	return sendgridbulkupload.Contact{
		Email:      fmt.Sprintf("u%02d@example.com", index),
		ExternalID: fmt.Sprintf("user_%02d", index),
	}
}

// uniformStagingLines renders count staging lines, numbered from one, whose contacts all
// serialize to an identical size and whose external IDs encode the job that produced them.
//
// Encoding the job ID in the contact is what makes the chunker's index alignment provable from
// outside the package: a captured chunk's contacts can be turned back into the job IDs the
// uploader must have reported for that same chunk.
func uniformStagingLines(t *testing.T, count int) []string {
	t.Helper()
	require.LessOrEqual(t, count, 99, "uniform lines are numbered with two digits to keep every contact the same size")
	lines := make([]string, 0, count)
	for index := 1; index <= count; index++ {
		contact := uniformContact(index)
		lines = append(lines, stagingLine(t, int64(index), fmt.Sprintf(
			`{"type":"identify","userId":%q,"traits":{"email":%q}}`, contact.ExternalID, contact.Email,
		)))
	}
	return lines
}

// jobIDsOf recovers the job IDs a captured request's contacts were produced from, using the
// external ID uniformStagingLines encodes them in.
//
// This is the index-alignment assertion: the chunker flushes its contact and job-ID chunks
// together, so the job IDs recovered from a chunk's contacts must be exactly the job IDs the
// uploader reported for that chunk - importing when it was accepted, retryable when it was not.
func jobIDsOf(t *testing.T, request sendgridbulkupload.UpsertRequest) []int64 {
	t.Helper()
	jobIDs := make([]int64, 0, len(request.Contacts))
	for _, contact := range request.Contacts {
		jobID, err := strconv.ParseInt(strings.TrimPrefix(contact.ExternalID, "user_"), 10, 64)
		require.NoError(t, err, "contact %q carries no recoverable job id", contact.Email)
		jobIDs = append(jobIDs, jobID)
	}
	return jobIDs
}

// importingJobs rebuilds the importing job list the batch router hands to reconciliation from
// the staging lines an upload was built from.
//
// The payload is shaped {"body":{"JSON":{...}}} because that is how an event is queued and where
// both Transform and reconciliation read the message from. Deriving the list from the same lines
// the upload used is what makes the identifiers in the errors document line up with real jobs,
// exactly as they would in production - and it is also the point of the design: reconciliation
// keeps no state from the upload and re-derives every identifier from the jobs it is handed.
func importingJobs(t *testing.T, lines []string) []*jobsdb.JobT {
	t.Helper()
	jobs := make([]*jobsdb.JobT, 0, len(lines))
	for _, line := range lines {
		jobID := gjson.Get(line, "metadata.job_id").Int()
		require.NotZero(t, jobID, "staging line carries no metadata.job_id")
		payload, err := jsonrs.Marshal(map[string]any{
			"body": map[string]any{"JSON": gjson.Get(line, "message").Value()},
		})
		require.NoError(t, err)
		jobs = append(jobs, &jobsdb.JobT{JobID: jobID, EventPayload: payload})
	}
	return jobs
}

// jobIDsIn lists the job IDs of an importing list, which is the population every reconciliation
// outcome has to account for exactly once.
func jobIDsIn(jobs []*jobsdb.JobT) []int64 {
	jobIDs := make([]int64, 0, len(jobs))
	for _, job := range jobs {
		if job == nil {
			continue
		}
		jobIDs = append(jobIDs, job.JobID)
	}
	return jobIDs
}

// requireDisjoint asserts that two outcome sets share no job.
//
// It is the invariant that keeps a job from being reported twice - importing and failed, or
// failed and aborted - to a batch router that writes one status per job it is told about.
func requireDisjoint(t *testing.T, first, second []int64) {
	t.Helper()
	for _, jobID := range first {
		require.NotContains(t, second, jobID, "job %d was reported in two outcome sets at once", jobID)
	}
}

// statusFromWireBody decodes a VERBATIM SendGrid import-status body into the connector's own
// response type.
//
// Every poll case is driven through this helper rather than through a hand-built struct, and
// that is the single most valuable decision in this file. SendGrid reports errored_count and
// errors_url INSIDE a nested "results" object; a flattened response type would still unmarshal
// without any error and would then read errored_count as 0 forever, so every partially failed
// import would be reported as a clean success and the rejected contacts would be marked
// delivered. Decoding the real body makes that impossible to regress silently.
func statusFromWireBody(t *testing.T, body string) *sendgridbulkupload.ImportStatusResponse {
	t.Helper()
	var status sendgridbulkupload.ImportStatusResponse
	require.NoError(t, jsonrs.Unmarshal([]byte(body), &status))
	return &status
}

// importStatusBody renders a SendGrid import-status body with the counters and the errors
// document URL in the nested results object the API actually uses.
func importStatusBody(importID, status string, erroredCount int, errorsURL string) string {
	const requestedCount = 5
	createdCount := max(requestedCount-erroredCount, 0)
	return fmt.Sprintf(
		`{"id":%q,"status":%q,"job_type":"upsert","results":{"requested_count":%d,"created_count":%d,"updated_count":0,"deleted_count":0,"errored_count":%d,"errors_url":%q},"started_at":"2026-02-25T12:00:00Z","finished_at":"2026-02-25T12:00:30Z"}`,
		importID, status, requestedCount, createdCount, erroredCount, errorsURL,
	)
}

// importParameters marshals the import parameters the batch router persists, so that the
// reconciliation cases can be handed exactly what the router would hand them.
func importParameters(t *testing.T, importID string, importCount int) []byte {
	t.Helper()
	parameters, err := jsonrs.Marshal(common.ImportParameters{ImportId: importID, ImportCount: importCount})
	require.NoError(t, err)
	return parameters
}

// newRateLimitError is the typed error the API adapter returns for a 429, carrying the reset
// window SendGrid advertised.
//
// resetAt is an absolute UNIX timestamp in SECONDS, not a delay: X-RateLimit-Reset is an instant,
// and reading it as a duration would produce a wait of decades.
func newRateLimitError(resetAt int64) *sendgridbulkupload.RateLimitError {
	return &sendgridbulkupload.RateLimitError{
		StatusCode: 429,
		RetryAfter: "30",
		ResetAt:    resetAt,
		Limit:      600,
		Remaining:  0,
		Message:    "too many requests",
	}
}

// resetWindow renders a rate-limit reset instant the way the typed error does, so that a test
// can assert the advertised window actually reached the recorded failure reason.
//
// It survives the connector's sanitization gate intact - an RFC3339 instant carries only eight
// consecutive digits, and neither "T" nor ":" is a digit separator the personal-data patterns
// accept - which is what keeps a rate-limit reason actionable for an operator.
func resetWindow(resetAt int64) string {
	return time.Unix(resetAt, 0).UTC().Format(time.RFC3339)
}

// TestRegistration pins the three registrations that make this connector reachable at all.
//
// This is the feature's highest-impact failure mode and the one no other assertion in this file
// would catch: with any single one of them missing the package still compiles, the manager still
// satisfies its interface and every scenario below still passes, while the destination silently
// never executes. Each entry is a plain string comparison in a different file, so only a test
// that names the registered value can protect them.
func TestRegistration(t *testing.T) {
	t.Parallel()

	t.Run("the processor routes this destination to the batch router", func(t *testing.T) {
		t.Parallel()

		// Without this entry the processor writes these jobs to the REGULAR router's queue and
		// the batch router never receives them.
		require.Contains(t, misc.BatchDestinations(), destinationDefinitionName)
	})

	t.Run("the batch router classifies this destination as asynchronous", func(t *testing.T) {
		t.Parallel()

		// Without these the async upload worker early-returns, the batch router rejects the
		// type outright and the factory's classifier never reaches the manager switch.
		require.True(t, common.IsAsyncDestination(destinationDefinitionName))
		require.True(t, common.IsAsyncRegularDestination(destinationDefinitionName))
		require.False(t, common.IsSFTPDestination(destinationDefinitionName))
	})

	t.Run("the manager factory constructs this connector", func(t *testing.T) {
		t.Parallel()

		manager, err := asyncdestinationmanager.NewManager(
			config.New(),
			logger.NOP,
			stats.NOP,
			newDestination(newDestinationConfig()),
			nil, // the backend config client is unused: SendGrid authenticates with a static bearer key
		)
		require.NoError(t, err, "the factory must not fall through to its invalid destination type error")
		require.NotNil(t, manager)
		require.IsType(t, &sendgridbulkupload.SendGridBulkUploader{}, manager)
	})

	t.Run("the uploader satisfies the full four method manager contract", func(t *testing.T) {
		t.Parallel()

		// A compile-time assertion as well as a runtime one, so drift in the shared contract
		// breaks the build rather than surfacing inside a batch router worker.
		var manager common.AsyncDestinationManager = &sendgridbulkupload.SendGridBulkUploader{}
		require.Implements(t, (*common.AsyncDestinationManager)(nil), manager)
	})
}

// TestNewManager covers construction, which is where a misconfigured destination has to be
// rejected.
//
// Failing here rather than on the first upload means an operator is told once, clearly, instead
// of being handed an opaque 401 per batch for as long as the destination stays broken.
func TestNewManager(t *testing.T) {
	t.Parallel()

	t.Run("a valid destination yields a fully configured manager", func(t *testing.T) {
		t.Parallel()

		uploader, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, newDestination(newDestinationConfig()))
		require.NoError(t, err)
		require.NotNil(t, uploader)

		require.Equal(t, testDestinationID, uploader.DestinationID)
		require.Equal(t, destinationDefinitionName, uploader.DestinationName)
		require.Equal(t, testAPIKey, uploader.DestinationConfig.APIKey)
		require.NotNil(t, uploader.SendGridAPIService, "the manager must build its own API service")

		// The two caps default to the endpoint's documented limits: 30,000 contacts, and the
		// 6MB request ceiling less the reserve held back for the request envelope and its
		// list_ids array, which count against the same limit.
		require.Equal(t, 30000, uploader.MaxContactsPerRequest)
		require.Positive(t, uploader.MaxRequestBytes)
		require.Less(t, uploader.MaxRequestBytes, 6*1000*1000,
			"the byte budget must sit below the documented ceiling so the request envelope fits inside it")
	})

	t.Run("the list IDs and the custom field mapping are parsed", func(t *testing.T) {
		t.Parallel()

		destinationConfig := newDestinationConfig()
		destinationConfig["listIds"] = []any{testEventListID, testConfigListID}

		uploader, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, newDestination(destinationConfig))
		require.NoError(t, err)

		require.Equal(t, []string{testEventListID, testConfigListID}, uploader.DestinationConfig.ListIDs)
		// The mapping is trait name to PRE-CREATED SendGrid custom field ID. SendGrid requires
		// the field to exist and addresses it by that opaque ID, so the mapping has to be
		// supplied rather than inferred from the trait names.
		require.Equal(t,
			map[string]string{"plan": "w1", "signedUpAt": "w2"},
			uploader.DestinationConfig.CustomFieldsMapping,
		)
	})

	t.Run("a destination with no list IDs and no mapping is still valid", func(t *testing.T) {
		t.Parallel()

		uploader, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, newDestination(map[string]any{
			"apiKey": testAPIKey,
		}))
		require.NoError(t, err)
		require.Empty(t, uploader.DestinationConfig.ListIDs)
		require.Empty(t, uploader.DestinationConfig.CustomFieldsMapping)
	})

	t.Run("observability is defaulted rather than demanded", func(t *testing.T) {
		t.Parallel()

		// A nil logger or stats factory must not become a nil-pointer dereference on the first
		// upload, so both fall back to the no-op implementations.
		uploader, err := sendgridbulkupload.NewManager(nil, nil, newDestination(newDestinationConfig()))
		require.NoError(t, err)
		require.NotNil(t, uploader.Logger)
		require.NotNil(t, uploader.StatsFactory)
	})

	for _, testCase := range []struct {
		name              string
		destinationConfig map[string]any
	}{
		{
			name:              "the API key is absent",
			destinationConfig: map[string]any{"listIds": []any{testEventListID}},
		},
		{
			name:              "the API key is not a string",
			destinationConfig: map[string]any{"apiKey": 12345},
		},
		{
			name:              "the API key is empty",
			destinationConfig: map[string]any{"apiKey": ""},
		},
		{
			name:              "the API key is only whitespace",
			destinationConfig: map[string]any{"apiKey": "   \t "},
		},
		{
			name:              "the configuration is nil",
			destinationConfig: nil,
		},
	} {
		t.Run("construction fails when "+testCase.name, func(t *testing.T) {
			t.Parallel()

			uploader, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, newDestination(testCase.destinationConfig))
			require.Error(t, err)
			require.ErrorContains(t, err, "apiKey")
			require.Nil(t, uploader, "a half-configured manager must never be returned")
		})
	}

	t.Run("construction fails when the destination itself is nil", func(t *testing.T) {
		t.Parallel()

		uploader, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, nil)
		require.Error(t, err)
		require.Nil(t, uploader)
	})

	t.Run("the API service constructor enforces the same guard", func(t *testing.T) {
		t.Parallel()

		apiService, err := sendgridbulkupload.NewSendGridAPIService(
			newDestination(map[string]any{"apiKey": " "}), logger.NOP, stats.NOP,
		)
		require.Error(t, err)
		require.Nil(t, apiService)

		apiService, err = sendgridbulkupload.NewSendGridAPIService(
			newDestination(newDestinationConfig()), logger.NOP, stats.NOP,
		)
		require.NoError(t, err)
		require.NotNil(t, apiService)
	})
}

// TestTransform covers the reduction of one event to one staging-file line.
//
// The line's shape is the contract between Transform and Upload: the message is what the contact
// is built from and the metadata is what tags it with the job that produced it. The metadata key
// is job_id - the key the shared marshalling helper writes and the key Upload reads back - and it
// is asserted to be the only key present, so no alternative spelling can slip in.
func TestTransform(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		eventType    string
		payload      string
		expectEmail  string
		expectUserID string
	}{
		{
			name:      "an identify event becomes one contact",
			eventType: "identify",
			payload: `{"body":{"JSON":{"type":"identify","userId":"user_123","anonymousId":"anon_456",` +
				`"traits":{"email":"Alex@Example.com","firstName":"Alex"}}}}`,
			expectEmail:  "Alex@Example.com",
			expectUserID: "user_123",
		},
		{
			// Event-type agnostic on purpose: the contact is derived from the event's traits and
			// identifiers, not from its type, so a track event reduces to a contact just as an
			// identify does.
			name:      "a track event becomes one contact too",
			eventType: "track",
			payload: `{"body":{"JSON":{"type":"track","event":"Signed Up","userId":"user_223",` +
				`"traits":{"email":"blake@example.com"}}}}`,
			expectEmail:  "blake@example.com",
			expectUserID: "user_223",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			uploader := newUploader(t, newMockAPIService(t), newDestinationConfig())

			line, err := uploader.Transform(&jobsdb.JobT{
				JobID:        7,
				EventPayload: []byte(testCase.payload),
			})
			require.NoError(t, err)

			require.Equal(t, int64(7), gjson.Get(line, "metadata.job_id").Int())
			// Stronger than asserting that one known-stale spelling is absent: the metadata object
			// must carry exactly the single key the batch router reads back, so no alternative
			// spelling of it can ever slip in unnoticed.
			metadataKeys := make([]string, 0, 1)
			gjson.Get(line, "metadata").ForEach(func(key, _ gjson.Result) bool {
				metadataKeys = append(metadataKeys, key.String())
				return true
			})
			require.Equal(t, []string{"job_id"}, metadataKeys,
				"metadata must carry exactly the job_id key that Upload reads back")
			require.Equal(t, testCase.eventType, gjson.Get(line, "message.type").String())
			require.Equal(t, testCase.expectEmail, gjson.Get(line, "message.traits.email").String())
			require.Equal(t, testCase.expectUserID, gjson.Get(line, "message.userId").String())

			// The line has to survive the round trip Upload performs on it.
			require.True(t, gjson.Get(line, "message").IsObject())
			require.True(t, gjson.Valid(line))
		})
	}

	t.Run("an event carrying no transformed payload is rejected", func(t *testing.T) {
		t.Parallel()

		uploader := newUploader(t, newMockAPIService(t), newDestinationConfig())

		line, err := uploader.Transform(&jobsdb.JobT{JobID: 9, EventPayload: []byte(`{"body":{}}`)})
		require.Error(t, err, "an event with no body.JSON cannot become a contact")
		require.Empty(t, line)
	})

	t.Run("the transformed line is what Upload turns into a contact", func(t *testing.T) {
		t.Parallel()

		// The whole point of this case: Transform's own output is fed straight into Upload, so
		// the two halves of the contract are pinned together rather than separately. It is also
		// where the documented userId to external_id mapping is proven.
		recorder := &requestRecorder{}
		apiService := newMockAPIService(t)
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
			}).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())

		line, err := uploader.Transform(&jobsdb.JobT{
			JobID: 11,
			EventPayload: []byte(`{"body":{"JSON":{"type":"identify","userId":"user_123",` +
				`"anonymousId":"anon_456","traits":{"email":"Casey@Example.com","firstName":"Casey",` +
				`"phone":"+14155551234","plan":"enterprise"}}}}`),
		})
		require.NoError(t, err)

		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination:     newDestination(newDestinationConfig()),
			FileName:        writeStagingFile(t, line),
			ImportingJobIDs: []int64{11},
		})
		require.Equal(t, []int64{11}, output.ImportingJobIDs)

		requests := recorder.all()
		require.Len(t, requests, 1)
		require.Len(t, requests[0].Contacts, 1)

		contact := requests[0].Contacts[0]
		// Lower-cased locally even though SendGrid lower-cases it on ingestion, because the
		// address is also the reconciliation key and both sides must be comparable without
		// depending on SendGrid's normalization having happened first.
		require.Equal(t, "casey@example.com", contact.Email)
		// userId maps onto external_id: this repository's documented SendGrid convention.
		require.Equal(t, "user_123", contact.ExternalID)
		require.Equal(t, "anon_456", contact.AnonymousID)
		require.Equal(t, "+14155551234", contact.PhoneNumberID)
		require.Equal(t, "Casey", contact.FirstName)
		// A mapped trait travels as a custom field keyed by its PRE-CREATED SendGrid field ID.
		require.Equal(t, map[string]any{"w1": "enterprise"}, contact.CustomFields)
	})
}

// TestUploadHappyPath is mandated scenario S1: N track and identify events become ONE upsert
// carrying the correct contact fields and the configured list IDs, SendGrid accepts it with a
// job_id, and the subsequent poll reports completion once nothing errored.
//
// The destination's own list IDs are set to the list the fixture's events target, because Upload
// groups contacts by their RESOLVED list IDs before chunking: one upsert carries a single
// list_ids array that applies to every contact in it, so contacts destined for different lists
// can never share a request. With both sources agreeing, all five events travel together.
func TestUploadHappyPath(t *testing.T) {
	t.Parallel()

	recorder := &requestRecorder{}
	apiService := newMockAPIService(t)
	apiService.EXPECT().
		UploadContacts(gomock.Any()).
		DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			recorder.record(request)
			return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
		}).
		Times(1)

	uploader := newUploader(t, apiService, newDestinationConfig())
	stagedJobIDs := []int64{1, 2, 3, 4, 5}

	output := uploader.Upload(&common.AsyncDestinationStruct{
		Destination:     newDestination(newDestinationConfig()),
		FileName:        fixtureStagingFile(),
		ImportingJobIDs: stagedJobIDs,
	})

	t.Run("one request carries every contact and the configured list IDs", func(t *testing.T) {
		requests := recorder.all()
		require.Len(t, requests, 1, "five contacts sharing one target list must travel in a single upsert")
		require.Equal(t, []string{testEventListID}, requests[0].ListIDs)
		require.Len(t, requests[0].Contacts, 5)
		require.Equal(t,
			[]string{"alex@example.com", "blake@example.com", "casey@example.com", "devon@example.com", "erin@example.com"},
			[]string{
				requests[0].Contacts[0].Email, requests[0].Contacts[1].Email, requests[0].Contacts[2].Email,
				requests[0].Contacts[3].Email, requests[0].Contacts[4].Email,
			},
			"contacts keep the staging file's order and their addresses are lower-cased",
		)
	})

	t.Run("the contact fields are mapped as the destination documents them", func(t *testing.T) {
		requests := recorder.all()
		require.Len(t, requests, 1)

		// The whole contact is asserted rather than a field at a time, so that a mapping which
		// silently starts sending an EMPTY value is caught too: SendGrid leaves an omitted field
		// untouched but OVERWRITES one sent empty, which is why every field carries omitempty.
		require.Equal(t, sendgridbulkupload.Contact{
			Email:               "alex@example.com",
			PhoneNumberID:       "+14155551234",
			ExternalID:          "user_123",
			AnonymousID:         "anon_456",
			FirstName:           "Alex",
			LastName:            "Keener",
			AddressLine1:        "123 Main St",
			City:                "San Francisco",
			StateProvinceRegion: "CA",
			PostalCode:          "94105",
			Country:             "US",
			CustomFields:        map[string]any{"w1": "enterprise", "w2": "2026-01-15"},
		}, requests[0].Contacts[0])

		// A track event reduces to a contact exactly as an identify does.
		require.Equal(t, "track", gjson.Get(stagingFixtureLines(t)[1], "message.type").String())
		require.Equal(t, "user_223", requests[0].Contacts[1].ExternalID)

		// Alternate addresses travel as a list, and an event with no userId simply carries no
		// external ID rather than an empty one.
		require.Equal(t,
			[]string{"casey.alt@example.com", "casey.work@example.com"},
			requests[0].Contacts[2].AlternateEmails,
		)
		require.Empty(t, requests[0].Contacts[3].ExternalID)
		require.Equal(t, "anon_889", requests[0].Contacts[3].AnonymousID)

		// A trait with no entry in the mapping is not invented as a custom field, because
		// SendGrid requires the field to exist before a value can be written to it.
		require.NotContains(t, requests[0].Contacts[0].CustomFields, "firstName")
	})

	t.Run("every staged job is reported importing and nothing failed or aborted", func(t *testing.T) {
		require.Equal(t, testDestinationID, output.DestinationID)
		require.Equal(t, stagedJobIDs, output.ImportingJobIDs)
		require.Equal(t, len(stagedJobIDs), output.ImportingCount)

		require.Empty(t, output.FailedJobIDs)
		require.Zero(t, output.FailedCount)
		require.Empty(t, output.FailedReason)
		require.Empty(t, output.AbortJobIDs)
		require.Zero(t, output.AbortCount)
		require.Empty(t, output.AbortReason)
	})

	t.Run("the import parameters round trip exactly as the batch router reads them", func(t *testing.T) {
		require.NotEmpty(t, output.ImportingParameters)

		// Read back with gjson from the marshalled bytes, which is literally what the batch
		// router does when it rebuilds the poll input from the persisted job parameters.
		require.Equal(t, testImportID, gjson.GetBytes(output.ImportingParameters, "importId").String())
		require.Equal(t, int64(len(stagedJobIDs)), gjson.GetBytes(output.ImportingParameters, "importCount").Int())
		// Non-zero on purpose: a count marshalled before the accepted jobs are known is always
		// zero, and this assertion is what keeps that mistake out of this connector.
		require.NotZero(t, gjson.GetBytes(output.ImportingParameters, "importCount").Int())
	})

	t.Run("polling the accepted import reports completion once nothing errored", func(t *testing.T) {
		pollAPIService := newMockAPIService(t)
		pollAPIService.EXPECT().
			GetImportStatus(testImportID).
			Return(statusFromWireBody(t, importStatusBody(testImportID, "completed", 0, "")), nil).
			Times(1)

		pollUploader := newUploader(t, pollAPIService, newDestinationConfig())
		response := pollUploader.Poll(common.AsyncPoll{
			ImportId:    gjson.GetBytes(output.ImportingParameters, "importId").String(),
			ImportCount: int(gjson.GetBytes(output.ImportingParameters, "importCount").Int()),
		})

		require.Equal(t, 200, response.StatusCode)
		require.True(t, response.Complete)
		require.False(t, response.InProgress)
		require.False(t, response.HasFailed)
		require.False(t, response.HasWarning)
		require.Empty(t, response.FailedJobParameters)
		require.Empty(t, response.WarningJobParameters)
	})
}

// TestUploadListIDResolution pins the documented precedence: a per-event context.externalId entry
// of type listIds wins over the destination configuration, so one destination can target
// different lists per event.
//
// Because a request's list_ids array applies to every contact in it, that precedence also decides
// how the batch is split: two contacts resolving to different lists cannot share a request no
// matter how small they are.
func TestUploadListIDResolution(t *testing.T) {
	t.Parallel()

	recorder := &requestRecorder{}
	apiService := newMockAPIService(t)
	apiService.EXPECT().
		UploadContacts(gomock.Any()).
		DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			recorder.record(request)
			return &sendgridbulkupload.UpsertResponse{JobID: fmt.Sprintf("%s-%d", testImportID, len(recorder.all()))}, nil
		}).
		Times(2)

	// The destination targets a DIFFERENT list from the one the fixture's first two events name.
	destinationConfig := newDestinationConfig()
	destinationConfig["listIds"] = []any{testConfigListID}

	uploader := newUploader(t, apiService, destinationConfig)
	output := uploader.Upload(&common.AsyncDestinationStruct{
		Destination:     newDestination(destinationConfig),
		FileName:        fixtureStagingFile(),
		ImportingJobIDs: []int64{1, 2, 3, 4, 5},
	})

	requests := recorder.all()
	require.Len(t, requests, 2, "two target lists cannot share one upsert")

	// Jobs 1 and 2 carry the per-event list, jobs 3, 4 and 5 fall back to the destination's.
	require.Equal(t, []string{testEventListID}, requests[0].ListIDs)
	require.Equal(t, []string{"alex@example.com", "blake@example.com"},
		[]string{requests[0].Contacts[0].Email, requests[0].Contacts[1].Email})

	require.Equal(t, []string{testConfigListID}, requests[1].ListIDs)
	require.Len(t, requests[1].Contacts, 3)

	// Both imports are accepted, so both job_ids are persisted and every job is importing.
	require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, output.ImportingJobIDs)
	require.Equal(t, 5, output.ImportingCount)
	require.Empty(t, output.FailedJobIDs)
	require.Empty(t, output.AbortJobIDs)

	persistedImportID := gjson.GetBytes(output.ImportingParameters, "importId").String()
	require.Contains(t, persistedImportID, testImportID+"-1")
	require.Contains(t, persistedImportID, testImportID+"-2")
	require.Equal(t, int64(5), gjson.GetBytes(output.ImportingParameters, "importCount").Int())
}

// TestUploadRateLimited is mandated scenario S3: a 429 from the upsert must NOT abort.
//
// The affected jobs come back as retryable failures with the advertised reset window in the
// reason, and never as aborts. The batch router owns retry, backoff and the decision to give up -
// it records these jobs as failed and escalates to an abort itself once its retry budget is
// exhausted - so a connector that aborted here would discard deliverable contacts on the first
// transient throttle.
//
// The empty importing fields matter just as much: the router keeps an upload in the importing
// state only when BOTH an import identifier and importing job IDs come back, and returning
// neither is exactly what releases these jobs to be re-queued.
func TestUploadRateLimited(t *testing.T) {
	t.Parallel()

	const resetAt int64 = 1772020800 // an absolute instant, not a delay

	apiService := newMockAPIService(t)
	apiService.EXPECT().
		UploadContacts(gomock.Any()).
		Return(nil, newRateLimitError(resetAt)).
		Times(1)

	uploader := newUploader(t, apiService, newDestinationConfig())
	output := uploader.Upload(&common.AsyncDestinationStruct{
		Destination:     newDestination(newDestinationConfig()),
		FileName:        fixtureStagingFile(),
		ImportingJobIDs: []int64{1, 2, 3, 4, 5},
	})

	t.Run("the affected jobs are retryable failures", func(t *testing.T) {
		require.Equal(t, []int64{1, 2, 3, 4, 5}, output.FailedJobIDs)
		require.Equal(t, len(output.FailedJobIDs), output.FailedCount)
		require.Equal(t, testDestinationID, output.DestinationID)
	})

	t.Run("the advertised reset window reaches the failure reason", func(t *testing.T) {
		require.Contains(t, output.FailedReason, "rate limited")
		require.Contains(t, output.FailedReason, "429")
		// Retry-After is read first when present, even though SendGrid does not document it,
		// because an edge or proxy may inject it and it is then the most direct statement of
		// how long to wait.
		require.Contains(t, output.FailedReason, "Retry-After: 30")
		// X-RateLimit-Reset is epoch SECONDS, so it is rendered as an absolute instant rather
		// than as a remaining duration - reading it as a delay would produce a wait of decades.
		require.Contains(t, output.FailedReason, resetWindow(resetAt))
		// The quota values go into the reason too, so an operator can tell a throttle apart
		// from an outage without reading logs.
		require.Contains(t, output.FailedReason, "X-RateLimit-Limit: 600")
		require.Contains(t, output.FailedReason, "X-RateLimit-Remaining: 0")
	})

	t.Run("no importing state is returned, so the jobs are released", func(t *testing.T) {
		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.ImportingParameters)
		require.Zero(t, output.ImportingCount)
	})

	t.Run("nothing is aborted", func(t *testing.T) {
		// The single most important assertion of this scenario: a rate limit is never terminal.
		require.Empty(t, output.AbortJobIDs)
		require.Zero(t, output.AbortCount)
		require.Empty(t, output.AbortReason)
		requireDisjoint(t, output.FailedJobIDs, output.AbortJobIDs)
	})

	t.Run("the documented 429 body decodes with its null field intact", func(t *testing.T) {
		t.Parallel()

		// SendGrid's documented rate-limit body is {"errors":[{"field":null,"message":"..."}]}.
		// That null is the whole reason the entry's field is a POINTER: a plain string would
		// decode an explicit null and an empty field name to the same value, and the entry would
		// then be rendered as though SendGrid had blamed a field it never named. The distinction
		// is asserted here rather than assumed because it is invisible at compile time - the
		// wrong type decodes this body without error and only reads back wrong.
		var envelope sendgridbulkupload.APIErrorResponse
		require.NoError(t, jsonrs.Unmarshal(
			[]byte(`{"errors":[{"field":null,"message":"too many requests"}]}`), &envelope))
		require.Len(t, envelope.Errors, 1)
		require.Nil(t, envelope.Errors[0].Field)
		require.Equal(t, "too many requests", envelope.Errors[0].String())

		// A named field is rendered with its name, which is what makes the null case worth
		// telling apart in the first place.
		fieldName := "contacts"
		named := sendgridbulkupload.APIErrorItem{Field: &fieldName, Message: "is required"}
		require.Equal(t, "field=contacts: is required", named.String())
	})
}

// TestUploadPartiallyRateLimited covers a multi-chunk upload in which SendGrid accepts one chunk
// and throttles the next.
//
// The accepted chunk must keep its importing state while only the throttled chunk's jobs are
// reported retryable, and the two sets must stay disjoint. This is also the sharpest available
// proof that the chunker's contact and job-ID chunks are INDEX-ALIGNED: the job IDs recovered
// from each captured request's contacts are exactly the job IDs the uploader reported for that
// same chunk, so a misalignment would report the wrong jobs as failed.
func TestUploadPartiallyRateLimited(t *testing.T) {
	t.Parallel()

	const resetAt int64 = 1772024400

	recorder := &requestRecorder{}
	apiService := newMockAPIService(t)
	gomock.InOrder(
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
			}),
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return nil, newRateLimitError(resetAt)
			}),
	)

	uploader := newUploader(t, apiService, newDestinationConfig())
	uploader.MaxContactsPerRequest = 3

	output := uploader.Upload(&common.AsyncDestinationStruct{
		Destination:     newDestination(newDestinationConfig()),
		FileName:        writeStagingFile(t, uniformStagingLines(t, 6)...),
		ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
	})

	requests := recorder.all()
	require.Len(t, requests, 2)
	require.Equal(t, []int{3, 3}, recorder.contactsIn())
	require.Equal(t, []int64{1, 2, 3}, jobIDsOf(t, requests[0]))
	require.Equal(t, []int64{4, 5, 6}, jobIDsOf(t, requests[1]))

	require.Equal(t, []int64{1, 2, 3}, output.ImportingJobIDs, "the accepted chunk keeps its importing state")
	require.Equal(t, 3, output.ImportingCount)
	require.Equal(t, testImportID, gjson.GetBytes(output.ImportingParameters, "importId").String())
	require.Equal(t, int64(3), gjson.GetBytes(output.ImportingParameters, "importCount").Int())

	require.Equal(t, []int64{4, 5, 6}, output.FailedJobIDs, "only the throttled chunk's jobs are retried")
	require.Equal(t, 3, output.FailedCount)
	require.Contains(t, output.FailedReason, resetWindow(resetAt))

	require.Empty(t, output.AbortJobIDs)
	requireDisjoint(t, output.ImportingJobIDs, output.FailedJobIDs)
}

// TestUploadProviderRejections covers every other way SendGrid can refuse a chunk.
//
// All of them are RETRYABLE, whatever the status code: the batch router owns the retry budget and
// escalates to an abort itself, so no response from the provider is ever terminal here.
func TestUploadProviderRejections(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name           string
		uploadResponse *sendgridbulkupload.UpsertResponse
		uploadError    error
		expectReason   string
	}{
		{
			name: "an authorization failure is retried, not aborted",
			uploadError: &sendgridbulkupload.APIError{
				StatusCode: 401,
				Operation:  "upload contacts",
				Message:    "unauthorized",
			},
			expectReason: "Error in Uploading contacts",
		},
		{
			name: "a provider outage is retried",
			uploadError: &sendgridbulkupload.APIError{
				StatusCode: 503,
				Operation:  "upload contacts",
				Message:    "service unavailable",
			},
			expectReason: "Error in Uploading contacts",
		},
		{
			name:         "a transport failure is retried",
			uploadError:  fmt.Errorf("sendgrid upload contacts: dial tcp: connection refused"),
			expectReason: "Error in Uploading contacts",
		},
		{
			name: "an acceptance carrying no import job id is retried",
			// Recording this as importing would strand the jobs forever, because there is
			// nothing to poll. The endpoint upserts, so repeating the request is harmless.
			uploadResponse: &sendgridbulkupload.UpsertResponse{JobID: "   "},
			expectReason:   "without returning an import job id",
		},
		{
			name:           "a nil response with no error is retried",
			uploadResponse: nil,
			expectReason:   "without returning an import job id",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apiService := newMockAPIService(t)
			apiService.EXPECT().
				UploadContacts(gomock.Any()).
				Return(testCase.uploadResponse, testCase.uploadError).
				Times(1)

			uploader := newUploader(t, apiService, newDestinationConfig())
			output := uploader.Upload(&common.AsyncDestinationStruct{
				Destination:     newDestination(newDestinationConfig()),
				FileName:        fixtureStagingFile(),
				ImportingJobIDs: []int64{1, 2, 3, 4, 5},
			})

			require.Equal(t, []int64{1, 2, 3, 4, 5}, output.FailedJobIDs)
			require.Equal(t, 5, output.FailedCount)
			require.Contains(t, output.FailedReason, testCase.expectReason)
			require.Empty(t, output.ImportingJobIDs)
			require.Empty(t, output.ImportingParameters)
			require.Empty(t, output.AbortJobIDs, "no provider response is terminal")
			require.Zero(t, output.AbortCount)
		})
	}
}

// TestUploadLocalRejections covers the only three outcomes this connector treats as TERMINAL,
// and they are all local: a contact carrying none of SendGrid's identifiers, a staging record
// that is malformed yet still names its job, and a contact too large for any request.
//
// All three are permanent - the batch router would rebuild an identical payload on a retry - so
// retrying could only fail again and burn the retry budget. Each abandons exactly ONE job and
// must never poison the rest of the batch.
func TestUploadLocalRejections(t *testing.T) {
	t.Parallel()

	t.Run("a contact with none of the four identifiers is aborted on its own", func(t *testing.T) {
		t.Parallel()

		recorder := &requestRecorder{}
		apiService := newMockAPIService(t)
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
			}).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: newDestination(newDestinationConfig()),
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_01","traits":{"email":"u01@example.com"}}`),
				// No email, no userId, no anonymousId and no phone: SendGrid rejects a contact
				// carrying none of the four identifiers it accepts.
				stagingLine(t, 2, `{"type":"track","event":"Page Viewed","properties":{"plan":"growth"}}`),
			),
			ImportingJobIDs: []int64{1, 2},
		})

		requests := recorder.all()
		require.Len(t, requests, 1)
		require.Len(t, requests[0].Contacts, 1, "the unusable record must not be sent")
		require.Equal(t, "u01@example.com", requests[0].Contacts[0].Email)

		require.Equal(t, []int64{1}, output.ImportingJobIDs, "the rest of the batch is delivered")
		require.Equal(t, []int64{2}, output.AbortJobIDs, "the rejected job's ID is retained, not lost")
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "at least one of email, phone_number_id, external_id or anonymous_id")
		require.Empty(t, output.FailedJobIDs)
		requireDisjoint(t, output.ImportingJobIDs, output.AbortJobIDs)
	})

	t.Run("a contact too large for any request is aborted on its own", func(t *testing.T) {
		t.Parallel()

		oversizedContact := sendgridbulkupload.Contact{
			Email:      "u02@example.com",
			ExternalID: "user_02",
			FirstName:  strings.Repeat("x", 400),
		}
		smallContact := uniformContact(1)

		recorder := &requestRecorder{}
		apiService := newMockAPIService(t)
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
			}).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())
		// One byte below the oversized contact's own footprint: it can never fit in any chunk,
		// while the small contact still fits comfortably.
		uploader.MaxRequestBytes = contactSize(t, oversizedContact) - 1
		require.Less(t, contactSize(t, smallContact), uploader.MaxRequestBytes)

		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: newDestination(newDestinationConfig()),
			FileName: writeStagingFile(t,
				stagingLine(t, 1, fmt.Sprintf(
					`{"type":"identify","userId":%q,"traits":{"email":%q}}`, smallContact.ExternalID, smallContact.Email)),
				stagingLine(t, 2, fmt.Sprintf(
					`{"type":"identify","userId":%q,"traits":{"email":%q,"firstName":%q}}`,
					oversizedContact.ExternalID, oversizedContact.Email, oversizedContact.FirstName)),
			),
			ImportingJobIDs: []int64{1, 2},
		})

		requests := recorder.all()
		require.Len(t, requests, 1)
		require.Equal(t, []int64{1}, jobIDsOf(t, requests[0]),
			"an oversized contact must not wedge the chunker into emitting an over-cap request")

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "larger than the maximum sendgrid request size")
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("a malformed but attributable record is aborted on its own", func(t *testing.T) {
		t.Parallel()

		recorder := &requestRecorder{}
		apiService := newMockAPIService(t)
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				recorder.record(request)
				return &sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil
			}).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: newDestination(newDestinationConfig()),
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_01","traits":{"email":"u01@example.com"}}`),
				// Attributable - the job ID is right there - but carrying no message object, so
				// the same bytes would fail identically on every retry.
				`{"metadata":{"job_id":2}}`,
				// A blank line carries no record at all and is charged against nobody.
				"",
			),
			ImportingJobIDs: []int64{1, 2},
		})

		require.Len(t, recorder.all(), 1)
		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "malformed")
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("an unattributable record fails the whole batch instead of naming a job that does not exist", func(t *testing.T) {
		t.Parallel()

		apiService := newMockAPIService(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: newDestination(newDestinationConfig()),
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_01","traits":{"email":"u01@example.com"}}`),
				// No metadata at all. Reporting this against job ID 0 would name a job that
				// cannot exist while leaving the real one unaccounted for, so the read fails and
				// the batch is retried in full.
				`{"message":{"type":"identify","traits":{"email":"u02@example.com"}}}`,
			),
			ImportingJobIDs: []int64{1, 2},
		})

		require.ElementsMatch(t, []int64{1, 2}, output.FailedJobIDs)
		require.Equal(t, 2, output.FailedCount)
		require.Contains(t, output.FailedReason, "Error in reading staging file")
		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("an unreadable staging file fails the whole batch", func(t *testing.T) {
		t.Parallel()

		apiService := newMockAPIService(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination:     newDestination(newDestinationConfig()),
			FileName:        filepath.Join(t.TempDir(), "no-such-staging-file.jsonl"),
			FailedJobIDs:    []int64{9},
			ImportingJobIDs: []int64{1, 2},
		})

		// Jobs that had already failed before this upload are reported again, so none of them is
		// left without a status.
		require.ElementsMatch(t, []int64{1, 2, 9}, output.FailedJobIDs)
		require.Equal(t, 3, output.FailedCount)
		require.Contains(t, output.FailedReason, "Error in reading staging file")
		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("a job missing from the staging file is swept into the retryable set", func(t *testing.T) {
		t.Parallel()

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			UploadContacts(gomock.Any()).
			Return(&sendgridbulkupload.UpsertResponse{JobID: testImportID}, nil).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: newDestination(newDestinationConfig()),
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_01","traits":{"email":"u01@example.com"}}`),
			),
			// Job 42 is in the batch but not in the file. The batch router writes a status only
			// for the jobs an upload names, so leaving it out would strand it silently.
			ImportingJobIDs: []int64{1, 42},
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{42}, output.FailedJobIDs)
		require.Contains(t, output.FailedReason, "not present in the staging file")
		requireDisjoint(t, output.ImportingJobIDs, output.FailedJobIDs)
	})

	t.Run("an upload with no batch at all reports nothing rather than panicking", func(t *testing.T) {
		t.Parallel()

		apiService := newMockAPIService(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, newDestinationConfig())
		output := uploader.Upload(nil)

		require.Equal(t, common.AsyncUploadOutput{DestinationID: testDestinationID}, output)
	})
}

// TestUploadChunking pins the DUAL-cap chunker at its boundaries.
//
// SendGrid caps one upsert at 30,000 contacts OR 6MB of data, whichever is lower, so both caps
// have to be honoured at once. Every case here drives the real chunker through Upload and asserts
// the resulting chunk-size sequence, that no empty chunk is ever emitted, and that the contact
// and job-ID chunks stayed index-aligned.
func TestUploadChunking(t *testing.T) {
	t.Parallel()

	// Every uniform contact serializes to the same footprint, which is what makes the byte-cap
	// boundaries below exact rather than approximate.
	size := contactSize(t, uniformContact(1))
	require.Positive(t, size)

	for _, testCase := range []struct {
		name         string
		lineCount    int
		maxContacts  int
		maxBytes     int
		expectChunks []int
	}{
		{
			name:         "just below the contact cap: one request",
			lineCount:    2,
			maxContacts:  3,
			expectChunks: []int{2},
		},
		{
			name:         "exactly at the contact cap: still one request",
			lineCount:    3,
			maxContacts:  3,
			expectChunks: []int{3},
		},
		{
			name:         "one above the contact cap: the chunk flushes",
			lineCount:    4,
			maxContacts:  3,
			expectChunks: []int{3, 1},
		},
		{
			name:         "just below the byte cap: one request",
			lineCount:    3,
			maxBytes:     3*size + 1,
			expectChunks: []int{3},
		},
		{
			name:         "exactly at the byte cap: the chunk flushes before it is reached",
			lineCount:    3,
			maxBytes:     3 * size,
			expectChunks: []int{2, 1},
		},
		{
			name:         "one byte below the byte cap: the chunk flushes too",
			lineCount:    3,
			maxBytes:     3*size - 1,
			expectChunks: []int{2, 1},
		},
		{
			name:         "a byte cap admitting one contact at a time: a request each",
			lineCount:    3,
			maxBytes:     2 * size,
			expectChunks: []int{1, 1, 1},
		},
		{
			name:         "both caps together: the lower one decides",
			lineCount:    6,
			maxContacts:  4,
			maxBytes:     3*size + 1,
			expectChunks: []int{3, 3},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			recorder := &requestRecorder{}
			apiService := newMockAPIService(t)
			apiService.EXPECT().
				UploadContacts(gomock.Any()).
				DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
					recorder.record(request)
					return &sendgridbulkupload.UpsertResponse{
						JobID: fmt.Sprintf("%s-%d", testImportID, len(recorder.all())),
					}, nil
				}).
				Times(len(testCase.expectChunks))

			uploader := newUploader(t, apiService, newDestinationConfig())
			// A zero override means "use the endpoint's documented default", which is what lets
			// each case constrain exactly one cap.
			uploader.MaxContactsPerRequest = testCase.maxContacts
			uploader.MaxRequestBytes = testCase.maxBytes

			stagedJobIDs := make([]int64, 0, testCase.lineCount)
			for index := 1; index <= testCase.lineCount; index++ {
				stagedJobIDs = append(stagedJobIDs, int64(index))
			}

			output := uploader.Upload(&common.AsyncDestinationStruct{
				Destination:     newDestination(newDestinationConfig()),
				FileName:        writeStagingFile(t, uniformStagingLines(t, testCase.lineCount)...),
				ImportingJobIDs: stagedJobIDs,
			})

			require.Equal(t, testCase.expectChunks, recorder.contactsIn())
			require.NotContains(t, recorder.contactsIn(), 0, "an empty chunk must never be emitted")

			// Index alignment: the job IDs recovered from each chunk's contacts, concatenated in
			// call order, are exactly the job IDs reported as importing.
			alignedJobIDs := make([]int64, 0, testCase.lineCount)
			for _, request := range recorder.all() {
				alignedJobIDs = append(alignedJobIDs, jobIDsOf(t, request)...)
			}
			require.Equal(t, stagedJobIDs, alignedJobIDs)
			require.Equal(t, stagedJobIDs, output.ImportingJobIDs)
			require.Equal(t, testCase.lineCount, output.ImportingCount)
			require.Equal(t, int64(testCase.lineCount), gjson.GetBytes(output.ImportingParameters, "importCount").Int())

			// One import identifier per accepted chunk, joined into the single value the batch
			// router persists.
			persistedImportID := gjson.GetBytes(output.ImportingParameters, "importId").String()
			for chunk := range testCase.expectChunks {
				require.Contains(t, persistedImportID, fmt.Sprintf("%s-%d", testImportID, chunk+1))
			}

			require.Empty(t, output.FailedJobIDs)
			require.Empty(t, output.AbortJobIDs)
		})
	}

	t.Run("a byte cap no contact can fit aborts each contact instead of chunking", func(t *testing.T) {
		t.Parallel()

		apiService := newMockAPIService(t)
		// Not a single request is issued, which is the strongest statement available that an
		// empty chunk is never manufactured to carry contacts that cannot fit.
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, newDestinationConfig())
		uploader.MaxRequestBytes = size

		output := uploader.Upload(&common.AsyncDestinationStruct{
			Destination:     newDestination(newDestinationConfig()),
			FileName:        writeStagingFile(t, uniformStagingLines(t, 3)...),
			ImportingJobIDs: []int64{1, 2, 3},
		})

		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.ImportingParameters)
		require.Equal(t, []int64{1, 2, 3}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "larger than the maximum sendgrid request size")
		require.Empty(t, output.FailedJobIDs)
	})
}

// TestPoll walks the whole import-state mapping, one subtest per row.
//
// Every case feeds the connector a VERBATIM SendGrid status body, so each row also re-proves that
// the counters and the errors document URL are read out of the NESTED results object. SendGrid's
// documented enumeration is exactly pending, completed, errored and failed - there is no
// processing or in_progress value, and pending is the only non-terminal state - and it signals a
// PARTIAL failure with errored, not with completed.
//
// HasWarning and WarningJobParameters are asserted zero in every single row: SendGrid has no
// warning tier, so a connector that ever set them would send jobs down a channel this destination
// has no meaning for.
func TestPoll(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                string
		importID            string
		setupMock           func(apiService *mockAPIService.MockSendGridAPIService)
		expect              common.PollStatusResponse
		expectErrorContains []string
	}{
		{
			name:     "pending keeps the batch router polling with no state change",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "pending", 0, "")), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{StatusCode: 200, InProgress: true},
		},
		{
			name:     "completed with nothing errored succeeds every job",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "completed", 0, "")), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{StatusCode: 200, Complete: true},
		},
		{
			// The defensive branch. SendGrid promises that completed carries no errors, but the
			// batch router marks EVERY importing job succeeded wholesale unless HasFailed is set,
			// so a completed import that nonetheless reports errored rows has to take the
			// reconciliation branch too. Getting this wrong reports rejected contacts as
			// delivered, which is the worst outcome this connector can produce.
			name:     "completed with errored rows still routes to reconciliation",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "completed", 2, testErrorsURL)), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{
				StatusCode:          200,
				Complete:            true,
				HasFailed:           true,
				FailedJobParameters: testErrorsURL,
			},
			expectErrorContains: []string{"partially failed", "errored 2", testErrorsURL},
		},
		{
			name:     "errored routes to reconciliation",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "errored", 3, testErrorsURL)), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{
				StatusCode:          200,
				Complete:            true,
				HasFailed:           true,
				FailedJobParameters: testErrorsURL,
			},
			expectErrorContains: []string{"partially failed", "errored 3"},
		},
		{
			// failed means finished with ALL errors, or entirely unprocessable: a permanent
			// condition for which 400 is the framework's terminal path. Reconciling it instead
			// would classify every row as retryable and burn the retry budget for nothing.
			name:     "failed aborts the batch terminally",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "failed", 5, testErrorsURL)), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{StatusCode: 400, Complete: true, HasFailed: true},
			// The errors document is surfaced for diagnosis only, never routed through
			// reconciliation, which is why it appears in the reason and not in the parameters.
			expectErrorContains: []string{"SendGrid Bulk Upload Failed", "status failed", testErrorsURL},
		},
		{
			name:     "an unrecognized state is retried rather than guessed at",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "quiesced", 0, "")), nil).
					Times(1)
			},
			expect:              common.PollStatusResponse{StatusCode: 500},
			expectErrorContains: []string{"Unknown status", "quiesced"},
		},
		{
			name:     "a provider error is retried",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(nil, &sendgridbulkupload.APIError{
						StatusCode: 500,
						Operation:  "get import status",
						Message:    "internal server error",
					}).
					Times(1)
			},
			expect:              common.PollStatusResponse{StatusCode: 500},
			expectErrorContains: []string{"internal server error"},
		},
		{
			name:     "a status call answering nothing at all is retried",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().GetImportStatus(testImportID).Return(nil, nil).Times(1)
			},
			expect:              common.PollStatusResponse{StatusCode: 500},
			expectErrorContains: []string{"no status for import"},
		},
		{
			name:     "a rate limit while polling is reported with its own status code",
			importID: testImportID,
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(nil, newRateLimitError(1772020800)).
					Times(1)
			},
			expect:              common.PollStatusResponse{StatusCode: 429},
			expectErrorContains: []string{"rate limited", resetWindow(1772020800)},
		},
		{
			name:     "a poll with no persisted import id is retried",
			importID: "   ",
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().GetImportStatus(gomock.Any()).Times(0)
			},
			expect:              common.PollStatusResponse{StatusCode: 500},
			expectErrorContains: []string{"no sendgrid import id was persisted"},
		},
		{
			// One upload can produce several imports, because the batch is chunked and every
			// accepted chunk gets its own job_id. A single pending import keeps the whole batch
			// polling, because nothing may be resolved while part of it is still in flight.
			name:     "several imports with one still pending keep polling",
			importID: testImportID + ":" + testImportID + "-2",
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "completed", 0, "")), nil).
					Times(1)
				apiService.EXPECT().
					GetImportStatus(testImportID+"-2").
					Return(statusFromWireBody(t, importStatusBody(testImportID+"-2", "pending", 0, "")), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{StatusCode: 200, InProgress: true},
		},
		{
			// Only SOME imports failed, so aborting would also abort the jobs of the import that
			// merely errored. The mapping deliberately falls through to reconciliation.
			name:     "several imports with one failed and one errored reconcile rather than abort",
			importID: testImportID + ":" + testImportID + "-2",
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "failed", 5, testErrorsURL)), nil).
					Times(1)
				apiService.EXPECT().
					GetImportStatus(testImportID+"-2").
					Return(statusFromWireBody(t, importStatusBody(testImportID+"-2", "errored", 1, testErrorsURL+"-2")), nil).
					Times(1)
			},
			expect: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  true,
				// Both documents travel, joined by a newline - a character no URL may contain,
				// so the join is always reversible.
				FailedJobParameters: testErrorsURL + "\n" + testErrorsURL + "-2",
			},
			expectErrorContains: []string{"partially failed", "status failed", "status errored"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apiService := newMockAPIService(t)
			testCase.setupMock(apiService)

			uploader := newUploader(t, apiService, newDestinationConfig())
			response := uploader.Poll(common.AsyncPoll{ImportId: testCase.importID, ImportCount: 5})

			require.Equal(t, testCase.expect.StatusCode, response.StatusCode)
			require.Equal(t, testCase.expect.Complete, response.Complete)
			require.Equal(t, testCase.expect.InProgress, response.InProgress)
			require.Equal(t, testCase.expect.HasFailed, response.HasFailed)
			require.Equal(t, testCase.expect.FailedJobParameters, response.FailedJobParameters)

			// SendGrid has no warning tier, in any state.
			require.False(t, response.HasWarning)
			require.Empty(t, response.WarningJobParameters)

			if len(testCase.expectErrorContains) == 0 {
				require.Empty(t, response.Error)
			}
			for _, fragment := range testCase.expectErrorContains {
				require.Contains(t, response.Error, fragment)
			}
		})
	}

	t.Run("the errored counters are read from the nested results object", func(t *testing.T) {
		t.Parallel()

		// This is the single highest-risk detail in the connector, so it is pinned directly as
		// well as through every row above. SendGrid nests the counters and the errors document URL
		// inside a results object; a flattened response type would unmarshal this body without any
		// error and then read errored_count as 0, turning every partial failure into a reported
		// success.
		body := `{"id":"` + testImportID + `","status":"errored","job_type":"upsert",` +
			`"results":{"requested_count":5,"created_count":3,"updated_count":0,"deleted_count":0,` +
			`"errored_count":2,"errors_url":"` + testErrorsURL + `"},` +
			`"started_at":"2026-02-25T12:00:00Z","finished_at":"2026-02-25T12:00:30Z"}`

		status := statusFromWireBody(t, body)
		require.Equal(t, "errored", status.Status)
		require.Equal(t, 2, status.Results.ErroredCount, "errored_count lives inside results, not at the top level")
		require.Equal(t, testErrorsURL, status.Results.ErrorsURL)
		require.Equal(t, 5, status.Results.RequestedCount)
		require.Equal(t, 3, status.Results.CreatedCount)

		apiService := newMockAPIService(t)
		apiService.EXPECT().GetImportStatus(testImportID).Return(status, nil).Times(1)

		response := newUploader(t, apiService, newDestinationConfig()).
			Poll(common.AsyncPoll{ImportId: testImportID, ImportCount: 5})

		require.True(t, response.HasFailed, "a nested errored_count above zero must reach the poll mapping")
		require.Equal(t, testErrorsURL, response.FailedJobParameters)
	})
}

// TestGetUploadStats is mandated scenario S2: one import yields BOTH failed and succeeded jobs.
//
// The errored rows go to the FAILED channel, never the aborted one - a rejected address or a value
// SendGrid would not take is recoverable, and failed is the batch router's retryable state - and
// the remainder is marked delivered by exclusion.
//
// The reconciliation is stateless by design: it re-derives every contact identifier from the
// importing jobs it is handed, because this method may run in a different process invocation, or
// on a different pod, from the upload that produced the import. Anything cached at upload time
// would simply be missing after a restart, and a connector that relied on it would then report
// rejected contacts as delivered.
func TestGetUploadStats(t *testing.T) {
	t.Parallel()

	t.Run("an errored import fails the rejected contacts and succeeds the rest", func(t *testing.T) {
		t.Parallel()

		jobs := importingJobs(t, stagingFixtureLines(t))
		require.Equal(t, []int64{1, 2, 3, 4, 5}, jobIDsIn(jobs))

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(matchedErrorsDocument), nil).
			Times(1)

		response := newUploader(t, apiService, newDestinationConfig()).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		// 200 is mandatory: the batch router discards the entire reconciliation, and reports an
		// error, for any other status.
		require.Equal(t, 200, response.StatusCode)
		require.Empty(t, response.Error)

		// blake@example.com is job 2 and devon@example.com is job 4, the latter reported under the
		// nested contact.email spelling.
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[2], "Invalid email address provided for contact.")
		require.Contains(t, response.Metadata.FailedReasons[4], "custom field value exceeds the maximum allowed length")

		// The exact remainder, succeeded by exclusion, which is sound only because every row in
		// the document was attributed.
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
		requireDisjoint(t, response.Metadata.FailedKeys, response.Metadata.SucceededKeys)
		require.Len(t,
			append(append([]int64{}, response.Metadata.FailedKeys...), response.Metadata.SucceededKeys...),
			len(jobs),
			"every importing job is accounted for exactly once",
		)

		// Errored rows are retryable, so nothing is aborted, and SendGrid has no warning tier.
		require.Empty(t, response.Metadata.AbortedKeys)
		require.Empty(t, response.Metadata.AbortedReasons)
		require.Empty(t, response.Metadata.WarningKeys)
		require.Empty(t, response.Metadata.WarningReasons)
	})

	t.Run("a completed import reporting errored rows reconciles exactly as an errored one does", func(t *testing.T) {
		t.Parallel()

		// The companion of the poll mapping's defensive branch, driven end to end: poll a
		// completed import that nonetheless reports errored rows, then reconcile with the errors
		// document URL the poll forwarded. Without HasFailed the batch router would already have
		// marked all five jobs succeeded and this method would never have been called.
		jobs := importingJobs(t, stagingFixtureLines(t))

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportStatus(testImportID).
			Return(statusFromWireBody(t, importStatusBody(testImportID, "completed", 2, testErrorsURL)), nil).
			Times(1)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(matchedErrorsDocument), nil).
			Times(1)

		uploader := newUploader(t, apiService, newDestinationConfig())

		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: testImportID, ImportCount: len(jobs)})
		require.Equal(t, 200, pollResponse.StatusCode)
		require.True(t, pollResponse.Complete)
		require.True(t, pollResponse.HasFailed)
		require.Equal(t, testErrorsURL, pollResponse.FailedJobParameters)

		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: pollResponse.FailedJobParameters,
			Parameters:          importParameters(t, testImportID, len(jobs)),
			ImportingList:       jobs,
		})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.AbortedKeys)
	})

	t.Run("an errored row that matches no importing job fails the import closed", func(t *testing.T) {
		t.Parallel()

		// The committed fixture carries a deliberately unmatched identifier alongside two matched
		// ones. That row proves a contact was rejected while leaving it unknown WHICH, so "no row
		// named this job" stops being evidence for anybody: every unresolved job is retried and
		// NOTHING is marked delivered. The alternative - succeeding the rest anyway - would lose
		// a contact permanently and silently.
		statsStore, err := memstats.New()
		require.NoError(t, err)

		jobs := importingJobs(t, stagingFixtureLines(t))

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return(errorsFixtureDocument(t), nil).
			Times(1)

		response := newUploaderWithStats(t, apiService, newDestinationConfig(), statsStore).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		// Still 200, and deliberately so: a non-200 makes the batch router write NO job status at
		// all, and its poll route has no retry budget that could escalate, so every job would sit
		// in the importing state forever and the destination would stop accepting work.
		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys)
		require.Empty(t, response.Metadata.SucceededKeys, "nothing may be marked delivered on the fail-closed path")
		require.Empty(t, response.Metadata.AbortedKeys, "failing closed is retryable, never terminal")

		// The FIRST reason recorded for a job wins, so the specific explanation SendGrid gave for
		// an attributed contact is never overwritten by the generic sweep.
		require.Contains(t, response.Metadata.FailedReasons[2], "Invalid email address provided for contact.")
		require.Contains(t, response.Metadata.FailedReasons[4], "custom field value exceeds the maximum allowed length")
		for _, jobID := range []int64{1, 3, 5} {
			require.Contains(t, response.Metadata.FailedReasons[jobID], "could not be attributed to specific jobs")
		}

		// Counted and logged, not silently dropped: a change in the undocumented document shape
		// has to show up in metrics rather than only in one job's reason.
		unmatched := statsStore.Get("unmatched_error_row_count", statLabels())
		require.NotNil(t, unmatched, "an unattributable row must be counted")
		require.EqualValues(t, 1, unmatched.LastValue())

		unattributed := statsStore.Get("unattributed_reconciliation_count", statLabels())
		require.NotNil(t, unattributed)
		require.EqualValues(t, 1, unattributed.LastValue())

		// Nothing in this document was unreadable, so that counter is never emitted at all.
		require.Nil(t, statsStore.Get("unrecognized_error_row_count", statLabels()))
	})

	t.Run("a row the parser cannot read fails the import closed as well", func(t *testing.T) {
		t.Parallel()

		// The other route to the same uncertainty: the entry was present, so a contact was
		// rejected, but nothing about it could be read. Tolerance is not the same as discarding.
		statsStore, err := memstats.New()
		require.NoError(t, err)

		jobs := importingJobs(t, stagingFixtureLines(t))

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(
				`{"email":"blake@example.com","message":"row one"}`+"\n"+
					`{"this line is not json at all`+"\n",
			), nil).
			Times(1)

		response := newUploaderWithStats(t, apiService, newDestinationConfig(), statsStore).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys)
		require.Empty(t, response.Metadata.SucceededKeys)
		require.Contains(t, response.Metadata.FailedReasons[2], "row one", "the readable row keeps its own reason")

		unrecognized := statsStore.Get("unrecognized_error_row_count", statLabels())
		require.NotNil(t, unrecognized, "an unreadable row must be counted")
		require.EqualValues(t, 1, unrecognized.LastValue())
	})
}

// TestGetUploadStatsTolerantParser pins every shape the errors-document parser accepts and every
// field it will read an identifier or a message out of.
//
// The tolerance is not stylistic. SendGrid's specification mentions the errors URL exactly twice
// and both times only as a bare string, with no media type, no schema and no stated retention, and
// the reference pages describe no format at all. Committing to a single guessed shape would turn
// any difference between the guess and reality into SILENT DATA LOSS, because a row that fails to
// parse leaves a contact that really failed reported as delivered.
func TestGetUploadStatsTolerantParser(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name            string
		document        string
		expectFailed    []int64
		expectSucceeded []int64
		expectReason    string
	}{
		{
			name:            "a bare array of rows",
			document:        `[{"email":"blake@example.com","message":"the address was rejected"}]`,
			expectFailed:    []int64{2},
			expectSucceeded: []int64{1, 3, 4, 5},
			expectReason:    "the address was rejected",
		},
		{
			name:            "rows wrapped under an errors key",
			document:        matchedErrorsDocument,
			expectFailed:    []int64{2, 4},
			expectSucceeded: []int64{1, 3, 5},
			expectReason:    "Invalid email address provided for contact.",
		},
		{
			name:            "rows wrapped under a results key",
			document:        `{"results":[{"email":"erin@example.com","detail":"the contact was not processed"}]}`,
			expectFailed:    []int64{5},
			expectSucceeded: []int64{1, 2, 3, 4},
			expectReason:    "the contact was not processed",
		},
		{
			name:            "a single row object",
			document:        `{"email":"casey@example.com","reason":"the contact was refused"}`,
			expectFailed:    []int64{3},
			expectSucceeded: []int64{1, 2, 4, 5},
			expectReason:    "the contact was refused",
		},
		{
			name: "newline delimited rows",
			document: `{"email":"alex@example.com","message":"the first row failed"}` + "\n" +
				`{"contact":{"email":"blake@example.com"},"error_message":"the second row failed"}` + "\n",
			expectFailed:    []int64{1, 2},
			expectSucceeded: []int64{3, 4, 5},
			expectReason:    "the first row failed",
		},
		{
			name:            "an identifier reported as an external id",
			document:        `[{"external_id":"user_323","message":"rejected by external id"}]`,
			expectFailed:    []int64{3},
			expectSucceeded: []int64{1, 2, 4, 5},
			expectReason:    "rejected by external id",
		},
		{
			name:            "an identifier reported as an anonymous id",
			document:        `[{"anonymous_id":"anon_889","message":"rejected by anonymous id"}]`,
			expectFailed:    []int64{4},
			expectSucceeded: []int64{1, 2, 3, 5},
			expectReason:    "rejected by anonymous id",
		},
		{
			name:            "an identifier reported under the generic identifier key",
			document:        `[{"identifier":"+14155551234","message":"rejected by phone identifier"}]`,
			expectFailed:    []int64{1},
			expectSucceeded: []int64{2, 3, 4, 5},
			expectReason:    "rejected by phone identifier",
		},
		{
			name:            "an identifier whose case differs from the staged address",
			document:        `[{"email":"ERIN@EXAMPLE.COM","message":"rejected with a different case"}]`,
			expectFailed:    []int64{5},
			expectSucceeded: []int64{1, 2, 3, 4},
			expectReason:    "rejected with a different case",
		},
		{
			name:            "a row carrying an identifier but no message at all",
			document:        `[{"email":"erin@example.com"}]`,
			expectFailed:    []int64{5},
			expectSucceeded: []int64{1, 2, 3, 4},
			// A job is never marked failed with an empty explanation.
			expectReason: "without a message",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			jobs := importingJobs(t, stagingFixtureLines(t))

			apiService := newMockAPIService(t)
			apiService.EXPECT().
				GetImportErrors(testErrorsURL).
				Return([]byte(testCase.document), nil).
				Times(1)

			response := newUploader(t, apiService, newDestinationConfig()).
				GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: testErrorsURL,
					Parameters:          importParameters(t, testImportID, len(jobs)),
					ImportingList:       jobs,
				})

			require.Equal(t, 200, response.StatusCode)
			require.ElementsMatch(t, testCase.expectFailed, response.Metadata.FailedKeys)
			require.ElementsMatch(t, testCase.expectSucceeded, response.Metadata.SucceededKeys)
			requireDisjoint(t, response.Metadata.FailedKeys, response.Metadata.SucceededKeys)
			require.Contains(t, response.Metadata.FailedReasons[testCase.expectFailed[0]], testCase.expectReason)
			require.Empty(t, response.Metadata.AbortedKeys)
			require.Empty(t, response.Metadata.WarningKeys)
		})
	}
}

// TestGetUploadStatsUnusableDocument covers the documents no reconciliation can be built from.
//
// Each returns a non-200 so the batch router RETRIES, which is the only safe failure mode: a 200
// carrying an empty failed set would mark every job in a partially errored import delivered, and
// the rejected contacts would be lost silently and permanently.
func TestGetUploadStatsUnusableDocument(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		document     string
		fetchError   error
		expectReason string
	}{
		{
			name:         "bytes that are not JSON at all",
			document:     `this document is not json`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "an empty document",
			document:     ``,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "an empty array",
			document:     `[]`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "an array of rows carrying neither an identifier nor a message",
			document:     `[{"foo":"bar"},{"baz":1}]`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "an object of an entirely unexpected shape",
			document:     `{"unexpected":"shape"}`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "an empty errors array",
			document:     `{"errors":[]}`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name:         "a bare JSON scalar",
			document:     `42`,
			expectReason: "Failed to parse the sendgrid errors document",
		},
		{
			name: "a document that could not be fetched",
			fetchError: &sendgridbulkupload.APIError{
				StatusCode: 403,
				Operation:  "get import errors",
				Message:    "forbidden",
			},
			expectReason: "Failed to fetch the sendgrid errors document",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			jobs := importingJobs(t, stagingFixtureLines(t))

			apiService := newMockAPIService(t)
			if testCase.fetchError != nil {
				apiService.EXPECT().GetImportErrors(testErrorsURL).Return(nil, testCase.fetchError).Times(1)
			} else {
				apiService.EXPECT().GetImportErrors(testErrorsURL).Return([]byte(testCase.document), nil).Times(1)
			}

			response := newUploader(t, apiService, newDestinationConfig()).
				GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: testErrorsURL,
					Parameters:          importParameters(t, testImportID, len(jobs)),
					ImportingList:       jobs,
				})

			require.Equal(t, 500, response.StatusCode)
			require.Contains(t, response.Error, testCase.expectReason)
			// Emphatically NOT an empty failed set with a 200, which would deliver every job.
			require.Empty(t, response.Metadata.FailedKeys)
			require.Empty(t, response.Metadata.SucceededKeys)
			require.Empty(t, response.Metadata.AbortedKeys)
		})
	}
}

// TestGetUploadStatsErrorsURLResolution covers reconciliation running in a process that did not
// poll, so the errors document URL has to be recovered from the persisted import parameters.
//
// This is precisely the situation the stateless design exists for: the URL the poll forwarded is
// preferred because SendGrid supplied it, and when it is absent the import identifier is read back
// out of the parameters and the status is re-read to obtain it.
func TestGetUploadStatsErrorsURLResolution(t *testing.T) {
	t.Parallel()

	t.Run("the import parameters are enough to find the errors document", func(t *testing.T) {
		t.Parallel()

		jobs := importingJobs(t, stagingFixtureLines(t))

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportStatus(testImportID).
			Return(statusFromWireBody(t, importStatusBody(testImportID, "errored", 2, testErrorsURL)), nil).
			Times(1)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(matchedErrorsDocument), nil).
			Times(1)

		response := newUploader(t, apiService, newDestinationConfig()).
			GetUploadStats(common.GetUploadStatsInput{
				// Nothing was forwarded, exactly as when a different pod polled.
				FailedJobParameters: "",
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
	})

	t.Run("several imports contribute their own errors documents", func(t *testing.T) {
		t.Parallel()

		// One upload can produce several imports, and a row left unread in ANY of their documents
		// would make the whole pooled set unsafe to reconcile by exclusion, so all of them are
		// read and their rows accumulated.
		jobs := importingJobs(t, stagingFixtureLines(t))
		secondImportID := testImportID + "-2"
		secondErrorsURL := testErrorsURL + "-2"

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportStatus(testImportID).
			Return(statusFromWireBody(t, importStatusBody(testImportID, "errored", 1, testErrorsURL)), nil).
			Times(1)
		apiService.EXPECT().
			GetImportStatus(secondImportID).
			Return(statusFromWireBody(t, importStatusBody(secondImportID, "errored", 1, secondErrorsURL)), nil).
			Times(1)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(`[{"email":"blake@example.com","message":"the first import rejected this contact"}]`), nil).
			Times(1)
		apiService.EXPECT().
			GetImportErrors(secondErrorsURL).
			Return([]byte(`[{"email":"devon@example.com","message":"the second import rejected this contact"}]`), nil).
			Times(1)

		response := newUploader(t, apiService, newDestinationConfig()).
			GetUploadStats(common.GetUploadStatsInput{
				Parameters:    importParameters(t, testImportID+":"+secondImportID, len(jobs)),
				ImportingList: jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[2], "the first import rejected this contact")
		require.Contains(t, response.Metadata.FailedReasons[4], "the second import rejected this contact")
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
	})

	for _, testCase := range []struct {
		name         string
		parameters   []byte
		setupMock    func(apiService *mockAPIService.MockSendGridAPIService)
		expectReason string
	}{
		{
			name:       "the persisted parameters cannot be parsed",
			parameters: []byte(`{not json`),
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().GetImportStatus(gomock.Any()).Times(0)
				apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)
			},
			expectReason: "Failed to parse parameters",
		},
		{
			name:       "the import status cannot be re-read",
			parameters: nil, // filled in below, per case, so the import id is always valid JSON
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(nil, &sendgridbulkupload.APIError{
						StatusCode: 500,
						Operation:  "get import status",
						Message:    "internal server error",
					}).
					Times(1)
				apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)
			},
			expectReason: "Failed to fetch the sendgrid import status",
		},
		{
			name: "the import reports errored contacts but publishes no document",
			setupMock: func(apiService *mockAPIService.MockSendGridAPIService) {
				apiService.EXPECT().
					GetImportStatus(testImportID).
					Return(statusFromWireBody(t, importStatusBody(testImportID, "errored", 2, "")), nil).
					Times(1)
				apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)
			},
			expectReason: "without publishing an errors document",
		},
	} {
		t.Run("reconciliation is retried when "+testCase.name, func(t *testing.T) {
			t.Parallel()

			jobs := importingJobs(t, stagingFixtureLines(t))

			apiService := newMockAPIService(t)
			testCase.setupMock(apiService)

			parameters := testCase.parameters
			if parameters == nil {
				parameters = importParameters(t, testImportID, len(jobs))
			}

			response := newUploader(t, apiService, newDestinationConfig()).
				GetUploadStats(common.GetUploadStatsInput{
					Parameters:    parameters,
					ImportingList: jobs,
				})

			require.Equal(t, 500, response.StatusCode)
			require.Contains(t, response.Error, testCase.expectReason)
			require.Empty(t, response.Metadata.FailedKeys)
			require.Empty(t, response.Metadata.SucceededKeys)
		})
	}
}

// TestGetUploadStatsIdentifierEdgeCases covers the two ways an identifier can defeat attribution
// even though the document itself was read perfectly, plus a malformed importing list.
//
// Both resolve the same way, and for the same asymmetric reason: SendGrid upserts contacts, so
// failing a job that in fact succeeded costs one idempotent re-upsert bounded by the batch router's
// retry budget, whereas recording a rejected contact as delivered loses it permanently with
// nothing left in the system that could ever detect it.
func TestGetUploadStatsIdentifierEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("an identifier shared by two jobs fails both of them", func(t *testing.T) {
		t.Parallel()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		// Two staged events legitimately carrying the same address: SendGrid upserts them onto one
		// contact, so an errored row genuinely refers to both and neither may be cleared.
		jobs := importingJobs(t, []string{
			stagingLine(t, 10, `{"type":"identify","userId":"user_10","traits":{"email":"shared@example.com"}}`),
			stagingLine(t, 11, `{"type":"track","event":"Signed Up","userId":"user_11","traits":{"email":"shared@example.com"}}`),
			stagingLine(t, 12, `{"type":"identify","userId":"user_12","traits":{"email":"unique@example.com"}}`),
		})

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(`[{"email":"shared@example.com","message":"the contact was rejected"}]`), nil).
			Times(1)

		response := newUploaderWithStats(t, apiService, newDestinationConfig(), statsStore).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{10, 11}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{12}, response.Metadata.SucceededKeys)
		for _, jobID := range []int64{10, 11} {
			require.Contains(t, response.Metadata.FailedReasons[jobID], "the contact was rejected")
			require.Contains(t, response.Metadata.FailedReasons[jobID], "matches 2 jobs")
		}

		ambiguous := statsStore.Get("ambiguous_error_row_count", statLabels())
		require.NotNil(t, ambiguous)
		require.EqualValues(t, 1, ambiguous.LastValue())
	})

	t.Run("an importing job whose identifier cannot be re-derived is failed", func(t *testing.T) {
		t.Parallel()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		jobs := importingJobs(t, []string{
			stagingLine(t, 20, `{"type":"identify","userId":"user_20","traits":{"email":"rejected@example.com"}}`),
			stagingLine(t, 22, `{"type":"identify","userId":"user_22","traits":{"email":"delivered@example.com"}}`),
		})
		// A payload that no longer yields any identifier: it cannot be compared against the errors
		// document in either direction, so it can never be shown to be absent from it, and "not
		// shown to have failed" is not evidence of delivery.
		jobs = append(jobs, &jobsdb.JobT{
			JobID:        21,
			EventPayload: []byte(`{"body":{"JSON":{"type":"track","event":"Page Viewed"}}}`),
		})
		// A nil entry must be skipped rather than dereferenced: this runs inside a shared batch
		// router worker, where a panic would take down every destination it is serving.
		jobs = append(jobs, nil)

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(`[{"email":"rejected@example.com","message":"the contact was rejected"}]`), nil).
			Times(1)

		response := newUploaderWithStats(t, apiService, newDestinationConfig(), statsStore).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		require.ElementsMatch(t, []int64{20, 21}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{22}, response.Metadata.SucceededKeys)
		require.Contains(t, response.Metadata.FailedReasons[20], "the contact was rejected")
		require.Contains(t, response.Metadata.FailedReasons[21], "could not be re-derived")

		unresolvable := statsStore.Get("unresolvable_importing_job_count", statLabels())
		require.NotNil(t, unresolvable)
		require.EqualValues(t, 1, unresolvable.LastValue())
	})

	t.Run("an identifier is re-derived from every payload shape the router may hand over", func(t *testing.T) {
		t.Parallel()

		// Reconciliation keeps no state from the upload, so it re-derives every contact's
		// identifier from the job it is handed - and the batch router may hand that job over in
		// any of three shapes: the queued event, a staging-file line, or the bare message. All
		// three have to resolve, because a shape that does not is indistinguishable from a contact
		// that simply was not in the errors document, which is to say it would be reported
		// delivered. Each shape is exercised in BOTH directions at once: one job of that shape is
		// named in the errors document and one is not.
		jobs := []*jobsdb.JobT{
			// The queued wire shape, which is where Transform reads the message from.
			{JobID: 30, EventPayload: []byte(
				`{"body":{"JSON":{"type":"identify","userId":"user_30","traits":{"email":"wire@example.com"}}}}`)},
			{JobID: 40, EventPayload: []byte(
				`{"body":{"JSON":{"type":"identify","userId":"user_40","traits":{"email":"wire-ok@example.com"}}}}`)},
			// A staging-file line - message plus metadata - exactly as Transform emits it.
			{JobID: 31, EventPayload: []byte(
				`{"message":{"type":"identify","userId":"user_31","traits":{"email":"staged@example.com"}},` +
					`"metadata":{"job_id":31}}`)},
			{JobID: 41, EventPayload: []byte(
				`{"message":{"type":"identify","userId":"user_41","traits":{"email":"staged-ok@example.com"}},` +
					`"metadata":{"job_id":41}}`)},
			// The bare message itself, carrying no envelope at all.
			{JobID: 32, EventPayload: []byte(
				`{"type":"identify","userId":"user_32","traits":{"email":"bare@example.com"}}`)},
			{JobID: 42, EventPayload: []byte(
				`{"type":"identify","userId":"user_42","traits":{"email":"bare-ok@example.com"}}`)},
		}

		apiService := newMockAPIService(t)
		apiService.EXPECT().
			GetImportErrors(testErrorsURL).
			Return([]byte(`[`+
				`{"email":"wire@example.com","message":"the queued contact was rejected"},`+
				`{"email":"staged@example.com","message":"the staged contact was rejected"},`+
				`{"email":"bare@example.com","message":"the bare contact was rejected"}]`), nil).
			Times(1)

		response := newUploader(t, apiService, newDestinationConfig()).
			GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importParameters(t, testImportID, len(jobs)),
				ImportingList:       jobs,
			})

		require.Equal(t, 200, response.StatusCode)
		// A non-empty succeeded set is the crisp signal that no shape went unresolved: an
		// unmatched errors row or an unresolvable job would have tripped the fail-closed branch,
		// which sweeps every importing job into the failed set and leaves this empty.
		require.ElementsMatch(t, []int64{30, 31, 32}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{40, 41, 42}, response.Metadata.SucceededKeys)
		requireDisjoint(t, response.Metadata.FailedKeys, response.Metadata.SucceededKeys)
		// Each row landed on the job of its own shape rather than on some other job that happened
		// to resolve, which a shared or defaulted reason would have hidden.
		require.Contains(t, response.Metadata.FailedReasons[30], "the queued contact was rejected")
		require.Contains(t, response.Metadata.FailedReasons[31], "the staged contact was rejected")
		require.Contains(t, response.Metadata.FailedReasons[32], "the bare contact was rejected")
	})
}
