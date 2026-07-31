// Package sendgridbulkupload_test exercises the SENDGRID_BULK_UPLOAD connector through the four
// methods the batch router actually calls, with the SendGrid API replaced by its generated mock.
//
// It is an EXTERNAL test package on purpose. Every assertion here is written against the exported
// surface - NewManager, the four interface methods, and the wire types - which is precisely the
// surface the batch router depends on, so a change that breaks the framework contract cannot be
// hidden behind an internal helper the router never sees.
//
// Not one test opens a socket. The API is reached only through the SendGridAPIService seam, which
// is what lets a rate limit, a rejected request, a partially errored import and an unparseable
// errors document all be exercised deterministically and in microseconds.
package sendgridbulkupload_test

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	gomock "go.uber.org/mock/gomock"

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
	// testDestinationID is the destination every case reports against, so that an outcome carrying
	// the wrong destination ID - which the batch router keys its bookkeeping on - is caught.
	testDestinationID = "dest-sendgrid-1"

	// fixtureListID is the list ID the staged fixture's first two events target through
	// context.externalId. The destination configuration below targets the same list, which is what
	// makes the whole fixture resolve to ONE request rather than two.
	fixtureListID = "037ae8d4-25b4-496e-adff-2fded15fd0c5"

	// stagingFixture and errorsFixture are read-only test data, never written by any case, which is
	// what keeps the suite safe under the shuffled, parallel execution the Makefile uses.
	stagingFixture = "testdata/uploadData.jsonl"
	errorsFixture  = "testdata/errors.json"

	// testErrorsURL stands in for the opaque URL SendGrid publishes. Nothing ever dials it: the
	// API seam is mocked, so this value only has to be carried faithfully from Poll to
	// GetUploadStats.
	testErrorsURL = "https://api.sendgrid.com/v3/marketing/contacts/imports/errors/abc123"
)

// testConfig is the parsed destination configuration used by the injected-mock cases. It is
// returned by a function rather than held in a variable so that no case can mutate what another
// case reads.
func testConfig() sendgridbulkupload.DestinationConfig {
	return sendgridbulkupload.DestinationConfig{
		APIKey:              "SG.test-api-key",
		ListIDs:             []string{fixtureListID},
		CustomFieldsMapping: map[string]string{"plan": "w1", "signedUpAt": "w2"},
	}
}

// testDestination builds the backend-config destination NewManager is handed.
func testDestination(config map[string]any) *backendconfig.DestinationT {
	return &backendconfig.DestinationT{
		ID:                    testDestinationID,
		Name:                  "SENDGRID_BULK_UPLOAD",
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "SENDGRID_BULK_UPLOAD"},
		Config:                config,
		Enabled:               true,
		WorkspaceID:           "workspace-1",
	}
}

// newMockAPI builds the generated SendGridAPIService mock.
//
// The controller's verification is registered through t.Cleanup rather than deferred, which is what
// makes it correct for the parallel subtests throughout this suite: a deferred Finish would run when
// the subtest function RETURNS, which for a parallel case is before its body has actually finished.
// Every unmet or unexpected expectation therefore fails the case that caused it.
func newMockAPI(t *testing.T) *mockAPIService.MockSendGridAPIService {
	t.Helper()
	controller := gomock.NewController(t)
	t.Cleanup(controller.Finish)
	return mockAPIService.NewMockSendGridAPIService(controller)
}

// newUploader assembles the connector around the mocked API. The caps are left at zero unless a
// case overrides them, which selects the production defaults.
func newUploader(api sendgridbulkupload.SendGridAPIService) *sendgridbulkupload.SendGridBulkUploader {
	return &sendgridbulkupload.SendGridBulkUploader{
		Logger:             logger.NOP,
		StatsFactory:       stats.NOP,
		DestinationID:      testDestinationID,
		DestinationConfig:  testConfig(),
		SendGridAPIService: api,
	}
}

// stagingLine renders one staging-file line in exactly the shape Transform writes.
func stagingLine(t *testing.T, jobID int64, message string) string {
	t.Helper()
	line, err := common.GetMarshalledData(message, jobID)
	require.NoError(t, err)
	return line
}

// contactLine renders a minimal but complete staged contact for one job.
func contactLine(t *testing.T, jobID int64) string {
	t.Helper()
	return stagingLine(t, jobID, fmt.Sprintf(
		`{"type":"identify","userId":"user_%d","traits":{"email":"user%d@example.com"}}`, jobID, jobID))
}

// writeStagingFile writes lines to a per-case temporary file. t.TempDir is removed by the testing
// package when the case ends, so nothing is left behind and no two cases share a path.
func writeStagingFile(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "staging.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	return path
}

// fixtureJobs rebuilds the importing job list from the staged fixture.
//
// The job's payload IS the staging line, which is the shape reconciliation is designed to read:
// GetUploadStats re-derives every contact identifier from the importing jobs themselves, so feeding
// it the very lines that were uploaded is what makes the identifier matching a genuine test of the
// stateless reconciliation rather than of a fixture written to agree with it.
func fixtureJobs(t *testing.T, jobIDs ...int64) []*jobsdb.JobT {
	t.Helper()
	raw, err := os.ReadFile(stagingFixture)
	require.NoError(t, err)

	byJobID := make(map[int64][]byte)
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		byJobID[gjson.Get(line, "metadata.job_id").Int()] = []byte(line)
	}
	jobs := make([]*jobsdb.JobT, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		payload, ok := byJobID[jobID]
		require.Truef(t, ok, "job %d is missing from %s", jobID, stagingFixture)
		jobs = append(jobs, &jobsdb.JobT{JobID: jobID, EventPayload: payload})
	}
	return jobs
}

// firstSyntheticContact is the contact identifier syntheticImportingJobs gives its FIRST job, and
// therefore the one address a synthetic errors document can name to attribute to exactly one job.
const firstSyntheticContact = "user1@example.com"

// syntheticImportingJobs builds an importing list of the given size, each entry in exactly the
// staging-file shape Transform writes, so that reconciliation can re-derive every contact identifier
// from it the way it does in production.
//
// The committed five-line fixture cannot express an upload larger than the errors-row allowance's
// floor, and that is the only size at which the allowance's derivation from the upload it is
// reconciling becomes observable through the exported surface at all. Job N carries
// userN@example.com, so firstSyntheticContact resolves to exactly one job however large the list is.
func syntheticImportingJobs(t *testing.T, count int) []*jobsdb.JobT {
	t.Helper()

	jobs := make([]*jobsdb.JobT, 0, count)
	for jobID := int64(1); jobID <= int64(count); jobID++ {
		jobs = append(jobs, &jobsdb.JobT{JobID: jobID, EventPayload: []byte(contactLine(t, jobID))})
	}
	return jobs
}

// errorsDocumentNaming renders a wrapped errors document of the given row count, every row naming the
// same contact, with the last row carrying a message so the reason a job records is a real one.
//
// One address throughout is deliberate: it keeps a case about the row ALLOWANCE from also being a
// case about matching, because every row attributes to exactly one job and no row can be
// unattributable.
func errorsDocumentNaming(email string, rows int) string {
	return `{"errors":[` +
		strings.Repeat(`{"email":"`+email+`"},`, rows-1) +
		`{"email":"` + email + `","message":"rejected"}]}`
}

// importParametersOf reads back what Upload persisted, through the SAME gjson path the batch router
// uses in getPollInput. Asserting on the router's own read is the point: a value the router cannot
// recover would strand every job of the batch in the importing state.
func importParametersOf(t *testing.T, output common.AsyncUploadOutput) (string, int64) {
	t.Helper()
	require.NotNil(t, output.ImportingParameters)
	importID := gjson.GetBytes(output.ImportingParameters, "importId")
	importCount := gjson.GetBytes(output.ImportingParameters, "importCount")
	require.True(t, importID.Exists(), "importId must be recoverable by gjson")
	require.True(t, importCount.Exists(), "importCount must be recoverable by gjson")
	return importID.String(), importCount.Int()
}

// persistedParameters renders the job parameters the batch router stores for an importing job, which
// is the only channel through which reconciliation can recover the import identifier when it runs in
// a different process invocation from the Upload that produced it.
//
// It is built through the shared import-parameters struct rather than hand-written, so a change to
// the field names the router reads would break these cases rather than passing silently.
func persistedParameters(t *testing.T, importID string, importCount int) []byte {
	t.Helper()
	parameters, err := jsonrs.Marshal(common.ImportParameters{
		ImportId:    importID,
		ImportCount: importCount,
	})
	require.NoError(t, err)
	require.Equal(t, importID, gjson.GetBytes(parameters, "importId").String())
	return parameters
}

// pollStatus builds an import status response with the nested results object the API actually
// returns.
func pollStatus(importID, status string, erroredCount int, errorsURL string) *sendgridbulkupload.ImportStatusResponse {
	return &sendgridbulkupload.ImportStatusResponse{
		ID:      importID,
		Status:  status,
		JobType: "upsert",
		Results: sendgridbulkupload.ImportResults{
			RequestedCount: 5,
			CreatedCount:   5 - erroredCount,
			ErroredCount:   erroredCount,
			ErrorsURL:      errorsURL,
		},
	}
}

// errorsDocument returns the committed partial-failure fixture.
//
// It deliberately carries a third row for an address no staged job used, so it is the fixture for the
// FAIL-CLOSED path: an import that publishes it cannot be reconciled by exclusion. Cases that are
// about something else - rebuilding outcomes from the manifest, reading a shared document once, a nil
// entry in the importing list - use attributableErrorsDocument instead, so the policy under test is
// the one named in the case.
func errorsDocument(t *testing.T) []byte {
	t.Helper()
	document, err := os.ReadFile(errorsFixture)
	require.NoError(t, err)
	return document
}

// attributableErrorsDocument returns an errors document EVERY row of which resolves to a staged job:
// blake@example.com is job 2 through "email"+"message", devon@example.com is job 4 through
// "contact.email"+"error_message". Both key spellings are exercised and nothing is left over, which
// is the precondition that makes succeeding the remainder by exclusion sound.
func attributableErrorsDocument() []byte {
	return []byte(`{"errors":[` +
		`{"email":"blake@example.com","message":"Invalid email address provided for contact.","error_indices":[1]},` +
		`{"contact":{"email":"devon@example.com"},"error_message":"Contact rejected: custom field value exceeds the maximum allowed length.","error_indices":[3]}` +
		`]}`)
}

// TestNewManager covers construction: the API key guard that must fail a misconfigured destination
// once and clearly rather than once per batch, and the typed configuration the manager is built from.
func TestNewManager(t *testing.T) {
	t.Parallel()

	t.Run("rejects a destination without a usable api key", func(t *testing.T) {
		t.Parallel()

		for name, config := range map[string]map[string]any{
			"absent":     {"listIds": []string{fixtureListID}},
			"empty":      {"apiKey": ""},
			"whitespace": {"apiKey": "   "},
			"not a string": {
				// A non-string value fails the typed jsonrs round trip, which is the single
				// reading of the control plane's configuration.
				"apiKey": 42,
			},
			"nil config": nil,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, testDestination(config))
				require.Error(t, err)
				require.Nil(t, manager)
			})
		}
	})

	t.Run("parses the typed destination configuration", func(t *testing.T) {
		t.Parallel()

		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, testDestination(map[string]any{
			"apiKey":              "  SG.spaced-key  ",
			"listIds":             []any{fixtureListID, "second-list"},
			"customFieldsMapping": map[string]any{"plan": "w1", "signedUpAt": "w2"},
		}))
		require.NoError(t, err)
		require.NotNil(t, manager)
		require.Equal(t, testDestinationID, manager.DestinationID)
		require.Equal(t, "  SG.spaced-key  ", manager.DestinationConfig.APIKey)
		require.Equal(t, []string{fixtureListID, "second-list"}, manager.DestinationConfig.ListIDs)
		require.Equal(t, map[string]string{"plan": "w1", "signedUpAt": "w2"}, manager.DestinationConfig.CustomFieldsMapping)
		require.NotNil(t, manager.SendGridAPIService)
	})

	t.Run("satisfies the async destination manager contract", func(t *testing.T) {
		t.Parallel()

		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, testDestination(map[string]any{"apiKey": "SG.k"}))
		require.NoError(t, err)

		var contract common.AsyncDestinationManager = manager
		require.NotNil(t, contract)
	})

	t.Run("tolerates a nil logger and stats factory", func(t *testing.T) {
		t.Parallel()

		manager, err := sendgridbulkupload.NewManager(nil, nil, testDestination(map[string]any{"apiKey": "SG.k"}))
		require.NoError(t, err)
		require.NotNil(t, manager.Logger)
		require.NotNil(t, manager.StatsFactory)
	})
}

// TestTransform pins the staging-line shape and the event-type agnosticism the feature requires:
// both track and identify reduce to one contact line tagged with its originating job.
func TestTransform(t *testing.T) {
	t.Parallel()

	uploader := newUploader(newMockAPI(t))
	for name, eventType := range map[string]string{"identify": "identify", "track": "track"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			payload := fmt.Sprintf(
				`{"body":{"JSON":{"type":%q,"userId":"user_9","traits":{"email":"Nine@Example.com"}}},"endpoint":"ignored"}`,
				eventType)
			transformed, err := uploader.Transform(&jobsdb.JobT{JobID: 9, EventPayload: []byte(payload)})
			require.NoError(t, err)

			// The metadata key is job_id, which is what the shared marshalling helper writes and
			// what Upload reads back.
			require.Equal(t, int64(9), gjson.Get(transformed, "metadata.job_id").Int())
			require.Equal(t, eventType, gjson.Get(transformed, "message.type").String())
			require.Equal(t, "Nine@Example.com", gjson.Get(transformed, "message.traits.email").String())
			// Only body.JSON is carried through; the surrounding router envelope is not.
			require.False(t, gjson.Get(transformed, "message.endpoint").Exists())
		})
	}
}

// TestImportStatusResponseIsNested is the smallest test in the suite and guards the single most
// dangerous detail in the feature.
//
// SendGrid nests the counters and the errors document URL inside a results object. A flat Go struct
// would unmarshal this document without error and read errored_count as zero forever, so every
// partially errored import would be reported as a clean success - exactly the silent data loss
// reconciliation exists to prevent. This decodes the documented shape and insists the values arrive.
func TestImportStatusResponseIsNested(t *testing.T) {
	t.Parallel()

	document := []byte(`{
		"id": "sg-import-1",
		"status": "errored",
		"job_type": "upsert",
		"results": {
			"requested_count": 5,
			"created_count": 3,
			"updated_count": 0,
			"deleted_count": 0,
			"errored_count": 2,
			"errors_url": "` + testErrorsURL + `"
		},
		"started_at": "2026-02-25T12:00:00Z",
		"finished_at": "2026-02-25T12:00:30Z"
	}`)

	var status sendgridbulkupload.ImportStatusResponse
	require.NoError(t, jsonrs.Unmarshal(document, &status))
	require.Equal(t, "sg-import-1", status.ID)
	require.Equal(t, "errored", status.Status)
	require.Equal(t, 5, status.Results.RequestedCount)
	require.Equal(t, 3, status.Results.CreatedCount)
	require.Equal(t, 2, status.Results.ErroredCount, "errored_count must be read from the NESTED results object")
	require.Equal(t, testErrorsURL, status.Results.ErrorsURL)
}

// TestUploadHappyPath is scenario S1: five staged track and identify events become ONE upsert
// carrying the right contact fields and list IDs, the accepted job IDs come back as importing, the
// import identifier round-trips through the batch router's own read, and nothing is failed or
// aborted. Poll then reports the import complete with no failure.
func TestUploadHappyPath(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	var captured sendgridbulkupload.UpsertRequest
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			captured = request
			return &sendgridbulkupload.UpsertResponse{JobID: "sg-import-1"}, nil
		})

	uploader := newUploader(api)
	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        stagingFixture,
		ImportingJobIDs: []int64{1, 2, 3, 4, 5},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	// One request, carrying the list IDs every contact in it resolved to.
	require.Equal(t, []string{fixtureListID}, captured.ListIDs)
	require.Len(t, captured.Contacts, 5)

	byEmail := make(map[string]sendgridbulkupload.Contact, len(captured.Contacts))
	for _, contact := range captured.Contacts {
		byEmail[contact.Email] = contact
	}
	require.Len(t, byEmail, 5)

	// The documented mapping, including userId -> external_id, which is this repository's own
	// SendGrid convention rather than an invention of the connector.
	alex := byEmail["alex@example.com"]
	require.Equal(t, "user_123", alex.ExternalID)
	require.Equal(t, "anon_456", alex.AnonymousID)
	require.Equal(t, "Alex", alex.FirstName)
	require.Equal(t, "Keener", alex.LastName)
	require.Equal(t, "+14155551234", alex.PhoneNumberID)
	require.Equal(t, "123 Main St", alex.AddressLine1)
	require.Equal(t, "San Francisco", alex.City)
	require.Equal(t, "CA", alex.StateProvinceRegion)
	require.Equal(t, "94105", alex.PostalCode)
	require.Equal(t, "US", alex.Country)
	// Custom fields are keyed by the PRE-CREATED SendGrid field IDs the mapping supplies, never by
	// the trait name, because SendGrid requires the field to exist before a value can be set.
	require.Equal(t, "enterprise", alex.CustomFields["w1"])
	require.Equal(t, "2026-01-15", alex.CustomFields["w2"])

	// A track event is accepted exactly like an identify.
	require.Equal(t, "user_223", byEmail["blake@example.com"].ExternalID)
	// Alternate emails and an anonymous-only event both survive the mapping.
	require.Equal(t, []string{"casey.alt@example.com", "casey.work@example.com"}, byEmail["casey@example.com"].AlternateEmails)
	require.Equal(t, "anon_889", byEmail["devon@example.com"].AnonymousID)
	require.Empty(t, byEmail["devon@example.com"].ExternalID)

	require.Equal(t, []int64{1, 2, 3, 4, 5}, output.ImportingJobIDs)
	require.Equal(t, 5, output.ImportingCount)
	require.Equal(t, testDestinationID, output.DestinationID)
	require.Empty(t, output.FailedJobIDs)
	require.Zero(t, output.FailedCount)
	require.Empty(t, output.AbortJobIDs)
	require.Zero(t, output.AbortCount)

	importID, importCount := importParametersOf(t, output)
	require.Contains(t, importID, "sg-import-1")
	require.Equal(t, int64(5), importCount, "importCount must be the number of jobs actually accepted")

	// The same value decodes as the shared import-parameters struct, whose ImportId is typed any -
	// SendGrid's string job_id therefore round-trips with no coercion at either end.
	var parameters common.ImportParameters
	require.NoError(t, jsonrs.Unmarshal(output.ImportingParameters, &parameters))
	require.Equal(t, importID, parameters.ImportId)
	require.Equal(t, 5, parameters.ImportCount)

	api.EXPECT().GetImportStatus("sg-import-1").Times(1).Return(pollStatus("sg-import-1", "completed", 0, ""), nil)
	pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: 5})
	require.Equal(t, http.StatusOK, pollResponse.StatusCode)
	require.True(t, pollResponse.Complete)
	require.False(t, pollResponse.InProgress)
	require.False(t, pollResponse.HasFailed)
	require.False(t, pollResponse.HasWarning)
	require.Empty(t, pollResponse.FailedJobParameters)
	require.Empty(t, pollResponse.WarningJobParameters)
}

// TestUploadRateLimited is scenario S3, and it is the awkward case the connector exists to get
// right: a 429 must NEVER abort. The affected jobs come back retryable with the advertised reset
// window in the reason, and no importing state is produced, which is what makes the batch router
// release the struct and re-queue the jobs.
func TestUploadRateLimited(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 2, 25, 12, 30, 0, 0, time.UTC)
	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(nil, &sendgridbulkupload.RateLimitError{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: "120",
		ResetAt:    resetAt.Unix(),
		Limit:      600,
		Remaining:  0,
		Message:    "too many requests",
	})

	output := newUploader(api).Upload(&common.AsyncDestinationStruct{
		FileName:        stagingFixture,
		ImportingJobIDs: []int64{1, 2, 3, 4, 5},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, output.FailedJobIDs)
	require.Equal(t, 5, output.FailedCount)
	require.Equal(t, testDestinationID, output.DestinationID)

	// The reset window has to reach the operator-facing reason, in both the forms SendGrid may
	// advertise it in.
	require.Contains(t, output.FailedReason, "Retry-After: 120")
	require.Contains(t, output.FailedReason, resetAt.Format(time.RFC3339))
	require.Contains(t, output.FailedReason, "600")

	// The contract of the awkward case, stated as three assertions.
	require.Empty(t, output.AbortJobIDs, "a rate limit must never abort a job")
	require.Zero(t, output.AbortCount)
	require.Empty(t, output.ImportingJobIDs)
	require.Nil(t, output.ImportingParameters)
	require.Zero(t, output.ImportingCount)
}

// TestUploadMultiChunkPartialRateLimit pins the boundary between the two channels when one request
// of several is rate limited: the accepted chunk keeps its importing state, only the rejected
// chunk's jobs are retried, the two sets are disjoint, and nothing is aborted.
func TestUploadMultiChunkPartialRateLimit(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	gomock.InOrder(
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-accepted"}, nil),
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(nil, &sendgridbulkupload.RateLimitError{
			StatusCode: http.StatusTooManyRequests,
			ResetAt:    time.Date(2026, 2, 25, 13, 0, 0, 0, time.UTC).Unix(),
			Message:    "too many requests",
		}),
	)

	uploader := newUploader(api)
	uploader.MaxContactsPerRequest = 2
	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName: writeStagingFile(t,
			contactLine(t, 1), contactLine(t, 2), contactLine(t, 3), contactLine(t, 4)),
		ImportingJobIDs: []int64{1, 2, 3, 4},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.Equal(t, []int64{1, 2}, output.ImportingJobIDs)
	require.Equal(t, 2, output.ImportingCount)
	require.ElementsMatch(t, []int64{3, 4}, output.FailedJobIDs)
	require.Equal(t, 2, output.FailedCount)
	require.Empty(t, output.AbortJobIDs)

	// Disjoint, which the batch router relies on: a job in two sets would receive two statuses.
	for _, importing := range output.ImportingJobIDs {
		require.NotContains(t, output.FailedJobIDs, importing)
	}

	importID, importCount := importParametersOf(t, output)
	require.Contains(t, importID, "sg-accepted")
	require.Equal(t, int64(2), importCount)
}

// TestUploadChunkBoundaries walks the dual-cap chunker across both caps, one below, exactly at and
// one above each, and insists no chunk is ever empty and that a chunk's jobs are exactly the jobs
// whose contacts it carried.
func TestUploadChunkBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("element cap", func(t *testing.T) {
		t.Parallel()

		for name, testCase := range map[string]struct {
			jobCount       int
			cap            int
			expectedChunks []int
		}{
			"one below the cap":     {jobCount: 2, cap: 3, expectedChunks: []int{2}},
			"exactly at the cap":    {jobCount: 3, cap: 3, expectedChunks: []int{3}},
			"one above the cap":     {jobCount: 4, cap: 3, expectedChunks: []int{3, 1}},
			"exactly twice the cap": {jobCount: 6, cap: 3, expectedChunks: []int{3, 3}},
			"a single contact":      {jobCount: 1, cap: 3, expectedChunks: []int{1}},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				lines := make([]string, 0, testCase.jobCount)
				jobIDs := make([]int64, 0, testCase.jobCount)
				for jobID := int64(1); jobID <= int64(testCase.jobCount); jobID++ {
					lines = append(lines, contactLine(t, jobID))
					jobIDs = append(jobIDs, jobID)
				}

				api := newMockAPI(t)
				chunkSizes := make([]int, 0, len(testCase.expectedChunks))
				api.EXPECT().UploadContacts(gomock.Any()).Times(len(testCase.expectedChunks)).DoAndReturn(
					func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						require.NotEmpty(t, request.Contacts, "a chunk must never be empty")
						chunkSizes = append(chunkSizes, len(request.Contacts))
						return &sendgridbulkupload.UpsertResponse{
							JobID: "sg-" + strconv.Itoa(len(chunkSizes)),
						}, nil
					})

				uploader := newUploader(api)
				uploader.MaxContactsPerRequest = testCase.cap
				output := uploader.Upload(&common.AsyncDestinationStruct{
					FileName:        writeStagingFile(t, lines...),
					ImportingJobIDs: jobIDs,
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				require.Equal(t, testCase.expectedChunks, chunkSizes)
				require.Equal(t, jobIDs, output.ImportingJobIDs)
				require.Empty(t, output.FailedJobIDs)
				require.Empty(t, output.AbortJobIDs)

				// Index alignment, asserted through the persisted membership: every chunk's import
				// names exactly the jobs whose contacts travelled in it, in file order.
				importID, importCount := importParametersOf(t, output)
				require.Equal(t, int64(testCase.jobCount), importCount)
				for index := range testCase.expectedChunks {
					require.Contains(t, importID, "sg-"+strconv.Itoa(index+1))
				}
			})
		}
	})

	t.Run("byte cap", func(t *testing.T) {
		t.Parallel()

		// The budget is expressed in terms of the ACTUAL marshalled contact rather than a magic
		// number, because the chunker measures each element with the same marshaller and adds one
		// byte for the comma that will separate it from its neighbor. Deriving the cap the same way
		// the chunker spends it is what makes the boundary exact instead of approximate.
		marshalled, err := jsonrs.Marshal(sendgridbulkupload.Contact{
			Email:      "user1@example.com",
			ExternalID: "user_1",
		})
		require.NoError(t, err)
		perContact := len(marshalled) + 1

		// The cap governs the WHOLE serialized body, so the {"list_ids":[...],"contacts":[]}
		// envelope is charged against it too and the uploader measures it exactly, once per list
		// group, before handing the chunker what is left. A budget expressed only in contacts would
		// therefore be short by the envelope and split the batch one element too early, so the
		// envelope is measured here the same way - which is what keeps each boundary below exact.
		envelope, err := jsonrs.Marshal(sendgridbulkupload.UpsertRequest{
			ListIDs:  []string{fixtureListID},
			Contacts: []sendgridbulkupload.Contact{},
		})
		require.NoError(t, err)
		envelopeBytes := len(envelope)

		for name, testCase := range map[string]struct {
			contactsPerChunk int
			expectedChunks   []int
		}{
			"a budget that fits exactly one contact": {contactsPerChunk: 1, expectedChunks: []int{1, 1, 1, 1}},
			"a budget that fits exactly two":         {contactsPerChunk: 2, expectedChunks: []int{2, 2}},
			"a budget that fits exactly three":       {contactsPerChunk: 3, expectedChunks: []int{3, 1}},
			"a budget that fits the whole batch":     {contactsPerChunk: 4, expectedChunks: []int{4}},
			"a budget larger than the whole batch":   {contactsPerChunk: 9, expectedChunks: []int{4}},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				api := newMockAPI(t)
				chunkSizes := make([]int, 0, len(testCase.expectedChunks))
				api.EXPECT().UploadContacts(gomock.Any()).Times(len(testCase.expectedChunks)).DoAndReturn(
					func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						require.NotEmpty(t, request.Contacts, "a chunk must never be empty")
						chunkSizes = append(chunkSizes, len(request.Contacts))
						return &sendgridbulkupload.UpsertResponse{
							JobID: "sg-" + strconv.Itoa(len(chunkSizes)),
						}, nil
					})

				uploader := newUploader(api)
				// Far above the element cap, so only the byte budget can be what splits the batch.
				uploader.MaxContactsPerRequest = 1000
				uploader.MaxRequestBytes = envelopeBytes + testCase.contactsPerChunk*perContact + 1
				output := uploader.Upload(&common.AsyncDestinationStruct{
					FileName: writeStagingFile(t,
						contactLine(t, 1), contactLine(t, 2), contactLine(t, 3), contactLine(t, 4)),
					ImportingJobIDs: []int64{1, 2, 3, 4},
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				require.Equal(t, testCase.expectedChunks, chunkSizes)
				require.Equal(t, []int64{1, 2, 3, 4}, output.ImportingJobIDs)
				require.Empty(t, output.FailedJobIDs)
				require.Empty(t, output.AbortJobIDs)
			})
		}
	})

	t.Run("an override above the documented ceiling is clamped, not honored", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				// 30,000 is the documented element ceiling; an operator asking for more cannot be
				// given more, so the four staged contacts still travel in one request rather than
				// the request being built against an impossible cap.
				require.Len(t, request.Contacts, 4)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		uploader := newUploader(api)
		uploader.MaxContactsPerRequest = 10_000_000
		uploader.MaxRequestBytes = 10 << 30
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				contactLine(t, 1), contactLine(t, 2), contactLine(t, 3), contactLine(t, 4)),
			ImportingJobIDs: []int64{1, 2, 3, 4},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1, 2, 3, 4}, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})
}

// TestUploadRejectsUnusableRecords covers the three local, PERMANENT rejections. Each abandons
// exactly one job on the terminal channel while every other contact in the batch is still delivered,
// which is the whole point: one unusable record must not poison the request it happens to share.
func TestUploadRejectsUnusableRecords(t *testing.T) {
	t.Parallel()

	t.Run("a contact carrying none of the four identifiers", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 1)
				require.Equal(t, "user1@example.com", request.Contacts[0].Email)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				contactLine(t, 1),
				stagingLine(t, 2, `{"type":"track","event":"No Identity","properties":{"plan":"growth"}}`)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "at least one of email")
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("a malformed staging line that still names its job", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				contactLine(t, 1),
				`{"message":"not an object","metadata":{"job_id":2}}`),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "malformed")
	})

	t.Run("a contact larger than any request budget", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 1)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		// 250 characters, deliberately just inside the 255 this connector will send for a name: the
		// contact has to be REJECTED BY THE CHUNKER for exceeding the request budget, which is what
		// this case pins, and a name long enough to trip the field-length bound instead would be
		// rejected earlier for a different and equally correct reason, silently retargeting the test.
		oversized := stagingLine(t, 2, fmt.Sprintf(
			`{"type":"identify","userId":"user_2","traits":{"email":"two@example.com","firstName":%q}}`,
			strings.Repeat("x", 250)))

		uploader := newUploader(api)
		uploader.MaxRequestBytes = 200
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1), oversized),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "larger than the maximum sendgrid request size")
	})

	t.Run("an accepted upload that returns no import job id", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "  "}, nil)

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		// Retryable, never importing: an import with no identifier could never be polled.
		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("an import job id that cannot be persisted", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg;1=2"}, nil)

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Empty(t, output.AbortJobIDs)
	})
}

// TestUploadRejectedRequestIsRetryable covers every provider failure that is NOT a rate limit. All
// of them are retryable, because the batch router alone owns the decision to give up.
func TestUploadRejectedRequestIsRetryable(t *testing.T) {
	t.Parallel()

	for name, uploadErr := range map[string]error{
		"a rejected request": &sendgridbulkupload.APIError{
			StatusCode: http.StatusBadRequest,
			Operation:  "upsert contacts",
			Message:    "invalid list id",
		},
		"an authorization failure": &sendgridbulkupload.APIError{
			StatusCode: http.StatusUnauthorized,
			Operation:  "upsert contacts",
			Message:    "permission denied",
		},
		"a provider outage": &sendgridbulkupload.APIError{
			StatusCode: http.StatusServiceUnavailable,
			Operation:  "upsert contacts",
		},
		"a transport failure": errors.New("dial tcp: connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(nil, uploadErr)

			output := newUploader(api).Upload(&common.AsyncDestinationStruct{
				FileName:        writeStagingFile(t, contactLine(t, 1), contactLine(t, 2)),
				ImportingJobIDs: []int64{1, 2},
				Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
			})

			require.ElementsMatch(t, []int64{1, 2}, output.FailedJobIDs)
			require.Equal(t, 2, output.FailedCount)
			require.NotEmpty(t, output.FailedReason)
			require.Empty(t, output.AbortJobIDs, "no provider response is terminal")
			require.Empty(t, output.ImportingJobIDs)
		})
	}
}

// TestUploadUnreadableStagingFile checks the batch-level failure: nothing can be said about any
// individual job, so every job in the batch is retried rather than lost.
func TestUploadUnreadableStagingFile(t *testing.T) {
	t.Parallel()

	output := newUploader(newMockAPI(t)).Upload(&common.AsyncDestinationStruct{
		FileName:        filepath.Join(t.TempDir(), "does-not-exist.jsonl"),
		ImportingJobIDs: []int64{1, 2, 3},
		FailedJobIDs:    []int64{4},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.ElementsMatch(t, []int64{1, 2, 3, 4}, output.FailedJobIDs)
	require.Equal(t, 4, output.FailedCount)
	require.Empty(t, output.ImportingJobIDs)
	require.Empty(t, output.AbortJobIDs)
}

// TestUploadWithoutABatch guards a shared batch router worker: a missing async destination struct
// must be answered, not panicked on.
func TestUploadWithoutABatch(t *testing.T) {
	t.Parallel()

	output := newUploader(newMockAPI(t)).Upload(nil)
	require.Equal(t, testDestinationID, output.DestinationID)
	require.Empty(t, output.ImportingJobIDs)
	require.Empty(t, output.FailedJobIDs)
	require.Empty(t, output.AbortJobIDs)
}

// TestPollStatusMapping walks the whole documented status enumeration plus every way a status read
// can fail, and asserts the batch-router-visible verdict for each. The two rows that matter most are
// the errored one and the completed-with-errors one: neither may be reported as blanket success,
// because the router marks EVERY importing job succeeded unless HasFailed is set.
func TestPollStatusMapping(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		status              string
		erroredCount        int
		errorsURL           string
		expectedStatusCode  int
		expectedComplete    bool
		expectedInProgress  bool
		expectedHasFailed   bool
		expectFailedDetails bool
		expectedErrorText   string
	}{
		"pending keeps the batch polling": {
			status:             "pending",
			expectedStatusCode: http.StatusOK,
			expectedInProgress: true,
		},
		"completed with no errors is a clean success": {
			status:             "completed",
			expectedStatusCode: http.StatusOK,
			expectedComplete:   true,
		},
		"completed with errors still reconciles": {
			status:              "completed",
			erroredCount:        2,
			errorsURL:           testErrorsURL,
			expectedStatusCode:  http.StatusOK,
			expectedComplete:    true,
			expectedHasFailed:   true,
			expectFailedDetails: true,
		},
		"errored reconciles": {
			status:              "errored",
			erroredCount:        2,
			errorsURL:           testErrorsURL,
			expectedStatusCode:  http.StatusOK,
			expectedComplete:    true,
			expectedHasFailed:   true,
			expectFailedDetails: true,
		},
		"errored without a published document still reconciles": {
			status:              "errored",
			erroredCount:        1,
			expectedStatusCode:  http.StatusOK,
			expectedComplete:    true,
			expectedHasFailed:   true,
			expectFailedDetails: true,
		},
		"failed is terminal": {
			status:             "failed",
			erroredCount:       5,
			errorsURL:          testErrorsURL,
			expectedStatusCode: http.StatusBadRequest,
			expectedComplete:   true,
			expectedHasFailed:  true,
			expectedErrorText:  "SendGrid Bulk Upload Failed",
		},
		"an unrecognized status is retried, never guessed at": {
			status:             "quiesced",
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorText:  "Unknown status: quiesced",
		},
		"a status compared case insensitively": {
			status:             "  COMPLETED  ",
			expectedStatusCode: http.StatusOK,
			expectedComplete:   true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().GetImportStatus("sg-1").Times(1).Return(
				pollStatus("sg-1", testCase.status, testCase.erroredCount, testCase.errorsURL), nil)

			response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-1", ImportCount: 5})

			require.Equal(t, testCase.expectedStatusCode, response.StatusCode)
			require.Equal(t, testCase.expectedComplete, response.Complete)
			require.Equal(t, testCase.expectedInProgress, response.InProgress)
			require.Equal(t, testCase.expectedHasFailed, response.HasFailed)
			if testCase.expectedErrorText != "" {
				require.Contains(t, response.Error, testCase.expectedErrorText)
			}
			if testCase.expectFailedDetails {
				require.NotEmpty(t, response.FailedJobParameters,
					"reconciliation needs the import's own state and document")
			} else {
				require.Empty(t, response.FailedJobParameters)
			}

			// SendGrid has no warning tier, so these must be zero on every single row.
			require.False(t, response.HasWarning)
			require.Empty(t, response.WarningJobParameters)
		})
	}
}

// TestPollStatusReadFailures covers the ways the status read itself can fail. Every one of them is
// retryable, and a rate limit reports its own status code so the reset window reaches the operator.
func TestPollStatusReadFailures(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 3, 1, 9, 15, 0, 0, time.UTC)

	t.Run("a rate limit while polling", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(nil, &sendgridbulkupload.RateLimitError{
			StatusCode: http.StatusTooManyRequests,
			ResetAt:    resetAt.Unix(),
			Limit:      600,
			Remaining:  0,
		})

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-1", ImportCount: 2})
		require.Equal(t, http.StatusTooManyRequests, response.StatusCode)
		require.False(t, response.Complete)
		require.Contains(t, response.Error, resetAt.Format(time.RFC3339))
	})

	t.Run("a rejected status read", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(nil, &sendgridbulkupload.APIError{
			StatusCode: http.StatusNotFound,
			Operation:  "get import status",
			Message:    "not found",
		})

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-1", ImportCount: 2})
		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.False(t, response.Complete)
		require.Contains(t, response.Error, "not found")
	})

	t.Run("a status response that is absent altogether", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(nil, nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-1", ImportCount: 2})
		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.False(t, response.Complete)
		require.NotEmpty(t, response.Error)
	})

	t.Run("no import id was persisted at all", func(t *testing.T) {
		t.Parallel()

		for name, importID := range map[string]string{
			"an empty identifier":         "",
			"a whitespace identifier":     "   ",
			"separators and nothing else": ";;",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				// The API is never reached, which is asserted by the controller: no call was
				// expected on it.
				response := newUploader(newMockAPI(t)).Poll(common.AsyncPoll{ImportId: importID, ImportCount: 3})
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				require.False(t, response.Complete)
				require.NotEmpty(t, response.Error)
			})
		}
	})
}

// TestPollMultipleImports covers the case per-import membership exists for: one upload, several
// imports, states that do not agree. The strongest outcome has to win, and a mixture must reach
// reconciliation rather than being resolved wholesale in either direction.
func TestPollMultipleImports(t *testing.T) {
	t.Parallel()

	t.Run("any pending import keeps the whole batch polling", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "pending", 0, ""), nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4", ImportCount: 4})
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.True(t, response.InProgress)
		require.False(t, response.Complete)
		require.False(t, response.HasFailed)
	})

	t.Run("every import clean is a clean success", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "completed", 0, ""), nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4", ImportCount: 4})
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.True(t, response.Complete)
		require.False(t, response.HasFailed)
		require.Empty(t, response.FailedJobParameters)
	})

	t.Run("every import rejected is terminal for the whole batch", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "failed", 2, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "failed", 2, ""), nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4", ImportCount: 4})
		require.Equal(t, http.StatusBadRequest, response.StatusCode)
		require.True(t, response.Complete)
		require.True(t, response.HasFailed)
		require.Contains(t, response.Error, "SendGrid Bulk Upload Failed")
	})

	t.Run("a mixture reaches reconciliation carrying every import's own state", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "failed", 2, ""), nil)
		api.EXPECT().GetImportStatus("sg-c").Times(1).Return(pollStatus("sg-c", "errored", 1, testErrorsURL), nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4;sg-c=5-6", ImportCount: 6})

		// Not the terminal 400: only ONE import of three was rejected, and aborting the batch
		// would discard the other two imports' delivered contacts.
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.True(t, response.Complete)
		require.True(t, response.HasFailed)

		// Every import is described, including the clean one, because reconciliation can only
		// clear its jobs if it is told they belong to an import that finished cleanly.
		require.Contains(t, response.FailedJobParameters, "sg-a")
		require.Contains(t, response.FailedJobParameters, "sg-b")
		require.Contains(t, response.FailedJobParameters, "sg-c")
		require.Contains(t, response.FailedJobParameters, testErrorsURL,
			"the URL SendGrid published travels verbatim, in memory, to reconciliation")

		// The operator-facing reason names the imports without their document paths.
		require.Contains(t, response.Error, "SendGrid Bulk Upload partially failed")
		require.NotContains(t, response.Error, "/imports/errors/abc123",
			"the provider-controlled path must never reach a logged or persisted string")
	})
}

// outcomeDocument renders what Poll hands to GetUploadStats for one import, so that the
// reconciliation cases start from exactly the value the previous call produces rather than from a
// shape invented for the test.
func outcomeDocument(t *testing.T, imports ...map[string]any) string {
	t.Helper()
	rendered, err := jsonrs.Marshal(map[string]any{"imports": imports})
	require.NoError(t, err)
	return string(rendered)
}

// importedFrom describes one import inside an outcome document.
//
// Membership is rendered as the comma-separated list the manifest's own encoding accepts, rather than
// as a JSON array: the field is a range-collapsed STRING, and writing it in the shape the connector
// actually reads is what keeps these cases honest about the wire format instead of about a convenient
// re-interpretation of it. Passing no job IDs leaves the field absent, which is how an import whose
// membership is unknown is expressed.
func importedFrom(importID, status, errorsURL string, jobIDs ...int64) map[string]any {
	described := map[string]any{"id": importID, "status": status}
	if errorsURL != "" {
		described["errorsUrl"] = errorsURL
	}
	if len(jobIDs) > 0 {
		rendered := make([]string, 0, len(jobIDs))
		for _, jobID := range jobIDs {
			rendered = append(rendered, strconv.FormatInt(jobID, 10))
		}
		described["jobs"] = strings.Join(rendered, ",")
	}
	return described
}

// TestGetUploadStatsPartialFailure is scenario S2, the second awkward case: an import that finished
// with SOME errors must yield failed AND succeeded jobs from one reconciliation, keyed on nothing
// but the importing jobs themselves and the document SendGrid published.
//
// The committed errors fixture names three contacts - two that the staged fixture carries and one it
// does not - which is what lets the matched rows, the succeeded remainder and the unmatched-row
// accounting all be asserted from a single realistic document.
func TestGetUploadStatsPartialFailure(t *testing.T) {
	t.Parallel()

	// Both spellings of the same situation. SendGrid signals a partial failure with errored, and
	// completed is documented to carry no errors at all - but a completed import that nonetheless
	// reports errored rows has to reconcile too, or its rejected contacts would be reported as
	// delivered. Poll normalizes the second onto the first, so reconciliation sees one state.
	for name, status := range map[string]string{
		"an errored import":                   "errored",
		"a completed import reporting errors": "errored",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

			response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: outcomeDocument(t,
					importedFrom("sg-1", status, testErrorsURL, 1, 2, 3, 4, 5)),
				ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
			})

			// 200 is mandatory: any other status makes the batch router discard the whole
			// reconciliation and write no status at all.
			require.Equal(t, http.StatusOK, response.StatusCode)

			require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
			require.Contains(t, response.Metadata.FailedReasons[2], "Invalid email address provided for contact")
			require.Contains(t, response.Metadata.FailedReasons[4], "custom field value exceeds the maximum allowed length")

			// The exact remainder, succeeded by exclusion. THE headline guarantee: one import
			// yields both a populated FailedKeys and a populated SucceededKeys.
			require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)

			// The retryable channel, never the terminal one: a per-row error is recoverable.
			require.Empty(t, response.Metadata.AbortedKeys)
			require.Empty(t, response.Metadata.AbortedReasons)
			require.Empty(t, response.Metadata.WarningKeys)
			require.Empty(t, response.Metadata.WarningReasons)

			// Disjoint, and complete: every importing job left with exactly one status.
			assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
		})
	}

	t.Run("a document carrying a row nothing can be attributed to fails the import closed", func(t *testing.T) {
		t.Parallel()

		// The committed fixture, which deliberately carries a third row for ghost@example.com - an
		// address no staged job used. That row is proof SendGrid rejected a contact of this import
		// without saying which, so the import's evidence is INCOMPLETE and exclusion stops being
		// sound: a remaining job might be the one the ghost row was about.
		//
		// Every job is therefore retried rather than three of them being reported delivered, and
		// the two jobs the document DID name keep their own specific provider reasons - the
		// general fail-closed reason never overwrites a specific one.
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(errorsDocument(t), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3, 4, 5)),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode,
			"the batch router writes no status at all for a non-200, so failing closed must still answer 200")
		require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys)
		require.Empty(t, response.Metadata.SucceededKeys,
			"nothing may be reported delivered once the import's evidence is known to be incomplete")

		require.Contains(t, response.Metadata.FailedReasons[2], "Invalid email address provided for contact")
		require.Contains(t, response.Metadata.FailedReasons[4], "custom field value exceeds the maximum allowed length")
		for _, jobID := range []int64{1, 3, 5} {
			require.Contains(t, response.Metadata.FailedReasons[jobID], "could not attribute",
				"a job the document never named is retried with the connector's own reason")
		}

		// Retryable, never terminal: the framework decides when to give up, and a re-upsert is
		// idempotent.
		require.Empty(t, response.Metadata.AbortedKeys)
		require.Empty(t, response.Metadata.WarningKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})
}

// assertSettledExactlyOnce is the completeness guarantee the batch router depends on. A job in the
// importing list that appears in none of the key sets receives NO status update at all and stays
// importing forever, and a job in two sets receives two conflicting ones.
func assertSettledExactlyOnce(t *testing.T, metadata common.EventStatMeta, importingJobIDs []int64) {
	t.Helper()

	settledCount := make(map[int64]int, len(importingJobIDs))
	for _, keys := range [][]int64{
		metadata.FailedKeys, metadata.AbortedKeys, metadata.SucceededKeys, metadata.WarningKeys,
	} {
		for _, jobID := range keys {
			settledCount[jobID]++
		}
	}
	for _, jobID := range importingJobIDs {
		require.Equal(t, 1, settledCount[jobID],
			"job %d must be settled exactly once, was settled %d times", jobID, settledCount[jobID])
	}
	require.Len(t, settledCount, len(importingJobIDs), "no job outside the importing list may be settled")
}

// TestGetUploadStatsPerImportReconciliation is the case per-import membership was built for: one
// upload, three imports, three different fates. Pooling them would be wrong in both directions -
// aborting the batch discards two imports' delivered contacts, clearing it by exclusion silently
// delivers the rejected import's.
func TestGetUploadStatsPerImportReconciliation(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(errorsDocument(t), nil)

	response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: outcomeDocument(t,
			// Clean: its jobs are cleared on the strength of the import's own state.
			importedFrom("sg-clean", "completed", "", 1, 3),
			// Rejected outright: SendGrid defines this as finished with all errors or entirely
			// unprocessable, so its jobs are the ONLY ones this connector abandons terminally.
			importedFrom("sg-rejected", "failed", "", 5),
			// Individually errored: its rows are failed, its remainder cleared.
			importedFrom("sg-errored", "errored", testErrorsURL, 2, 4),
		),
		ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
	})

	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, []int64{5}, response.Metadata.AbortedKeys)
	require.Contains(t, response.Metadata.AbortedReasons[5], "entire import")
	require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
	require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys)
	require.Empty(t, response.Metadata.WarningKeys)
	assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
}

// TestGetUploadStatsUnattributableRowsFailTheImportClosed is the regression guard for the most
// consequential decision in this connector: what an errors-document row that names nobody means.
//
// It means SendGrid rejected a contact of this import and did not say which one. There are exactly
// two readings of that, and only one of them is safe:
//
//   - treat it as evidence about the DOCUMENT only, count it, and settle the jobs it never named on
//     their own terms. Every remaining job is then reported DELIVERED - including, necessarily, the
//     one the row was actually about. That is silent, permanent data loss, and it is invisible
//     afterwards because the job's status says succeeded.
//   - treat it as evidence about the IMPORT, and retry every contact of that import whose delivery
//     was not confirmed. The cost is one idempotent re-upsert per contact, SendGrid upserts, and the
//     batch router escalates on its own if the retries keep failing.
//
// The second is what this connector does. The blast radius is the affected import rather than the
// whole upload, so a sibling import with complete evidence still reconciles normally - which is what
// keeps a single reconciliation able to report both failures and successes.
func TestGetUploadStatsUnattributableRowsFailTheImportClosed(t *testing.T) {
	t.Parallel()

	for name, document := range map[string]string{
		"an identifier that belongs to no importing job": `{"errors":[{"email":"stranger@example.com","message":"invalid email"}]}`,
		"an identifier spelled identifier":               `[{"identifier":"nobody@example.com","message":"invalid email"}]`,
		"a row carrying a message and no identifier":     `{"errors":[{"message":"something went wrong"}]}`,
		"a readable row alongside an unreadable line":    "{\"email\":\"stranger@example.com\",\"message\":\"invalid email\"}\nnot json at all\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(document), nil)

			response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: outcomeDocument(t,
					importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3, 4, 5)),
				ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
			})

			require.Equal(t, http.StatusOK, response.StatusCode,
				"failing closed must still answer 200, or the batch router writes no status at all")
			require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys,
				"a row that names nobody makes exclusion unsound for the whole import")
			require.Empty(t, response.Metadata.SucceededKeys,
				"no contact may be reported delivered while a rejection of unknown subject stands")
			require.Empty(t, response.Metadata.AbortedKeys,
				"retryable, never terminal: the framework decides when to give up")
			for _, jobID := range []int64{1, 2, 3, 4, 5} {
				require.Contains(t, response.Metadata.FailedReasons[jobID], "could not attribute")
			}
			assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
		})
	}

	t.Run("only the affected import is failed closed", func(t *testing.T) {
		t.Parallel()

		// The narrowing that per-import membership exists for. One import's document names nobody;
		// the other's names job 4 and nothing else. The first import's contacts are all retried,
		// the second's are settled precisely - job 4 failed with its own reason, job 5 succeeded by
		// exclusion - so ONE reconciliation still reports both outcomes.
		secondErrorsURL := testErrorsURL + "-2"

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`{"errors":[{"email":"stranger@example.com","message":"invalid email"}]}`), nil)
		api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).
			Return([]byte(`{"errors":[{"email":"devon@example.com","message":"rejected outright"}]}`), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-unreadable", "errored", testErrorsURL, 1, 2, 3),
				importedFrom("sg-readable", "errored", secondErrorsURL, 4, 5)),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2, 3, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{5}, response.Metadata.SucceededKeys,
			"a sibling import with complete evidence must still reconcile by exclusion")
		require.Contains(t, response.Metadata.FailedReasons[4], "rejected outright",
			"the attributed row keeps its own provider reason")
		for _, jobID := range []int64{1, 2, 3} {
			require.Contains(t, response.Metadata.FailedReasons[jobID], "could not attribute")
		}
		require.Empty(t, response.Metadata.AbortedKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("a shared document that names nobody fails every import that published it", func(t *testing.T) {
		t.Parallel()

		// One URL, two imports, and the document is read ONCE - so what it could not explain has to
		// be attributed back to both publishers rather than to whichever import happened to be
		// visited first. Getting this wrong would leave the second import's contacts reported as
		// delivered on evidence that was already known to be incomplete.
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`{"errors":[{"email":"stranger@example.com","message":"invalid email"}]}`), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-a", "errored", testErrorsURL, 1, 2),
				importedFrom("sg-b", "errored", testErrorsURL, 3, 4, 5)),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys,
			"both publishers of the shared document must fail closed")
		require.Empty(t, response.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("a clean import of the same upload is untouched", func(t *testing.T) {
		t.Parallel()

		// The narrowing at its most consequential: SendGrid stated that sg-clean finished with no
		// errors whatsoever, which is direct evidence about every contact in it. An unreadable row
		// in a SIBLING import's document says nothing about those contacts, so re-uploading them
		// would be pure churn.
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`{"errors":[{"email":"stranger@example.com","message":"invalid email"}]}`), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-clean", "completed", "", 1, 2),
				importedFrom("sg-errored", "errored", testErrorsURL, 3, 4, 5)),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{3, 4, 5}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 2}, response.Metadata.SucceededKeys,
			"a completed import's contacts are delivered on the provider's own word")
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})
}

// TestGetUploadStatsTolerantParser pins every document shape the parser accepts, because the shape
// SendGrid actually serves is undocumented. Each of these must resolve the same contact to the same
// job, so a change of shape at the provider degrades to a counted metric rather than to churn.
func TestGetUploadStatsTolerantParser(t *testing.T) {
	t.Parallel()

	for name, document := range map[string]string{
		"a bare array": `[{"email":"blake@example.com","message":"invalid phone"}]`,
		"an object wrapping the rows under errors":  `{"errors":[{"email":"blake@example.com","message":"invalid phone"}]}`,
		"an object wrapping the rows under results": `{"results":[{"email":"blake@example.com","message":"invalid phone"}]}`,
		"newline delimited rows":                    "{\"email\":\"blake@example.com\",\"message\":\"invalid phone\"}\n",
		"an identifier nested under contact":        `[{"contact":{"email":"blake@example.com"},"message":"invalid phone"}]`,
		"an identifier SendGrid up-cased":           `[{"email":"BLAKE@EXAMPLE.COM","message":"invalid phone"}]`,
		"an external identifier":                    `[{"external_id":"user_223","message":"invalid phone"}]`,
		"an anonymous identifier":                   `[{"anonymous_id":"anon_556","message":"invalid phone"}]`,
		"a message spelled reason":                  `[{"email":"blake@example.com","reason":"invalid phone"}]`,
		"a message spelled detail":                  `[{"email":"blake@example.com","detail":"invalid phone"}]`,
		"a message spelled error_message":           `[{"email":"blake@example.com","error_message":"invalid phone"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(document), nil)

			response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: outcomeDocument(t,
					importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3, 4, 5)),
				ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
			})

			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, []int64{2}, response.Metadata.FailedKeys,
				"every accepted shape must resolve to the same job")
			require.NotEmpty(t, response.Metadata.FailedReasons[2])
			require.ElementsMatch(t, []int64{1, 3, 4, 5}, response.Metadata.SucceededKeys)
			assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
		})
	}
}

// TestGetUploadStatsRetriesWhenNoEvidenceIsAvailable covers every way the evidence can be missing.
// A non-200 is reserved for the cases in which NO reconciliation whatsoever is possible, because the
// batch router discards the result and retries the whole import; anything narrower is scoped to the
// jobs it is actually missing evidence about and stays retryable.
func TestGetUploadStatsRetriesWhenNoEvidenceIsAvailable(t *testing.T) {
	t.Parallel()

	t.Run("an unparseable errors document fails the whole reconciliation", func(t *testing.T) {
		t.Parallel()

		// The last four are the load-bearing ones. A document the parser reduces to NO usable row
		// is reported as an error rather than as an empty result, because an empty result would be
		// reconciled as "nothing failed" and would mark every contact in an import SendGrid said
		// had errors as delivered. That is the single worst outcome this connector can produce, so
		// "I read the document and it told me nothing" has to be retried, not believed.
		for name, document := range map[string][]byte{
			"bytes that are not json at all":               []byte("<html><body>gateway timeout</body></html>"),
			"a truncated document":                         []byte(`{"errors":[{"email":"blake@`),
			"a scalar":                                     []byte(`42`),
			"an empty body":                                {},
			"an empty wrapped array":                       []byte(`{"errors":[]}`),
			"an empty bare array":                          []byte(`[]`),
			"rows carrying neither identifier nor message": []byte(`{"errors":[{"error_indices":[7]}]}`),
			"an object that is not a row at all":           []byte(`{"status":"ok"}`),
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				api := newMockAPI(t)
				api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(document, nil)

				response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: outcomeDocument(t,
						importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3)),
					ImportingList: fixtureJobs(t, 1, 2, 3),
				})

				// 500, emphatically not 200 with an empty failed set: the latter would mark every
				// job in the import delivered on the strength of a document nobody could read.
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				require.NotEmpty(t, response.Error)
				require.Empty(t, response.Metadata.SucceededKeys)
			})
		}
	})

	t.Run("an unfetchable errors document fails the whole reconciliation", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(nil, errors.New("errors document host is not allowed"))

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3)),
			ImportingList: fixtureJobs(t, 1, 2, 3),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "errors document")
		require.Empty(t, response.Metadata.SucceededKeys)
	})

	t.Run("an errored import that published no document retries only its own jobs", func(t *testing.T) {
		t.Parallel()

		response := newUploader(newMockAPI(t)).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-clean", "completed", "", 1, 3),
				importedFrom("sg-silent", "errored", "", 2, 4, 5),
			),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4, 5}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[2], "without publishing")
		require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys,
			"the clean import's jobs are cleared on the strength of its own state")
		require.Empty(t, response.Metadata.AbortedKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("an import that lost contacts with no recoverable membership retries every unresolved job", func(t *testing.T) {
		t.Parallel()

		// No jobs listed on either import, which is what the manifest degrading past its size
		// budget leaves behind. Any remaining job might be one the rejected import lost, so none
		// of them may be succeeded by exclusion - and none of them may be aborted either.
		response := newUploader(newMockAPI(t)).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-a", "completed", ""),
				importedFrom("sg-b", "failed", ""),
			),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[1], "could not be identified")
		require.Empty(t, response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.AbortedKeys,
			"an abort would terminally discard contacts that were very probably delivered")
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("an importing job whose own identifier cannot be re-derived", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`{"errors":[{"email":"blake@example.com","message":"invalid phone"}]}`), nil)

		jobs := fixtureJobs(t, 1, 2, 3)
		// Job 3's payload no longer yields any contact identifier, so it can be shown neither to
		// be named by the document nor to be absent from it.
		jobs[2].EventPayload = []byte(`{"message":{"type":"track","event":"Anonymous"},"metadata":{"job_id":3}}`)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t, importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3)),
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 3}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[3], "identifier")
		require.Equal(t, []int64{1}, response.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
	})

	t.Run("a job confirmed by its own completed import needs no identifier", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`{"errors":[{"email":"blake@example.com","message":"invalid phone"}]}`), nil)

		jobs := fixtureJobs(t, 1, 2, 3)
		jobs[2].EventPayload = []byte(`{"message":{"type":"track","event":"Anonymous"},"metadata":{"job_id":3}}`)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-errored", "errored", testErrorsURL, 1, 2),
				importedFrom("sg-clean", "completed", "", 3),
			),
			ImportingList: jobs,
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys,
			"the provider stated the import finished with no errors, which is evidence about job 3")
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
	})
}

// TestGetUploadStatsWithoutForwardedOutcomes covers the fallbacks. Reconciliation may run in a
// different process invocation from the Upload and the Poll that preceded it, so it must be able to
// rebuild everything it needs from the persisted parameters alone.
func TestGetUploadStatsWithoutForwardedOutcomes(t *testing.T) {
	t.Parallel()

	t.Run("rebuilt from the persisted manifest by re-reading the import", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(pollStatus("sg-1", "errored", 1, testErrorsURL), nil)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    persistedParameters(t, "sg-1=1-5", 5),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("the defensive reading of a completed import survives the re-read", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		// completed promises no errors, so a non-zero errored count means the import behaves as
		// errored whatever SendGrid calls it. Reading this wrongly would report rejected contacts
		// as delivered, which is the worst outcome this connector can produce.
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(pollStatus("sg-1", "completed", 2, testErrorsURL), nil)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    persistedParameters(t, "sg-1=1-5", 5),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
	})

	t.Run("a rejected import recovered from the manifest aborts exactly its own jobs", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "failed", 2, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "completed", 0, ""), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    persistedParameters(t, "sg-a=1-2;sg-b=3-5", 5),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2}, response.Metadata.AbortedKeys)
		require.ElementsMatch(t, []int64{3, 4, 5}, response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.FailedKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("a bare errors url list is read but never treated as a rejection", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.AbortedKeys, "an unknown state must never abort a job")
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("a status re-read that fails is retried whole", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(nil, errors.New("connection reset"))

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    persistedParameters(t, "sg-1=1-3", 3),
			ImportingList: fixtureJobs(t, 1, 2, 3),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "connection reset")
	})

	t.Run("no import id anywhere is retried whole", func(t *testing.T) {
		t.Parallel()

		for name, input := range map[string]common.GetUploadStatsInput{
			"no parameters at all":            {},
			"parameters holding no import id": {Parameters: []byte(`{"importCount":3}`)},
			"unparseable parameters":          {Parameters: []byte(`{"importId":`)},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				input.ImportingList = fixtureJobs(t, 1, 2, 3)
				response := newUploader(newMockAPI(t)).GetUploadStats(input)

				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				require.NotEmpty(t, response.Error)
				require.Empty(t, response.Metadata.SucceededKeys)
			})
		}
	})
}

// TestGetUploadStatsEdgeCases guards the reconciliation against the inputs a shared batch router
// worker can genuinely hand it, none of which may panic or strand a job.
func TestGetUploadStatsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("an empty importing list", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(errorsDocument(t), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t, importedFrom("sg-1", "errored", testErrorsURL)),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Empty(t, response.Metadata.FailedKeys)
		require.Empty(t, response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.AbortedKeys)
		// Allocated rather than nil, so the batch router's own iteration is safe.
		require.NotNil(t, response.Metadata.FailedReasons)
		require.NotNil(t, response.Metadata.AbortedReasons)
	})

	t.Run("a nil entry in the importing list", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		// A single row naming job 2 and nothing else, so this case is about the nil entry rather
		// than about the fail-closed policy an unattributable row would trigger.
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`[{"email":"blake@example.com","message":"invalid email"}]`), nil)

		jobs := fixtureJobs(t, 1, 2, 3)
		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t, importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3)),
			ImportingList:       append([]*jobsdb.JobT{nil}, jobs...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys)
	})

	t.Run("an ambiguous identifier fails every job that claimed it", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`[{"email":"shared@example.com","message":"invalid domain"}]`), nil)

		// Two staged events carrying one address: SendGrid upserts them onto a single contact, so
		// the errored row genuinely refers to both and neither may be cleared by exclusion.
		shared := `{"type":"identify","userId":"user_%d","traits":{"email":"shared@example.com"}}`
		jobs := []*jobsdb.JobT{
			{JobID: 1, EventPayload: []byte(stagingLine(t, 1, fmt.Sprintf(shared, 1)))},
			{JobID: 2, EventPayload: []byte(stagingLine(t, 2, fmt.Sprintf(shared, 2)))},
			{JobID: 3, EventPayload: []byte(contactLine(t, 3))},
		}

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t, importedFrom("sg-1", "errored", testErrorsURL, 1, 2, 3)),
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{1, 2}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[1], "matches 2 jobs")
		require.Equal(t, []int64{3}, response.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
	})

	t.Run("two imports publishing one shared document read it once", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		// Times(1), not Times(2): re-fetching the same URL per import would double the rows and
		// double the provider traffic.
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-a", "errored", testErrorsURL, 1, 2),
				importedFrom("sg-b", "errored", testErrorsURL, 3, 4, 5),
			),
			ImportingList: fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
	})

	t.Run("a row naming a job whose whole import was rejected never downgrades the abort", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		// The same address appears in the errored import's document while belonging to the
		// rejected import as well. The terminal verdict must stand, or the job would land in both
		// FailedKeys and AbortedKeys and the router would write two conflicting statuses for it.
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`[{"email":"blake@example.com","message":"invalid phone"}]`), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: outcomeDocument(t,
				importedFrom("sg-rejected", "failed", "", 2),
				importedFrom("sg-errored", "errored", testErrorsURL, 1, 3),
			),
			ImportingList: fixtureJobs(t, 1, 2, 3),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.AbortedKeys)
		require.Empty(t, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
	})
}

// supportedDistinctListIDs mirrors the connector's unexported maxListIDsPerContact: how many
// DISTINCT SendGrid lists one event's context.externalId targeting may name.
//
// Mirrored rather than exported, for the same reason persistedManifestBudget is: the bound is an
// internal invariant, and widening the package's API so a test could read it would be the test
// changing what it observes. The cases below pin it from BOTH sides - the largest targeting that is
// honored in full, and the smallest that is refused - so a change to the connector's bound that is
// not reflected here fails one of them rather than passing vacuously.
const supportedDistinctListIDs = 64

// distinctListIDs builds count list IDs that are all different from one another.
func distinctListIDs(count int) []string {
	listIDs := make([]string, 0, count)
	for index := 0; index < count; index++ {
		listIDs = append(listIDs, fmt.Sprintf("list-%04d", index))
	}
	return listIDs
}

// listTargetingLine renders one staged identify event whose context.externalId targets exactly the
// given lists, in the given order, through a single listIds entry.
func listTargetingLine(t *testing.T, jobID int64, listIDs ...string) string {
	t.Helper()

	quoted := make([]string, 0, len(listIDs))
	for _, listID := range listIDs {
		quoted = append(quoted, strconv.Quote(listID))
	}
	return stagingLine(t, jobID, fmt.Sprintf(
		`{"type":"identify","userId":"user_%d","traits":{"email":"user%d@example.com"},`+
			`"context":{"externalId":[{"type":"listIds","id":[%s]}]}}`,
		jobID, jobID, strings.Join(quoted, ",")))
}

// TestUploadListIDResolution pins the precedence this repository already documents for SendGrid: a
// per-event context.externalId entry of type listIds wins, and the destination configuration is the
// fallback. Contacts targeting different lists cannot share a request body, so the resolution also
// decides how the batch is grouped.
//
// Per-event targeting is the highest-precedence list request there is, which is why the cases below
// go further than "the override is used" and pin exactly WHICH lists reach the wire: a resolution
// that quietly delivered a contact to some of the lists an event named, and none of the rest, would
// satisfy every assertion about precedence while getting the delivery wrong.
func TestUploadListIDResolution(t *testing.T) {
	t.Parallel()

	t.Run("a per-event entry overrides the destination configuration", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Equal(t, []string{"per-event-list"}, request.ListIDs)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t, stagingLine(t, 1,
				`{"type":"identify","userId":"user_1","traits":{"email":"one@example.com"},`+
					`"context":{"externalId":[{"type":"listIds","id":["per-event-list"]}]}}`)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
	})

	t.Run("a single string id is accepted as well as an array", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Equal(t, []string{"scalar-list"}, request.ListIDs)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t, stagingLine(t, 1,
				`{"type":"identify","userId":"user_1","traits":{"email":"one@example.com"},`+
					`"context":{"externalId":[{"type":"LISTIDS","id":"scalar-list"}]}}`)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
	})

	t.Run("contacts targeting different lists travel in separate requests", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		seen := make(map[string][]string)
		api.EXPECT().UploadContacts(gomock.Any()).Times(2).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 1, "a request may carry only one list's contacts")
				seen[request.Contacts[0].Email] = request.ListIDs
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-" + strconv.Itoa(len(seen))}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_1","traits":{"email":"one@example.com"},`+
					`"context":{"externalId":[{"type":"listIds","id":["list-a"]}]}}`),
				stagingLine(t, 2, `{"type":"identify","userId":"user_2","traits":{"email":"two@example.com"},`+
					`"context":{"externalId":[{"type":"listIds","id":["list-b"]}]}}`)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, map[string][]string{
			"one@example.com": {"list-a"},
			"two@example.com": {"list-b"},
		}, seen)
		require.ElementsMatch(t, []int64{1, 2}, output.ImportingJobIDs)
	})

	t.Run("an upload with no list ids anywhere still upserts the contacts", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				// Optional by contract: an upsert with no list IDs creates or updates the contacts,
				// it simply associates them with no list.
				require.Empty(t, request.ListIDs)
				require.Len(t, request.Contacts, 1)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		uploader := newUploader(api)
		uploader.DestinationConfig.ListIDs = nil
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
	})

	t.Run("every distinct list up to the bound reaches the request", func(t *testing.T) {
		t.Parallel()

		// The accepting side of the bound, one below it and exactly at it, asserted on the request
		// body itself: the whole targeting has to travel, in the order the event named it, because
		// the list_ids array is what decides where the contact ends up.
		for name, distinctCount := range map[string]int{
			"one below the bound":  supportedDistinctListIDs - 1,
			"exactly at the bound": supportedDistinctListIDs,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				targeted := distinctListIDs(distinctCount)

				api := newMockAPI(t)
				api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
					func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						require.Equal(t, targeted, request.ListIDs,
							"all %d distinct lists must reach the request, in the order the event named them", distinctCount)
						return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
					})

				output := newUploader(api).Upload(&common.AsyncDestinationStruct{
					FileName:        writeStagingFile(t, listTargetingLine(t, 1, targeted...)),
					ImportingJobIDs: []int64{1},
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				require.Equal(t, []int64{1}, output.ImportingJobIDs)
				require.Empty(t, output.AbortJobIDs)
				require.Empty(t, output.FailedJobIDs)
			})
		}
	})

	t.Run("one distinct list past the bound is reported rather than truncated", func(t *testing.T) {
		t.Parallel()

		// NO upload expectation is registered on purpose. The only event in this batch cannot be
		// targeted as it asked, so nothing may go on the wire at all, and gomock fails the case if a
		// request is issued. A truncating implementation would upsert this contact onto the first
		// supportedDistinctListIDs lists, omit the rest, and report success - a wrong delivery
		// presented as a right one, which is precisely what must not happen.
		api := newMockAPI(t)

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				listTargetingLine(t, 1, distinctListIDs(supportedDistinctListIDs+1)...)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		// Terminal, like every other local rejection in this connector: the same event names the
		// same lists on every retry, so retrying could only fail again.
		require.Equal(t, []int64{1}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "more distinct sendgrid lists",
			"the durable reason must name the actual cause, so the fix is contained in the failure")
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("repeated list ids cannot displace a distinct one", func(t *testing.T) {
		t.Parallel()

		// THE REGRESSION CASE. The bound is charged against DISTINCT lists, so an event may repeat
		// one list as often as it likes and the genuinely different list that follows still reaches
		// the request. Charging the bound against RAW values instead - counting duplicates, then
		// de-duplicating afterwards - stopped collecting among the repeats, normalized them down to
		// one list, and upserted the contact onto that single list while silently dropping the
		// target the event had actually added.
		repeated := make([]string, 0, supportedDistinctListIDs+1)
		for index := 0; index < supportedDistinctListIDs; index++ {
			repeated = append(repeated, "list-a")
		}
		repeated = append(repeated, "list-b")

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Equal(t, []string{"list-a", "list-b"}, request.ListIDs,
					"a repeated list must cost the bound nothing, so the distinct list after it survives")
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, listTargetingLine(t, 1, repeated...)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("duplicates across entries and shapes collapse in first appearance order", func(t *testing.T) {
		t.Parallel()

		// Several listIds entries, an array id and a scalar id, a blank member, a non-scalar member,
		// an unrelated externalId type, and one list named three times across two entries. What
		// reaches the wire is each distinct list exactly once, in the order the event first named it.
		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Equal(t, []string{"list-b", "list-a", "list-c"}, request.ListIDs)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t, stagingLine(t, 1,
				`{"type":"identify","userId":"user_1","traits":{"email":"one@example.com"},`+
					`"context":{"externalId":[`+
					`{"type":"listIds","id":["list-b","  list-a  ","list-b","",{"nested":"x"}]},`+
					`{"type":"userId","id":"ignored-by-type"},`+
					`{"type":"LISTIDS","id":"list-c"},`+
					`{"type":"listIds","id":"list-a"}]}}`)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("an over targeted event does not take its siblings down with it", func(t *testing.T) {
		t.Parallel()

		// The rejection is scoped to the record that carries it, exactly like an identifier-less
		// contact or an over-long field: the sibling event, which targets a list normally, is still
		// upserted in the same upload.
		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 1)
				require.Equal(t, "one@example.com", request.Contacts[0].Email)
				require.Equal(t, []string{"list-a"}, request.ListIDs)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				stagingLine(t, 1, `{"type":"identify","userId":"user_1","traits":{"email":"one@example.com"},`+
					`"context":{"externalId":[{"type":"listIds","id":["list-a"]}]}}`),
				listTargetingLine(t, 2, distinctListIDs(supportedDistinctListIDs+1)...)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Equal(t, []int64{2}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "more distinct sendgrid lists")
		require.Empty(t, output.FailedJobIDs)
	})
}

// TestFullLifecycle drives the three methods in the order the batch router drives them, feeding each
// call NOTHING but what the previous one produced. It is the only place the wire formats between the
// calls are proven to agree rather than asserted twice against the same assumption: Upload's persisted
// manifest is what Poll is given, and Poll's forwarded outcomes are what reconciliation is given.
func TestFullLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("one import, partially errored", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(pollStatus("sg-1", "errored", 2, testErrorsURL), nil)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(attributableErrorsDocument(), nil)

		uploader := newUploader(api)
		jobs := fixtureJobs(t, 1, 2, 3, 4, 5)

		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        stagingFixture,
			ImportingJobIDs: []int64{1, 2, 3, 4, 5},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})
		require.Equal(t, []int64{1, 2, 3, 4, 5}, output.ImportingJobIDs)

		// Exactly the round trip the batch router performs: it persists ImportingParameters against
		// every importing job and later reads importId and importCount back out with gjson.
		importID, importCount := importParametersOf(t, output)
		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: int(importCount)})
		require.Equal(t, http.StatusOK, pollResponse.StatusCode)
		require.True(t, pollResponse.Complete)
		require.True(t, pollResponse.HasFailed)

		statsResponse := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: pollResponse.FailedJobParameters,
			Parameters:          persistedParameters(t, importID, int(importCount)),
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusOK, statsResponse.StatusCode)
		require.ElementsMatch(t, []int64{2, 4}, statsResponse.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 5}, statsResponse.Metadata.SucceededKeys)
		require.Empty(t, statsResponse.Metadata.AbortedKeys)
		assertSettledExactlyOnce(t, statsResponse.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("several imports, one rejected and one errored", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		gomock.InOrder(
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-a"}, nil),
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-b"}, nil),
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-c"}, nil),
		)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "failed", 2, ""), nil)
		api.EXPECT().GetImportStatus("sg-c").Times(1).Return(pollStatus("sg-c", "errored", 1, testErrorsURL), nil)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
			[]byte(`[{"email":"erin@example.com","message":"invalid postal code"}]`), nil)

		uploader := newUploader(api)
		// Two contacts per request over the five-record fixture: sg-a carries jobs 1 and 2, sg-b
		// carries 3 and 4, sg-c carries 5.
		uploader.MaxContactsPerRequest = 2

		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        stagingFixture,
			ImportingJobIDs: []int64{1, 2, 3, 4, 5},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})
		require.Equal(t, []int64{1, 2, 3, 4, 5}, output.ImportingJobIDs)

		importID, importCount := importParametersOf(t, output)
		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: int(importCount)})

		// One import of three was rejected, so the batch is NOT terminated wholesale: doing so
		// would discard the two imports whose contacts SendGrid took.
		require.Equal(t, http.StatusOK, pollResponse.StatusCode)
		require.True(t, pollResponse.HasFailed)

		statsResponse := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: pollResponse.FailedJobParameters,
			Parameters:          persistedParameters(t, importID, int(importCount)),
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, statsResponse.StatusCode)
		// The rejected import's jobs, and only those, are abandoned terminally.
		require.ElementsMatch(t, []int64{3, 4}, statsResponse.Metadata.AbortedKeys)
		require.Contains(t, statsResponse.Metadata.AbortedReasons[3], "entire import")
		// The errored import's named row is retried.
		require.Equal(t, []int64{5}, statsResponse.Metadata.FailedKeys)
		// The clean import's contacts are delivered.
		require.ElementsMatch(t, []int64{1, 2}, statsResponse.Metadata.SucceededKeys)
		assertSettledExactlyOnce(t, statsResponse.Metadata, []int64{1, 2, 3, 4, 5})
	})

	t.Run("every import clean", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)
		api.EXPECT().GetImportStatus("sg-1").Times(1).Return(pollStatus("sg-1", "completed", 0, ""), nil)

		uploader := newUploader(api)
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        stagingFixture,
			ImportingJobIDs: []int64{1, 2, 3, 4, 5},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		importID, importCount := importParametersOf(t, output)
		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: int(importCount)})

		// No reconciliation at all: the batch router marks every importing job succeeded wholesale
		// when Complete is set without HasFailed, so GetUploadStats is never even called.
		require.Equal(t, http.StatusOK, pollResponse.StatusCode)
		require.True(t, pollResponse.Complete)
		require.False(t, pollResponse.HasFailed)
		require.Empty(t, pollResponse.FailedJobParameters)
	})
}

// TestImportManifestPersistence covers the compact per-import membership Upload persists, through the
// only surface that can observe it: the parameters the batch router stores and hands back to Poll.
//
// The manifest has to stay SMALL. The batch router copies ImportingParameters into the status row of
// every importing job, so a membership rendered one job ID at a time would multiply into the job
// status table thousands of times over for a single upload.
// persistedManifestBudget mirrors the connector's unexported maxImportManifestBytes.
//
// Mirrored rather than exported: the bound is an internal invariant, and widening the package's API
// so a test can read it would be the test changing the thing it is meant to observe.
//
// It is asserted as an UPPER BOUND on what Upload actually persists, which is the direction that
// matters: if the connector's own ceiling were ever raised, or lost, the manifests measured below
// would grow past this literal and these cases would fail. If it is ever deliberately TIGHTENED,
// this literal must be lowered with it, or the assertion merely stops being the tightest statement
// available rather than becoming wrong.
const persistedManifestBudget = 1024

func TestImportManifestPersistence(t *testing.T) {
	t.Parallel()

	t.Run("contiguous membership collapses into ranges", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		gomock.InOrder(
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-a"}, nil),
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-b"}, nil),
		)

		lines := make([]string, 0, 6)
		jobIDs := make([]int64, 0, 6)
		for jobID := int64(1); jobID <= 6; jobID++ {
			lines = append(lines, contactLine(t, jobID))
			jobIDs = append(jobIDs, jobID)
		}

		uploader := newUploader(api)
		uploader.MaxContactsPerRequest = 3
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, lines...),
			ImportingJobIDs: jobIDs,
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		importID, importCount := importParametersOf(t, output)
		require.Equal(t, "sg-a=1-3;sg-b=4-6", importID)
		require.Equal(t, int64(6), importCount)
	})

	t.Run("two chunks answered with one import id are recorded once", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		// SendGrid is free to answer two requests with the same job_id, in which case the
		// membership of both chunks belongs to that one import.
		api.EXPECT().UploadContacts(gomock.Any()).Times(2).Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-same"}, nil)

		uploader := newUploader(api)
		uploader.MaxContactsPerRequest = 2
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				contactLine(t, 1), contactLine(t, 2), contactLine(t, 3), contactLine(t, 4)),
			ImportingJobIDs: []int64{1, 2, 3, 4},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		importID, importCount := importParametersOf(t, output)
		require.Equal(t, "sg-same=1-4", importID)
		require.Equal(t, int64(4), importCount)

		// One import, polled once, whatever the number of requests that produced it.
		api.EXPECT().GetImportStatus("sg-same").Times(1).Return(pollStatus("sg-same", "completed", 0, ""), nil)
		require.True(t, uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: 4}).Complete)
	})

	t.Run("the persisted manifest never exceeds its byte budget", func(t *testing.T) {
		t.Parallel()

		// The manifest's TWO degradation steps and its hard ceiling, in one case.
		//
		// Every job goes in its own chunk with a non-contiguous membership, repeated far past what
		// the budget can hold. Two things have to be true of the result, and only the first of them
		// used to be:
		//
		//  1. The identifiers survive and the membership is what is dropped. Without the
		//     identifiers the imports could never be polled at all.
		//  2. What is persisted fits maxImportManifestBytes. The batch router copies the import
		//     parameters into the status row of EVERY importing job, so the identifier-only
		//     fallback growing without bound - up to the import ceiling times the identifier
		//     length, far past the budget - multiplied straight into the job status table. The
		//     surplus imports are deferred instead, which is why (3) matters:
		//  3. Every job is accounted for exactly once, split between the imports that were
		//     recorded and the retryable jobs the next batch will carry. Truncating the manifest
		//     instead would have been silent data loss: an import nobody polls contributes nothing
		//     to reconciliation, so if the recorded imports all completed cleanly the batch router
		//     would mark its jobs delivered on evidence that never covered them.
		const jobCount = 400

		api := newMockAPI(t)
		uploadCount := 0
		// NOT Times(jobCount): once an accepted identifier no longer fits, the connector stops
		// sending, so the surplus chunks cost nothing at the provider. Exactly one chunk - the one
		// that discovered the budget was spent - is upserted and then deferred.
		api.EXPECT().UploadContacts(gomock.Any()).AnyTimes().DoAndReturn(
			func(sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				uploadCount++
				return &sendgridbulkupload.UpsertResponse{
					JobID: fmt.Sprintf("sendgrid-import-identifier-%04d", uploadCount),
				}, nil
			})

		lines := make([]string, 0, jobCount)
		jobIDs := make([]int64, 0, jobCount)
		for index := 0; index < jobCount; index++ {
			// Deliberately non-contiguous, so range collapsing cannot rescue the rendering.
			jobID := int64(index*7 + 1)
			lines = append(lines, contactLine(t, jobID))
			jobIDs = append(jobIDs, jobID)
		}

		uploader := newUploader(api)
		uploader.MaxContactsPerRequest = 1
		// The import budget is raised for this case only. Its default deliberately bounds an upload
		// to far fewer imports than this - which is what keeps a later poll's cost bounded - so
		// without the override the import ceiling, not the manifest budget, would be what bound,
		// and this case would stop testing the thing it exists to test.
		uploader.MaxImportsPerUpload = jobCount
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, lines...),
			ImportingJobIDs: jobIDs,
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		importID, importCount := importParametersOf(t, output)

		// (2) The ceiling, asserted against the value the batch router actually stores rather than
		// against the manifest alone, so the assertion cannot be satisfied by a manifest that fits
		// while the surrounding parameters do not.
		require.LessOrEqual(t, len(importID), persistedManifestBudget,
			"the manifest persisted against every importing job must fit its budget")
		require.LessOrEqual(t, len(output.ImportingParameters), persistedManifestBudget+128,
			"the persisted parameters must stay bounded, not just the manifest inside them")

		// (1) The membership degraded; the identifiers did not.
		require.NotContains(t, importID, "=", "the membership is what is dropped, not the identifiers")
		require.Contains(t, importID, "sendgrid-import-identifier-0001")

		// (3) A clean partition of all 400 jobs, with no job in both halves and none missing.
		require.NotEmpty(t, output.ImportingJobIDs)
		require.NotEmpty(t, output.FailedJobIDs, "the surplus must be deferred, never dropped")
		require.Empty(t, output.AbortJobIDs, "a full manifest is a retryable condition, never terminal")
		require.ElementsMatch(t, jobIDs, append(append([]int64{}, output.ImportingJobIDs...), output.FailedJobIDs...),
			"every job is either recorded against an import or deferred to the next batch")
		require.Equal(t, int64(len(output.ImportingJobIDs)), importCount)
		require.Contains(t, output.FailedReason, "import manifest")

		// Fewer imports were recorded than chunks were packed, and the surplus was not even sent.
		recordedImports := len(strings.Split(importID, ";"))
		require.Less(t, recordedImports, jobCount)
		require.Equal(t, recordedImports+1, uploadCount,
			"exactly one chunk is re-batched after being sent; the rest are never sent at all")

		// And the degraded value still polls: this is the whole reason the identifiers are kept.
		for index := 1; index <= recordedImports; index++ {
			identifier := fmt.Sprintf("sendgrid-import-identifier-%04d", index)
			api.EXPECT().GetImportStatus(identifier).Times(1).Return(pollStatus(identifier, "completed", 0, ""), nil)
		}
		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: int(importCount)})
		require.Equal(t, http.StatusOK, pollResponse.StatusCode)
		require.True(t, pollResponse.Complete)
		require.False(t, pollResponse.HasFailed)
	})

	t.Run("a legacy value carrying only identifiers is still understood", func(t *testing.T) {
		t.Parallel()

		// The colon-separated form this connector never writes but must keep reading, so that an
		// upload already in flight across a deployment is polled rather than stranded.
		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
		api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "completed", 0, ""), nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a:sg-b", ImportCount: 4})
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.True(t, response.Complete)
		require.False(t, response.HasFailed)
	})

	t.Run("a membership that cannot be read is treated as unknown, never as partial", func(t *testing.T) {
		t.Parallel()

		// A partial membership would be worse than none: the jobs the manifest failed to decode
		// would be attributed to no import and could then be cleared by exclusion even though the
		// import that carried them was rejected.
		for name, importID := range map[string]string{
			"a non-numeric element":     "sg-a=1,abc,3",
			"an inverted range":         "sg-a=5-1",
			"a zero job id":             "sg-a=0",
			"an implausibly wide range": "sg-a=1-999999999",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				api := newMockAPI(t)
				api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "failed", 2, ""), nil)

				// The import is still identified and still polled - only its membership is unknown.
				pollResponse := newUploader(api).Poll(common.AsyncPoll{ImportId: importID, ImportCount: 3})
				require.Equal(t, http.StatusBadRequest, pollResponse.StatusCode,
					"a single import, rejected outright, is terminal for the batch")

				// And with a second, surviving import in the picture, reconciliation retries rather
				// than guessing which jobs the rejected one held.
				statsResponse := newUploader(newMockAPI(t)).GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: outcomeDocument(t,
						map[string]any{"id": "sg-a", "status": "failed", "jobs": strings.SplitN(importID, "=", 2)[1]},
						importedFrom("sg-b", "completed", "", 3),
					),
					ImportingList: fixtureJobs(t, 1, 2, 3),
				})
				require.Equal(t, http.StatusOK, statsResponse.StatusCode)
				require.ElementsMatch(t, []int64{1, 2, 3}, statsResponse.Metadata.FailedKeys)
				require.Empty(t, statsResponse.Metadata.AbortedKeys)
				require.Empty(t, statsResponse.Metadata.SucceededKeys)
			})
		}
	})

	t.Run("a membership range ending at the largest int64 terminates", func(t *testing.T) {
		t.Parallel()

		// The manifest is read back out of persisted job parameters, so a corrupted or hostile value
		// is exactly the input this parser has to survive. A range whose end is math.MaxInt64 used to
		// hang the worker outright: the expansion incremented the terminal value, signed overflow
		// wrapped it to math.MinInt64, that is still <= end, and the loop appended forever. The
		// width guard does not catch a single-value range, because its width is zero.
		//
		// A timeout would prove it too, but only by hanging the suite for the whole timeout; this
		// asserts termination directly, and the completed reconciliation is the evidence.
		maxInt64 := strconv.FormatInt(math.MaxInt64, 10)

		for name, jobs := range map[string]string{
			"a single value at the maximum":    maxInt64,
			"a range collapsed at the maximum": maxInt64 + "-" + maxInt64,
			"a range ending at the maximum":    strconv.FormatInt(math.MaxInt64-2, 10) + "-" + maxInt64,
			"a range spanning to the maximum":  "1-" + maxInt64,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				response := newUploader(newMockAPI(t)).GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: outcomeDocument(t,
						map[string]any{"id": "sg-overflow", "status": "failed", "jobs": jobs},
						importedFrom("sg-clean", "completed", "", 1, 2, 3),
					),
					ImportingList: fixtureJobs(t, 1, 2, 3),
				})

				// Whatever the membership decodes to, the call RETURNS - and no staged job is left
				// without a status. A width that cannot be materialized is read as unknown
				// membership, which reconciliation already settles conservatively.
				require.Equal(t, http.StatusOK, response.StatusCode)
				assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
				require.Empty(t, response.Metadata.AbortedKeys,
					"no importing job belongs to the rejected import, so none may be abandoned")
			})
		}
	})
}

// TestTransformFeedsUpload closes the loop between the two methods that share the staging file's
// shape. Upload is fed lines produced by Transform and nothing else, so a drift in either side's idea
// of the format shows up here rather than in production.
func TestTransformFeedsUpload(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			require.Len(t, request.Contacts, 2)
			return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
		})

	uploader := newUploader(api)
	lines := make([]string, 0, 2)
	for _, event := range []struct {
		jobID   int64
		payload string
	}{
		{jobID: 11, payload: `{"body":{"JSON":{"type":"identify","userId":"user_11","traits":{"email":"Eleven@Example.com","firstName":"Ell"}}}}`},
		{jobID: 12, payload: `{"body":{"JSON":{"type":"track","event":"Signed Up","userId":"user_12","context":{"traits":{"email":"twelve@example.com"}}}}}`},
	} {
		line, err := uploader.Transform(&jobsdb.JobT{JobID: event.jobID, EventPayload: []byte(event.payload)})
		require.NoError(t, err)
		lines = append(lines, line)
	}

	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        writeStagingFile(t, lines...),
		ImportingJobIDs: []int64{11, 12},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.Equal(t, []int64{11, 12}, output.ImportingJobIDs)
	require.Empty(t, output.FailedJobIDs)
	require.Empty(t, output.AbortJobIDs)
}

// TestContactWireShape pins the request body against the documented contract. Every field carries
// omitempty for one specific reason: SendGrid UPSERTS, so a field sent empty overwrites whatever the
// contact already held, while a field omitted leaves it exactly as it was. An eager marshaller would
// therefore erase data on every event that happened not to carry a trait.
func TestContactWireShape(t *testing.T) {
	t.Parallel()

	t.Run("absent traits are omitted, never sent empty", func(t *testing.T) {
		t.Parallel()

		rendered, err := jsonrs.Marshal(sendgridbulkupload.Contact{Email: "one@example.com"})
		require.NoError(t, err)
		require.JSONEq(t, `{"email":"one@example.com"}`, string(rendered))
	})

	t.Run("the documented field names are used verbatim", func(t *testing.T) {
		t.Parallel()

		rendered, err := jsonrs.Marshal(sendgridbulkupload.Contact{
			Email:               "one@example.com",
			PhoneNumberID:       "+14155551234",
			ExternalID:          "user_1",
			AnonymousID:         "anon_1",
			FirstName:           "One",
			LastName:            "Example",
			AddressLine1:        "1 Main St",
			AddressLine2:        "Apt 2",
			City:                "San Francisco",
			StateProvinceRegion: "CA",
			PostalCode:          "94105",
			Country:             "US",
			AlternateEmails:     []string{"one.alt@example.com"},
			CustomFields:        map[string]any{"w1": "enterprise"},
		})
		require.NoError(t, err)
		require.JSONEq(t, `{
			"email":"one@example.com",
			"phone_number_id":"+14155551234",
			"external_id":"user_1",
			"anonymous_id":"anon_1",
			"first_name":"One",
			"last_name":"Example",
			"address_line_1":"1 Main St",
			"address_line_2":"Apt 2",
			"city":"San Francisco",
			"state_province_region":"CA",
			"postal_code":"94105",
			"country":"US",
			"alternate_emails":["one.alt@example.com"],
			"custom_fields":{"w1":"enterprise"}
		}`, string(rendered))
	})

	t.Run("an upsert request carrying no list ids omits the field", func(t *testing.T) {
		t.Parallel()

		rendered, err := jsonrs.Marshal(sendgridbulkupload.UpsertRequest{
			Contacts: []sendgridbulkupload.Contact{{Email: "one@example.com"}},
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"contacts":[{"email":"one@example.com"}]}`, string(rendered))
	})

	t.Run("an upsert request carrying list ids sends them", func(t *testing.T) {
		t.Parallel()

		rendered, err := jsonrs.Marshal(sendgridbulkupload.UpsertRequest{
			ListIDs:  []string{fixtureListID},
			Contacts: []sendgridbulkupload.Contact{{Email: "one@example.com"}},
		})
		require.NoError(t, err)
		require.JSONEq(t,
			`{"list_ids":["`+fixtureListID+`"],"contacts":[{"email":"one@example.com"}]}`,
			string(rendered))
	})

	t.Run("the accepted response carries only the import job id", func(t *testing.T) {
		t.Parallel()

		var response sendgridbulkupload.UpsertResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"job_id":"sg-1"}`), &response))
		require.Equal(t, "sg-1", response.JobID)
	})
}

// TestAPIErrorWireShape covers the error bodies. The rate-limit body sends a null field, which is why
// the field is a pointer: a string would fail to unmarshal the one body this connector most needs to
// understand.
func TestAPIErrorWireShape(t *testing.T) {
	t.Parallel()

	t.Run("a null field is decoded, not rejected", func(t *testing.T) {
		t.Parallel()

		var response sendgridbulkupload.APIErrorResponse
		require.NoError(t, jsonrs.Unmarshal(
			[]byte(`{"errors":[{"field":null,"message":"too many requests"}]}`), &response))
		require.Len(t, response.Errors, 1)
		require.Nil(t, response.Errors[0].Field)
		require.Equal(t, "too many requests", response.Errors[0].Message)
	})

	t.Run("a named field is decoded too", func(t *testing.T) {
		t.Parallel()

		var response sendgridbulkupload.APIErrorResponse
		require.NoError(t, jsonrs.Unmarshal(
			[]byte(`{"errors":[{"field":"list_ids","message":"invalid list id"}]}`), &response))
		require.NotNil(t, response.Errors[0].Field)
		require.Equal(t, "list_ids", *response.Errors[0].Field)
	})

	t.Run("an api error renders its status, operation and message", func(t *testing.T) {
		t.Parallel()

		field := "list_ids"
		rendered := (&sendgridbulkupload.APIError{
			StatusCode: http.StatusBadRequest,
			Operation:  "upsert contacts",
			Message:    "invalid list id",
			Errors:     []sendgridbulkupload.APIErrorItem{{Field: &field, Message: "invalid list id"}},
		}).Error()

		require.Contains(t, rendered, "upsert contacts")
		require.Contains(t, rendered, strconv.Itoa(http.StatusBadRequest))
		require.Contains(t, rendered, "invalid list id")
	})

	t.Run("a rate limit error renders the reset window", func(t *testing.T) {
		t.Parallel()

		resetAt := time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)
		rendered := (&sendgridbulkupload.RateLimitError{
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: "60",
			ResetAt:    resetAt.Unix(),
			Limit:      600,
			Remaining:  0,
			Message:    "too many requests",
		}).Error()

		require.Contains(t, rendered, strconv.Itoa(http.StatusTooManyRequests))
		require.Contains(t, rendered, "Retry-After: 60")
		// Rendered as an absolute instant, because X-RateLimit-Reset is epoch seconds rather than a
		// delta and an operator reading a bare number could not act on it.
		require.Contains(t, rendered, resetAt.Format(time.RFC3339))
		require.Contains(t, rendered, "too many requests")
	})

	t.Run("a rate limit error with no headers at all still renders", func(t *testing.T) {
		t.Parallel()

		rendered := (&sendgridbulkupload.RateLimitError{StatusCode: http.StatusTooManyRequests}).Error()
		require.Contains(t, rendered, strconv.Itoa(http.StatusTooManyRequests))
		require.NotContains(t, rendered, "Retry-After")
	})

	t.Run("both error types are recoverable with errors.As through a wrap", func(t *testing.T) {
		t.Parallel()

		// The connector's own branching depends on this: Upload distinguishes a rate limit from
		// every other rejection by unwrapping, not by inspecting a string.
		rateLimit := &sendgridbulkupload.RateLimitError{StatusCode: http.StatusTooManyRequests}
		var recoveredRateLimit *sendgridbulkupload.RateLimitError
		require.True(t, errors.As(fmt.Errorf("wrapped: %w", rateLimit), &recoveredRateLimit))
		require.Equal(t, rateLimit, recoveredRateLimit)

		apiErr := &sendgridbulkupload.APIError{StatusCode: http.StatusBadRequest, Operation: "upsert contacts"}
		var recoveredAPIError *sendgridbulkupload.APIError
		require.True(t, errors.As(fmt.Errorf("wrapped: %w", apiErr), &recoveredAPIError))
		require.Equal(t, apiErr, recoveredAPIError)

		// And they are not each other, which is what keeps the 429 branch from swallowing a 400.
		require.False(t, errors.As(error(apiErr), &recoveredRateLimit))
	})
}

// TestGetUploadStatsReadsEveryPayloadShape covers the three places a job's event message can live.
// Reconciliation re-derives every contact identifier from the importing jobs themselves, so a payload
// written by an older transformation must still be readable or its contact would be reported as
// unidentifiable and retried needlessly.
func TestGetUploadStatsReadsEveryPayloadShape(t *testing.T) {
	t.Parallel()

	for name, payload := range map[string]string{
		"the staging line shape Upload writes": `{"message":{"type":"identify","traits":{"email":"blake@example.com"}},"metadata":{"job_id":2}}`,
		"the router envelope Transform reads":  `{"body":{"JSON":{"type":"identify","traits":{"email":"blake@example.com"}}}}`,
		"a bare event message":                 `{"type":"identify","traits":{"email":"blake@example.com"}}`,
		"traits nested under context":          `{"message":{"type":"track","context":{"traits":{"email":"blake@example.com"}}},"metadata":{"job_id":2}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
				[]byte(`[{"email":"BLAKE@example.com","message":"invalid phone"}]`), nil)

			response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: outcomeDocument(t, importedFrom("sg-1", "errored", testErrorsURL, 2, 3)),
				ImportingList: []*jobsdb.JobT{
					{JobID: 2, EventPayload: []byte(payload)},
					{JobID: 3, EventPayload: []byte(contactLine(t, 3))},
				},
			})

			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, []int64{2}, response.Metadata.FailedKeys,
				"the identifier must be re-derivable from this payload shape")
			require.Equal(t, []int64{3}, response.Metadata.SucceededKeys)
		})
	}
}

// TestErrorsDocumentURLPolicy pins the trust boundary the errors-document fetch enforces, using the
// REAL API service rather than the mock.
//
// Only the rejection paths are exercised, and that is what keeps the suite free of sockets: every
// rule below is evaluated before any connection is attempted, so a URL that must be refused is
// refused without a packet leaving the process. The accepting side is deliberately not exercised
// here, because accepting means dialing.
//
// The policy is default-CLOSED on every rule, the host included. The errors document url is the one
// address this connector takes from remote input, so the hosts it may reach are enumerated rather
// than inferred: an arbitrary public host is refused for being unknown, SendGrid's own hosts are
// accepted out of the box, and a lookalike cannot suffix its way onto the list. The operator
// override that replaces the default list is exercised separately, in the sequential
// TestErrorsDocumentHostAllowListOverride, because it mutates process configuration.
func TestErrorsDocumentURLPolicy(t *testing.T) {
	t.Parallel()

	newAPI := func(t *testing.T, config sendgridbulkupload.DestinationConfig) sendgridbulkupload.SendGridAPIService {
		t.Helper()
		api, err := sendgridbulkupload.NewSendGridAPIService(testDestinationID, config, logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NotNil(t, api)
		return api
	}

	t.Run("construction requires an api key", func(t *testing.T) {
		t.Parallel()

		for name, apiKey := range map[string]string{
			"absent":     "",
			"whitespace": "   ",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				api, err := sendgridbulkupload.NewSendGridAPIService(
					testDestinationID, sendgridbulkupload.DestinationConfig{APIKey: apiKey}, logger.NOP, stats.NOP)
				require.Error(t, err)
				require.Nil(t, api)
				require.Contains(t, err.Error(), "apiKey")
			})
		}
	})

	t.Run("a url that breaks a transport rule is refused before any connection", func(t *testing.T) {
		t.Parallel()

		api := newAPI(t, testConfig())
		for name, testCase := range map[string]struct {
			url    string
			reason string
		}{
			"plain http": {
				url:    "http://api.sendgrid.com/v3/marketing/contacts/imports/errors/abc",
				reason: "https is required",
			},
			"a file url": {
				url:    "file:///etc/passwd",
				reason: "https is required",
			},
			"an embedded credential": {
				url:    "https://user:secret@api.sendgrid.com/errors/abc",
				reason: "user information",
			},
			"a fragment": {
				url:    "https://api.sendgrid.com/errors/abc#fragment",
				reason: "fragment",
			},
			"an ipv4 literal": {
				url:    "https://169.254.169.254/errors/abc",
				reason: "not an ip literal",
			},
			"an ipv6 literal": {
				url:    "https://[::1]/errors/abc",
				reason: "not an ip literal",
			},
			"a non-443 port": {
				url:    "https://api.sendgrid.com:8443/errors/abc",
				reason: "443 is required",
			},
			"a punycode host": {
				url:    "https://xn--80ak6aa92e.com/errors/abc",
				reason: "internationalised",
			},
			"no host at all": {
				url:    "https:///errors/abc",
				reason: "no host",
			},
			"an empty url": {
				url:    "   ",
				reason: "empty",
			},
			"an unparseable url": {
				url:    "https://api.sendgrid.com/errors/\x7f\x00",
				reason: "not a valid url",
			},
			"an absurdly long url": {
				url:    "https://api.sendgrid.com/errors/" + strings.Repeat("a", 8192),
				reason: "longer than",
			},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				document, err := api.GetImportErrors(testCase.url)
				require.Error(t, err)
				require.Nil(t, document)
				require.Contains(t, err.Error(), testCase.reason)
			})
		}
	})

	t.Run("the host allow list is default deny", func(t *testing.T) {
		t.Parallel()

		// The policy this connector ships is default-DENY: the errors document url is the one
		// address that arrives from remote input, so the hosts it may reach are enumerated rather
		// than inferred from the rest of the request's shape. No allow-list key is set in this
		// suite, so the default is what applies, and it is asserted through its observable
		// consequence - an unknown host is refused FOR BEING UNKNOWN, before any connection, with
		// the offending host and the configuration key that governs it named in the failure.
		api := newAPI(t, testConfig())
		document, err := api.GetImportErrors("https://sendgrid-exports.s3.amazonaws.com/errors/abc")
		require.Error(t, err)
		require.Nil(t, document)
		require.Contains(t, err.Error(), "allow list",
			"an arbitrary public host must be refused by default; enumerating the host is what bounds this request")
		require.Contains(t, err.Error(), "sendgrid-exports.s3.amazonaws.com",
			"the refusal must name the offending host so the corrective action is contained in the failure")
		require.Contains(t, err.Error(), errorsURLAllowedHostsConfigKey,
			"the refusal must name the configuration key an operator has to set")
	})

	// The accepting side of the allow list - that SendGrid's own hosts pass it - is deliberately NOT
	// asserted anywhere in this package, and the reason is a property of the assertion rather than an
	// oversight: a url the policy ACCEPTS is a url the adapter then DIALS, and no test here opens a
	// socket to a real provider. What is observable without one is asserted instead, in three places
	// that together bound the policy from both sides: the shipped default is a populated, non-empty
	// enumeration - errors_document_allowed_host_count is 2 in TestErrorsDocumentReadBudget, so an
	// unconfigured destination is default-deny rather than stalled - the effective list tracks an
	// operator override in TestErrorsDocumentHostAllowListOverride, and every host outside the list
	// is refused HERE for being outside it rather than by some incidental transport rule.

	t.Run("a lookalike host cannot pass for a sendgrid host", func(t *testing.T) {
		t.Parallel()

		// Suffix matching is exact-or-dot-suffix, which is what keeps a lookalike from qualifying.
		// These are deliberately https on port 443 with no other transport defect, so the ONLY rule
		// that can refuse them is the allow list itself - which is what makes this a test of the
		// host policy rather than of the scheme check. The point is that none of them may ever be
		// treated as a sendgrid.com host, whether for admission or for the credential.
		api := newAPI(t, testConfig())
		for _, lookalike := range []string{
			"https://api.sendgrid.com.attacker.example/errors/abc",
			"https://notsendgrid.com/errors/abc",
			"https://sendgrid.com.evil.example/errors/abc",
			"https://sendgrid.net.evil.example/errors/abc",
		} {
			document, err := api.GetImportErrors(lookalike)
			require.Error(t, err)
			require.Nil(t, document)
			require.Contains(t, err.Error(), "allow list",
				"a lookalike host must be refused by the allow list, not merely by a transport rule: %s", lookalike)
		}
	})
}

// errorsURLAllowedHostsConfigKey is the configuration key that governs the errors-document host
// allow list. It is restated here rather than imported because it is unexported in the connector,
// and a refusal is required to name it so an operator can act on the failure itself.
const errorsURLAllowedHostsConfigKey = "errorsURLAllowedHosts"

// TestErrorsDocumentHostAllowListOverride pins what an operator override does to the default-deny
// host policy, and what a mistaken override does NOT do.
//
// Sequential rather than parallel: it mutates process configuration, which the adapter reads once at
// construction. Two properties are asserted, and the second is the security-relevant one:
//
//   - an override REPLACES the default list, so a SendGrid host the override omits is no longer
//     accepted. Replacement rather than extension is what keeps the effective policy equal to the
//     configured one. That an overridden host becomes ACCEPTED is not asserted directly, for the
//     same reason the default's accepting side is not: acceptance is followed by a dial, and this
//     suite opens no socket. The published host count below is the socket-free observation of the
//     list actually in force.
//   - an override that normalizes away to nothing - an empty string, blanks, bare dots - falls back
//     to the DEFAULT list, never to "no list". Reading an emptied override as "allow every host"
//     would let one configuration typo silently reopen the exact hole the default closes, which is
//     the regression this test exists to prevent.
func TestErrorsDocumentHostAllowListOverride(t *testing.T) {
	newAPI := func(t *testing.T) sendgridbulkupload.SendGridAPIService {
		t.Helper()
		api, err := sendgridbulkupload.NewSendGridAPIService(testDestinationID, testConfig(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NotNil(t, api)
		return api
	}

	t.Run("an override replaces the default list", func(t *testing.T) {
		setBatchRouterConfig(t, errorsURLAllowedHostsConfigKey, []string{"sendgrid-exports.s3.amazonaws.com"})

		api := newAPI(t)

		document, err := api.GetImportErrors("https://api.sendgrid.com/errors/abc")
		require.Error(t, err)
		require.Nil(t, document)
		require.Contains(t, err.Error(), "allow list",
			"an override replaces the default, so a sendgrid host it omits is no longer allowed")
	})

	t.Run("an override that normalizes away to nothing falls back to the default", func(t *testing.T) {
		setBatchRouterConfig(t, errorsURLAllowedHostsConfigKey, []string{"   ", "", "."})

		api := newAPI(t)

		document, err := api.GetImportErrors("https://sendgrid-exports.s3.amazonaws.com/errors/abc")
		require.Error(t, err)
		require.Nil(t, document)
		require.Contains(t, err.Error(), "allow list",
			"an emptied override must fall back to the default deny policy, never to no policy")
	})

	t.Run("the published host count reflects the effective list", func(t *testing.T) {
		// The gauge is how an operator sees the effective policy without reading code, so it has to
		// track the list actually in force - the override's own entries here, de-duplicated - rather
		// than the default it replaced.
		setBatchRouterConfig(t, errorsURLAllowedHostsConfigKey,
			[]string{"sendgrid.com", "SENDGRID.com", "exports.example.net"})

		store, err := memstats.New()
		require.NoError(t, err)

		api, err := sendgridbulkupload.NewSendGridAPIService(testDestinationID, testConfig(), logger.NOP, store)
		require.NoError(t, err)
		require.NotNil(t, api)

		require.EqualValues(t, 2, gaugeValue(t, store, "errors_document_allowed_host_count"),
			"case-insensitive duplicates collapse, so the count is the effective one")
	})
}

// TestRegistration guards the one class of defect that every other test in this file would miss.
//
// A connector can compile, satisfy the interface, and pass its entire unit suite while never once
// executing in production, because reaching it depends on three separate registries in three separate
// packages. Nothing about the connector's own code fails if one of them is missing - the destination
// simply goes nowhere - which is exactly why the wiring is asserted here rather than assumed.
func TestRegistration(t *testing.T) {
	t.Parallel()

	const destinationType = "SENDGRID_BULK_UPLOAD"

	t.Run("the processor routes these jobs to the batch router", func(t *testing.T) {
		t.Parallel()

		// Without this entry the processor writes the jobs to the REGULAR router's queue and the
		// batch router never receives them at all.
		require.Contains(t, misc.BatchDestinations(), destinationType)
	})

	t.Run("the batch router classifies the destination as asynchronous", func(t *testing.T) {
		t.Parallel()

		// Without this the async upload worker early-returns, the batch-router destination check
		// rejects the type, and the factory's classifier never reaches the construction switch.
		require.True(t, common.IsAsyncDestination(destinationType))
		require.True(t, common.IsAsyncRegularDestination(destinationType))
		require.False(t, common.IsSFTPDestination(destinationType))
	})

	t.Run("the factory constructs the manager", func(t *testing.T) {
		t.Parallel()

		manager, err := asyncdestinationmanager.NewManager(
			config.New(), logger.NOP, stats.NOP, testDestination(map[string]any{
				"apiKey":  "SG.k",
				"listIds": []any{fixtureListID},
			}), nil)

		// Never the factory's fallthrough "invalid destination type".
		require.NoError(t, err)
		require.NotNil(t, manager)
		require.IsType(t, &sendgridbulkupload.SendGridBulkUploader{}, manager)
	})

	t.Run("a misconfigured destination fails the factory rather than the first batch", func(t *testing.T) {
		t.Parallel()

		manager, err := asyncdestinationmanager.NewManager(
			config.New(), logger.NOP, stats.NOP, testDestination(map[string]any{}), nil)
		require.Error(t, err)
		require.Nil(t, manager)
	})

	t.Run("the pre-existing sendgrid cloud destination is untouched", func(t *testing.T) {
		t.Parallel()

		// SENDGRID and SENDGRID_BULK_UPLOAD are different destinations. The bulk connector must not
		// have made the older one batch-routed or asynchronous, and the factory must still refuse it.
		require.NotContains(t, misc.BatchDestinations(), "SENDGRID")
		require.False(t, common.IsAsyncDestination("SENDGRID"))

		manager, err := asyncdestinationmanager.NewManager(
			config.New(), logger.NOP, stats.NOP, &backendconfig.DestinationT{
				ID:                    "dest-sendgrid-cloud",
				Name:                  "SENDGRID",
				DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "SENDGRID"},
				Config:                map[string]any{"apiKey": "SG.k"},
			}, nil)
		require.Error(t, err)
		require.Nil(t, manager)
	})
}

// TestUploadStagingLineHandling separates the two kinds of bad staging line, because they get
// opposite treatments and getting that backwards either strands a job or discards a whole batch.
//
// A line that NAMES its job but cannot be turned into a contact is rejected on its own, so the rest of
// the batch is still delivered. A line that names NO job aborts the read, because nothing can be
// reported against it and continuing would leave the job that produced it silently unaccounted for.
func TestUploadStagingLineHandling(t *testing.T) {
	t.Parallel()

	t.Run("a line that names its job is rejected alone", func(t *testing.T) {
		t.Parallel()

		for name, line := range map[string]string{
			"a message that is not an object":   `{"message":"a string","metadata":{"job_id":2}}`,
			"a message that is an array":        `{"message":[],"metadata":{"job_id":2}}`,
			"no message key at all":             `{"metadata":{"job_id":2}}`,
			"a job id sent as a numeric string": `{"message":42,"metadata":{"job_id":"2"}}`,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				api := newMockAPI(t)
				api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
					func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						require.Len(t, request.Contacts, 1, "the usable record must still be delivered")
						return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
					})

				output := newUploader(api).Upload(&common.AsyncDestinationStruct{
					FileName:        writeStagingFile(t, contactLine(t, 1), line),
					ImportingJobIDs: []int64{1, 2},
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				require.Equal(t, []int64{1}, output.ImportingJobIDs)
				require.Equal(t, []int64{2}, output.AbortJobIDs)
				require.Empty(t, output.FailedJobIDs)
			})
		}
	})

	t.Run("a line that names no job makes the whole batch retryable", func(t *testing.T) {
		t.Parallel()

		for name, line := range map[string]string{
			"not json at all":         `this is not json`,
			"a bare array":            `["nope"]`,
			"no metadata whatsoever":  `{"message":{"type":"identify"}}`,
			"a non-numeric job id":    `{"message":{"type":"identify"},"metadata":{"job_id":"abc"}}`,
			"a zero job id":           `{"message":{"type":"identify"},"metadata":{"job_id":0}}`,
			"a negative job id":       `{"message":{"type":"identify"},"metadata":{"job_id":-7}}`,
			"a job id sent as a list": `{"message":{"type":"identify"},"metadata":{"job_id":[1]}}`,
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				// The API is never reached: nothing is uploaded when the file cannot be read in full.
				output := newUploader(newMockAPI(t)).Upload(&common.AsyncDestinationStruct{
					FileName:        writeStagingFile(t, contactLine(t, 1), line),
					ImportingJobIDs: []int64{1, 2},
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				require.ElementsMatch(t, []int64{1, 2}, output.FailedJobIDs)
				require.Empty(t, output.ImportingJobIDs)
				require.Empty(t, output.AbortJobIDs,
					"an unreadable file says nothing terminal about any individual job")
			})
		}
	})

	t.Run("blank lines are skipped rather than rejected", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 2)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1), "", "   ", contactLine(t, 2)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1, 2}, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})
}

// TestUploadRejectsUnpersistableImportIDs pins the guard that keeps the import manifest readable.
//
// The manifest is a delimited string, so an import identifier containing one of its delimiters would
// corrupt every identifier after it and orphan those imports permanently. Such an upload is reported
// RETRYABLE rather than importing, because an import that cannot be polled is worse than one that is
// re-sent - SendGrid upserts, so the re-send is idempotent.
func TestUploadRejectsUnpersistableImportIDs(t *testing.T) {
	t.Parallel()

	for name, importID := range map[string]string{
		"a manifest separator":   "sg;1",
		"a membership mark":      "sg=1",
		"a job separator":        "sg,1",
		"a legacy colon":         "sg:1",
		"an embedded space":      "sg 1",
		"an embedded tab":        "sg\t1",
		"an embedded newline":    "sg\n1",
		"an implausibly long id": strings.Repeat("s", 200),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI(t)
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(
				&sendgridbulkupload.UpsertResponse{JobID: importID}, nil)

			output := newUploader(api).Upload(&common.AsyncDestinationStruct{
				FileName:        writeStagingFile(t, contactLine(t, 1), contactLine(t, 2)),
				ImportingJobIDs: []int64{1, 2},
				Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
			})

			require.ElementsMatch(t, []int64{1, 2}, output.FailedJobIDs)
			require.Empty(t, output.ImportingJobIDs)
			require.Nil(t, output.ImportingParameters)
			require.Empty(t, output.AbortJobIDs)
		})
	}

	t.Run("a hyphenated uuid is accepted and polls correctly", func(t *testing.T) {
		t.Parallel()

		// The hyphen is deliberately NOT forbidden, and this case is why: SendGrid returns a UUID,
		// which is full of hyphens. The range mark is only ever read INSIDE a membership section, so
		// an identifier is never parsed as a range and a hyphen in one is unambiguous. Forbidding it
		// would reject every import SendGrid actually creates.
		const uuid = "e3a4f0d8-4b1e-4c2a-9f77-2b6d5c8e1a90"

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(
			&sendgridbulkupload.UpsertResponse{JobID: uuid}, nil)

		uploader := newUploader(api)
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1), contactLine(t, 2)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1, 2}, output.ImportingJobIDs)
		importID, importCount := importParametersOf(t, output)
		require.Equal(t, uuid+"=1-2", importID)
		require.Equal(t, int64(2), importCount)

		// And the persisted value decodes back to exactly that one identifier, which is the whole
		// point of the guard: the manifest has to survive the round trip.
		api.EXPECT().GetImportStatus(uuid).Times(1).Return(pollStatus(uuid, "completed", 0, ""), nil)
		pollResponse := uploader.Poll(common.AsyncPoll{ImportId: importID, ImportCount: int(importCount)})
		require.Equal(t, http.StatusOK, pollResponse.StatusCode)
		require.True(t, pollResponse.Complete)
		require.False(t, pollResponse.HasFailed)
	})
}

// TestUploadCustomFieldMapping pins the custom-field contract: SendGrid addresses custom fields by an
// opaque pre-created ID, so a trait with no mapping entry is not sent at all. Inventing a field name
// would produce nothing but rejected requests.
func TestUploadCustomFieldMapping(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			require.Len(t, request.Contacts, 1)
			contact := request.Contacts[0]
			// Keyed by the SendGrid field ID, never by the trait name.
			require.Equal(t, "enterprise", contact.CustomFields["w1"])
			require.NotContains(t, contact.CustomFields, "plan")
			// An unmapped trait is absent entirely rather than guessed at.
			require.NotContains(t, contact.CustomFields, "unmappedTrait")
			require.Len(t, contact.CustomFields, 1)
			return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
		})

	output := newUploader(api).Upload(&common.AsyncDestinationStruct{
		FileName: writeStagingFile(t, stagingLine(t, 1,
			`{"type":"identify","userId":"user_1","traits":{"email":"one@example.com",`+
				`"plan":"enterprise","unmappedTrait":"ignored"}}`)),
		ImportingJobIDs: []int64{1},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.Equal(t, []int64{1}, output.ImportingJobIDs)
}

// ---------------------------------------------------------------------------------------------
// The errors document is the connector's least specified input, and the exhaustive coverage below
// is what keeps its parser honest.
//
// SendGrid publishes NO schema for the document behind results.errors_url: the official
// specification mentions the field exactly twice and both times only as a bare string URL, with no
// media type, no schema and no stated retention, and the reference pages describe no format at all.
// The parser is therefore deliberately tolerant, and tolerance that is not exhaustively pinned is
// indistinguishable from a guess - so every shape it accepts, every key precedence it applies,
// every shape it refuses and every bound it enforces is asserted here rather than assumed.
// ---------------------------------------------------------------------------------------------

// reconcileFixtureDocument reconciles ONE errors document against the five staged fixture jobs.
//
// The errors URL is passed in the legacy bare-URL form rather than as a rendered outcome document,
// which is the shape reconciliation must still accept from an upload that was polled by an older
// build: the URL then describes an import whose membership is unknown, so every job the document
// does not name is settled by exclusion. GetImportStatus is expected zero times, because a
// forwarded URL is all the evidence this path needs and re-reading the import would be a provider
// request nobody asked for.
func reconcileFixtureDocument(t *testing.T, document string) common.GetUploadStatsResponse {
	t.Helper()

	api := newMockAPI(t)
	api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(document), nil)
	api.EXPECT().GetImportStatus(gomock.Any()).Times(0)

	return newUploader(api).GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: testErrorsURL,
		Parameters:          persistedParameters(t, "sg-1", 5),
		ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
	})
}

// remainingFixtureJobs returns the staged fixture's job IDs minus the ones given, which is the set
// succeeded-by-exclusion must produce for a document naming exactly those.
func remainingFixtureJobs(failed ...int64) []int64 {
	remaining := make([]int64, 0, 5)
	for jobID := int64(1); jobID <= 5; jobID++ {
		if !slices.Contains(failed, jobID) {
			remaining = append(remaining, jobID)
		}
	}
	return remaining
}

// TestGetUploadStatsDocumentShapeTolerance pins the shapes the parser accepts beyond the ones the
// happy-path cases already exercise. Each has to yield the SAME reconciliation, because which of
// these shapes SendGrid happens to serve is not something the connector may depend on.
func TestGetUploadStatsDocumentShapeTolerance(t *testing.T) {
	t.Parallel()

	const (
		blakeReason = "Invalid email address provided for contact."
		devonReason = "Contact rejected: custom field value exceeds the maximum allowed length."
	)

	for name, testCase := range map[string]struct {
		document      string
		failedKeys    []int64
		failedReasons map[int64]string
	}{
		"a bare array of rows": {
			document:      fmt.Sprintf(`[{"email":"blake@example.com","message":%q}]`, blakeReason),
			failedKeys:    []int64{2},
			failedReasons: map[int64]string{2: blakeReason},
		},
		"a single row object that is not wrapped in anything": {
			document:      fmt.Sprintf(`{"email":"blake@example.com","message":%q}`, blakeReason),
			failedKeys:    []int64{2},
			failedReasons: map[int64]string{2: blakeReason},
		},
		"newline delimited rows padded with blank and whitespace-only lines": {
			document: fmt.Sprintf(
				"\n{\"email\":\"blake@example.com\",\"message\":%q}\n\n   \n{\"contact\":{\"email\":\"devon@example.com\"},\"error_message\":%q}\n\n",
				blakeReason, devonReason),
			failedKeys:    []int64{2, 4},
			failedReasons: map[int64]string{2: blakeReason, 4: devonReason},
		},
		"an errors array alongside unrelated keys the parser ignores": {
			document: fmt.Sprintf(
				`{"job_id":"sg-1","status":"errored","errored_count":1,"errors":[{"email":"blake@example.com","message":%q}]}`,
				blakeReason),
			failedKeys:    []int64{2},
			failedReasons: map[int64]string{2: blakeReason},
		},
		"an errors array in preference to a results array": {
			document: fmt.Sprintf(
				`{"errors":[{"email":"blake@example.com","message":%q}],"results":[{"email":"casey@example.com","message":%q}]}`,
				blakeReason, devonReason),
			failedKeys:    []int64{2},
			failedReasons: map[int64]string{2: blakeReason},
		},
		"a results array when there is no errors array": {
			document: fmt.Sprintf(
				`{"results":[{"email":"blake@example.com","message":%q},{"email":"casey@example.com","message":%q}]}`,
				blakeReason, devonReason),
			failedKeys:    []int64{2, 3},
			failedReasons: map[int64]string{2: blakeReason, 3: devonReason},
		},
		"rows carrying an unknown key alongside the ones the parser reads": {
			// The committed fixture carries error_indices, which this connector does not use.
			// An unknown key must neither make a row unrecognizable nor become its message.
			document: fmt.Sprintf(
				`{"errors":[{"email":"blake@example.com","message":%q,"error_indices":[1]}]}`, blakeReason),
			failedKeys:    []int64{2},
			failedReasons: map[int64]string{2: blakeReason},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := reconcileFixtureDocument(t, testCase.document)

			require.Equal(t, http.StatusOK, response.StatusCode)
			require.ElementsMatch(t, testCase.failedKeys, response.Metadata.FailedKeys)
			for jobID, reason := range testCase.failedReasons {
				require.Contains(t, response.Metadata.FailedReasons[jobID], reason)
			}
			require.ElementsMatch(t, remainingFixtureJobs(testCase.failedKeys...), response.Metadata.SucceededKeys)
			require.Empty(t, response.Metadata.AbortedKeys, "a per-row error is recoverable, never terminal")
			assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
		})
	}
}

// TestGetUploadStatsRowKeyPrecedence pins the key precedence WITHIN one errored row: which key an
// identifier is read from and which key a message is read from, when a row carries several.
//
// Precedence is the part tolerance cannot be sloppy about. A row carrying two identifiers must
// resolve to exactly ONE job - resolving it to both would fail a contact SendGrid never complained
// about - so the order the candidates are consulted in is asserted rather than left to whichever
// key the implementation happens to test first.
func TestGetUploadStatsRowKeyPrecedence(t *testing.T) {
	t.Parallel()

	// The reason recorded when a row names a contact but gives no usable message. Mirrored as a
	// literal because the batch router persists it verbatim against the job.
	const defaultFailureReason = "sendgrid reported an error for this contact without a message"

	for name, testCase := range map[string]struct {
		row          string
		failedKey    int64
		failedReason string
	}{
		"an email outranks every other identifier key": {
			// Job 2 is blake, job 4 is devon, job 3 is casey and job 5 carried user_523: four
			// different jobs are named by the four keys, so only precedence can decide.
			row: `{"email":"blake@example.com","contact":{"email":"devon@example.com"},` +
				`"identifier":"casey@example.com","external_id":"user_523","anonymous_id":"anon_889","message":"rejected"}`,
			failedKey:    2,
			failedReason: "rejected",
		},
		"a nested contact email is used when there is no email": {
			row:          `{"contact":{"email":"devon@example.com"},"identifier":"casey@example.com","message":"rejected"}`,
			failedKey:    4,
			failedReason: "rejected",
		},
		"an identifier is used when neither email key is present": {
			// Up-cased on purpose: SendGrid lower-cases every email it stores, so both sides of
			// the comparison are folded before they are matched.
			row:          `{"identifier":"CASEY@EXAMPLE.COM","external_id":"user_523","message":"rejected"}`,
			failedKey:    3,
			failedReason: "rejected",
		},
		"an external id resolves the job that carried that user id": {
			row:          `{"external_id":"user_523","message":"rejected"}`,
			failedKey:    5,
			failedReason: "rejected",
		},
		"an anonymous id resolves the job that carried that anonymous id": {
			// Job 4 is the staged event with no userId at all, so its anonymous id is the only
			// identifier besides its email that can resolve it.
			row:          `{"anonymous_id":"anon_889","message":"rejected"}`,
			failedKey:    4,
			failedReason: "rejected",
		},
		"a blank email falls through to the next identifier key": {
			row:          `{"email":"   ","identifier":"casey@example.com","message":"rejected"}`,
			failedKey:    3,
			failedReason: "rejected",
		},
		"a message outranks every other message key": {
			row:          `{"email":"blake@example.com","message":"from message","error_message":"from error_message","reason":"from reason","detail":"from detail"}`,
			failedKey:    2,
			failedReason: "from message",
		},
		"an error message is used when there is no message": {
			row:          `{"email":"blake@example.com","error_message":"from error_message","reason":"from reason","detail":"from detail"}`,
			failedKey:    2,
			failedReason: "from error_message",
		},
		"a reason is used when neither message nor error message is present": {
			row:          `{"email":"blake@example.com","reason":"from reason","detail":"from detail"}`,
			failedKey:    2,
			failedReason: "from reason",
		},
		"a detail is the last message key considered": {
			row:          `{"email":"blake@example.com","detail":"from detail"}`,
			failedKey:    2,
			failedReason: "from detail",
		},
		"a blank message falls back to this connector's own reason": {
			row:          `{"email":"blake@example.com","message":"   "}`,
			failedKey:    2,
			failedReason: defaultFailureReason,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := reconcileFixtureDocument(t, `{"errors":[`+testCase.row+`]}`)

			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, []int64{testCase.failedKey}, response.Metadata.FailedKeys,
				"a row carrying several candidate keys must resolve to exactly one job")
			// The persisted reason carries a stable connector-owned code in FRONT of the provider's
			// text, so this pins both halves: the code a downstream consumer matches on, and the
			// message key that precedence selected. Asserted by equality rather than containment,
			// because "the right message wins" is only meaningful if no other message is also there.
			recorded := response.Metadata.FailedReasons[testCase.failedKey]
			if testCase.failedReason == defaultFailureReason {
				require.Equal(t, "SENDGRID_CONTACT_REJECTED_WITHOUT_MESSAGE: "+defaultFailureReason, recorded)
			} else {
				require.Equal(t, "SENDGRID_CONTACT_REJECTED: "+testCase.failedReason, recorded)
			}
			require.ElementsMatch(t, remainingFixtureJobs(testCase.failedKey), response.Metadata.SucceededKeys)
			assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3, 4, 5})
		})
	}
}

// TestGetUploadStatsDocumentRejections pins the outcome for every document the parser cannot make
// sense of: HTTP 500, which asks the batch router to retry the whole import.
//
// This is the deliberately safer failure mode, and it is the single most consequential assertion in
// the suite. Returning 200 with an empty failed set would mark every job in the import as delivered
// on the strength of a document nobody understood, losing every contact SendGrid actually rejected.
// Nothing is ever reported succeeded on a guess.
func TestGetUploadStatsDocumentRejections(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		document string
		contains string
	}{
		"an empty document":                        {document: "", contains: "the errors document is empty"},
		"a document of nothing but whitespace":     {document: "   \n\t\n ", contains: "the errors document is empty"},
		"a truncated document":                     {document: `{"errors":[{"email":"blake@example.com"`, contains: "matches none of the shapes this parser understands"},
		"a json string":                            {document: `"a contact was rejected"`, contains: "neither an array nor an object"},
		"a json number":                            {document: `42`, contains: "neither an array nor an object"},
		"a json boolean":                           {document: `true`, contains: "neither an array nor an object"},
		"a json null":                              {document: `null`, contains: "neither an array nor an object"},
		"bytes that are not json at all":           {document: "<html><body>504 gateway timeout</body></html>", contains: "matches none of the shapes this parser understands"},
		"an empty array":                           {document: `[]`, contains: "an array carrying no recognizable rows"},
		"an array of rows the parser cannot read":  {document: `[{"unexpected":"shape"},{"another":1}]`, contains: "an array carrying no recognizable rows"},
		"an empty errors array":                    {document: `{"errors":[]}`, contains: `"errors" array carries no recognizable rows`},
		"an errors array the parser cannot read":   {document: `{"errors":[{"error_indices":[7]}]}`, contains: `"errors" array carries no recognizable rows`},
		"an empty results array":                   {document: `{"results":[]}`, contains: `"results" array carries no recognizable rows`},
		"an object that is not a row":              {document: `{"job_id":"sg-1","status":"errored"}`, contains: "an object carrying no recognizable rows"},
		"a document nested deeper than is allowed": {document: strings.Repeat("[", 33) + strings.Repeat("]", 33), contains: "nested 33 levels deep, more than the 32 allowed"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := reconcileFixtureDocument(t, testCase.document)

			require.Equal(t, http.StatusInternalServerError, response.StatusCode,
				"an unusable errors document must ask the batch router to retry")
			require.Contains(t, response.Error, "Failed to parse the sendgrid errors document")
			require.Contains(t, response.Error, testCase.contains)
			require.Equal(t, common.EventStatMeta{}, response.Metadata,
				"no job may be reported either way out of a document that was never understood")
		})
	}
}

// TestGetUploadStatsDocumentRowLimits pins the row cap the parser applies on top of the byte cap the
// transport already enforces, so that a document within the byte budget still cannot turn into an
// unbounded number of rows inside a batch router worker shared by every destination in the process.
//
// The cap sits far above the 30,000 contacts one request can carry, so only a document that does not
// describe a single import can reach it. Each shape enforces the cap at a different point - a
// wrapped array knows its length up front, a newline delimited document only finds out as it is
// scanned - so each is exercised.
func TestGetUploadStatsDocumentRowLimits(t *testing.T) {
	t.Parallel()

	// One row past the cap, so it is the boundary itself that is crossed.
	const rowsPastTheCap = 100_001

	for name, document := range map[string]string{
		"a bare array holding more rows than the parser allows": "[" +
			strings.Repeat(`{"email":"over@example.com"},`, rowsPastTheCap-1) + `{"email":"over@example.com"}]`,
		"an errors array holding more rows than the parser allows": `{"errors":[` +
			strings.Repeat(`{"email":"over@example.com"},`, rowsPastTheCap-1) + `{"email":"over@example.com"}]}`,
		"a newline delimited document holding more rows than the parser allows": strings.Repeat(
			"{\"email\":\"over@example.com\"}\n", rowsPastTheCap),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := reconcileFixtureDocument(t, document)

			require.Equal(t, http.StatusInternalServerError, response.StatusCode)
			require.Contains(t, response.Error, "still allowed for this upload")
			require.Equal(t, common.EventStatMeta{}, response.Metadata)
		})
	}

	t.Run("a document exactly at the cap is reconciled", func(t *testing.T) {
		t.Parallel()

		// The boundary from the accepting side, so the cap is proven to be off-by-none: the same
		// document one row shorter must be read in full rather than refused.
		// Attributable filler, for the same reason as above: the property under test is the cap.
		document := `{"errors":[` +
			strings.Repeat(`{"email":"blake@example.com"},`, 100_000-1) +
			`{"email":"blake@example.com","message":"rejected"}]}`
		response := reconcileFixtureDocument(t, document)

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
		require.ElementsMatch(t, []int64{1, 3, 4, 5}, response.Metadata.SucceededKeys)
	})
}

// TestGetUploadStatsRowBudgetSpansEveryDocumentOfAnImport pins that the row allowance is ONE budget
// for the whole import rather than one per document.
//
// An import can publish several errors documents, and a per-document bound would let two documents
// that are each comfortably acceptable together allocate twice the allowance - which is precisely
// the case a per-document reading of the cap would fail to notice.
func TestGetUploadStatsRowBudgetSpansEveryDocumentOfAnImport(t *testing.T) {
	t.Parallel()

	// Three fifths of the allowance each: either document is fine alone, the pair is not.
	const rowsPerDocument = 60_000
	secondErrorsURL := testErrorsURL + "-2"

	// The filler names job 2 as well, so every row is attributable: this case is about the row
	// BUDGET, and an unattributable filler row would fail the import closed for an unrelated reason.
	wrappedRows := `{"errors":[` +
		strings.Repeat(`{"email":"blake@example.com"},`, rowsPerDocument-1) +
		`{"email":"blake@example.com","message":"rejected"}]}`

	t.Run("one document inside the allowance is reconciled", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(wrappedRows), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
	})

	t.Run("two documents that together exceed the allowance are refused", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(wrappedRows), nil)
		api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).Return([]byte(wrappedRows), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL + "\n" + secondErrorsURL,
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "still allowed for this upload")
		require.Empty(t, response.Metadata.SucceededKeys,
			"nothing may be reported delivered once the evidence stopped being readable")
	})
}

// TestGetUploadStatsRowBudgetChargesEveryEntryItReads pins that the aggregate row allowance is spent
// on every entry a document PRESENTS, not merely on the entries this connector managed to read.
//
// Charging only the reduced rows leaves the one document shape most likely to be hostile as the one
// shape the cap does not bound: a document made of entries the parser declines - objects with no
// field this connector knows, values that are not objects, malformed lines - would cost nothing at
// all against the allowance, so an import could publish document after document of them. The
// arithmetic below is chosen so the two readings disagree: each document alone is comfortably inside
// the allowance, and each carries exactly ONE reducible row, so an accounting that charges rows would
// spend 1 per document and accept an unbounded number of them.
func TestGetUploadStatsRowBudgetChargesEveryEntryItReads(t *testing.T) {
	t.Parallel()

	// Three fifths of the allowance each in ENTRIES, one reducible row each.
	const entriesPerDocument = 60_000
	secondErrorsURL := testErrorsURL + "-2"

	unreadableEntries := strings.Repeat(`{"unknown_field":"nothing this connector can reduce"},`, entriesPerDocument-1)
	wrappedDocument := `{"errors":[` + unreadableEntries + `{"email":"blake@example.com","message":"rejected"}]}`

	t.Run("one document inside the allowance is still read", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(wrappedDocument), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Contains(t, response.Metadata.FailedKeys, int64(2),
			"the one reducible row must still be attributed to its job")
	})

	t.Run("two documents whose entries together exceed the allowance are refused", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(wrappedDocument), nil)
		api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).Return([]byte(wrappedDocument), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL + "\n" + secondErrorsURL,
			ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode,
			"entries the parser declined must be charged against the allowance, or this pair passes")
		require.Contains(t, response.Error, "still allowed for this upload")
		require.Empty(t, response.Metadata.SucceededKeys,
			"nothing may be reported delivered once the evidence stopped being readable")
	})

	t.Run("a newline delimited document of unreadable lines is bounded too", func(t *testing.T) {
		t.Parallel()

		// One line past the cap, all but one of them unreadable. Charging only reducible rows would
		// read this document to its end and accept it on the strength of its single usable line.
		document := strings.Repeat("{\"unknown_field\":\"nothing to reduce\"}\n", 100_000) +
			"{\"email\":\"blake@example.com\",\"message\":\"rejected\"}\n"

		response := reconcileFixtureDocument(t, document)

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "still allowed for this upload")
		require.Equal(t, common.EventStatMeta{}, response.Metadata)
	})
}

// TestGetUploadStatsRowAllowanceScalesWithTheUpload pins that the allowance an upload's errors
// documents share is DERIVED from the number of contacts that upload sent, not a flat literal.
//
// This is the regression guard for a stall, and the stall is the whole point of the case. One upload
// may create many imports of up to 30,000 contacts each, so it can legitimately carry far more
// contacts than the allowance's floor - and if SendGrid rejects them, a truthful set of errors
// documents holds a row for every one. Against a flat allowance that upload becomes a document the
// connector refuses to finish reading, and a refusal here does not merely lose information: the
// batch router writes NO job status at all for a non-200 from GetUploadStats and its poll route has
// no retry budget that could escalate, so every job of the upload would stay importing forever and
// the destination would stop accepting new work. Deriving the allowance from the importing list is
// what makes that unreachable, because SendGrid cannot report an errored contact it was never sent.
//
// The two bounds that survive the derivation are pinned here as well, because a fix that removed
// them instead of scaling one of them would look identical in the first case and be a regression:
// the derived allowance is still a bound, and no single document may still be read into more rows
// than the connector will hold at one time.
//
// The subtests run SEQUENTIALLY on purpose. They share one deliberately large importing list, and
// running them one at a time keeps this case's peak memory at a single reconciliation's worth
// however the suite happens to be shuffled.
func TestGetUploadStatsRowAllowanceScalesWithTheUpload(t *testing.T) {
	t.Parallel()

	// Four contacts past the floor, so only an allowance derived from the upload can cover the pair
	// of documents below, and an even count so the pair divides exactly.
	const importingJobCount = 100_004
	// Half the allowance each: either document is comfortably inside the per-document ceiling, and
	// together they come to exactly one row per importing job.
	const rowsPerDocument = importingJobCount / 2

	// Built once. Materializing an upload this size is the only expensive thing here, and nothing
	// below mutates it.
	jobs := syntheticImportingJobs(t, importingJobCount)
	secondErrorsURL := testErrorsURL + "-2"
	// Every row names the first job's contact, so attribution is unambiguous and the case is about
	// the allowance rather than about matching: job 1 is rejected and every other job succeeds by
	// exclusion.
	firstDocument := errorsDocumentNaming(firstSyntheticContact, rowsPerDocument)

	t.Run("documents summing to one row per importing job are read in full", func(t *testing.T) {
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(firstDocument), nil)
		api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).Return([]byte(firstDocument), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL + "\n" + secondErrorsURL,
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusOK, response.StatusCode,
			"an upload larger than the floor must still be reconcilable, or its jobs stay importing forever")
		require.Empty(t, response.Error)
		require.Equal(t, []int64{1}, response.Metadata.FailedKeys)
		require.Len(t, response.Metadata.SucceededKeys, importingJobCount-1,
			"every contact no row named must be reported delivered")
		require.Empty(t, response.Metadata.AbortedKeys)
	})

	t.Run("one row past the derived allowance is still refused", func(t *testing.T) {
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(firstDocument), nil)
		api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).
			Return([]byte(errorsDocumentNaming(firstSyntheticContact, rowsPerDocument+1)), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL + "\n" + secondErrorsURL,
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode,
			"the derived allowance is a bound, not the removal of one")
		require.Contains(t, response.Error, "still allowed for this upload")
		require.Empty(t, response.Metadata.SucceededKeys,
			"nothing may be reported delivered once the evidence stopped being readable")
	})

	t.Run("one document may still not exceed the per document ceiling", func(t *testing.T) {
		api := newMockAPI(t)
		api.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(errorsDocumentNaming(firstSyntheticContact, 100_001)), nil)

		response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			ImportingList:       jobs,
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode,
			"peak memory is bounded per document, so a larger upload may not enlarge one document's reading")
		require.Contains(t, response.Error, "still allowed for this upload")
	})
}

// TestImportStatusDecoding is the regression guard for the single highest-risk detail in this
// connector: results is a NESTED object on the import status response, not a flat one.
//
// A flat struct compiles, unmarshals without error and reports errored_count as zero forever, so
// every partially errored import would be reported as a clean success - exactly the bug
// reconciliation exists to prevent. The flat payload below is what that mistake would look like on
// the wire, and its counters MUST stay at zero.
func TestImportStatusDecoding(t *testing.T) {
	t.Parallel()

	const nestedPayload = `{
		"id": "sg-import-1",
		"status": "errored",
		"job_type": "upsert",
		"results": {
			"requested_count": 5,
			"created_count": 2,
			"updated_count": 1,
			"deleted_count": 0,
			"errored_count": 2,
			"errors_url": "https://api.sendgrid.com/v3/marketing/contacts/imports/sg-import-1/errors"
		},
		"started_at": "2026-02-25T12:00:00Z",
		"finished_at": "2026-02-25T12:00:30Z"
	}`

	t.Run("the nested results object supplies the counters and the errors document", func(t *testing.T) {
		t.Parallel()

		var status sendgridbulkupload.ImportStatusResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(nestedPayload), &status))

		require.Equal(t, "sg-import-1", status.ID)
		require.Equal(t, "errored", status.Status)
		require.Equal(t, "upsert", status.JobType)
		require.Equal(t, 5, status.Results.RequestedCount)
		require.Equal(t, 2, status.Results.CreatedCount)
		require.Equal(t, 1, status.Results.UpdatedCount)
		require.Equal(t, 0, status.Results.DeletedCount)
		require.Equal(t, 2, status.Results.ErroredCount)
		require.Equal(t, "https://api.sendgrid.com/v3/marketing/contacts/imports/sg-import-1/errors", status.Results.ErrorsURL)
		require.Equal(t, "2026-02-25T12:00:00Z", status.StartedAt)
		require.Equal(t, "2026-02-25T12:00:30Z", status.FinishedAt)
	})

	t.Run("a flat payload leaves the counters at zero", func(t *testing.T) {
		t.Parallel()

		var status sendgridbulkupload.ImportStatusResponse
		require.NoError(t, jsonrs.Unmarshal(
			[]byte(`{"id":"sg-import-1","status":"errored","errored_count":7,"errors_url":"https://api.sendgrid.com/x"}`),
			&status))

		require.Zero(t, status.Results.ErroredCount,
			"a flat errored_count must NOT be readable, or the nesting could silently regress")
		require.Empty(t, status.Results.ErrorsURL)
	})

	t.Run("a decoded status drives the poll response", func(t *testing.T) {
		t.Parallel()

		var status sendgridbulkupload.ImportStatusResponse
		require.NoError(t, jsonrs.Unmarshal([]byte(nestedPayload), &status))

		api := newMockAPI(t)
		api.EXPECT().GetImportStatus("sg-import-1").Times(1).Return(&status, nil)

		response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-import-1", ImportCount: 5})
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.True(t, response.Complete)
		require.True(t, response.HasFailed)
		require.Contains(t, response.FailedJobParameters, status.Results.ErrorsURL,
			"the URL SendGrid published must travel verbatim to reconciliation")
	})

	t.Run("every documented status value decodes", func(t *testing.T) {
		t.Parallel()

		// The documented enumeration is exactly these four. There is no processing or in_progress
		// value, and pending is the only non-terminal one.
		for _, documented := range []string{"pending", "completed", "errored", "failed"} {
			var status sendgridbulkupload.ImportStatusResponse
			require.NoError(t, jsonrs.Unmarshal(
				[]byte(fmt.Sprintf(`{"status":%q,"results":{"errored_count":0}}`, documented)), &status))
			require.Equal(t, documented, status.Status)
		}
	})
}

// ---------------------------------------------------------------------------------------------
// Effective-settings coverage.
//
// Three of this connector's settings are operator-supplied, unbounded, and consumed somewhere a
// bad value cannot be recovered from: the staging-file scan buffer, the errors-document read
// budget, and the number of imports one upload may create. Each is therefore CLAMPED rather than
// honored verbatim, which means the value in force can legitimately differ from the value that was
// configured - so each is also PUBLISHED. The cases below assert the clamp and the publication
// together, because a clamp nobody can observe is indistinguishable from a setting being ignored.
// ---------------------------------------------------------------------------------------------

// statLabels is the tag set every stat this connector publishes carries. destType is sourced from
// the registered destination-definition name rather than from the destination's own name, so an
// operator renaming a destination cannot move its metrics.
func statLabels() stats.Tags {
	return stats.Tags{
		"module":   "batch_router",
		"destType": "SENDGRID_BULK_UPLOAD",
		"destID":   testDestinationID,
	}
}

// gaugeValue reads one published gauge, failing the case if it was never published at all.
func gaugeValue(t *testing.T, store *memstats.Store, name string) float64 {
	t.Helper()
	measurement := store.Get(name, statLabels())
	require.NotNilf(t, measurement, "the %q gauge was never published", name)
	return measurement.LastValue()
}

// setBatchRouterConfig applies a destination-scoped batch router setting for one case.
//
// config.Set mutates a process-wide singleton, so every caller of this helper is SEQUENTIAL - never
// t.Parallel() - and restores the defaults through t.Cleanup. Go runs the sequential top-level
// tests to completion before it resumes any parallel one, so a setting applied here can never be
// observed by a parallel case elsewhere in this suite, under -shuffle=on or otherwise.
func setBatchRouterConfig(t *testing.T, key string, value any) {
	t.Helper()
	t.Cleanup(config.Reset)
	config.Set("BatchRouter.SENDGRID_BULK_UPLOAD."+key, value)
}

// TestStagingFileBufferCapacity pins the scan buffer the staging file is read with.
//
// The buffer is the one setting a bad value cannot be recovered from: bufio.Scanner cannot read a
// line longer than its buffer and reports a bare "token too long", so a value too small turns every
// batch carrying a large contact into an unexplained retry loop, while an unbounded one lets a
// single malformed line allocate without limit inside a shared batch router worker. Both ends are
// therefore closed, and the effective value is published rather than merely applied.
func TestStagingFileBufferCapacity(t *testing.T) {
	t.Parallel()

	const (
		// 6MB of request plus 2MB of headroom for everything a staging record wraps a contact in.
		defaultCapacity = 8_000_000
		// bufio's own default, below which the connector would read fewer records than bufio would.
		floorCapacity = 65536
		// Four times the default: enough for any record the per-record isolation could classify.
		ceilingCapacity = 32_000_000
	)

	t.Run("the effective capacity is clamped and published", func(t *testing.T) {
		t.Parallel()

		for name, testCase := range map[string]struct {
			configured int
			expected   int
		}{
			"unset falls back to the default":    {configured: 0, expected: defaultCapacity},
			"negative falls back to the default": {configured: -1, expected: defaultCapacity},
			"below the floor is raised":          {configured: 1, expected: floorCapacity},
			"at the floor is kept":               {configured: floorCapacity, expected: floorCapacity},
			"in range is kept":                   {configured: 1_048_576, expected: 1_048_576},
			"at the ceiling is kept":             {configured: ceilingCapacity, expected: ceilingCapacity},
			"above the ceiling is clamped":       {configured: 512_000_000, expected: ceilingCapacity},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				store, err := memstats.New()
				require.NoError(t, err)

				api := newMockAPI(t)
				api.EXPECT().UploadContacts(gomock.Any()).Times(1).
					Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)

				uploader := newUploader(api)
				uploader.StatsFactory = store
				uploader.MaxBufferCapacity = testCase.configured
				output := uploader.Upload(&common.AsyncDestinationStruct{
					FileName:        writeStagingFile(t, contactLine(t, 1)),
					ImportingJobIDs: []int64{1},
					Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
				})

				// Whatever the configuration said, an ordinary staging file is still read.
				require.Equal(t, []int64{1}, output.ImportingJobIDs)
				require.EqualValues(t, testCase.expected,
					gaugeValue(t, store, "staging_file_buffer_capacity_bytes"))
			})
		}
	})

	t.Run("a record larger than the effective capacity is retried, not silently dropped", func(t *testing.T) {
		t.Parallel()

		// Clamped up to the floor, so the line below is comfortably beyond it. A record the reader
		// cannot take must fail the read - the batch is then retried - rather than being skipped,
		// because a skipped record is a contact that is never delivered and never reported.
		oversized := stagingLine(t, 1, fmt.Sprintf(
			`{"type":"identify","userId":"user_1","traits":{"email":"user1@example.com","bio":%q}}`,
			strings.Repeat("x", 200_000)))

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(api)
		uploader.MaxBufferCapacity = 1
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, oversized),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.Contains(t, output.FailedReason, "Error in reading staging file")
		require.Empty(t, output.AbortJobIDs, "an unread file says nothing permanent about the contacts in it")
		require.Empty(t, output.ImportingJobIDs)
	})

	t.Run("the same record is read when the capacity allows it", func(t *testing.T) {
		t.Parallel()

		// The other side of the same boundary: the record is not intrinsically unreadable, it was
		// only larger than a capacity an operator had shrunk.
		oversized := stagingLine(t, 1, fmt.Sprintf(
			`{"type":"identify","userId":"user_1","traits":{"email":"user1@example.com","bio":%q}}`,
			strings.Repeat("x", 200_000)))

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, oversized),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
	})
}

// TestErrorsDocumentReadBudget pins the validation applied to the largest allocation this connector
// can be asked to make. A non-positive budget would make every document unreadable and an unbounded
// one would let a single provider response exhaust a shared worker, so the resolved value is clamped
// and published.
//
// Sequential, because it configures the process-wide config singleton.
func TestErrorsDocumentReadBudget(t *testing.T) {
	const (
		defaultBudget = 32 * 1024 * 1024
		floorBudget   = 64 * 1024
		ceilingBudget = 128 * 1024 * 1024
	)

	for name, testCase := range map[string]struct {
		configured any
		expected   int64
	}{
		"unset falls back to the default":    {configured: nil, expected: defaultBudget},
		"zero falls back to the default":     {configured: 0, expected: defaultBudget},
		"negative falls back to the default": {configured: -4096, expected: defaultBudget},
		"below the floor is raised":          {configured: 1024, expected: floorBudget},
		"at the floor is kept":               {configured: floorBudget, expected: floorBudget},
		"in range is kept":                   {configured: 1_048_576, expected: 1_048_576},
		"at the ceiling is kept":             {configured: ceilingBudget, expected: ceilingBudget},
		"above the ceiling is clamped":       {configured: 4 * ceilingBudget, expected: ceilingBudget},
	} {
		t.Run(name, func(t *testing.T) {
			if testCase.configured != nil {
				setBatchRouterConfig(t, "maxErrorsDocumentBytes", testCase.configured)
			}

			store, err := memstats.New()
			require.NoError(t, err)

			// The REAL adapter, not the mock: the budget is the adapter's own setting, and
			// construction publishes it without a packet leaving the process.
			api, err := sendgridbulkupload.NewSendGridAPIService(
				testDestinationID, testConfig(), logger.NOP, store)
			require.NoError(t, err)
			require.NotNil(t, api)

			require.EqualValues(t, testCase.expected,
				gaugeValue(t, store, "errors_document_read_budget_bytes"))

			// Published alongside it, so an operator can see how many hosts the errors document may
			// be fetched from. The host policy is default-DENY, so this is never zero: an
			// unconfigured destination still enforces SendGrid's own two domains, and a zero here
			// would mean the enumeration that bounds this request had been lost.
			require.EqualValues(t, 2, gaugeValue(t, store, "errors_document_allowed_host_count"),
				"the default host allow list is sendgrid.com and sendgrid.net, never empty")
		})
	}
}

// TestNewSendGridAPIServiceRejectsAnUnusableCredential proves the credential guard lives in the one
// place that owns the bearer credential, so a misconfigured destination fails at construction rather
// than at its first upload with an opaque 401 - once, instead of once per batch forever.
func TestNewSendGridAPIServiceRejectsAnUnusableCredential(t *testing.T) {
	t.Parallel()

	for name, apiKey := range map[string]string{
		"absent":          "",
		"whitespace only": " \t \n ",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			api, err := sendgridbulkupload.NewSendGridAPIService(
				testDestinationID,
				sendgridbulkupload.DestinationConfig{APIKey: apiKey},
				logger.NOP, stats.NOP)

			require.Error(t, err)
			require.Nil(t, api, "a nil manager and an error, never a half-configured adapter")
			require.Contains(t, err.Error(), "apiKey")
		})
	}

	t.Run("a credential carrying surrounding whitespace is accepted after trimming", func(t *testing.T) {
		t.Parallel()

		// A key pasted into a configuration form very often carries whitespace, and a bearer header
		// built from such a value is rejected by SendGrid on every single request.
		api, err := sendgridbulkupload.NewSendGridAPIService(
			testDestinationID,
			sendgridbulkupload.DestinationConfig{APIKey: "  SG.test-api-key\n"},
			logger.NOP, stats.NOP)

		require.NoError(t, err)
		require.NotNil(t, api)
	})

	t.Run("a nil logger and stats factory are defaulted rather than trusted", func(t *testing.T) {
		t.Parallel()

		// A caller that has not wired observability up yet must not be able to turn a delivery
		// failure into a nil-pointer panic inside a router worker.
		api, err := sendgridbulkupload.NewSendGridAPIService(
			testDestinationID, sendgridbulkupload.DestinationConfig{APIKey: "SG.k"}, nil, nil)

		require.NoError(t, err)
		require.NotNil(t, api)
	})
}

// TestNewManagerValidatesTheCustomFieldsMapping pins the configuration that must fail construction
// rather than be applied.
//
// The duplicate-field-ID case is the load-bearing one, and it is the worst available failure mode:
// two traits mapped to one SendGrid custom field ID both look perfectly healthy, still deliver data,
// and - because Go randomizes map iteration order - deliver DIFFERENT data on every run.
func TestNewManagerValidatesTheCustomFieldsMapping(t *testing.T) {
	t.Parallel()

	t.Run("rejected mappings", func(t *testing.T) {
		t.Parallel()

		for name, testCase := range map[string]struct {
			mapping  map[string]any
			contains []string
		}{
			"a blank trait name": {
				mapping:  map[string]any{"  ": "w1"},
				contains: []string{"trait name is empty"},
			},
			"a blank custom field id": {
				mapping:  map[string]any{"plan": "   "},
				contains: []string{`trait "plan"`, "empty custom field id"},
			},
			"two traits claiming one custom field id": {
				mapping: map[string]any{"plan": "w1", "tier": "w1"},
				// BOTH traits are named, because that is the only form of the message an operator
				// can act on without going back to the configuration to work out what collided.
				contains: []string{`"plan"`, `"tier"`, `"w1"`, "ambiguous"},
			},
			"one trait spelled twice through whitespace": {
				mapping:  map[string]any{"plan": "w1", " plan": "w2"},
				contains: []string{`trait "plan"`, "mapped more than once"},
			},
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP,
					testDestination(map[string]any{"apiKey": "SG.k", "customFieldsMapping": testCase.mapping}))

				require.Error(t, err)
				require.Nil(t, manager)
				for _, fragment := range testCase.contains {
					require.Contains(t, err.Error(), fragment)
				}
			})
		}
	})

	t.Run("an accepted mapping is normalized once, at construction", func(t *testing.T) {
		t.Parallel()

		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP,
			testDestination(map[string]any{
				"apiKey": "SG.k",
				"customFieldsMapping": map[string]any{
					"  plan  ":   "  w1  ",
					"signedUpAt": "w2",
				},
			}))

		require.NoError(t, err)
		require.NotNil(t, manager)
		require.Equal(t, map[string]string{"plan": "w1", "signedUpAt": "w2"},
			manager.DestinationConfig.CustomFieldsMapping,
			"every later read must work from trimmed values without per-entry defensiveness")
	})

	t.Run("an absent mapping is valid and never nil", func(t *testing.T) {
		t.Parallel()

		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP,
			testDestination(map[string]any{"apiKey": "SG.k"}))

		require.NoError(t, err)
		require.NotNil(t, manager)
		require.NotNil(t, manager.DestinationConfig.CustomFieldsMapping,
			"custom fields are optional, and the resolved map must still be safe to range over")
		require.Empty(t, manager.DestinationConfig.CustomFieldsMapping)
	})
}

// TestNewManagerWrapsTheConfigurationFailure pins that the configuration round trip wraps the
// codec's own error with %w rather than rendering it with %v.
//
// A wrapped error stays inspectable with errors.Is and errors.As all the way up to the factory; a
// rendered one is flattened into a string at the first hop and can never be branched on again.
func TestNewManagerWrapsTheConfigurationFailure(t *testing.T) {
	t.Parallel()

	// A channel cannot be marshalled, so this reaches the marshal error branch specifically.
	manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP,
		testDestination(map[string]any{"apiKey": "SG.k", "listIds": make(chan int)}))

	require.Error(t, err)
	require.Nil(t, manager)
	require.Contains(t, err.Error(), "destination config")
	require.NotNil(t, errors.Unwrap(err),
		"the codec's own error must stay inspectable rather than being flattened into a string")
}

// TestUploadRejectsListIDsThatConsumeTheWholeRequest pins the one case in which no contact can be
// uploaded at all and yet nothing is wrong with any contact.
//
// SendGrid's byte ceiling applies to the WHOLE request body, and the list IDs are part of it. If
// they leave no room for even one contact, reporting the batch as contacts that are individually too
// large would name the wrong cause, and a retry would rebuild exactly the same envelope - so the
// group is abandoned terminally with the real reason.
func TestUploadRejectsListIDsThatConsumeTheWholeRequest(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(0)

	uploader := newUploader(api)
	// Smaller than the envelope one list ID alone produces, so the contact budget is exhausted
	// before any contact is measured.
	uploader.MaxRequestBytes = 50
	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        writeStagingFile(t, contactLine(t, 1), contactLine(t, 2)),
		ImportingJobIDs: []int64{1, 2},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.ElementsMatch(t, []int64{1, 2}, output.AbortJobIDs)
	require.Contains(t, output.AbortReason, "consume the entire request budget")
	require.Empty(t, output.FailedJobIDs, "a retry would rebuild exactly the same envelope")
	require.Empty(t, output.ImportingJobIDs)
	require.Nil(t, output.ImportingParameters)
}

// TestUploadBoundsTheImportsOneBatchMayCreate pins the import budget.
//
// Every later poll of an upload has to ask about each import it created, so an unbounded number of
// imports here hands an unbounded cost to a poll loop that is shared by every destination of this
// type. The surplus chunks are DEFERRED - reported retryable, so the framework re-queues them into
// the next batch - rather than being sent and forgotten.
func TestUploadBoundsTheImportsOneBatchMayCreate(t *testing.T) {
	t.Parallel()

	t.Run("the surplus chunks are deferred to the next batch", func(t *testing.T) {
		t.Parallel()

		store, err := memstats.New()
		require.NoError(t, err)

		api := newMockAPI(t)
		// Exactly the budget, and not one request more: a deferred chunk is never sent.
		requests := 0
		api.EXPECT().UploadContacts(gomock.Any()).Times(2).DoAndReturn(
			func(sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				requests++
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-" + strconv.Itoa(requests)}, nil
			})

		uploader := newUploader(api)
		uploader.StatsFactory = store
		// One contact per request turns five staged events into five chunks, of which two fit.
		uploader.MaxContactsPerRequest = 1
		uploader.MaxImportsPerUpload = 2
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName: writeStagingFile(t,
				contactLine(t, 1), contactLine(t, 2), contactLine(t, 3),
				contactLine(t, 4), contactLine(t, 5)),
			ImportingJobIDs: []int64{1, 2, 3, 4, 5},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1, 2}, output.ImportingJobIDs)
		require.ElementsMatch(t, []int64{3, 4, 5}, output.FailedJobIDs,
			"the surplus is retryable: those contacts were never sent")
		require.Contains(t, output.FailedReason, "more than the 2 sendgrid imports one upload may create")
		require.Contains(t, output.FailedReason, "3 job(s) were deferred")
		require.Empty(t, output.AbortJobIDs, "a deferred contact has had nothing decided about it")

		// The accepted chunks keep their importing state, so the two outcome sets stay disjoint and
		// the deferral costs the accepted contacts nothing.
		_, importCount := importParametersOf(t, output)
		require.EqualValues(t, 2, importCount)

		measurement := store.Get("deferred_job_count", statLabels())
		require.NotNil(t, measurement, "a deferral an operator cannot see is a deferral nobody acts on")
		require.EqualValues(t, 3, measurement.LastValue())
	})

	t.Run("an absurd override cannot buy an unbounded number of imports", func(t *testing.T) {
		t.Parallel()

		// An operator asking for an unbounded poll cost cannot be given one, and TWO independent
		// bounds refuse: the import-count clamp and the manifest byte budget. Whichever is tighter
		// is what binds, and for any realistic identifier that is the byte budget - 512 identifiers
		// could only fit 1024 bytes if they averaged a single byte each, which distinct identifiers
		// cannot. So this case asserts the guarantee Upload actually makes, rather than asserting a
		// number only one of the two bounds would produce. The clamp itself is pinned directly, on
		// the accessor that applies it, by TestImportBudgetOverrideIsClamped.
		const jobCount = 513
		const ceiling = 512

		lines := make([]string, 0, jobCount)
		jobIDs := make([]int64, 0, jobCount)
		for jobID := int64(1); jobID <= jobCount; jobID++ {
			lines = append(lines, contactLine(t, jobID))
			jobIDs = append(jobIDs, jobID)
		}

		api := newMockAPI(t)
		accepted := 0
		api.EXPECT().UploadContacts(gomock.Any()).AnyTimes().DoAndReturn(
			func(sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				accepted++
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-" + strconv.Itoa(accepted)}, nil
			})

		uploader := newUploader(api)
		uploader.MaxContactsPerRequest = 1
		uploader.MaxImportsPerUpload = 10_000_000
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, lines...),
			ImportingJobIDs: jobIDs,
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		importID, importCount := importParametersOf(t, output)

		// Bounded by both, and by the tighter of them.
		require.LessOrEqual(t, int(importCount), ceiling, "the import-count clamp is never exceeded")
		require.LessOrEqual(t, len(importID), persistedManifestBudget, "the manifest budget is never exceeded")
		require.Less(t, int(importCount), jobCount, "an override cannot make every chunk its own import")

		// The surplus is retryable and every job is accounted for exactly once. This is the part
		// that must hold no matter which bound binds: a bound that dropped jobs instead of
		// deferring them would be a silent loss, not a limit.
		require.Empty(t, output.AbortJobIDs, "a deferred contact has had nothing decided about it")
		require.ElementsMatch(t, jobIDs, append(append([]int64{}, output.ImportingJobIDs...), output.FailedJobIDs...))
		require.Len(t, output.ImportingJobIDs, int(importCount))
		require.Contains(t, output.FailedReason, "deferred to the next batch")

		// The chunks beyond the binding limit are not sent at all, bar the single one that
		// discovered the limit, so an oversized batch costs the provider nothing extra.
		require.LessOrEqual(t, accepted, int(importCount)+1)
	})

	t.Run("a batch inside the budget defers nothing", func(t *testing.T) {
		t.Parallel()

		store, err := memstats.New()
		require.NoError(t, err)

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)

		uploader := newUploader(api)
		uploader.StatsFactory = store
		output := uploader.Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1), contactLine(t, 2)),
			ImportingJobIDs: []int64{1, 2},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1, 2}, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
		require.Nil(t, store.Get("deferred_job_count", statLabels()),
			"the counter must stay silent when nothing was deferred")
	})
}

// TestPollStopsAtTheFirstPendingImport pins the bound on what one poll costs.
//
// A pending import already decides the whole poll - the upload is in progress until every one of its
// imports has settled, and the response writes no job status at all - so asking about the remaining
// imports could only produce answers that are discarded. The poll loop is shared by every
// destination of this type and runs them one after another, so a poll that lingers delays all of
// them.
func TestPollStopsAtTheFirstPendingImport(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "pending", 0, ""), nil)
	api.EXPECT().GetImportStatus("sg-b").Times(0)
	api.EXPECT().GetImportStatus("sg-c").Times(0)

	response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4;sg-c=5-6", ImportCount: 6})

	require.Equal(t, http.StatusOK, response.StatusCode)
	require.True(t, response.InProgress)
	require.False(t, response.Complete)
	require.False(t, response.HasFailed)
	require.Empty(t, response.FailedJobParameters,
		"nothing may be forwarded from a poll that settled nothing")
}

// TestPollAsksAboutEverySettledImportExactlyOnce is the other half of the same bound: once no import
// is pending, each one is read once and only once, however many chunks were answered with the same
// import job id.
func TestPollAsksAboutEverySettledImportExactlyOnce(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().GetImportStatus("sg-a").Times(1).Return(pollStatus("sg-a", "completed", 0, ""), nil)
	api.EXPECT().GetImportStatus("sg-b").Times(1).Return(pollStatus("sg-b", "errored", 1, testErrorsURL), nil)

	// The same import named twice: SendGrid is free to answer two chunks with one job id, and the
	// manifest's own encoding must not turn that into two provider requests.
	response := newUploader(api).Poll(common.AsyncPoll{ImportId: "sg-a=1-2;sg-b=3-4;sg-b=5", ImportCount: 5})

	require.Equal(t, http.StatusOK, response.StatusCode)
	require.True(t, response.Complete)
	require.True(t, response.HasFailed)

	// One document, forwarded once, even though two chunks pointed at the same import.
	require.Equal(t, 1, strings.Count(response.FailedJobParameters, testErrorsURL))
}

// ---------------------------------------------------------------------------------------------
// The observability contract.
// ---------------------------------------------------------------------------------------------

// providerText enumerates every string the PROVIDER chose that must never appear in a log line: the
// contacts that were submitted, the credential, SendGrid's own prose, and a status value SendGrid
// might introduce tomorrow. A log stream is broadcast more widely, retained longer, and read by more
// people than the job statuses are, so the provider's words travel on the channels that exist for
// them instead - which this test also asserts they arrive on.
func providerText() []string {
	return []string{
		"alex@example.com", "blake@example.com", "casey@example.com",
		"devon@example.com", "erin@example.com", "ghost@example.com",
		"SG.test-api-key",
		"Invalid email address provided for contact",
		"a-state-sendgrid-has-not-published-yet",
	}
}

// captureLogs builds a real JSON logger writing into the case's own temporary directory and returns
// a reader for the entries it produced.
//
// A capturing logger rather than logger.NOP, because the properties under test are properties of the
// log OUTPUT: that every line carries the destination's identity, and that no line carries a word the
// provider chose. Neither can be asserted against a logger that discards everything. The logger is
// built on a PRIVATE config instance, so this never touches the process-wide singleton.
func captureLogs(t *testing.T) (logger.Logger, func() []map[string]any) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sendgrid.log")

	conf := config.New()
	conf.Set("LOG_LEVEL", "DEBUG")
	conf.Set("Logger.enableConsole", false)
	conf.Set("Logger.enableFile", true)
	conf.Set("Logger.fileJsonFormat", true)
	conf.Set("Logger.logFileLocation", path)

	factory := logger.NewFactory(conf)
	t.Cleanup(factory.Sync)

	return factory.NewLogger(), func() []map[string]any {
		factory.Sync()
		raw, err := os.ReadFile(path)
		require.NoError(t, err)

		entries := make([]map[string]any, 0)
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var entry map[string]any
			require.NoErrorf(t, jsonrs.Unmarshal([]byte(line), &entry), "log line is not JSON: %s", line)
			entries = append(entries, entry)
		}
		return entries
	}
}

// TestObservabilityAttributionAndProviderText drives every failure path the connector logs from and
// then asserts three properties over the ACTUAL log output.
//
// Attribution is bound to the logger ONCE at construction rather than passed at each call site, so
// every line - including the ones written from helpers that never receive a destination, and the
// ones the HTTP adapter writes - must carry destinationId and destinationType. Provider text must
// appear nowhere. And failure logging has a single owner, so one rate-limited chunk yields exactly
// one warning rather than one from the adapter and another from the manager.
func TestObservabilityAttributionAndProviderText(t *testing.T) {
	t.Parallel()

	const (
		unknownStatus  = "a-state-sendgrid-has-not-published-yet"
		providerReason = "Invalid email address provided for contact: alex@example.com at 123 Main St"
	)

	log, readLogs := captureLogs(t)
	store, err := memstats.New()
	require.NoError(t, err)

	uploader, err := sendgridbulkupload.NewManager(log, store,
		testDestination(map[string]any{"apiKey": "SG.test-api-key", "listIds": []any{fixtureListID}}))
	require.NoError(t, err)

	api := newMockAPI(t)
	uploader.SendGridAPIService = api
	// Two contacts per request turns five staged events into three chunks, so one upload can
	// exercise a rate limit, a provider rejection and an acceptance at once.
	uploader.MaxContactsPerRequest = 2

	gomock.InOrder(
		api.EXPECT().UploadContacts(gomock.Any()).Return(nil, &sendgridbulkupload.RateLimitError{
			StatusCode: http.StatusTooManyRequests, RetryAfter: "60",
			ResetAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
			Limit:   600, Remaining: 0, Message: providerReason,
		}),
		api.EXPECT().UploadContacts(gomock.Any()).Return(nil, &sendgridbulkupload.APIError{
			StatusCode: http.StatusForbidden, Operation: "upload contacts", Message: providerReason,
		}),
		api.EXPECT().UploadContacts(gomock.Any()).
			Return(&sendgridbulkupload.UpsertResponse{JobID: "sg-accepted"}, nil),
	)

	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        stagingFixture,
		ImportingJobIDs: []int64{1, 2, 3, 4, 5},
		Destination: testDestination(map[string]any{
			"apiKey": "SG.test-api-key", "listIds": []any{fixtureListID},
		}),
	})
	require.NotEmpty(t, output.FailedJobIDs)
	require.Empty(t, output.AbortJobIDs)
	require.Contains(t, output.FailedReason, "rate limited",
		"the provider's explanation belongs on the affected jobs")

	// A state this connector has not been taught: retried, never guessed at - and the raw value
	// still has to reach the operator even though it may not reach the log.
	api.EXPECT().GetImportStatus("sg-accepted").Times(1).Return(
		&sendgridbulkupload.ImportStatusResponse{ID: "sg-accepted", Status: unknownStatus}, nil)
	poll := uploader.Poll(common.AsyncPoll{ImportId: "sg-accepted", ImportCount: 5})
	require.Equal(t, http.StatusInternalServerError, poll.StatusCode)
	require.Contains(t, poll.Error, unknownStatus)

	// An unparseable document: a decoder reports the fragment it choked on, and that fragment is
	// part of a document quoting rejected contacts.
	api.EXPECT().GetImportErrors(testErrorsURL).Times(1).
		Return([]byte("{not json at all: "+providerReason), nil)
	require.Equal(t, http.StatusInternalServerError, uploader.GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: testErrorsURL,
		ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
	}).StatusCode)

	// A reconciliation carrying one matched row and one that names a contact this upload never
	// staged, so both the per-row path and the unattributable-row accounting are exercised. The
	// unattributable row fails the import closed, and the matched job still keeps the provider's
	// own explanation - sanitized - rather than the general reason.
	secondErrorsURL := testErrorsURL + "-2"
	api.EXPECT().GetImportErrors(secondErrorsURL).Times(1).Return([]byte(fmt.Sprintf(
		`{"errors":[{"email":"blake@example.com","message":%q},{"email":"ghost@example.com","reason":%q}]}`,
		providerReason, providerReason)), nil)
	reconciled := uploader.GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: secondErrorsURL,
		ImportingList:       fixtureJobs(t, 1, 2, 3, 4, 5),
	})
	require.Equal(t, http.StatusOK, reconciled.StatusCode)
	require.ElementsMatch(t, []int64{1, 2, 3, 4, 5}, reconciled.Metadata.FailedKeys)
	require.Empty(t, reconciled.Metadata.SucceededKeys)
	require.Contains(t, reconciled.Metadata.FailedReasons[2], "Invalid email address",
		"the provider's explanation belongs on the affected job")
	require.NotContains(t, reconciled.Metadata.FailedReasons[2], "alex@example.com",
		"and it is redacted even there, because a reason is persisted and read widely too")
	require.Contains(t, reconciled.Metadata.FailedReasons[1], "could not attribute",
		"a job no row named is retried with this connector's own reason")
	for _, text := range providerText() {
		require.NotContains(t, reconciled.Metadata.FailedReasons[1], text,
			"the general reason carries no provider text at all")
	}

	// The logger held by the manager is the very instance handed to the HTTP adapter at
	// construction, so proving it carries the attribution proves the adapter's own lines do too.
	uploader.Logger.Infon("[sendgrid bulk upload] bound logger probe")

	entries := readLogs()
	require.NotEmpty(t, entries)

	for _, entry := range entries {
		rendered, err := jsonrs.Marshal(entry)
		require.NoError(t, err)
		line := string(rendered)

		require.Equalf(t, testDestinationID, entry["destinationId"],
			"a log line carries no destinationId: %s", line)
		require.Equalf(t, "SENDGRID_BULK_UPLOAD", entry["destinationType"],
			"a log line carries no destinationType: %s", line)

		for _, text := range providerText() {
			require.NotContainsf(t, line, text, "provider text %q reached a log line: %s", text, line)
		}
	}

	// Exactly one rate-limit warning, and this connector's own label - never the provider's value -
	// for a status it has not been taught.
	rateLimitWarnings, unrecognizedStatusLogged := 0, false
	var uploadSummary map[string]any
	for _, entry := range entries {
		message := fmt.Sprint(entry["msg"])
		switch {
		case fmt.Sprint(entry["level"]) == "WARN" && strings.Contains(message, "rate limited while uploading contacts"):
			rateLimitWarnings++
		case strings.Contains(message, "upload finished"):
			uploadSummary = entry
		case strings.Contains(message, "unrecognized import status"):
			require.Equal(t, "unrecognized", entry["status"],
				"a provider-chosen value must neither enter the log nor give a metric tag unbounded cardinality")
			unrecognizedStatusLogged = true
		}
	}
	require.Equal(t, 1, rateLimitWarnings, "failure logging has exactly one owner")
	require.True(t, unrecognizedStatusLogged)

	// Attempted requests and accepted imports answer different questions, and conflating them hides
	// exactly the case an operator most needs to see: a chunk rejected with 429 or 5xx never becomes
	// an import and would simply vanish from a count derived from the accepted ones.
	require.NotNil(t, uploadSummary)
	require.EqualValues(t, 3, uploadSummary["requestCount"])
	require.EqualValues(t, 1, uploadSummary["acceptedRequestCount"])

	// The conditions worth alerting on are counters, so none of the above depends on log inspection.
	require.NotNil(t, store.Get("unrecognized_import_status_count", statLabels()))
	require.NotNil(t, store.Get("unmatched_error_row_count", statLabels()))
}

// TestUploadCustomFieldWithADottedTraitName pins that a trait is looked up by its exact NAME.
//
// gjson reads a dot as a path separator, so a trait genuinely called "plan.tier" would silently
// resolve to the tier field of a nested plan object - a different value, from a different place,
// with nothing to indicate the substitution. Exact keys are therefore consulted first, and only a
// name that matches no key at all is treated as a path.
func TestUploadCustomFieldWithADottedTraitName(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			require.Len(t, request.Contacts, 1)
			require.Equal(t, map[string]any{"w1": "gold"}, request.Contacts[0].CustomFields,
				"the exact key must win over a same-named nested path")
			return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
		})

	uploader := newUploader(api)
	uploader.DestinationConfig = sendgridbulkupload.DestinationConfig{
		APIKey:              "SG.test-api-key",
		CustomFieldsMapping: map[string]string{"plan.tier": "w1"},
	}
	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName: writeStagingFile(t, stagingLine(t, 1,
			`{"type":"identify","userId":"user_1","traits":{"email":"casey@example.com",`+
				`"plan.tier":"gold","plan":{"tier":"bronze"}}}`)),
		ImportingJobIDs: []int64{1},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.Equal(t, []int64{1}, output.ImportingJobIDs)
}

// TestUploadChunkJobIDsStayAlignedWithTheirContacts pins the invariant the whole chunking design
// rests on: chunk N's job IDs are exactly the jobs whose contacts travelled in chunk N.
//
// Misalignment by even one position would attribute an import's contacts to the wrong jobs, and
// every later poll and reconciliation would then settle the wrong ones - a failure that no
// individual outcome assertion would notice, because all the right job IDs would still be present
// somewhere. The membership the manifest persists is what makes the alignment observable.
func TestUploadChunkJobIDsStayAlignedWithTheirContacts(t *testing.T) {
	t.Parallel()

	const jobCount = 9

	lines := make([]string, 0, jobCount)
	jobIDs := make([]int64, 0, jobCount)
	for jobID := int64(1); jobID <= jobCount; jobID++ {
		lines = append(lines, contactLine(t, jobID))
		jobIDs = append(jobIDs, jobID)
	}

	// The import id each chunk is answered with names the emails it carried, so the membership the
	// manifest records can be checked against the contacts that were actually sent.
	api := newMockAPI(t)
	chunkIndex := 0
	sentEmails := make(map[string][]string)
	api.EXPECT().UploadContacts(gomock.Any()).Times(3).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			chunkIndex++
			importID := "sg-" + strconv.Itoa(chunkIndex)
			for _, contact := range request.Contacts {
				sentEmails[importID] = append(sentEmails[importID], contact.Email)
			}
			return &sendgridbulkupload.UpsertResponse{JobID: importID}, nil
		})

	uploader := newUploader(api)
	uploader.MaxContactsPerRequest = 3
	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        writeStagingFile(t, lines...),
		ImportingJobIDs: jobIDs,
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})

	require.Equal(t, jobIDs, output.ImportingJobIDs)
	importID, importCount := importParametersOf(t, output)
	require.EqualValues(t, jobCount, importCount)

	// Each import names three consecutive jobs, and the fixture's email for job N is userN@..., so
	// the recorded membership and the contacts that were sent must agree job for job.
	require.Equal(t, "sg-1=1-3;sg-2=4-6;sg-3=7-9", importID)
	require.Equal(t, map[string][]string{
		"sg-1": {"user1@example.com", "user2@example.com", "user3@example.com"},
		"sg-2": {"user4@example.com", "user5@example.com", "user6@example.com"},
		"sg-3": {"user7@example.com", "user8@example.com", "user9@example.com"},
	}, sentEmails)
}

// TestGetUploadStatsReportsARepeatedImportingJobOnce pins that a job listed twice is settled once.
//
// The batch router builds the importing list from job state, and a job appearing twice must not
// produce two status updates for one job: the router would write both, and the second would
// contradict the first. The completeness guarantee is stated over the SET of importing jobs, so it
// has to survive a duplicated entry.
func TestGetUploadStatsReportsARepeatedImportingJobOnce(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
		[]byte(`{"errors":[{"email":"blake@example.com","message":"rejected"}]}`), nil)

	jobs := fixtureJobs(t, 1, 2, 3)
	// Job 2 - the one the document names - and job 3 - one it does not - are both duplicated, so
	// neither the failed nor the succeeded channel can be relying on the list being a set.
	jobs = append(jobs, jobs[1], jobs[2])

	response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: testErrorsURL,
		ImportingList:       jobs,
	})

	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
	require.ElementsMatch(t, []int64{1, 3}, response.Metadata.SucceededKeys)
	assertSettledExactlyOnce(t, response.Metadata, []int64{1, 2, 3})
}

// TestGetUploadStatsReconcilesAFullSizedImport reconciles a whole request's worth of jobs that all
// share ONE contact identifier.
//
// That combination is the worst case for the identifier index, because every job crowds into the
// same entry. The assertions are on the OUTCOME rather than on a duration - a wall-clock threshold
// is not a fact a test can rely on - but what they establish is that the index is built and consumed
// in one pass per job: a construction that re-scanned each identifier's growing job list would
// perform hundreds of millions of comparisons to reach the same verdict.
func TestGetUploadStatsReconcilesAFullSizedImport(t *testing.T) {
	t.Parallel()

	const importedJobs = 20_000

	api := newMockAPI(t)
	api.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(
		[]byte(`[{"email":"crowded@example.com","message":"rejected"}]`), nil)

	importingList := make([]*jobsdb.JobT, 0, importedJobs)
	expected := make([]int64, 0, importedJobs)
	for jobID := int64(1); jobID <= importedJobs; jobID++ {
		importingList = append(importingList, &jobsdb.JobT{
			JobID: jobID,
			EventPayload: []byte(stagingLine(t, jobID, fmt.Sprintf(
				`{"type":"identify","userId":"user_%d","traits":{"email":"crowded@example.com"}}`, jobID))),
		})
		expected = append(expected, jobID)
	}

	response := newUploader(api).GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: testErrorsURL,
		ImportingList:       importingList,
	})

	require.Equal(t, http.StatusOK, response.StatusCode)
	// The rejection refers to every job that claimed the address, so all of them are retried and
	// none is reported delivered. Failing a job that in fact succeeded costs one idempotent
	// re-upsert; reporting a failed contact as delivered loses it.
	require.Equal(t, expected, response.Metadata.FailedKeys)
	require.Len(t, response.Metadata.FailedReasons, importedJobs)
	require.Empty(t, response.Metadata.SucceededKeys)
	require.Empty(t, response.Metadata.AbortedKeys)
	require.Contains(t, response.Metadata.FailedReasons[1],
		fmt.Sprintf("matches %d jobs", importedJobs))
}

// TestUploadRejectsNonScalarFieldValues pins the scalar guard on every reserved contact field.
//
// gjson renders a container as its RAW JSON TEXT rather than failing, so before the guard a trait
// that arrived as an object or an array produced a non-blank string that satisfied every "is it
// set?" check in the connector and was then sent to SendGrid as the field's value. The identifier
// check was the worst of it: an object is never blank, so `{"email":{"work":"a@b.com"}}` passed the
// check that exists to reject a contact SendGrid cannot key on, and the whole request carrying it
// was refused for one malformed field.
func TestUploadRejectsNonScalarFieldValues(t *testing.T) {
	t.Parallel()

	for name, message := range map[string]string{
		"an object where the email belongs": `{"type":"identify","traits":{"email":{"work":"nested@example.com"}}}`,
		"an array where the email belongs":  `{"type":"identify","traits":{"email":["first@example.com"]}}`,
		"an object as the external id":      `{"type":"identify","userId":{"id":"user_9"}}`,
		"an array as the anonymous id":      `{"type":"track","anonymousId":["anon_9"]}`,
		"an object as the phone number":     `{"type":"identify","traits":{"phone":{"mobile":"+15551234567"}}}`,
		"a boolean as the email":            `{"type":"identify","traits":{"email":true}}`,
		"a null email with nothing else":    `{"type":"identify","traits":{"email":null}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// No upload is expected at all: the only record in the file yields no usable contact, so
			// there is nothing to send. gomock fails the test if UploadContacts is called.
			api := newMockAPI(t)
			output := newUploader(api).Upload(&common.AsyncDestinationStruct{
				FileName:        writeStagingFile(t, stagingLine(t, 1, message)),
				ImportingJobIDs: []int64{1},
				Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
			})

			require.Equal(t, []int64{1}, output.AbortJobIDs,
				"a contact with no scalar identifier can never be delivered, so it is terminal")
			require.Contains(t, output.AbortReason, "at least one of email")
			require.Empty(t, output.ImportingJobIDs)
			// And the raw JSON text of the container never reaches the durable metadata.
			require.NotContains(t, output.AbortReason, "{")
			require.NotContains(t, output.AbortReason, "nested@example.com")
		})
	}

	t.Run("a non-scalar field alongside a usable identifier is dropped, not sent", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Len(t, request.Contacts, 1)
				contact := request.Contacts[0]
				// The usable fields survive; the containers are simply absent.
				require.Equal(t, "keep@example.com", contact.Email)
				require.Equal(t, "user_1", contact.ExternalID)
				require.Empty(t, contact.FirstName, "an object is not a first name")
				require.Empty(t, contact.City, "an array is not a city")
				require.Empty(t, contact.AlternateEmails, "a nested object is not an alternate email")
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		message := `{"type":"identify","userId":"user_1","traits":{` +
			`"email":"keep@example.com",` +
			`"firstName":{"given":"Ada"},` +
			`"city":["Springfield"],` +
			`"alternateEmails":[{"work":"work@example.com"}]}}`

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, stagingLine(t, 1, message)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})
		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})

	t.Run("an alternate emails array keeps its scalars and drops its containers", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
			func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				require.Equal(t, []string{"a@example.com", "b@example.com"}, request.Contacts[0].AlternateEmails)
				return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
			})

		message := `{"type":"identify","traits":{"email":"keep@example.com",` +
			`"alternateEmails":["a@example.com",{"x":1},["nested"],null,"b@example.com"]}}`

		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, stagingLine(t, 1, message)),
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})
		require.Equal(t, []int64{1}, output.ImportingJobIDs)
	})
}

// TestUploadRejectsOversizedFieldValues pins the field-length and value-count bounds.
//
// The bounds protect the BATCH, not the record: a field the provider refuses fails the whole request
// that carried it, so one bad contact among 30,000 good ones would take all of them down. Rejecting
// the single record is the cheaper outcome, and it is reported terminally because the same bytes
// produce the same oversized field on every retry.
func TestUploadRejectsOversizedFieldValues(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		traits string
		reason string
	}{
		"an email beyond the deliverable maximum": {
			traits: fmt.Sprintf(`"email":"%s@example.com"`, strings.Repeat("e", 250)),
			reason: "longer than the maximum length",
		},
		"a first name beyond the bound": {
			traits: fmt.Sprintf(`"email":"ok@example.com","firstName":%q`, strings.Repeat("n", 256)),
			reason: "longer than the maximum length",
		},
		"a city beyond the bound": {
			traits: fmt.Sprintf(`"email":"ok@example.com","city":%q`, strings.Repeat("c", 300)),
			reason: "longer than the maximum length",
		},
		"a postal code beyond the bound": {
			traits: fmt.Sprintf(`"email":"ok@example.com","postalCode":%q`, strings.Repeat("9", 101)),
			reason: "longer than the maximum length",
		},
		"an oversized alternate email": {
			traits: fmt.Sprintf(`"email":"ok@example.com","alternateEmails":["%s@example.com"]`, strings.Repeat("a", 250)),
			reason: "longer than the maximum length",
		},
		"more alternate emails than will be sent": {
			traits: `"email":"ok@example.com","alternateEmails":[` + strings.TrimSuffix(strings.Repeat(`"x@example.com",`, 51), ",") + `]`,
			reason: "more values than",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The good record must still be delivered: rejecting the batch instead of the record
			// would be the exact failure these bounds exist to prevent.
			api := newMockAPI(t)
			api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
				func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
					require.Len(t, request.Contacts, 1)
					require.Equal(t, "user1@example.com", request.Contacts[0].Email)
					return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
				})

			oversized := stagingLine(t, 2, fmt.Sprintf(`{"type":"identify","traits":{%s}}`, testCase.traits))
			output := newUploader(api).Upload(&common.AsyncDestinationStruct{
				FileName:        writeStagingFile(t, contactLine(t, 1), oversized),
				ImportingJobIDs: []int64{1, 2},
				Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
			})

			require.Equal(t, []int64{1}, output.ImportingJobIDs, "the good contact is still delivered")
			require.Equal(t, []int64{2}, output.AbortJobIDs)
			require.Contains(t, output.AbortReason, testCase.reason)
			// The offending VALUE is never written into durable job metadata; only the field and
			// the limit are named.
			require.NotContains(t, output.AbortReason, strings.Repeat("e", 40))
			require.NotContains(t, output.AbortReason, strings.Repeat("n", 40))
			require.NotContains(t, output.AbortReason, strings.Repeat("c", 40))
		})
	}
}

// TestUploadRejectsNonPrimitiveCustomFields pins the custom-field value guard.
//
// A SendGrid custom field holds one scalar, so an object or an array is not a value the provider can
// store - and passing one through failed the WHOLE request rather than the one field.
func TestUploadRejectsNonPrimitiveCustomFields(t *testing.T) {
	t.Parallel()

	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(1).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			custom := request.Contacts[0].CustomFields
			// The scalars are stored, each in the kind SendGrid can hold.
			require.Equal(t, "gold", custom["w1"])
			require.EqualValues(t, 42, custom["w2"])
			require.Equal(t, "true", custom["w3"], "a boolean is rendered as text, not sent as a bare boolean")
			// The containers are refused rather than flattened or serialized.
			require.NotContains(t, custom, "w4")
			require.NotContains(t, custom, "w5")
			// And nothing that looks like raw JSON reached the wire.
			for _, value := range custom {
				require.NotContains(t, fmt.Sprintf("%v", value), "{")
				require.NotContains(t, fmt.Sprintf("%v", value), "[")
			}
			return &sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil
		})

	uploader := newUploader(api)
	uploader.DestinationConfig.CustomFieldsMapping = map[string]string{
		"tier": "w1", "score": "w2", "active": "w3", "profile": "w4", "tags": "w5",
	}

	message := `{"type":"identify","traits":{"email":"one@example.com",` +
		`"tier":"gold","score":42,"active":true,` +
		`"profile":{"nested":"value"},"tags":["a","b"]}}`

	output := uploader.Upload(&common.AsyncDestinationStruct{
		FileName:        writeStagingFile(t, stagingLine(t, 1, message)),
		ImportingJobIDs: []int64{1},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})
	require.Equal(t, []int64{1}, output.ImportingJobIDs)
	require.Empty(t, output.AbortJobIDs, "a refused custom field costs the field, never the contact")
}

// TestListIDBatchingKeyCannotCollide pins the collision-free batching key.
//
// The key decides which contacts share a request, and therefore which SendGrid lists they are added
// to. A comma join collides - ["a,b"] and ["a","b"] both render "a,b" - so a contact targeting one
// list whose ID contained a comma was batched with, and DELIVERED TO, the two lists of an unrelated
// contact. That is a data-disclosure bug dressed as a batching optimisation.
func TestListIDBatchingKeyCannotCollide(t *testing.T) {
	t.Parallel()

	requests := make([]sendgridbulkupload.UpsertRequest, 0, 2)
	api := newMockAPI(t)
	api.EXPECT().UploadContacts(gomock.Any()).Times(2).DoAndReturn(
		func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			requests = append(requests, request)
			return &sendgridbulkupload.UpsertResponse{JobID: fmt.Sprintf("sg-%d", len(requests))}, nil
		})

	// One contact targeting the single list "a,b"; another targeting the two lists "a" and "b".
	// These must NOT share a request.
	single := stagingLine(t, 1, `{"type":"identify","traits":{"email":"single@example.com"},`+
		`"context":{"externalId":[{"type":"listIds","id":["a,b"]}]}}`)
	double := stagingLine(t, 2, `{"type":"identify","traits":{"email":"double@example.com"},`+
		`"context":{"externalId":[{"type":"listIds","id":["a","b"]}]}}`)

	output := newUploader(api).Upload(&common.AsyncDestinationStruct{
		FileName:        writeStagingFile(t, single, double),
		ImportingJobIDs: []int64{1, 2},
		Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
	})
	require.ElementsMatch(t, []int64{1, 2}, output.ImportingJobIDs)
	require.Len(t, requests, 2, "two different list targets must produce two different requests")

	for _, request := range requests {
		require.Len(t, request.Contacts, 1, "a collision would have put both contacts in one request")
		if request.Contacts[0].Email == "single@example.com" {
			require.Equal(t, []string{"a,b"}, request.ListIDs)
		} else {
			require.Equal(t, []string{"a", "b"}, request.ListIDs)
		}
	}
}

// TestDurableReasonsCarryACodeAndRedactIdentifiers pins the F4 defences on per-contact reasons.
//
// The provider's message is REQUIRED in the reason - the AAP mandates recording it - so it cannot be
// dropped. What can be done is bound it, classify it, and remove the one piece of personal data that
// is known exactly at the point the reason is built: the affected contact's own identifier.
func TestDurableReasonsCarryACodeAndRedactIdentifiers(t *testing.T) {
	t.Parallel()

	t.Run("a stable code prefixes every provider derived reason", func(t *testing.T) {
		t.Parallel()

		response := reconcileFixtureDocument(t,
			`{"errors":[{"email":"blake@example.com","message":"Invalid postal code for this contact."}]}`)

		require.Equal(t, http.StatusOK, response.StatusCode)
		reason := response.Metadata.FailedReasons[2]
		require.True(t, strings.HasPrefix(reason, "SENDGRID_CONTACT_REJECTED: "),
			"the code must come FIRST so triage never has to parse provider prose: %q", reason)
		require.Contains(t, reason, "Invalid postal code",
			"the provider's explanation is required by the AAP and must survive")
	})

	t.Run("the contact's own identifier is redacted from the provider text", func(t *testing.T) {
		t.Parallel()

		// An external ID, deliberately: it is neither email-shaped nor a long digit run, so the
		// pattern-based sanitizer structurally cannot catch it. Only exact-match redaction can.
		response := reconcileFixtureDocument(t,
			`{"errors":[{"external_id":"user_523","message":"Contact user_523 was refused by the list."}]}`)

		require.Equal(t, http.StatusOK, response.StatusCode)
		reason := response.Metadata.FailedReasons[5]
		require.NotContains(t, reason, "user_523", "the identifier must not survive into durable metadata")
		require.Contains(t, reason, "[redacted-identifier]")
		require.Contains(t, reason, "was refused by the list", "the explanation still reads")
	})

	t.Run("redaction is case insensitive because sendgrid lower cases what it stores", func(t *testing.T) {
		t.Parallel()

		// The event carried casey@example.com; the provider echoes it up-cased. A case-sensitive
		// replacement would miss it entirely, which is the whole reason this is not ReplaceAll.
		response := reconcileFixtureDocument(t,
			`{"errors":[{"identifier":"casey@example.com","message":"Address CASEY@Example.COM is suppressed."}]}`)

		require.Equal(t, http.StatusOK, response.StatusCode)
		reason := response.Metadata.FailedReasons[3]
		require.NotContains(t, strings.ToLower(reason), "casey@example.com")
		require.Contains(t, reason, "is suppressed")
	})

	t.Run("a reason with no message still carries its own code", func(t *testing.T) {
		t.Parallel()

		response := reconcileFixtureDocument(t, `{"errors":[{"email":"blake@example.com"}]}`)

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t,
			"SENDGRID_CONTACT_REJECTED_WITHOUT_MESSAGE: sendgrid reported an error for this contact without a message",
			response.Metadata.FailedReasons[2],
			"an unexplained rejection is a distinct condition and gets its own code")
	})

	t.Run("an unbounded provider message is capped", func(t *testing.T) {
		t.Parallel()

		// Written once per failed contact, so an errored import could persist this tens of thousands
		// of times. The per-row cap is tighter than the whole-upload cap for exactly that reason.
		response := reconcileFixtureDocument(t, fmt.Sprintf(
			`{"errors":[{"email":"blake@example.com","message":%q}]}`, strings.Repeat("z", 5000)))

		require.Equal(t, http.StatusOK, response.StatusCode)
		reason := response.Metadata.FailedReasons[2]
		require.Less(t, len([]rune(reason)), 320, "the per-row reason must be bounded: got %d runes", len([]rune(reason)))
		require.Contains(t, reason, "truncated")
		require.True(t, strings.HasPrefix(reason, "SENDGRID_CONTACT_REJECTED: "),
			"capping happens LAST, so it can never remove the code or a value redaction was about to remove")
	})
}

// TestStagingPathIsNeverDisclosed pins F6: the absolute staging path stays internal.
//
// The path names the deployment's directory layout and the batch router's internal file naming. It was
// reaching the FAILURE REASON persisted against every job of the batch, which is durable and read far
// more widely than a log line - and it got there through the filesystem error itself, because
// *os.PathError renders its own path however carefully the surrounding sentence is written.
func TestStagingPathIsNeverDisclosed(t *testing.T) {
	t.Parallel()

	t.Run("a missing staging file names the cause, not the path", func(t *testing.T) {
		t.Parallel()

		directory := t.TempDir()
		missing := filepath.Join(directory, "staging-file-that-is-absent.jsonl")

		output := newUploader(newMockAPI(t)).Upload(&common.AsyncDestinationStruct{
			FileName:        missing,
			ImportingJobIDs: []int64{1, 2, 3},
			FailedJobIDs:    []int64{4},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		// The behaviour is unchanged: every job is still retryable.
		require.ElementsMatch(t, []int64{1, 2, 3, 4}, output.FailedJobIDs)
		require.Equal(t, 4, output.FailedCount)
		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.AbortJobIDs)

		// What changed is the text. Neither the directory nor the full path appears, and the base
		// name is absent too - the reason is durable, so it carries the cause alone.
		require.NotContains(t, output.FailedReason, directory)
		require.NotContains(t, output.FailedReason, missing)
		require.NotContains(t, output.FailedReason, "staging-file-that-is-absent")
		require.NotContains(t, output.FailedReason, "/", "no path separator may appear at all")
		require.Contains(t, output.FailedReason, "the file does not exist",
			"the CAUSE is what an operator acts on, and it is stated plainly")
	})

	t.Run("an unreadable staging file names permission, not the path", func(t *testing.T) {
		t.Parallel()

		if os.Geteuid() == 0 {
			t.Skip("root bypasses the permission bits this case depends on")
		}

		directory := t.TempDir()
		unreadable := filepath.Join(directory, "unreadable.jsonl")
		require.NoError(t, os.WriteFile(unreadable, []byte(contactLine(t, 1)+"\n"), 0o200))
		t.Cleanup(func() { _ = os.Chmod(unreadable, 0o600) })

		output := newUploader(newMockAPI(t)).Upload(&common.AsyncDestinationStruct{
			FileName:        unreadable,
			ImportingJobIDs: []int64{1},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.FailedJobIDs)
		require.NotContains(t, output.FailedReason, directory)
		require.NotContains(t, output.FailedReason, "unreadable.jsonl")
		require.Contains(t, output.FailedReason, "permission was denied")
	})

	t.Run("jobs missing from the staging file are reported without the path", func(t *testing.T) {
		t.Parallel()

		api := newMockAPI(t)
		api.EXPECT().UploadContacts(gomock.Any()).Times(1).Return(
			&sendgridbulkupload.UpsertResponse{JobID: "sg-1"}, nil)

		// The batch claims four jobs; the file holds one. The sweep must report the other three.
		output := newUploader(api).Upload(&common.AsyncDestinationStruct{
			FileName:        writeStagingFile(t, contactLine(t, 1)),
			ImportingJobIDs: []int64{1, 2, 3, 4},
			Destination:     testDestination(map[string]any{"apiKey": "SG.k"}),
		})

		require.Equal(t, []int64{1}, output.ImportingJobIDs)
		require.ElementsMatch(t, []int64{2, 3, 4}, output.FailedJobIDs)
		require.Contains(t, output.FailedReason, "not present in the staging file")
		require.NotContains(t, output.FailedReason, "/", "the sweep's reason carries no path either")
	})
}
