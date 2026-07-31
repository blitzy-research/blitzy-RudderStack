package sendgridbulkupload_test

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksendgrid "github.com/rudderlabs/rudder-server/mocks/router/sendgridbulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	sendgridbulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sendgrid-bulk-upload"
)

const (
	testDestinationID = "2sendGridBulkUploadDestID"
	testAPIKey        = "SG.unit-test-api-key"

	// testEventListID is the list the committed staging fixture targets per event, through a
	// context.externalId entry of type listIds on its first two records.
	testEventListID = "037ae8d4-25b4-496e-adff-2fded15fd0c5"
	// testConfigListID is a DIFFERENT list, used where the destination configuration must be
	// distinguishable from per-event targeting.
	testConfigListID = "9b0f4d6c-1f1c-4a3f-9f16-6a86e7a9f321"

	testImportJobID = "01HZY7QK9P6RJ9V2X4C8N3TB5M"
	testErrorsURL   = "https://api.sendgrid.com/v3/marketing/contacts/imports/errors/01HZY7QK9P6RJ9V2X4C8N3TB5M"

	stagingFixturePath = "testdata/uploadData.jsonl"
	errorsFixturePath  = "testdata/errors.json"
)

// The emails of the committed staging fixture, in job order. Reconciliation keys on them, and the
// privacy assertions check that none of them ever reaches a persisted reason.
var stagedEmails = map[int64]string{
	1: "alex@example.com",
	2: "blake@example.com",
	3: "casey@example.com",
	4: "devon@example.com",
	5: "erin@example.com",
}

// stagedJobIDs are the job IDs of the committed staging fixture, in file order.
func stagedJobIDs() []int64 { return []int64{1, 2, 3, 4, 5} }

// newUploader assembles the manager directly, which is what the external test package exists for:
// the generated API-service mock is injected in place of the HTTP adapter, so the whole suite runs
// with no network access at all.
func newUploader(t *testing.T, apiService sendgridbulkupload.SendGridAPIService, listIDs ...string) *sendgridbulkupload.SendGridBulkUploader {
	t.Helper()
	return &sendgridbulkupload.SendGridBulkUploader{
		Logger:        logger.NOP,
		StatsFactory:  stats.NOP,
		DestinationID: testDestinationID,
		DestinationConfig: sendgridbulkupload.DestinationConfig{
			APIKey:              testAPIKey,
			ListIDs:             listIDs,
			CustomFieldsMapping: map[string]string{"plan": "w1", "signedUpAt": "w2"},
		},
		SendGridAPIService: apiService,
	}
}

func newAPIServiceMock(t *testing.T) *mocksendgrid.MockSendGridAPIService {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mocksendgrid.NewMockSendGridAPIService(ctrl)
}

func asyncDestination(fileName string, jobIDs []int64) *common.AsyncDestinationStruct {
	return &common.AsyncDestinationStruct{
		Destination:     &backendconfig.DestinationT{ID: testDestinationID, Name: "sendgrid-bulk-upload"},
		FileName:        fileName,
		ImportingJobIDs: jobIDs,
		Count:           len(jobIDs),
	}
}

// stagedMessages returns the event of every record of the committed staging fixture, keyed by job ID.
func stagedMessages(t *testing.T) map[int64]string {
	t.Helper()
	contents, err := os.ReadFile(stagingFixturePath)
	require.NoError(t, err)
	messages := make(map[int64]string)
	for _, line := range strings.Split(strings.TrimSpace(string(contents)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		jobID := gjson.Get(line, "metadata.job_id").Int()
		require.NotZero(t, jobID, "every staged line must be attributable to a job")
		messages[jobID] = gjson.Get(line, "message").Raw
	}
	return messages
}

// importingJobs rebuilds the importing jobs the router hands to GetUploadStats out of the same
// committed staging fixture the upload was built from. An importing job carries the ORIGINAL event
// payload, whose event sits under body.JSON - which is exactly where Transform reads it from, and
// therefore where reconciliation has to re-derive the contact from.
func importingJobs(t *testing.T, jobIDs ...int64) []*jobsdb.JobT {
	t.Helper()
	messages := stagedMessages(t)
	jobs := make([]*jobsdb.JobT, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		message, staged := messages[jobID]
		require.Truef(t, staged, "job %d is not part of the staging fixture", jobID)
		jobs = append(jobs, importingJob(jobID, message))
	}
	return jobs
}

func importingJob(jobID int64, message string) *jobsdb.JobT {
	return &jobsdb.JobT{JobID: jobID, EventPayload: []byte(`{"body":{"JSON":` + message + `}}`)}
}

// writeStagingFile stages lines into a temporary file, for the cases the committed fixture
// deliberately does not cover.
func writeStagingFile(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "uploadData.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	return path
}

func stagedLine(jobID int64, message string) string {
	return `{"message":` + message + `,"metadata":{"job_id":` + strconv.FormatInt(jobID, 10) + `}}`
}

// importingParameters is the persisted import state the router hands back to reconciliation: a
// marshalled common.ImportParameters, which is the only shape it can read the import identifier out
// of.
func importingParameters(t *testing.T) []byte {
	t.Helper()
	parameters, err := jsonrs.Marshal(common.ImportParameters{ImportId: testImportJobID, ImportCount: 5})
	require.NoError(t, err)
	return parameters
}

func contactByEmail(t *testing.T, contacts []sendgridbulkupload.Contact, email string) sendgridbulkupload.Contact {
	t.Helper()
	for _, contact := range contacts {
		if contact.Email == email {
			return contact
		}
	}
	require.FailNowf(t, "contact not found", "the request carries no contact for %s", email)
	return sendgridbulkupload.Contact{}
}

// requireCarriesNoContactData is the privacy assertion this connector's failure reasons have to
// satisfy: a reason is persisted in JobsDB, where it long outlives the delivery attempt, so it must
// never carry a contact identifier or free-form provider prose.
func requireCarriesNoContactData(t *testing.T, reason string) {
	t.Helper()
	lowered := strings.ToLower(reason)
	for _, email := range stagedEmails {
		require.NotContainsf(t, lowered, email, "reason %q leaks a contact identifier", reason)
	}
	for _, provider := range []string{
		"invalid email address provided for contact",
		"custom field value exceeds the maximum allowed length",
		"no matching contact record",
		"too many requests",
	} {
		require.NotContainsf(t, lowered, provider, "reason %q repeats provider prose", reason)
	}
}

func TestNewManager(t *testing.T) {
	t.Parallel()

	t.Run("a configured destination produces a manager that satisfies the whole contract", func(t *testing.T) {
		t.Parallel()
		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, &backendconfig.DestinationT{
			ID: testDestinationID,
			Config: map[string]any{
				"apiKey":              testAPIKey,
				"listIds":             []any{testConfigListID, " " + testConfigListID + " ", ""},
				"customFieldsMapping": map[string]any{" plan ": " w1 ", "signedUpAt": "w2"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, manager)

		// The registered destination must be constructible through the shared contract, since that is
		// how the batch router holds it.
		var contract common.AsyncDestinationManager = manager
		require.NotNil(t, contract)

		require.Equal(t, testAPIKey, manager.DestinationConfig.APIKey)
		// Trimmed, de-duplicated, and blanks dropped.
		require.Equal(t, []string{testConfigListID}, manager.DestinationConfig.ListIDs)
		require.Equal(t, map[string]string{"plan": "w1", "signedUpAt": "w2"}, manager.DestinationConfig.CustomFieldsMapping)
	})

	t.Run("a destination that cannot be used is rejected at construction", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name   string
			config map[string]any
		}{
			{
				name:   "with no api key at all",
				config: map[string]any{"listIds": []any{testConfigListID}},
			},
			{
				name:   "with a blank api key",
				config: map[string]any{"apiKey": "   "},
			},
			{
				name:   "with an api key of the wrong json type",
				config: map[string]any{"apiKey": 42},
			},
			{
				name:   "with a custom field mapping keyed by a blank trait",
				config: map[string]any{"apiKey": testAPIKey, "customFieldsMapping": map[string]any{"  ": "w1"}},
			},
			{
				name:   "with a custom field mapping onto a blank field id",
				config: map[string]any{"apiKey": testAPIKey, "customFieldsMapping": map[string]any{"plan": " "}},
			},
			{
				// Two traits claiming one field would resolve by Go's randomized map iteration order,
				// delivering a different value on each run while looking healthy.
				name: "with two traits claiming one custom field id",
				config: map[string]any{
					"apiKey":              testAPIKey,
					"customFieldsMapping": map[string]any{"plan": "w1", "tier": "w1"},
				},
			},
			{
				name:   "with a list id longer than this connector will send",
				config: map[string]any{"apiKey": testAPIKey, "listIds": []any{strings.Repeat("l", 200)}},
			},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP,
					&backendconfig.DestinationT{ID: testDestinationID, Config: testCase.config})
				require.Error(t, err)
				require.Nil(t, manager)
			})
		}
	})

	t.Run("a nil destination is rejected", func(t *testing.T) {
		t.Parallel()
		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, nil)
		require.Error(t, err)
		require.Nil(t, manager)
	})
}

func TestTransform(t *testing.T) {
	t.Parallel()

	messages := stagedMessages(t)
	cases := []struct {
		name      string
		jobID     int64
		eventType string
	}{
		{name: "an identify event", jobID: 1, eventType: "identify"},
		{name: "a track event", jobID: 2, eventType: "track"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			uploader := newUploader(t, newAPIServiceMock(t), testEventListID)
			// The router hands Transform the original job payload, whose event sits under body.JSON.
			job := importingJob(testCase.jobID, messages[testCase.jobID])

			staged, err := uploader.Transform(job)
			require.NoError(t, err)

			// The staged shape is the framework's, and the job id is what makes every later outcome
			// attributable.
			require.Equal(t, testCase.jobID, gjson.Get(staged, "metadata.job_id").Int())
			require.True(t, gjson.Get(staged, "message").IsObject())
			// Event-type agnostic: the event is carried through untouched, whatever its type, so a
			// track and an identify both reduce to a contact when the batch is uploaded.
			require.Equal(t, testCase.eventType, gjson.Get(staged, "message.type").String())
			require.Equal(t, stagedEmails[testCase.jobID], gjson.Get(staged, "message.traits.email").String())
		})
	}
}

// TestUploadHappyPath is scenario S1: N staged track/identify events become ONE PUT carrying the
// correct contact fields and list IDs, SendGrid accepts it with a job ID, and that job ID is
// persisted in the shape the router reads back.
func TestUploadHappyPath(t *testing.T) {
	t.Parallel()

	apiService := newAPIServiceMock(t)
	var captured sendgridbulkupload.UpsertRequest
	apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
		DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			captured = request
			return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
		})

	// The destination targets the same list the fixture's per-event targeting names, so all five
	// staged records share one request.
	uploader := newUploader(t, apiService, testEventListID)
	output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

	require.Equal(t, []string{testEventListID}, captured.ListIDs)
	require.Len(t, captured.Contacts, 5)

	// A fully populated identify: every documented mapping, with the email lower-cased, userId as
	// external_id, anonymousId as anonymous_id, the nested address traits flattened, and only the
	// explicitly mapped traits as custom fields - addressed by their pre-created field IDs.
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
	}, contactByEmail(t, captured.Contacts, "alex@example.com"))

	// A track event reduces to a contact through the same mapping, which is what makes the connector
	// event-type agnostic.
	blake := contactByEmail(t, captured.Contacts, "blake@example.com")
	require.Equal(t, "user_223", blake.ExternalID)
	require.Equal(t, "Portland", blake.City)

	casey := contactByEmail(t, captured.Contacts, "casey@example.com")
	require.Equal(t, []string{"casey.alt@example.com", "casey.work@example.com"}, casey.AlternateEmails)
	// Traits the event does not carry stay absent rather than being sent empty, because SendGrid
	// upserts: an empty value would overwrite whatever it already holds.
	require.Empty(t, casey.PhoneNumberID)
	require.Empty(t, casey.City)

	// A contact identified only by anonymousId and email is accepted: SendGrid needs just one of the
	// four identifiers.
	devon := contactByEmail(t, captured.Contacts, "devon@example.com")
	require.Empty(t, devon.ExternalID)
	require.Equal(t, "anon_889", devon.AnonymousID)

	require.Equal(t, testDestinationID, output.DestinationID)
	require.Equal(t, stagedJobIDs(), output.ImportingJobIDs)
	require.Equal(t, 5, output.ImportingCount)
	require.Empty(t, output.FailedJobIDs)
	require.Empty(t, output.FailedReason)
	require.Zero(t, output.FailedCount)
	require.Empty(t, output.AbortJobIDs)
	require.Empty(t, output.AbortReason)
	require.Zero(t, output.AbortCount)

	// The import identifier has to round-trip through exactly the shape the router reads with gjson,
	// and it has to be the string SendGrid issued, unchanged: anything else and the import is
	// orphaned in the importing state forever.
	require.NotEmpty(t, output.ImportingParameters)
	require.Equal(t, testImportJobID, gjson.GetBytes(output.ImportingParameters, "importId").String())
	require.Equal(t, int64(5), gjson.GetBytes(output.ImportingParameters, "importCount").Int())
	var parameters common.ImportParameters
	require.NoError(t, jsonrs.Unmarshal(output.ImportingParameters, &parameters))
	require.Equal(t, testImportJobID, parameters.ImportId)
	require.Equal(t, 5, parameters.ImportCount)

	// And the import it produced polls to a clean completion.
	apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
		Return(&sendgridbulkupload.ImportStatusResponse{
			ID:      testImportJobID,
			Status:  "completed",
			Results: sendgridbulkupload.ImportResults{RequestedCount: 5, CreatedCount: 5, ErroredCount: 0},
		}, nil)
	poll := uploader.Poll(common.AsyncPoll{ImportId: testImportJobID, ImportCount: 5})
	require.Equal(t, common.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}, poll)
}

// TestUploadRateLimited is scenario S3: a 429 must leave every affected job on the RETRYABLE channel
// and must not leave any importing state behind, so the batch router releases the batch and tries
// again instead of stranding the destination.
func TestUploadRateLimited(t *testing.T) {
	t.Parallel()

	resetEpoch := time.Date(2035, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
	apiService := newAPIServiceMock(t)
	apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
		Return(nil, &sendgridbulkupload.RateLimitError{
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: "30s",
			ResetEpoch: resetEpoch,
			Limit:      600,
			Remaining:  0,
		})

	uploader := newUploader(t, newAPIServiceMock(t), testEventListID)
	uploader.SendGridAPIService = apiService
	output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

	// Retryable, and ONLY retryable.
	require.ElementsMatch(t, stagedJobIDs(), output.FailedJobIDs)
	require.Equal(t, 5, output.FailedCount)
	require.Empty(t, output.AbortJobIDs, "a rate limit is transient, so nothing may be aborted")
	require.Zero(t, output.AbortCount)
	require.Empty(t, output.AbortReason)

	// No importing state at all: the router treats importing parameters plus a non-empty importing
	// set as "upload in progress", and anything else as "release the batch".
	require.Empty(t, output.ImportingJobIDs)
	require.Zero(t, output.ImportingCount)
	require.Nil(t, output.ImportingParameters)

	// The reset window reaches the reason, rendered as an absolute instant because the provider's
	// header is epoch seconds rather than a delta.
	require.Contains(t, output.FailedReason, "429")
	require.Contains(t, output.FailedReason, "retryAfter=30s")
	require.Contains(t, output.FailedReason, "reset="+time.Unix(resetEpoch, 0).UTC().Format(time.RFC3339))
	require.Contains(t, output.FailedReason, "limit=600")
	require.Contains(t, output.FailedReason, "remaining=0")
	require.Contains(t, output.FailedReason, "will be retried")
	requireCarriesNoContactData(t, output.FailedReason)
}

func TestUploadRejectionsAndFailures(t *testing.T) {
	t.Parallel()

	t.Run("a rejected upsert keeps its jobs retryable and reports only the status", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		field := "contacts"
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(nil, &sendgridbulkupload.APIError{
				Operation:  "marketing contacts upsert",
				StatusCode: http.StatusBadRequest,
				Items: []sendgridbulkupload.APIErrorItem{
					{Field: &field, Message: "invalid email address provided for contact blake@example.com"},
					{Field: nil, Message: "too many requests"},
				},
			})

		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		require.ElementsMatch(t, stagedJobIDs(), output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Contains(t, output.FailedReason, "400")
		require.Contains(t, output.FailedReason, "2 error item(s)")
		// The provider's own prose - which restated a contact's email - is counted, never repeated.
		requireCarriesNoContactData(t, output.FailedReason)
	})

	t.Run("an unusable response keeps its jobs retryable", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(nil, errors.New("Post \"https://api.sendgrid.com/v3/marketing/contacts\": dial tcp: lookup failed"))

		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		require.ElementsMatch(t, stagedJobIDs(), output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Contains(t, output.FailedReason, "did not produce a usable response")
		requireCarriesNoContactData(t, output.FailedReason)
	})

	t.Run("a contact carrying none of the four identifiers is refused without poisoning the batch", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		var captured sendgridbulkupload.UpsertRequest
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				captured = request
				return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
			})

		staging := writeStagingFile(t,
			stagedLine(11, `{"type":"track","event":"Viewed","traits":{"plan":"growth"}}`),
			stagedLine(12, `{"type":"identify","userId":"user_12","traits":{"email":"keeper@example.com"}}`),
		)
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{11, 12}))

		require.Len(t, captured.Contacts, 1)
		require.Equal(t, "keeper@example.com", captured.Contacts[0].Email)
		require.Equal(t, []int64{12}, output.ImportingJobIDs)
		// Permanent: the same event would be refused identically on every retry, so it does not
		// consume the framework's retry budget.
		require.Equal(t, []int64{11}, output.AbortJobIDs)
		require.Equal(t, 1, output.AbortCount)
		require.Contains(t, output.AbortReason, "none of the contact identifiers sendgrid accepts")
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("an event mapping an object onto a contact field is refused rather than flattened", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		staging := writeStagingFile(t,
			stagedLine(21, `{"type":"identify","userId":"user_21","traits":{"email":{"primary":"nested@example.com"}}}`),
		)
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{21}))

		require.Equal(t, []int64{21}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "can only carry a single value")
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
	})

	t.Run("an over-long contact field is refused rather than truncated", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		staging := writeStagingFile(t, stagedLine(22, fmt.Sprintf(
			`{"type":"identify","userId":"user_22","traits":{"email":"long@example.com","firstName":%q}}`,
			strings.Repeat("n", 300))))
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{22}))

		require.Equal(t, []int64{22}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "longer than this connector will send")
	})

	t.Run("a staged line that cannot be attributed to a job fails the whole batch retryably", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		staging := writeStagingFile(t,
			stagedLine(31, `{"type":"identify","userId":"user_31","traits":{"email":"first@example.com"}}`),
			`{"message":{"type":"identify"},"metadata":{}}`,
		)
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{31, 32}))

		// Reporting the unattributable line against job 0 would name a job that does not exist while
		// leaving the real one unaccounted for, so the batch is retried instead.
		require.ElementsMatch(t, []int64{31, 32}, output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Contains(t, output.FailedReason, "staging file for this batch could not be read")
	})

	t.Run("a staged line whose event is unusable is refused on its own", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(&sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil)

		staging := writeStagingFile(t,
			`{"message":"not an object","metadata":{"job_id":41}}`,
			stagedLine(42, `{"type":"identify","userId":"user_42","traits":{"email":"kept@example.com"}}`),
		)
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{41, 42}))

		require.Equal(t, []int64{42}, output.ImportingJobIDs)
		require.Equal(t, []int64{41}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "could not be read as a sendgrid contact")
	})

	t.Run("an unreadable staging file fails the batch retryably without disclosing its path", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		directory := t.TempDir()
		missing := filepath.Join(directory, "absent.jsonl")
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(missing, stagedJobIDs()))

		require.ElementsMatch(t, stagedJobIDs(), output.FailedJobIDs)
		require.Equal(t, 5, output.FailedCount)
		require.Empty(t, output.AbortJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Contains(t, output.FailedReason, "staging file for this batch could not be read")
		// Internal filesystem layout is not something a job status should carry.
		require.NotContains(t, output.FailedReason, directory)
		require.NotContains(t, output.FailedReason, "absent.jsonl")
	})
}

// TestUploadListTargetingAndDeferral proves the documented list-targeting precedence and the
// consequence of it: contacts aimed at different lists cannot share a request, and only ONE import
// can be persisted per upload, so the rest of the batch is deferred RETRYABLY rather than lost.
func TestUploadListTargetingAndDeferral(t *testing.T) {
	t.Parallel()

	apiService := newAPIServiceMock(t)
	var captured sendgridbulkupload.UpsertRequest
	apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
		DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			captured = request
			return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
		})

	// The fixture's first two records carry per-event targeting for a different list than the one
	// configured here, so the batch splits in two: jobs 1 and 2 against the event's list, jobs 3, 4
	// and 5 against the destination's.
	uploader := newUploader(t, apiService, testConfigListID)
	output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

	// The larger group goes first, so an upload makes as much progress as one request can.
	require.Equal(t, []string{testConfigListID}, captured.ListIDs)
	require.Len(t, captured.Contacts, 3)
	require.Equal(t, []int64{3, 4, 5}, output.ImportingJobIDs)
	require.Equal(t, testImportJobID, gjson.GetBytes(output.ImportingParameters, "importId").String())
	// importCount must match the jobs this import actually covers: the router uses it to fetch
	// exactly that many importing jobs when it polls.
	require.Equal(t, int64(3), gjson.GetBytes(output.ImportingParameters, "importCount").Int())

	require.Equal(t, []int64{1, 2}, output.FailedJobIDs)
	require.Equal(t, 2, output.FailedCount)
	require.Contains(t, output.FailedReason, "deferred to a later upload")
	require.Empty(t, output.AbortJobIDs)
	requireCarriesNoContactData(t, output.FailedReason)
}

func TestUploadChunkBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("both caps are honoured at their boundaries", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name             string
			maxContacts      int
			expectedContacts int
			expectedImports  []int64
			expectedDeferred []int64
		}{
			{
				name: "one below the contact cap", maxContacts: 6, expectedContacts: 5,
				expectedImports: stagedJobIDs(),
			},
			{
				name: "exactly at the contact cap", maxContacts: 5, expectedContacts: 5,
				expectedImports: stagedJobIDs(),
			},
			{
				name: "one above the contact cap", maxContacts: 4, expectedContacts: 4,
				expectedImports: []int64{1, 2, 3, 4}, expectedDeferred: []int64{5},
			},
			{
				name: "well above the contact cap", maxContacts: 2, expectedContacts: 2,
				expectedImports: []int64{1, 2}, expectedDeferred: []int64{3, 4, 5},
			},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				apiService := newAPIServiceMock(t)
				var captured sendgridbulkupload.UpsertRequest
				apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
					DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						captured = request
						return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
					})

				uploader := newUploader(t, apiService, testEventListID)
				uploader.MaxContactsPerRequest = testCase.maxContacts
				output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

				require.Len(t, captured.Contacts, testCase.expectedContacts)
				require.Equal(t, testCase.expectedImports, output.ImportingJobIDs)
				require.Equal(t, testCase.expectedDeferred, nilIfEmpty(output.FailedJobIDs))
				require.Empty(t, output.AbortJobIDs)
				// Never an empty chunk, and never a chunk over the cap.
				require.NotEmpty(t, captured.Contacts)
				require.LessOrEqual(t, len(captured.Contacts), testCase.maxContacts)
			})
		}
	})

	t.Run("the byte cap is measured against the whole request body, envelope included", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		var captured sendgridbulkupload.UpsertRequest
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				captured = request
				return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
			})

		const maxRequestBytes = 400
		uploader := newUploader(t, apiService, testEventListID)
		uploader.MaxRequestBytes = maxRequestBytes
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		body, err := jsonrs.Marshal(captured)
		require.NoError(t, err)
		// The provider charges its byte ceiling for the WHOLE body, so the list IDs and the
		// surrounding object are part of the budget - not covered by a guessed reserve.
		require.LessOrEqual(t, len(body), maxRequestBytes)
		require.NotEmpty(t, captured.Contacts)
		require.Less(t, len(captured.Contacts), 5)

		// Nothing is lost to chunking: every staged job is either importing or deferred retryably.
		require.ElementsMatch(t, stagedJobIDs(), append(append([]int64{}, output.ImportingJobIDs...), output.FailedJobIDs...))
		require.Empty(t, output.AbortJobIDs)
		require.Contains(t, output.FailedReason, "deferred to a later upload")
	})

	t.Run("a contact no request could ever carry is refused terminally", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		// Above the envelope, so the request itself is plannable, but below any single contact.
		uploader.MaxRequestBytes = 120
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		require.ElementsMatch(t, stagedJobIDs(), output.AbortJobIDs)
		require.Equal(t, 5, output.AbortCount)
		require.Contains(t, output.AbortReason, "larger than one sendgrid marketing contacts request can carry")
		require.Empty(t, output.ImportingJobIDs)
		require.Nil(t, output.ImportingParameters)
		require.Empty(t, output.FailedJobIDs)
	})

	t.Run("list ids that exhaust the request budget keep their jobs retryable", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		// Below the envelope itself, so not one contact can fit - which only a configuration change
		// can fix, so the jobs are retried rather than discarded.
		uploader.MaxRequestBytes = 40
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		require.ElementsMatch(t, stagedJobIDs(), output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
		require.Empty(t, output.ImportingJobIDs)
		require.Contains(t, output.FailedReason, "list identifiers alone exceed one request's byte budget")
	})
}

func nilIfEmpty(jobIDs []int64) []int64 {
	if len(jobIDs) == 0 {
		return nil
	}
	return jobIDs
}

func TestPoll(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		status        *sendgridbulkupload.ImportStatusResponse
		err           error
		expected      common.PollStatusResponse
		errorContains []string
		errorOmits    []string
	}{
		{
			name: "a pending import is polled again with no state change",
			status: &sendgridbulkupload.ImportStatusResponse{
				Status:  "pending",
				Results: sendgridbulkupload.ImportResults{RequestedCount: 5},
			},
			expected: common.PollStatusResponse{StatusCode: http.StatusOK, InProgress: true},
		},
		{
			name: "a completed import with no errored rows succeeds wholesale",
			status: &sendgridbulkupload.ImportStatusResponse{
				Status:  "completed",
				Results: sendgridbulkupload.ImportResults{RequestedCount: 5, CreatedCount: 5},
			},
			expected: common.PollStatusResponse{StatusCode: http.StatusOK, Complete: true},
		},
		{
			name: "an errored import is routed into reconciliation",
			status: &sendgridbulkupload.ImportStatusResponse{
				Status: "errored",
				Results: sendgridbulkupload.ImportResults{
					RequestedCount: 5, UpdatedCount: 3, ErroredCount: 2, ErrorsURL: testErrorsURL,
				},
			},
			expected: common.PollStatusResponse{
				StatusCode: http.StatusOK, Complete: true, HasFailed: true, FailedJobParameters: testErrorsURL,
			},
		},
		{
			// The provider documents completed as "finished without any errors", so a non-zero
			// errored count contradicts the status. Reconciling is the only safe reading: the
			// alternative branch marks EVERY importing job succeeded wholesale.
			name: "a completed import that nevertheless reports errored rows is routed into reconciliation",
			status: &sendgridbulkupload.ImportStatusResponse{
				Status: "completed",
				Results: sendgridbulkupload.ImportResults{
					RequestedCount: 5, UpdatedCount: 3, ErroredCount: 2, ErrorsURL: testErrorsURL,
				},
			},
			expected: common.PollStatusResponse{
				StatusCode: http.StatusOK, Complete: true, HasFailed: true, FailedJobParameters: testErrorsURL,
			},
		},
		{
			// failed is documented as finished with all errors or entirely unprocessable, which
			// retrying cannot change, so it takes the framework's terminal path.
			name: "a failed import is terminal",
			status: &sendgridbulkupload.ImportStatusResponse{
				Status:  "failed",
				Results: sendgridbulkupload.ImportResults{RequestedCount: 5, ErroredCount: 5, ErrorsURL: testErrorsURL},
			},
			expected: common.PollStatusResponse{
				StatusCode: http.StatusBadRequest, Complete: true, HasFailed: true,
			},
			errorContains: []string{"all errors or was entirely unprocessable"},
		},
		{
			name:          "an unrecognized status is retried and named",
			status:        &sendgridbulkupload.ImportStatusResponse{Status: "Processing"},
			expected:      common.PollStatusResponse{StatusCode: http.StatusInternalServerError},
			errorContains: []string{"does not recognize", "processing"},
		},
		{
			name:          "an unrecognized status that is not a plain token is not echoed",
			status:        &sendgridbulkupload.ImportStatusResponse{Status: `done <script>alert("x")</script>`},
			expected:      common.PollStatusResponse{StatusCode: http.StatusInternalServerError},
			errorContains: []string{"does not recognize"},
			errorOmits:    []string{"script", "alert"},
		},
		{
			name:          "a rate limited poll is retried with its window",
			err:           &sendgridbulkupload.RateLimitError{StatusCode: http.StatusTooManyRequests, Limit: 600, Remaining: 0},
			expected:      common.PollStatusResponse{StatusCode: http.StatusTooManyRequests},
			errorContains: []string{"429", "limit=600", "remaining=0"},
		},
		{
			name: "a rejected poll is retried with its status",
			err: &sendgridbulkupload.APIError{
				Operation: "contacts import status", StatusCode: http.StatusServiceUnavailable,
			},
			expected:      common.PollStatusResponse{StatusCode: http.StatusInternalServerError},
			errorContains: []string{"503", "contacts import status"},
		},
		{
			name:          "an unreachable provider is retried",
			err:           errors.New("dial tcp 1.2.3.4:443: i/o timeout"),
			expected:      common.PollStatusResponse{StatusCode: http.StatusInternalServerError},
			errorContains: []string{"could not be read"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			apiService := newAPIServiceMock(t)
			// Exactly one provider request per poll, so one poll can never monopolize the router's
			// polling loop.
			apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).Return(testCase.status, testCase.err)

			uploader := newUploader(t, apiService, testEventListID)
			response := uploader.Poll(common.AsyncPoll{ImportId: testImportJobID, ImportCount: 5})

			require.Equal(t, testCase.expected.StatusCode, response.StatusCode)
			require.Equal(t, testCase.expected.Complete, response.Complete)
			require.Equal(t, testCase.expected.InProgress, response.InProgress)
			require.Equal(t, testCase.expected.HasFailed, response.HasFailed)
			require.Equal(t, testCase.expected.FailedJobParameters, response.FailedJobParameters)
			// SendGrid has no warning tier, so the warning channel stays untouched throughout.
			require.False(t, response.HasWarning)
			require.Empty(t, response.WarningJobParameters)
			for _, expected := range testCase.errorContains {
				require.Contains(t, strings.ToLower(response.Error), expected)
			}
			for _, unexpected := range testCase.errorOmits {
				require.NotContains(t, strings.ToLower(response.Error), unexpected)
			}
			requireCarriesNoContactData(t, response.Error)
		})
	}

	t.Run("an import with no identifier is not polled at all", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.Poll(common.AsyncPoll{ImportId: "   ", ImportCount: 5})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "no sendgrid import identifier")
		require.False(t, response.Complete)
	})
}

func TestGetUploadStatsDocumentShapes(t *testing.T) {
	t.Parallel()

	// The document behind errors_url has no published schema, so every shape it has been seen to
	// take must reconcile identically. Each of these names job 2 and job 4, and nothing else.
	cases := []struct {
		name     string
		document string
	}{
		{
			name:     "a bare json array",
			document: `[{"email":"blake@example.com","message":"invalid email"},{"identifier":"devon@example.com","detail":"custom field rejected"}]`,
		},
		{
			name:     "an object wrapping the rows under errors",
			document: `{"errors":[{"email":"blake@example.com","error_message":"invalid email"},{"contact":{"email":"devon@example.com"},"reason":"custom field rejected"}]}`,
		},
		{
			name:     "an object wrapping the rows under results",
			document: `{"results":[{"email":"BLAKE@example.com","message":"invalid email"},{"email":"devon@example.com","message":"custom field rejected"}]}`,
		},
		{
			name:     "newline delimited json",
			document: "{\"email\":\"blake@example.com\",\"message\":\"invalid email\"}\n\n{\"email\":\"devon@example.com\",\"message\":\"custom field rejected\"}\n",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			apiService := newAPIServiceMock(t)
			apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(testCase.document), nil)

			uploader := newUploader(t, apiService, testEventListID)
			response := uploader.GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importingParameters(t),
				ImportingList:       importingJobs(t, stagedJobIDs()...),
			})

			// 200 is mandatory: any other status has the router discard the whole reconciliation.
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, []int64{2, 4}, response.Metadata.FailedKeys)
			require.Equal(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)
			// Retryable, never terminal: these rows are per-contact rejections, not permanent
			// failures of the whole import.
			require.Empty(t, response.Metadata.AbortedKeys)
			require.Empty(t, response.Metadata.WarningKeys)
			require.NotNil(t, response.Metadata.FailedReasons)
			require.NotNil(t, response.Metadata.AbortedReasons)
			require.NotNil(t, response.Metadata.WarningReasons)

			require.Contains(t, response.Metadata.FailedReasons[2], "error class: invalid_email")
			require.Contains(t, response.Metadata.FailedReasons[4], "error class: custom_field_rejected")
			for _, jobID := range []int64{2, 4} {
				requireCarriesNoContactData(t, response.Metadata.FailedReasons[jobID])
			}
			// Case never splits a match: both sides are lower-cased.
			require.Len(t, response.Metadata.FailedReasons, 2)
		})
	}
}

func TestGetUploadStatsReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("a row that cannot be attributed changes no other job's outcome", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`[{"email":"ghost@example.com","message":"no matching contact record"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		// An unattributable row is reported and counted, but it is NOT evidence that some other
		// contact failed - so every job the import actually covered succeeds.
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Empty(t, response.Metadata.FailedKeys)
		require.Empty(t, response.Metadata.FailedReasons)
		require.Equal(t, stagedJobIDs(), response.Metadata.SucceededKeys)
		require.Empty(t, response.Metadata.AbortedKeys)
	})

	t.Run("attributable and unattributable rows are reconciled independently", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(
			`[{"email":"ghost@example.com","message":"no matching contact record"},`+
				`{"email":"erin@example.com","message":"invalid email"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{5}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{1, 2, 3, 4}, response.Metadata.SucceededKeys)
	})

	t.Run("a row naming a non-email identifier resolves to its job", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(
			`[{"external_id":"user_323","message":"invalid phone number"},`+
				`{"anonymous_id":"anon_889","message":"list membership rejected"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{3, 4}, response.Metadata.FailedKeys)
		require.Contains(t, response.Metadata.FailedReasons[3], "error class: invalid_phone_number")
		require.Contains(t, response.Metadata.FailedReasons[4], "error class: list_rejected")
		require.Equal(t, []int64{1, 2, 5}, response.Metadata.SucceededKeys)
	})

	t.Run("an identifier several jobs sent fails every one of them", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`[{"email":"shared@example.com","message":"invalid email"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList: []*jobsdb.JobT{
				importingJob(51, `{"type":"identify","userId":"user_51","traits":{"email":"shared@example.com"}}`),
				importingJob(52, `{"type":"track","userId":"user_52","traits":{"email":"SHARED@example.com"}}`),
				importingJob(53, `{"type":"identify","userId":"user_53","traits":{"email":"other@example.com"}}`),
			},
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{51, 52}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{53}, response.Metadata.SucceededKeys)
	})

	t.Run("an unrecognized message is classified rather than repeated", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(
			`[{"email":"erin@example.com","message":"Erin Okafor at 500 Harbor Blvd could not be stored"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{5}, response.Metadata.FailedKeys)
		reason := response.Metadata.FailedReasons[5]
		require.Contains(t, reason, "error class: unspecified")
		// Provider prose can restate any contact field it likes, and a failure reason is persisted,
		// so only the class ever reaches it.
		require.NotContains(t, reason, "Erin Okafor")
		require.NotContains(t, reason, "500 Harbor Blvd")
		requireCarriesNoContactData(t, reason)
	})

	t.Run("the errors document url is re-read from the import when the poll response carried none", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
			Return(&sendgridbulkupload.ImportStatusResponse{
				ID:     testImportJobID,
				Status: "errored",
				Results: sendgridbulkupload.ImportResults{
					RequestedCount: 5, ErroredCount: 1, ErrorsURL: testErrorsURL,
				},
			}, nil)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`[{"email":"blake@example.com","message":"invalid email"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importingParameters(t),
			ImportingList: importingJobs(t, stagedJobIDs()...),
		})

		// Reconciliation is STATELESS: everything it needs is re-derived from what the router hands
		// it, so it stays correct in a different process invocation from the upload.
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{1, 3, 4, 5}, response.Metadata.SucceededKeys)
	})
}

func TestGetUploadStatsRetriesRatherThanReportingFalseSuccess(t *testing.T) {
	t.Parallel()

	// Every case here could be "resolved" by returning 200 with an empty failed set - which would
	// have the router mark every job of the import succeeded. None of them is evidence of delivery,
	// so each is retried instead.
	cases := []struct {
		name          string
		document      string
		documentErr   error
		parameters    []byte
		statusURL     string
		statusErr     error
		expectStatus  bool
		errorContains string
	}{
		{
			name:          "a document that is not json at all",
			document:      "<html><body>gateway timeout</body></html>",
			parameters:    []byte(`{"importId":"` + testImportJobID + `","importCount":5}`),
			errorContains: "could not be read in any shape",
		},
		{
			name:          "an empty document",
			document:      "   ",
			parameters:    []byte(`{"importId":"` + testImportJobID + `","importCount":5}`),
			errorContains: "could not be read in any shape",
		},
		{
			name:          "a json document carrying no recognizable row",
			document:      `{"unexpected":"shape"}`,
			parameters:    []byte(`{"importId":"` + testImportJobID + `","importCount":5}`),
			errorContains: "could not be read in any shape",
		},
		{
			name:          "a newline delimited document with an unreadable line",
			document:      "{\"email\":\"blake@example.com\",\"message\":\"invalid email\"}\n{not json}\n",
			parameters:    []byte(`{"importId":"` + testImportJobID + `","importCount":5}`),
			errorContains: "could not be read in any shape",
		},
		{
			name:          "a document that cannot be fetched",
			documentErr:   errors.New("Get \"https://errors.example.com/doc\": context deadline exceeded"),
			parameters:    []byte(`{"importId":"` + testImportJobID + `","importCount":5}`),
			errorContains: "could not be fetched",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			apiService := newAPIServiceMock(t)
			apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
				Return([]byte(testCase.document), testCase.documentErr)

			uploader := newUploader(t, apiService, testEventListID)
			response := uploader.GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          testCase.parameters,
				ImportingList:       importingJobs(t, stagedJobIDs()...),
			})

			require.Equal(t, http.StatusInternalServerError, response.StatusCode)
			require.Contains(t, response.Error, testCase.errorContains)
			require.Empty(t, response.Metadata.FailedKeys)
			require.Empty(t, response.Metadata.SucceededKeys)
			require.Empty(t, response.Metadata.AbortedKeys)
			requireCarriesNoContactData(t, response.Error)
		})
	}

	t.Run("importing jobs carrying no import identifier are retried without any provider call", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(gomock.Any()).Times(0)
		apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters:    []byte(`{"importCount":5}`),
			ImportingList: importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "no sendgrid import identifier")
		require.Empty(t, response.Metadata.SucceededKeys)
	})

	t.Run("an import publishing no errors document is retried", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
			Return(&sendgridbulkupload.ImportStatusResponse{
				ID:      testImportJobID,
				Status:  "errored",
				Results: sendgridbulkupload.ImportResults{RequestedCount: 5, ErroredCount: 2},
			}, nil)
		apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importingParameters(t),
			ImportingList: importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "published no errors document")
	})

	t.Run("an import status that cannot be re-read is retried", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
			Return(nil, &sendgridbulkupload.RateLimitError{StatusCode: http.StatusTooManyRequests, Limit: -1, Remaining: -1})
		apiService.EXPECT().GetImportErrors(gomock.Any()).Times(0)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importingParameters(t),
			ImportingList: importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusInternalServerError, response.StatusCode)
		require.Contains(t, response.Error, "could not be read")
	})
}

// errorsDocumentFixture is the COMMITTED partial-failure document. It is the oracle for scenario S2:
// two of its rows name contacts the staging fixture carries - one under a flat email, one under a
// nested contact.email - and its third names a contact this import never carried.
func errorsDocumentFixture(t *testing.T) []byte {
	t.Helper()
	document, err := os.ReadFile(errorsFixturePath)
	require.NoError(t, err)
	return document
}

// TestUploadPartialFailureAcceptance is scenario S2, driven end to end by the committed fixtures: an
// import finishes with some errors, the connector fetches and parses the errors document, and one
// import yields BOTH outcomes - the errored contacts retryably failed with reasons, and the exact
// remainder succeeded.
//
// Both statuses that can carry errored rows are exercised. The provider documents completed as
// "finished without any errors", so a completed status with a non-zero nested errored_count
// contradicts itself; reconciling anyway is what stops a partially errored import from being
// reported as a clean success.
func TestUploadPartialFailureAcceptance(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status string
	}{
		{name: "an import sendgrid reports as errored", status: "errored"},
		{name: "an import sendgrid reports as completed while still counting errored rows", status: "completed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			apiService := newAPIServiceMock(t)
			apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
				Return(&sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil)
			apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
				Return(&sendgridbulkupload.ImportStatusResponse{
					ID:     testImportJobID,
					Status: testCase.status,
					Results: sendgridbulkupload.ImportResults{
						RequestedCount: 5,
						UpdatedCount:   3,
						// Nested, which is the whole point: read flat, this would be 0 forever and every
						// partial failure would be reported as a clean success.
						ErroredCount: 2,
						ErrorsURL:    testErrorsURL,
					},
				}, nil)
			apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
				Return(errorsDocumentFixture(t), nil)

			uploader := newUploader(t, apiService, testEventListID)

			output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))
			require.Equal(t, stagedJobIDs(), output.ImportingJobIDs)
			require.Equal(t, testImportJobID, gjson.GetBytes(output.ImportingParameters, "importId").String())

			poll := uploader.Poll(common.AsyncPoll{
				ImportId:    gjson.GetBytes(output.ImportingParameters, "importId").String(),
				ImportCount: int(gjson.GetBytes(output.ImportingParameters, "importCount").Int()),
			})
			require.Equal(t, http.StatusOK, poll.StatusCode)
			require.True(t, poll.Complete)
			// HasFailed is what makes the router reconcile instead of marking every importing job
			// succeeded wholesale.
			require.True(t, poll.HasFailed)
			require.Equal(t, testErrorsURL, poll.FailedJobParameters)

			response := uploader.GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: poll.FailedJobParameters,
				Parameters:          output.ImportingParameters,
				ImportingList:       importingJobs(t, output.ImportingJobIDs...),
			})

			// 200, or the router discards the whole reconciliation.
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Empty(t, response.Error)

			// The two attributable rows of the committed fixture, and only those.
			require.Equal(t, []int64{2, 4}, response.Metadata.FailedKeys)
			require.Len(t, response.Metadata.FailedReasons, 2)
			require.Contains(t, response.Metadata.FailedReasons[2], "error class: invalid_email")
			require.Contains(t, response.Metadata.FailedReasons[4], "error class: custom_field_rejected")

			// The exact remainder, including job 5 - which the fixture's third row does NOT name, and
			// which an unattributable row must never be allowed to fail.
			require.Equal(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)

			// One import, both outcomes, and every importing job accounted for exactly once.
			require.NotEmpty(t, response.Metadata.FailedKeys)
			require.NotEmpty(t, response.Metadata.SucceededKeys)
			require.Len(t, append(append([]int64{}, response.Metadata.FailedKeys...),
				response.Metadata.SucceededKeys...), len(stagedJobIDs()))
			for _, failed := range response.Metadata.FailedKeys {
				require.NotContains(t, response.Metadata.SucceededKeys, failed)
			}

			// Retryable, never terminal: the framework decides when to give up on these contacts.
			require.Empty(t, response.Metadata.AbortedKeys)
			require.Empty(t, response.Metadata.AbortedReasons)
			require.Empty(t, response.Metadata.WarningKeys)
			require.Empty(t, response.Metadata.WarningReasons)

			// The fixture's rows restate contact emails in their own prose. None of it may reach a
			// persisted reason.
			for _, reason := range response.Metadata.FailedReasons {
				requireCarriesNoContactData(t, reason)
				require.NotContains(t, reason, "Invalid email address provided")
				require.NotContains(t, reason, "maximum allowed length")
			}
		})
	}
}

// TestReconciliationHandlesUnicodeIdentifiers pins the behaviour of identifier matching for values
// whose case folding changes their byte length. Case folding is what makes matching robust, but it
// is also where offset arithmetic over a folded copy of a string goes wrong: U+023A folds to a
// LONGER encoding and U+212A to a shorter one, so any implementation that indexes the original with
// offsets taken from the folded copy either panics or mismatches.
func TestReconciliationHandlesUnicodeIdentifiers(t *testing.T) {
	t.Parallel()

	const (
		growingEmail  = "\u023Alex@example.com" // U+023A folds to U+2C65: 2 bytes become 3
		shrinkingKelv = "\u212Aelvin@example.com"
		dottedEmail   = "\u0130rem@example.com" // U+0130 folds to two runes
		shortEmail    = "\u023A@e.co"
	)

	apiService := newAPIServiceMock(t)
	// Every row names its contact in a different case from the event that produced it, so matching
	// has to fold both sides rather than compare bytes.
	apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(
		`[{"email":"`+growingEmail+`","message":"invalid email address"},`+
			`{"email":"kelvin@example.com","message":"invalid phone number"},`+
			`{"email":"`+dottedEmail+`","message":"custom field rejected"},`+
			`{"email":"`+shortEmail+`","message":"duplicate contact"},`+
			`{"email":"\u0416\u0438\u0432\u043E\u0439@example.com","message":"no matching contact record"}]`), nil)

	uploader := newUploader(t, apiService, testEventListID)
	response := uploader.GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: testErrorsURL,
		Parameters:          importingParameters(t),
		ImportingList: []*jobsdb.JobT{
			importingJob(61, `{"type":"identify","userId":"user_61","traits":{"email":"`+growingEmail+`"}}`),
			importingJob(62, `{"type":"identify","userId":"user_62","traits":{"email":"`+shrinkingKelv+`"}}`),
			importingJob(63, `{"type":"identify","userId":"user_63","traits":{"email":"`+dottedEmail+`"}}`),
			importingJob(64, `{"type":"identify","userId":"user_64","traits":{"email":"`+shortEmail+`"}}`),
			importingJob(65, `{"type":"identify","userId":"user_65","traits":{"email":"safe@example.com"}}`),
		},
	})

	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, []int64{61, 62, 63, 64}, response.Metadata.FailedKeys)
	require.Equal(t, []int64{65}, response.Metadata.SucceededKeys)
	require.Contains(t, response.Metadata.FailedReasons[61], "error class: invalid_email")
	require.Contains(t, response.Metadata.FailedReasons[62], "error class: invalid_phone_number")
	require.Contains(t, response.Metadata.FailedReasons[63], "error class: custom_field_rejected")
	require.Contains(t, response.Metadata.FailedReasons[64], "error class: duplicate_contact")
	for _, jobID := range response.Metadata.FailedKeys {
		reason := response.Metadata.FailedReasons[jobID]
		for _, identifier := range []string{growingEmail, shrinkingKelv, dottedEmail, shortEmail, "\u2C65", "kelvin"} {
			require.NotContains(t, strings.ToLower(reason), strings.ToLower(identifier),
				"a persisted reason must never carry a contact identifier, however it is encoded")
		}
	}
}

// TestPersistedReasonsCarryNoProviderTextOrContactData is the standing regression for the one
// property every reason this connector reports has to hold: a reason is written into JobsDB, where
// it outlives the delivery attempt by far, so it may carry neither third-party prose nor contact
// data - whatever the provider chose to put in its own messages.
func TestPersistedReasonsCarryNoProviderTextOrContactData(t *testing.T) {
	t.Parallel()

	t.Run("per-row reasons carry only a class, however much the row restates", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return([]byte(
			`[{"email":"erin@example.com","message":"Erin Okafor, 500 Harbor Blvd, Seattle WA 98101, +14155554234, plan=enterprise: rejected"},`+
				`{"contact":{"email":"casey@example.com"},"error_message":"contact casey@example.com has an invalid alternate email casey.work@example.com"},`+
				`{"identifier":"devon@example.com","detail":"phone +14155554234 belongs to \u0416\u0438\u0432\u043E\u0439 \u041F\u0435\u0442\u0440\u043E\u0432"}]`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{3, 4, 5}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{1, 2}, response.Metadata.SucceededKeys)
		for _, jobID := range response.Metadata.FailedKeys {
			reason := response.Metadata.FailedReasons[jobID]
			require.Contains(t, reason, "sendgrid rejected this contact during the marketing contacts import")
			for _, leaked := range []string{
				"Erin Okafor", "500 Harbor Blvd", "Seattle", "98101", "+14155554234", "enterprise",
				"casey@example.com", "casey.work@example.com", "devon@example.com",
				"\u0416\u0438\u0432\u043E\u0439", "alternate email", "belongs to",
			} {
				require.NotContainsf(t, reason, leaked, "reason %q carries %q", reason, leaked)
			}
			requireCarriesNoContactData(t, reason)
		}
	})

	t.Run("a poll error carries no provider text", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
			Return(&sendgridbulkupload.ImportStatusResponse{
				Status: "rejected because alex@example.com is not a valid contact",
			}, nil)

		uploader := newUploader(t, apiService, testEventListID)
		poll := uploader.Poll(common.AsyncPoll{ImportId: testImportJobID, ImportCount: 5})

		require.Equal(t, http.StatusInternalServerError, poll.StatusCode)
		require.Contains(t, poll.Error, "does not recognize")
		require.NotContains(t, poll.Error, "alex@example.com")
		require.NotContains(t, poll.Error, "not a valid contact")
		requireCarriesNoContactData(t, poll.Error)
	})

	t.Run("an upload failure reason carries only the status and the item count", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		field := "contacts[0].email"
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(nil, &sendgridbulkupload.APIError{
				Operation:  "marketing contacts upsert",
				StatusCode: http.StatusBadRequest,
				Items: []sendgridbulkupload.APIErrorItem{
					{Field: &field, Message: "alex@example.com at 123 Main St, San Francisco is invalid"},
				},
			})

		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

		require.Contains(t, output.FailedReason, "status 400")
		require.Contains(t, output.FailedReason, "1 error item(s)")
		require.NotContains(t, output.FailedReason, "123 Main St")
		require.NotContains(t, output.FailedReason, "San Francisco")
		require.NotContains(t, output.FailedReason, "contacts[0].email")
		requireCarriesNoContactData(t, output.FailedReason)
	})
}

// TestImportingParametersCarryTheProviderJobIDUnchanged pins the one detail that decides whether a
// polled import can ever be resolved: the persisted import identifier must be exactly the string
// SendGrid issued, recoverable by the router's own gjson read, and handed back to the provider
// verbatim. An identifier this connector composed itself - a range list, a manifest, anything - would
// leave the import unresolvable however well the rest of the connector behaved.
func TestImportingParametersCarryTheProviderJobIDUnchanged(t *testing.T) {
	t.Parallel()

	// Deliberately awkward: separators, an equals sign and digits, so any attempt to encode extra
	// meaning into this field would corrupt it visibly.
	const providerJobID = "id=job-ranges;3-7,9;chunk=2"

	apiService := newAPIServiceMock(t)
	apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
		Return(&sendgridbulkupload.UpsertResponse{JobID: providerJobID}, nil)
	// The expectation is on the EXACT string, so the round trip is proven end to end rather than
	// merely asserted on the persisted bytes.
	apiService.EXPECT().GetImportStatus(providerJobID).Times(1).
		Return(&sendgridbulkupload.ImportStatusResponse{
			ID:      providerJobID,
			Status:  "completed",
			Results: sendgridbulkupload.ImportResults{RequestedCount: 5, UpdatedCount: 5},
		}, nil)

	uploader := newUploader(t, apiService, testEventListID)
	output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

	// Exactly the shape the router reads back, with no transformation of the identifier.
	require.Equal(t, providerJobID, gjson.GetBytes(output.ImportingParameters, "importId").String())
	require.Equal(t, int64(len(stagedJobIDs())), gjson.GetBytes(output.ImportingParameters, "importCount").Int())
	var parameters common.ImportParameters
	require.NoError(t, jsonrs.Unmarshal(output.ImportingParameters, &parameters))
	require.Equal(t, providerJobID, parameters.ImportId)
	require.Equal(t, len(stagedJobIDs()), parameters.ImportCount)

	poll := uploader.Poll(common.AsyncPoll{
		ImportId:    gjson.GetBytes(output.ImportingParameters, "importId").String(),
		ImportCount: parameters.ImportCount,
	})
	require.Equal(t, common.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}, poll)
}
