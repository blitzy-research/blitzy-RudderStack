package sendgridbulkupload_test

import (
	"errors"
	"fmt"
	"math"
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

func stagedJobIDs() []int64 { return []int64{1, 2, 3, 4, 5} }

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

func stagedMessages(t *testing.T) map[int64]string {
	t.Helper()
	contents, err := os.ReadFile(stagingFixturePath)
	require.NoError(t, err)
	messages := make(map[int64]string)
	for line := range strings.SplitSeq(strings.TrimSpace(string(contents)), "\n") {
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

		var contract common.AsyncDestinationManager = manager
		require.NotNil(t, contract)

		require.Equal(t, testAPIKey, manager.DestinationConfig.APIKey)
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

// TestAPIServiceRejectsUnusableInput covers the HTTP adapter's input guards, which decide before any
// request is built and are therefore reachable through the exported constructor without a network, a
// server or a mock. The controls that need a socket - the dial and redirect guards - are deliberately
// left out: this suite's worth rests on it never opening one.
func TestAPIServiceRejectsUnusableInput(t *testing.T) {
	t.Parallel()

	newService := func(t *testing.T) sendgridbulkupload.SendGridAPIService {
		t.Helper()
		service, err := sendgridbulkupload.NewSendGridAPIService(testDestinationID,
			sendgridbulkupload.DestinationConfig{APIKey: testAPIKey}, stats.NOP)
		require.NoError(t, err)
		require.NotNil(t, service)
		return service
	}

	t.Run("construction fails without an api key", func(t *testing.T) {
		t.Parallel()
		service, err := sendgridbulkupload.NewSendGridAPIService(testDestinationID,
			sendgridbulkupload.DestinationConfig{APIKey: "   "}, stats.NOP)
		require.Error(t, err)
		require.Nil(t, service)
		require.Contains(t, err.Error(), "apiKey is missing")
	})

	t.Run("an upsert carrying no contacts is refused", func(t *testing.T) {
		t.Parallel()
		upsert, err := newService(t).UploadContacts(sendgridbulkupload.UpsertRequest{ListIDs: []string{testConfigListID}})
		require.Error(t, err)
		require.Nil(t, upsert)
		require.Contains(t, err.Error(), "carrying no contacts")
		require.NotContains(t, err.Error(), testAPIKey)
	})

	t.Run("import job ids the path will not carry are refused", func(t *testing.T) {
		t.Parallel()
		for name, jobID := range map[string]string{
			"a blank job id":      "   ",
			"an over-long job id": strings.Repeat("j", 257),
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				status, err := newService(t).GetImportStatus(jobID)
				require.Error(t, err)
				require.Nil(t, status)
				require.NotContains(t, err.Error(), testAPIKey)
			})
		}
	})

	// Every rejection reports a stable token and never the URL, which may be pre-signed and therefore
	// carry a credential in its query string.
	t.Run("errors document urls this connector will not fetch", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name   string
			url    string
			reason string
		}{
			{name: "a blank url", url: "  ", reason: "blank_url"},
			{
				name:   "a url longer than the connector accepts",
				url:    "https://errors.example.com/doc?signature=" + strings.Repeat("a", 2048),
				reason: "url_too_long",
			},
			{name: "a url that cannot be parsed", url: "https://errors.example.com/doc%zz", reason: "unparseable_url"},
			{name: "a plaintext url", url: "http://errors.example.com/doc", reason: "unsupported_scheme"},
			{
				name:   "a url embedding credentials",
				url:    "https://svc:s3cret@errors.example.com/doc",
				reason: "credentials_in_url",
			},
			{name: "a url with no host", url: "https:///doc", reason: "blank_host"},
			{name: "a url on another port", url: "https://errors.example.com:8443/doc", reason: "unsupported_port"},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				document, err := newService(t).GetImportErrors(testCase.url)
				require.Error(t, err)
				require.Nil(t, document)
				require.Contains(t, err.Error(), "reason: "+testCase.reason)
				require.NotContains(t, err.Error(), "errors.example.com", "a rejection must not echo the url")
				require.NotContains(t, err.Error(), "s3cret")
				require.NotContains(t, err.Error(), testAPIKey)
			})
		}
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

	apiService.EXPECT().GetImportStatus(testImportJobID).Times(1).
		Return(&sendgridbulkupload.ImportStatusResponse{
			ID:      testImportJobID,
			Status:  "completed",
			Results: sendgridbulkupload.ImportResults{RequestedCount: 5, CreatedCount: 5, ErroredCount: 0},
		}, nil)
	poll := uploader.Poll(common.AsyncPoll{ImportId: testImportJobID, ImportCount: 5})
	require.Equal(t, common.PollStatusResponse{StatusCode: http.StatusOK, Complete: true}, poll)
}

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

	uploader := newUploader(t, apiService, testEventListID)
	output := uploader.Upload(asyncDestination(stagingFixturePath, stagedJobIDs()))

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

// TestRateLimitReasonRendersOnlyARenderableResetWindow pins how X-RateLimit-Reset reaches an operator.
//
// The header is provider-controlled and unvalidated, and it lands in a JobsDB failure reason that long
// outlives the delivery attempt. RFC 3339 expresses a four-digit year, so an epoch beyond that range is
// not a timestamp at all - Go's formatter silently emits a twelve-digit year - and presenting that as
// the moment a rate limit lifts is noise dressed up as a fact. Every value that IS renderable must
// still be rendered exactly as the provider sent it, so both halves are asserted.
func TestRateLimitReasonRendersOnlyARenderableResetWindow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		resetEpoch    int64
		expectedReset string
		renderable    bool
	}{
		{
			name: "a window seconds away is rendered exactly", resetEpoch: 1893456000,
			expectedReset: "2030-01-01T00:00:00Z", renderable: true,
		},
		{
			// The latest instant RFC 3339 can express: still rendered, because it still is a timestamp.
			name: "the last renderable instant is still rendered", resetEpoch: 253402300799,
			expectedReset: "9999-12-31T23:59:59Z", renderable: true,
		},
		{
			name:       "one second past the last renderable instant is reported out of range",
			resetEpoch: 253402300800, expectedReset: "out-of-range",
		},
		{
			name: "an absurd header value is reported out of range", resetEpoch: math.MaxInt64,
			expectedReset: "out-of-range",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			reason := (&sendgridbulkupload.RateLimitError{
				StatusCode: http.StatusTooManyRequests,
				ResetEpoch: testCase.resetEpoch,
				Limit:      600,
				Remaining:  0,
			}).Error()

			require.Contains(t, reason, "reset="+testCase.expectedReset)
			require.NotContains(t, reason, strconv.FormatInt(testCase.resetEpoch, 10),
				"the raw header value is never repeated back")
			require.Contains(t, reason, "limit=600", "an unusable window must not cost the usable fields")
			require.Contains(t, reason, "remaining=0")
			if testCase.renderable {
				parsed, err := time.Parse(time.RFC3339, testCase.expectedReset)
				require.NoError(t, err, "a rendered window must be valid RFC 3339")
				require.Equal(t, testCase.resetEpoch, parsed.Unix())
			}
		})
	}
}

// TestStagingLineJobIDMustBeAnExactInteger pins which staged lines may name a job.
//
// A staged line's metadata.job_id is the only link between a contact and the job that produced it, and
// the numeric view of a JSON value is lossy in exactly the wrong direction: it truncates a fractional
// value and saturates one too large for an int64. Either would report a job that is not in this batch
// while leaving the real one unaccounted for. Such a line must therefore be unattributable - which
// defers the batch retryably - rather than being silently re-pointed at some other job.
func TestStagingLineJobIDMustBeAnExactInteger(t *testing.T) {
	t.Parallel()

	const event = `{"type":"identify","userId":"user_1","traits":{"email":"exact@example.com"}}`

	t.Run("a job id that is not an exact int64 makes the line unattributable", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name  string
			jobID string
		}{
			{name: "a fractional value", jobID: "1.5"},
			{name: "a value larger than an int64", jobID: "99999999999999999999"},
			{name: "an exponent form", jobID: "1e3"},
			{name: "zero", jobID: "0"},
			{name: "a negative value", jobID: "-1"},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				apiService := newAPIServiceMock(t)
				apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

				staging := writeStagingFile(t,
					`{"message":`+event+`,"metadata":{"job_id":`+testCase.jobID+`}}`)
				uploader := newUploader(t, apiService, testEventListID)
				output := uploader.Upload(asyncDestination(staging, []int64{1}))

				// The batch is deferred as a whole, and only the jobs the router actually claimed are
				// ever named - never a job ID conjured out of a malformed value.
				require.Equal(t, []int64{1}, output.FailedJobIDs)
				require.Contains(t, output.FailedReason, "staging file for this batch could not be read")
				require.Empty(t, output.ImportingJobIDs)
				require.Nil(t, output.ImportingParameters)
				require.Empty(t, output.AbortJobIDs)
			})
		}
	})

	t.Run("an exact integer job id is accepted", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			Return(&sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil)

		staging := writeStagingFile(t, stagedLine(7, event))
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{7}))

		require.Equal(t, []int64{7}, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
		require.Empty(t, output.AbortJobIDs)
	})
}

// TestListIDCountIsBoundedBeforeDeduplication pins the documented bound on how many list identifiers
// one upsert will target, and the order in which it is applied.
//
// The count is checked on the RAW set, before duplicates are collapsed, which is the DoS-safer
// ordering: an event naming an unbounded number of repeats is refused before anything is allocated or
// normalised. That is a deliberate choice rather than an accident, so both halves are pinned - repeats
// within the bound still collapse to the distinct set, and a raw count over the bound is refused with
// this connector's own reason and no partial delivery.
func TestListIDCountIsBoundedBeforeDeduplication(t *testing.T) {
	t.Parallel()

	const maxListIDs = 64
	repeatedListIDs := func(count int, ids ...string) []string {
		listIDs := make([]string, 0, count)
		for len(listIDs) < count-len(ids) {
			listIDs = append(listIDs, testEventListID)
		}
		return append(listIDs, ids...)
	}
	asJSONArray := func(listIDs []string) string {
		encoded, err := jsonrs.Marshal(listIDs)
		require.NoError(t, err)
		return string(encoded)
	}

	t.Run("the destination configuration collapses repeats within the bound", func(t *testing.T) {
		t.Parallel()
		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, &backendconfig.DestinationT{
			ID: testDestinationID,
			Config: map[string]any{
				"apiKey":  testAPIKey,
				"listIds": repeatedListIDs(maxListIDs, testConfigListID),
			},
		})
		require.NoError(t, err)
		require.Equal(t, []string{testEventListID, testConfigListID}, manager.DestinationConfig.ListIDs,
			"64 raw identifiers naming 2 distinct lists are accepted and collapsed")
	})

	t.Run("the destination configuration is refused past the bound", func(t *testing.T) {
		t.Parallel()
		manager, err := sendgridbulkupload.NewManager(logger.NOP, stats.NOP, &backendconfig.DestinationT{
			ID: testDestinationID,
			Config: map[string]any{
				"apiKey":  testAPIKey,
				"listIds": repeatedListIDs(maxListIDs+1, testConfigListID),
			},
		})
		require.Error(t, err)
		require.Nil(t, manager, "never a partially initialised manager")
	})

	t.Run("a per-event target collapses repeats within the bound", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		var captured sendgridbulkupload.UpsertRequest
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				captured = request
				return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
			})

		staging := writeStagingFile(t, stagedLine(111,
			`{"type":"identify","userId":"user_111","traits":{"email":"bounded@example.com"},`+
				`"context":{"externalId":[{"type":"listIds","id":`+
				asJSONArray(repeatedListIDs(maxListIDs, testConfigListID))+`}]}}`))
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{111}))

		require.Equal(t, []int64{111}, output.ImportingJobIDs)
		require.Equal(t, []string{testEventListID, testConfigListID}, captured.ListIDs)
	})

	t.Run("a per-event target past the bound is refused terminally, never partially delivered", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(0)

		staging := writeStagingFile(t, stagedLine(112,
			`{"type":"identify","userId":"user_112","traits":{"email":"unbounded@example.com"},`+
				`"context":{"externalId":[{"type":"listIds","id":`+
				asJSONArray(repeatedListIDs(maxListIDs+1, testConfigListID))+`}]}}`))
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{112}))

		// Terminal by design: the same event would be refused identically on every retry, and the
		// alternative - dropping the lists that did not fit - would present a subset delivery as
		// complete.
		require.Equal(t, []int64{112}, output.AbortJobIDs)
		require.Contains(t, output.AbortReason, "more sendgrid lists, or longer list identifiers, than this connector will send")
		require.Empty(t, output.ImportingJobIDs)
		require.Empty(t, output.FailedJobIDs)
		requireCarriesNoContactData(t, output.AbortReason)
	})
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

// requestEnvelopeSize is the fixed cost of one upsert body that is not a contact: the list IDs and
// the surrounding object, with an empty contacts array. It mirrors what the connector measures, and
// it is what turns a configured cap into the number of bytes the contacts themselves may occupy.
func requestEnvelopeSize(t *testing.T, listIDs ...string) int {
	t.Helper()
	envelope, err := jsonrs.Marshal(sendgridbulkupload.UpsertRequest{
		ListIDs:  listIDs,
		Contacts: []sendgridbulkupload.Contact{},
	})
	require.NoError(t, err)
	return len(envelope)
}

// contactOfSize is the contact stagedContactOfSize' line reduces to: an email plus one mapped
// custom field, whose padded value is the only variable part of the serialization.
func contactOfSize(email, pad string) sendgridbulkupload.Contact {
	return sendgridbulkupload.Contact{Email: email, CustomFields: map[string]any{"w1": pad}}
}

// stagedContactOfSize stages one event whose contact serializes to EXACTLY size bytes.
//
// Byte-cap boundary cases are only meaningful if the body's size is known to the byte, so the size is
// solved for rather than approximated: everything but the mapped custom field's value is fixed, so the
// padding length follows directly, and the result is re-measured before being returned.
func stagedContactOfSize(t *testing.T, jobID int64, size int) string {
	t.Helper()
	email := fmt.Sprintf("c%d@example.com", jobID)
	empty, err := jsonrs.Marshal(contactOfSize(email, ""))
	require.NoError(t, err)
	padLength := size - len(empty)
	require.GreaterOrEqualf(t, padLength, 1, "a contact of %d bytes is smaller than this helper builds", size)

	pad := strings.Repeat("p", padLength)
	measured, err := jsonrs.Marshal(contactOfSize(email, pad))
	require.NoError(t, err)
	require.Len(t, measured, size, "the helper must land on the requested contact size exactly")

	return stagedLine(jobID, fmt.Sprintf(
		`{"type":"identify","traits":{"email":%q,"plan":%q}}`, email, pad))
}

// stagedContactsFillingBody stages the contacts whose whole serialized request body - envelope
// included - measures exactly wholeBody bytes. The bulk records are uniform and a single trailing
// record absorbs the remainder, so the total lands on the target to the byte.
func stagedContactsFillingBody(t *testing.T, envelope, wholeBody int) ([]string, []int64) {
	t.Helper()
	// A serialized array of N contacts spends sum(sizes) + (N-1) bytes: one separator between each
	// adjacent pair, and none before the first.
	const (
		bulkContactSize = 1024
		minFinalContact = 128
	)
	content := wholeBody - envelope
	require.Greater(t, content, bulkContactSize+minFinalContact)

	lines := make([]string, 0, content/bulkContactSize+1)
	jobIDs := make([]int64, 0, content/bulkContactSize+1)
	spent := 0
	for jobID := int64(1); ; jobID++ {
		cost := bulkContactSize
		if len(lines) > 0 {
			cost += 1 // separator
		}
		// Stop while enough of the budget is left for the trailing record to absorb the remainder.
		if spent+cost+minFinalContact > content {
			lines = append(lines, stagedContactOfSize(t, jobID, content-spent-1))
			jobIDs = append(jobIDs, jobID)
			return lines, jobIDs
		}
		lines = append(lines, stagedContactOfSize(t, jobID, bulkContactSize))
		jobIDs = append(jobIDs, jobID)
		spent += cost
	}
}

// TestUploadFillsTheByteBudgetToTheLastByte pins the byte cap at its exact boundary, for a lone
// contact and for a multi-contact request, and against the shipped ceiling rather than only a
// convenient override.
//
// SendGrid charges its ceiling for the WHOLE serialized body, so the only correct reading is that a
// body measuring the cap fits and a body one byte larger does not. Both directions matter, and they
// fail differently. Under-filling silently wastes provider capacity and defers jobs that would have
// shipped. Over-strictness is worse: a lone contact measuring exactly the budget must never be judged
// impossible to send, because that verdict is TERMINAL for a job SendGrid would have accepted.
func TestUploadFillsTheByteBudgetToTheLastByte(t *testing.T) {
	t.Parallel()

	t.Run("a lone contact is judged against the whole budget", func(t *testing.T) {
		t.Parallel()

		const contactSize = 400
		envelope := requestEnvelopeSize(t, testEventListID)
		wholeBody := envelope + contactSize

		cases := []struct {
			name             string
			maxRequestBytes  int
			expectedRequests int
		}{
			{name: "one byte short of the body it produces", maxRequestBytes: wholeBody - 1, expectedRequests: 0},
			{name: "exactly the body it produces", maxRequestBytes: wholeBody, expectedRequests: 1},
			{name: "one byte more than the body it produces", maxRequestBytes: wholeBody + 1, expectedRequests: 1},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				apiService := newAPIServiceMock(t)
				var captured sendgridbulkupload.UpsertRequest
				apiService.EXPECT().UploadContacts(gomock.Any()).Times(testCase.expectedRequests).
					DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
						captured = request
						return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
					})

				staging := writeStagingFile(t, stagedContactOfSize(t, 1, contactSize))
				uploader := newUploader(t, apiService, testEventListID)
				uploader.MaxRequestBytes = testCase.maxRequestBytes
				output := uploader.Upload(asyncDestination(staging, []int64{1}))

				if testCase.expectedRequests == 0 {
					// Genuinely impossible, and permanently so: no configuration this connector will
					// accept could carry it, so the job is refused terminally rather than retried.
					require.Equal(t, []int64{1}, output.AbortJobIDs)
					require.Contains(t, output.AbortReason, "larger than one sendgrid marketing contacts request can carry")
					require.Empty(t, output.ImportingJobIDs)
					return
				}
				body, err := jsonrs.Marshal(captured)
				require.NoError(t, err)
				require.Len(t, body, wholeBody, "the body must be the size the contact and envelope imply")
				require.LessOrEqual(t, len(body), testCase.maxRequestBytes)
				require.Equal(t, []int64{1}, output.ImportingJobIDs)
				require.Empty(t, output.AbortJobIDs, "a contact that fits is never a terminal failure")
				require.Empty(t, output.FailedJobIDs)
			})
		}
	})

	t.Run("one request is filled up to the last byte the cap allows", func(t *testing.T) {
		t.Parallel()

		const (
			firstContactSize  = 300
			secondContactSize = 500
		)
		envelope := requestEnvelopeSize(t, testEventListID)
		// Two contacts cost their sizes plus the single separator between them.
		wholeBody := envelope + firstContactSize + 1 + secondContactSize

		cases := []struct {
			name             string
			maxRequestBytes  int
			expectedContacts int
			expectedImports  []int64
			expectedDeferred []int64
		}{
			{
				name: "two bytes short of both contacts", maxRequestBytes: wholeBody - 2,
				expectedContacts: 1, expectedImports: []int64{1}, expectedDeferred: []int64{2},
			},
			{
				name: "one byte short of both contacts", maxRequestBytes: wholeBody - 1,
				expectedContacts: 1, expectedImports: []int64{1}, expectedDeferred: []int64{2},
			},
			{
				name: "exactly both contacts", maxRequestBytes: wholeBody,
				expectedContacts: 2, expectedImports: []int64{1, 2},
			},
			{
				name: "one byte more than both contacts", maxRequestBytes: wholeBody + 1,
				expectedContacts: 2, expectedImports: []int64{1, 2},
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

				staging := writeStagingFile(t,
					stagedContactOfSize(t, 1, firstContactSize),
					stagedContactOfSize(t, 2, secondContactSize))
				uploader := newUploader(t, apiService, testEventListID)
				uploader.MaxRequestBytes = testCase.maxRequestBytes
				output := uploader.Upload(asyncDestination(staging, []int64{1, 2}))

				body, err := jsonrs.Marshal(captured)
				require.NoError(t, err)
				require.Len(t, captured.Contacts, testCase.expectedContacts)
				require.LessOrEqualf(t, len(body), testCase.maxRequestBytes,
					"a %d byte body exceeds the %d byte cap", len(body), testCase.maxRequestBytes)
				require.Equal(t, testCase.expectedImports, output.ImportingJobIDs)
				require.Equal(t, testCase.expectedDeferred, nilIfEmpty(output.FailedJobIDs))
				require.Empty(t, output.AbortJobIDs)
				if testCase.expectedContacts == 2 {
					require.Len(t, body, wholeBody, "both contacts fit, so the body is the full size")
				}
			})
		}
	})

	t.Run("the shipped six-megabyte ceiling is filled exactly", func(t *testing.T) {
		t.Parallel()

		// The documented ceiling, which is also this connector's default. No override is set, so this
		// is the budget production actually runs with.
		const documentedRequestCeiling = 6_000_000
		envelope := requestEnvelopeSize(t, testEventListID)

		cases := []struct {
			name      string
			wholeBody int
			// deferred is how many trailing jobs cannot travel with the rest.
			deferred int
		}{
			{name: "a body measuring the ceiling travels as one request", wholeBody: documentedRequestCeiling},
			{name: "a body one byte over the ceiling splits", wholeBody: documentedRequestCeiling + 1, deferred: 1},
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

				lines, jobIDs := stagedContactsFillingBody(t, envelope, testCase.wholeBody)
				staging := writeStagingFile(t, lines...)
				uploader := newUploader(t, apiService, testEventListID)
				output := uploader.Upload(asyncDestination(staging, jobIDs))

				body, err := jsonrs.Marshal(captured)
				require.NoError(t, err)
				require.LessOrEqualf(t, len(body), documentedRequestCeiling,
					"a %d byte body exceeds the documented %d byte ceiling", len(body), documentedRequestCeiling)
				require.Len(t, captured.Contacts, len(jobIDs)-testCase.deferred)
				require.Len(t, output.ImportingJobIDs, len(jobIDs)-testCase.deferred)
				require.Len(t, output.FailedJobIDs, testCase.deferred)
				require.Empty(t, output.AbortJobIDs, "nothing here is too large to send")
				if testCase.deferred == 0 {
					require.Len(t, body, documentedRequestCeiling,
						"the request must fill the ceiling to the byte, not stop short of it")
				}
			})
		}
	})
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

	// An import that rejected a single contact can publish that one row on its own. It is valid JSON
	// as a whole document, so it never reaches the newline-delimited branch and has to be read as a
	// one-row document: reporting it unusable would hold the import in "importing" for as long as it
	// exists, because this route has no retry budget to escalate.
	t.Run("a lone row object published on its own", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`{"contact":{"email":"blake@example.com"},"error_message":"invalid email"}`), nil)

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingJobs(t, stagedJobIDs()...),
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{1, 3, 4, 5}, response.Metadata.SucceededKeys)
		require.Contains(t, response.Metadata.FailedReasons[2], "error class: invalid_email")
		require.Empty(t, response.Metadata.AbortedKeys)
		requireCarriesNoContactData(t, response.Metadata.FailedReasons[2])
	})
}

// errorRowsBudget mirrors the connector's per-document row budget, which is twice the 30,000 contacts
// one request can carry. The budget is charged per entry SCANNED, so a document made of entries the
// connector cannot use costs the same bounded work as one made of usable rows.
const errorRowsBudget = 2 * 30_000

// TestGetUploadStatsBoundsTheErrorsDocumentRowBudget proves the row budget bounds how much of a
// document is examined, not merely how many rows are kept. A row beyond the budget is not reached, and
// the import is retried rather than reported delivered.
func TestGetUploadStatsBoundsTheErrorsDocumentRowBudget(t *testing.T) {
	t.Parallel()

	arrayDocument := func(unusableEntries int) string {
		var document strings.Builder
		document.WriteByte('[')
		for range unusableEntries {
			// A number is a well-formed entry that can never be a row, which is exactly the shape
			// that used to be scanned without ever consuming the budget.
			document.WriteString("0,")
		}
		document.WriteString(`{"email":"blake@example.com","message":"invalid email"}]`)
		return document.String()
	}
	newlineDocument := func(unusableLines int) string {
		var document strings.Builder
		for range unusableLines {
			document.WriteString("{\"unexpected\":\"shape\"}\n")
		}
		document.WriteString("{\"email\":\"blake@example.com\",\"message\":\"invalid email\"}\n")
		return document.String()
	}

	cases := []struct {
		name     string
		document string
		reached  bool
	}{
		{name: "an array row within the budget is reconciled", document: arrayDocument(8), reached: true},
		{name: "an array row beyond the budget is not reached", document: arrayDocument(errorRowsBudget), reached: false},
		{name: "a newline delimited row within the budget is reconciled", document: newlineDocument(8), reached: true},
		{name: "a newline delimited row beyond the budget is not reached", document: newlineDocument(errorRowsBudget), reached: false},
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

			if testCase.reached {
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
				require.Equal(t, []int64{1, 3, 4, 5}, response.Metadata.SucceededKeys)
				return
			}
			// Nothing was read out of the document, so no job may be reported either way: 500 has the
			// framework retry instead of marking the whole import succeeded.
			require.Equal(t, http.StatusInternalServerError, response.StatusCode)
			require.Contains(t, response.Error, "could not be read in any shape")
			require.Empty(t, response.Metadata.FailedKeys)
			require.Empty(t, response.Metadata.SucceededKeys)
		})
	}
}

// TestReconciledReasonClassPrecedence pins the order provider messages are classified in. A message
// frequently names more than one thing, so the precedence is part of the contract rather than an
// accident of ordering: the most specific match wins, and the class is all a persisted reason carries.
func TestReconciledReasonClassPrecedence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "a message naming an email and a list describes the email",
			message:  "Invalid email address provided for list 4207",
			expected: "invalid_email",
		},
		{
			name:     "a custom field verdict outranks every other keyword",
			message:  "custom field w1 rejected for this email address on list 4207",
			expected: "custom_field_rejected",
		},
		{
			name:     "an authorization verdict outranks a field word",
			message:  "not authorized to add this email address to the list",
			expected: "not_permitted",
		},
		{
			name:     "a duplication verdict outranks a field word",
			message:  "duplicate email address for this list",
			expected: "duplicate_contact",
		},
		{
			name:     "a phone verdict outranks an email mention",
			message:  "phone number is not valid, and no email address was supplied",
			expected: "invalid_phone_number",
		},
		{
			name:     "a list is still recognized on its own",
			message:  "list membership rejected",
			expected: "list_rejected",
		},
		{
			name:     "prose matching no keyword is unspecified",
			message:  "the contact could not be stored",
			expected: "unspecified",
		},
		{
			name:     "a row carrying no message at all is unspecified",
			message:  "",
			expected: "unspecified",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			document, err := jsonrs.Marshal([]map[string]string{
				{"email": stagedEmails[2], "message": testCase.message},
			})
			require.NoError(t, err)

			apiService := newAPIServiceMock(t)
			apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).Return(document, nil)

			uploader := newUploader(t, apiService, testEventListID)
			response := uploader.GetUploadStats(common.GetUploadStatsInput{
				FailedJobParameters: testErrorsURL,
				Parameters:          importingParameters(t),
				ImportingList:       importingJobs(t, stagedJobIDs()...),
			})

			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, []int64{2}, response.Metadata.FailedKeys)
			reason := response.Metadata.FailedReasons[2]
			require.Contains(t, reason, "error class: "+testCase.expected)
			if testCase.message != "" {
				require.NotContains(t, reason, testCase.message, "provider prose must never reach a persisted reason")
			}
			requireCarriesNoContactData(t, reason)
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

	t.Run("an importing job whose contact cannot be rebuilt is retried rather than reported delivered", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
			Return([]byte(`[{"email":"blake@example.com","message":"invalid email"}]`), nil)

		// This job carries none of the identifiers SendGrid accepts, so no error row could ever name
		// it and the document says nothing about it either way. Succeeding it by exclusion would claim
		// a delivery that was never established.
		importingList := append(importingJobs(t, 1, 2),
			importingJob(9, `{"type":"track","event":"Signed Up","properties":{"plan":"pro"}}`))

		uploader := newUploader(t, apiService, testEventListID)
		response := uploader.GetUploadStats(common.GetUploadStatsInput{
			FailedJobParameters: testErrorsURL,
			Parameters:          importingParameters(t),
			ImportingList:       importingList,
		})

		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, []int64{2, 9}, response.Metadata.FailedKeys)
		require.Equal(t, []int64{1}, response.Metadata.SucceededKeys)
		require.Contains(t, response.Metadata.FailedReasons[9], "could not be rebuilt from its payload")
		// Retryable, never terminal: the framework decides when to give up.
		require.Empty(t, response.Metadata.AbortedKeys)
		requireCarriesNoContactData(t, response.Metadata.FailedReasons[9])
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

// errorsDocumentFixture contains two attributable rows (flat and nested email) plus one unmatched
// row.
func errorsDocumentFixture(t *testing.T) []byte {
	t.Helper()
	document, err := os.ReadFile(errorsFixturePath)
	require.NoError(t, err)
	return document
}

// TestUploadPartialFailureAcceptance verifies both "errored" and defensive "completed with
// errored_count > 0" statuses reconcile instead of succeeding wholesale.
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
						ErroredCount:   2,
						ErrorsURL:      testErrorsURL,
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

			require.Equal(t, []int64{2, 4}, response.Metadata.FailedKeys)
			require.Len(t, response.Metadata.FailedReasons, 2)
			require.Contains(t, response.Metadata.FailedReasons[2], "error class: invalid_email")
			require.Contains(t, response.Metadata.FailedReasons[4], "error class: custom_field_rejected")

			// The exact remainder, including job 5 - which the fixture's third row does NOT name, and
			// which an unattributable row must never be allowed to fail.
			require.Equal(t, []int64{1, 3, 5}, response.Metadata.SucceededKeys)

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

			for _, reason := range response.Metadata.FailedReasons {
				requireCarriesNoContactData(t, reason)
				require.NotContains(t, reason, "Invalid email address provided")
				require.NotContains(t, reason, "maximum allowed length")
			}
		})
	}
}

// TestReconciliationHandlesUnicodeIdentifiers covers lowercasing that changes UTF-8 byte length;
// matching must not reuse offsets from the normalized string against the original. U+023A grows and
// U+212A shrinks.
func TestReconciliationHandlesUnicodeIdentifiers(t *testing.T) {
	t.Parallel()

	const (
		growingEmail  = "\u023Alex@example.com" // U+023A lowercases to U+2C65: two bytes become three.
		shrinkingKelv = "\u212Aelvin@example.com"
		dottedEmail   = "\u0130rem@example.com" // U+0130 lowercases to ASCII 'i' (two UTF-8 bytes become one).
		shortEmail    = "\u023A@e.co"
	)

	apiService := newAPIServiceMock(t)
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

// TestPersistedReasonsCarryNoProviderTextOrContactData verifies that JobsDB-persisted reasons contain
// neither provider prose nor contact data.
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

// TestUploadAccountsForEveryJobInTheBatch pins the completeness sweep.
//
// The batch router writes a job status ONLY for the jobs an upload's output names, so a job that
// landed in none of the three outcome sets receives no status at all and is left unresolved -
// invisibly, because nothing ever failed. The router builds the batch from the very lines it wrote,
// so the two should always agree; the sweep exists for when they do not, which is exactly what a
// truncated or partially written staging file looks like.
//
// The missing job is RETRYABLE, never aborted: a line that is not in the file says nothing permanent
// about the job that produced it, and a later batch either uploads it or fails it with a reason.
func TestUploadAccountsForEveryJobInTheBatch(t *testing.T) {
	t.Parallel()

	apiService := newAPIServiceMock(t)
	apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
		DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
			require.Len(t, request.Contacts, 2, "only the staged contacts can be uploaded")
			return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
		})

	// The batch claims three jobs; the staging file holds two of them.
	staging := writeStagingFile(t,
		stagedLine(81, `{"type":"identify","userId":"user_81","traits":{"email":"staged.one@example.com"}}`),
		stagedLine(82, `{"type":"identify","userId":"user_82","traits":{"email":"staged.two@example.com"}}`),
	)
	uploader := newUploader(t, apiService, testEventListID)
	output := uploader.Upload(asyncDestination(staging, []int64{81, 82, 83}))

	require.Equal(t, []int64{81, 82}, output.ImportingJobIDs)
	require.Equal(t, []int64{83}, output.FailedJobIDs,
		"a job the staging file never carried must be reported, not dropped")
	require.Equal(t, 1, output.FailedCount)
	require.Contains(t, output.FailedReason, "not present in the staging file")
	require.Empty(t, output.AbortJobIDs, "nothing permanent has been decided about the missing job")

	// The accepted chunk keeps its own importing state, so the sweep costs the staged contacts
	// nothing and importCount still describes only the jobs the import actually covers.
	require.EqualValues(t, 2, gjson.GetBytes(output.ImportingParameters, "importCount").Int())

	// Settled exactly once, and nothing outside the batch settled at all: that is the guarantee the
	// router depends on, and it is what makes the sweep an invariant rather than a log line.
	settledCount := make(map[int64]int, 3)
	for _, jobIDs := range [][]int64{output.ImportingJobIDs, output.FailedJobIDs, output.AbortJobIDs} {
		for _, jobID := range jobIDs {
			settledCount[jobID]++
		}
	}
	require.Equal(t, map[int64]int{81: 1, 82: 1, 83: 1}, settledCount)
	requireCarriesNoContactData(t, output.FailedReason)
}

// TestJobSideIdentifierCaseNormalization pins identifier case handling on the JOB side of
// reconciliation, which is where a regression would be silent and expensive.
//
// Two distinct normalisation sites exist and they behave differently on purpose. buildContact
// lower-cases ONLY Contact.Email, because that is the field SendGrid itself lower-cases; external_id,
// anonymous_id and phone_number_id are sent exactly as the event carried them, because they are
// opaque customer identifiers that the provider matches verbatim. reconciliationKey then lower-cases
// BOTH sides when an errors-document row is attributed to a job, so a difference in case can never
// split a match for any of the four identifiers.
//
// Without an assertion from the job side, losing that lower-casing would report contacts SendGrid
// actually rejected as delivered - the errored row would simply fail to attribute, and the job would
// succeed by exclusion.
func TestJobSideIdentifierCaseNormalization(t *testing.T) {
	t.Parallel()

	t.Run("the wire lower-cases the email and leaves the external id verbatim", func(t *testing.T) {
		t.Parallel()
		apiService := newAPIServiceMock(t)
		var captured sendgridbulkupload.UpsertRequest
		apiService.EXPECT().UploadContacts(gomock.Any()).Times(1).
			DoAndReturn(func(request sendgridbulkupload.UpsertRequest) (*sendgridbulkupload.UpsertResponse, error) {
				captured = request
				return &sendgridbulkupload.UpsertResponse{JobID: testImportJobID}, nil
			})

		staging := writeStagingFile(t, stagedLine(91,
			`{"type":"identify","userId":"User_MixedCase","anonymousId":"Anon_MixedCase",`+
				`"traits":{"email":"Mixed.Case@Example.COM"}}`))
		uploader := newUploader(t, apiService, testEventListID)
		output := uploader.Upload(asyncDestination(staging, []int64{91}))

		require.Equal(t, []int64{91}, output.ImportingJobIDs)
		require.Len(t, captured.Contacts, 1)
		require.Equal(t, "mixed.case@example.com", captured.Contacts[0].Email)
		require.Equal(t, "User_MixedCase", captured.Contacts[0].ExternalID)
		require.Equal(t, "Anon_MixedCase", captured.Contacts[0].AnonymousID)
	})

	t.Run("a lower-cased row still resolves to its mixed-case staged job", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name string
			row  string
		}{
			{
				// SendGrid lower-cases the email field itself, so this is the shape a real errors
				// document takes for a contact staged in mixed case.
				name: "keyed by email",
				row:  `{"email":"mixed.case@example.com","message":"invalid email address"}`,
			},
			{
				// external_id is sent verbatim, so only the reconciliation key can bridge a case
				// difference here - which is exactly why this case exists alongside the email one.
				name: "keyed by external id",
				row:  `{"external_id":"user_mixedcase","message":"invalid email address"}`,
			},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()
				apiService := newAPIServiceMock(t)
				apiService.EXPECT().GetImportErrors(testErrorsURL).Times(1).
					Return([]byte(`[`+testCase.row+`]`), nil)

				uploader := newUploader(t, apiService, testEventListID)
				response := uploader.GetUploadStats(common.GetUploadStatsInput{
					FailedJobParameters: testErrorsURL,
					Parameters:          importingParameters(t),
					ImportingList: []*jobsdb.JobT{
						importingJob(91, `{"type":"identify","userId":"User_MixedCase","traits":{"email":"Mixed.Case@Example.COM"}}`),
						importingJob(92, `{"type":"identify","userId":"User_Other","traits":{"email":"Other.Case@Example.COM"}}`),
					},
				})

				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Equal(t, []int64{91}, response.Metadata.FailedKeys)
				require.Equal(t, []int64{92}, response.Metadata.SucceededKeys)
				require.NotEmpty(t, response.Metadata.FailedReasons[91])
				require.Empty(t, response.Metadata.AbortedKeys)
				requireCarriesNoContactData(t, response.Metadata.FailedReasons[91])
			})
		}
	})
}

// TestUploadHonoursTheDocumentedContactCeiling anchors the chunker to the literal ceiling SendGrid
// documents for a single upsert - 30,000 contacts - rather than only to relative splitting.
//
// Both readings of the cap are pinned, because they fail differently: the SHIPPED DEFAULT is what
// production actually uses, while an OVERRIDE ABOVE THE CEILING must be clamped rather than obeyed,
// since obeying it would have the provider reject the whole request.
func TestUploadHonoursTheDocumentedContactCeiling(t *testing.T) {
	t.Parallel()

	const documentedContactCeiling = 30000

	// One contact more than a single request may carry, so the split is observable at the exact
	// boundary. Each record is deliberately small, so the contact count is the binding cap rather
	// than the byte budget.
	lines := make([]string, 0, documentedContactCeiling+1)
	for jobID := 1; jobID <= documentedContactCeiling+1; jobID++ {
		lines = append(lines, stagedLine(int64(jobID), fmt.Sprintf(
			`{"type":"identify","userId":"u%d","traits":{"email":"c%d@example.com"}}`, jobID, jobID)))
	}
	staging := writeStagingFile(t, lines...)

	cases := []struct {
		name        string
		maxContacts int
	}{
		{name: "under the shipped default", maxContacts: 0},
		{name: "under an override above the ceiling", maxContacts: documentedContactCeiling + 10000},
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
			output := uploader.Upload(asyncDestination(staging, func() []int64 {
				jobIDs := make([]int64, 0, documentedContactCeiling+1)
				for jobID := 1; jobID <= documentedContactCeiling+1; jobID++ {
					jobIDs = append(jobIDs, int64(jobID))
				}
				return jobIDs
			}()))

			require.Len(t, captured.Contacts, documentedContactCeiling,
				"one request carries exactly the documented ceiling, never more")
			require.Len(t, output.ImportingJobIDs, documentedContactCeiling)
			// The one contact that did not fit is deferred retryably, so nothing is lost to the cap.
			require.Equal(t, []int64{documentedContactCeiling + 1}, output.FailedJobIDs)
			require.Empty(t, output.AbortJobIDs)
			require.Contains(t, output.FailedReason, "deferred to a later upload")
		})
	}
}

// TestUploadListIDTypeDiscrimination pins the negative half of per-event list targeting.
//
// context.externalId is a shared array carrying every kind of external identifier a source may send,
// so the only thing that makes an entry list targeting is its "type". Without a negative case, a
// guard that accepted ANY entry would pass every positive test while quietly upserting contacts into
// whichever list an unrelated identifier happened to name.
func TestUploadListIDTypeDiscrimination(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		externalID      string
		expectedListIDs []string
	}{
		{
			name:            "an unrelated external id type alone falls back to the destination configuration",
			externalID:      `[{"type":"brazeExternalId","id":"` + testEventListID + `"}]`,
			expectedListIDs: []string{testConfigListID},
		},
		{
			name: "an unrelated external id type beside real list targeting is skipped",
			externalID: `[{"type":"brazeExternalId","id":"not-a-list"},` +
				`{"type":"listIds","id":["` + testEventListID + `"]}]`,
			expectedListIDs: []string{testEventListID},
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

			staging := writeStagingFile(t, stagedLine(101,
				`{"type":"identify","userId":"user_101","traits":{"email":"targeted@example.com"},`+
					`"context":{"externalId":`+testCase.externalID+`}}`))
			uploader := newUploader(t, apiService, testConfigListID)
			output := uploader.Upload(asyncDestination(staging, []int64{101}))

			require.Equal(t, []int64{101}, output.ImportingJobIDs)
			require.Equal(t, testCase.expectedListIDs, captured.ListIDs)
			require.Empty(t, output.FailedJobIDs)
			require.Empty(t, output.AbortJobIDs)
		})
	}
}
