package sendgridbulkupload

// WHY THIS FILE IS INTERNAL
//
// Every other test for this connector lives in the external sendgridbulkupload_test package, which is
// the convention and the right default: it exercises the connector through the surface the batch
// router actually uses. Three properties cannot be reached that way, and all three are transport
// policies rather than behavior:
//
//   - the two canonical endpoints are compile-time constants on api.sendgrid.com, so the redirect
//     policy of the client that calls them is not observable through the exported interface;
//   - the errors-document redirect policy is an http.Client.CheckRedirect closure, which no exported
//     symbol returns;
//   - the ACCEPTING side of the host allow list cannot be asserted from outside without dialing the
//     provider, and a unit test must never open a socket to a real host.
//
// Go permits a package and its external test package to coexist in one directory, so these live here,
// white-box, deliberately scoped to the transport boundary and nothing else.

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// errorsRedirectRequest builds the pair of requests Go hands to CheckRedirect: the request that is
// about to be sent, and the chain it came from. The Authorization and Referer headers are set exactly
// as Go sets them - Authorization is carried over from the original request, and Referer is derived
// from the PREVIOUS url, query string included - so what the policy is asked to protect here is what
// it is asked to protect in production.
func errorsRedirectRequest(t *testing.T, target string, chain ...string) (*http.Request, []*http.Request) {
	t.Helper()

	via := make([]*http.Request, 0, len(chain))
	for _, previous := range chain {
		request, err := http.NewRequest(http.MethodGet, previous, nil)
		require.NoError(t, err)
		via = append(via, request)
	}

	request, err := http.NewRequest(http.MethodGet, target, nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer SG.test-key-id.test-key-secret")
	if len(chain) > 0 {
		request.Header.Set("Referer", chain[len(chain)-1])
	}
	return request, via
}

// TestErrorsDocumentRedirectPolicy pins what the errors-document client does at a redirect.
//
// The document can be served from object storage through a PRE-SIGNED url whose query string IS the
// credential, and a redirect is where both credentials this request carries can escape: the bearer
// key in the Authorization header, and the pre-signed url itself in the Referer header Go builds from
// the previous hop. Go's own stripSensitiveHeaders removes Authorization and cookies on a
// cross-domain redirect but never touches Referer, so the deletion of Referer is this connector's
// responsibility and is asserted on every branch below, including the branches that refuse the hop.
func TestErrorsDocumentRedirectPolicy(t *testing.T) {
	t.Parallel()

	// A pre-signed url: the signature lives in the query string, which is exactly what Referer would
	// otherwise disclose to the next host.
	const presigned = "https://exports.sendgrid.net/imports/9/errors.json?X-Amz-Signature=deadbeefcafe&X-Amz-Expires=900"

	policy := newErrorsDocumentHTTPClient([]string{"sendgrid.com", "sendgrid.net"}).CheckRedirect
	require.NotNil(t, policy, "the errors document client must have a redirect policy at all")

	t.Run("a hop onto an allowed host is followed with neither credential", func(t *testing.T) {
		t.Parallel()

		request, via := errorsRedirectRequest(t, "https://exports.sendgrid.com/imports/9/errors.json", presigned)
		require.NotEmpty(t, request.Header.Get("Referer"), "the fixture must start with the header Go would set")

		require.NoError(t, policy(request, via))
		require.Empty(t, request.Header.Get("Authorization"),
			"the bearer credential must never cross a redirect")
		require.Empty(t, request.Header.Get("Referer"),
			"Referer carries the previous url including its signed query string and must never cross a redirect")
	})

	t.Run("a hop off the allowed hosts is refused and still strips both headers", func(t *testing.T) {
		t.Parallel()

		for _, target := range []string{
			"https://collector.attacker.example/errors.json",
			"https://sendgrid.com.attacker.example/errors.json",
			"https://exports.s3.amazonaws.com/errors.json",
		} {
			request, via := errorsRedirectRequest(t, target, presigned)

			err := policy(request, via)
			require.Error(t, err, "a hop must not be able to walk off the enumerated hosts: %s", target)
			require.Contains(t, err.Error(), "allow list")
			require.Empty(t, request.Header.Get("Authorization"),
				"the credential is deleted before the hop is judged, so a refusal cannot leak it either")
			require.Empty(t, request.Header.Get("Referer"),
				"the previous url is deleted before the hop is judged, so a refusal cannot leak it either")
		}
	})

	t.Run("a hop that breaks a transport rule is refused", func(t *testing.T) {
		t.Parallel()

		for target, reason := range map[string]string{
			"http://exports.sendgrid.net/errors.json":         "https is required",
			"https://exports.sendgrid.net:8443/errors.json":   "443 is required",
			"https://10.0.0.5/errors.json":                    "ip literal",
			"https://user:secret@exports.sendgrid.net/e.json": "user information",
		} {
			request, via := errorsRedirectRequest(t, target, presigned)

			err := policy(request, via)
			require.Error(t, err, "every hop is re-validated against the original policy: %s", target)
			require.Contains(t, err.Error(), reason)
		}
	})

	t.Run("the redirect chain is capped", func(t *testing.T) {
		t.Parallel()

		chain := make([]string, 0, maxErrorsDocumentRedirects)
		for range maxErrorsDocumentRedirects {
			chain = append(chain, presigned)
		}

		request, via := errorsRedirectRequest(t, "https://exports.sendgrid.net/errors.json", chain...)

		err := policy(request, via)
		require.Error(t, err, "an endless redirect chain must terminate on the connector's own cap")
		require.Contains(t, err.Error(), "redirected more than")
		require.Empty(t, request.Header.Get("Authorization"))
		require.Empty(t, request.Header.Get("Referer"))
	})

	t.Run("a policy built without an allow list is still fail closed", func(t *testing.T) {
		t.Parallel()

		// The redirect check is the path an attacker would use to leave an allowed host, so it must
		// not depend on the caller having remembered to resolve a list. An empty list is read as "no
		// effective list" and the shipped default is substituted.
		failClosed := newErrorsDocumentHTTPClient(nil).CheckRedirect
		require.NotNil(t, failClosed)

		request, via := errorsRedirectRequest(t, "https://collector.attacker.example/errors.json", presigned)

		err := failClosed(request, via)
		require.Error(t, err)
		require.Contains(t, err.Error(), "allow list")
	})
}

// TestRefererDeletionInCheckRedirectIsEffective pins the Go behavior the policy above depends on.
//
// Deleting Referer inside CheckRedirect is only worth doing if Go populates it BEFORE calling the
// hook and sends the request the hook mutated. That is current behavior, and it is not something this
// connector controls, so it is asserted directly: a control client leaks the previous url, including
// its query string, to the next hop, and a client that deletes the header in CheckRedirect does not.
// If a future Go release were to set Referer after the hook, this test fails and the policy above
// would have to move to a RoundTripper.
func TestRefererDeletionInCheckRedirectIsEffective(t *testing.T) {
	t.Parallel()

	var observed atomic.Value
	observed.Store("")

	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed.Store(r.Header.Get("Referer"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(final.Close)

	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+"/document", http.StatusFound)
	}))
	t.Cleanup(first.Close)

	// The query string stands in for a pre-signed signature.
	signed := first.URL + "/errors.json?X-Amz-Signature=deadbeefcafe"

	response, err := (&http.Client{}).Get(signed) //nolint:noctx // a fixture request against a local test server
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Contains(t, observed.Load().(string), "X-Amz-Signature",
		"control: Go forwards the previous url, query string included, unless it is deleted")

	observed.Store("")
	deleting := &http.Client{CheckRedirect: func(req *http.Request, _ []*http.Request) error {
		req.Header.Del("Referer")
		return nil
	}}

	response, err = deleting.Get(signed) //nolint:noctx // a fixture request against a local test server
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Empty(t, observed.Load().(string),
		"deleting Referer inside CheckRedirect must reach the next hop, or the errors document policy is cosmetic")
}

// TestSendGridAPIClientRefusesEveryRedirect pins the redirect policy of the client used for the two
// endpoints whose url is a compile-time constant.
//
// Both are documented, fixed and non-redirecting, so a 3xx from either is an anomaly. Following one
// would hand over two capabilities: Go attaches the bearer credential to a redirected request whose
// host matches or is a subdomain of the original, and a 307 or 308 replays the whole request body -
// for the upsert, every contact in the batch including their personal data - at the new location.
// This is asserted end to end against real servers: the second hop must never be reached, whatever
// the redirect status.
func TestSendGridAPIClientRefusesEveryRedirect(t *testing.T) {
	t.Parallel()

	for name, status := range map[string]int{
		"301 moved permanently":  http.StatusMovedPermanently,
		"302 found":              http.StatusFound,
		"307 temporary redirect": http.StatusTemporaryRedirect,
		"308 permanent redirect": http.StatusPermanentRedirect,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var secondHop atomic.Int32

			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				secondHop.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(target.Close)

			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+"/contacts", status)
			}))
			t.Cleanup(origin.Close)

			request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, origin.URL+"/v3/marketing/contacts",
				strings.NewReader(`{"contacts":[{"email":"alex@example.com"}]}`))
			require.NoError(t, err)
			request.Header.Set("Authorization", "Bearer SG.test-key-id.test-key-secret")
			request.Header.Set("Content-Type", "application/json")

			response, err := getDefaultHTTPClient().Do(request)
			if response != nil {
				require.NoError(t, response.Body.Close())
			}
			require.Error(t, err, "a redirect from a fixed sendgrid endpoint must not be followed")
			require.Contains(t, err.Error(), "does not follow")
			require.EqualValues(t, 0, secondHop.Load(),
				"the credential must not be forwarded and the payload must not be replayed at the redirect target")
		})
	}
}

// TestErrorsURLAllowListAcceptsTheHostsItMust asserts the accepting side of the default-deny host
// policy without opening a socket.
//
// A default-deny list that also refused SendGrid's own domains would stall reconciliation for every
// partially errored import out of the box, which is the failure the previous default-open policy was
// reaching for. Both halves therefore have to hold: SendGrid's hosts pass, and nothing else does.
func TestErrorsURLAllowListAcceptsTheHostsItMust(t *testing.T) {
	t.Parallel()

	t.Run("the shipped default accepts sendgrid's own hosts", func(t *testing.T) {
		t.Parallel()

		for _, accepted := range []string{
			"https://api.sendgrid.com/v3/marketing/contacts/imports/9/errors",
			"https://sendgrid.com/errors.json",
			"https://exports.sendgrid.net/imports/9/errors.json?X-Amz-Signature=deadbeef",
			"https://us-east-1.exports.sendgrid.net:443/errors.json",
		} {
			parsed, err := validateErrorsURL(accepted, defaultErrorsURLAllowedHosts)
			require.NoError(t, err, "a sendgrid host must pass the default allow list: %s", accepted)
			require.NotNil(t, parsed)
		}
	})

	t.Run("the shipped default refuses everything else", func(t *testing.T) {
		t.Parallel()

		for _, refused := range []string{
			"https://collector.attacker.example/errors.json",
			"https://sendgrid.com.attacker.example/errors.json",
			"https://notsendgrid.com/errors.json",
			"https://exports.s3.amazonaws.com/errors.json",
		} {
			parsed, err := validateErrorsURL(refused, defaultErrorsURLAllowedHosts)
			require.Error(t, err, "an unenumerated host must be refused: %s", refused)
			require.Nil(t, parsed)
			reason, classified := errorsURLRejectionReasonOf(err)
			require.True(t, classified, "the refusal must be a classified rejection, not an opaque error")
			require.Equal(t, rejectionReasonHostNotAllowed, reason)
		}
	})

	t.Run("an empty list is fail closed rather than permissive", func(t *testing.T) {
		t.Parallel()

		parsed, err := validateErrorsURL("https://collector.attacker.example/errors.json", nil)
		require.Error(t, err, "an empty list is a caller without a list, never a permissive one")
		require.Nil(t, parsed)
		reason, classified := errorsURLRejectionReasonOf(err)
		require.True(t, classified)
		require.Equal(t, rejectionReasonHostNotAllowed, reason)

		parsed, err = validateErrorsURL("https://api.sendgrid.com/errors.json", nil)
		require.NoError(t, err, "the substituted list is the default, so sendgrid's hosts still pass")
		require.NotNil(t, parsed)
	})
}

// TestErrorsDocumentArrayIsStreamedNotMaterialized pins that the row cap is applied WHILE the
// document is read rather than after it has been decoded.
//
// gjson.Result.Array() decodes and retains every element before the first one is looked at, so a cap
// checked against the resulting slice bounds nothing: the allocation it exists to prevent has already
// happened. A provider response - or a compromised object-storage document - carrying millions of
// tiny entries could exhaust a shared batch router worker with the bound sitting one line below the
// allocation. Two independent properties are asserted here, and neither is observable from outside
// the package:
//
//   - the scan STOPS at the first entry past the budget, so nothing beyond it is ever decoded;
//   - the whole refusal allocates a small multiple of the accepted entries rather than a multiple of
//     the document's entry count, which is what "streamed" actually means.
//
// Sequential, because the allocation measurement reads a process-wide counter.
func TestErrorsDocumentArrayIsStreamedNotMaterialized(t *testing.T) {
	const (
		entries   = 200_000
		rowBudget = 8
	)

	document := []byte(`[` + strings.Repeat(`{"email":"over@example.com"},`, entries-1) + `{"email":"last@example.com"}]`)

	scan := streamImportErrorRows(gjson.ParseBytes(document), rowBudget)
	require.True(t, scan.overBudget, "a document holding %d entries must break a budget of %d", entries, rowBudget)
	require.Equal(t, rowBudget+1, scan.scanned,
		"the scan must stop at the first entry past the budget, not read the rest of the document")
	require.Nil(t, scan.rows, "a partial reading of a refused document must not be handed back")

	// Parsed OUTSIDE the measured region on purpose: gjson.ParseBytes converts the document to a
	// string, which copies it once, and that copy is a property of gjson's API rather than of this
	// scan. It is bounded by the errors-document read budget, so what is measured here is only what
	// the scan itself allocates on top of the document it was handed.
	parsed := gjson.ParseBytes(document)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	scan = streamImportErrorRows(parsed, rowBudget)
	runtime.ReadMemStats(&after)
	require.True(t, scan.overBudget)

	// Materializing 200k gjson results costs well over ten megabytes; streaming nine of them costs
	// kilobytes. The threshold is deliberately generous - this is a regression guard against
	// wholesale materialization, not a memory benchmark.
	const allocationCeiling = 1 << 20
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(allocationCeiling),
		"refusing an oversized document must not first decode all of it")

	// The same property through the parser's own entry point, for both array shapes.
	for _, oversized := range [][]byte{
		document,
		[]byte(`{"errors":` + string(document) + `}`),
	} {
		parsed, err := parseImportErrors(oversized, rowBudget)
		require.Error(t, err)
		require.Contains(t, err.Error(), "still allowed for this import")
		require.Empty(t, parsed.Rows)
	}
}

// TestErrorsDocumentScanChargesEveryEntry pins the accounting the aggregate budget depends on: an
// entry the parser declines is still an entry that was read.
func TestErrorsDocumentScanChargesEveryEntry(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		document          string
		expectedRows      int
		expectedUnread    int
		expectedScanned   int
		expectedOverspent bool
	}{
		"every entry reduces": {
			document:        `[{"email":"a@example.com"},{"email":"b@example.com"}]`,
			expectedRows:    2,
			expectedScanned: 2,
		},
		"an entry with no known field is charged": {
			document:        `[{"email":"a@example.com"},{"unknown":"x"},{"another":"y"}]`,
			expectedRows:    1,
			expectedUnread:  2,
			expectedScanned: 3,
		},
		"an entry that is not an object is charged": {
			document:        `[{"email":"a@example.com"},"not an object",42,null]`,
			expectedRows:    1,
			expectedUnread:  3,
			expectedScanned: 4,
		},
		"unreadable entries can break the budget on their own": {
			document:          `[{"unknown":"x"},{"unknown":"y"},{"unknown":"z"}]`,
			expectedScanned:   3,
			expectedOverspent: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			budget := 2
			if !testCase.expectedOverspent {
				budget = testCase.expectedScanned
			}

			scan := streamImportErrorRows(gjson.Parse(testCase.document), budget)

			require.Equal(t, testCase.expectedOverspent, scan.overBudget)
			require.Equal(t, testCase.expectedScanned, scan.scanned)
			if testCase.expectedOverspent {
				return
			}
			require.Len(t, scan.rows, testCase.expectedRows)
			require.Equal(t, testCase.expectedUnread, scan.unrecognized)
		})
	}
}

// TestResolveErrorsURLAllowedHostsFallsBackToTheDefault covers the configuration seam of the same
// policy. Sequential, because it writes the process-wide config singleton.
func TestResolveErrorsURLAllowedHostsFallsBackToTheDefault(t *testing.T) {
	for name, testCase := range map[string]struct {
		configured any
		expected   []string
	}{
		"unset uses the default":            {configured: nil, expected: []string{"sendgrid.com", "sendgrid.net"}},
		"blanks fall back to the default":   {configured: []string{"  ", "", "."}, expected: []string{"sendgrid.com", "sendgrid.net"}},
		"an override replaces the default":  {configured: []string{"exports.example.net"}, expected: []string{"exports.example.net"}},
		"entries are normalized and unique": {configured: []string{".SendGrid.COM.", "sendgrid.com"}, expected: []string{"sendgrid.com"}},
	} {
		t.Run(name, func(t *testing.T) {
			if testCase.configured != nil {
				t.Cleanup(config.Reset)
				config.Set("BatchRouter."+destName+"."+configKeyErrorsURLAllowedHosts, testCase.configured)
			}

			require.Equal(t, testCase.expected, resolveErrorsURLAllowedHosts())
		})
	}
}

// TestManifestBudgetMirrorIsAccurate pins the literal the external suite mirrors for
// maxImportManifestBytes.
//
// The external suite cannot read an unexported constant, and exporting one so a test can see it
// would widen the package's API for no production reason. So it mirrors the literal instead, and
// this test - which CAN see the real constant - fails the moment the two disagree, which is what
// stops the mirror from silently going stale and quietly weakening the ceiling assertion over
// there into a tautology.
func TestManifestBudgetMirrorIsAccurate(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1024, maxImportManifestBytes,
		"persistedManifestBudget in sendgridbulkupload_test.go mirrors this value and must be updated with it")
}

// TestImportBudgetOverrideIsClamped pins the import-count clamp on the accessor that applies it.
//
// Pinned here rather than through Upload because Upload can no longer reach it: the manifest byte
// budget bounds the identifiers an upload may record, and 512 distinct identifiers cannot fit
// maxImportManifestBytes at any realistic length. The clamp is still a real guard - it is the
// second of the two bounds, and the one that would bind if the manifest budget were ever raised -
// so it is asserted directly rather than left unpinned.
func TestImportBudgetOverrideIsClamped(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		configured int
		expected   int
	}{
		"an absurd override is clamped to the ceiling": {configured: 10_000_000, expected: maxImportsPerUploadCeiling},
		"the ceiling itself is honored":                {configured: maxImportsPerUploadCeiling, expected: maxImportsPerUploadCeiling},
		"one past the ceiling is clamped":              {configured: maxImportsPerUploadCeiling + 1, expected: maxImportsPerUploadCeiling},
		"a value inside the ceiling is honored":        {configured: 7, expected: 7},
		"an unset override falls back to the default":  {configured: 0, expected: defaultMaxImportsPerUpload},
		"a negative override falls back too":           {configured: -1, expected: defaultMaxImportsPerUpload},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			uploader := &SendGridBulkUploader{MaxImportsPerUpload: testCase.configured}
			require.Equal(t, testCase.expected, uploader.maxImportsPerUpload())
		})
	}
}
