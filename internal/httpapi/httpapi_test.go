package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/delivery"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
)

type stubReader struct {
	datasets         []delivery.DatasetDescriptor
	listDatasetsFn   func(context.Context) ([]delivery.DatasetDescriptor, error)
	scopes           []delivery.ScopeDescriptor
	rawSnapshots     []delivery.SnapshotDescriptor
	replay           []delivery.ReplaySnapshotDescriptor
	resolvedRaw      delivery.ResolvedSnapshot
	resolveRawErr    error
	rawPlan          delivery.FetchPlan
	buildPlanCalls   int
	resolvedReplay   delivery.ResolvedReplaySnapshot
	resolveReplayErr error
	replayPlan       delivery.ReplayFetchPlan
	buildReplayCalls int
	fetchCalls       int
	fetchReplayCalls int
}

func (s *stubReader) ListDatasets(ctx context.Context) ([]delivery.DatasetDescriptor, error) {
	if s.listDatasetsFn != nil {
		return s.listDatasetsFn(ctx)
	}
	return s.datasets, nil
}
func (s *stubReader) ListScopes(context.Context, string) ([]delivery.ScopeDescriptor, error) {
	return s.scopes, nil
}
func (s *stubReader) ListRawSnapshots(context.Context, delivery.RawDayScope) ([]delivery.SnapshotDescriptor, error) {
	return s.rawSnapshots, nil
}
func (s *stubReader) ResolveSnapshot(context.Context, delivery.SnapshotSelector) (delivery.ResolvedSnapshot, error) {
	if s.resolveRawErr != nil {
		return delivery.ResolvedSnapshot{}, s.resolveRawErr
	}
	return s.resolvedRaw, nil
}
func (s *stubReader) BuildFetchPlan(context.Context, delivery.ResolvedSnapshot) (delivery.FetchPlan, error) {
	s.buildPlanCalls++
	return s.rawPlan, nil
}
func (s *stubReader) Fetch(context.Context, delivery.FetchPlan, string) (delivery.FetchResult, error) {
	s.fetchCalls++
	return delivery.FetchResult{}, nil
}
func (s *stubReader) VerifyDay(context.Context, delivery.SnapshotSelector) (delivery.DayVerificationReport, error) {
	return delivery.DayVerificationReport{}, nil
}
func (s *stubReader) VerifyScope(context.Context, delivery.RawScopeSelector, string) (delivery.ScopeVerificationReport, error) {
	return delivery.ScopeVerificationReport{}, nil
}
func (s *stubReader) ListReplaySnapshots(context.Context, delivery.ReplayDayScope) ([]delivery.ReplaySnapshotDescriptor, error) {
	return s.replay, nil
}
func (s *stubReader) ResolveReplaySnapshot(context.Context, delivery.ReplaySnapshotSelector) (delivery.ResolvedReplaySnapshot, error) {
	if s.resolveReplayErr != nil {
		return delivery.ResolvedReplaySnapshot{}, s.resolveReplayErr
	}
	return s.resolvedReplay, nil
}
func (s *stubReader) BuildReplayFetchPlan(context.Context, delivery.ResolvedReplaySnapshot) (delivery.ReplayFetchPlan, error) {
	s.buildReplayCalls++
	return s.replayPlan, nil
}
func (s *stubReader) FetchReplay(context.Context, delivery.ReplayFetchPlan, string) (delivery.ReplayFetchResult, error) {
	s.fetchReplayCalls++
	return delivery.ReplayFetchResult{}, nil
}
func (s *stubReader) VerifyReplayDay(context.Context, delivery.ReplaySnapshotSelector) (delivery.ReplayDayVerificationReport, error) {
	return delivery.ReplayDayVerificationReport{}, nil
}

func newHTTPAPIStub() *stubReader {
	var digest [32]byte
	digest[0] = 1
	return &stubReader{
		datasets:       []delivery.DatasetDescriptor{{DatasetID: "dataset-1"}},
		scopes:         []delivery.ScopeDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", StableFeedID: "feed-1", ExactSourceSymbol: "EURUSD.raw", BrokerServerFingerprint: "broker-1", DayDefinitionID: "utc-day-v1", PublisherID: "publisher-1", PublisherEpoch: 1, ConfigHash: digest}},
		rawSnapshots:   []delivery.SnapshotDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", ExactSourceSymbol: "EURUSD.raw", DayDefinitionID: "utc-day-v1", Date: "2024-01-01", Revision: 1, PublisherID: "publisher-1", PublisherEpoch: 1, ManifestKey: "v1/raw-manifest", ManifestSHA256: digest, ChainSliceStart: 1, ChainSliceStartRoot: digest, ChainSliceEnd: 1, ChainSliceEndRoot: digest, AcceptedRecordCount: 2}},
		replay:         []delivery.ReplaySnapshotDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", ExactSourceSymbol: "EURUSD.raw", DayDefinitionID: "utc-day-v1", Date: "2024-01-01", ReplayContractID: "replay-v1", ConversionID: "conversion-v1", Revision: 1, ManifestKey: "v1/replay-manifest", ManifestSHA256: digest, RawDayManifestKey: "v1/raw-manifest", RawDayManifestSHA256: digest, PartSetRoot: digest, CanonicalStreamRowChainRoot: digest, PartCount: 1}},
		resolvedRaw:    delivery.ResolvedSnapshot{Manifest: archive.RawDayManifest{Revision: 1, Date: "2024-01-01"}, ManifestKey: "v1/raw-manifest", ManifestBytes: []byte(`{"manifest":"raw"}`), ManifestSHA256: digest},
		rawPlan:        delivery.FetchPlan{ManifestKey: "v1/raw-manifest", ManifestSHA256: digest, ManifestBytes: []byte(`{"manifest":"raw"}`), Objects: []delivery.FetchObject{{Key: "raw/object", SHA256: digest, Bytes: 12}}},
		resolvedReplay: delivery.ResolvedReplaySnapshot{Manifest: protocol.ReplayDayManifest{Revision: 1, Date: "2024-01-01"}, ManifestKey: "v1/replay-manifest", ManifestBytes: []byte(`{"manifest":"replay"}`), ManifestSHA256: digest},
		replayPlan:     delivery.ReplayFetchPlan{Manifest: delivery.ReplayFetchObject{Kind: delivery.ReplayFetchManifest, Key: "v1/replay-manifest", Digest: digest, Bytes: 22}, Parts: []delivery.ReplayFetchObject{{Kind: delivery.ReplayFetchPartManifest, Key: "v1/part", Digest: digest, Bytes: 10}}, Parquet: []delivery.ReplayFetchObject{{Kind: delivery.ReplayFetchParquet, Key: "v1/part.parquet", Digest: digest, Bytes: 20}}},
	}
}

func newHTTPAPIHandler(t *testing.T, reader delivery.ArchiveReaderV1) *Handler {
	t.Helper()
	handler, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "127.0.0.1:17002", Limits: operations.DefaultResourceLimits})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestHTTPAPIMapsReaderAndPlanContracts(t *testing.T) {
	reader := newHTTPAPIStub()
	handler := newHTTPAPIHandler(t, reader)

	for _, test := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "datasets", method: http.MethodGet, path: "/v1/datasets"},
		{name: "scopes", method: http.MethodGet, path: "/v1/datasets/dataset-1/scopes"},
		{name: "raw snapshots", method: http.MethodGet, path: "/v1/snapshots/raw?dataset=dataset-1&source=source-1&symbol=EURUSD.raw&date=2024-01-01"},
		{name: "replay snapshots", method: http.MethodGet, path: "/v1/snapshots/replay?dataset=dataset-1&source=source-1&symbol=EURUSD.raw&date=2024-01-01&stream=replay-v1&conversion=conversion-v1"},
		{name: "manifest", method: http.MethodGet, path: "/v1/manifests/0100000000000000000000000000000000000000000000000000000000000000"},
		{name: "health", method: http.MethodGet, path: "/v1/health"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := request(t, handler, test.method, test.path, test.body)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("X-Request-ID") == "" {
				t.Fatalf("security headers missing: %+v", response.Header())
			}
			var payload struct {
				APIVersion string            `json:"api_version"`
				Items      []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.APIVersion != APIVersion || len(payload.Items) == 0 {
				t.Fatalf("invalid API envelope: %s err=%v", response.Body.String(), err)
			}
		})
	}

	response := request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"raw","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("raw fetch plan status=%d body=%s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"key":"raw/object"`)) || !bytes.Contains(response.Body.Bytes(), []byte(`"sha256":"0100000000000000000000000000000000000000000000000000000000000000"`)) {
		t.Fatalf("fetch plan omitted immutable object identity: %s", response.Body.String())
	}
	response = request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"replay","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"key":"v1/part.parquet"`)) {
		t.Fatalf("replay fetch plan status=%d body=%s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"raw","selector":{"key":"arbitrary"}}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown selector field status=%d body=%s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"raw","selector":{"manifest":"v1/raw-manifest"}}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("manifest key selector status=%d body=%s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodPost, "/v1/fetch-plans?unexpected=x", `{"kind":"raw","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("fetch-plan unknown query status=%d body=%s", response.Code, response.Body.String())
	}
	if reader.fetchCalls != 0 || reader.fetchReplayCalls != 0 {
		t.Fatalf("HTTP API invoked body fetch: raw=%d replay=%d", reader.fetchCalls, reader.fetchReplayCalls)
	}
}

func TestHTTPAPIRejectsUnboundedAndUnsafeRequests(t *testing.T) {
	reader := newHTTPAPIStub()
	limits := operations.DefaultResourceLimits
	limits.MaxAPIRequestBytes = 128
	handler, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "127.0.0.1:17002", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if response := request(t, handler, http.MethodGet, "/v1/datasets?unexpected=x", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("unknown query status=%d", response.Code)
	}
	if response := request(t, handler, http.MethodGet, "/v1/snapshots/raw?dataset=dataset-1&source=source-1&symbol=EURUSD.raw&date=2024-02-30", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid date status=%d", response.Code)
	}
	if response := request(t, handler, http.MethodGet, "/v1/snapshots/raw?dataset=a%2Fb&source=s&symbol=EURUSD", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid identity status=%d", response.Code)
	}
	if response := request(t, handler, http.MethodPost, "/v1/fetch-plans", strings.Repeat("x", 128)); response.Code != http.StatusRequestEntityTooLarge && response.Code != http.StatusBadRequest {
		t.Fatalf("oversized request status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "0.0.0.0:17002", Limits: operations.DefaultResourceLimits}); err == nil {
		t.Fatal("non-loopback API bind was accepted without explicit security policy")
	}
}

func TestHTTPAPIPropagatesTimeoutAndRejectsMalformedQuery(t *testing.T) {
	reader := newHTTPAPIStub()
	reader.listDatasetsFn = func(ctx context.Context) ([]delivery.DatasetDescriptor, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	limits := operations.DefaultResourceLimits
	limits.RequestTimeoutMS = 5
	handler, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "127.0.0.1:17002", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if response := request(t, handler, http.MethodGet, "/v1/datasets?dataset=%zz", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("malformed query status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(t, handler, http.MethodGet, "/v1/datasets", ""); response.Code != http.StatusGatewayTimeout {
		t.Fatalf("timeout status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPIMapsBodyLimitTo413(t *testing.T) {
	reader := newHTTPAPIStub()
	limits := operations.DefaultResourceLimits
	limits.MaxAPIRequestBytes = 128
	handler, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "127.0.0.1:17002", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"kind":"raw","selector":{"manifest":"` + strings.Repeat("a", 256) + `"}}`
	response := request(t, handler, http.MethodPost, "/v1/fetch-plans", body)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized valid JSON status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPIMapsUnknownFetchPlanTo404(t *testing.T) {
	reader := newHTTPAPIStub()
	reader.resolveRawErr = delivery.ErrSelectorNotFound
	reader.resolveReplayErr = delivery.ErrSelectorNotFound
	handler := newHTTPAPIHandler(t, reader)
	response := request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"raw","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown manifest status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPIMapsIntegrityConflictTo409(t *testing.T) {
	reader := newHTTPAPIStub()
	reader.resolveRawErr = archive.ErrIntegrity
	handler := newHTTPAPIHandler(t, reader)
	response := request(t, handler, http.MethodPost, "/v1/fetch-plans", `{"kind":"raw","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`)
	if response.Code != http.StatusConflict {
		t.Fatalf("integrity conflict status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPIFetchPlanChecksManifestItemLimitBeforeBuild(t *testing.T) {
	reader := newHTTPAPIStub()
	reader.resolvedRaw.Manifest.ChainObjects = []archive.RawChainObject{{}, {}}
	reader.resolvedReplay.Manifest.PartManifestKeys = []string{"part-1", "part-2"}
	limits := operations.DefaultResourceLimits
	limits.MaxAPIResponseItems = 2
	handler, err := NewHandler(reader, Config{Version: APIConfigVersion, ReaderConfig: "reader.toml", ListenAddress: "127.0.0.1:17002", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"kind":"raw","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`,
		`{"kind":"replay","selector":{"manifest":"0100000000000000000000000000000000000000000000000000000000000000"}}`,
	} {
		response := request(t, handler, http.MethodPost, "/v1/fetch-plans", body)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized manifest plan status=%d body=%s", response.Code, response.Body.String())
		}
	}
	if reader.buildPlanCalls != 0 || reader.buildReplayCalls != 0 {
		t.Fatalf("fetch-plan builder was called before limit check: raw=%d replay=%d", reader.buildPlanCalls, reader.buildReplayCalls)
	}
}

func TestHTTPAPIManifestDoesNotMaskMixedResolutionConflict(t *testing.T) {
	reader := newHTTPAPIStub()
	reader.resolveRawErr = delivery.ErrSelectorNotFound
	reader.resolveReplayErr = archive.ErrIntegrity
	handler := newHTTPAPIHandler(t, reader)
	response := request(t, handler, http.MethodGet, "/v1/manifests/0100000000000000000000000000000000000000000000000000000000000000", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("mixed manifest resolution status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPIRejectsBodiesOnGETEndpoints(t *testing.T) {
	handler := newHTTPAPIHandler(t, newHTTPAPIStub())
	response := request(t, handler, http.MethodGet, "/v1/datasets", "unexpected body")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("GET body status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestHTTPAPINonLoopbackRequiresAndRunsPolicyHooks(t *testing.T) {
	called := map[string]bool{}
	config := Config{
		Version:       APIConfigVersion,
		ReaderConfig:  "reader.toml",
		ListenAddress: "192.0.2.10:17002",
		Limits:        operations.DefaultResourceLimits,
		Authenticate: func(*http.Request) error {
			called["authenticate"] = true
			return nil
		},
		RateLimit: func(*http.Request) (func(), error) {
			called["rate_limit"] = true
			return func() { called["release"] = true }, nil
		},
		TrustedProxy: func(*http.Request) error {
			called["trusted_proxy"] = true
			return nil
		},
		ShortLivedCredential: func(*http.Request) error {
			called["short_lived_credential"] = true
			return nil
		},
	}
	handler, err := NewHandler(newHTTPAPIStub(), config)
	if err != nil {
		t.Fatal(err)
	}
	if response := request(t, handler, http.MethodGet, "/v1/datasets", ""); response.Code != http.StatusOK {
		t.Fatalf("policy-protected request status=%d body=%s", response.Code, response.Body.String())
	}
	for _, name := range []string{"authenticate", "rate_limit", "trusted_proxy", "short_lived_credential", "release"} {
		if !called[name] {
			t.Fatalf("policy hook %q was not called", name)
		}
	}
}
