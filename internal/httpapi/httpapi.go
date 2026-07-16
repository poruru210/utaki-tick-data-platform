package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/delivery"
	"tick-data-platform/internal/r2"
)

const APIVersion = "tick-api-v1"

type Handler struct {
	reader     delivery.ArchiveReaderV1
	config     Config
	concurrent chan struct{}
	requestID  atomic.Uint64
}

func NewHandler(reader delivery.ArchiveReaderV1, config Config) (*Handler, error) {
	if reader == nil {
		return nil, fmt.Errorf("archive reader is nil")
	}
	config = config.withDefaults()
	if config.Health == nil {
		config.Health = defaultHealth
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Handler{
		reader: reader, config: config,
		concurrent: make(chan struct{}, config.Limits.MaxConcurrentRequests),
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := fmt.Sprintf("tick-api-%d", h.requestID.Add(1))
	w.Header().Set("Cache-Control", h.config.CacheControl)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Request-ID", requestID)
	select {
	case h.concurrent <- struct{}{}:
		defer func() { <-h.concurrent }()
	default:
		h.writeError(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS")
		return
	}
	timeout, err := h.config.Limits.RequestTimeout()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "CONFIGURATION_ERROR")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	r = r.WithContext(ctx)
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, int64(h.config.Limits.MaxAPIRequestBytes))
	}
	if h.config.RateLimit != nil {
		release, err := h.config.RateLimit(r)
		if err != nil {
			h.writeError(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS")
			return
		}
		if release != nil {
			defer release()
		}
	}
	if h.config.TrustedProxy != nil {
		if err := h.config.TrustedProxy(r); err != nil {
			h.writeError(w, http.StatusForbidden, "FORBIDDEN")
			return
		}
	}
	if h.config.ShortLivedCredential != nil {
		if err := h.config.ShortLivedCredential(r); err != nil {
			h.writeError(w, http.StatusUnauthorized, "UNAUTHORIZED")
			return
		}
	}
	if h.config.Authenticate != nil {
		if err := h.config.Authenticate(r); err != nil {
			h.writeError(w, http.StatusUnauthorized, "UNAUTHORIZED")
			return
		}
	}
	if r.Method != http.MethodPost && requestHasBody(r) {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	if len(r.URL.RawQuery) > int(h.config.Limits.MaxAPIRequestBytes) {
		h.writeError(w, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE")
		return
	}
	h.route(w, r)
}

func (h *Handler) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/datasets":
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.listDatasets(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/datasets/") && strings.HasSuffix(r.URL.Path, "/campaigns"):
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.listCampaigns(w, r)
	case r.URL.Path == "/v1/snapshots/raw":
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.listRawSnapshots(w, r)
	case r.URL.Path == "/v1/snapshots/replay":
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.listReplaySnapshots(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/manifests/"):
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.getManifest(w, r)
	case r.URL.Path == "/v1/fetch-plans":
		if r.Method != http.MethodPost {
			h.methodNotAllowed(w, http.MethodPost)
			return
		}
		h.createFetchPlan(w, r)
	case r.URL.Path == "/v1/health":
		if r.Method != http.MethodGet {
			h.methodNotAllowed(w, http.MethodGet)
			return
		}
		h.health(w, r)
	default:
		h.writeError(w, http.StatusNotFound, "NOT_FOUND")
	}
}

func (h *Handler) listDatasets(w http.ResponseWriter, r *http.Request) {
	query, err := strictQuery(r, nil)
	if err != nil || len(query) != 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	items, err := h.reader.ListDatasets(r.Context())
	if err != nil {
		h.writeReaderError(w, err)
		return
	}
	if !h.checkItemLimit(w, len(items)) {
		return
	}
	result := make([]datasetItem, len(items))
	for index, item := range items {
		result[index] = datasetItem{DatasetID: item.DatasetID}
	}
	h.writeItems(w, result)
}

func (h *Handler) listCampaigns(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "datasets" || parts[3] != "campaigns" {
		h.writeError(w, http.StatusBadRequest, "INVALID_PATH")
		return
	}
	dataset, ok := pathSegment(parts[2])
	if !ok {
		h.writeError(w, http.StatusBadRequest, "INVALID_PATH")
		return
	}
	if query, err := strictQuery(r, nil); err != nil || len(query) != 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	items, err := h.reader.ListCampaigns(r.Context(), dataset)
	if err != nil {
		h.writeReaderError(w, err)
		return
	}
	if !h.checkItemLimit(w, len(items)) {
		return
	}
	result := make([]campaignItem, len(items))
	for index, item := range items {
		result[index] = campaignItem{
			DatasetID: item.DatasetID, CampaignID: item.CampaignID, ProviderID: item.ProviderID,
			StableFeedID: item.StableFeedID, ExactSourceSymbol: item.ExactSourceSymbol,
			BrokerServerFingerprint: item.BrokerServerFingerprint, DayDefinitionID: item.DayDefinitionID,
			PublisherID: item.PublisherID, PublisherEpoch: item.PublisherEpoch, ConfigHash: hash(item.ConfigHash),
		}
	}
	h.writeItems(w, result)
}

func (h *Handler) listRawSnapshots(w http.ResponseWriter, r *http.Request) {
	query, err := strictQuery(r, map[string]bool{"dataset": true, "campaign": true, "date": true})
	if err != nil || !validIdentityQuery(query["dataset"]) || !validIdentityQuery(query["campaign"]) || (query["date"] != "" && !validUTCDate(query["date"])) {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	items, err := h.reader.ListRawSnapshots(r.Context(), delivery.RawDayScope{DatasetID: query["dataset"], CampaignID: query["campaign"], Date: query["date"]})
	if err != nil {
		h.writeReaderError(w, err)
		return
	}
	if !h.checkItemLimit(w, len(items)) {
		return
	}
	result := make([]rawSnapshotItem, len(items))
	for index, item := range items {
		result[index] = rawSnapshotItem{
			DatasetID: item.DatasetID, CampaignID: item.CampaignID, DayDefinitionID: item.DayDefinitionID,
			Date: item.Date, Revision: item.Revision, PublisherID: item.PublisherID, PublisherEpoch: item.PublisherEpoch,
			ManifestKey: item.ManifestKey, ManifestSHA256: hash(item.ManifestSHA256), ChainSliceStart: item.ChainSliceStart,
			ChainSliceStartRoot: hash(item.ChainSliceStartRoot), ChainSliceEnd: item.ChainSliceEnd,
			ChainSliceEndRoot: hash(item.ChainSliceEndRoot), AcceptedRecordCount: item.AcceptedRecordCount, ErrorCount: item.ErrorCount,
		}
	}
	h.writeItems(w, result)
}

func (h *Handler) listReplaySnapshots(w http.ResponseWriter, r *http.Request) {
	query, err := strictQuery(r, map[string]bool{"dataset": true, "campaign": true, "date": true, "stream": true, "conversion": true, "day_definition": true})
	if err != nil || !validIdentityQuery(query["dataset"]) || !validIdentityQuery(query["campaign"]) || !validUTCDate(query["date"]) || !validIdentityQuery(query["stream"]) || !validIdentityQuery(query["conversion"]) || (query["day_definition"] != "" && !validIdentityQuery(query["day_definition"])) {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	items, err := h.reader.ListReplaySnapshots(r.Context(), delivery.ReplayDayScope{
		DatasetID: query["dataset"], CampaignID: query["campaign"], DayDefinitionID: query["day_definition"], Date: query["date"],
		ReplayContractID: query["stream"], ConversionID: query["conversion"],
	})
	if err != nil {
		h.writeReaderError(w, err)
		return
	}
	if !h.checkItemLimit(w, len(items)) {
		return
	}
	result := make([]replaySnapshotItem, len(items))
	for index, item := range items {
		previous := ""
		if item.PreviousManifestSHA256 != nil {
			previous = hash(*item.PreviousManifestSHA256)
		}
		result[index] = replaySnapshotItem{
			DatasetID: item.DatasetID, CampaignID: item.CampaignID, DayDefinitionID: item.DayDefinitionID,
			Date: item.Date, ReplayContractID: item.ReplayContractID, ConversionID: item.ConversionID,
			Revision: item.Revision, ManifestKey: item.ManifestKey, ManifestSHA256: hash(item.ManifestSHA256),
			PreviousManifestSHA256: previous, RawDayManifestKey: item.RawDayManifestKey,
			RawDayManifestSHA256: hash(item.RawDayManifestSHA256), PartSetRoot: hash(item.PartSetRoot),
			CanonicalStreamRowChainRoot: hash(item.CanonicalStreamRowChainRoot), PartCount: item.PartCount,
		}
	}
	h.writeItems(w, result)
}

func (h *Handler) getManifest(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "v1" || parts[1] != "manifests" || !validDigest(parts[2]) {
		h.writeError(w, http.StatusBadRequest, "INVALID_SELECTOR")
		return
	}
	if query, err := strictQuery(r, nil); err != nil || len(query) != 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	selector := parts[2]
	raw, rawErr := h.reader.ResolveSnapshot(r.Context(), delivery.SnapshotSelector{Manifest: selector})
	if rawErr == nil {
		h.writeItems(w, []manifestItem{rawManifestItem(raw)})
		return
	}
	if !errors.Is(rawErr, delivery.ErrSelectorNotFound) {
		h.writeReaderError(w, rawErr)
		return
	}
	replay, replayErr := h.reader.ResolveReplaySnapshot(r.Context(), delivery.ReplaySnapshotSelector{Manifest: selector})
	if replayErr == nil {
		h.writeItems(w, []manifestItem{replayManifestItem(replay)})
		return
	}
	if !errors.Is(replayErr, delivery.ErrSelectorNotFound) {
		h.writeReaderError(w, replayErr)
		return
	}
	h.writeError(w, http.StatusNotFound, "MANIFEST_NOT_FOUND")
}

func (h *Handler) createFetchPlan(w http.ResponseWriter, r *http.Request) {
	if query, err := strictQuery(r, nil); err != nil || len(query) != 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	var request fetchPlanRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		h.writeDecodeError(w, err)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		h.writeDecodeError(w, err)
		return
	}
	if request.Kind == "" || len(request.Selector) == 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	switch request.Kind {
	case "raw":
		var selector rawFetchSelector
		if err := decodeSelector(request.Selector, &selector); err != nil || !validDigest(selector.Manifest) {
			h.writeError(w, http.StatusBadRequest, "INVALID_SELECTOR")
			return
		}
		resolved, err := h.reader.ResolveSnapshot(r.Context(), delivery.SnapshotSelector{Manifest: selector.Manifest})
		if err != nil {
			h.writeReaderError(w, err)
			return
		}
		if !h.checkUintItemLimit(w, uint64(len(resolved.Manifest.ChainObjects))+1) {
			return
		}
		plan, err := h.reader.BuildFetchPlan(r.Context(), resolved)
		if err != nil {
			h.writeReaderError(w, err)
			return
		}
		if !h.checkItemLimit(w, len(plan.Objects)+1) {
			return
		}
		h.writeItems(w, []fetchPlanResponse{rawFetchPlan(plan)})
	case "replay":
		var selector replayFetchSelector
		if err := decodeSelector(request.Selector, &selector); err != nil || !validDigest(selector.Manifest) {
			h.writeError(w, http.StatusBadRequest, "INVALID_SELECTOR")
			return
		}
		resolved, err := h.reader.ResolveReplaySnapshot(r.Context(), delivery.ReplaySnapshotSelector{Manifest: selector.Manifest})
		if err != nil {
			h.writeReaderError(w, err)
			return
		}
		partCount := uint64(len(resolved.Manifest.PartManifestKeys))
		if h.config.Limits.MaxAPIResponseItems < 1 || partCount > (h.config.Limits.MaxAPIResponseItems-1)/2 {
			h.writeError(w, http.StatusRequestEntityTooLarge, "RESPONSE_TOO_LARGE")
			return
		}
		plan, err := h.reader.BuildReplayFetchPlan(r.Context(), resolved)
		if err != nil {
			h.writeReaderError(w, err)
			return
		}
		if !h.checkItemLimit(w, len(plan.Parts)+len(plan.Parquet)+1) {
			return
		}
		h.writeItems(w, []fetchPlanResponse{replayFetchPlan(plan)})
	default:
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST")
	}
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if query, err := strictQuery(r, nil); err != nil || len(query) != 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_QUERY")
		return
	}
	status, err := h.config.Health(r.Context())
	if err != nil {
		h.writeReaderError(w, err)
		return
	}
	h.writeItems(w, []HealthSnapshot{status})
}

func (h *Handler) checkItemLimit(w http.ResponseWriter, count int) bool {
	return h.checkUintItemLimit(w, uint64(count))
}

func (h *Handler) checkUintItemLimit(w http.ResponseWriter, count uint64) bool {
	if count > h.config.Limits.MaxAPIResponseItems {
		h.writeError(w, http.StatusRequestEntityTooLarge, "RESPONSE_TOO_LARGE")
		return false
	}
	return true
}

func (h *Handler) writeItems(w http.ResponseWriter, items any) {
	payload := map[string]any{"api_version": APIVersion, "items": items, "next_cursor": nil}
	h.writeJSON(w, http.StatusOK, payload)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "RESPONSE_ENCODING_ERROR")
		return
	}
	if uint64(len(data)) > h.config.Limits.MaxAPIRequestBytes {
		h.writeError(w, http.StatusRequestEntityTooLarge, "RESPONSE_TOO_LARGE")
		return
	}
	h.writeJSONBytes(w, status, data)
}

func (h *Handler) writeJSONBytes(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code string) {
	payload := map[string]any{"api_version": APIVersion, "error": map[string]string{"code": code, "message": publicErrorMessage(code)}}
	data, err := json.Marshal(payload)
	if err != nil {
		data = []byte(`{"api_version":"` + APIVersion + `","error":{"code":"INTERNAL_ERROR","message":"internal server error"}}`)
	}
	// Error responses are intentionally written directly. If the original
	// response exceeded the configured cap, applying the same cap to the
	// explanatory error would recurse forever.
	h.writeJSONBytes(w, status, data)
}

func (h *Handler) writeReaderError(w http.ResponseWriter, err error) {
	status, code := classifyError(err)
	h.writeError(w, status, code)
}

func (h *Handler) writeDecodeError(w http.ResponseWriter, err error) {
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		h.writeError(w, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE")
		return
	}
	h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST")
}

func requestHasBody(r *http.Request) bool {
	if r.Body == nil || r.Body == http.NoBody {
		return false
	}
	// A negative ContentLength denotes chunked or otherwise unknown input.
	// No-body endpoints reject it conservatively instead of reading an
	// unbounded stream merely to decide whether a body exists.
	return r.ContentLength != 0
}

func (h *Handler) methodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
}

func publicErrorMessage(code string) string {
	switch code {
	case "INVALID_QUERY", "INVALID_SELECTOR", "INVALID_REQUEST", "INVALID_PATH":
		return "request is invalid"
	case "MANIFEST_NOT_FOUND":
		return "manifest was not found"
	case "UNAUTHORIZED":
		return "request is not authorized"
	case "FORBIDDEN":
		return "request is forbidden"
	case "TOO_MANY_REQUESTS":
		return "request concurrency limit exceeded"
	case "REQUEST_TOO_LARGE", "RESPONSE_TOO_LARGE":
		return "configured resource limit exceeded"
	default:
		return "request could not be completed"
	}
}

func classifyError(err error) (int, string) {
	if err == nil {
		return http.StatusOK, "OK"
	}
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		return http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return http.StatusGatewayTimeout, "TIMEOUT"
	}
	if errors.Is(err, delivery.ErrSelectorInvalid) {
		return http.StatusBadRequest, "INVALID_SELECTOR"
	}
	if errors.Is(err, delivery.ErrSelectorNotFound) {
		return http.StatusNotFound, "NOT_FOUND"
	}
	if errors.Is(err, r2.ErrMetadataTooLarge) || errors.Is(err, r2.ErrResourceLimit) {
		return http.StatusRequestEntityTooLarge, "RESOURCE_LIMIT"
	}
	if errors.Is(err, archive.ErrIntegrity) {
		return http.StatusConflict, "INTEGRITY_CONFLICT"
	}
	return http.StatusBadGateway, "REMOTE_UNAVAILABLE"
}

func strictQuery(r *http.Request, allowed map[string]bool) (map[string]string, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("query encoding is invalid")
	}
	result := make(map[string]string, len(query))
	for key, values := range query {
		if allowed == nil || !allowed[key] || len(values) != 1 || values[0] == "" {
			return nil, fmt.Errorf("query field is invalid")
		}
		result[key] = values[0]
	}
	return result, nil
}

func validIdentityQuery(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f || char == '/' || char == '\\' {
			return false
		}
	}
	return true
}

func validUTCDate(value string) bool {
	parsed, err := time.Parse("2006-01-02", value)
	return err == nil && parsed.Format("2006-01-02") == value
}

func pathSegment(value string) (string, bool) {
	if value == "" || value == "." || value == ".." || strings.Contains(value, "/") || strings.ContainsAny(value, "\\\x00\r\n") {
		return "", false
	}
	return value, true
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return false
	}
	for _, item := range decoded {
		if item != 0 {
			return true
		}
	}
	return false
}

func decodeSelector(data []byte, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("selector has trailing JSON")
		}
		return err
	}
	return nil
}

func hash(value [32]byte) string { return hex.EncodeToString(value[:]) }

type datasetItem struct {
	DatasetID string `json:"dataset_id"`
}

type campaignItem struct {
	DatasetID               string `json:"dataset_id"`
	CampaignID              string `json:"campaign_id"`
	ProviderID              string `json:"provider_id"`
	StableFeedID            string `json:"stable_feed_id"`
	ExactSourceSymbol       string `json:"exact_source_symbol"`
	BrokerServerFingerprint string `json:"broker_server_fingerprint"`
	DayDefinitionID         string `json:"day_definition_id"`
	PublisherID             string `json:"publisher_id"`
	PublisherEpoch          uint64 `json:"publisher_epoch"`
	ConfigHash              string `json:"config_hash"`
}

type rawSnapshotItem struct {
	DatasetID           string `json:"dataset_id"`
	CampaignID          string `json:"campaign_id"`
	DayDefinitionID     string `json:"day_definition_id"`
	Date                string `json:"date"`
	Revision            uint64 `json:"revision"`
	PublisherID         string `json:"publisher_id"`
	PublisherEpoch      uint64 `json:"publisher_epoch"`
	ManifestKey         string `json:"manifest_key"`
	ManifestSHA256      string `json:"manifest_sha256"`
	ChainSliceStart     uint64 `json:"chain_slice_start"`
	ChainSliceStartRoot string `json:"chain_slice_start_root"`
	ChainSliceEnd       uint64 `json:"chain_slice_end"`
	ChainSliceEndRoot   string `json:"chain_slice_end_root"`
	AcceptedRecordCount uint64 `json:"accepted_record_count"`
	ErrorCount          uint64 `json:"error_count"`
}

type replaySnapshotItem struct {
	DatasetID                   string `json:"dataset_id"`
	CampaignID                  string `json:"campaign_id"`
	DayDefinitionID             string `json:"day_definition_id"`
	Date                        string `json:"date"`
	ReplayContractID            string `json:"replay_contract_id"`
	ConversionID                string `json:"conversion_id"`
	Revision                    uint64 `json:"revision"`
	ManifestKey                 string `json:"manifest_key"`
	ManifestSHA256              string `json:"manifest_sha256"`
	PreviousManifestSHA256      string `json:"previous_manifest_sha256"`
	RawDayManifestKey           string `json:"raw_day_manifest_key"`
	RawDayManifestSHA256        string `json:"raw_day_manifest_sha256"`
	PartSetRoot                 string `json:"part_set_root"`
	CanonicalStreamRowChainRoot string `json:"canonical_stream_row_chain_root"`
	PartCount                   uint64 `json:"part_count"`
}

type manifestItem struct {
	Kind              string          `json:"kind"`
	ManifestKey       string          `json:"manifest_key"`
	ManifestSHA256    string          `json:"manifest_sha256"`
	Revision          uint64          `json:"revision"`
	Date              string          `json:"date"`
	CanonicalManifest json.RawMessage `json:"canonical_manifest"`
}

func rawManifestItem(snapshot delivery.ResolvedSnapshot) manifestItem {
	return manifestItem{Kind: "raw", ManifestKey: snapshot.ManifestKey, ManifestSHA256: hash(snapshot.ManifestSHA256), Revision: snapshot.Manifest.Revision, Date: snapshot.Manifest.Date, CanonicalManifest: append(json.RawMessage(nil), snapshot.ManifestBytes...)}
}

func replayManifestItem(snapshot delivery.ResolvedReplaySnapshot) manifestItem {
	return manifestItem{Kind: "replay", ManifestKey: snapshot.ManifestKey, ManifestSHA256: hash(snapshot.ManifestSHA256), Revision: snapshot.Manifest.Revision, Date: snapshot.Manifest.Date, CanonicalManifest: append(json.RawMessage(nil), snapshot.ManifestBytes...)}
}

type fetchPlanRequest struct {
	Kind     string          `json:"kind"`
	Selector json.RawMessage `json:"selector"`
}

type rawFetchSelector struct {
	Manifest string `json:"manifest"`
}

type replayFetchSelector struct {
	Manifest string `json:"manifest"`
}

type fetchPlanResponse struct {
	Kind     string          `json:"kind"`
	Manifest fetchPlanItem   `json:"manifest"`
	Objects  []fetchPlanItem `json:"objects"`
}

type fetchPlanItem struct {
	Kind          string `json:"kind"`
	Key           string `json:"key"`
	SHA256        string `json:"sha256"`
	Bytes         uint64 `json:"bytes"`
	CacheIdentity string `json:"cache_identity"`
}

func rawFetchPlan(plan delivery.FetchPlan) fetchPlanResponse {
	manifest := fetchPlanItem{Kind: "raw_manifest", Key: plan.ManifestKey, SHA256: hash(plan.ManifestSHA256), Bytes: uint64(len(plan.ManifestBytes))}
	manifest.CacheIdentity = cacheIdentity(manifest.Kind, manifest.Key, manifest.SHA256)
	objects := make([]fetchPlanItem, len(plan.Objects))
	for index, object := range plan.Objects {
		objects[index] = fetchPlanItem{Kind: "raw_object", Key: object.Key, SHA256: hash(object.SHA256), Bytes: object.Bytes}
		objects[index].CacheIdentity = cacheIdentity(objects[index].Kind, objects[index].Key, objects[index].SHA256)
	}
	return fetchPlanResponse{Kind: "raw", Manifest: manifest, Objects: objects}
}

func replayFetchPlan(plan delivery.ReplayFetchPlan) fetchPlanResponse {
	manifest := replayPlanItem(plan.Manifest)
	objects := make([]fetchPlanItem, 0, len(plan.Parts)+len(plan.Parquet))
	for _, object := range plan.Parts {
		objects = append(objects, replayPlanItem(object))
	}
	for _, object := range plan.Parquet {
		objects = append(objects, replayPlanItem(object))
	}
	return fetchPlanResponse{Kind: "replay", Manifest: manifest, Objects: objects}
}

func replayPlanItem(object delivery.ReplayFetchObject) fetchPlanItem {
	item := fetchPlanItem{Kind: string(object.Kind), Key: object.Key, SHA256: hash(object.Digest), Bytes: object.Bytes}
	item.CacheIdentity = cacheIdentity(item.Kind, item.Key, item.SHA256)
	return item
}

func cacheIdentity(kind, key, digest string) string {
	return kind + ":" + key + ":" + digest
}
