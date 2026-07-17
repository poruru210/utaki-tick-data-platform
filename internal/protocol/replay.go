package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ReplayRowData   uint8 = 1
	ReplayRowMarker uint8 = 2
	MaxPathBytes          = 1024

	ReplayDayManifestVersion = "replay-day-manifest-v1"
	PartManifestVersion      = "part-manifest-v1"
	ReplayFormatID           = "ticks-parquet-v1"

	ReplayRowDomain         = "tick-data-platform/replay-row/v1\x00"
	ReplayMarkerDomain      = "tick-data-platform/replay-marker/v1\x00"
	ReplayRowChainDomain    = "tick-data-platform/replay-row-chain/v1\x00"
	ContinuitySegmentDomain = "tick-data-platform/continuity-segment/v1\x00"
	PartManifestDomain      = "tick-data-platform/part-manifest/v1\x00"
	PartSetRootDomain       = "tick-data-platform/part-set/v1\x00"
	ReplayManifestDomain    = "tick-data-platform/replay-day-manifest/v1\x00"
	RawDayManifestDomain    = "tick-data-platform/raw-day-manifest/v1\x00"
)

const (
	MarkerSegmentStart         = "SEGMENT_START"
	MarkerAmbiguousOverlap     = "AMBIGUOUS_OVERLAP"
	MarkerSourceHistoryChanged = "SOURCE_HISTORY_CHANGED"
	MarkerSourceError          = "SOURCE_ERROR"
	MarkerGap                  = "GAP"
	MarkerTimestampRegression  = "TIMESTAMP_REGRESSION"
	MarkerScopeBoundary        = "SCOPE_BOUNDARY"

	ReasonInitial             = "INITIAL"
	ReasonNoUniqueOverlap     = "NO_UNIQUE_OVERLAP"
	ReasonSamePositionChanged = "SAME_POSITION_PAYLOAD_CHANGED"
	ReasonSourceReportedError = "SOURCE_REPORTED_ERROR"
	ReasonWALSequenceGap      = "WAL_SEQUENCE_GAP"
	ReasonTimeMSCRegression   = "TIME_MSC_REGRESSION"
	ReasonScopeChanged        = "SCOPE_CHANGED"
)

var markerReasons = map[string]string{
	MarkerSegmentStart:         ReasonInitial,
	MarkerAmbiguousOverlap:     ReasonNoUniqueOverlap,
	MarkerSourceHistoryChanged: ReasonSamePositionChanged,
	MarkerSourceError:          ReasonSourceReportedError,
	MarkerGap:                  ReasonWALSequenceGap,
	MarkerTimestampRegression:  ReasonTimeMSCRegression,
	MarkerScopeBoundary:        ReasonScopeChanged,
}

// ReplayScope is the immutable input binding for one replay stream.
// RawDayManifestKey and RawDayManifestSHA256 are always checked together.
type ReplayScope struct {
	DatasetID            string
	DayDefinitionID      string
	Date                 string
	ReplayContractID     string
	ConversionID         string
	RawDayManifestKey    string
	RawDayManifestSHA256 [32]byte
}

func (s ReplayScope) Validate() error {
	for name, value := range map[string]string{
		"dataset_id":           s.DatasetID,
		"day_definition_id":    s.DayDefinitionID,
		"date":                 s.Date,
		"replay_contract_id":   s.ReplayContractID,
		"conversion_id":        s.ConversionID,
		"raw_day_manifest_key": s.RawDayManifestKey,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
		if !utf8.ValidString(value) || len(value) > int(MaxStringBytes) {
			return fmt.Errorf("%s is not a Protocol V1 string", name)
		}
	}
	parsed, err := time.Parse("2006-01-02", s.Date)
	if err != nil || parsed.Format("2006-01-02") != s.Date {
		return fmt.Errorf("date is not UTC YYYY-MM-DD")
	}
	if strings.ContainsAny(s.RawDayManifestKey, "\\\r\n") || strings.Contains(s.RawDayManifestKey, "//") {
		return fmt.Errorf("raw_day_manifest_key is not canonical")
	}
	for _, part := range strings.Split(s.RawDayManifestKey, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("raw_day_manifest_key has a non-canonical path component")
		}
	}
	if s.RawDayManifestSHA256 == ([32]byte{}) {
		return fmt.Errorf("raw_day_manifest_sha256 must not be zero")
	}
	return nil
}

// ReplayDataRow is one source occurrence. Repeated payloads remain separate
// because the observation and coordinates identify their occurrence.
type ReplayDataRow struct {
	Scope                    ReplayScope
	StreamSequence           uint64
	ContinuitySegmentID      string
	RawObjectKey             string
	RawObjectSHA256          [32]byte
	GatewayIngestSequence    uint64
	ProducerInstanceID       string
	ProducerSessionID        string
	BatchSequence            uint64
	RecordOrdinal            uint32
	CaptureSequence          uint64
	Record                   RawMqlTickV1
	SourcePayloadFingerprint [32]byte
	ObservationHash          [32]byte
	FetchWallStartS          int64
	FetchWallEndS            int64
	FetchMonotonicStartUS    uint64
	FetchMonotonicEndUS      uint64
	CopyTicksError           int32
	SourceStatusFlags        uint32
}

// ReplayMarkerRow records a continuity fact without inventing a source tick.
type ReplayMarkerRow struct {
	Scope                          ReplayScope
	StreamSequence                 uint64
	ContinuitySegmentID            string
	RawObjectKey                   string
	RawObjectSHA256                [32]byte
	MarkerCode                     string
	Reason                         string
	Detail                         string
	ReferenceGatewayIngestSequence uint64
	ReferenceRecordOrdinal         uint32
	PredecessorRowChainHash        [32]byte
	ContinuitySegmentStartHash     [32]byte
}

// ReplayRow is the canonical row-chain input envelope.
type ReplayRow struct {
	Kind   uint8
	Data   *ReplayDataRow
	Marker *ReplayMarkerRow
}

func (r ReplayRow) Scope() ReplayScope {
	if r.Kind == ReplayRowData && r.Data != nil {
		return r.Data.Scope
	}
	if r.Kind == ReplayRowMarker && r.Marker != nil {
		return r.Marker.Scope
	}
	return ReplayScope{}
}

func (r ReplayRow) StreamSequence() uint64 {
	if r.Kind == ReplayRowData && r.Data != nil {
		return r.Data.StreamSequence
	}
	if r.Kind == ReplayRowMarker && r.Marker != nil {
		return r.Marker.StreamSequence
	}
	return 0
}

func (r ReplayRow) CanonicalBytes() ([]byte, error) {
	var w writer
	w.WriteString(ReplayRowDomain)
	w.u8(r.Kind)
	scope := r.Scope()
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	w.u64(r.StreamSequence())
	segmentID := ""
	if r.Kind == ReplayRowData && r.Data != nil {
		segmentID = r.Data.ContinuitySegmentID
	}
	if r.Kind == ReplayRowMarker && r.Marker != nil {
		segmentID = r.Marker.ContinuitySegmentID
	}
	if err := writeScope(&w, scope, segmentID); err != nil {
		return nil, err
	}
	switch r.Kind {
	case ReplayRowData:
		if r.Data == nil {
			return nil, fmt.Errorf("data row payload is missing")
		}
		if err := r.Data.validate(); err != nil {
			return nil, err
		}
		if err := writeLP(&w, r.Data.RawObjectKey); err != nil {
			return nil, err
		}
		w.hash(r.Data.RawObjectSHA256)
		w.u64(r.Data.GatewayIngestSequence)
		if err := writeLP(&w, r.Data.ProducerInstanceID); err != nil {
			return nil, err
		}
		if err := writeLP(&w, r.Data.ProducerSessionID); err != nil {
			return nil, err
		}
		w.u64(r.Data.BatchSequence)
		w.u32(r.Data.RecordOrdinal)
		w.u64(r.Data.CaptureSequence)
		w.i64(r.Data.Record.Time)
		w.u64(r.Data.Record.BidBits)
		w.u64(r.Data.Record.AskBits)
		w.u64(r.Data.Record.LastBits)
		w.u64(r.Data.Record.Volume)
		w.i64(r.Data.Record.TimeMSC)
		w.u32(r.Data.Record.Flags)
		w.u64(r.Data.Record.VolumeRealBits)
		w.hash(r.Data.SourcePayloadFingerprint)
		w.hash(r.Data.ObservationHash)
		w.i64(r.Data.FetchWallStartS)
		w.i64(r.Data.FetchWallEndS)
		w.u64(r.Data.FetchMonotonicStartUS)
		w.u64(r.Data.FetchMonotonicEndUS)
		w.i32(r.Data.CopyTicksError)
		w.u32(r.Data.SourceStatusFlags)
	case ReplayRowMarker:
		if r.Marker == nil {
			return nil, fmt.Errorf("marker row payload is missing")
		}
		if err := r.Marker.validate(); err != nil {
			return nil, err
		}
		if err := writeLP(&w, r.Marker.RawObjectKey); err != nil {
			return nil, err
		}
		w.hash(r.Marker.RawObjectSHA256)
		w.WriteString(ReplayMarkerDomain)
		if err := writeLP(&w, r.Marker.MarkerCode); err != nil {
			return nil, err
		}
		if err := writeLP(&w, r.Marker.Reason); err != nil {
			return nil, err
		}
		if err := writeLP(&w, r.Marker.Detail); err != nil {
			return nil, err
		}
		w.u64(r.Marker.ReferenceGatewayIngestSequence)
		w.u32(r.Marker.ReferenceRecordOrdinal)
		w.hash(r.Marker.PredecessorRowChainHash)
		w.hash(r.Marker.ContinuitySegmentStartHash)
	default:
		return nil, fmt.Errorf("unknown replay row kind %d", r.Kind)
	}
	return append([]byte(nil), w.Bytes()...), nil
}

func writeLP(w *writer, value string) error { return w.string(value) }

func writePath(w *writer, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("path is not UTF-8")
	}
	if len(value) > MaxPathBytes {
		return fmt.Errorf("path has %d bytes", len(value))
	}
	w.u32(uint32(len(value)))
	w.WriteString(value)
	return nil
}

func writeScope(w *writer, scope ReplayScope, segmentID string) error {
	for _, value := range []string{scope.DatasetID, scope.DayDefinitionID, scope.Date, scope.ReplayContractID, scope.ConversionID, segmentID} {
		if err := writeLP(w, value); err != nil {
			return err
		}
	}
	w.hash(scope.RawDayManifestSHA256)
	return nil
}

func (r ReplayDataRow) validate() error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	if r.ContinuitySegmentID == "" || len(r.ContinuitySegmentID) > int(MaxStringBytes) {
		return fmt.Errorf("continuity_segment_id is required")
	}
	if r.RawObjectKey == "" || r.RawObjectSHA256 == ([32]byte{}) {
		return fmt.Errorf("data row raw object binding is required")
	}
	if r.RawObjectKey != RawWALObjectKey(r.RawObjectSHA256) {
		return fmt.Errorf("data row raw object key does not match hash")
	}
	for name, value := range map[string]string{"producer_instance_id": r.ProducerInstanceID, "producer_session_id": r.ProducerSessionID} {
		if value == "" || !utf8.ValidString(value) || len(value) > int(MaxStringBytes) {
			return fmt.Errorf("%s is not a Protocol V1 string", name)
		}
	}
	expectedSource := SourcePayloadFingerprint(r.Record)
	if r.SourcePayloadFingerprint != expectedSource {
		return fmt.Errorf("source_payload_fingerprint does not match record")
	}
	expectedObservation := ObservationHash(r.ProducerInstanceID, r.ProducerSessionID, r.BatchSequence, r.RecordOrdinal, r.CaptureSequence, r.SourcePayloadFingerprint)
	if r.ObservationHash != expectedObservation {
		return fmt.Errorf("observation_hash does not match occurrence")
	}
	return nil
}

func (r ReplayMarkerRow) validate() error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	if r.ContinuitySegmentID == "" || len(r.ContinuitySegmentID) > int(MaxStringBytes) {
		return fmt.Errorf("continuity_segment_id is required")
	}
	if expected, ok := markerReasons[r.MarkerCode]; !ok || expected != r.Reason {
		return fmt.Errorf("marker code/reason pair is not allowed")
	}
	if !utf8.ValidString(r.Detail) || len(r.Detail) > int(MaxStringBytes) {
		return fmt.Errorf("marker_detail is not a Protocol V1 string")
	}
	if r.RawObjectKey == "" {
		if r.RawObjectSHA256 != ([32]byte{}) {
			return fmt.Errorf("marker raw object key/hash must bind together")
		}
	} else if r.RawObjectSHA256 == ([32]byte{}) {
		return fmt.Errorf("marker raw object hash is required when key is present")
	} else if r.RawObjectKey != RawWALObjectKey(r.RawObjectSHA256) {
		return fmt.Errorf("marker raw object key does not match hash")
	}
	return nil
}

// SegmentID derives a stable id from the first coordinate and its reason.
func SegmentID(scope ReplayScope, gatewaySequence uint64, recordOrdinal uint32, markerCode string, predecessor [32]byte) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if _, ok := markerReasons[markerCode]; !ok {
		return "", fmt.Errorf("unknown segment marker %q", markerCode)
	}
	var w writer
	w.WriteString(ContinuitySegmentDomain)
	for _, value := range []string{scope.DatasetID, scope.DayDefinitionID, scope.Date, scope.ReplayContractID, scope.ConversionID, scope.RawDayManifestKey} {
		if err := writeLP(&w, value); err != nil {
			return "", err
		}
	}
	w.hash(scope.RawDayManifestSHA256)
	w.u64(gatewaySequence)
	w.u32(recordOrdinal)
	if err := writeLP(&w, markerCode); err != nil {
		return "", err
	}
	w.hash(predecessor)
	digest := sha256.Sum256(w.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

// RowChainStep returns the next hash. The empty stream root is 32 zero bytes.
func RowChainStep(streamSequence uint64, previous [32]byte, canonicalRow []byte) [32]byte {
	var w writer
	w.WriteString(ReplayRowChainDomain)
	w.u64(streamSequence)
	w.hash(previous)
	w.u32(uint32(len(canonicalRow)))
	w.Write(canonicalRow)
	return sha256.Sum256(w.Bytes())
}

func RowChainRoot(rows []ReplayRow) ([32]byte, error) {
	var root [32]byte
	for index, row := range rows {
		if row.StreamSequence() != uint64(index) {
			return root, fmt.Errorf("stream sequence %d is not %d", row.StreamSequence(), index)
		}
		canonical, err := row.CanonicalBytes()
		if err != nil {
			return root, err
		}
		root = RowChainStep(row.StreamSequence(), root, canonical)
	}
	return root, nil
}

type PartManifest struct {
	ManifestVersion         string
	DatasetID               string
	DayDefinitionID         string
	Date                    string
	ReplayContractID        string
	FormatID                string
	ConversionID            string
	ConverterBuildID        string
	DependencyLockHash      [32]byte
	WriterConfigurationHash [32]byte
	TargetPlatformContract  string
	RawDayManifestKey       string
	RawDayManifestSHA256    [32]byte
	PartSequence            uint32
	PartKey                 string
	PartSHA256              [32]byte
	PartBytes               uint64
	RowCount                uint64
	CanonicalRowBytes       uint64
	FirstStreamSequence     uint64
	LastStreamSequence      uint64
	PreviousRowChainHash    [32]byte
	FirstRowChainHash       [32]byte
	LastRowChainHash        [32]byte
	PreviousManifestSHA256  *[32]byte
	ManifestSHA256          [32]byte
}

func (p PartManifest) Validate() error {
	if p.ManifestVersion != PartManifestVersion || p.PartKey == "" || p.PartSHA256 == ([32]byte{}) || p.PartBytes == 0 || p.RowCount == 0 || p.FirstRowChainHash == ([32]byte{}) || p.LastRowChainHash == ([32]byte{}) {
		return fmt.Errorf("invalid part manifest identity or empty part")
	}
	if !utf8.ValidString(p.PartKey) || len(p.PartKey) > MaxPathBytes {
		return fmt.Errorf("part key is not a Protocol V1 path")
	}
	scope := ReplayScope{
		DatasetID: p.DatasetID, DayDefinitionID: p.DayDefinitionID,
		Date: p.Date, ReplayContractID: p.ReplayContractID, ConversionID: p.ConversionID,
		RawDayManifestKey: p.RawDayManifestKey, RawDayManifestSHA256: p.RawDayManifestSHA256,
	}
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("part manifest scope: %w", err)
	}
	for name, value := range map[string]string{
		"format_id": p.FormatID, "converter_build_id": p.ConverterBuildID,
		"target_platform_contract": p.TargetPlatformContract,
	} {
		if value == "" || !utf8.ValidString(value) || len(value) > int(MaxStringBytes) {
			return fmt.Errorf("%s is not a Protocol V1 string", name)
		}
	}
	if p.FormatID != ReplayFormatID || p.DependencyLockHash == ([32]byte{}) || p.WriterConfigurationHash == ([32]byte{}) {
		return fmt.Errorf("part manifest conversion tuple is incomplete")
	}
	if p.LastStreamSequence < p.FirstStreamSequence || p.CanonicalRowBytes == 0 || p.LastStreamSequence-p.FirstStreamSequence != p.RowCount-1 {
		return fmt.Errorf("invalid part row range")
	}
	wantKey, err := replayPartObjectKey(scope, p.FirstStreamSequence, p.LastStreamSequence, p.PartSHA256)
	if err != nil {
		return err
	}
	if p.PartKey != wantKey {
		return fmt.Errorf("part key does not match part hash")
	}
	if p.PartSequence == 0 {
		if p.PreviousManifestSHA256 != nil || p.PreviousRowChainHash != ([32]byte{}) {
			return fmt.Errorf("first part must not have a predecessor")
		}
	} else {
		if p.PreviousManifestSHA256 == nil || *p.PreviousManifestSHA256 == ([32]byte{}) || p.PreviousRowChainHash == ([32]byte{}) {
			return fmt.Errorf("non-first part requires manifest and row-chain predecessors")
		}
	}
	return nil
}

func (p PartManifest) canonicalValue() map[string]any {
	value := map[string]any{
		"canonical_row_bytes":       p.CanonicalRowBytes,
		"conversion_id":             p.ConversionID,
		"converter_build_id":        p.ConverterBuildID,
		"dataset_id":                p.DatasetID,
		"date":                      p.Date,
		"day_definition_id":         p.DayDefinitionID,
		"dependency_lock_hash":      hex.EncodeToString(p.DependencyLockHash[:]),
		"first_row_chain_hash":      hex.EncodeToString(p.FirstRowChainHash[:]),
		"first_stream_sequence":     p.FirstStreamSequence,
		"last_row_chain_hash":       hex.EncodeToString(p.LastRowChainHash[:]),
		"last_stream_sequence":      p.LastStreamSequence,
		"manifest_version":          p.ManifestVersion,
		"format_id":                 p.FormatID,
		"part_bytes":                p.PartBytes,
		"part_key":                  p.PartKey,
		"part_sequence":             p.PartSequence,
		"part_sha256":               hex.EncodeToString(p.PartSHA256[:]),
		"previous_row_chain_hash":   hex.EncodeToString(p.PreviousRowChainHash[:]),
		"raw_day_manifest_key":      p.RawDayManifestKey,
		"raw_day_manifest_sha256":   hex.EncodeToString(p.RawDayManifestSHA256[:]),
		"replay_contract_id":        p.ReplayContractID,
		"row_count":                 p.RowCount,
		"target_platform_contract":  p.TargetPlatformContract,
		"writer_configuration_hash": hex.EncodeToString(p.WriterConfigurationHash[:]),
	}
	if p.PreviousManifestSHA256 == nil {
		value["previous_manifest_sha256"] = nil
	} else {
		value["previous_manifest_sha256"] = hex.EncodeToString(p.PreviousManifestSHA256[:])
	}
	return value
}

func PartManifestCanonicalJSON(p PartManifest) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return CanonicalJSON(p.canonicalValue())
}

func PartManifestDigest(p PartManifest) ([32]byte, error) {
	canonical, err := PartManifestCanonicalJSON(p)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(PartManifestDomain), canonical...)), nil
}

// ExactIdentityPathKey hashes the exact UTF-8 bytes of an identity component.
// It deliberately performs no normalization, case folding, or trimming.
func ExactIdentityPathKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// ReplayDerivativeBaseKey returns the scope-relative, date-local physical
// key prefix shared by every M3 derivative for one exact replay scope.
func ReplayDerivativeBaseKey(scope ReplayScope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"derivatives/stream=%s/format=%s/conversion=%s/day-definition=%s/date=%s",
		ExactIdentityPathKey(scope.ReplayContractID),
		ReplayFormatID,
		ExactIdentityPathKey(scope.ConversionID),
		ExactIdentityPathKey(scope.DayDefinitionID),
		scope.Date,
	), nil
}

func replayPartObjectKey(scope ReplayScope, firstStreamSequence, lastStreamSequence uint64, partSHA256 [32]byte) (string, error) {
	base, err := ReplayDerivativeBaseKey(scope)
	if err != nil {
		return "", err
	}
	if partSHA256 == ([32]byte{}) || lastStreamSequence < firstStreamSequence {
		return "", fmt.Errorf("invalid replay part key identity")
	}
	return fmt.Sprintf("%s/parquet/%d-%d-%x.parquet", base, firstStreamSequence, lastStreamSequence, partSHA256), nil
}

// ReplayPartObjectKey derives the exact scope-relative Parquet key from
// the verified replay scope, row range, and content hash.
func ReplayPartObjectKey(scope ReplayScope, firstStreamSequence, lastStreamSequence uint64, partSHA256 [32]byte) (string, error) {
	return replayPartObjectKey(scope, firstStreamSequence, lastStreamSequence, partSHA256)
}

func partManifestKeyFromDigest(part PartManifest, digest [32]byte) (string, error) {
	scope := ReplayScope{
		DatasetID: part.DatasetID, DayDefinitionID: part.DayDefinitionID,
		Date: part.Date, ReplayContractID: part.ReplayContractID, ConversionID: part.ConversionID,
		RawDayManifestKey: part.RawDayManifestKey, RawDayManifestSHA256: part.RawDayManifestSHA256,
	}
	base, err := ReplayDerivativeBaseKey(scope)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/manifests/part-%08d-%x.json", base, part.PartSequence, digest), nil
}

// PartManifestKey derives a part manifest key from the complete canonical
// part identity, including its scope, raw binding, conversion, range, and
// object digest.
func PartManifestKey(part PartManifest) (string, error) {
	if err := part.Validate(); err != nil {
		return "", err
	}
	digest, err := PartManifestDigest(part)
	if err != nil {
		return "", err
	}
	return partManifestKeyFromDigest(part, digest)
}

func RawWALObjectKey(sha256 [32]byte) string {
	return fmt.Sprintf("objects/raw/wal-%x.rtw", sha256)
}

func ReplayDayManifestKey(m ReplayDayManifest) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	digest, err := ReplayDayManifestDigest(m)
	if err != nil {
		return "", err
	}
	scope := ReplayScope{
		DatasetID: m.DatasetID, DayDefinitionID: m.DayDefinitionID,
		Date: m.Date, ReplayContractID: m.ReplayContractID, ConversionID: m.ConversionID,
		RawDayManifestKey: m.RawDayManifestKey, RawDayManifestSHA256: m.RawDayManifestSHA256,
	}
	base, err := ReplayDerivativeBaseKey(scope)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/replay-day-%d-%x.json", base, m.Revision, digest), nil
}

func PartSetRoot(parts []PartManifest) ([32]byte, error) {
	var root [32]byte
	if len(parts) == 0 {
		return root, nil
	}
	var w writer
	w.WriteString(PartSetRootDomain)
	w.u32(uint32(len(parts)))
	for i, part := range parts {
		if err := part.Validate(); err != nil {
			return root, err
		}
		if int(part.PartSequence) != i {
			return root, fmt.Errorf("part sequence is not day-local and contiguous")
		}
		if i > 0 && !samePartBinding(parts[0], part) {
			return root, fmt.Errorf("part provenance or conversion binding differs")
		}
		digest, err := PartManifestDigest(part)
		if err != nil {
			return root, err
		}
		if i > 0 {
			previousDigest, previousErr := PartManifestDigest(parts[i-1])
			if previousErr != nil || part.PreviousManifestSHA256 == nil || *part.PreviousManifestSHA256 != previousDigest {
				return root, fmt.Errorf("part predecessor does not match previous digest")
			}
			if part.PreviousRowChainHash != parts[i-1].LastRowChainHash {
				return root, fmt.Errorf("part predecessor does not match previous row-chain hash")
			}
			if parts[i-1].LastStreamSequence == ^uint64(0) || part.FirstStreamSequence != parts[i-1].LastStreamSequence+1 {
				return root, fmt.Errorf("part stream ranges are not contiguous")
			}
		} else if part.FirstStreamSequence != 0 || part.PreviousRowChainHash != ([32]byte{}) {
			return root, fmt.Errorf("first part has an invalid stream or row-chain predecessor")
		}
		key, err := partManifestKeyFromDigest(part, digest)
		if err != nil {
			return root, err
		}
		if err := writePath(&w, key); err != nil {
			return root, err
		}
		w.hash(digest)
		w.u64(part.FirstStreamSequence)
		w.u64(part.LastStreamSequence)
	}
	return sha256.Sum256(w.Bytes()), nil
}

func samePartBinding(left, right PartManifest) bool {
	return left.DatasetID == right.DatasetID && left.DayDefinitionID == right.DayDefinitionID && left.Date == right.Date &&
		left.ReplayContractID == right.ReplayContractID && left.FormatID == right.FormatID &&
		left.ConversionID == right.ConversionID && left.ConverterBuildID == right.ConverterBuildID &&
		left.DependencyLockHash == right.DependencyLockHash &&
		left.WriterConfigurationHash == right.WriterConfigurationHash &&
		left.TargetPlatformContract == right.TargetPlatformContract &&
		left.RawDayManifestKey == right.RawDayManifestKey &&
		left.RawDayManifestSHA256 == right.RawDayManifestSHA256
}

type ReplayDayManifest struct {
	ManifestVersion             string
	ManifestID                  string
	DatasetID                   string
	DayDefinitionID             string
	Date                        string
	Revision                    uint64
	RawDayManifestKey           string
	RawDayManifestSHA256        [32]byte
	ReplayContractID            string
	FormatID                    string
	ConversionID                string
	ConverterBuildID            string
	DependencyLockHash          [32]byte
	WriterConfigurationHash     [32]byte
	TargetPlatformContract      string
	CompletenessStatus          string
	PartManifestKeys            []string
	PartSetRoot                 [32]byte
	CanonicalStreamRowChainRoot [32]byte
	PreviousManifestSHA256      *[32]byte
	ManifestSHA256              [32]byte
	M0EmptyPartsCompatibility   bool
}

func (m ReplayDayManifest) Validate() error {
	if m.M0EmptyPartsCompatibility {
		if m.PartManifestKeys != nil && len(m.PartManifestKeys) != 0 {
			return fmt.Errorf("M0 compatibility form must have empty parts")
		}
		return nil
	}
	if m.ManifestVersion != ReplayDayManifestVersion || m.ManifestID == "" || m.Revision == 0 || m.RawDayManifestKey == "" || m.RawDayManifestSHA256 == ([32]byte{}) {
		return fmt.Errorf("invalid replay manifest identity or raw binding")
	}
	parsed, err := time.Parse("2006-01-02", m.Date)
	if err != nil || parsed.Format("2006-01-02") != m.Date {
		return fmt.Errorf("date is not UTC YYYY-MM-DD")
	}
	if !utf8.ValidString(m.RawDayManifestKey) || len(m.RawDayManifestKey) > int(MaxStringBytes) || strings.ContainsAny(m.RawDayManifestKey, "\\\r\n") || strings.Contains(m.RawDayManifestKey, "//") {
		return fmt.Errorf("raw_day_manifest_key is not canonical")
	}
	for _, part := range strings.Split(m.RawDayManifestKey, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("raw_day_manifest_key has a non-canonical path component")
		}
	}
	if m.FormatID != ReplayFormatID || m.CompletenessStatus != "provisional" && m.CompletenessStatus != "settled_snapshot" {
		return fmt.Errorf("invalid replay format or completeness")
	}
	for name, value := range map[string]string{
		"dataset_id": m.DatasetID, "day_definition_id": m.DayDefinitionID,
		"date": m.Date, "replay_contract_id": m.ReplayContractID, "conversion_id": m.ConversionID,
		"converter_build_id": m.ConverterBuildID, "target_platform_contract": m.TargetPlatformContract,
	} {
		if value == "" || !utf8.ValidString(value) || len(value) > int(MaxStringBytes) {
			return fmt.Errorf("%s is not a Protocol V1 string", name)
		}
	}
	if m.DependencyLockHash == ([32]byte{}) || m.WriterConfigurationHash == ([32]byte{}) {
		return fmt.Errorf("conversion tuple hash is zero")
	}
	if m.Revision == 1 && m.PreviousManifestSHA256 != nil || m.Revision > 1 && (m.PreviousManifestSHA256 == nil || *m.PreviousManifestSHA256 == ([32]byte{})) {
		return fmt.Errorf("invalid replay revision predecessor")
	}
	seen := map[string]bool{}
	if len(m.PartManifestKeys) == 0 {
		if m.PartSetRoot != ([32]byte{}) || m.CanonicalStreamRowChainRoot != ([32]byte{}) {
			return fmt.Errorf("empty replay manifest must have zero roots")
		}
	} else if m.PartSetRoot == ([32]byte{}) || m.CanonicalStreamRowChainRoot == ([32]byte{}) {
		return fmt.Errorf("non-empty replay manifest must have nonzero roots")
	}
	scope := ReplayScope{
		DatasetID: m.DatasetID, DayDefinitionID: m.DayDefinitionID,
		Date: m.Date, ReplayContractID: m.ReplayContractID, ConversionID: m.ConversionID,
		RawDayManifestKey: m.RawDayManifestKey, RawDayManifestSHA256: m.RawDayManifestSHA256,
	}
	base, err := ReplayDerivativeBaseKey(scope)
	if err != nil {
		return fmt.Errorf("replay derivative key scope: %w", err)
	}
	for _, key := range m.PartManifestKeys {
		if key == "" || seen[key] || !validPartManifestKeyShape(base, key) {
			return fmt.Errorf("invalid or duplicate part manifest key")
		}
		seen[key] = true
	}
	return nil
}

func validPartManifestKeyShape(base, key string) bool {
	prefix := base + "/manifests/part-"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, ".json") {
		return false
	}
	suffix := strings.TrimSuffix(strings.TrimPrefix(key, prefix), ".json")
	if len(suffix) != len("00000000-")+64 || suffix[8] != '-' {
		return false
	}
	for _, character := range suffix[:8] {
		if character < '0' || character > '9' {
			return false
		}
	}
	for _, character := range suffix[9:] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func (m ReplayDayManifest) canonicalValue() map[string]any {
	value := map[string]any{
		"canonical_stream_row_chain_root": hex.EncodeToString(m.CanonicalStreamRowChainRoot[:]),
		"completeness_status":             m.CompletenessStatus,
		"conversion_id":                   m.ConversionID,
		"converter_build_id":              m.ConverterBuildID,
		"dataset_id":                      m.DatasetID,
		"date":                            m.Date,
		"day_definition_id":               m.DayDefinitionID,
		"dependency_lock_hash":            hex.EncodeToString(m.DependencyLockHash[:]),
		"format_id":                       m.FormatID,
		"manifest_id":                     m.ManifestID,
		"manifest_version":                m.ManifestVersion,
		"part_manifest_keys":              m.PartManifestKeys,
		"part_set_root":                   hex.EncodeToString(m.PartSetRoot[:]),
		"raw_day_manifest_key":            m.RawDayManifestKey,
		"raw_day_manifest_sha256":         hex.EncodeToString(m.RawDayManifestSHA256[:]),
		"replay_contract_id":              m.ReplayContractID,
		"revision":                        m.Revision,
		"target_platform_contract":        m.TargetPlatformContract,
		"writer_configuration_hash":       hex.EncodeToString(m.WriterConfigurationHash[:]),
	}
	if m.PreviousManifestSHA256 == nil {
		value["previous_manifest_sha256"] = nil
	} else {
		value["previous_manifest_sha256"] = hex.EncodeToString(m.PreviousManifestSHA256[:])
	}
	return value
}

func ReplayDayManifestCanonicalJSON(m ReplayDayManifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if m.M0EmptyPartsCompatibility {
		return nil, fmt.Errorf("M0 compatibility form has no M3 canonical writer")
	}
	return CanonicalJSON(m.canonicalValue())
}

func ReplayDayManifestDigest(m ReplayDayManifest) ([32]byte, error) {
	canonical, err := ReplayDayManifestCanonicalJSON(m)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(ReplayManifestDomain), canonical...)), nil
}

func VerifyReplayDayManifestBinding(m ReplayDayManifest, key string, bytes []byte) error {
	if m.M0EmptyPartsCompatibility {
		return fmt.Errorf("M0 compatibility manifest cannot satisfy M3 raw binding")
	}
	if key != m.RawDayManifestKey {
		return fmt.Errorf("raw day manifest key mismatch")
	}
	digest := RawDayManifestDigest(bytes)
	if digest != m.RawDayManifestSHA256 {
		return fmt.Errorf("raw day manifest sha256 mismatch")
	}
	if _, err := DecodeCanonicalJSON(bytes); err != nil {
		return fmt.Errorf("raw day manifest is not canonical JSON: %w", err)
	}
	return nil
}

// RawDayManifestDigest hashes canonical raw-day manifest bytes in the M2
// domain. Callers must not replace this with plain SHA-256.
func RawDayManifestDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(RawDayManifestDomain), canonical...))
}

func ParseHashHex(value string) ([32]byte, error) {
	var result [32]byte
	if len(value) != 64 || strings.ToLower(value) != value {
		return result, fmt.Errorf("hash is not lowercase 64-character hexadecimal")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return result, err
	}
	copy(result[:], decoded)
	return result, nil
}

func EncodeHashHex(value [32]byte) string {
	return hex.EncodeToString(value[:])
}
