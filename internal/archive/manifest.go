package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
	"unicode/utf8"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

const (
	RawDayManifestVersion = "raw-day-manifest-v1"
	RawDayManifestDomain  = "tick-data-platform/raw-day-manifest/v1\x00"
	RawSetRootDomain      = "tick-data-platform/raw-set/v1\x00"
	ArchiveConfigDomain   = "tick-data-platform/archive-config/v1\x00"
	CampaignScopeVersion  = "campaign-scope-v1"
)

type ProtocolLimits struct {
	MaxFrameBytes  uint32
	MaxRecords     uint32
	MaxStringBytes uint16
}

// ScopeConfig is the operator-supplied identity and publication contract for
// one archive campaign. It intentionally contains no secret, environment
// variable name, or filesystem path.
type ScopeConfig struct {
	DatasetID               string
	CampaignID              string
	ProviderID              string
	StableFeedID            string
	ExactSourceSymbol       string
	BrokerServerFingerprint string
	GatewayBuildIdentity    string
	ProducerBuildIdentity   string
	DayDefinitionID         string
	SettlePolicy            string
	PublisherID             string
	PublisherEpoch          uint64
	ProtocolVersion         uint16
	ProtocolLimits          ProtocolLimits
}

type RawObjectRange struct {
	Key                 string
	SHA256              [32]byte
	Bytes               uint64
	StartIngestSequence uint64
	EndIngestSequence   uint64
	FirstRecordOrdinal  uint32
	LastRecordOrdinal   uint32
}

type RawChainObject struct {
	Key                 string
	SHA256              [32]byte
	Bytes               uint64
	StartIngestSequence uint64
	EndIngestSequence   uint64
}

type RawDayManifest struct {
	ManifestVersion           string
	ManifestID                string
	DatasetID                 string
	CampaignID                string
	DayDefinitionID           string
	Date                      string
	Revision                  uint64
	PublisherID               string
	PublisherEpoch            uint64
	ConfigHash                [32]byte
	ProtocolVersion           uint16
	SourceSchemaID            string
	WALSchemaID               string
	ObservedThroughSourceMSC  int64
	ObservedThroughCaptureSeq uint64
	TerminalSyncStatus        string
	SettlePolicy              string
	CompletenessStatus        string
	Objects                   []RawObjectRange
	ChainObjects              []RawChainObject
	AcceptedRecordCount       uint64
	ErrorCount                uint64
	ChainSliceStartSequence   uint64
	ChainSliceStartRoot       [32]byte
	ChainSliceEndSequence     uint64
	ChainSliceEndRoot         [32]byte
	RawSetRoot                [32]byte
	PreviousManifestSHA256    *[32]byte
	LogicalCloseTimeS         int64
	ManifestSHA256            [32]byte
}

type RawDayManifestInput struct {
	Scope              ScopeConfig
	Date               string
	RawObjects         []RawObject
	Revision           uint64
	Previous           *RawDayManifest
	TerminalSyncStatus string
	CompletenessStatus string
	LogicalCloseTimeS  int64
}

func (s ScopeConfig) normalized() (ScopeConfig, error) {
	if s.ProtocolVersion == 0 {
		s.ProtocolVersion = protocol.ProtocolVersion
	}
	if s.ProtocolLimits.MaxFrameBytes == 0 {
		s.ProtocolLimits.MaxFrameBytes = protocol.MaxFrameBytes
	}
	if s.ProtocolLimits.MaxRecords == 0 {
		s.ProtocolLimits.MaxRecords = protocol.MaxRecords
	}
	if s.ProtocolLimits.MaxStringBytes == 0 {
		s.ProtocolLimits.MaxStringBytes = protocol.MaxStringBytes
	}
	if s.ProtocolVersion != protocol.ProtocolVersion {
		return ScopeConfig{}, fmt.Errorf("unsupported archive protocol version %d", s.ProtocolVersion)
	}
	for name, value := range map[string]string{
		"dataset_id":                s.DatasetID,
		"campaign_id":               s.CampaignID,
		"provider_id":               s.ProviderID,
		"stable_feed_id":            s.StableFeedID,
		"exact_source_symbol":       s.ExactSourceSymbol,
		"broker_server_fingerprint": s.BrokerServerFingerprint,
		"gateway_build_identity":    s.GatewayBuildIdentity,
		"producer_build_identity":   s.ProducerBuildIdentity,
		"day_definition_id":         s.DayDefinitionID,
		"settle_policy":             s.SettlePolicy,
		"publisher_id":              s.PublisherID,
	} {
		if value == "" {
			return ScopeConfig{}, fmt.Errorf("%s is required", name)
		}
		if !utf8.ValidString(value) {
			return ScopeConfig{}, fmt.Errorf("%s is not valid UTF-8", name)
		}
	}
	if s.ProtocolLimits.MaxFrameBytes < protocol.MinFrameBytes ||
		s.ProtocolLimits.MaxFrameBytes > protocol.MaxFrameBytes {
		return ScopeConfig{}, fmt.Errorf("max_frame_bytes is outside Protocol V1 limits")
	}
	if s.ProtocolLimits.MaxRecords == 0 || s.ProtocolLimits.MaxRecords > protocol.MaxRecords {
		return ScopeConfig{}, fmt.Errorf("max_records is outside Protocol V1 limits")
	}
	if s.ProtocolLimits.MaxStringBytes == 0 || s.ProtocolLimits.MaxStringBytes > protocol.MaxStringBytes {
		return ScopeConfig{}, fmt.Errorf("max_string_bytes is outside Protocol V1 limits")
	}
	return s, nil
}

func (s ScopeConfig) configValue() (map[string]any, error) {
	normalized, err := s.normalized()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"broker_server_fingerprint": normalized.BrokerServerFingerprint,
		"campaign_id":               normalized.CampaignID,
		"dataset_id":                normalized.DatasetID,
		"day_definition_id":         normalized.DayDefinitionID,
		"exact_source_symbol":       normalized.ExactSourceSymbol,
		"gateway_build_identity":    normalized.GatewayBuildIdentity,
		"producer_build_identity":   normalized.ProducerBuildIdentity,
		"protocol_limits": map[string]any{
			"max_frame_bytes":  normalized.ProtocolLimits.MaxFrameBytes,
			"max_records":      normalized.ProtocolLimits.MaxRecords,
			"max_string_bytes": normalized.ProtocolLimits.MaxStringBytes,
		},
		"protocol_version": normalized.ProtocolVersion,
		"provider_id":      normalized.ProviderID,
		"publisher_epoch":  normalized.PublisherEpoch,
		"publisher_id":     normalized.PublisherID,
		"settle_policy":    normalized.SettlePolicy,
		"stable_feed_id":   normalized.StableFeedID,
	}, nil
}

func (s ScopeConfig) CanonicalConfigJSON() ([]byte, error) {
	value, err := s.configValue()
	if err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(value)
}

// ScopeConfigFromCanonicalJSON decodes the exact archive-config-v1 document
// used by campaign-scope descriptors.  It returns normalized protocol limits
// only after proving that the supplied bytes are the canonical representation.
func ScopeConfigFromCanonicalJSON(data []byte) (ScopeConfig, error) {
	value, err := protocol.DecodeCanonicalJSON(data)
	if err != nil {
		return ScopeConfig{}, fmt.Errorf("decode archive config: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return ScopeConfig{}, fmt.Errorf("archive config must be a JSON object")
	}
	if err := requireExactKeys(object, []string{
		"broker_server_fingerprint", "campaign_id", "dataset_id", "day_definition_id",
		"exact_source_symbol", "gateway_build_identity", "producer_build_identity",
		"protocol_limits", "protocol_version", "provider_id", "publisher_epoch",
		"publisher_id", "settle_policy", "stable_feed_id",
	}); err != nil {
		return ScopeConfig{}, err
	}
	var result ScopeConfig
	for key, destination := range map[string]*string{
		"broker_server_fingerprint": &result.BrokerServerFingerprint,
		"campaign_id":               &result.CampaignID,
		"dataset_id":                &result.DatasetID,
		"day_definition_id":         &result.DayDefinitionID,
		"exact_source_symbol":       &result.ExactSourceSymbol,
		"gateway_build_identity":    &result.GatewayBuildIdentity,
		"producer_build_identity":   &result.ProducerBuildIdentity,
		"provider_id":               &result.ProviderID,
		"publisher_id":              &result.PublisherID,
		"settle_policy":             &result.SettlePolicy,
		"stable_feed_id":            &result.StableFeedID,
	} {
		parsed, err := stringValue(object, key, true)
		if err != nil {
			return ScopeConfig{}, err
		}
		*destination = parsed
	}
	protocolVersion, err := uint64Value(object, "protocol_version")
	if err != nil || protocolVersion > uint64(^uint16(0)) {
		return ScopeConfig{}, fmt.Errorf("protocol_version is outside uint16 range")
	}
	result.ProtocolVersion = uint16(protocolVersion)
	result.PublisherEpoch, err = uint64Value(object, "publisher_epoch")
	if err != nil {
		return ScopeConfig{}, err
	}
	limits, ok := object["protocol_limits"].(map[string]any)
	if !ok {
		return ScopeConfig{}, fmt.Errorf("protocol_limits must be an object")
	}
	if err := requireExactKeys(limits, []string{"max_frame_bytes", "max_records", "max_string_bytes"}); err != nil {
		return ScopeConfig{}, err
	}
	maxFrameBytes, err := uint64Value(limits, "max_frame_bytes")
	if err != nil || maxFrameBytes > uint64(^uint32(0)) {
		return ScopeConfig{}, fmt.Errorf("max_frame_bytes is outside uint32 range")
	}
	maxRecords, err := uint64Value(limits, "max_records")
	if err != nil || maxRecords > uint64(^uint32(0)) {
		return ScopeConfig{}, fmt.Errorf("max_records is outside uint32 range")
	}
	maxStringBytes, err := uint64Value(limits, "max_string_bytes")
	if err != nil || maxStringBytes > uint64(^uint16(0)) {
		return ScopeConfig{}, fmt.Errorf("max_string_bytes is outside uint16 range")
	}
	result.ProtocolLimits = ProtocolLimits{
		MaxFrameBytes:  uint32(maxFrameBytes),
		MaxRecords:     uint32(maxRecords),
		MaxStringBytes: uint16(maxStringBytes),
	}
	normalized, err := result.normalized()
	if err != nil {
		return ScopeConfig{}, err
	}
	canonical, err := normalized.CanonicalConfigJSON()
	if err != nil || !bytes.Equal(canonical, data) {
		return ScopeConfig{}, fmt.Errorf("archive config bytes are not canonical")
	}
	return normalized, nil
}

func (s ScopeConfig) ConfigHash() ([32]byte, error) {
	canonical, err := s.CanonicalConfigJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(ArchiveConfigDomain), canonical...)), nil
}

// IdentityPathKey hashes exact identity bytes without normalization or case
// folding, producing a lowercase hexadecimal path-safe component.
func IdentityPathKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func ScopePathKey(scope ScopeConfig) (string, error) {
	normalized, err := scope.normalized()
	if err != nil {
		return "", err
	}
	var input bytes.Buffer
	input.WriteString("tick-data-platform/campaign-scope/v1\x00")
	writeIdentity(&input, normalized.DatasetID)
	writeIdentity(&input, normalized.CampaignID)
	digest := sum256(input.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

// EnsureCampaignScopeDescriptor creates a no-clobber local descriptor. An
// identical retry succeeds; a different descriptor at the same scope fails
// with ErrIntegrity.
func EnsureCampaignScopeDescriptor(root string, scope ScopeConfig) (string, error) {
	if root == "" {
		return "", fmt.Errorf("campaign scope root is empty")
	}
	canonical, err := scope.CanonicalConfigJSON()
	if err != nil {
		return "", err
	}
	key, err := ScopePathKey(scope)
	if err != nil {
		return "", err
	}
	directory := filepath.Join(root, CampaignScopeVersion, key)
	path := filepath.Join(directory, "descriptor.json")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create campaign scope directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if _, writeErr := file.Write(canonical); writeErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write campaign scope descriptor: %w", writeErr)
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("sync campaign scope descriptor: %w", syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return "", fmt.Errorf("close campaign scope descriptor: %w", closeErr)
		}
		return path, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create campaign scope descriptor: %w", err)
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read campaign scope descriptor: %w", err)
	}
	if !bytes.Equal(existing, canonical) {
		return "", fmt.Errorf("%w: campaign scope descriptor differs", ErrIntegrity)
	}
	return path, nil
}

// RawSetRoot binds ordered content hashes and inclusive WAL coordinate ranges,
// while deliberately excluding object keys.
func RawSetRoot(objects []RawObjectRange) ([32]byte, error) {
	if len(objects) > int(^uint32(0)) {
		return [32]byte{}, fmt.Errorf("raw set has too many ranges")
	}
	var input bytes.Buffer
	input.WriteString(RawSetRootDomain)
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(objects)))
	input.Write(count[:])
	for i, object := range objects {
		if err := validateObjectRange(object); err != nil {
			return [32]byte{}, fmt.Errorf("raw set range %d: %w", i, err)
		}
		input.Write(object.SHA256[:])
		var value [8]byte
		binary.LittleEndian.PutUint64(value[:], object.Bytes)
		input.Write(value[:])
		binary.LittleEndian.PutUint64(value[:], object.StartIngestSequence)
		input.Write(value[:])
		binary.LittleEndian.PutUint64(value[:], object.EndIngestSequence)
		input.Write(value[:])
		var ordinal [4]byte
		binary.LittleEndian.PutUint32(ordinal[:], object.FirstRecordOrdinal)
		input.Write(ordinal[:])
		binary.LittleEndian.PutUint32(ordinal[:], object.LastRecordOrdinal)
		input.Write(ordinal[:])
	}
	return sha256.Sum256(input.Bytes()), nil
}

// BuildRawDayManifest builds a deterministic raw-day snapshot from RawObjects
// returned by PromoteSealedSegment. No wall clock is read; logical close time
// and status fields are explicit inputs.
func BuildRawDayManifest(input RawDayManifestInput) (RawDayManifest, error) {
	scope, err := input.Scope.normalized()
	if err != nil {
		return RawDayManifest{}, err
	}
	if _, err := parseUTCDate(input.Date); err != nil {
		return RawDayManifest{}, err
	}
	if !validSyncStatus(input.TerminalSyncStatus) {
		return RawDayManifest{}, fmt.Errorf("invalid terminal_sync_status %q", input.TerminalSyncStatus)
	}
	if !validCompletenessStatus(input.CompletenessStatus) {
		return RawDayManifest{}, fmt.Errorf("invalid completeness_status %q", input.CompletenessStatus)
	}
	revision := input.Revision
	if input.Previous == nil {
		if revision == 0 {
			revision = 1
		}
		if revision != 1 {
			return RawDayManifest{}, fmt.Errorf("genesis raw-day revision must be 1")
		}
	} else {
		if input.Previous.Revision < 1 {
			return RawDayManifest{}, fmt.Errorf("previous raw-day revision must be at least 1")
		}
		if revision == 0 {
			revision = input.Previous.Revision + 1
		}
		if revision != input.Previous.Revision+1 {
			return RawDayManifest{}, fmt.Errorf("raw-day revision must succeed previous revision")
		}
	}

	objects, err := orderedVerifiedObjects(input.RawObjects)
	if err != nil {
		return RawDayManifest{}, err
	}
	configHash, err := scope.ConfigHash()
	if err != nil {
		return RawDayManifest{}, err
	}
	manifest := RawDayManifest{
		ManifestVersion:    RawDayManifestVersion,
		DatasetID:          scope.DatasetID,
		CampaignID:         scope.CampaignID,
		DayDefinitionID:    scope.DayDefinitionID,
		Date:               input.Date,
		Revision:           revision,
		PublisherID:        scope.PublisherID,
		PublisherEpoch:     scope.PublisherEpoch,
		ConfigHash:         configHash,
		ProtocolVersion:    protocol.ProtocolVersion,
		SourceSchemaID:     protocol.SourceSchemaMT5,
		WALSchemaID:        "gateway-wal-v1",
		TerminalSyncStatus: input.TerminalSyncStatus,
		SettlePolicy:       scope.SettlePolicy,
		CompletenessStatus: input.CompletenessStatus,
		LogicalCloseTimeS:  input.LogicalCloseTimeS,
	}
	manifest.ManifestID = manifestID(scope, input.Date, revision)

	selection, err := deriveDaySelection(objects, input.Date, 0, 0, scope.ProtocolLimits.MaxRecords)
	if err != nil {
		return RawDayManifest{}, err
	}
	manifest.Objects = selection.Objects
	manifest.AcceptedRecordCount = selection.AcceptedRecordCount
	manifest.ErrorCount = selection.ErrorCount
	manifest.ObservedThroughSourceMSC = selection.ObservedThroughSourceMSC
	manifest.ObservedThroughCaptureSeq = selection.ObservedThroughCaptureSeq
	manifest.ChainSliceStartSequence = selection.ChainSliceStartSequence
	manifest.ChainSliceStartRoot = selection.ChainSliceStartRoot
	manifest.ChainSliceEndSequence = selection.ChainSliceEndSequence
	manifest.ChainSliceEndRoot = selection.ChainSliceEndRoot
	manifest.ChainObjects = chainObjectsForSlice(objects, selection.ChainSliceStartSequence, selection.ChainSliceEndSequence)
	if err := validateOrderedRanges(manifest.Objects); err != nil {
		return RawDayManifest{}, err
	}
	manifest.RawSetRoot, err = RawSetRoot(manifest.Objects)
	if err != nil {
		return RawDayManifest{}, err
	}
	if input.Previous != nil {
		if err := validatePreviousManifest(*input.Previous, manifest); err != nil {
			return RawDayManifest{}, err
		}
		previousDigest, err := ManifestDigest(*input.Previous)
		if err != nil {
			return RawDayManifest{}, err
		}
		if input.Previous.ManifestSHA256 != ([32]byte{}) && input.Previous.ManifestSHA256 != previousDigest {
			return RawDayManifest{}, fmt.Errorf("%w: previous manifest digest does not match its canonical bytes", ErrIntegrity)
		}
		manifest.PreviousManifestSHA256 = &previousDigest
	}
	if err := ValidateRawDayManifest(manifest); err != nil {
		return RawDayManifest{}, err
	}
	digest, err := ManifestDigest(manifest)
	if err != nil {
		return RawDayManifest{}, err
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

func orderedVerifiedObjects(input []RawObject) ([]RawObject, error) {
	objects := append([]RawObject(nil), input...)
	for i := range objects {
		object := &objects[i]
		if object.Path == "" {
			return nil, fmt.Errorf("%w: raw object %d has no verified source path", ErrIntegrity, i)
		}
		verified, err := wal.VerifySealedSegment(object.Path)
		if err != nil {
			return nil, fmt.Errorf("%w: raw object %d failed sealed WAL verification: %v", ErrIntegrity, i, err)
		}
		object.Segment = verified
		if object.Key == "" || object.Bytes < 0 || len(object.Segment.Entries) == 0 {
			return nil, fmt.Errorf("%w: raw object %d has invalid metadata", ErrIntegrity, i)
		}
		if object.SHA256 != object.Segment.ObjectSHA256 || uint64(object.Bytes) != uint64(object.Segment.FileBytes) {
			return nil, fmt.Errorf("%w: raw object %d metadata does not match verified segment", ErrIntegrity, i)
		}
		if err := validateRawWALObjectKey(object.Key, object.SHA256); err != nil {
			return nil, fmt.Errorf("raw object %d: %w", i, err)
		}
	}
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Segment.StartSequence != objects[j].Segment.StartSequence {
			return objects[i].Segment.StartSequence < objects[j].Segment.StartSequence
		}
		return bytes.Compare(objects[i].SHA256[:], objects[j].SHA256[:]) < 0
	})
	for i := 1; i < len(objects); i++ {
		previous := objects[i-1].Segment
		current := objects[i].Segment
		if previous.LastSequence == ^uint64(0) || current.StartSequence != previous.LastSequence+1 {
			return nil, fmt.Errorf("%w: campaign WAL segments are not contiguous", ErrIntegrity)
		}
		if current.ChainStart != previous.ChainRoot {
			return nil, fmt.Errorf("%w: campaign WAL chain predecessor mismatch", ErrIntegrity)
		}
	}
	return objects, nil
}

type daySelection struct {
	Objects                   []RawObjectRange
	AcceptedRecordCount       uint64
	ErrorCount                uint64
	ObservedThroughSourceMSC  int64
	ObservedThroughCaptureSeq uint64
	ChainSliceStartSequence   uint64
	ChainSliceStartRoot       [32]byte
	ChainSliceEndSequence     uint64
	ChainSliceEndRoot         [32]byte
	hasSourceWatermark        bool
}

func deriveDaySelection(objects []RawObject, date string, startSequence, endSequence uint64, maxRecords uint32) (daySelection, error) {
	selection := daySelection{}
	for _, object := range objects {
		for _, entry := range object.Segment.Entries {
			if startSequence != 0 && (entry.Sequence < startSequence || entry.Sequence > endSequence) {
				continue
			}
			frame, err := protocol.DecodeFrame(entry.Frame)
			if err != nil {
				return daySelection{}, fmt.Errorf("decode WAL entry %d: %w", entry.Sequence, err)
			}
			message, err := protocol.DecodeMessage(frame)
			if err != nil {
				return daySelection{}, fmt.Errorf("decode WAL entry %d: %w", entry.Sequence, err)
			}
			batch, ok := message.(protocol.BatchFrameV1)
			if !ok {
				return daySelection{}, fmt.Errorf("WAL entry %d is not BatchFrameV1", entry.Sequence)
			}
			if len(batch.Records) > int(maxRecords) {
				return daySelection{}, fmt.Errorf("WAL entry %d exceeds configured record limit", entry.Sequence)
			}
			ordinals := make([]uint32, 0, len(batch.Records))
			for ordinal, record := range batch.Records {
				if utcDate(record.TimeMSC) != date {
					continue
				}
				ordinals = append(ordinals, uint32(ordinal))
				selection.AcceptedRecordCount++
				if !selection.hasSourceWatermark || record.TimeMSC > selection.ObservedThroughSourceMSC {
					selection.ObservedThroughSourceMSC = record.TimeMSC
					selection.hasSourceWatermark = true
				}
				if record.CaptureSequence > selection.ObservedThroughCaptureSeq {
					selection.ObservedThroughCaptureSeq = record.CaptureSequence
				}
			}
			if len(batch.Records) == 0 && utcDate(batch.RequestedFromMSC) == date {
				ordinals = []uint32{0}
			}
			if len(ordinals) == 0 {
				continue
			}
			if selection.ChainSliceStartSequence == 0 {
				selection.ChainSliceStartSequence = entry.Sequence
				selection.ChainSliceStartRoot = entry.PreviousEntryHash
			}
			selection.ChainSliceEndSequence = entry.Sequence
			selection.ChainSliceEndRoot = entry.EntryHash
			if batch.CopyTicksError != 0 {
				selection.ErrorCount++
			}
			for start := 0; start < len(ordinals); {
				end := start
				for end+1 < len(ordinals) && ordinals[end+1] == ordinals[end]+1 {
					end++
				}
				selection.Objects = append(selection.Objects, RawObjectRange{
					Key:                 object.Key,
					SHA256:              object.SHA256,
					Bytes:               uint64(object.Bytes),
					StartIngestSequence: entry.Sequence,
					EndIngestSequence:   entry.Sequence,
					FirstRecordOrdinal:  ordinals[start],
					LastRecordOrdinal:   ordinals[end],
				})
				start = end + 1
			}
		}
	}
	return selection, nil
}

func chainObjectsForSlice(objects []RawObject, startSequence, endSequence uint64) []RawChainObject {
	if startSequence == 0 || endSequence == 0 {
		return nil
	}
	result := make([]RawChainObject, 0, len(objects))
	for _, object := range objects {
		if object.Segment.LastSequence < startSequence || object.Segment.StartSequence > endSequence {
			continue
		}
		result = append(result, RawChainObject{
			Key:                 object.Key,
			SHA256:              object.SHA256,
			Bytes:               uint64(object.Bytes),
			StartIngestSequence: object.Segment.StartSequence,
			EndIngestSequence:   object.Segment.LastSequence,
		})
	}
	return result
}

func validatePreviousManifest(previous RawDayManifest, current RawDayManifest) error {
	if err := ValidateRawDayManifest(previous); err != nil {
		return fmt.Errorf("%w: previous manifest is invalid: %v", ErrIntegrity, err)
	}
	if previous.ManifestVersion != RawDayManifestVersion || previous.DatasetID != current.DatasetID ||
		previous.CampaignID != current.CampaignID || previous.DayDefinitionID != current.DayDefinitionID ||
		previous.Date != current.Date || previous.PublisherID != current.PublisherID ||
		previous.PublisherEpoch != current.PublisherEpoch || previous.ConfigHash != current.ConfigHash {
		return fmt.Errorf("%w: previous raw-day scope or publisher differs", ErrIntegrity)
	}
	if len(previous.Objects) > len(current.Objects) {
		return fmt.Errorf("%w: raw-day objects are not cumulative", ErrIntegrity)
	}
	for i := range previous.Objects {
		if previous.Objects[i] != current.Objects[i] {
			return fmt.Errorf("%w: raw-day object ranges were reordered or changed", ErrIntegrity)
		}
	}
	if len(previous.ChainObjects) > len(current.ChainObjects) {
		return fmt.Errorf("%w: raw-day chain objects are not cumulative", ErrIntegrity)
	}
	for i := range previous.ChainObjects {
		if previous.ChainObjects[i] != current.ChainObjects[i] {
			return fmt.Errorf("%w: raw-day chain objects were reordered or changed", ErrIntegrity)
		}
	}
	if previous.AcceptedRecordCount > current.AcceptedRecordCount || previous.ErrorCount > current.ErrorCount {
		return fmt.Errorf("%w: raw-day counts decreased", ErrIntegrity)
	}
	if previous.ObservedThroughSourceMSC > current.ObservedThroughSourceMSC ||
		previous.ObservedThroughCaptureSeq > current.ObservedThroughCaptureSeq {
		return fmt.Errorf("%w: raw-day watermark decreased", ErrIntegrity)
	}
	if previous.ChainSliceStartSequence != current.ChainSliceStartSequence || previous.ChainSliceStartRoot != current.ChainSliceStartRoot {
		return fmt.Errorf("%w: raw-day chain start changed", ErrIntegrity)
	}
	if previous.ChainSliceEndSequence > current.ChainSliceEndSequence {
		return fmt.Errorf("%w: raw-day chain end moved backwards", ErrIntegrity)
	}
	if previous.ChainSliceEndSequence == current.ChainSliceEndSequence && previous.ChainSliceEndRoot != current.ChainSliceEndRoot {
		return fmt.Errorf("%w: raw-day chain end root changed without extension", ErrIntegrity)
	}
	return nil
}

func ManifestDigest(manifest RawDayManifest) ([32]byte, error) {
	canonical, err := ManifestCanonicalJSON(manifest)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(RawDayManifestDomain), canonical...)), nil
}

func ManifestCanonicalJSON(manifest RawDayManifest) ([]byte, error) {
	value, err := manifestValue(manifest)
	if err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(value)
}

func VerifyRawDayManifest(data []byte) (RawDayManifest, error) {
	value, err := protocol.DecodeCanonicalJSON(data)
	if err != nil {
		return RawDayManifest{}, err
	}
	manifest, err := rawDayManifestFromValue(value)
	if err != nil {
		return RawDayManifest{}, err
	}
	digest, err := ManifestDigest(manifest)
	if err != nil {
		return RawDayManifest{}, err
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

// VerifyRawDaySnapshot reopens every full sealed WAL object named by
// ChainObjects and proves that the manifest is a semantic view of those bytes.
// The caller supplies paths by their canonical campaign-relative object key.
func VerifyRawDaySnapshot(manifest RawDayManifest, objectPaths map[string]string) error {
	if err := ValidateRawDayManifest(manifest); err != nil {
		return fmt.Errorf("%w: manifest is invalid: %v", ErrIntegrity, err)
	}
	if manifest.ManifestSHA256 != ([32]byte{}) {
		digest, err := ManifestDigest(manifest)
		if err != nil {
			return fmt.Errorf("%w: manifest digest cannot be computed: %v", ErrIntegrity, err)
		}
		if manifest.ManifestSHA256 != digest {
			return fmt.Errorf("%w: manifest digest does not match canonical bytes", ErrIntegrity)
		}
	}
	if len(manifest.ChainObjects) == 0 {
		return nil
	}
	verifiedObjects := make([]RawObject, len(manifest.ChainObjects))
	seen := make(map[string]struct{}, len(manifest.ChainObjects))
	for i, descriptor := range manifest.ChainObjects {
		if _, ok := seen[descriptor.Key]; ok {
			return fmt.Errorf("%w: chain object %d is duplicated", ErrIntegrity, i)
		}
		seen[descriptor.Key] = struct{}{}
		path, ok := objectPaths[descriptor.Key]
		if !ok || path == "" {
			return fmt.Errorf("%w: chain object %q is missing locally", ErrIntegrity, descriptor.Key)
		}
		verified, err := wal.VerifySealedSegment(path)
		if err != nil {
			return fmt.Errorf("%w: chain object %q failed sealed WAL verification: %v", ErrIntegrity, descriptor.Key, err)
		}
		if verified.ObjectSHA256 != descriptor.SHA256 || uint64(verified.FileBytes) != descriptor.Bytes ||
			verified.StartSequence != descriptor.StartIngestSequence || verified.LastSequence != descriptor.EndIngestSequence {
			return fmt.Errorf("%w: chain object %q metadata does not match its bytes", ErrIntegrity, descriptor.Key)
		}
		verifiedObjects[i] = RawObject{
			Key:     descriptor.Key,
			Path:    path,
			SHA256:  verified.ObjectSHA256,
			Bytes:   verified.FileBytes,
			Segment: verified,
		}
		if i > 0 {
			previous := verifiedObjects[i-1].Segment
			if previous.LastSequence == ^uint64(0) || verified.StartSequence != previous.LastSequence+1 || verified.ChainStart != previous.ChainRoot {
				return fmt.Errorf("%w: chain object sequence or hash continuity failed at %d", ErrIntegrity, i)
			}
		}
	}

	expectedSequence := manifest.ChainSliceStartSequence
	expectedPrevious := manifest.ChainSliceStartRoot
	reachedEnd := false
	for _, object := range verifiedObjects {
		for _, entry := range object.Segment.Entries {
			if entry.Sequence < manifest.ChainSliceStartSequence {
				continue
			}
			if entry.Sequence > manifest.ChainSliceEndSequence {
				break
			}
			if entry.Sequence != expectedSequence || entry.PreviousEntryHash != expectedPrevious {
				return fmt.Errorf("%w: selected WAL entry chain is discontinuous at sequence %d", ErrIntegrity, entry.Sequence)
			}
			expectedPrevious = entry.EntryHash
			if entry.Sequence == manifest.ChainSliceEndSequence {
				reachedEnd = true
				break
			}
			expectedSequence++
		}
		if reachedEnd {
			break
		}
	}
	if !reachedEnd || expectedPrevious != manifest.ChainSliceEndRoot {
		return fmt.Errorf("%w: selected WAL entry chain does not match manifest roots", ErrIntegrity)
	}

	selection, err := deriveDaySelection(
		verifiedObjects,
		manifest.Date,
		manifest.ChainSliceStartSequence,
		manifest.ChainSliceEndSequence,
		protocol.MaxRecords,
	)
	if err != nil {
		return fmt.Errorf("%w: could not rederive selected ranges: %v", ErrIntegrity, err)
	}
	if !equalRawObjectRanges(selection.Objects, manifest.Objects) ||
		selection.AcceptedRecordCount != manifest.AcceptedRecordCount || selection.ErrorCount != manifest.ErrorCount ||
		selection.ObservedThroughSourceMSC != manifest.ObservedThroughSourceMSC || selection.ObservedThroughCaptureSeq != manifest.ObservedThroughCaptureSeq ||
		selection.ChainSliceStartSequence != manifest.ChainSliceStartSequence || selection.ChainSliceStartRoot != manifest.ChainSliceStartRoot ||
		selection.ChainSliceEndSequence != manifest.ChainSliceEndSequence || selection.ChainSliceEndRoot != manifest.ChainSliceEndRoot {
		return fmt.Errorf("%w: manifest selection or watermark does not match WAL bytes", ErrIntegrity)
	}
	wantChainObjects := chainObjectsForSlice(verifiedObjects, manifest.ChainSliceStartSequence, manifest.ChainSliceEndSequence)
	if !equalRawChainObjects(wantChainObjects, manifest.ChainObjects) {
		return fmt.Errorf("%w: chain_objects does not match WAL bytes", ErrIntegrity)
	}
	return nil
}

func equalRawObjectRanges(left, right []RawObjectRange) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalRawChainObjects(left, right []RawChainObject) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func manifestValue(manifest RawDayManifest) (map[string]any, error) {
	if manifest.ManifestVersion == "" {
		manifest.ManifestVersion = RawDayManifestVersion
	}
	objects := make([]any, len(manifest.Objects))
	for i, object := range manifest.Objects {
		objects[i] = map[string]any{
			"bytes":                 object.Bytes,
			"end_ingest_sequence":   object.EndIngestSequence,
			"first_record_ordinal":  object.FirstRecordOrdinal,
			"key":                   object.Key,
			"last_record_ordinal":   object.LastRecordOrdinal,
			"sha256":                hex.EncodeToString(object.SHA256[:]),
			"start_ingest_sequence": object.StartIngestSequence,
		}
	}
	chainObjects := make([]any, len(manifest.ChainObjects))
	for i, object := range manifest.ChainObjects {
		chainObjects[i] = map[string]any{
			"bytes":                 object.Bytes,
			"end_ingest_sequence":   object.EndIngestSequence,
			"key":                   object.Key,
			"sha256":                hex.EncodeToString(object.SHA256[:]),
			"start_ingest_sequence": object.StartIngestSequence,
		}
	}
	previous := any(nil)
	if manifest.PreviousManifestSHA256 != nil {
		previous = hex.EncodeToString(manifest.PreviousManifestSHA256[:])
	}
	return map[string]any{
		"accepted_record_count":             manifest.AcceptedRecordCount,
		"campaign_id":                       manifest.CampaignID,
		"chain_slice_end_root":              hex.EncodeToString(manifest.ChainSliceEndRoot[:]),
		"chain_slice_end_sequence":          manifest.ChainSliceEndSequence,
		"chain_slice_start_root":            hex.EncodeToString(manifest.ChainSliceStartRoot[:]),
		"chain_slice_start_sequence":        manifest.ChainSliceStartSequence,
		"completeness_status":               manifest.CompletenessStatus,
		"chain_objects":                     chainObjects,
		"config_hash":                       hex.EncodeToString(manifest.ConfigHash[:]),
		"dataset_id":                        manifest.DatasetID,
		"date":                              manifest.Date,
		"day_definition_id":                 manifest.DayDefinitionID,
		"error_count":                       manifest.ErrorCount,
		"logical_close_time_s":              manifest.LogicalCloseTimeS,
		"manifest_id":                       manifest.ManifestID,
		"manifest_version":                  manifest.ManifestVersion,
		"objects":                           objects,
		"observed_through_capture_sequence": manifest.ObservedThroughCaptureSeq,
		"observed_through_source_msc":       manifest.ObservedThroughSourceMSC,
		"previous_manifest_sha256":          previous,
		"protocol_version":                  manifest.ProtocolVersion,
		"publisher_epoch":                   manifest.PublisherEpoch,
		"publisher_id":                      manifest.PublisherID,
		"raw_set_root":                      hex.EncodeToString(manifest.RawSetRoot[:]),
		"revision":                          manifest.Revision,
		"settle_policy":                     manifest.SettlePolicy,
		"source_schema_id":                  manifest.SourceSchemaID,
		"terminal_sync_status":              manifest.TerminalSyncStatus,
		"wal_schema_id":                     manifest.WALSchemaID,
	}, nil
}

func rawDayManifestFromValue(value any) (RawDayManifest, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return RawDayManifest{}, fmt.Errorf("raw-day manifest must be a JSON object")
	}
	if err := requireExactKeys(object, []string{
		"accepted_record_count", "campaign_id", "chain_slice_end_root", "chain_slice_end_sequence",
		"chain_slice_start_root", "chain_slice_start_sequence", "completeness_status", "config_hash",
		"chain_objects", "dataset_id", "date", "day_definition_id", "error_count", "logical_close_time_s",
		"manifest_id", "manifest_version", "objects", "observed_through_capture_sequence", "observed_through_source_msc",
		"previous_manifest_sha256", "protocol_version", "publisher_epoch", "publisher_id", "raw_set_root",
		"revision", "settle_policy", "source_schema_id", "terminal_sync_status", "wal_schema_id",
	}); err != nil {
		return RawDayManifest{}, err
	}
	var result RawDayManifest
	var err error
	result.ManifestVersion, err = stringValue(object, "manifest_version", true)
	if err != nil || result.ManifestVersion != RawDayManifestVersion {
		return RawDayManifest{}, fmt.Errorf("invalid manifest_version")
	}
	result.ManifestID, err = stringValue(object, "manifest_id", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.DatasetID, err = stringValue(object, "dataset_id", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.CampaignID, err = stringValue(object, "campaign_id", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.DayDefinitionID, err = stringValue(object, "day_definition_id", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.Date, err = stringValue(object, "date", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	if _, err := parseUTCDate(result.Date); err != nil {
		return RawDayManifest{}, err
	}
	result.PublisherID, err = stringValue(object, "publisher_id", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.SettlePolicy, err = stringValue(object, "settle_policy", true)
	if err != nil {
		return RawDayManifest{}, err
	}
	result.CompletenessStatus, err = stringValue(object, "completeness_status", true)
	if err != nil || !validCompletenessStatus(result.CompletenessStatus) {
		return RawDayManifest{}, fmt.Errorf("invalid completeness_status")
	}
	result.TerminalSyncStatus, err = stringValue(object, "terminal_sync_status", true)
	if err != nil || !validSyncStatus(result.TerminalSyncStatus) {
		return RawDayManifest{}, fmt.Errorf("invalid terminal_sync_status")
	}
	result.SourceSchemaID, err = stringValue(object, "source_schema_id", true)
	if err != nil || result.SourceSchemaID != protocol.SourceSchemaMT5 {
		return RawDayManifest{}, fmt.Errorf("invalid source_schema_id")
	}
	result.WALSchemaID, err = stringValue(object, "wal_schema_id", true)
	if err != nil || result.WALSchemaID != "gateway-wal-v1" {
		return RawDayManifest{}, fmt.Errorf("invalid wal_schema_id")
	}
	result.ProtocolVersion, err = uint16Value(object, "protocol_version")
	if err != nil || result.ProtocolVersion != protocol.ProtocolVersion {
		return RawDayManifest{}, fmt.Errorf("invalid protocol_version")
	}
	result.Revision, err = uint64Value(object, "revision")
	if err != nil || result.Revision < 1 {
		return RawDayManifest{}, fmt.Errorf("revision must be at least 1")
	}
	result.PublisherEpoch, err = uint64Value(object, "publisher_epoch")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.AcceptedRecordCount, err = uint64Value(object, "accepted_record_count")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.ErrorCount, err = uint64Value(object, "error_count")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.ObservedThroughCaptureSeq, err = uint64Value(object, "observed_through_capture_sequence")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.ObservedThroughSourceMSC, err = int64Value(object, "observed_through_source_msc")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.ChainSliceStartSequence, err = uint64Value(object, "chain_slice_start_sequence")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.ChainSliceEndSequence, err = uint64Value(object, "chain_slice_end_sequence")
	if err != nil {
		return RawDayManifest{}, err
	}
	result.LogicalCloseTimeS, err = int64Value(object, "logical_close_time_s")
	if err != nil {
		return RawDayManifest{}, err
	}
	for key, destination := range map[string]*[32]byte{
		"config_hash":            &result.ConfigHash,
		"chain_slice_start_root": &result.ChainSliceStartRoot,
		"chain_slice_end_root":   &result.ChainSliceEndRoot,
		"raw_set_root":           &result.RawSetRoot,
	} {
		if err := hashValue(object, key, destination); err != nil {
			return RawDayManifest{}, err
		}
	}
	previous, ok := object["previous_manifest_sha256"]
	if !ok {
		return RawDayManifest{}, fmt.Errorf("missing previous_manifest_sha256")
	}
	if previous != nil {
		var digest [32]byte
		if err := parseHash(previous, &digest); err != nil {
			return RawDayManifest{}, err
		}
		result.PreviousManifestSHA256 = &digest
	}
	rawChainObjects, ok := object["chain_objects"].([]any)
	if !ok {
		return RawDayManifest{}, fmt.Errorf("chain_objects must be an array")
	}
	result.ChainObjects = make([]RawChainObject, 0, len(rawChainObjects))
	for i, rawObject := range rawChainObjects {
		item, ok := rawObject.(map[string]any)
		if !ok {
			return RawDayManifest{}, fmt.Errorf("chain_objects[%d] must be an object", i)
		}
		if err := requireExactKeys(item, []string{"bytes", "end_ingest_sequence", "key", "sha256", "start_ingest_sequence"}); err != nil {
			return RawDayManifest{}, fmt.Errorf("chain_objects[%d]: %w", i, err)
		}
		var chainObject RawChainObject
		chainObject.Key, err = stringValue(item, "key", true)
		if err != nil {
			return RawDayManifest{}, err
		}
		if err := hashValue(item, "sha256", &chainObject.SHA256); err != nil {
			return RawDayManifest{}, err
		}
		chainObject.Bytes, err = uint64Value(item, "bytes")
		if err != nil {
			return RawDayManifest{}, err
		}
		chainObject.StartIngestSequence, err = uint64Value(item, "start_ingest_sequence")
		if err != nil {
			return RawDayManifest{}, err
		}
		chainObject.EndIngestSequence, err = uint64Value(item, "end_ingest_sequence")
		if err != nil {
			return RawDayManifest{}, err
		}
		result.ChainObjects = append(result.ChainObjects, chainObject)
	}
	rawObjects, ok := object["objects"].([]any)
	if !ok {
		return RawDayManifest{}, fmt.Errorf("objects must be an array")
	}
	result.Objects = make([]RawObjectRange, 0, len(rawObjects))
	for i, rawObject := range rawObjects {
		item, ok := rawObject.(map[string]any)
		if !ok {
			return RawDayManifest{}, fmt.Errorf("objects[%d] must be an object", i)
		}
		if err := requireExactKeys(item, []string{"bytes", "end_ingest_sequence", "first_record_ordinal", "key", "last_record_ordinal", "sha256", "start_ingest_sequence"}); err != nil {
			return RawDayManifest{}, fmt.Errorf("objects[%d]: %w", i, err)
		}
		var itemRange RawObjectRange
		itemRange.Key, err = stringValue(item, "key", true)
		if err != nil {
			return RawDayManifest{}, err
		}
		if err := hashValue(item, "sha256", &itemRange.SHA256); err != nil {
			return RawDayManifest{}, err
		}
		itemRange.Bytes, err = uint64Value(item, "bytes")
		if err != nil {
			return RawDayManifest{}, err
		}
		itemRange.StartIngestSequence, err = uint64Value(item, "start_ingest_sequence")
		if err != nil {
			return RawDayManifest{}, err
		}
		itemRange.EndIngestSequence, err = uint64Value(item, "end_ingest_sequence")
		if err != nil {
			return RawDayManifest{}, err
		}
		itemRange.FirstRecordOrdinal, err = uint32Value(item, "first_record_ordinal")
		if err != nil {
			return RawDayManifest{}, err
		}
		itemRange.LastRecordOrdinal, err = uint32Value(item, "last_record_ordinal")
		if err != nil {
			return RawDayManifest{}, err
		}
		result.Objects = append(result.Objects, itemRange)
	}
	if err := validateOrderedRanges(result.Objects); err != nil {
		return RawDayManifest{}, err
	}
	wantRoot, err := RawSetRoot(result.Objects)
	if err != nil {
		return RawDayManifest{}, err
	}
	if result.RawSetRoot != wantRoot {
		return RawDayManifest{}, fmt.Errorf("raw_set_root does not match ordered objects")
	}
	if err := ValidateRawDayManifest(result); err != nil {
		return RawDayManifest{}, err
	}
	return result, nil
}

func ValidateRawDayManifest(manifest RawDayManifest) error {
	if manifest.ManifestVersion != RawDayManifestVersion || manifest.ManifestID == "" || manifest.Revision < 1 {
		return fmt.Errorf("invalid raw-day manifest identity or revision")
	}
	if manifest.DatasetID == "" || manifest.CampaignID == "" || manifest.DayDefinitionID == "" || manifest.Date == "" ||
		manifest.PublisherID == "" || manifest.SettlePolicy == "" {
		return fmt.Errorf("raw-day manifest identity fields are required")
	}
	if _, err := parseUTCDate(manifest.Date); err != nil {
		return err
	}
	if manifest.ProtocolVersion != protocol.ProtocolVersion || manifest.SourceSchemaID != protocol.SourceSchemaMT5 || manifest.WALSchemaID != "gateway-wal-v1" {
		return fmt.Errorf("raw-day manifest protocol schema mismatch")
	}
	if !validSyncStatus(manifest.TerminalSyncStatus) || !validCompletenessStatus(manifest.CompletenessStatus) {
		return fmt.Errorf("raw-day manifest status is invalid")
	}
	if err := validateOrderedRanges(manifest.Objects); err != nil {
		return err
	}
	wantRoot, err := RawSetRoot(manifest.Objects)
	if err != nil {
		return err
	}
	if manifest.RawSetRoot != wantRoot {
		return fmt.Errorf("raw_set_root does not match ordered objects")
	}
	if err := validateChainObjects(manifest); err != nil {
		return err
	}
	if (manifest.Revision == 1) != (manifest.PreviousManifestSHA256 == nil) {
		return fmt.Errorf("raw-day revision and previous manifest hash do not agree")
	}
	if len(manifest.Objects) == 0 {
		if manifest.ChainSliceStartSequence != 0 || manifest.ChainSliceEndSequence != 0 || manifest.ChainSliceStartRoot != ([32]byte{}) || manifest.ChainSliceEndRoot != ([32]byte{}) {
			return fmt.Errorf("empty raw-day manifest must have zero chain slice")
		}
	} else if manifest.ChainSliceStartSequence == 0 || manifest.ChainSliceEndSequence < manifest.ChainSliceStartSequence {
		return fmt.Errorf("raw-day chain slice range is invalid")
	}
	return nil
}

func validateObjectRange(object RawObjectRange) error {
	if object.Key == "" || !utf8.ValidString(object.Key) {
		return fmt.Errorf("object key is empty or invalid UTF-8")
	}
	if err := validateRawWALObjectKey(object.Key, object.SHA256); err != nil {
		return err
	}
	if object.Bytes == 0 {
		return fmt.Errorf("object bytes must be nonzero")
	}
	if object.StartIngestSequence == 0 || object.EndIngestSequence < object.StartIngestSequence {
		return fmt.Errorf("object sequence range is empty or reversed")
	}
	if object.StartIngestSequence == object.EndIngestSequence && object.LastRecordOrdinal < object.FirstRecordOrdinal {
		return fmt.Errorf("object ordinal range is empty or reversed")
	}
	return nil
}

func validateChainObjects(manifest RawDayManifest) error {
	if len(manifest.ChainObjects) == 0 {
		if len(manifest.Objects) != 0 || manifest.ChainSliceStartSequence != 0 || manifest.ChainSliceEndSequence != 0 ||
			manifest.ChainSliceStartRoot != ([32]byte{}) || manifest.ChainSliceEndRoot != ([32]byte{}) {
			return fmt.Errorf("empty chain_objects must have empty objects and zero chain slice")
		}
		return nil
	}
	if len(manifest.Objects) == 0 || manifest.ChainSliceStartSequence == 0 || manifest.ChainSliceEndSequence < manifest.ChainSliceStartSequence {
		return fmt.Errorf("non-empty chain_objects require a non-empty chain slice and objects")
	}
	seen := make(map[string]struct{}, len(manifest.ChainObjects))
	for i, object := range manifest.ChainObjects {
		if err := validateRawWALObjectKey(object.Key, object.SHA256); err != nil {
			return fmt.Errorf("chain object %d: %w", i, err)
		}
		if object.Bytes == 0 || object.StartIngestSequence == 0 || object.EndIngestSequence < object.StartIngestSequence {
			return fmt.Errorf("chain object %d has an empty or reversed range", i)
		}
		if _, ok := seen[object.Key]; ok {
			return fmt.Errorf("chain object %d duplicates an earlier object", i)
		}
		seen[object.Key] = struct{}{}
		if object.StartIngestSequence > manifest.ChainSliceEndSequence || object.EndIngestSequence < manifest.ChainSliceStartSequence {
			return fmt.Errorf("chain object %d does not intersect the chain slice", i)
		}
		if i > 0 {
			previous := manifest.ChainObjects[i-1]
			if previous.EndIngestSequence == ^uint64(0) || object.StartIngestSequence != previous.EndIngestSequence+1 {
				return fmt.Errorf("chain objects are not strictly contiguous")
			}
		}
	}
	first := manifest.ChainObjects[0]
	last := manifest.ChainObjects[len(manifest.ChainObjects)-1]
	if manifest.ChainSliceStartSequence < first.StartIngestSequence || manifest.ChainSliceStartSequence > first.EndIngestSequence ||
		manifest.ChainSliceEndSequence < last.StartIngestSequence || manifest.ChainSliceEndSequence > last.EndIngestSequence {
		return fmt.Errorf("chain slice is not contained by its first and last chain objects")
	}
	for i, object := range manifest.Objects {
		var matches int
		for _, chainObject := range manifest.ChainObjects {
			if object.Key == chainObject.Key && object.SHA256 == chainObject.SHA256 && object.Bytes == chainObject.Bytes {
				matches++
				if object.StartIngestSequence < chainObject.StartIngestSequence || object.EndIngestSequence > chainObject.EndIngestSequence ||
					object.StartIngestSequence < manifest.ChainSliceStartSequence || object.EndIngestSequence > manifest.ChainSliceEndSequence {
					return fmt.Errorf("object range %d is outside its chain object or chain slice", i)
				}
			}
		}
		if matches != 1 {
			return fmt.Errorf("object range %d does not match exactly one chain object", i)
		}
	}
	return nil
}

func validateOrderedRanges(objects []RawObjectRange) error {
	var previous RawObjectRange
	for i, object := range objects {
		if err := validateObjectRange(object); err != nil {
			return fmt.Errorf("object range %d: %w", i, err)
		}
		if i > 0 && compareCoordinate(object.StartIngestSequence, object.FirstRecordOrdinal, previous.EndIngestSequence, previous.LastRecordOrdinal) <= 0 {
			return fmt.Errorf("object ranges are not strictly ascending and non-overlapping")
		}
		previous = object
	}
	return nil
}

func compareCoordinate(leftSequence uint64, leftOrdinal uint32, rightSequence uint64, rightOrdinal uint32) int {
	if leftSequence < rightSequence {
		return -1
	}
	if leftSequence > rightSequence {
		return 1
	}
	if leftOrdinal < rightOrdinal {
		return -1
	}
	if leftOrdinal > rightOrdinal {
		return 1
	}
	return 0
}

func manifestID(scope ScopeConfig, date string, revision uint64) string {
	key, _ := ScopePathKey(scope)
	return fmt.Sprintf("raw-%s-%s-r%d", key[:16], date, revision)
}

func parseUTCDate(value string) (time.Time, error) {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil || parsed.Format("2006-01-02") != value {
		return time.Time{}, fmt.Errorf("date must be UTC YYYY-MM-DD")
	}
	return parsed.UTC(), nil
}

func utcDate(milliseconds int64) string {
	return time.UnixMilli(milliseconds).UTC().Format("2006-01-02")
}

func validSyncStatus(value string) bool {
	return value != ""
}

func validCompletenessStatus(value string) bool {
	switch value {
	case "provisional", "settled_snapshot", "incomplete_source_error", "incomplete_sync", "incomplete_gateway_outage":
		return true
	default:
		return false
	}
}

func requireExactKeys(object map[string]any, expected []string) error {
	want := make(map[string]struct{}, len(expected))
	for _, key := range expected {
		want[key] = struct{}{}
	}
	for key := range object {
		if _, ok := want[key]; !ok {
			return fmt.Errorf("unknown JSON key %q", key)
		}
	}
	for key := range want {
		if _, ok := object[key]; !ok {
			return fmt.Errorf("missing JSON key %q", key)
		}
	}
	return nil
}

func stringValue(object map[string]any, key string, nonEmpty bool) (string, error) {
	value, ok := object[key].(string)
	if !ok || !utf8.ValidString(value) || (nonEmpty && value == "") {
		return "", fmt.Errorf("%s must be a %s string", key, map[bool]string{true: "non-empty", false: ""}[nonEmpty])
	}
	return value, nil
}

func uint64Value(object map[string]any, key string) (uint64, error) {
	value, ok := object[key].(uint64)
	if !ok {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return value, nil
}

func uint32Value(object map[string]any, key string) (uint32, error) {
	value, err := uint64Value(object, key)
	if err != nil || value > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%s is outside uint32 range", key)
	}
	return uint32(value), nil
}

func uint16Value(object map[string]any, key string) (uint16, error) {
	value, err := uint64Value(object, key)
	if err != nil || value > uint64(^uint16(0)) {
		return 0, fmt.Errorf("%s is outside uint16 range", key)
	}
	return uint16(value), nil
}

func int64Value(object map[string]any, key string) (int64, error) {
	value, ok := object[key].(int64)
	if ok {
		return value, nil
	}
	if unsigned, ok := object[key].(uint64); ok && unsigned <= uint64(^uint64(0)>>1) {
		return int64(unsigned), nil
	}
	return 0, fmt.Errorf("%s must be an integer in int64 range", key)
}

func hashValue(object map[string]any, key string, destination *[32]byte) error {
	value, ok := object[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	return parseHash(value, destination)
}

func parseHash(value any, destination *[32]byte) error {
	text, ok := value.(string)
	if !ok || len(text) != 64 {
		return fmt.Errorf("hash must be 64 lowercase hexadecimal characters")
	}
	for _, char := range text {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return fmt.Errorf("hash must be lowercase hexadecimal")
		}
	}
	decoded, err := hex.DecodeString(text)
	if err != nil {
		return fmt.Errorf("invalid hash: %w", err)
	}
	copy(destination[:], decoded)
	return nil
}

func sum256(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func writeIdentity(buffer *bytes.Buffer, value string) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	buffer.Write(length[:])
	buffer.WriteString(value)
}
