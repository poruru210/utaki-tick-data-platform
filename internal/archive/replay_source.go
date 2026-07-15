package archive

import (
	"bytes"
	"fmt"
	"os"

	"tick-data-platform/internal/continuity"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

// ReplaySourceInput is the handoff from a read-only archive fetch to M3.
// ManifestBytes and ObjectPaths must be the bytes and local paths obtained
// from M2 delivery; no caller-supplied WAL entries are accepted.
type ReplaySourceInput struct {
	Scope               ScopeConfig
	ProducerInstanceID  string
	ManifestRelativeKey string
	ManifestBytes       []byte
	ObjectPaths         map[string]string
	ReplayContractID    string
	ConversionID        string
	ResourceLimits      ReplayResourceLimits
}

// ReplayResourceLimits are mandatory replay_contract_id-scoped limits for
// archive verification. They bound descriptor count, one sealed object parse,
// and aggregate chain bytes retained by the current verifier.
type ReplayResourceLimits struct {
	MaxChainObjects uint64
	MaxObjectBytes  uint64
	MaxChainBytes   uint64
}

func (limits ReplayResourceLimits) validate() error {
	if limits.MaxChainObjects == 0 || limits.MaxObjectBytes == 0 || limits.MaxChainBytes == 0 {
		return fmt.Errorf("%w: replay resource limits must be nonzero", ErrIntegrity)
	}
	return nil
}

func validateReplayResourceLimits(manifest RawDayManifest, limits ReplayResourceLimits) error {
	if err := limits.validate(); err != nil {
		return err
	}
	if uint64(len(manifest.ChainObjects)) > limits.MaxChainObjects {
		return fmt.Errorf("%w: chain object count exceeds replay resource limit", ErrIntegrity)
	}
	var totalBytes uint64
	for index, descriptor := range manifest.ChainObjects {
		if descriptor.Bytes > limits.MaxObjectBytes {
			return fmt.Errorf("%w: chain object %d exceeds max object bytes", ErrIntegrity, index)
		}
		if descriptor.Bytes > limits.MaxChainBytes-totalBytes {
			return fmt.Errorf("%w: chain object bytes exceed max chain bytes", ErrIntegrity)
		}
		totalBytes += descriptor.Bytes
	}
	return nil
}

func checkReplayObjectPath(path string, limits ReplayResourceLimits, loadedBytes uint64) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("%w: stat sealed WAL: %v", ErrIntegrity, err)
	}
	if !info.Mode().IsRegular() || info.Size() < 0 {
		return 0, fmt.Errorf("%w: sealed WAL path is not a regular file", ErrIntegrity)
	}
	bytes := uint64(info.Size())
	if bytes > limits.MaxObjectBytes {
		return 0, fmt.Errorf("%w: sealed WAL exceeds max object bytes", ErrIntegrity)
	}
	if bytes > limits.MaxChainBytes-loadedBytes {
		return 0, fmt.Errorf("%w: sealed WAL bytes exceed max chain bytes", ErrIntegrity)
	}
	return bytes, nil
}

// OpenVerifiedReplaySource verifies the complete raw-day snapshot and returns
// a reader that reopens sealed objects in chain order. The reader exposes only
// manifest.objects-selected BatchFrameV1 coordinates; chain-only entries are
// consumed by verification but never become replay data.
func OpenVerifiedReplaySource(input ReplaySourceInput) (continuity.VerifiedBatchReader, error) {
	if input.Scope.ProtocolLimits.MaxRecords == 0 || input.Scope.ProtocolLimits.MaxRecords > protocol.MaxRecords {
		return nil, fmt.Errorf("%w: max_records must be explicit and bounded", ErrIntegrity)
	}
	if input.ProducerInstanceID == "" {
		return nil, fmt.Errorf("%w: producer instance identity is required", ErrIntegrity)
	}
	manifest, err := VerifyRawDayManifest(input.ManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: raw-day manifest verification failed: %v", ErrIntegrity, err)
	}
	canonical, err := ManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, input.ManifestBytes) {
		return nil, fmt.Errorf("%w: raw-day manifest bytes are not canonical", ErrIntegrity)
	}
	if err := ValidateRawDayManifest(manifest); err != nil {
		return nil, fmt.Errorf("%w: raw-day manifest is invalid: %v", ErrIntegrity, err)
	}
	relativeManifestKey, err := RawDayManifestRelativeKey(input.Scope, manifest)
	if err != nil {
		return nil, err
	}
	if err := VerifyRawDayManifestRelativeKey(input.Scope, manifest, input.ManifestRelativeKey); err != nil {
		return nil, err
	}
	if err := validateReplayResourceLimits(manifest, input.ResourceLimits); err != nil {
		return nil, err
	}
	verificationManifest, err := expandManifestRangesForVerification(manifest, input.ObjectPaths, input.ResourceLimits)
	if err != nil {
		return nil, err
	}
	if err := verifyReplaySnapshot(manifest, verificationManifest, input.ObjectPaths, input.Scope, input.ResourceLimits); err != nil {
		return nil, err
	}
	if input.ReplayContractID == "" || input.ConversionID == "" {
		return nil, fmt.Errorf("%w: replay contract and conversion are required", ErrIntegrity)
	}
	scope := protocol.ReplayScope{
		DatasetID: input.Scope.DatasetID, CampaignID: input.Scope.CampaignID,
		DayDefinitionID: input.Scope.DayDefinitionID, Date: manifest.Date,
		ReplayContractID: input.ReplayContractID, ConversionID: input.ConversionID,
		RawDayManifestKey: relativeManifestKey, RawDayManifestSHA256: manifest.ManifestSHA256,
	}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: replay scope: %v", ErrIntegrity, err)
	}
	selected := make(map[string]map[uint64][]recordOrdinalRange, len(verificationManifest.Objects))
	for _, object := range verificationManifest.Objects {
		bySequence := selected[object.Key]
		if bySequence == nil {
			bySequence = make(map[uint64][]recordOrdinalRange)
			selected[object.Key] = bySequence
		}
		for sequence := object.StartIngestSequence; ; sequence++ {
			rangeSelection := recordOrdinalRange{first: 0, last: 0, throughEnd: true}
			if sequence == object.StartIngestSequence {
				rangeSelection.first = object.FirstRecordOrdinal
			}
			if sequence == object.EndIngestSequence {
				rangeSelection.last = object.LastRecordOrdinal
				rangeSelection.throughEnd = false
			}
			bySequence[sequence] = append(bySequence[sequence], rangeSelection)
			if sequence == object.EndIngestSequence {
				break
			}
			if sequence == ^uint64(0) {
				return nil, fmt.Errorf("%w: raw object range sequence overflow", ErrIntegrity)
			}
		}
	}
	chain := append([]RawChainObject(nil), manifest.ChainObjects...)
	paths := make(map[string]string, len(input.ObjectPaths))
	for key, path := range input.ObjectPaths {
		paths[key] = path
	}
	return &verifiedReplaySource{
		scope: scope, maxRecords: input.Scope.ProtocolLimits.MaxRecords,
		producerIdentity: continuity.ProducerIdentity{
			ProducerInstanceID: input.ProducerInstanceID,
			CampaignID:         input.Scope.CampaignID, ProviderID: input.Scope.ProviderID,
			StableFeedID:            input.Scope.StableFeedID,
			BrokerServerFingerprint: input.Scope.BrokerServerFingerprint,
			ExactSourceSymbol:       input.Scope.ExactSourceSymbol,
		},
		chain: chain, paths: paths, selected: selected, resourceLimits: input.ResourceLimits,
	}, nil
}

type recordOrdinalRange struct {
	first      uint32
	last       uint32
	throughEnd bool
}

type verifiedReplaySource struct {
	scope            protocol.ReplayScope
	producerIdentity continuity.ProducerIdentity
	maxRecords       uint32
	chain            []RawChainObject
	paths            map[string]string
	selected         map[string]map[uint64][]recordOrdinalRange
	resourceLimits   ReplayResourceLimits
	objectAt         int
	entries          []wal.Entry
	entryAt          int
	objectKey        string
	objectSHA        [32]byte
}

func (s *verifiedReplaySource) ReplayScope() protocol.ReplayScope { return s.scope }

func (s *verifiedReplaySource) ProducerIdentity() continuity.ProducerIdentity {
	return s.producerIdentity
}

func (s *verifiedReplaySource) MaxRecords() uint32 { return s.maxRecords }

func (s *verifiedReplaySource) Next() (continuity.VerifiedBatch, bool, error) {
	for {
		if s.entryAt >= len(s.entries) {
			if s.objectAt >= len(s.chain) {
				return continuity.VerifiedBatch{}, false, nil
			}
			descriptor := s.chain[s.objectAt]
			path := s.paths[descriptor.Key]
			if _, err := checkReplayObjectPath(path, s.resourceLimits, 0); err != nil {
				return continuity.VerifiedBatch{}, false, fmt.Errorf("%w: reverify %q resource limit: %v", ErrIntegrity, descriptor.Key, err)
			}
			segment, err := wal.VerifySealedSegment(path)
			if err != nil {
				return continuity.VerifiedBatch{}, false, fmt.Errorf("%w: reverify %q: %v", ErrIntegrity, descriptor.Key, err)
			}
			if uint64(segment.FileBytes) > s.resourceLimits.MaxObjectBytes || segment.ObjectSHA256 != descriptor.SHA256 || uint64(segment.FileBytes) != descriptor.Bytes ||
				segment.StartSequence != descriptor.StartIngestSequence || segment.LastSequence != descriptor.EndIngestSequence {
				return continuity.VerifiedBatch{}, false, fmt.Errorf("%w: reverified object %q changed", ErrIntegrity, descriptor.Key)
			}
			s.entries = segment.Entries
			s.entryAt = 0
			s.objectAt++
			s.objectKey = descriptor.Key
			s.objectSHA = descriptor.SHA256
		}
		entry := s.entries[s.entryAt]
		s.entryAt++
		ranges := s.selected[s.objectKey][entry.Sequence]
		if len(ranges) == 0 {
			continue
		}
		ordinals, err := selectRecordOrdinals(entry.Frame, ranges)
		if err != nil {
			return continuity.VerifiedBatch{}, false, fmt.Errorf("%w: object %q sequence %d selection: %v", ErrIntegrity, s.objectKey, entry.Sequence, err)
		}
		if len(ordinals) == 0 {
			continue
		}
		return continuity.VerifiedBatch{
			ObjectKey: s.objectKey, ObjectSHA256: s.objectSHA,
			GatewayIngestSequence: entry.Sequence, PreviousEntryHash: entry.PreviousEntryHash,
			EntryHash: entry.EntryHash, ReceiveWallS: entry.ReceiveWallS,
			ReceiveMonotonicUS: entry.ReceiveMonotonicUS, Frame: append([]byte(nil), entry.Frame...),
			SelectedRecordOrdinals: ordinals,
		}, true, nil
	}
}

func selectRecordOrdinals(frame []byte, ranges []recordOrdinalRange) ([]uint32, error) {
	batch, err := decodeBatchFrame(frame)
	if err != nil {
		return nil, err
	}
	ordinals := make([]uint32, 0)
	for _, selected := range ranges {
		if len(batch.Records) == 0 {
			if !selected.throughEnd && selected.first == 0 && selected.last == 0 {
				ordinals = appendUniqueOrdinal(ordinals, 0)
				continue
			}
			return nil, fmt.Errorf("range selects records from a zero-record BatchFrameV1")
		}
		last := selected.last
		if selected.throughEnd {
			last = uint32(len(batch.Records) - 1)
		}
		if selected.first > last || selected.first >= uint32(len(batch.Records)) || last >= uint32(len(batch.Records)) {
			return nil, fmt.Errorf("ordinal range %d..%d is outside record count %d", selected.first, last, len(batch.Records))
		}
		for ordinal := selected.first; ; ordinal++ {
			ordinals = appendUniqueOrdinal(ordinals, ordinal)
			if ordinal == last {
				break
			}
		}
	}
	return ordinals, nil
}

func appendUniqueOrdinal(ordinals []uint32, ordinal uint32) []uint32 {
	for _, existing := range ordinals {
		if existing == ordinal {
			return ordinals
		}
	}
	return append(ordinals, ordinal)
}

func decodeBatchFrame(frame []byte) (protocol.BatchFrameV1, error) {
	decoded, err := protocol.DecodeFrame(frame)
	if err != nil {
		return protocol.BatchFrameV1{}, err
	}
	message, err := protocol.DecodeMessage(decoded)
	if err != nil {
		return protocol.BatchFrameV1{}, err
	}
	batch, ok := message.(protocol.BatchFrameV1)
	if !ok {
		return protocol.BatchFrameV1{}, fmt.Errorf("WAL entry is not BatchFrameV1")
	}
	canonical, err := protocol.EncodeMessage(batch)
	if err != nil {
		return protocol.BatchFrameV1{}, err
	}
	if !bytes.Equal(canonical, frame) {
		return protocol.BatchFrameV1{}, fmt.Errorf("BatchFrameV1 bytes are not canonical")
	}
	return batch, nil
}

// expandManifestRangesForVerification translates a compact multi-entry range
// into the per-entry ranges understood by the existing M2 semantic verifier.
// The original manifest remains the replay identity; this copy exists only to
// prove that every expanded coordinate is present in the verified sealed WAL.
func expandManifestRangesForVerification(manifest RawDayManifest, objectPaths map[string]string, limits ReplayResourceLimits) (RawDayManifest, error) {
	if len(manifest.Objects) == 0 {
		return manifest, nil
	}
	type verifiedObject struct {
		segment wal.VerifiedSegment
		entries map[uint64]wal.Entry
	}
	objects := make(map[string]verifiedObject, len(manifest.ChainObjects))
	var loadedBytes uint64
	for index, descriptor := range manifest.ChainObjects {
		path, ok := objectPaths[descriptor.Key]
		if !ok || path == "" {
			return RawDayManifest{}, fmt.Errorf("%w: chain object %q is missing locally", ErrIntegrity, descriptor.Key)
		}
		fileBytes, err := checkReplayObjectPath(path, limits, loadedBytes)
		if err != nil {
			return RawDayManifest{}, fmt.Errorf("%w: chain object %q resource limit: %v", ErrIntegrity, descriptor.Key, err)
		}
		segment, err := wal.VerifySealedSegment(path)
		if err != nil {
			return RawDayManifest{}, fmt.Errorf("%w: chain object %q failed sealed verification: %v", ErrIntegrity, descriptor.Key, err)
		}
		if segment.ObjectSHA256 != descriptor.SHA256 || uint64(segment.FileBytes) != descriptor.Bytes ||
			segment.StartSequence != descriptor.StartIngestSequence || segment.LastSequence != descriptor.EndIngestSequence {
			return RawDayManifest{}, fmt.Errorf("%w: chain object %q metadata does not match verified bytes", ErrIntegrity, descriptor.Key)
		}
		loadedBytes += fileBytes
		if _, duplicate := objects[descriptor.Key]; duplicate {
			return RawDayManifest{}, fmt.Errorf("%w: chain object %d is duplicated", ErrIntegrity, index)
		}
		entries := make(map[uint64]wal.Entry, len(segment.Entries))
		for _, entry := range segment.Entries {
			entries[entry.Sequence] = entry
		}
		objects[descriptor.Key] = verifiedObject{segment: segment, entries: entries}
	}

	expanded := manifest
	expanded.ManifestSHA256 = [32]byte{}
	expanded.Objects = make([]RawObjectRange, 0, len(manifest.Objects))
	for _, object := range manifest.Objects {
		verified, ok := objects[object.Key]
		if !ok {
			return RawDayManifest{}, fmt.Errorf("%w: selected object %q is not in chain_objects", ErrIntegrity, object.Key)
		}
		for sequence := object.StartIngestSequence; ; sequence++ {
			entry, ok := verified.entries[sequence]
			if !ok {
				return RawDayManifest{}, fmt.Errorf("%w: selected sequence %d is absent from object %q", ErrIntegrity, sequence, object.Key)
			}
			batch, err := decodeBatchFrame(entry.Frame)
			if err != nil {
				return RawDayManifest{}, fmt.Errorf("%w: selected sequence %d is malformed: %v", ErrIntegrity, sequence, err)
			}
			if object.StartIngestSequence == object.EndIngestSequence {
				expanded.Objects = append(expanded.Objects, object)
				break
			}
			if len(batch.Records) == 0 {
				if utcDate(batch.RequestedFromMSC) != manifest.Date {
					return RawDayManifest{}, fmt.Errorf("%w: zero-record sentinel sequence %d is outside selected UTC day", ErrIntegrity, sequence)
				}
				if (sequence == object.StartIngestSequence && object.FirstRecordOrdinal != 0) ||
					(sequence == object.EndIngestSequence && object.LastRecordOrdinal != 0) {
					return RawDayManifest{}, fmt.Errorf("%w: zero-record sentinel sequence %d must select ordinal zero", ErrIntegrity, sequence)
				}
				expanded.Objects = append(expanded.Objects, RawObjectRange{
					Key: object.Key, SHA256: object.SHA256, Bytes: object.Bytes,
					StartIngestSequence: sequence, EndIngestSequence: sequence,
					FirstRecordOrdinal: 0, LastRecordOrdinal: 0,
				})
				if sequence == object.EndIngestSequence {
					break
				}
				if sequence == ^uint64(0) {
					return RawDayManifest{}, fmt.Errorf("%w: selected range sequence overflow", ErrIntegrity)
				}
				continue
			}
			first := uint32(0)
			last := uint32(len(batch.Records) - 1)
			if sequence == object.StartIngestSequence {
				first = object.FirstRecordOrdinal
			}
			if sequence == object.EndIngestSequence {
				last = object.LastRecordOrdinal
			}
			if first > last || first >= uint32(len(batch.Records)) || last >= uint32(len(batch.Records)) {
				return RawDayManifest{}, fmt.Errorf("%w: range %d..%d is outside sequence %d record count %d", ErrIntegrity, object.FirstRecordOrdinal, object.LastRecordOrdinal, sequence, len(batch.Records))
			}
			expanded.Objects = append(expanded.Objects, RawObjectRange{
				Key: object.Key, SHA256: object.SHA256, Bytes: object.Bytes,
				StartIngestSequence: sequence, EndIngestSequence: sequence,
				FirstRecordOrdinal: first, LastRecordOrdinal: last,
			})
			if sequence == object.EndIngestSequence {
				break
			}
			if sequence == ^uint64(0) {
				return RawDayManifest{}, fmt.Errorf("%w: selected range sequence overflow", ErrIntegrity)
			}
		}
	}
	root, err := RawSetRoot(expanded.Objects)
	if err != nil {
		return RawDayManifest{}, err
	}
	expanded.RawSetRoot = root
	return expanded, nil
}

// verifyReplaySnapshot preserves the M2 full-day proof while allowing the M3
// replay manifest to select a compact inclusive range across several WAL
// entries. M2's VerifyRawDaySnapshot intentionally rederives every record for
// the UTC day, whereas replay must additionally prove the exact coordinates
// named by manifest.objects.
func verifyReplaySnapshot(manifest, expanded RawDayManifest, objectPaths map[string]string, scope ScopeConfig, limits ReplayResourceLimits) error {
	normalizedScope, err := scope.normalized()
	if err != nil {
		return fmt.Errorf("%w: scope config is invalid: %v", ErrIntegrity, err)
	}
	configHash, err := normalizedScope.ConfigHash()
	if err != nil {
		return fmt.Errorf("%w: scope config hash cannot be computed: %v", ErrIntegrity, err)
	}
	if manifest.DatasetID != normalizedScope.DatasetID || manifest.CampaignID != normalizedScope.CampaignID ||
		manifest.DayDefinitionID != normalizedScope.DayDefinitionID || manifest.PublisherID != normalizedScope.PublisherID ||
		manifest.PublisherEpoch != normalizedScope.PublisherEpoch || manifest.SettlePolicy != normalizedScope.SettlePolicy ||
		manifest.ConfigHash != configHash {
		return fmt.Errorf("%w: manifest scope does not match verification scope", ErrIntegrity)
	}
	if err := ValidateRawDayManifest(manifest); err != nil {
		return fmt.Errorf("%w: manifest is invalid: %v", ErrIntegrity, err)
	}
	if err := ValidateRawDayManifest(expanded); err != nil {
		return fmt.Errorf("%w: expanded replay selection is invalid: %v", ErrIntegrity, err)
	}
	objects, err := loadReplayRawObjects(manifest, objectPaths, limits)
	if err != nil {
		return err
	}

	// Retain the M2 guarantee with a separate canonical value derived from
	// verified bytes. The caller's manifest is never overwritten before proof.
	fullSelection, err := deriveDaySelection(objects, manifest.Date, 0, 0, normalizedScope.ProtocolLimits.MaxRecords)
	if err != nil {
		return fmt.Errorf("%w: could not rederive full-day selection: %v", ErrIntegrity, err)
	}
	canonical, err := canonicalRawDayManifest(manifest, fullSelection, objects)
	if err != nil {
		return fmt.Errorf("%w: canonical full-day manifest: %v", ErrIntegrity, err)
	}
	if err := preflightReplayObjectFiles(manifest, objectPaths, limits); err != nil {
		return err
	}
	if err := VerifyRawDaySnapshot(canonical, objectPaths, scope); err != nil {
		return err
	}
	if !equalRawObjectRanges(expanded.Objects, canonical.Objects) || expanded.RawSetRoot != canonical.RawSetRoot {
		return fmt.Errorf("%w: expanded compact selection does not equal M2 canonical selection", ErrIntegrity)
	}
	if manifest.AcceptedRecordCount != canonical.AcceptedRecordCount || manifest.ErrorCount != canonical.ErrorCount ||
		manifest.ObservedThroughSourceMSC != canonical.ObservedThroughSourceMSC || manifest.ObservedThroughCaptureSeq != canonical.ObservedThroughCaptureSeq ||
		manifest.ChainSliceStartSequence != canonical.ChainSliceStartSequence || manifest.ChainSliceStartRoot != canonical.ChainSliceStartRoot ||
		manifest.ChainSliceEndSequence != canonical.ChainSliceEndSequence || manifest.ChainSliceEndRoot != canonical.ChainSliceEndRoot ||
		!equalRawChainObjects(manifest.ChainObjects, canonical.ChainObjects) {
		return fmt.Errorf("%w: replay manifest summary or chain slice does not equal M2 canonical selection", ErrIntegrity)
	}

	selected, err := deriveReplaySelection(objects, expanded.Objects, manifest.Date, normalizedScope.ProtocolLimits.MaxRecords)
	if err != nil {
		return fmt.Errorf("%w: replay selection does not match WAL bytes: %v", ErrIntegrity, err)
	}
	if selected.AcceptedRecordCount != canonical.AcceptedRecordCount || selected.ErrorCount != canonical.ErrorCount ||
		selected.ObservedThroughSourceMSC != canonical.ObservedThroughSourceMSC ||
		selected.ObservedThroughCaptureSeq != canonical.ObservedThroughCaptureSeq ||
		selected.ChainSliceStartSequence != canonical.ChainSliceStartSequence ||
		selected.ChainSliceStartRoot != canonical.ChainSliceStartRoot ||
		selected.ChainSliceEndSequence != canonical.ChainSliceEndSequence ||
		selected.ChainSliceEndRoot != canonical.ChainSliceEndRoot {
		return fmt.Errorf("%w: replay selection does not equal M2 canonical summary", ErrIntegrity)
	}
	return nil
}

func preflightReplayObjectFiles(manifest RawDayManifest, objectPaths map[string]string, limits ReplayResourceLimits) error {
	if err := validateReplayResourceLimits(manifest, limits); err != nil {
		return err
	}
	var loadedBytes uint64
	for index, descriptor := range manifest.ChainObjects {
		path, ok := objectPaths[descriptor.Key]
		if !ok || path == "" {
			return fmt.Errorf("%w: chain object %q is missing locally", ErrIntegrity, descriptor.Key)
		}
		fileBytes, err := checkReplayObjectPath(path, limits, loadedBytes)
		if err != nil {
			return fmt.Errorf("%w: chain object %d resource limit: %v", ErrIntegrity, index, err)
		}
		loadedBytes += fileBytes
	}
	return nil
}

func canonicalRawDayManifest(input RawDayManifest, selection daySelection, objects []RawObject) (RawDayManifest, error) {
	canonical := RawDayManifest{
		ManifestVersion: input.ManifestVersion, ManifestID: input.ManifestID,
		DatasetID: input.DatasetID, CampaignID: input.CampaignID, DayDefinitionID: input.DayDefinitionID,
		Date: input.Date, Revision: input.Revision, PublisherID: input.PublisherID, PublisherEpoch: input.PublisherEpoch,
		ConfigHash: input.ConfigHash, ProtocolVersion: input.ProtocolVersion, SourceSchemaID: input.SourceSchemaID,
		WALSchemaID: input.WALSchemaID, TerminalSyncStatus: input.TerminalSyncStatus, SettlePolicy: input.SettlePolicy,
		CompletenessStatus: input.CompletenessStatus, LogicalCloseTimeS: input.LogicalCloseTimeS,
		Objects: append([]RawObjectRange(nil), selection.Objects...), AcceptedRecordCount: selection.AcceptedRecordCount,
		ErrorCount: selection.ErrorCount, ObservedThroughSourceMSC: selection.ObservedThroughSourceMSC,
		ObservedThroughCaptureSeq: selection.ObservedThroughCaptureSeq,
		ChainSliceStartSequence:   selection.ChainSliceStartSequence, ChainSliceStartRoot: selection.ChainSliceStartRoot,
		ChainSliceEndSequence: selection.ChainSliceEndSequence, ChainSliceEndRoot: selection.ChainSliceEndRoot,
		ChainObjects: chainObjectsForSlice(objects, selection.ChainSliceStartSequence, selection.ChainSliceEndSequence),
	}
	if input.PreviousManifestSHA256 != nil {
		previous := *input.PreviousManifestSHA256
		canonical.PreviousManifestSHA256 = &previous
	}
	root, err := RawSetRoot(canonical.Objects)
	if err != nil {
		return RawDayManifest{}, err
	}
	canonical.RawSetRoot = root
	return canonical, nil
}

func loadReplayRawObjects(manifest RawDayManifest, objectPaths map[string]string, limits ReplayResourceLimits) ([]RawObject, error) {
	objects := make([]RawObject, len(manifest.ChainObjects))
	seen := make(map[string]struct{}, len(manifest.ChainObjects))
	var loadedBytes uint64
	for index, descriptor := range manifest.ChainObjects {
		if _, duplicate := seen[descriptor.Key]; duplicate {
			return nil, fmt.Errorf("%w: chain object %d is duplicated", ErrIntegrity, index)
		}
		seen[descriptor.Key] = struct{}{}
		path, ok := objectPaths[descriptor.Key]
		if !ok || path == "" {
			return nil, fmt.Errorf("%w: chain object %q is missing locally", ErrIntegrity, descriptor.Key)
		}
		fileBytes, err := checkReplayObjectPath(path, limits, loadedBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: chain object %q resource limit: %v", ErrIntegrity, descriptor.Key, err)
		}
		segment, err := wal.VerifySealedSegment(path)
		if err != nil {
			return nil, fmt.Errorf("%w: chain object %q failed sealed verification: %v", ErrIntegrity, descriptor.Key, err)
		}
		if segment.ObjectSHA256 != descriptor.SHA256 || uint64(segment.FileBytes) != descriptor.Bytes ||
			segment.StartSequence != descriptor.StartIngestSequence || segment.LastSequence != descriptor.EndIngestSequence {
			return nil, fmt.Errorf("%w: chain object %q metadata does not match verified bytes", ErrIntegrity, descriptor.Key)
		}
		loadedBytes += fileBytes
		objects[index] = RawObject{Key: descriptor.Key, Path: path, SHA256: segment.ObjectSHA256, Bytes: segment.FileBytes, Segment: segment}
	}
	return objects, nil
}

func deriveReplaySelection(objects []RawObject, ranges []RawObjectRange, date string, maxRecords uint32) (daySelection, error) {
	byKey := make(map[string]RawObject, len(objects))
	for _, object := range objects {
		byKey[object.Key] = object
	}
	selection := daySelection{}
	seenEntries := make(map[string]struct{})
	for index, objectRange := range ranges {
		object, ok := byKey[objectRange.Key]
		if !ok || object.SHA256 != objectRange.SHA256 || uint64(object.Bytes) != objectRange.Bytes {
			return daySelection{}, fmt.Errorf("range %d does not match a verified chain object", index)
		}
		if objectRange.StartIngestSequence != objectRange.EndIngestSequence {
			return daySelection{}, fmt.Errorf("range %d was not expanded to one WAL entry", index)
		}
		var entry *wal.Entry
		for entryIndex := range object.Segment.Entries {
			candidate := &object.Segment.Entries[entryIndex]
			if candidate.Sequence == objectRange.StartIngestSequence {
				entry = candidate
				break
			}
		}
		if entry == nil {
			return daySelection{}, fmt.Errorf("range %d selects a missing WAL sequence", index)
		}
		batch, err := decodeBatchFrame(entry.Frame)
		if err != nil {
			return daySelection{}, fmt.Errorf("range %d selects malformed BatchFrameV1: %v", index, err)
		}
		if len(batch.Records) > int(maxRecords) {
			return daySelection{}, fmt.Errorf("range %d exceeds configured record limit", index)
		}
		selected, err := selectRecordOrdinals(entry.Frame, []recordOrdinalRange{{
			first: objectRange.FirstRecordOrdinal, last: objectRange.LastRecordOrdinal,
			throughEnd: false,
		}})
		if err != nil {
			return daySelection{}, fmt.Errorf("range %d ordinal selection: %v", index, err)
		}
		if len(selected) == 0 {
			continue
		}
		entryID := fmt.Sprintf("%s/%d", objectRange.Key, entry.Sequence)
		if _, alreadySeen := seenEntries[entryID]; !alreadySeen {
			seenEntries[entryID] = struct{}{}
			if selection.ChainSliceStartSequence == 0 {
				selection.ChainSliceStartSequence = entry.Sequence
				selection.ChainSliceStartRoot = entry.PreviousEntryHash
			}
			selection.ChainSliceEndSequence = entry.Sequence
			selection.ChainSliceEndRoot = entry.EntryHash
			if batch.CopyTicksError != 0 {
				selection.ErrorCount++
			}
		}
		if len(batch.Records) == 0 {
			if utcDate(batch.RequestedFromMSC) != date {
				return daySelection{}, fmt.Errorf("zero-record sentinel is outside selected UTC day")
			}
			continue
		}
		for _, ordinal := range selected {
			record := batch.Records[ordinal]
			if utcDate(record.TimeMSC) != date {
				return daySelection{}, fmt.Errorf("record coordinate %d/%d is outside selected UTC day", entry.Sequence, ordinal)
			}
			selection.AcceptedRecordCount++
			if !selection.hasSourceWatermark || record.TimeMSC > selection.ObservedThroughSourceMSC {
				selection.ObservedThroughSourceMSC = record.TimeMSC
				selection.hasSourceWatermark = true
			}
			if record.CaptureSequence > selection.ObservedThroughCaptureSeq {
				selection.ObservedThroughCaptureSeq = record.CaptureSequence
			}
		}
	}
	return selection, nil
}
