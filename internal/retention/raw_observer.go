package retention

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

const rawRetentionVerificationDomain = "tick-data-platform/retention-verification/v1\x00"

// RawRetentionObserver proves one sealed WAL segment against a bounded,
// read-only immutable scope. It intentionally accepts only the narrowed R2
// capability and a trusted Layout; neither a remote key nor a local path is a
// supplied authority.
type RawRetentionObserver struct {
	remote  r2.ReplayRemoteReadBackend
	layout  r2.Layout
	date    string
	clock   func() uint64
	graceMS uint64
}

// NewRawRetentionObserver constructs the raw-WAL observer for one immutable
// scope date. clock returns a durable wall-clock observation in Unix ms.
func NewRawRetentionObserver(remote r2.ReplayRemoteReadBackend, layout r2.Layout, date string, clock func() uint64, graceMS uint64) (*RawRetentionObserver, error) {
	if remote == nil || clock == nil || date == "" || graceMS == 0 {
		return nil, fmt.Errorf("raw retention observer dependencies are incomplete")
	}
	if _, err := layout.ManifestPrefix(date); err != nil {
		return nil, fmt.Errorf("raw retention observer date: %w", err)
	}
	return &RawRetentionObserver{remote: remote, layout: layout, date: date, clock: clock, graceMS: graceMS}, nil
}

// Observe performs one aggregate bounded read. Any remote uncertainty is
// returned as a non-Exact fact so callers can produce a blocked plan without
// turning an outage into a planner error.
func (o *RawRetentionObserver) Observe(ctx context.Context, artifact LocalArtifact, limits ProofLimits) (RemoteFact, error) {
	budget, err := NewProofObservationBudget(limits)
	if err != nil {
		return RemoteFact{}, err
	}
	return o.ObserveWithBudget(ctx, artifact, limits, budget)
}

func (o *RawRetentionObserver) ObserveWithBudget(ctx context.Context, artifact LocalArtifact, limits ProofLimits, budget *ProofObservationBudget) (RemoteFact, error) {
	if err := limits.Validate(); err != nil {
		return RemoteFact{}, err
	}
	if budget == nil || budget.limits != limits {
		return RemoteFact{}, fmt.Errorf("raw retention observation budget does not match proof limits")
	}
	if (artifact.Kind != ArtifactWALSegment && artifact.Kind != ArtifactRawOutbox) || artifact.WALRange == nil {
		return RemoteFact{Class: RemoteObservationUnavailable}, nil
	}
	if _, err := artifact.StableID(); err != nil {
		return RemoteFact{}, fmt.Errorf("raw retention artifact identity: %w", err)
	}
	if artifact.Bytes == 0 || artifact.Bytes > limits.MaxProofBytes {
		return RemoteFact{Class: RemoteObservationOversized}, nil
	}
	durableWallTime := o.clock()
	if durableWallTime == 0 {
		return RemoteFact{Class: RemoteObservationUnavailable}, nil
	}
	remoteKey, err := o.layout.RemoteKey(archive.RawWALObjectKey(artifact.ContentSHA256))
	if err != nil {
		return RemoteFact{}, fmt.Errorf("derive raw retention object key: %w", err)
	}
	claim, err := r2.NewPublisherClaim(o.layout.Scope)
	if err != nil {
		return RemoteFact{}, fmt.Errorf("derive trusted publisher claim: %w", err)
	}
	claimKey, err := o.layout.ClaimKey(claim.PublisherEpoch)
	if err != nil {
		return RemoteFact{}, fmt.Errorf("derive publisher claim key: %w", err)
	}
	claimBytes, err := claim.CanonicalJSON()
	if err != nil {
		return RemoteFact{}, fmt.Errorf("canonicalize trusted publisher claim: %w", err)
	}
	claimDigest, err := claim.Digest()
	if err != nil {
		return RemoteFact{}, fmt.Errorf("digest trusted publisher claim: %w", err)
	}
	if !budget.chargeObjects(1) {
		return RemoteFact{Class: RemoteObservationOversized}, nil
	}
	claimBody, class := o.readRemote(ctx, claimKey, budget.limits.MaxProofBytes-budget.bytes, uint64Pointer(uint64(len(claimBytes))), budget)
	if class != RemoteObservationExact {
		return RemoteFact{Class: class}, nil
	}
	if !bytes.Equal(claimBody, claimBytes) {
		return RemoteFact{Class: RemoteObservationDifferent}, nil
	}
	if !budget.chargeObjects(1) {
		return RemoteFact{Class: RemoteObservationOversized}, nil
	}
	body, class := o.readRemote(ctx, remoteKey, budget.limits.MaxProofBytes-budget.bytes, &artifact.Bytes, budget)
	if class != RemoteObservationExact {
		return RemoteFact{Class: class}, nil
	}
	segment, err := wal.VerifySealedBytes(remoteKey, body)
	if err == nil && (segment.ObjectSHA256 != artifact.ContentSHA256 || uint64(segment.FileBytes) != artifact.Bytes) {
		return RemoteFact{Class: RemoteObservationDifferent}, nil
	}
	if err != nil || segment.StartSequence != artifact.WALRange.StartSequence || segment.LastSequence != artifact.WALRange.EndSequence || segment.ChainStart != artifact.WALRange.StartChainRoot || segment.ChainRoot != artifact.WALRange.EndChainRoot {
		return RemoteFact{Class: RemoteObservationAmbiguous}, nil
	}
	if !segmentOnlyContainsDate(segment, o.date) {
		return RemoteFact{Class: RemoteObservationAmbiguous}, nil
	}

	records, class := o.readManifestGraph(ctx, limits, budget)
	if class != RemoteObservationExact {
		return RemoteFact{Class: class}, nil
	}
	covering, coverage, covered := findCoveringManifest(records, artifact, segment, o.layout.Scope)
	if !covered {
		return RemoteFact{Class: RemoteObservationAmbiguous}, nil
	}
	snapshotReport, class := o.readRemoteSnapshot(ctx, covering.Manifest, artifact, body, segment, budget)
	if class != RemoteObservationExact {
		return RemoteFact{Class: class}, nil
	}
	observedWallTime, err := retentionObservationWallTime(covering.Manifest.LogicalCloseTimeS, durableWallTime)
	if err != nil || o.graceMS > ^uint64(0)-observedWallTime {
		return RemoteFact{Class: RemoteObservationAmbiguous}, nil
	}
	verificationDigest, err := rawVerificationReportDigest(o.layout, o.date, remoteKey, artifact, records, covering, coverage, snapshotReport, claimKey, claimDigest)
	if err != nil {
		return RemoteFact{}, err
	}
	scopeConfigHash, err := o.layout.Scope.ConfigHash()
	if err != nil {
		return RemoteFact{}, fmt.Errorf("derive retention scope hash: %w", err)
	}
	proof := &RetentionProof{
		ProofVersion:             RetentionProofVersion,
		ArtifactKind:             artifact.Kind,
		TrustedRelativePath:      artifact.TrustedPath,
		Bytes:                    artifact.Bytes,
		ContentSHA256:            artifact.ContentSHA256,
		ScopeConfigHash:          scopeConfigHash,
		WALRange:                 cloneWALRange(artifact.WALRange),
		Remote:                   RemoteObjectObservation{Class: RemoteObservationExact, FullKey: remoteKey, SHA256: artifact.ContentSHA256, Bytes: artifact.Bytes},
		CoveringManifestKey:      covering.Key,
		CoveringManifestDigest:   covering.Manifest.ManifestSHA256,
		VerificationReportDigest: verificationDigest,
		ObservedWallTimeUnixMS:   observedWallTime,
		GraceNotBeforeUnixMS:     observedWallTime + o.graceMS,
		Limits:                   limits,
	}
	if err := proof.Validate(); err != nil {
		return RemoteFact{}, fmt.Errorf("build raw retention proof: %w", err)
	}
	return RemoteFact{Class: RemoteObservationExact, Proof: proof, CoverageVerified: true}, nil
}

func retentionObservationWallTime(logicalCloseTimeS int64, durableWallTime uint64) (uint64, error) {
	if logicalCloseTimeS <= 0 || uint64(logicalCloseTimeS) > ^uint64(0)/1000 {
		return 0, fmt.Errorf("manifest logical close time is invalid")
	}
	logicalClose := uint64(logicalCloseTimeS) * 1000
	if logicalClose < durableWallTime {
		return durableWallTime, nil
	}
	return logicalClose, nil
}

func cloneWALRange(value *WALRange) *WALRange {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (o *RawRetentionObserver) readRemote(ctx context.Context, key string, capBytes uint64, expectedBytes *uint64, budget *ProofObservationBudget) ([]byte, string) {
	if capBytes == 0 || capBytes > uint64(^uint64(0)>>1)-1 {
		return nil, RemoteObservationOversized
	}
	body, advertised, err := o.remote.OpenLimited(ctx, key, capBytes)
	if err != nil {
		return nil, classifyRetentionRemoteError(err)
	}
	if body == nil {
		return nil, RemoteObservationUnavailable
	}
	data, readErr := io.ReadAll(io.LimitReader(body, int64(capBytes)+1))
	closeErr := body.Close()
	if budget == nil || !budget.consumeBytes(uint64(len(data))) {
		return nil, RemoteObservationOversized
	}
	if readErr != nil || closeErr != nil {
		return nil, RemoteObservationUnavailable
	}
	if advertised < 0 {
		return nil, RemoteObservationUnavailable
	}
	if uint64(advertised) > capBytes || uint64(len(data)) > capBytes {
		return nil, RemoteObservationOversized
	}
	if uint64(len(data)) != uint64(advertised) {
		return nil, RemoteObservationAmbiguous
	}
	if expectedBytes != nil && uint64(len(data)) != *expectedBytes {
		return nil, RemoteObservationDifferent
	}
	return data, RemoteObservationExact
}

func classifyRetentionRemoteError(err error) string {
	switch {
	case errors.Is(err, r2.ErrObjectNotFound):
		return RemoteObservationAbsent
	case errors.Is(err, r2.ErrMetadataTooLarge), errors.Is(err, r2.ErrResourceLimit):
		return RemoteObservationOversized
	default:
		return RemoteObservationUnavailable
	}
}

func (o *RawRetentionObserver) readManifestGraph(ctx context.Context, limits ProofLimits, budget *ProofObservationBudget) ([]r2.ManifestRecord, string) {
	prefix, err := o.layout.ManifestPrefix(o.date)
	if err != nil {
		return nil, RemoteObservationAmbiguous
	}
	if limits.MaxProofObjects <= budget.objects || limits.MaxManifestNodes <= budget.manifestNodes {
		return nil, RemoteObservationOversized
	}
	maxObjects := minUint64(limits.MaxManifestNodes-budget.manifestNodes, limits.MaxProofObjects-budget.objects)
	listed, err := o.remote.ListLimited(ctx, prefix, maxObjects)
	if err != nil {
		return nil, classifyRetentionRemoteError(err)
	}
	if !listed.Complete {
		return nil, RemoteObservationAmbiguous
	}
	if uint64(len(listed.Objects)) == 0 {
		return nil, RemoteObservationAbsent
	}
	if uint64(len(listed.Objects)) > limits.MaxManifestNodes-budget.manifestNodes || uint64(len(listed.Objects)) > limits.MaxProofObjects-budget.objects {
		return nil, RemoteObservationOversized
	}
	if !budget.chargeManifestNodes(uint64(len(listed.Objects))) {
		return nil, RemoteObservationOversized
	}
	sort.Slice(listed.Objects, func(i, j int) bool { return listed.Objects[i].Key < listed.Objects[j].Key })
	records := make([]r2.ManifestRecord, 0, len(listed.Objects))
	seenKeys := make(map[string]struct{}, len(listed.Objects))
	seenDigests := make(map[[32]byte]struct{}, len(listed.Objects))
	for _, object := range listed.Objects {
		if object.Size < 0 || !strings.HasPrefix(object.Key, prefix) || object.Key == prefix {
			return nil, RemoteObservationAmbiguous
		}
		if _, duplicate := seenKeys[object.Key]; duplicate {
			return nil, RemoteObservationAmbiguous
		}
		seenKeys[object.Key] = struct{}{}
		if uint64(object.Size) > limits.MaxProofBytes-budget.bytes {
			return nil, RemoteObservationOversized
		}
		if !budget.chargeObjects(1) {
			return nil, RemoteObservationOversized
		}
		body, class := o.readRemote(ctx, object.Key, limits.MaxProofBytes-budget.bytes, uint64Pointer(uint64(object.Size)), budget)
		if class != RemoteObservationExact {
			return nil, class
		}
		manifest, err := archive.VerifyRawDayManifest(body)
		if err != nil {
			return nil, RemoteObservationAmbiguous
		}
		canonical, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(canonical, body) {
			return nil, RemoteObservationAmbiguous
		}
		if err := archive.ValidateRawDayManifest(manifest); err != nil || !rawManifestMatchesScope(manifest, o.layout.Scope, o.date) {
			return nil, RemoteObservationAmbiguous
		}
		wantKey, err := o.layout.ManifestKey(manifest)
		if err != nil || wantKey != object.Key {
			return nil, RemoteObservationAmbiguous
		}
		if _, duplicate := seenDigests[manifest.ManifestSHA256]; duplicate {
			return nil, RemoteObservationAmbiguous
		}
		seenDigests[manifest.ManifestSHA256] = struct{}{}
		records = append(records, r2.ManifestRecord{Key: object.Key, Bytes: body, Manifest: manifest})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Manifest.Revision < records[j].Manifest.Revision })
	for index, record := range records {
		if record.Manifest.Revision != uint64(index+1) {
			return nil, RemoteObservationAmbiguous
		}
		if index == 0 {
			if record.Manifest.PreviousManifestSHA256 != nil {
				return nil, RemoteObservationAmbiguous
			}
			continue
		}
		previous := records[index-1].Manifest
		if record.Manifest.PreviousManifestSHA256 == nil || *record.Manifest.PreviousManifestSHA256 != previous.ManifestSHA256 {
			return nil, RemoteObservationAmbiguous
		}
		if _, err := r2.ValidateRevisionGraph(record.Manifest, record.Bytes, records[:index]); err != nil {
			return nil, RemoteObservationAmbiguous
		}
	}
	return records, RemoteObservationExact
}

func (o *RawRetentionObserver) readRemoteSnapshot(ctx context.Context, manifest archive.RawDayManifest, artifact LocalArtifact, candidateBody []byte, candidateSegment wal.VerifiedSegment, budget *ProofObservationBudget) (archive.RawDaySnapshotReport, string) {
	if len(manifest.ChainObjects) == 0 {
		return archive.RawDaySnapshotReport{}, RemoteObservationAmbiguous
	}
	candidateKey := archive.RawWALObjectKey(artifact.ContentSHA256)
	objects := make([]archive.RawObject, 0, len(manifest.ChainObjects))
	for _, descriptor := range manifest.ChainObjects {
		if descriptor.Key != archive.RawWALObjectKey(descriptor.SHA256) || descriptor.Bytes == 0 || descriptor.EndIngestSequence < descriptor.StartIngestSequence {
			return archive.RawDaySnapshotReport{}, RemoteObservationAmbiguous
		}
		if descriptor.Key == candidateKey {
			if descriptor.SHA256 != artifact.ContentSHA256 || descriptor.Bytes != artifact.Bytes || uint64(len(candidateBody)) != descriptor.Bytes || candidateSegment.ObjectSHA256 != descriptor.SHA256 || uint64(candidateSegment.FileBytes) != descriptor.Bytes || candidateSegment.StartSequence != descriptor.StartIngestSequence || candidateSegment.LastSequence != descriptor.EndIngestSequence {
				return archive.RawDaySnapshotReport{}, RemoteObservationDifferent
			}
			objects = append(objects, archive.RawObject{Key: descriptor.Key, SHA256: descriptor.SHA256, Bytes: int64(descriptor.Bytes), Segment: candidateSegment})
			continue
		}
		if !budget.chargeObjects(1) {
			return archive.RawDaySnapshotReport{}, RemoteObservationOversized
		}
		remoteKey, err := o.layout.RemoteKey(descriptor.Key)
		if err != nil {
			return archive.RawDaySnapshotReport{}, RemoteObservationAmbiguous
		}
		body, class := o.readRemote(ctx, remoteKey, budget.limits.MaxProofBytes-budget.bytes, uint64Pointer(descriptor.Bytes), budget)
		if class != RemoteObservationExact {
			return archive.RawDaySnapshotReport{}, class
		}
		segment, err := wal.VerifySealedBytes(remoteKey, body)
		if err != nil || segment.ObjectSHA256 != descriptor.SHA256 || uint64(segment.FileBytes) != descriptor.Bytes || segment.StartSequence != descriptor.StartIngestSequence || segment.LastSequence != descriptor.EndIngestSequence {
			return archive.RawDaySnapshotReport{}, RemoteObservationAmbiguous
		}
		objects = append(objects, archive.RawObject{Key: descriptor.Key, SHA256: descriptor.SHA256, Bytes: int64(descriptor.Bytes), Segment: segment})
	}
	report, err := archive.VerifyRawDaySnapshotSegments(manifest, objects, o.layout.Scope)
	if err != nil {
		return archive.RawDaySnapshotReport{}, RemoteObservationAmbiguous
	}
	return report, RemoteObservationExact
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func rawManifestMatchesScope(manifest archive.RawDayManifest, scope archive.ScopeConfig, date string) bool {
	configHash, err := scope.ConfigHash()
	return err == nil && manifest.ManifestVersion == archive.RawDayManifestVersion && manifest.DatasetID == scope.DatasetID && manifest.DayDefinitionID == scope.DayDefinitionID && manifest.Date == date && manifest.PublisherID == scope.PublisherID && manifest.PublisherEpoch == scope.PublisherEpoch && manifest.ConfigHash == configHash && manifest.SettlePolicy == scope.SettlePolicy
}

func findCoveringManifest(records []r2.ManifestRecord, artifact LocalArtifact, segment wal.VerifiedSegment, scope archive.ScopeConfig) (r2.ManifestRecord, archive.RawDaySegmentCoverage, bool) {
	if artifact.WALRange == nil {
		return r2.ManifestRecord{}, archive.RawDaySegmentCoverage{}, false
	}
	for index := len(records) - 1; index >= 0; index-- {
		manifest := records[index].Manifest
		chainFound := false
		for _, object := range manifest.ChainObjects {
			if object.Key == archive.RawWALObjectKey(artifact.ContentSHA256) && object.SHA256 == artifact.ContentSHA256 && object.Bytes == artifact.Bytes && object.StartIngestSequence == artifact.WALRange.StartSequence && object.EndIngestSequence == artifact.WALRange.EndSequence {
				chainFound = true
				break
			}
		}
		if !chainFound {
			continue
		}
		coverage, err := archive.VerifyRawDaySegmentCoverage(manifest, segment, archive.RawWALObjectKey(artifact.ContentSHA256), artifact.ContentSHA256, artifact.Bytes, scope)
		if err != nil {
			continue
		}
		if len(coverage.SelectedRanges) == 0 {
			continue
		}
		if manifest.ChainSliceStartSequence > artifact.WALRange.StartSequence || manifest.ChainSliceEndSequence < artifact.WALRange.EndSequence {
			continue
		}
		return records[index], coverage, true
	}
	return r2.ManifestRecord{}, archive.RawDaySegmentCoverage{}, false
}

func segmentOnlyContainsDate(segment wal.VerifiedSegment, date string) bool {
	for _, entry := range segment.Entries {
		frame, err := protocol.DecodeFrame(entry.Frame)
		if err != nil {
			return false
		}
		message, err := protocol.DecodeMessage(frame)
		if err != nil {
			return false
		}
		batch, ok := message.(protocol.BatchFrameV1)
		if !ok {
			return false
		}
		if len(batch.Records) == 0 {
			if retentionUTCDate(batch.RequestedFromMSC) != date {
				return false
			}
			continue
		}
		for _, record := range batch.Records {
			if retentionUTCDate(record.TimeMSC) != date {
				return false
			}
		}
	}
	return true
}

func retentionUTCDate(milliseconds int64) string {
	return time.UnixMilli(milliseconds).UTC().Format("2006-01-02")
}

func rawVerificationReportDigest(layout r2.Layout, date, remoteKey string, artifact LocalArtifact, records []r2.ManifestRecord, covering r2.ManifestRecord, coverage archive.RawDaySegmentCoverage, snapshot archive.RawDaySnapshotReport, claimKey string, claimDigest [32]byte) ([32]byte, error) {
	manifests := make([]any, len(records))
	for index, record := range records {
		manifests[index] = map[string]any{"digest": protocol.EncodeHashHex(record.Manifest.ManifestSHA256), "key": record.Key, "revision": record.Manifest.Revision}
	}
	ranges := make([]any, len(coverage.SelectedRanges))
	for index, object := range coverage.SelectedRanges {
		ranges[index] = map[string]any{
			"end_ingest_sequence":   object.EndIngestSequence,
			"first_record_ordinal":  object.FirstRecordOrdinal,
			"last_record_ordinal":   object.LastRecordOrdinal,
			"start_ingest_sequence": object.StartIngestSequence,
		}
	}
	value := map[string]any{
		"artifact_key":       remoteKey,
		"artifact_sha256":    protocol.EncodeHashHex(artifact.ContentSHA256),
		"scope_prefix":       layout.ImmutableScopePrefix(),
		"claim_digest":       protocol.EncodeHashHex(claimDigest),
		"claim_key":          claimKey,
		"covering_manifest":  covering.Key,
		"date":               date,
		"manifest_revisions": manifests,
		"report_version":     "raw-retention-verification-v1",
		"semantic_report": map[string]any{
			"accepted_record_count":      coverage.AcceptedRecordCount,
			"chain_slice_end_root":       protocol.EncodeHashHex(coverage.ChainSliceEndRoot),
			"chain_slice_end_sequence":   coverage.ChainSliceEndSequence,
			"chain_slice_start_root":     protocol.EncodeHashHex(coverage.ChainSliceStartRoot),
			"chain_slice_start_sequence": coverage.ChainSliceStartSequence,
			"date":                       coverage.Date,
			"error_count":                coverage.ErrorCount,
			"manifest_sha256":            protocol.EncodeHashHex(coverage.ManifestSHA256),
			"object_bytes":               coverage.ObjectBytes,
			"object_key":                 coverage.ObjectKey,
			"object_sha256":              protocol.EncodeHashHex(coverage.ObjectSHA256),
			"selected_ranges":            ranges,
		},
		"snapshot_report": map[string]any{
			"accepted_record_count":       snapshot.AcceptedRecordCount,
			"chain_slice_end_root":        protocol.EncodeHashHex(snapshot.ChainSliceEndRoot),
			"chain_slice_end_sequence":    snapshot.ChainSliceEndSequence,
			"chain_slice_start_root":      protocol.EncodeHashHex(snapshot.ChainSliceStartRoot),
			"chain_slice_start_sequence":  snapshot.ChainSliceStartSequence,
			"error_count":                 snapshot.ErrorCount,
			"manifest_sha256":             protocol.EncodeHashHex(snapshot.ManifestSHA256),
			"object_count":                snapshot.ObjectCount,
			"observed_through_capture":    snapshot.ObservedThroughCapture,
			"observed_through_source_msc": snapshot.ObservedThroughSourceMSC,
		},
	}
	canonical, err := protocol.CanonicalJSON(value)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(rawRetentionVerificationDomain), canonical...)), nil
}
