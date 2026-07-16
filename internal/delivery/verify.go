package delivery

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

func (r *archiveReaderV1) VerifyDay(ctx context.Context, selector SnapshotSelector) (DayVerificationReport, error) {
	snapshot, err := r.ResolveSnapshot(ctx, selector)
	if err != nil {
		return DayVerificationReport{}, err
	}
	plan, err := r.BuildFetchPlan(ctx, snapshot)
	if err != nil {
		return DayVerificationReport{}, err
	}
	result, err := r.Fetch(ctx, plan, "")
	if err != nil {
		return DayVerificationReport{}, err
	}
	if err := archive.VerifyRawDaySnapshot(snapshot.Manifest, result.ObjectPaths, snapshot.Scope); err != nil {
		return DayVerificationReport{}, err
	}
	manifest := snapshot.Manifest
	return DayVerificationReport{
		GenesisVerified:     false,
		VerificationScope:   VerificationScopeAnchoredDay,
		DatasetID:           manifest.DatasetID,
		CampaignID:          manifest.CampaignID,
		Date:                manifest.Date,
		Revision:            manifest.Revision,
		ManifestKey:         snapshot.ManifestKey,
		ManifestSHA256:      snapshot.ManifestSHA256,
		PredecessorAnchor:   manifest.ChainSliceStartRoot,
		ChainSliceStart:     manifest.ChainSliceStartSequence,
		ChainSliceStartRoot: manifest.ChainSliceStartRoot,
		ChainSliceEnd:       manifest.ChainSliceEndSequence,
		ChainSliceEndRoot:   manifest.ChainSliceEndRoot,
		AcceptedRecordCount: manifest.AcceptedRecordCount,
		ErrorCount:          manifest.ErrorCount,
		Entries:             result.Entries,
	}, nil
}

func (r *archiveReaderV1) VerifyCampaign(ctx context.Context, datasetID, campaignID, throughRoot string) (CampaignVerificationReport, error) {
	root, err := decodeRoot(throughRoot)
	if err != nil {
		return CampaignVerificationReport{}, err
	}
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return CampaignVerificationReport{}, err
	}
	var selected *discoveredScope
	for i := range scopes {
		if scopes[i].Scope.DatasetID != datasetID || scopes[i].Scope.CampaignID != campaignID {
			continue
		}
		if selected != nil {
			return CampaignVerificationReport{}, fmt.Errorf("%w: campaign identity is ambiguous", archive.ErrIntegrity)
		}
		selected = &scopes[i]
	}
	if selected == nil {
		return CampaignVerificationReport{}, fmt.Errorf("%w: campaign identity was not discovered", archive.ErrIntegrity)
	}
	snapshots, err := r.loadSnapshots(ctx, *selected)
	if err != nil {
		return CampaignVerificationReport{}, err
	}
	if len(snapshots) == 0 {
		return CampaignVerificationReport{}, fmt.Errorf("%w: campaign has no raw snapshots", archive.ErrIntegrity)
	}
	layout, err := r2.NewLayout(r.config.ImmutableRoot, selected.Scope)
	if err != nil {
		return CampaignVerificationReport{}, fmt.Errorf("%w: campaign layout is invalid", archive.ErrIntegrity)
	}
	type campaignObject struct {
		FetchObject
		startSequence uint64
		endSequence   uint64
	}
	objects := make(map[string]campaignObject)
	manifests := make([]archive.RawDayManifest, 0, len(snapshots))
	for _, snapshot := range snapshots {
		manifest, err := r.metadata(ctx, snapshot.ManifestKey)
		if err != nil {
			return CampaignVerificationReport{}, err
		}
		decoded, err := archive.VerifyRawDayManifest(manifest)
		if err != nil || decoded.ManifestSHA256 != snapshot.ManifestSHA256 {
			return CampaignVerificationReport{}, fmt.Errorf("%w: campaign manifest changed", archive.ErrIntegrity)
		}
		if err := publishVerifiedBytes(r.cacheManifestPath(decoded), manifest); err != nil {
			return CampaignVerificationReport{}, err
		}
		manifests = append(manifests, decoded)
		for _, object := range decoded.ChainObjects {
			if object.Key != archive.RawWALObjectKey(object.SHA256) || object.Bytes == 0 || object.EndIngestSequence < object.StartIngestSequence {
				return CampaignVerificationReport{}, fmt.Errorf("%w: campaign chain object identity is invalid", archive.ErrIntegrity)
			}
			remoteKey, err := layout.RemoteKey(object.Key)
			if err != nil {
				return CampaignVerificationReport{}, fmt.Errorf("%w: campaign chain object key is invalid", archive.ErrIntegrity)
			}
			candidate := campaignObject{FetchObject: FetchObject{
				Key: object.Key, RemoteKey: remoteKey, SHA256: object.SHA256, Bytes: object.Bytes,
				CachePath: r.cacheObjectPath(object.SHA256),
			}, startSequence: object.StartIngestSequence, endSequence: object.EndIngestSequence}
			if prior, ok := objects[object.Key]; ok && (prior.RemoteKey != candidate.RemoteKey || prior.SHA256 != candidate.SHA256 || prior.Bytes != candidate.Bytes || prior.startSequence != candidate.startSequence || prior.endSequence != candidate.endSequence) {
				return CampaignVerificationReport{}, fmt.Errorf("%w: campaign chain object descriptor conflicts", archive.ErrIntegrity)
			}
			objects[object.Key] = candidate
		}
	}
	ordered := make([]campaignObject, 0, len(objects))
	for _, object := range objects {
		ordered = append(ordered, object)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Key < ordered[j].Key
	})
	segments := make([]wal.VerifiedSegment, 0, len(ordered))
	objectPaths := make(map[string]string, len(ordered))
	for _, object := range ordered {
		if err := r.fetchRawObject(ctx, object.FetchObject); err != nil {
			return CampaignVerificationReport{}, err
		}
		objectPaths[object.Key] = object.CachePath
		segment, err := wal.VerifySealedSegment(object.CachePath)
		if err != nil || segment.ObjectSHA256 != object.SHA256 || uint64(segment.FileBytes) != object.Bytes || segment.StartSequence != object.startSequence || segment.LastSequence != object.endSequence {
			return CampaignVerificationReport{}, fmt.Errorf("%w: campaign segment does not match descriptor", archive.ErrIntegrity)
		}
		segments = append(segments, segment)
	}
	for _, manifest := range manifests {
		if err := archive.VerifyRawDaySnapshot(manifest, objectPaths, selected.Scope); err != nil {
			return CampaignVerificationReport{}, fmt.Errorf("%w: campaign raw-day manifest semantic verification failed: %v", archive.ErrIntegrity, err)
		}
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].StartSequence < segments[j].StartSequence })
	var expectedSequence uint64 = 1
	var expectedPrevious [32]byte
	verifiedThrough := uint64(0)
	segmentCount := 0
	entryCount := 0
	found := false
	for _, segment := range segments {
		if found {
			break
		}
		if segment.StartSequence != expectedSequence || segment.ChainStart != expectedPrevious {
			return CampaignVerificationReport{}, fmt.Errorf("%w: campaign chain has a gap or conflicting segment", archive.ErrIntegrity)
		}
		segmentCount++
		for _, entry := range segment.Entries {
			if entry.Sequence != expectedSequence || entry.PreviousEntryHash != expectedPrevious {
				return CampaignVerificationReport{}, fmt.Errorf("%w: campaign entry chain has a gap or conflict", archive.ErrIntegrity)
			}
			expectedPrevious = entry.EntryHash
			expectedSequence++
			entryCount++
			if entry.EntryHash == root {
				verifiedThrough = entry.Sequence
				found = true
				break
			}
		}
	}
	if !found {
		return CampaignVerificationReport{}, fmt.Errorf("%w: requested campaign root is absent", archive.ErrIntegrity)
	}
	return CampaignVerificationReport{
		GenesisVerified:   true,
		VerificationScope: VerificationScopeCampaign,
		DatasetID:         datasetID,
		CampaignID:        campaignID,
		ThroughRoot:       root,
		VerifiedThrough:   verifiedThrough,
		SegmentCount:      segmentCount,
		EntryCount:        entryCount,
	}, nil
}

func decodeRoot(value string) ([32]byte, error) {
	var result [32]byte
	if len(value) != 64 || strings.ToLower(value) != value {
		return result, fmt.Errorf("%w: through-root must be lowercase SHA-256", ErrSelectorInvalid)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("%w: through-root is invalid", ErrSelectorInvalid)
	}
	copy(result[:], decoded)
	if result == ([32]byte{}) {
		return result, fmt.Errorf("%w: through-root is zero", ErrSelectorInvalid)
	}
	return result, nil
}
