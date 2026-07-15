package delivery

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/r2"
)

type archiveReaderV1 struct {
	config  ReaderConfig
	backend r2.ReadBackend
}

func NewArchiveReaderV1(ctx context.Context, config ReaderConfig) (ArchiveReaderV1, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	bucket, err := config.Bucket()
	if err != nil {
		return nil, err
	}
	backend, err := r2.NewS3ReadBackend(ctx, r2.S3ReadBackendConfig{
		Bucket:           bucket,
		Endpoint:         config.Endpoint,
		Region:           config.Region,
		AccessKeyEnv:     config.AccessKeyEnv,
		SecretKeyEnv:     config.SecretKeyEnv,
		MaxMetadataBytes: int64(config.MaxMetadataBytes),
	})
	if err != nil {
		return nil, err
	}
	return &archiveReaderV1{config: config, backend: backend}, nil
}

func NewArchiveReaderV1WithBackend(config ReaderConfig, backend r2.ReadBackend) (ArchiveReaderV1, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if backend == nil {
		return nil, fmt.Errorf("read-only backend is nil")
	}
	return &archiveReaderV1{config: config, backend: backend}, nil
}

type discoveredScope struct {
	Scope         archive.ScopeConfig
	Descriptor    []byte
	DescriptorKey string
}

func (r *archiveReaderV1) discoverScopes(ctx context.Context) ([]discoveredScope, error) {
	prefix := strings.TrimRight(r.config.ImmutableRoot, "/") + "/"
	objects, err := r.backend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	seen := make(map[string]struct{})
	result := make([]discoveredScope, 0)
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, "/scope-descriptor-v1.json") {
			continue
		}
		if _, ok := seen[object.Key]; ok {
			return nil, fmt.Errorf("%w: duplicate scope descriptor key", archive.ErrIntegrity)
		}
		seen[object.Key] = struct{}{}
		body, err := r.metadata(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		if object.Size >= 0 && int64(len(body)) != object.Size {
			return nil, fmt.Errorf("%w: scope descriptor size changed", archive.ErrIntegrity)
		}
		scope, err := archive.ScopeConfigFromCanonicalJSON(body)
		if err != nil {
			return nil, fmt.Errorf("%w: scope descriptor is invalid", archive.ErrIntegrity)
		}
		layout, err := r2.NewLayout(r.config.ImmutableRoot, "", scope)
		if err != nil {
			return nil, fmt.Errorf("%w: scope layout is invalid", archive.ErrIntegrity)
		}
		wantKey, err := layout.ScopeDescriptorKey()
		if err != nil || wantKey != object.Key {
			return nil, fmt.Errorf("%w: scope descriptor key does not match identity", archive.ErrIntegrity)
		}
		scopeKey, err := archive.ScopePathKey(scope)
		if err != nil {
			return nil, err
		}
		if _, ok := seenScopeKey(result, scopeKey); ok {
			return nil, fmt.Errorf("%w: duplicate scope descriptor identity", archive.ErrIntegrity)
		}
		result = append(result, discoveredScope{Scope: scope, Descriptor: append([]byte(nil), body...), DescriptorKey: object.Key})
	}
	return result, nil
}

func seenScopeKey(scopes []discoveredScope, want string) (discoveredScope, bool) {
	for _, item := range scopes {
		key, err := archive.ScopePathKey(item.Scope)
		if err == nil && key == want {
			return item, true
		}
	}
	return discoveredScope{}, false
}

func (r *archiveReaderV1) metadata(ctx context.Context, key string) ([]byte, error) {
	body, err := r.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) > r.config.MaxMetadataBytes {
		return nil, r2.ErrMetadataTooLarge
	}
	return body, nil
}

func (r *archiveReaderV1) ListDatasets(ctx context.Context) ([]DatasetDescriptor, error) {
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	result := make([]DatasetDescriptor, 0)
	for _, item := range scopes {
		if _, ok := seen[item.Scope.DatasetID]; ok {
			continue
		}
		seen[item.Scope.DatasetID] = struct{}{}
		result = append(result, DatasetDescriptor{DatasetID: item.Scope.DatasetID})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DatasetID < result[j].DatasetID })
	return result, nil
}

func (r *archiveReaderV1) ListCampaigns(ctx context.Context, datasetID string) ([]CampaignDescriptor, error) {
	if datasetID == "" {
		return nil, fmt.Errorf("dataset is required")
	}
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]CampaignDescriptor, 0)
	for _, item := range scopes {
		if item.Scope.DatasetID != datasetID {
			continue
		}
		hash, err := item.Scope.ConfigHash()
		if err != nil {
			return nil, err
		}
		result = append(result, CampaignDescriptor{
			DatasetID: item.Scope.DatasetID, CampaignID: item.Scope.CampaignID,
			ProviderID: item.Scope.ProviderID, StableFeedID: item.Scope.StableFeedID,
			ExactSourceSymbol:       item.Scope.ExactSourceSymbol,
			BrokerServerFingerprint: item.Scope.BrokerServerFingerprint,
			DayDefinitionID:         item.Scope.DayDefinitionID, PublisherID: item.Scope.PublisherID,
			PublisherEpoch: item.Scope.PublisherEpoch, ConfigHash: hash,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CampaignID != result[j].CampaignID {
			return result[i].CampaignID < result[j].CampaignID
		}
		return result[i].ProviderID < result[j].ProviderID
	})
	return result, nil
}

func (r *archiveReaderV1) ListRawSnapshots(ctx context.Context, scope RawDayScope) ([]SnapshotDescriptor, error) {
	if scope.DatasetID == "" || scope.CampaignID == "" {
		return nil, fmt.Errorf("dataset and campaign are required")
	}
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return nil, err
	}
	var matches []discoveredScope
	for _, item := range scopes {
		if item.Scope.DatasetID == scope.DatasetID && item.Scope.CampaignID == scope.CampaignID {
			matches = append(matches, item)
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("%w: campaign identity is ambiguous or absent", archive.ErrIntegrity)
	}
	snapshots, err := r.loadSnapshots(ctx, matches[0])
	if err != nil {
		return nil, err
	}
	if scope.Date == "" {
		return snapshots, nil
	}
	filtered := snapshots[:0]
	for _, snapshot := range snapshots {
		if snapshot.Date == scope.Date {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered, nil
}

func (r *archiveReaderV1) loadSnapshots(ctx context.Context, discovered discoveredScope) ([]SnapshotDescriptor, error) {
	layout, err := r2.NewLayout(r.config.ImmutableRoot, "", discovered.Scope)
	if err != nil {
		return nil, fmt.Errorf("%w: campaign layout is invalid", archive.ErrIntegrity)
	}
	base, err := layout.RemoteKey("snapshots/raw/day-definition=" + archive.IdentityPathKey(discovered.Scope.DayDefinitionID))
	if err != nil {
		return nil, err
	}
	objects, err := r.backend.List(ctx, base+"/")
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	records := make([]r2.ManifestRecord, 0, len(objects))
	descriptors := make([]SnapshotDescriptor, 0, len(objects))
	seenKey := make(map[string]struct{})
	seenDigest := make(map[[32]byte]struct{})
	for _, object := range objects {
		if _, ok := seenKey[object.Key]; ok {
			return nil, fmt.Errorf("%w: duplicate manifest key", archive.ErrIntegrity)
		}
		seenKey[object.Key] = struct{}{}
		body, err := r.metadata(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		manifest, err := archive.VerifyRawDayManifest(body)
		if err != nil {
			return nil, fmt.Errorf("%w: remote manifest is invalid", archive.ErrIntegrity)
		}
		canonical, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(canonical, body) {
			return nil, fmt.Errorf("%w: remote manifest bytes are not canonical", archive.ErrIntegrity)
		}
		wantKey, err := layout.ManifestKey(manifest)
		if err != nil || wantKey != object.Key {
			return nil, fmt.Errorf("%w: remote manifest key does not match digest", archive.ErrIntegrity)
		}
		if object.Size >= 0 && int64(len(body)) != object.Size {
			return nil, fmt.Errorf("%w: remote manifest size changed", archive.ErrIntegrity)
		}
		if err := manifestMatchesScope(manifest, discovered.Scope); err != nil {
			return nil, err
		}
		if _, ok := seenDigest[manifest.ManifestSHA256]; ok {
			return nil, fmt.Errorf("%w: duplicate manifest digest", archive.ErrIntegrity)
		}
		seenDigest[manifest.ManifestSHA256] = struct{}{}
		records = append(records, r2.ManifestRecord{Key: object.Key, Bytes: append([]byte(nil), body...), Manifest: manifest})
		descriptors = append(descriptors, snapshotDescriptor(manifest, object.Key))
	}
	byDate := make(map[string][]r2.ManifestRecord)
	for _, record := range records {
		byDate[record.Manifest.Date] = append(byDate[record.Manifest.Date], record)
	}
	for _, dateRecords := range byDate {
		if len(dateRecords) == 0 {
			continue
		}
		if _, err := r2.ValidateRevisionGraph(dateRecords[0].Manifest, dateRecords[0].Bytes, dateRecords); err != nil {
			return nil, fmt.Errorf("%w: revision graph is invalid: %v", archive.ErrIntegrity, err)
		}
	}
	sort.Slice(descriptors, func(i, j int) bool {
		if descriptors[i].Date != descriptors[j].Date {
			return descriptors[i].Date < descriptors[j].Date
		}
		return descriptors[i].Revision < descriptors[j].Revision
	})
	return descriptors, nil
}

func snapshotDescriptor(manifest archive.RawDayManifest, key string) SnapshotDescriptor {
	return SnapshotDescriptor{
		DatasetID: manifest.DatasetID, CampaignID: manifest.CampaignID, DayDefinitionID: manifest.DayDefinitionID,
		Date: manifest.Date, Revision: manifest.Revision, PublisherID: manifest.PublisherID,
		PublisherEpoch: manifest.PublisherEpoch, ManifestKey: key, ManifestSHA256: manifest.ManifestSHA256,
		ChainSliceStart: manifest.ChainSliceStartSequence, ChainSliceStartRoot: manifest.ChainSliceStartRoot,
		ChainSliceEnd: manifest.ChainSliceEndSequence, ChainSliceEndRoot: manifest.ChainSliceEndRoot,
		AcceptedRecordCount: manifest.AcceptedRecordCount, ErrorCount: manifest.ErrorCount,
	}
}

func manifestMatchesScope(manifest archive.RawDayManifest, scope archive.ScopeConfig) error {
	hash, err := scope.ConfigHash()
	if err != nil {
		return err
	}
	if manifest.DatasetID != scope.DatasetID || manifest.CampaignID != scope.CampaignID ||
		manifest.DayDefinitionID != scope.DayDefinitionID || manifest.PublisherID != scope.PublisherID ||
		manifest.PublisherEpoch != scope.PublisherEpoch || manifest.SettlePolicy != scope.SettlePolicy ||
		manifest.ConfigHash != hash {
		return fmt.Errorf("%w: manifest scope differs from descriptor", archive.ErrIntegrity)
	}
	return nil
}

func (r *archiveReaderV1) ResolveSnapshot(ctx context.Context, selector SnapshotSelector) (ResolvedSnapshot, error) {
	if selector.Manifest == "" {
		return ResolvedSnapshot{}, fmt.Errorf("%w: immutable manifest selector is required", ErrSelectorInvalid)
	}
	selectorKind, digest, err := parseManifestSelector(selector.Manifest, r.config.ImmutableRoot)
	if err != nil {
		return ResolvedSnapshot{}, err
	}
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return ResolvedSnapshot{}, err
	}
	var matches []ResolvedSnapshot
	for _, discovered := range scopes {
		if selector.DatasetID != "" && discovered.Scope.DatasetID != selector.DatasetID {
			continue
		}
		if selector.CampaignID != "" && discovered.Scope.CampaignID != selector.CampaignID {
			continue
		}
		snapshots, err := r.loadSnapshots(ctx, discovered)
		if err != nil {
			return ResolvedSnapshot{}, err
		}
		for _, descriptor := range snapshots {
			if selector.Date != "" && descriptor.Date != selector.Date {
				continue
			}
			matched := (selectorKind == "key" && descriptor.ManifestKey == selector.Manifest) ||
				(selectorKind == "digest" && descriptor.ManifestSHA256 == digest)
			if !matched {
				continue
			}
			body, err := r.metadata(ctx, descriptor.ManifestKey)
			if err != nil {
				return ResolvedSnapshot{}, err
			}
			manifest, err := archive.VerifyRawDayManifest(body)
			if err != nil || manifest.ManifestSHA256 != descriptor.ManifestSHA256 {
				return ResolvedSnapshot{}, fmt.Errorf("%w: resolved manifest changed", archive.ErrIntegrity)
			}
			matches = append(matches, ResolvedSnapshot{
				Descriptor: descriptor, Scope: discovered.Scope, Manifest: manifest,
				ManifestKey: descriptor.ManifestKey, ManifestBytes: append([]byte(nil), body...), ManifestSHA256: manifest.ManifestSHA256,
			})
		}
	}
	if len(matches) == 0 {
		return ResolvedSnapshot{}, fmt.Errorf("%w: manifest selector did not match exactly one snapshot", archive.ErrIntegrity)
	}
	if len(matches) != 1 {
		return ResolvedSnapshot{}, fmt.Errorf("%w: manifest selector matched multiple snapshots", archive.ErrIntegrity)
	}
	return matches[0], nil
}

func parseManifestSelector(value, immutableRoot string) (string, [32]byte, error) {
	var zero [32]byte
	if len(value) == 64 {
		if strings.ToLower(value) != value {
			return "", zero, fmt.Errorf("%w: manifest digest must be lowercase", ErrSelectorInvalid)
		}
		decoded, err := hex.DecodeString(value)
		if err != nil || bytes.Equal(decoded, zero[:]) {
			return "", zero, fmt.Errorf("%w: manifest digest is invalid", ErrSelectorInvalid)
		}
		copy(zero[:], decoded)
		return "digest", zero, nil
	}
	root := strings.TrimRight(immutableRoot, "/") + "/"
	if !strings.HasPrefix(value, root) || strings.ContainsAny(value, "\\\r\n") || strings.Contains(value, "//") {
		return "", zero, fmt.Errorf("%w: immutable manifest key is not canonical", ErrSelectorInvalid)
	}
	for _, part := range strings.Split(strings.TrimPrefix(value, root), "/") {
		if part == "" || part == "." || part == ".." {
			return "", zero, fmt.Errorf("%w: immutable manifest key contains a traversal segment", ErrSelectorInvalid)
		}
	}
	return "key", zero, nil
}
