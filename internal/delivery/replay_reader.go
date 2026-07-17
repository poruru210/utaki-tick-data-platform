package delivery

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
)

type replaySnapshotRecord struct {
	descriptor ReplaySnapshotDescriptor
	scope      archive.ScopeConfig
	manifest   protocol.ReplayDayManifest
	body       []byte
}

func (r *archiveReaderV1) ListReplaySnapshots(ctx context.Context, scope ReplayDayScope) ([]ReplaySnapshotDescriptor, error) {
	if scope.DatasetID == "" || scope.ProviderID == "" || scope.ExactSourceSymbol == "" || scope.Date == "" || scope.ReplayContractID == "" || scope.ConversionID == "" {
		return nil, fmt.Errorf("%w: dataset, provider, exact source symbol, date, stream, and conversion are required", ErrSelectorInvalid)
	}
	records, err := r.loadReplaySnapshotRecords(ctx, scope)
	if err != nil {
		return nil, err
	}
	if err := requireSingleReplayIdentity(records); err != nil {
		return nil, err
	}
	result := make([]ReplaySnapshotDescriptor, len(records))
	for index := range records {
		result[index] = records[index].descriptor
	}
	return result, nil
}

func (r *archiveReaderV1) ResolveReplaySnapshot(ctx context.Context, selector ReplaySnapshotSelector) (ResolvedReplaySnapshot, error) {
	if selector.Manifest == "" && selector.Revision == nil && (selector.DatasetID == "" || selector.ProviderID == "" || selector.ExactSourceSymbol == "" || selector.Date == "" || selector.ReplayContractID == "" || selector.ConversionID == "") {
		return ResolvedReplaySnapshot{}, fmt.Errorf("%w: replay scope or immutable manifest selector is required", ErrSelectorInvalid)
	}
	if selector.Revision != nil && (*selector.Revision == 0 || selector.Manifest != "") {
		return ResolvedReplaySnapshot{}, fmt.Errorf("%w: revision and manifest selectors are mutually exclusive", ErrSelectorInvalid)
	}
	var selectorKind string
	var selectorDigest [32]byte
	var err error
	if selector.Manifest != "" {
		selectorKind, selectorDigest, err = parseManifestSelector(selector.Manifest, r.config.ImmutableRoot)
		if err != nil {
			return ResolvedReplaySnapshot{}, err
		}
	}
	records, err := r.loadReplaySnapshotRecords(ctx, selector.ReplayDayScope)
	if err != nil {
		return ResolvedReplaySnapshot{}, err
	}
	matched := records
	if selector.Manifest != "" || selector.Revision != nil {
		matched = nil
		for _, record := range records {
			if selector.Revision != nil && record.manifest.Revision == *selector.Revision ||
				selectorKind == "key" && record.descriptor.ManifestKey == selector.Manifest ||
				selectorKind == "digest" && record.descriptor.ManifestSHA256 == selectorDigest {
				matched = append(matched, record)
			}
		}
	}
	if len(matched) == 0 {
		return ResolvedReplaySnapshot{}, fmt.Errorf("%w: replay selector did not match", ErrSelectorNotFound)
	}
	if len(matched) != 1 && (selector.Manifest != "" || selector.Revision != nil) {
		return ResolvedReplaySnapshot{}, fmt.Errorf("%w: replay selector is ambiguous", archive.ErrIntegrity)
	}
	if selector.Manifest == "" {
		if err := requireSingleReplayIdentity(records); err != nil {
			return ResolvedReplaySnapshot{}, err
		}
	}
	record := matched[len(matched)-1]
	return ResolvedReplaySnapshot{
		Descriptor: record.descriptor, Scope: record.scope, Manifest: record.manifest,
		ManifestKey: record.descriptor.ManifestKey, ManifestBytes: append([]byte(nil), record.body...), ManifestSHA256: record.descriptor.ManifestSHA256,
	}, nil
}

func (r *archiveReaderV1) loadReplaySnapshotRecords(ctx context.Context, filter ReplayDayScope) ([]replaySnapshotRecord, error) {
	scopes, err := r.discoverScopes(ctx)
	if err != nil {
		return nil, err
	}
	var records []replaySnapshotRecord
	seenKeys := make(map[string]struct{})
	for _, discovered := range scopes {
		if filter.DatasetID != "" && discovered.Scope.DatasetID != filter.DatasetID ||
			filter.ProviderID != "" && discovered.Scope.ProviderID != filter.ProviderID ||
			filter.ExactSourceSymbol != "" && discovered.Scope.ExactSourceSymbol != filter.ExactSourceSymbol ||
			filter.DayDefinitionID != "" && discovered.Scope.DayDefinitionID != filter.DayDefinitionID {
			continue
		}
		layout, err := r2.NewLayout(r.config.ImmutableRoot, discovered.Scope)
		if err != nil {
			return nil, fmt.Errorf("%w: replay scope layout is invalid", archive.ErrIntegrity)
		}
		prefix, err := layout.RemoteKey("derivatives")
		if err != nil {
			return nil, err
		}
		objects, err := r.backend.ListLimited(ctx, prefix+"/", r.config.MaxRemoteObjects)
		if err != nil {
			return nil, err
		}
		sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
		for _, object := range objects {
			if !strings.Contains(object.Key, "/replay-day-") || !strings.HasSuffix(object.Key, ".json") {
				continue
			}
			if _, duplicate := seenKeys[object.Key]; duplicate {
				return nil, fmt.Errorf("%w: duplicate replay manifest key", archive.ErrIntegrity)
			}
			seenKeys[object.Key] = struct{}{}
			body, err := r.metadata(ctx, object.Key)
			if err != nil {
				return nil, err
			}
			if object.Size < 0 || int64(len(body)) != object.Size {
				return nil, fmt.Errorf("%w: replay manifest list size differs", archive.ErrIntegrity)
			}
			manifest, err := protocol.VerifyReplayDayManifest(body)
			if err != nil || manifest.M0EmptyPartsCompatibility {
				return nil, fmt.Errorf("%w: replay manifest is not strict M3 canonical JSON", archive.ErrIntegrity)
			}
			canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
			if err != nil || !bytes.Equal(canonical, body) {
				return nil, fmt.Errorf("%w: replay manifest bytes are not canonical", archive.ErrIntegrity)
			}
			digest, err := protocol.ReplayDayManifestDigest(manifest)
			if err != nil || manifest.ManifestSHA256 != digest {
				return nil, fmt.Errorf("%w: replay manifest digest is invalid", archive.ErrIntegrity)
			}
			wantKey, err := layout.ReplayDayManifestKey(manifest)
			if err != nil || wantKey != object.Key {
				return nil, fmt.Errorf("%w: replay manifest full key is invalid", archive.ErrIntegrity)
			}
			if manifest.DatasetID != discovered.Scope.DatasetID || manifest.DayDefinitionID != discovered.Scope.DayDefinitionID {
				return nil, fmt.Errorf("%w: replay manifest scope differs from descriptor", archive.ErrIntegrity)
			}
			if filter.Date != "" && manifest.Date != filter.Date || filter.ReplayContractID != "" && manifest.ReplayContractID != filter.ReplayContractID || filter.ConversionID != "" && manifest.ConversionID != filter.ConversionID {
				continue
			}
			if err := r.verifyReplayRawBinding(ctx, layout, manifest); err != nil {
				return nil, err
			}
			manifest.ManifestSHA256 = digest
			descriptor := replaySnapshotDescriptor(discovered.Scope, manifest, object.Key)
			records = append(records, replaySnapshotRecord{descriptor: descriptor, scope: discovered.Scope, manifest: manifest, body: append([]byte(nil), body...)})
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i].manifest, records[j].manifest
		if left.ManifestID != right.ManifestID {
			return left.ManifestID < right.ManifestID
		}
		if left.Revision != right.Revision {
			return left.Revision < right.Revision
		}
		return records[i].descriptor.ManifestKey < records[j].descriptor.ManifestKey
	})
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && records[end].manifest.ManifestID == records[start].manifest.ManifestID {
			end++
		}
		revisions := make([]protocol.ReplayDayManifest, end-start)
		for index := range revisions {
			revisions[index] = records[start+index].manifest
		}
		if _, err := r2.VerifyReplayRevisionGraph(revisions, uint64(len(revisions))); err != nil {
			return nil, fmt.Errorf("%w: replay revision graph is invalid: %v", archive.ErrIntegrity, err)
		}
		start = end
	}
	return records, nil
}

func (r *archiveReaderV1) verifyReplayRawBinding(ctx context.Context, layout r2.Layout, manifest protocol.ReplayDayManifest) error {
	fullKey, err := layout.RemoteKey(manifest.RawDayManifestKey)
	if err != nil {
		return fmt.Errorf("%w: replay raw manifest key is invalid", archive.ErrIntegrity)
	}
	body, err := r.metadata(ctx, fullKey)
	if err != nil {
		return err
	}
	raw, err := archive.VerifyRawDayManifest(body)
	if err != nil || raw.ManifestSHA256 != manifest.RawDayManifestSHA256 || raw.DatasetID != manifest.DatasetID || raw.DayDefinitionID != manifest.DayDefinitionID || raw.Date != manifest.Date {
		return fmt.Errorf("%w: replay raw binding mismatch", archive.ErrIntegrity)
	}
	wantKey, err := layout.ManifestKey(raw)
	if err != nil || wantKey != fullKey {
		return fmt.Errorf("%w: replay raw manifest full key is invalid", archive.ErrIntegrity)
	}
	return nil
}

func requireSingleReplayIdentity(records []replaySnapshotRecord) error {
	if len(records) == 0 {
		return fmt.Errorf("%w: no replay snapshots matched", archive.ErrIntegrity)
	}
	identity := records[0].manifest.ManifestID
	for _, record := range records[1:] {
		if record.manifest.ManifestID != identity {
			return fmt.Errorf("%w: replay conversion or stream identity is ambiguous", archive.ErrIntegrity)
		}
	}
	return nil
}

func replaySnapshotDescriptor(scope archive.ScopeConfig, manifest protocol.ReplayDayManifest, key string) ReplaySnapshotDescriptor {
	var predecessor *[32]byte
	if manifest.PreviousManifestSHA256 != nil {
		value := *manifest.PreviousManifestSHA256
		predecessor = &value
	}
	return ReplaySnapshotDescriptor{
		DatasetID: manifest.DatasetID, ProviderID: scope.ProviderID, ExactSourceSymbol: scope.ExactSourceSymbol,
		DayDefinitionID: manifest.DayDefinitionID,
		Date:            manifest.Date, ReplayContractID: manifest.ReplayContractID, ConversionID: manifest.ConversionID,
		Revision: manifest.Revision, ManifestKey: key, ManifestSHA256: manifest.ManifestSHA256,
		PreviousManifestSHA256: predecessor, RawDayManifestKey: manifest.RawDayManifestKey, RawDayManifestSHA256: manifest.RawDayManifestSHA256,
		PartSetRoot: manifest.PartSetRoot, CanonicalStreamRowChainRoot: manifest.CanonicalStreamRowChainRoot, PartCount: uint64(len(manifest.PartManifestKeys)),
	}
}

func replayManifestDigestString(value [32]byte) string { return hex.EncodeToString(value[:]) }
