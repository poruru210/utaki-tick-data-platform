package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
)

func (r *archiveReaderV1) BuildReplayFetchPlan(ctx context.Context, snapshot ResolvedReplaySnapshot) (ReplayFetchPlan, error) {
	if snapshot.ManifestKey == "" {
		return ReplayFetchPlan{}, fmt.Errorf("%w: resolved replay snapshot is incomplete", archive.ErrIntegrity)
	}
	trusted, err := r.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{Manifest: snapshot.ManifestKey})
	if err != nil {
		return ReplayFetchPlan{}, err
	}
	if trusted.ManifestSHA256 != snapshot.ManifestSHA256 || !bytes.Equal(trusted.ManifestBytes, snapshot.ManifestBytes) {
		return ReplayFetchPlan{}, fmt.Errorf("%w: resolved replay snapshot differs from remote immutable manifest", archive.ErrIntegrity)
	}
	manifest := trusted.Manifest
	layout, err := r2.NewLayout(r.config.ImmutableRoot, trusted.Scope)
	if err != nil {
		return ReplayFetchPlan{}, err
	}
	scope := replayProtocolScope(manifest)
	conversion := replayConversion(manifest)
	parts := make([]protocol.PartManifest, len(manifest.PartManifestKeys))
	partObjects := make([]ReplayFetchObject, len(parts))
	parquetObjects := make([]ReplayFetchObject, len(parts))
	for index, relativeKey := range manifest.PartManifestKeys {
		fullKey, err := layout.RemoteKey(relativeKey)
		if err != nil {
			return ReplayFetchPlan{}, fmt.Errorf("%w: part manifest key is invalid", archive.ErrIntegrity)
		}
		body, err := r.metadata(ctx, fullKey)
		if err != nil {
			return ReplayFetchPlan{}, err
		}
		decoded, err := protocol.VerifyPartManifest(body)
		if err != nil {
			return ReplayFetchPlan{}, fmt.Errorf("%w: part manifest is invalid", archive.ErrIntegrity)
		}
		digest, err := protocol.PartManifestDigest(decoded)
		if err != nil {
			return ReplayFetchPlan{}, err
		}
		part, err := archive.VerifyPartManifestObject(body, relativeKey, digest)
		if err != nil || uint32(index) != part.PartSequence {
			return ReplayFetchPlan{}, fmt.Errorf("%w: part manifest chain order is invalid", archive.ErrIntegrity)
		}
		if err := layout.VerifyReplayPartManifestKey(part, fullKey); err != nil {
			return ReplayFetchPlan{}, fmt.Errorf("%w: %v", archive.ErrIntegrity, err)
		}
		parquetKey, err := layout.ReplayPartObjectKey(part)
		if err != nil {
			return ReplayFetchPlan{}, fmt.Errorf("%w: parquet key is invalid", archive.ErrIntegrity)
		}
		parts[index] = part
		partObjects[index] = ReplayFetchObject{Kind: ReplayFetchPartManifest, Key: relativeKey, RemoteKey: fullKey, Digest: digest, Bytes: uint64(len(body)), CachePath: r.cacheReplayPartManifestPath(part, digest), CanonicalBytes: append([]byte(nil), body...)}
		parquetObjects[index] = ReplayFetchObject{Kind: ReplayFetchParquet, Key: part.PartKey, RemoteKey: parquetKey, Digest: part.PartSHA256, Bytes: part.PartBytes, CachePath: r.cacheReplayParquetPath(part.PartSHA256)}
	}
	graph, err := r2.VerifyReplayPartGraph(parts, scope, conversion, maxReplayNodes(len(parts)))
	if err != nil || graph.PartSetRoot != manifest.PartSetRoot || graph.CanonicalRowChainRoot != manifest.CanonicalStreamRowChainRoot {
		return ReplayFetchPlan{}, fmt.Errorf("%w: replay part graph differs from manifest", archive.ErrIntegrity)
	}
	plan := ReplayFetchPlan{
		Manifest: ReplayFetchObject{Kind: ReplayFetchManifest, Key: trusted.ManifestKey, RemoteKey: trusted.ManifestKey, Digest: trusted.ManifestSHA256, Bytes: uint64(len(trusted.ManifestBytes)), CachePath: r.cacheReplayManifestPath(manifest), CanonicalBytes: append([]byte(nil), trusted.ManifestBytes...)},
		Parts:    partObjects, Parquet: parquetObjects,
	}
	if err := r.validateReplayFetchPlan(plan); err != nil {
		return ReplayFetchPlan{}, err
	}
	if err := r.validateReplayFetchPlanAgainstSnapshot(plan, trusted); err != nil {
		return ReplayFetchPlan{}, err
	}
	return plan, nil
}

func (r *archiveReaderV1) FetchReplay(ctx context.Context, plan ReplayFetchPlan, destination string) (ReplayFetchResult, error) {
	if err := r.validateReplayFetchPlan(plan); err != nil {
		return ReplayFetchResult{}, err
	}
	trusted, err := r.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{Manifest: plan.Manifest.Key})
	if err != nil {
		return ReplayFetchResult{}, err
	}
	if err := r.validateReplayFetchPlanAgainstSnapshot(plan, trusted); err != nil {
		return ReplayFetchResult{}, err
	}
	if err := r.fetchReplayMetadata(ctx, plan.Manifest); err != nil {
		return ReplayFetchResult{}, err
	}
	result := ReplayFetchResult{ManifestPath: plan.Manifest.CachePath, PartManifestPaths: make(map[string]string, len(plan.Parts)), ParquetPaths: make(map[string]string, len(plan.Parquet))}
	for _, object := range plan.Parts {
		if err := r.fetchReplayMetadata(ctx, object); err != nil {
			return ReplayFetchResult{}, err
		}
		result.PartManifestPaths[object.Key] = object.CachePath
	}
	for _, object := range plan.Parquet {
		if err := r.fetchReplayParquet(ctx, object); err != nil {
			return ReplayFetchResult{}, err
		}
		result.ParquetPaths[object.Key] = object.CachePath
	}
	if destination == "" {
		return result, nil
	}
	manifestOutput := filepath.Join(destination, "replay-manifest-"+hex.EncodeToString(plan.Manifest.Digest[:])+".json")
	if err := publishVerifiedBytes(manifestOutput, plan.Manifest.CanonicalBytes); err != nil {
		return ReplayFetchResult{}, err
	}
	result.ManifestPath = manifestOutput
	result.PartManifestPaths = make(map[string]string, len(plan.Parts))
	for index, object := range plan.Parts {
		output := filepath.Join(destination, fmt.Sprintf("part-manifest-%08d-%s.json", index, hex.EncodeToString(object.Digest[:])))
		if err := publishVerifiedBytes(output, object.CanonicalBytes); err != nil {
			return ReplayFetchResult{}, err
		}
		result.PartManifestPaths[object.Key] = output
	}
	result.ParquetPaths = make(map[string]string, len(plan.Parquet))
	for _, object := range plan.Parquet {
		output := filepath.Join(destination, "parquet-"+hex.EncodeToString(object.Digest[:])+".parquet")
		if err := copyVerifiedFile(object.CachePath, output, object.Digest, object.Bytes, false); err != nil {
			return ReplayFetchResult{}, err
		}
		result.ParquetPaths[object.Key] = output
	}
	return result, nil
}

func (r *archiveReaderV1) validateReplayFetchPlanAgainstSnapshot(plan ReplayFetchPlan, snapshot ResolvedReplaySnapshot) error {
	if plan.Manifest.Key != snapshot.ManifestKey || plan.Manifest.Digest != snapshot.ManifestSHA256 || !bytes.Equal(plan.Manifest.CanonicalBytes, snapshot.ManifestBytes) {
		return fmt.Errorf("%w: replay plan is not bound to the resolved manifest", archive.ErrIntegrity)
	}
	layout, err := r2.NewLayout(r.config.ImmutableRoot, snapshot.Scope)
	if err != nil {
		return err
	}
	for index, object := range plan.Parts {
		part, err := protocol.VerifyPartManifest(object.CanonicalBytes)
		if err != nil {
			return fmt.Errorf("%w: replay plan part is invalid", archive.ErrIntegrity)
		}
		partRemote, err := layout.ReplayPartManifestKey(part)
		if err != nil || object.RemoteKey != partRemote {
			return fmt.Errorf("%w: replay plan part remote key is not trusted", archive.ErrIntegrity)
		}
		parquetRemote, err := layout.ReplayPartObjectKey(part)
		if err != nil || plan.Parquet[index].RemoteKey != parquetRemote {
			return fmt.Errorf("%w: replay plan parquet remote key is not trusted", archive.ErrIntegrity)
		}
	}
	return nil
}

func (r *archiveReaderV1) validateReplayFetchPlan(plan ReplayFetchPlan) error {
	if plan.Manifest.Kind != ReplayFetchManifest || plan.Manifest.Key == "" || plan.Manifest.Key != plan.Manifest.RemoteKey || len(plan.Manifest.CanonicalBytes) == 0 || uint64(len(plan.Manifest.CanonicalBytes)) != plan.Manifest.Bytes {
		return fmt.Errorf("%w: replay fetch plan manifest is incomplete", archive.ErrIntegrity)
	}
	manifest, err := protocol.VerifyReplayDayManifest(plan.Manifest.CanonicalBytes)
	if err != nil || manifest.ManifestSHA256 != plan.Manifest.Digest || plan.Manifest.CachePath != r.cacheReplayManifestPath(manifest) {
		return fmt.Errorf("%w: replay fetch plan manifest is invalid", archive.ErrIntegrity)
	}
	if len(plan.Parts) != len(manifest.PartManifestKeys) || len(plan.Parquet) != len(plan.Parts) {
		return fmt.Errorf("%w: replay fetch plan object counts differ", archive.ErrIntegrity)
	}
	for index, object := range plan.Parts {
		if object.Kind != ReplayFetchPartManifest || object.Key != manifest.PartManifestKeys[index] || len(object.CanonicalBytes) == 0 || uint64(len(object.CanonicalBytes)) != object.Bytes {
			return fmt.Errorf("%w: replay part manifest plan is invalid", archive.ErrIntegrity)
		}
		part, err := protocol.VerifyPartManifest(object.CanonicalBytes)
		if err != nil || part.ManifestSHA256 != object.Digest || part.PartSequence != uint32(index) || object.CachePath != r.cacheReplayPartManifestPath(part, object.Digest) {
			return fmt.Errorf("%w: replay part manifest plan differs", archive.ErrIntegrity)
		}
		parquetObject := plan.Parquet[index]
		if parquetObject.Kind != ReplayFetchParquet || parquetObject.Key != part.PartKey || parquetObject.Digest != part.PartSHA256 || parquetObject.Bytes != part.PartBytes || parquetObject.CachePath != r.cacheReplayParquetPath(part.PartSHA256) || len(parquetObject.CanonicalBytes) != 0 {
			return fmt.Errorf("%w: replay parquet plan differs", archive.ErrIntegrity)
		}
	}
	return nil
}

func (r *archiveReaderV1) fetchReplayMetadata(ctx context.Context, object ReplayFetchObject) error {
	body, err := r.metadata(ctx, object.RemoteKey)
	if err != nil {
		return err
	}
	if !bytes.Equal(body, object.CanonicalBytes) || uint64(len(body)) != object.Bytes {
		return fmt.Errorf("%w: remote replay metadata changed", archive.ErrIntegrity)
	}
	return publishVerifiedBytes(object.CachePath, body)
}

func (r *archiveReaderV1) fetchReplayParquet(ctx context.Context, object ReplayFetchObject) error {
	if object.Bytes == 0 || object.Bytes > r.config.MaxRawObjectBytes {
		return fmt.Errorf("%w: replay parquet exceeds configured limit", archive.ErrIntegrity)
	}
	if err := verifyCachedFile(object.CachePath, object.Digest, object.Bytes, false); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	body, advertised, err := r.backend.Open(ctx, object.RemoteKey)
	if err != nil {
		return err
	}
	if advertised < 0 || uint64(advertised) != object.Bytes {
		_ = body.Close()
		return fmt.Errorf("%w: replay parquet advertised size differs", archive.ErrIntegrity)
	}
	return streamReplayObjectToCache(body, object)
}

func streamReplayObjectToCache(source io.ReadCloser, object ReplayFetchObject) error {
	if err := os.MkdirAll(filepath.Dir(object.CachePath), 0o700); err != nil {
		_ = source.Close()
		return fmt.Errorf("create replay cache directory")
	}
	temporary, err := os.CreateTemp(filepath.Dir(object.CachePath), ".delivery-replay-*.tmp")
	if err != nil {
		_ = source.Close()
		return fmt.Errorf("create replay temporary")
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		_ = source.Close()
		return fmt.Errorf("set replay temporary permissions")
	}
	digest := sha256.New()
	count, copyErr := io.Copy(io.MultiWriter(temporary, digest), io.LimitReader(source, int64(object.Bytes)+1))
	closeSourceErr := source.Close()
	if copyErr != nil || closeSourceErr != nil {
		_ = temporary.Close()
		return fmt.Errorf("stream or close replay object")
	}
	if uint64(count) != object.Bytes || !bytes.Equal(digest.Sum(nil), object.Digest[:]) {
		_ = temporary.Close()
		return fmt.Errorf("%w: streamed replay object size or hash differs", archive.ErrIntegrity)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync replay temporary")
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close replay temporary")
	}
	if err := os.Link(temporaryPath, object.CachePath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("publish replay cache")
	}
	return verifyCachedFile(object.CachePath, object.Digest, object.Bytes, false)
}

func (r *archiveReaderV1) cacheReplayManifestPath(manifest protocol.ReplayDayManifest) string {
	return filepath.Join(r.config.CacheRoot, "manifests", fmt.Sprintf("replay-day-%d-%s.json", manifest.Revision, hex.EncodeToString(manifest.ManifestSHA256[:])))
}

func (r *archiveReaderV1) cacheReplayPartManifestPath(part protocol.PartManifest, digest [32]byte) string {
	return filepath.Join(r.config.CacheRoot, "manifests", fmt.Sprintf("replay-part-%08d-%s.json", part.PartSequence, hex.EncodeToString(digest[:])))
}

func (r *archiveReaderV1) cacheReplayParquetPath(digest [32]byte) string {
	return filepath.Join(r.config.CacheRoot, "objects", "replay", "parquet-"+hex.EncodeToString(digest[:])+".parquet")
}

func replayProtocolScope(manifest protocol.ReplayDayManifest) protocol.ReplayScope {
	return protocol.ReplayScope{DatasetID: manifest.DatasetID, CampaignID: manifest.CampaignID, DayDefinitionID: manifest.DayDefinitionID, Date: manifest.Date, ReplayContractID: manifest.ReplayContractID, ConversionID: manifest.ConversionID, RawDayManifestKey: manifest.RawDayManifestKey, RawDayManifestSHA256: manifest.RawDayManifestSHA256}
}

func replayConversion(manifest protocol.ReplayDayManifest) archive.ConversionTuple {
	return archive.ConversionTuple{ReplayContractID: manifest.ReplayContractID, FormatID: manifest.FormatID, ConversionID: manifest.ConversionID, ConverterBuildID: manifest.ConverterBuildID, DependencyLockHash: manifest.DependencyLockHash, WriterConfigurationHash: manifest.WriterConfigurationHash, TargetPlatformContract: manifest.TargetPlatformContract}
}

func maxReplayNodes(count int) uint64 {
	if count == 0 {
		return 1
	}
	return uint64(count)
}

func replayPartArtifact(part protocol.PartManifest, path string) parquet.PartArtifact {
	return parquet.PartArtifact{PartSequence: part.PartSequence, Path: path, PartKey: part.PartKey, PartSHA256: part.PartSHA256, PartBytes: part.PartBytes, RowCount: part.RowCount, CanonicalRowBytes: part.CanonicalRowBytes, FirstStreamSequence: part.FirstStreamSequence, LastStreamSequence: part.LastStreamSequence, PreviousRowChainHash: part.PreviousRowChainHash, FirstRowChainHash: part.FirstRowChainHash, LastRowChainHash: part.LastRowChainHash}
}
