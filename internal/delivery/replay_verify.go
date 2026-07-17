package delivery

import (
	"context"
	"fmt"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
)

func (r *archiveReaderV1) VerifyReplayDay(ctx context.Context, selector ReplaySnapshotSelector) (ReplayDayVerificationReport, error) {
	snapshot, err := r.ResolveReplaySnapshot(ctx, selector)
	if err != nil {
		return ReplayDayVerificationReport{}, err
	}
	plan, err := r.BuildReplayFetchPlan(ctx, snapshot)
	if err != nil {
		return ReplayDayVerificationReport{}, err
	}
	fetched, err := r.FetchReplay(ctx, plan, "")
	if err != nil {
		return ReplayDayVerificationReport{}, err
	}
	layout, err := r2.NewLayout(r.config.ImmutableRoot, snapshot.Scope)
	if err != nil {
		return ReplayDayVerificationReport{}, err
	}
	rawKey, err := layout.RemoteKey(snapshot.Manifest.RawDayManifestKey)
	if err != nil {
		return ReplayDayVerificationReport{}, err
	}
	rawSnapshot, err := r.ResolveSnapshot(ctx, SnapshotSelector{Manifest: rawKey})
	if err != nil || rawSnapshot.ManifestSHA256 != snapshot.Manifest.RawDayManifestSHA256 {
		return ReplayDayVerificationReport{}, fmt.Errorf("%w: replay raw binding did not resolve exactly", archive.ErrIntegrity)
	}
	if _, err := r.VerifyDay(ctx, SnapshotSelector{Manifest: rawKey}); err != nil {
		return ReplayDayVerificationReport{}, fmt.Errorf("%w: replay raw day semantic verification failed: %v", archive.ErrIntegrity, err)
	}
	parts := make([]protocol.PartManifest, len(plan.Parts))
	var rowCount uint64
	for index, object := range plan.Parts {
		part, err := archive.ReadAndVerifyPartManifest(fetched.PartManifestPaths[object.Key], object.Key, object.Digest)
		if err != nil {
			return ReplayDayVerificationReport{}, fmt.Errorf("%w: cached part manifest verification failed", archive.ErrIntegrity)
		}
		parts[index] = part
		if rowCount > ^uint64(0)-part.RowCount {
			return ReplayDayVerificationReport{}, fmt.Errorf("%w: replay row count overflow", archive.ErrIntegrity)
		}
		rowCount += part.RowCount
		parquetPath, ok := fetched.ParquetPaths[part.PartKey]
		if !ok {
			return ReplayDayVerificationReport{}, fmt.Errorf("%w: fetched parquet part is missing", archive.ErrIntegrity)
		}
		if err := parquet.VerifyPartFile(parquetPath, replayPartArtifact(part, parquetPath), replayProtocolScope(snapshot.Manifest)); err != nil {
			return ReplayDayVerificationReport{}, fmt.Errorf("%w: parquet verification failed: %v", archive.ErrIntegrity, err)
		}
	}
	graph, err := r2.VerifyReplayPartGraph(parts, replayProtocolScope(snapshot.Manifest), replayConversion(snapshot.Manifest), maxReplayNodes(len(parts)))
	if err != nil || graph.PartSetRoot != snapshot.Manifest.PartSetRoot || graph.CanonicalRowChainRoot != snapshot.Manifest.CanonicalStreamRowChainRoot {
		return ReplayDayVerificationReport{}, fmt.Errorf("%w: replay part or row-chain root differs", archive.ErrIntegrity)
	}
	manifest := snapshot.Manifest
	return ReplayDayVerificationReport{
		GenesisVerified: false, VerificationScope: VerificationScopeReplayAnchoredDay,
		DatasetID: manifest.DatasetID, ProviderID: snapshot.Scope.ProviderID, ExactSourceSymbol: snapshot.Scope.ExactSourceSymbol,
		DayDefinitionID: manifest.DayDefinitionID,
		Date:            manifest.Date, ReplayContractID: manifest.ReplayContractID, ConversionID: manifest.ConversionID,
		Revision: manifest.Revision, ManifestKey: snapshot.ManifestKey, ManifestSHA256: snapshot.ManifestSHA256,
		RawBindingVerified: true, RawDaySemanticsVerified: true, PartManifestChainVerified: true,
		PartSetRootVerified: true, ParquetSchemaVerified: true, ParquetRowsVerified: true,
		ParquetFileHashesVerified: true, CanonicalRowChainRootVerified: true,
		EmptyDay: len(parts) == 0, PartCount: uint64(len(parts)), RowCount: rowCount,
	}, nil
}
