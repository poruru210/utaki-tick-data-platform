package r2

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

const replayObjectIDDomain = "tick-data-platform/replay-object-id/v1\x00"

type ReplayObjectID string

type ReplayPublicationBundleInput struct {
	Layout                 Layout
	Conversion             parquet.ConversionSpec
	Limits                 protocol.ReplayPublicationLimits
	RawManifest            []byte
	RawObjectPaths         map[string]string
	Parts                  []parquet.PartArtifact
	PartManifests          [][]byte
	ReplayManifest         []byte
	PreviousReplayManifest []byte
	ReceiptPath            string
}

type ReplayLocalArtifact struct {
	Kind           string
	Path           string
	CanonicalBytes []byte
	Bytes          uint64
	Digest         string
	ContentSHA256  string
}

type ReplayLocalSources struct {
	Artifacts   map[ReplayObjectID]ReplayLocalArtifact
	ReceiptPath string
}

// ReplayPublicationBundle keeps local sources outside the Protocol V1
// identity. CanonicalBytes and Digest are derived only from Contract.
type ReplayPublicationBundle struct {
	Contract       protocol.ReplayPublicationBundle
	CanonicalBytes []byte
	Digest         [32]byte
	Layout         Layout
	LocalSources   ReplayLocalSources
}

func SealReplayPublicationBundle(input ReplayPublicationBundleInput) (ReplayPublicationBundle, error) {
	if err := protocol.ValidateReplayPublicationLimits(input.Limits); err != nil {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: invalid replay publication limits: %v", ErrResourceLimit, err)
	}
	if err := input.Conversion.Validate(); err != nil {
		return ReplayPublicationBundle{}, err
	}

	rawManifest, err := archive.VerifyRawDayManifest(input.RawManifest)
	if err != nil {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: raw manifest verification failed: %v", archive.ErrIntegrity, err)
	}
	rawCanonical, err := archive.ManifestCanonicalJSON(rawManifest)
	if err != nil || !bytes.Equal(rawCanonical, input.RawManifest) {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: raw manifest bytes are not canonical", archive.ErrIntegrity)
	}
	if err := manifestMatchesScope(rawManifest, input.Layout.Scope); err != nil {
		return ReplayPublicationBundle{}, err
	}
	if err := archive.VerifyRawDaySnapshot(rawManifest, input.RawObjectPaths, input.Layout.Scope); err != nil {
		return ReplayPublicationBundle{}, err
	}
	rawRelative, err := archive.RawDayManifestRelativeKey(input.Layout.Scope, rawManifest)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	replayScope := protocol.ReplayScope{
		DatasetID:       input.Layout.Scope.DatasetID,
		DayDefinitionID: input.Layout.Scope.DayDefinitionID, Date: rawManifest.Date,
		ReplayContractID: input.Conversion.ReplayContractID, ConversionID: input.Conversion.ConversionID,
		RawDayManifestKey: rawRelative, RawDayManifestSHA256: rawManifest.ManifestSHA256,
	}
	if err := replayScope.Validate(); err != nil {
		return ReplayPublicationBundle{}, err
	}
	conversion, err := archive.ConversionTupleFromSpec(input.Conversion)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}

	if len(input.Parts) != len(input.PartManifests) {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: Parquet and part manifest counts differ", archive.ErrIntegrity)
	}
	verifiedParts := make([]protocol.PartManifest, 0, len(input.Parts))
	partCanonical := make([][]byte, 0, len(input.Parts))
	var previousPart *protocol.PartManifest
	for index, artifact := range input.Parts {
		if int(artifact.PartSequence) != index {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: Parquet part sequence is not contiguous", archive.ErrIntegrity)
		}
		if err := parquet.VerifyPartFile(artifact.Path, artifact, replayScope); err != nil {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: Parquet part %d failed reopen verification: %v", archive.ErrIntegrity, index, err)
		}
		partInput, err := archive.PartManifestInputFromArtifact(replayScope, conversion, artifact)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		expected, err := archive.BuildPartManifest(partInput, previousPart)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		canonical, err := protocol.PartManifestCanonicalJSON(expected)
		if err != nil || !bytes.Equal(canonical, input.PartManifests[index]) {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: part manifest %d differs from verified Parquet input", archive.ErrIntegrity, index)
		}
		partKey, err := protocol.PartManifestKey(expected)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		verified, err := archive.VerifyPartManifestObject(input.PartManifests[index], partKey, expected.ManifestSHA256)
		if err != nil || verified.ManifestSHA256 != expected.ManifestSHA256 {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: part manifest %d failed Protocol verification", archive.ErrIntegrity, index)
		}
		verifiedParts = append(verifiedParts, expected)
		partCanonical = append(partCanonical, append([]byte(nil), canonical...))
		previousPart = &verifiedParts[len(verifiedParts)-1]
	}

	replayManifest, err := protocol.VerifyReplayDayManifest(input.ReplayManifest)
	if err != nil || replayManifest.M0EmptyPartsCompatibility {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: replay manifest is not a strict M3 manifest", archive.ErrIntegrity)
	}
	var previousReplay *protocol.ReplayDayManifest
	if len(input.PreviousReplayManifest) != 0 {
		if uint64(len(input.PreviousReplayManifest)) > input.Limits.MaxMetadataObjectBytes {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: replay predecessor exceeds metadata object limit", ErrResourceLimit)
		}
		value, verifyErr := protocol.VerifyReplayDayManifest(input.PreviousReplayManifest)
		if verifyErr != nil || value.M0EmptyPartsCompatibility {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: replay predecessor is invalid", archive.ErrIntegrity)
		}
		previousReplay = &value
	}
	if replayManifest.Revision > 1 && previousReplay == nil || replayManifest.Revision == 1 && previousReplay != nil {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: replay predecessor input does not match revision", archive.ErrIntegrity)
	}
	expectedReplay, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{
		Scope: replayScope, Conversion: conversion, CompletenessStatus: replayManifest.CompletenessStatus,
		Revision: replayManifest.Revision, Previous: previousReplay, Parts: verifiedParts,
		CanonicalStreamRowChainRoot: replayManifest.CanonicalStreamRowChainRoot,
	})
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	expectedReplayCanonical, err := protocol.ReplayDayManifestCanonicalJSON(expectedReplay)
	if err != nil || !bytes.Equal(expectedReplayCanonical, input.ReplayManifest) {
		return ReplayPublicationBundle{}, fmt.Errorf("%w: replay manifest differs from verified part graph", archive.ErrIntegrity)
	}
	replayRelative, err := protocol.ReplayDayManifestKey(expectedReplay)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	if _, err := archive.VerifyReplayDayManifestObject(input.ReplayManifest, replayRelative, replayScope, conversion, verifiedParts, previousReplay, expectedReplay.CanonicalStreamRowChainRoot); err != nil {
		return ReplayPublicationBundle{}, err
	}

	claim, err := NewPublisherClaim(input.Layout.Scope)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	claimCanonical, err := claim.CanonicalJSON()
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	claimDigest, err := claim.Digest()
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	claimKey, err := input.Layout.ClaimKey(claim.PublisherEpoch)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	configHash, err := input.Layout.Scope.ConfigHash()
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	rawFullKey, err := input.Layout.ManifestKey(rawManifest)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	localSources := ReplayLocalSources{
		Artifacts: make(map[ReplayObjectID]ReplayLocalArtifact), ReceiptPath: input.ReceiptPath,
	}
	rawManifestID := makeReplayObjectID("raw_manifest", rawManifest.Revision, rawRelative)
	localSources.Artifacts[rawManifestID] = ReplayLocalArtifact{
		Kind: "raw_manifest", CanonicalBytes: append([]byte(nil), input.RawManifest...),
		Bytes: uint64(len(input.RawManifest)), Digest: hex.EncodeToString(rawManifest.ManifestSHA256[:]),
		ContentSHA256: contentSHA256Hex(input.RawManifest),
	}
	rawObjects := make([]protocol.ReplayPublicationRawObject, 0, len(rawManifest.ChainObjects))
	for _, descriptor := range rawManifest.ChainObjects {
		fullKey, err := input.Layout.RemoteKey(descriptor.Key)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		digest := hex.EncodeToString(descriptor.SHA256[:])
		rawObjects = append(rawObjects, protocol.ReplayPublicationRawObject{
			Bytes: descriptor.Bytes, FullKey: fullKey, RelativeKey: descriptor.Key,
			SHA256: digest,
		})
		objectID := makeReplayObjectID("raw_object", 0, descriptor.Key)
		localSources.Artifacts[objectID] = ReplayLocalArtifact{
			Kind: "raw_object", Path: input.RawObjectPaths[descriptor.Key], Bytes: descriptor.Bytes, Digest: digest, ContentSHA256: digest,
		}
	}
	sort.Slice(rawObjects, func(i, j int) bool { return rawObjects[i].FullKey < rawObjects[j].FullKey })

	parquetObjects := make([]protocol.ReplayPublicationParquetObject, 0, len(input.Parts))
	partManifests := make([]protocol.ReplayPublicationPartManifest, 0, len(verifiedParts))
	for index, part := range verifiedParts {
		artifact := input.Parts[index]
		parquetFullKey, err := input.Layout.ReplayPartObjectKey(part)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		parquetID := makeReplayObjectID("parquet", uint64(part.PartSequence), part.PartKey)
		parquetObjects = append(parquetObjects, protocol.ReplayPublicationParquetObject{
			Bytes: artifact.PartBytes, FirstStreamSequence: artifact.FirstStreamSequence,
			FullKey: parquetFullKey, LastStreamSequence: artifact.LastStreamSequence,
			ObjectID: string(parquetID), RelativeKey: part.PartKey,
			SHA256: hex.EncodeToString(artifact.PartSHA256[:]),
		})
		localSources.Artifacts[parquetID] = ReplayLocalArtifact{
			Kind: "parquet", Path: artifact.Path, Bytes: artifact.PartBytes,
			Digest: hex.EncodeToString(artifact.PartSHA256[:]), ContentSHA256: hex.EncodeToString(artifact.PartSHA256[:]),
		}

		partKey, err := protocol.PartManifestKey(part)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		partFullKey, err := input.Layout.ReplayPartManifestKey(part)
		if err != nil {
			return ReplayPublicationBundle{}, err
		}
		partID := makeReplayObjectID("part_manifest", uint64(part.PartSequence), partKey)
		partManifests = append(partManifests, protocol.ReplayPublicationPartManifest{
			Bytes: uint64(len(partCanonical[index])), DomainDigest: hex.EncodeToString(part.ManifestSHA256[:]),
			FullKey: partFullKey, ObjectID: string(partID), PartSequence: uint64(part.PartSequence),
			RelativeKey: partKey,
		})
		localSources.Artifacts[partID] = ReplayLocalArtifact{
			Kind: "part_manifest", CanonicalBytes: append([]byte(nil), partCanonical[index]...),
			Bytes: uint64(len(partCanonical[index])), Digest: hex.EncodeToString(part.ManifestSHA256[:]),
			ContentSHA256: contentSHA256Hex(partCanonical[index]),
		}
	}

	replayFullKey, err := input.Layout.ReplayDayManifestKey(expectedReplay)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	replayID := makeReplayObjectID("replay_manifest", expectedReplay.Revision, replayRelative)
	localSources.Artifacts[replayID] = ReplayLocalArtifact{
		Kind: "replay_manifest", CanonicalBytes: append([]byte(nil), input.ReplayManifest...),
		Bytes: uint64(len(input.ReplayManifest)), Digest: hex.EncodeToString(expectedReplay.ManifestSHA256[:]),
		ContentSHA256: contentSHA256Hex(input.ReplayManifest),
	}

	contract := protocol.ReplayPublicationBundle{
		BundleVersion:               protocol.ReplayPublicationBundleVersion,
		CanonicalStreamRowChainRoot: hex.EncodeToString(expectedReplay.CanonicalStreamRowChainRoot[:]),
		Claim: protocol.ReplayPublicationClaim{
			CanonicalJSON: string(claimCanonical), DomainDigest: hex.EncodeToString(claimDigest[:]), FullKey: claimKey,
		},
		Conversion: protocol.ReplayPublicationConversion{
			ConversionID: input.Conversion.ConversionID, ConverterBuildID: input.Conversion.ConverterBuildID,
			DependencyLockHash: hex.EncodeToString(input.Conversion.DependencyLockHash[:]), FormatID: input.Conversion.FormatID,
			MaxCanonicalBytesPerPart: input.Conversion.MaxCanonicalBytesPerPart,
			MaxRowsPerPart:           input.Conversion.MaxRowsPerPart, MaxRowsPerRowGroup: input.Conversion.MaxRowsPerRowGroup,
			ReplayContractID: input.Conversion.ReplayContractID, TargetPlatformContract: input.Conversion.TargetPlatformContract,
			WriterConfigurationHash: hex.EncodeToString(input.Conversion.WriterConfigurationHash[:]),
		},
		Limits: input.Limits, ParquetObjects: parquetObjects, PartManifests: partManifests,
		PartSetRoot: hex.EncodeToString(expectedReplay.PartSetRoot[:]),
		RawManifest: protocol.ReplayPublicationRawManifest{
			Bytes: uint64(len(input.RawManifest)), DomainDigest: hex.EncodeToString(rawManifest.ManifestSHA256[:]),
			FullKey: rawFullKey, RelativeKey: rawRelative, Revision: rawManifest.Revision,
		},
		RawObjects: rawObjects,
		ReplayManifest: protocol.ReplayPublicationReplayManifest{
			Bytes: uint64(len(input.ReplayManifest)), DomainDigest: hex.EncodeToString(expectedReplay.ManifestSHA256[:]),
			FullKey: replayFullKey, RelativeKey: replayRelative, Revision: expectedReplay.Revision,
		},
		Scope: protocol.ReplayPublicationScope{
			BrokerServerFingerprint: input.Layout.Scope.BrokerServerFingerprint,
			DatasetID:               input.Layout.Scope.DatasetID,
			Date:                    rawManifest.Date, DayDefinitionID: input.Layout.Scope.DayDefinitionID,
			ExactSourceSymbol: input.Layout.Scope.ExactSourceSymbol,
			ImmutablePrefix:   input.Layout.ImmutableScopePrefix(), ProviderID: input.Layout.Scope.ProviderID,
			PublisherEpoch: input.Layout.Scope.PublisherEpoch, PublisherID: input.Layout.Scope.PublisherID,
			ScopeConfigHash: hex.EncodeToString(configHash[:]),
			ScopeKey:        claim.ScopeKey, SettlePolicy: input.Layout.Scope.SettlePolicy, StableFeedID: input.Layout.Scope.StableFeedID,
		},
	}
	var budgetErr error
	if previousReplay == nil || expectedReplay.Revision == 2 {
		revisions := []protocol.ReplayDayManifest{expectedReplay}
		if previousReplay != nil {
			revisions = []protocol.ReplayDayManifest{*previousReplay, expectedReplay}
		}
		revisionGraph, graphErr := VerifyReplayRevisionGraph(revisions, contract.Limits.MaxGraphNodes)
		if graphErr != nil {
			return ReplayPublicationBundle{}, fmt.Errorf("complete replay graph is unavailable before lock: %w", graphErr)
		}
		edges, edgeErr := replayObservedRevisionEdges(input.Layout, revisionGraph.Edges, revisionGraph.Revisions)
		if edgeErr != nil {
			return ReplayPublicationBundle{}, edgeErr
		}
		budgetErr = protocol.ReplayFinalObservationBudgetFeasible(contract, edges)
	} else {
		if err := VerifyReplayRevisionSuccessor(*previousReplay, expectedReplay); err != nil {
			return ReplayPublicationBundle{}, fmt.Errorf("replay predecessor is not the immediate successor input: %w", err)
		}
		budgetErr = protocol.ReplayFinalObservationBudgetFeasibleForRevision(contract, expectedReplay.Revision)
	}
	if budgetErr != nil {
		if protocol.ErrorCodeOf(budgetErr) == protocol.ErrResourceLimit {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: final observation is infeasible before lock: %v", ErrResourceLimit, budgetErr)
		}
		return ReplayPublicationBundle{}, fmt.Errorf("final observation is infeasible before lock: %w", budgetErr)
	}
	canonical, err := protocol.ReplayPublicationBundleCanonicalJSON(contract)
	if err != nil {
		if protocol.ErrorCodeOf(err) == protocol.ErrResourceLimit {
			return ReplayPublicationBundle{}, fmt.Errorf("%w: %v", ErrResourceLimit, err)
		}
		return ReplayPublicationBundle{}, fmt.Errorf("seal replay publication bundle: %w", err)
	}
	digest, err := protocol.ReplayPublicationBundleDigest(contract)
	if err != nil {
		return ReplayPublicationBundle{}, err
	}
	verified, verifiedDigest, err := protocol.VerifyReplayPublicationBundle(canonical)
	if err != nil || verifiedDigest != digest {
		return ReplayPublicationBundle{}, fmt.Errorf("sealed replay publication bundle failed Protocol verification: %v", err)
	}
	return ReplayPublicationBundle{
		Contract: verified, CanonicalBytes: append([]byte(nil), canonical...), Digest: digest, Layout: input.Layout, LocalSources: localSources,
	}, nil
}

func makeReplayObjectID(kind string, sequence uint64, relativeKey string) ReplayObjectID {
	input := []byte(replayObjectIDDomain + kind + "\x00" + strconv.FormatUint(sequence, 10) + "\x00" + relativeKey)
	digest := sha256.Sum256(input)
	return ReplayObjectID(hex.EncodeToString(digest[:]))
}

func replayRawObjectID(object protocol.ReplayPublicationRawObject) ReplayObjectID {
	return makeReplayObjectID("raw_object", 0, object.RelativeKey)
}

func replayManifestObjectID(bundle protocol.ReplayPublicationBundle) ReplayObjectID {
	return makeReplayObjectID("replay_manifest", bundle.ReplayManifest.Revision, bundle.ReplayManifest.RelativeKey)
}

func contentSHA256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
