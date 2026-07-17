package r2

import (
	"encoding/hex"
	"fmt"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

type ReplayPartGraph struct {
	Parts                 []protocol.PartManifest
	PartManifestKeys      []string
	PartSetRoot           [32]byte
	CanonicalRowChainRoot [32]byte
}

// VerifyReplayPartGraph accepts exactly one already ordered chain. It never
// sorts competing nodes or selects a winner from a branch.
func VerifyReplayPartGraph(parts []protocol.PartManifest, scope protocol.ReplayScope, conversion archive.ConversionTuple, maxNodes uint64) (ReplayPartGraph, error) {
	if maxNodes == 0 || uint64(len(parts)) > maxNodes {
		return ReplayPartGraph{}, fmt.Errorf("%w: part graph node limit", ErrResourceLimit)
	}
	if err := scope.Validate(); err != nil {
		return ReplayPartGraph{}, err
	}
	copyParts := append([]protocol.PartManifest(nil), parts...)
	keys := make([]string, len(copyParts))
	seenDigest := make(map[[32]byte]struct{}, len(copyParts))
	for index := range copyParts {
		part := copyParts[index]
		if err := part.Validate(); err != nil {
			return ReplayPartGraph{}, fmt.Errorf("part %d: %w", index, err)
		}
		digest, err := protocol.PartManifestDigest(part)
		if err != nil || part.ManifestSHA256 != ([32]byte{}) && part.ManifestSHA256 != digest {
			return ReplayPartGraph{}, fmt.Errorf("part %d manifest digest mismatch", index)
		}
		if _, duplicate := seenDigest[digest]; duplicate {
			return ReplayPartGraph{}, fmt.Errorf("part graph contains a duplicate or branch")
		}
		seenDigest[digest] = struct{}{}
		if uint64(part.PartSequence) != uint64(index) || !replayPartMatchesScope(part, scope, conversion) {
			return ReplayPartGraph{}, fmt.Errorf("part graph has a gap, branch, or scope contradiction")
		}
		if index == 0 {
			if part.FirstStreamSequence != 0 || part.PreviousManifestSHA256 != nil || part.PreviousRowChainHash != ([32]byte{}) {
				return ReplayPartGraph{}, fmt.Errorf("part graph genesis is invalid")
			}
		} else {
			previous := copyParts[index-1]
			previousDigest, digestErr := protocol.PartManifestDigest(previous)
			if digestErr != nil || part.PreviousManifestSHA256 == nil || *part.PreviousManifestSHA256 != previousDigest || part.PreviousRowChainHash != previous.LastRowChainHash || previous.LastStreamSequence == ^uint64(0) || part.FirstStreamSequence != previous.LastStreamSequence+1 {
				return ReplayPartGraph{}, fmt.Errorf("part graph predecessor is missing or conflicting")
			}
		}
		key, err := protocol.PartManifestKey(part)
		if err != nil {
			return ReplayPartGraph{}, err
		}
		keys[index] = key
		copyParts[index].ManifestSHA256 = digest
	}
	root, err := protocol.PartSetRoot(copyParts)
	if err != nil {
		return ReplayPartGraph{}, err
	}
	var rowRoot [32]byte
	if len(copyParts) != 0 {
		rowRoot = copyParts[len(copyParts)-1].LastRowChainHash
	}
	return ReplayPartGraph{Parts: copyParts, PartManifestKeys: keys, PartSetRoot: root, CanonicalRowChainRoot: rowRoot}, nil
}

func replayPartMatchesScope(part protocol.PartManifest, scope protocol.ReplayScope, conversion archive.ConversionTuple) bool {
	return part.DatasetID == scope.DatasetID && part.DayDefinitionID == scope.DayDefinitionID && part.Date == scope.Date && part.RawDayManifestKey == scope.RawDayManifestKey && part.RawDayManifestSHA256 == scope.RawDayManifestSHA256 && part.ReplayContractID == conversion.ReplayContractID && part.FormatID == conversion.FormatID && part.ConversionID == conversion.ConversionID && part.ConverterBuildID == conversion.ConverterBuildID && part.DependencyLockHash == conversion.DependencyLockHash && part.WriterConfigurationHash == conversion.WriterConfigurationHash && part.TargetPlatformContract == conversion.TargetPlatformContract
}

type ReplayRevisionGraph struct {
	Revisions []protocol.ReplayDayManifest
	Edges     []protocol.ReplayObservedRevisionEdge
}

// VerifyReplayRevisionGraph requires a single, complete, caller-ordered chain
// from revision 1. Duplicate revisions, branches and missing predecessors are
// rejected; the verifier never chooses a winning head.
func VerifyReplayRevisionGraph(revisions []protocol.ReplayDayManifest, maxNodes uint64) (ReplayRevisionGraph, error) {
	if len(revisions) == 0 {
		return ReplayRevisionGraph{}, fmt.Errorf("replay revision graph is empty")
	}
	if maxNodes == 0 || uint64(len(revisions)) > maxNodes {
		return ReplayRevisionGraph{}, fmt.Errorf("%w: replay graph node limit", ErrResourceLimit)
	}
	copyRevisions := append([]protocol.ReplayDayManifest(nil), revisions...)
	edges := make([]protocol.ReplayObservedRevisionEdge, len(copyRevisions))
	seen := make(map[[32]byte]struct{}, len(copyRevisions))
	for index := range copyRevisions {
		manifest := copyRevisions[index]
		if err := manifest.Validate(); err != nil {
			return ReplayRevisionGraph{}, fmt.Errorf("replay revision %d: %w", index, err)
		}
		digest, err := protocol.ReplayDayManifestDigest(manifest)
		if err != nil || manifest.ManifestSHA256 != ([32]byte{}) && manifest.ManifestSHA256 != digest {
			return ReplayRevisionGraph{}, fmt.Errorf("replay revision digest mismatch")
		}
		if _, duplicate := seen[digest]; duplicate {
			return ReplayRevisionGraph{}, fmt.Errorf("replay graph contains a duplicate or branch")
		}
		seen[digest] = struct{}{}
		if manifest.Revision != uint64(index+1) {
			return ReplayRevisionGraph{}, fmt.Errorf("replay graph has a missing revision or branch")
		}
		if index == 0 {
			if manifest.PreviousManifestSHA256 != nil {
				return ReplayRevisionGraph{}, fmt.Errorf("replay graph genesis has a predecessor")
			}
		} else {
			previous := copyRevisions[index-1]
			if err := VerifyReplayRevisionSuccessor(previous, manifest); err != nil {
				return ReplayRevisionGraph{}, fmt.Errorf("replay graph predecessor is missing or conflicting: %w", err)
			}
		}
		copyRevisions[index].ManifestSHA256 = digest
		canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
		if err != nil {
			return ReplayRevisionGraph{}, fmt.Errorf("replay revision canonical bytes: %w", err)
		}
		manifestDigest := hex.EncodeToString(digest[:])
		edge := protocol.ReplayObservedRevisionEdge{Revision: manifest.Revision, CanonicalJSON: string(canonical), ManifestDigest: manifestDigest, PartCount: uint64(len(manifest.PartManifestKeys)), PartSetRoot: hex.EncodeToString(manifest.PartSetRoot[:]), CanonicalStreamRowChainRoot: hex.EncodeToString(manifest.CanonicalStreamRowChainRoot[:])}
		if manifest.PreviousManifestSHA256 != nil {
			previous := hex.EncodeToString(manifest.PreviousManifestSHA256[:])
			edge.PreviousManifestDigest = &previous
		}
		edges[index] = edge
	}
	return ReplayRevisionGraph{Revisions: copyRevisions, Edges: edges}, nil
}

// VerifyReplayRevisionSuccessor verifies the adjacent edge needed when a
// publisher has only the immediately previous replay manifest locally.
// Complete remote observations still use VerifyReplayRevisionGraph, which
// requires the chain from revision 1 and never selects a winning branch.
func VerifyReplayRevisionSuccessor(previous, current protocol.ReplayDayManifest) error {
	if previous.M0EmptyPartsCompatibility || current.M0EmptyPartsCompatibility {
		return fmt.Errorf("M0 compatibility manifest cannot be a replay revision predecessor or successor")
	}
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("invalid replay predecessor: %w", err)
	}
	if err := current.Validate(); err != nil {
		return fmt.Errorf("invalid replay successor: %w", err)
	}
	previousDigest, err := protocol.ReplayDayManifestDigest(previous)
	if err != nil {
		return fmt.Errorf("replay predecessor digest: %w", err)
	}
	if previous.ManifestSHA256 != ([32]byte{}) && previous.ManifestSHA256 != previousDigest {
		return fmt.Errorf("replay predecessor digest does not match canonical bytes")
	}
	currentDigest, err := protocol.ReplayDayManifestDigest(current)
	if err != nil {
		return fmt.Errorf("replay successor digest: %w", err)
	}
	if current.ManifestSHA256 != ([32]byte{}) && current.ManifestSHA256 != currentDigest {
		return fmt.Errorf("replay successor digest does not match canonical bytes")
	}
	if previous.Revision == 0 || previous.Revision == ^uint64(0) || current.Revision != previous.Revision+1 {
		return fmt.Errorf("replay successor revision is not adjacent")
	}
	if current.PreviousManifestSHA256 == nil || *current.PreviousManifestSHA256 != previousDigest {
		return fmt.Errorf("replay successor predecessor digest is missing or conflicting")
	}
	if !sameReplayStableIdentity(previous, current) {
		return fmt.Errorf("replay successor changed stable identity")
	}
	if previous.RawDayManifestKey == current.RawDayManifestKey || previous.RawDayManifestSHA256 == current.RawDayManifestSHA256 {
		return fmt.Errorf("replay successor did not bind a new raw revision")
	}
	return nil
}

func sameReplayStableIdentity(left, right protocol.ReplayDayManifest) bool {
	return left.ManifestID == right.ManifestID && left.DatasetID == right.DatasetID && left.DayDefinitionID == right.DayDefinitionID && left.Date == right.Date && left.ReplayContractID == right.ReplayContractID && left.FormatID == right.FormatID && left.ConversionID == right.ConversionID && left.ConverterBuildID == right.ConverterBuildID && left.DependencyLockHash == right.DependencyLockHash && left.WriterConfigurationHash == right.WriterConfigurationHash && left.TargetPlatformContract == right.TargetPlatformContract
}
