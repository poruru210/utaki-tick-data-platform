package retention

import (
	"context"
	"fmt"
	"sort"

	"tick-data-platform/internal/operations"
)

// RemoteFact is the bounded read result for one local artifact. A source may
// return a non-Exact class without an error; the planner will then produce a
// blocked candidate and zero delete action.
type RemoteFact struct {
	Class            string
	Proof            *RetentionProof
	CoverageVerified bool
}

// ReadOnlyRemoteObserver exposes no put, delete, list-without-budget, or
// caller-supplied path capability. Implementations must perform a fresh
// aggregate observation for each artifact.
type ReadOnlyRemoteObserver interface {
	Observe(ctx context.Context, artifact LocalArtifact, limits ProofLimits) (RemoteFact, error)
}

// ProofObservationBudget is shared across all candidates in one observation
// run. Per-object ProofLimits remain part of the proof contract, while this
// mutable counter enforces the aggregate run budget.
type ProofObservationBudget struct {
	limits        ProofLimits
	objects       uint64
	bytes         uint64
	manifestNodes uint64
}

func NewProofObservationBudget(limits ProofLimits) (*ProofObservationBudget, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &ProofObservationBudget{limits: limits}, nil
}

func (b *ProofObservationBudget) chargeObjects(count uint64) bool {
	if count > b.limits.MaxProofObjects-b.objects {
		return false
	}
	b.objects += count
	return true
}

// consumeBytes accounts for bytes actually returned by a bounded remote
// reader, including bytes read before a short-read, close, size, or content
// mismatch is classified. Saturating the counter keeps a failed observation
// fail-closed without allowing a later candidate to reuse exhausted budget.
func (b *ProofObservationBudget) consumeBytes(count uint64) bool {
	if count > b.limits.MaxProofBytes-b.bytes {
		b.bytes = b.limits.MaxProofBytes
		return false
	}
	b.bytes += count
	return true
}

func (b *ProofObservationBudget) chargeManifestNodes(count uint64) bool {
	if count > b.limits.MaxManifestNodes-b.manifestNodes {
		return false
	}
	b.manifestNodes += count
	return true
}

type BudgetedReadOnlyRemoteObserver interface {
	ObserveWithBudget(ctx context.Context, artifact LocalArtifact, limits ProofLimits, budget *ProofObservationBudget) (RemoteFact, error)
}

// ObserveCandidates converts local identities and bounded fresh remote facts
// into planner inputs. The order is stable regardless of local directory or
// remote list order.
func ObserveCandidates(ctx context.Context, observer ReadOnlyRemoteObserver, artifacts []LocalArtifact, limits operations.ResourceLimits) ([]CandidateFact, error) {
	if observer == nil {
		return nil, fmt.Errorf("read-only retention observer is nil")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if uint64(len(artifacts)) > limits.MaxPruneCandidates {
		return nil, fmt.Errorf("retention candidates exceed configured limit")
	}
	proofLimits := ProofLimits{MaxProofObjects: limits.MaxProofObjects, MaxProofBytes: limits.MaxProofBytes, MaxManifestNodes: limits.MaxManifestNodes}
	budget, err := NewProofObservationBudget(proofLimits)
	if err != nil {
		return nil, err
	}
	ordered := append([]LocalArtifact(nil), artifacts...)
	sort.Slice(ordered, func(i, j int) bool {
		left, leftErr := ordered[i].StableID()
		right, rightErr := ordered[j].StableID()
		if leftErr != nil || rightErr != nil {
			return ordered[i].TrustedPath < ordered[j].TrustedPath
		}
		return left < right
	})
	budgetedObserver, hasAggregateBudget := observer.(BudgetedReadOnlyRemoteObserver)
	if !hasAggregateBudget && len(ordered) > 1 {
		return nil, fmt.Errorf("read-only observer does not expose an aggregate proof budget")
	}
	result := make([]CandidateFact, 0, len(ordered))
	for _, artifact := range ordered {
		var fact RemoteFact
		if hasAggregateBudget {
			fact, err = budgetedObserver.ObserveWithBudget(ctx, artifact, proofLimits, budget)
		} else {
			fact, err = observer.Observe(ctx, artifact, proofLimits)
		}
		if err != nil {
			return nil, fmt.Errorf("observe retention artifact %s: %w", artifact.TrustedPath, err)
		}
		result = append(result, CandidateFact{Artifact: artifact, Proof: fact.Proof, FreshRemote: fact.Class == RemoteObservationExact, CoverageVerified: fact.CoverageVerified})
	}
	return result, nil
}
