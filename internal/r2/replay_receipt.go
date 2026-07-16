package r2

import (
	"context"
	"encoding/hex"
	"fmt"

	"tick-data-platform/internal/protocol"
)

const ReplayReceiptVersion = "replay-verification-receipt-v1"

type ReplayVerificationReceipt struct {
	ReceiptVersion            string
	BundleCanonical           []byte
	BundleDigest              [32]byte
	FinalObservationDigest    [32]byte
	FinalObservationCanonical []byte
	Claim                     protocol.ReplayPublicationClaim
	PartSetRoot               string
	CanonicalRowChainRoot     string
	Limits                    protocol.ReplayPublicationLimits
	VerificationComplete      bool
}

func BuildReplayVerificationReceipt(bundle ReplayPublicationBundle, finalObservation ReplayRemoteObservation) (ReplayVerificationReceipt, error) {
	if err := verifySealedReplayBundle(bundle); err != nil {
		return ReplayVerificationReceipt{}, err
	}
	if !finalObservation.Complete || finalObservation.BundleDigest != bundle.Digest || finalObservation.FinalObservation == nil || finalObservation.FinalDigest == ([32]byte{}) || !replayObservationAllExact(finalObservation) {
		return ReplayVerificationReceipt{}, fmt.Errorf("complete Exact final observation is required")
	}
	canonical, err := protocol.ReplayFinalObservationCanonicalJSON(*finalObservation.FinalObservation, bundle.Contract)
	if err != nil {
		return ReplayVerificationReceipt{}, err
	}
	digest, err := protocol.ReplayFinalObservationDigest(*finalObservation.FinalObservation, bundle.Contract)
	if err != nil || digest != finalObservation.FinalDigest {
		return ReplayVerificationReceipt{}, fmt.Errorf("final observation digest mismatch")
	}
	return ReplayVerificationReceipt{
		ReceiptVersion: ReplayReceiptVersion, BundleCanonical: append([]byte(nil), bundle.CanonicalBytes...), BundleDigest: bundle.Digest,
		FinalObservationDigest: digest, FinalObservationCanonical: append([]byte(nil), canonical...),
		Claim: bundle.Contract.Claim, PartSetRoot: bundle.Contract.PartSetRoot,
		CanonicalRowChainRoot: bundle.Contract.CanonicalStreamRowChainRoot,
		Limits:                bundle.Contract.Limits,
		VerificationComplete:  true,
	}, nil
}

func (r ReplayVerificationReceipt) CanonicalJSON() ([]byte, error) {
	if err := validateReplayReceipt(r); err != nil {
		return nil, err
	}
	limits := map[string]any{
		"max_graph_nodes": r.Limits.MaxGraphNodes, "max_list_objects": r.Limits.MaxListObjects,
		"max_metadata_object_bytes": r.Limits.MaxMetadataObjectBytes, "max_observation_bytes": r.Limits.MaxObservationBytes,
		"max_observation_requests": r.Limits.MaxObservationRequests, "max_parquet_object_bytes": r.Limits.MaxParquetObjectBytes,
		"max_parts": r.Limits.MaxParts, "max_publication_rounds": r.Limits.MaxPublicationRounds,
		"max_total_metadata_bytes": r.Limits.MaxTotalMetadataBytes, "max_total_parquet_bytes": r.Limits.MaxTotalParquetBytes,
	}
	claim := map[string]any{
		"canonical_json": r.Claim.CanonicalJSON, "domain_digest": r.Claim.DomainDigest, "full_key": r.Claim.FullKey,
	}
	return protocol.CanonicalJSON(map[string]any{
		"bundle_canonical_json": string(r.BundleCanonical), "bundle_digest": hex.EncodeToString(r.BundleDigest[:]),
		"canonical_stream_row_chain_root": r.CanonicalRowChainRoot,
		"claim":                           claim, "final_observation_canonical_json": string(r.FinalObservationCanonical),
		"final_observation_digest": hex.EncodeToString(r.FinalObservationDigest[:]),
		"limits":                   limits, "part_set_root": r.PartSetRoot, "receipt_version": r.ReceiptVersion,
		"verification_complete": r.VerificationComplete,
	})
}

func validateReplayReceipt(receipt ReplayVerificationReceipt) error {
	if receipt.ReceiptVersion != ReplayReceiptVersion || !receipt.VerificationComplete || receipt.BundleDigest == ([32]byte{}) || receipt.FinalObservationDigest == ([32]byte{}) || len(receipt.BundleCanonical) == 0 || len(receipt.FinalObservationCanonical) == 0 {
		return fmt.Errorf("replay verification receipt is incomplete")
	}
	if receipt.Claim.CanonicalJSON == "" || receipt.Claim.DomainDigest == "" || receipt.Claim.FullKey == "" || receipt.PartSetRoot == "" || receipt.CanonicalRowChainRoot == "" {
		return fmt.Errorf("replay verification receipt binding is incomplete")
	}
	bundle, bundleDigest, err := protocol.VerifyReplayPublicationBundle(receipt.BundleCanonical)
	if err != nil || bundleDigest != receipt.BundleDigest {
		return fmt.Errorf("replay receipt bundle binding is invalid")
	}
	_, finalDigest, err := protocol.VerifyReplayFinalObservation(receipt.FinalObservationCanonical, bundle)
	if err != nil || finalDigest != receipt.FinalObservationDigest {
		return fmt.Errorf("replay receipt final observation binding is invalid")
	}
	if receipt.Claim != bundle.Claim || receipt.PartSetRoot != bundle.PartSetRoot || receipt.CanonicalRowChainRoot != bundle.CanonicalStreamRowChainRoot || receipt.Limits != bundle.Limits {
		return fmt.Errorf("replay receipt contract binding differs from canonical bundle")
	}
	return nil
}

type ReplayReceiptStore interface {
	SaveNoClobber(ctx context.Context, path string, receipt ReplayVerificationReceipt) error
}

type FileReplayReceiptStore struct{}

func (FileReplayReceiptStore) SaveNoClobber(ctx context.Context, path string, receipt ReplayVerificationReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return SaveReplayVerificationReceipt(path, receipt)
}

func SaveReplayVerificationReceipt(path string, receipt ReplayVerificationReceipt) error {
	canonical, err := receipt.CanonicalJSON()
	if err != nil {
		return err
	}
	return saveNoClobberBytes(path, canonical, ".replay-verification-receipt-*.tmp")
}
