package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ReplayPublicationBundleVersion   = "replay-publication-bundle-v1"
	ReplayFinalObservationVersion    = "replay-publication-final-observation-v1"
	ReplayPublicationBundleDomain    = "tick-data-platform/replay-publication-bundle/v1\x00"
	ReplayFinalObservationDomain     = "tick-data-platform/replay-publication-final-observation/v1\x00"
	PublisherClaimDomain             = "tick-data-platform/publisher-claim/v1\x00"
	PublisherClaimVersion            = "publisher-claim-v1"
	ReplayDerivativeDigestSHA256     = "sha256"
	ReplayDerivativeDigestPart       = "part-manifest-v1"
	ReplayDerivativeDigestReplay     = "replay-day-manifest-v1"
	ReplayDerivativeKindParquet      = "parquet"
	ReplayDerivativeKindPartManifest = "part_manifest"
	ReplayDerivativeKindReplay       = "replay_manifest"
)

const (
	ErrInvalidCanonicalJSON  ErrorCode = "INVALID_CANONICAL_JSON"
	ErrDuplicateField        ErrorCode = "DUPLICATE_FIELD"
	ErrUnknownField          ErrorCode = "UNKNOWN_FIELD"
	ErrZeroDigest            ErrorCode = "ZERO_DIGEST"
	ErrWrongDomain           ErrorCode = "WRONG_DOMAIN"
	ErrWrongKey              ErrorCode = "WRONG_KEY"
	ErrScopeCollision        ErrorCode = "SCOPE_COLLISION"
	ErrRawClaimMissing       ErrorCode = "RAW_CLAIM_MISSING"
	ErrResourceLimit         ErrorCode = "RESOURCE_LIMIT"
	ErrIncompleteObservation ErrorCode = "INCOMPLETE_OBSERVATION"
)

// ReplayPublicationScope is the secret-free trusted Layout scope bound into a
// replay publication bundle. The prefix is derived by Layout; Protocol V1 only
// verifies that every full key is under the exact prefix.
type ReplayPublicationScope struct {
	BrokerServerFingerprint string `json:"broker_server_fingerprint"`
	DatasetID               string `json:"dataset_id"`
	Date                    string `json:"date"`
	DayDefinitionID         string `json:"day_definition_id"`
	ExactSourceSymbol       string `json:"exact_source_symbol"`
	ImmutablePrefix         string `json:"immutable_prefix"`
	ProviderID              string `json:"provider_id"`
	PublisherEpoch          uint64 `json:"publisher_epoch"`
	PublisherID             string `json:"publisher_id"`
	ScopeConfigHash         string `json:"scope_config_hash"`
	ScopeKey                string `json:"scope_key"`
	SettlePolicy            string `json:"settle_policy"`
	StableFeedID            string `json:"stable_feed_id"`
}

type ReplayPublicationClaim struct {
	CanonicalJSON string `json:"canonical_json"`
	DomainDigest  string `json:"domain_digest"`
	FullKey       string `json:"full_key"`
}

type ReplayPublicationConversion struct {
	ConversionID             string `json:"conversion_id"`
	ConverterBuildID         string `json:"converter_build_id"`
	DependencyLockHash       string `json:"dependency_lock_hash"`
	FormatID                 string `json:"format_id"`
	MaxCanonicalBytesPerPart uint64 `json:"max_canonical_bytes_per_part"`
	MaxRowsPerPart           uint64 `json:"max_rows_per_part"`
	MaxRowsPerRowGroup       uint64 `json:"max_rows_per_row_group"`
	ReplayContractID         string `json:"replay_contract_id"`
	TargetPlatformContract   string `json:"target_platform_contract"`
	WriterConfigurationHash  string `json:"writer_configuration_hash"`
}

// ReplayPublicationLimits is canonical policy input. Every value is nonzero
// and no value may exceed the Protocol V1 implementation bound below.
type ReplayPublicationLimits struct {
	MaxGraphNodes          uint64 `json:"max_graph_nodes"`
	MaxListObjects         uint64 `json:"max_list_objects"`
	MaxMetadataObjectBytes uint64 `json:"max_metadata_object_bytes"`
	MaxObservationBytes    uint64 `json:"max_observation_bytes"`
	MaxObservationRequests uint64 `json:"max_observation_requests"`
	MaxParquetObjectBytes  uint64 `json:"max_parquet_object_bytes"`
	MaxParts               uint64 `json:"max_parts"`
	MaxPublicationRounds   uint64 `json:"max_publication_rounds"`
	MaxTotalMetadataBytes  uint64 `json:"max_total_metadata_bytes"`
	MaxTotalParquetBytes   uint64 `json:"max_total_parquet_bytes"`
}

var ReplayPublicationImplementationBounds = ReplayPublicationLimits{
	MaxGraphNodes:          50_000,
	MaxListObjects:         50_000,
	MaxMetadataObjectBytes: 16_777_216,
	MaxObservationBytes:    70_368_744_177_664,
	MaxObservationRequests: 100_000,
	MaxParquetObjectBytes:  1_099_511_627_776,
	MaxParts:               10_000,
	MaxPublicationRounds:   20_002,
	MaxTotalMetadataBytes:  268_435_456,
	MaxTotalParquetBytes:   17_592_186_044_416,
}

// RequiredReplayPublicationRounds is the minimum number of fresh-observation
// rounds for one action per round: one action for each Parquet object, one for
// each part manifest, one for the replay manifest, and a terminal observation.
func RequiredReplayPublicationRounds(partCount uint64) (uint64, error) {
	if partCount > (^uint64(0)-2)/2 {
		return 0, newError(ErrResourceLimit, "publication round count overflows")
	}
	return partCount*2 + 2, nil
}

type ReplayPublicationRawManifest struct {
	Bytes        uint64 `json:"bytes"`
	DomainDigest string `json:"domain_digest"`
	FullKey      string `json:"full_key"`
	RelativeKey  string `json:"relative_key"`
	Revision     uint64 `json:"revision"`
}

type ReplayPublicationRawObject struct {
	Bytes       uint64 `json:"bytes"`
	FullKey     string `json:"full_key"`
	RelativeKey string `json:"relative_key"`
	SHA256      string `json:"sha256"`
}

type ReplayPublicationParquetObject struct {
	Bytes               uint64 `json:"bytes"`
	FirstStreamSequence uint64 `json:"first_stream_sequence"`
	FullKey             string `json:"full_key"`
	LastStreamSequence  uint64 `json:"last_stream_sequence"`
	ObjectID            string `json:"object_id"`
	RelativeKey         string `json:"relative_key"`
	SHA256              string `json:"sha256"`
}

type ReplayPublicationPartManifest struct {
	Bytes        uint64 `json:"bytes"`
	DomainDigest string `json:"domain_digest"`
	FullKey      string `json:"full_key"`
	ObjectID     string `json:"object_id"`
	PartSequence uint64 `json:"part_sequence"`
	RelativeKey  string `json:"relative_key"`
}

type ReplayPublicationReplayManifest struct {
	Bytes        uint64 `json:"bytes"`
	DomainDigest string `json:"domain_digest"`
	FullKey      string `json:"full_key"`
	RelativeKey  string `json:"relative_key"`
	Revision     uint64 `json:"revision"`
}

type ReplayPublicationBundle struct {
	BundleVersion               string                           `json:"bundle_version"`
	CanonicalStreamRowChainRoot string                           `json:"canonical_stream_row_chain_root"`
	Claim                       ReplayPublicationClaim           `json:"claim"`
	Conversion                  ReplayPublicationConversion      `json:"conversion"`
	Limits                      ReplayPublicationLimits          `json:"limits"`
	ParquetObjects              []ReplayPublicationParquetObject `json:"parquet_objects"`
	PartManifests               []ReplayPublicationPartManifest  `json:"part_manifests"`
	PartSetRoot                 string                           `json:"part_set_root"`
	RawManifest                 ReplayPublicationRawManifest     `json:"raw_manifest"`
	RawObjects                  []ReplayPublicationRawObject     `json:"raw_objects"`
	ReplayManifest              ReplayPublicationReplayManifest  `json:"replay_manifest"`
	Scope                       ReplayPublicationScope           `json:"scope"`
}

type ReplayObservedRawManifest struct {
	Bytes        uint64 `json:"bytes"`
	DomainDigest string `json:"domain_digest"`
	FullKey      string `json:"full_key"`
}

type ReplayObservedRawObject struct {
	Bytes   uint64 `json:"bytes"`
	FullKey string `json:"full_key"`
	SHA256  string `json:"sha256"`
}

type ReplayObservedDerivativeObject struct {
	Bytes        uint64 `json:"bytes"`
	Digest       string `json:"digest"`
	DigestDomain string `json:"digest_domain"`
	FullKey      string `json:"full_key"`
	Kind         string `json:"kind"`
}

type ReplayObservedRevisionEdge struct {
	CanonicalStreamRowChainRoot string  `json:"canonical_stream_row_chain_root"`
	CanonicalJSON               string  `json:"canonical_json"`
	FullKey                     string  `json:"full_key"`
	ManifestDigest              string  `json:"manifest_digest"`
	PartCount                   uint64  `json:"part_count"`
	PartSetRoot                 string  `json:"part_set_root"`
	PreviousManifestDigest      *string `json:"previous_manifest_digest"`
	Revision                    uint64  `json:"revision"`
}

type ReplayFinalObservation struct {
	BundleDigest        string                           `json:"bundle_digest"`
	Claim               ReplayPublicationClaim           `json:"claim"`
	Complete            bool                             `json:"complete"`
	DerivativeObjects   []ReplayObservedDerivativeObject `json:"derivative_objects"`
	ObservationBytes    uint64                           `json:"observation_bytes"`
	ObservationRequests uint64                           `json:"observation_requests"`
	ObservationVersion  string                           `json:"observation_version"`
	RawManifest         ReplayObservedRawManifest        `json:"raw_manifest"`
	RawObjects          []ReplayObservedRawObject        `json:"raw_objects"`
	ReplayEdges         []ReplayObservedRevisionEdge     `json:"replay_edges"`
}

func PublisherClaimDomainDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(PublisherClaimDomain), canonical...))
}

func ReplayPublicationBundleCanonicalJSON(bundle ReplayPublicationBundle) ([]byte, error) {
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	return canonicalStructJSON(bundle)
}

func ReplayPublicationBundleDigest(bundle ReplayPublicationBundle) ([32]byte, error) {
	canonical, err := ReplayPublicationBundleCanonicalJSON(bundle)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(ReplayPublicationBundleDomain), canonical...)), nil
}

func VerifyReplayPublicationBundle(data []byte) (ReplayPublicationBundle, [32]byte, error) {
	value, err := decodePublicationCanonicalJSON(data)
	if err != nil {
		return ReplayPublicationBundle{}, [32]byte{}, err
	}
	if err := validateBundleShape(value); err != nil {
		return ReplayPublicationBundle{}, [32]byte{}, err
	}
	var bundle ReplayPublicationBundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		return ReplayPublicationBundle{}, [32]byte{}, newError(ErrInvalidField, "bundle JSON types: %v", err)
	}
	digest, err := ReplayPublicationBundleDigest(bundle)
	if err != nil {
		return ReplayPublicationBundle{}, [32]byte{}, err
	}
	return bundle, digest, nil
}

func ReplayFinalObservationCanonicalJSON(observation ReplayFinalObservation, bundle ReplayPublicationBundle) ([]byte, error) {
	if err := observation.Validate(bundle); err != nil {
		return nil, err
	}
	return canonicalStructJSON(observation)
}

func ReplayFinalObservationDigest(observation ReplayFinalObservation, bundle ReplayPublicationBundle) ([32]byte, error) {
	canonical, err := ReplayFinalObservationCanonicalJSON(observation, bundle)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(ReplayFinalObservationDomain), canonical...)), nil
}

func VerifyReplayFinalObservation(data []byte, bundle ReplayPublicationBundle) (ReplayFinalObservation, [32]byte, error) {
	value, err := decodePublicationCanonicalJSON(data)
	if err != nil {
		return ReplayFinalObservation{}, [32]byte{}, err
	}
	if err := validateFinalObservationShape(value); err != nil {
		return ReplayFinalObservation{}, [32]byte{}, err
	}
	var observation ReplayFinalObservation
	if err := json.Unmarshal(data, &observation); err != nil {
		return ReplayFinalObservation{}, [32]byte{}, newError(ErrInvalidField, "final observation JSON types: %v", err)
	}
	digest, err := ReplayFinalObservationDigest(observation, bundle)
	if err != nil {
		return ReplayFinalObservation{}, [32]byte{}, err
	}
	return observation, digest, nil
}

func (bundle ReplayPublicationBundle) Validate() error {
	if bundle.BundleVersion != ReplayPublicationBundleVersion {
		return newError(ErrInvalidField, "invalid bundle version")
	}
	if err := bundle.Scope.validate(); err != nil {
		return err
	}
	if err := bundle.Claim.validate(bundle.Scope); err != nil {
		return err
	}
	if err := bundle.Conversion.validate(); err != nil {
		return err
	}
	if err := bundle.Limits.validate(); err != nil {
		return err
	}
	if err := validateNonzeroHash("part_set_root", bundle.PartSetRoot); err != nil {
		if len(bundle.PartManifests) != 0 || bundle.PartSetRoot != strings.Repeat("0", 64) {
			return err
		}
	}
	if err := validateNonzeroHash("canonical_stream_row_chain_root", bundle.CanonicalStreamRowChainRoot); err != nil {
		if len(bundle.PartManifests) != 0 || bundle.CanonicalStreamRowChainRoot != strings.Repeat("0", 64) {
			return err
		}
	}
	if len(bundle.ParquetObjects) != len(bundle.PartManifests) {
		return newError(ErrScopeCollision, "Parquet and part manifest counts differ")
	}
	requiredRounds, requiredErr := RequiredReplayPublicationRounds(uint64(len(bundle.PartManifests)))
	if requiredErr != nil {
		return requiredErr
	}
	if bundle.Limits.MaxPublicationRounds < requiredRounds {
		return newError(ErrResourceLimit, "publication round budget is too small for the bundle")
	}
	if uint64(len(bundle.PartManifests)) > bundle.Limits.MaxParts {
		return newError(ErrResourceLimit, "part count exceeds limit")
	}
	if uint64(len(bundle.PartManifests)+1) > bundle.Limits.MaxGraphNodes {
		return newError(ErrResourceLimit, "graph node count exceeds limit")
	}
	if uint64(len(bundle.ParquetObjects)+len(bundle.PartManifests)+1) > bundle.Limits.MaxListObjects {
		return newError(ErrResourceLimit, "derivative inventory exceeds list limit")
	}
	if err := bundle.RawManifest.validate(bundle.Scope, bundle.Limits); err != nil {
		return err
	}
	metadataTotal := uint64(len(bundle.Claim.CanonicalJSON))
	var observationTotal uint64
	var err error
	metadataTotal, err = checkedPublicationTotal(metadataTotal, bundle.RawManifest.Bytes, bundle.Limits.MaxTotalMetadataBytes)
	if err != nil {
		return err
	}
	observationTotal, err = checkedPublicationTotal(observationTotal, uint64(len(bundle.Claim.CanonicalJSON)), bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}
	observationTotal, err = checkedPublicationTotal(observationTotal, bundle.RawManifest.Bytes, bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}
	previousRawKey := ""
	for index := range bundle.RawObjects {
		object := bundle.RawObjects[index]
		if err := object.validate(bundle.Scope); err != nil {
			return err
		}
		if object.FullKey <= previousRawKey {
			return newError(ErrInvalidField, "raw objects are not key-sorted")
		}
		previousRawKey = object.FullKey
		observationTotal, err = checkedPublicationTotal(observationTotal, object.Bytes, bundle.Limits.MaxObservationBytes)
		if err != nil {
			return err
		}
	}
	seenObjectIDs := map[string]bool{}
	var parquetTotal uint64
	for index := range bundle.ParquetObjects {
		object := bundle.ParquetObjects[index]
		if err := object.validate(bundle); err != nil {
			return err
		}
		if seenObjectIDs[object.ObjectID] {
			return newError(ErrInvalidField, "duplicate bundle object ID")
		}
		seenObjectIDs[object.ObjectID] = true
		parquetTotal, err = checkedPublicationTotal(parquetTotal, object.Bytes, bundle.Limits.MaxTotalParquetBytes)
		if err != nil {
			return err
		}
		observationTotal, err = checkedPublicationTotal(observationTotal, object.Bytes, bundle.Limits.MaxObservationBytes)
		if err != nil {
			return err
		}
	}
	for index := range bundle.PartManifests {
		manifest := bundle.PartManifests[index]
		if manifest.PartSequence != uint64(index) {
			return newError(ErrInvalidField, "part manifest sequence is not contiguous")
		}
		if err := manifest.validate(bundle); err != nil {
			return err
		}
		if seenObjectIDs[manifest.ObjectID] {
			return newError(ErrInvalidField, "duplicate bundle object ID")
		}
		seenObjectIDs[manifest.ObjectID] = true
		metadataTotal, err = checkedPublicationTotal(metadataTotal, manifest.Bytes, bundle.Limits.MaxTotalMetadataBytes)
		if err != nil {
			return err
		}
		observationTotal, err = checkedPublicationTotal(observationTotal, manifest.Bytes, bundle.Limits.MaxObservationBytes)
		if err != nil {
			return err
		}
	}
	if err := bundle.ReplayManifest.validate(bundle); err != nil {
		return err
	}
	if bundle.ReplayManifest.Revision > bundle.Limits.MaxGraphNodes {
		return newError(ErrResourceLimit, "replay revision exceeds graph node limit")
	}
	metadataTotal, err = checkedPublicationTotal(metadataTotal, bundle.ReplayManifest.Bytes, bundle.Limits.MaxTotalMetadataBytes)
	if err != nil {
		return err
	}
	observationTotal, err = checkedPublicationTotal(observationTotal, bundle.ReplayManifest.Bytes, bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}
	minimumRequests := uint64(3 + len(bundle.RawObjects) + len(bundle.ParquetObjects) + len(bundle.PartManifests))
	if bundle.ReplayManifest.Revision > bundle.Limits.MaxObservationRequests || minimumRequests > bundle.Limits.MaxObservationRequests-bundle.ReplayManifest.Revision {
		return newError(ErrResourceLimit, "complete observation request budget is too small")
	}
	if observationTotal > bundle.Limits.MaxObservationBytes {
		return newError(ErrResourceLimit, "complete observation byte budget is too small")
	}
	return nil
}

func (observation ReplayFinalObservation) Validate(bundle ReplayPublicationBundle) error {
	if err := bundle.Validate(); err != nil {
		return err
	}
	if observation.ObservationVersion != ReplayFinalObservationVersion || !observation.Complete {
		return newError(ErrIncompleteObservation, "final observation is not complete")
	}
	bundleDigest, err := ReplayPublicationBundleDigest(bundle)
	if err != nil {
		return err
	}
	if observation.BundleDigest != hex.EncodeToString(bundleDigest[:]) {
		return newError(ErrWrongDomain, "bundle digest mismatch")
	}
	if observation.Claim != bundle.Claim {
		return newError(ErrIncompleteObservation, "claim is not Exact")
	}
	wantRawManifest := ReplayObservedRawManifest{Bytes: bundle.RawManifest.Bytes, DomainDigest: bundle.RawManifest.DomainDigest, FullKey: bundle.RawManifest.FullKey}
	if observation.RawManifest != wantRawManifest {
		return newError(ErrIncompleteObservation, "raw manifest is not Exact")
	}
	if len(observation.RawObjects) != len(bundle.RawObjects) {
		return newError(ErrIncompleteObservation, "raw object inventory is incomplete")
	}
	for index := range bundle.RawObjects {
		want := ReplayObservedRawObject{Bytes: bundle.RawObjects[index].Bytes, FullKey: bundle.RawObjects[index].FullKey, SHA256: bundle.RawObjects[index].SHA256}
		if observation.RawObjects[index] != want {
			return newError(ErrIncompleteObservation, "raw object inventory is not Exact")
		}
	}
	wantDerivatives := bundle.expectedDerivatives()
	if len(observation.DerivativeObjects) != len(wantDerivatives) {
		return newError(ErrIncompleteObservation, "derivative inventory is incomplete")
	}
	for index := range wantDerivatives {
		if observation.DerivativeObjects[index] != wantDerivatives[index] {
			return newError(ErrIncompleteObservation, "derivative inventory is not Exact or key-sorted")
		}
	}
	if len(observation.ReplayEdges) == 0 {
		return newError(ErrIncompleteObservation, "replay graph is empty")
	}
	if err := validateReplayObservedEdges(bundle, observation.ReplayEdges); err != nil {
		return err
	}
	lastEdge := observation.ReplayEdges[len(observation.ReplayEdges)-1]
	if lastEdge.Revision != bundle.ReplayManifest.Revision || lastEdge.ManifestDigest != bundle.ReplayManifest.DomainDigest || lastEdge.FullKey != bundle.ReplayManifest.FullKey || lastEdge.PartCount != uint64(len(bundle.PartManifests)) || lastEdge.PartSetRoot != bundle.PartSetRoot || lastEdge.CanonicalStreamRowChainRoot != bundle.CanonicalStreamRowChainRoot {
		return newError(ErrIncompleteObservation, "final replay edge does not match bundle")
	}
	minimumRequests := uint64(2 + len(observation.RawObjects) + len(observation.DerivativeObjects))
	if minimumRequests > bundle.Limits.MaxObservationRequests || uint64(len(observation.ReplayEdges)) > bundle.Limits.MaxObservationRequests-minimumRequests {
		return newError(ErrResourceLimit, "observation request counter is outside budget")
	}
	minimumRequests += uint64(len(observation.ReplayEdges))
	if observation.ObservationRequests < minimumRequests || observation.ObservationRequests > bundle.Limits.MaxObservationRequests {
		return newError(ErrResourceLimit, "observation request counter is outside budget")
	}
	minimumBytes, err := replayFinalObservationMinimumBytes(observation, bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}
	if observation.ObservationBytes < minimumBytes || observation.ObservationBytes > bundle.Limits.MaxObservationBytes {
		return newError(ErrResourceLimit, "observation byte counter is outside budget")
	}
	return nil
}

// ReplayFinalObservationBudgetFeasible rejects a canonical bundle before its
// digest is sealed when the complete replay graph cannot fit in the declared
// final-observation budget. The maximum counters make the JSON-size estimate
// conservative without depending on runtime I/O.
func ReplayFinalObservationBudgetFeasible(bundle ReplayPublicationBundle, edges []ReplayObservedRevisionEdge) error {
	_, err := ReplayFinalObservationRequiredBytes(bundle, edges)
	return err
}

// ReplayFinalObservationBudgetFeasibleForRevision performs the pre-lock
// budget check when the sealer has only the immediate predecessor. Earlier
// revision edges are not guessed or accepted as evidence; instead this uses a
// bounded conservative estimate for every edge that the remote observer must
// later read. The complete remote graph remains the acceptance authority.
func ReplayFinalObservationBudgetFeasibleForRevision(bundle ReplayPublicationBundle, revision uint64) error {
	if err := bundle.Validate(); err != nil {
		return err
	}
	if revision == 0 || revision != bundle.ReplayManifest.Revision {
		return newError(ErrIncompleteObservation, "replay revision does not match bundle")
	}
	if revision > bundle.Limits.MaxGraphNodes {
		return newError(ErrResourceLimit, "replay revision exceeds graph node limit")
	}
	observation := ReplayFinalObservation{
		ObservationVersion:  ReplayFinalObservationVersion,
		BundleDigest:        strings.Repeat("0", 64),
		Claim:               bundle.Claim,
		Complete:            true,
		DerivativeObjects:   bundle.expectedDerivatives(),
		ObservationBytes:    ^uint64(0),
		ObservationRequests: ^uint64(0),
		RawManifest:         ReplayObservedRawManifest{Bytes: bundle.RawManifest.Bytes, DomainDigest: bundle.RawManifest.DomainDigest, FullKey: bundle.RawManifest.FullKey},
		ReplayEdges:         make([]ReplayObservedRevisionEdge, 0),
	}
	observation.RawObjects = make([]ReplayObservedRawObject, len(bundle.RawObjects))
	for index, object := range bundle.RawObjects {
		observation.RawObjects[index] = ReplayObservedRawObject{Bytes: object.Bytes, FullKey: object.FullKey, SHA256: object.SHA256}
	}
	base, err := replayFinalObservationMinimumBytes(observation, bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}

	// One unknown edge contributes its raw key and canonical bytes, plus the
	// escaped edge strings and fixed fields in the final canonical observation.
	// Sixteen metadata-object widths leave room for UTF-16 escaping and JSON
	// structure while remaining far below the implementation observation bound.
	perEdge := uint64(0)
	for index := 0; index < 16; index++ {
		perEdge, err = checkedPublicationTotal(perEdge, bundle.Limits.MaxMetadataObjectBytes, bundle.Limits.MaxObservationBytes)
		if err != nil {
			return err
		}
	}
	perEdge, err = checkedPublicationTotal(perEdge, 4096, bundle.Limits.MaxObservationBytes)
	if err != nil {
		return err
	}
	total := base
	for index := uint64(0); index < revision; index++ {
		total, err = checkedPublicationTotal(total, perEdge, bundle.Limits.MaxObservationBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReplayFinalObservationRequiredBytes returns the overflow-safe conservative
// byte requirement used by the pre-lock feasibility gate.
func ReplayFinalObservationRequiredBytes(bundle ReplayPublicationBundle, edges []ReplayObservedRevisionEdge) (uint64, error) {
	if err := bundle.Validate(); err != nil {
		return 0, err
	}
	if len(edges) == 0 {
		return 0, newError(ErrIncompleteObservation, "replay graph is empty")
	}
	if err := validateReplayObservedEdges(bundle, edges); err != nil {
		return 0, err
	}
	last := edges[len(edges)-1]
	if last.Revision != bundle.ReplayManifest.Revision || last.ManifestDigest != bundle.ReplayManifest.DomainDigest || last.FullKey != bundle.ReplayManifest.FullKey || last.PartCount != uint64(len(bundle.PartManifests)) || last.PartSetRoot != bundle.PartSetRoot || last.CanonicalStreamRowChainRoot != bundle.CanonicalStreamRowChainRoot {
		return 0, newError(ErrIncompleteObservation, "final replay edge does not match bundle")
	}
	observation := ReplayFinalObservation{
		ObservationVersion:  ReplayFinalObservationVersion,
		BundleDigest:        strings.Repeat("0", 64),
		Claim:               bundle.Claim,
		Complete:            true,
		DerivativeObjects:   bundle.expectedDerivatives(),
		ObservationBytes:    ^uint64(0),
		ObservationRequests: ^uint64(0),
		RawManifest:         ReplayObservedRawManifest{Bytes: bundle.RawManifest.Bytes, DomainDigest: bundle.RawManifest.DomainDigest, FullKey: bundle.RawManifest.FullKey},
		ReplayEdges:         append([]ReplayObservedRevisionEdge(nil), edges...),
	}
	observation.RawObjects = make([]ReplayObservedRawObject, len(bundle.RawObjects))
	for index, object := range bundle.RawObjects {
		observation.RawObjects[index] = ReplayObservedRawObject{Bytes: object.Bytes, FullKey: object.FullKey, SHA256: object.SHA256}
	}
	return replayFinalObservationMinimumBytes(observation, bundle.Limits.MaxObservationBytes)
}

func replayFinalObservationMinimumBytes(observation ReplayFinalObservation, limit uint64) (uint64, error) {
	total := uint64(len([]byte(observation.Claim.CanonicalJSON)))
	var err error
	for _, size := range []uint64{observation.RawManifest.Bytes} {
		total, err = checkedPublicationTotal(total, size, limit)
		if err != nil {
			return 0, err
		}
	}
	for _, object := range observation.RawObjects {
		total, err = checkedPublicationTotal(total, object.Bytes, limit)
		if err != nil {
			return 0, err
		}
	}
	for _, object := range observation.DerivativeObjects {
		total, err = checkedPublicationTotal(total, object.Bytes, limit)
		if err != nil {
			return 0, err
		}
	}
	for _, edge := range observation.ReplayEdges {
		total, err = checkedPublicationTotal(total, uint64(len([]byte(edge.FullKey))), limit)
		if err != nil {
			return 0, err
		}
		total, err = checkedPublicationTotal(total, uint64(len([]byte(edge.CanonicalJSON))), limit)
		if err != nil {
			return 0, err
		}
	}
	canonical, err := canonicalStructJSON(observation)
	if err != nil {
		return 0, err
	}
	return checkedPublicationTotal(total, uint64(len(canonical)), limit)
}

func validateReplayObservedEdges(bundle ReplayPublicationBundle, edges []ReplayObservedRevisionEdge) error {
	var previousManifest *ReplayDayManifest
	for index := range edges {
		edge := edges[index]
		if edge.Revision == 0 || edge.CanonicalJSON == "" || edge.FullKey == "" {
			return newError(ErrIncompleteObservation, "replay edge identity is incomplete")
		}
		canonical := []byte(edge.CanonicalJSON)
		manifest, err := VerifyReplayDayManifest(canonical)
		if err != nil || manifest.M0EmptyPartsCompatibility {
			return newError(ErrInvalidField, "replay edge manifest is invalid or noncanonical")
		}
		reencoded, err := ReplayDayManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(reencoded, canonical) {
			return newError(ErrInvalidField, "replay edge manifest is noncanonical")
		}
		digest, err := ReplayDayManifestDigest(manifest)
		if err != nil || edge.ManifestDigest != hex.EncodeToString(digest[:]) {
			return newError(ErrWrongDomain, "replay edge manifest digest mismatch")
		}
		relativeKey, err := ReplayDayManifestKey(manifest)
		if err != nil || edge.FullKey != bundle.Scope.ImmutablePrefix+"/"+relativeKey {
			return newError(ErrWrongKey, "replay edge manifest key mismatch")
		}
		if edge.Revision != manifest.Revision || edge.PartCount != uint64(len(manifest.PartManifestKeys)) || edge.PartSetRoot != hex.EncodeToString(manifest.PartSetRoot[:]) || edge.CanonicalStreamRowChainRoot != hex.EncodeToString(manifest.CanonicalStreamRowChainRoot[:]) {
			return newError(ErrIncompleteObservation, "replay edge differs from canonical manifest")
		}
		var predecessor *string
		if manifest.PreviousManifestSHA256 != nil {
			value := hex.EncodeToString(manifest.PreviousManifestSHA256[:])
			predecessor = &value
		}
		if edge.PreviousManifestDigest == nil != (predecessor == nil) || edge.PreviousManifestDigest != nil && *edge.PreviousManifestDigest != *predecessor {
			return newError(ErrIncompleteObservation, "replay edge predecessor differs from canonical manifest")
		}
		if !replayManifestMatchesPublicationScope(manifest, bundle) {
			return newError(ErrScopeCollision, "replay edge scope or conversion mismatch")
		}
		if index == 0 {
			if manifest.Revision != 1 || predecessor != nil {
				return newError(ErrIncompleteObservation, "genesis replay edge is incomplete")
			}
		} else {
			previous := edges[index-1]
			if manifest.Revision != previous.Revision+1 || predecessor == nil || *predecessor != previous.ManifestDigest || previousManifest == nil || previousManifest.ManifestID != manifest.ManifestID || previousManifest.RawDayManifestKey == manifest.RawDayManifestKey || previousManifest.RawDayManifestSHA256 == manifest.RawDayManifestSHA256 {
				return newError(ErrIncompleteObservation, "replay edge chain is incomplete")
			}
		}
		copyManifest := manifest
		previousManifest = &copyManifest
	}
	return nil
}

func replayManifestMatchesPublicationScope(manifest ReplayDayManifest, bundle ReplayPublicationBundle) bool {
	dependency, err := ParseHashHex(bundle.Conversion.DependencyLockHash)
	if err != nil {
		return false
	}
	writer, err := ParseHashHex(bundle.Conversion.WriterConfigurationHash)
	if err != nil {
		return false
	}
	return manifest.DatasetID == bundle.Scope.DatasetID && manifest.DayDefinitionID == bundle.Scope.DayDefinitionID && manifest.Date == bundle.Scope.Date && manifest.ReplayContractID == bundle.Conversion.ReplayContractID && manifest.FormatID == bundle.Conversion.FormatID && manifest.ConversionID == bundle.Conversion.ConversionID && manifest.ConverterBuildID == bundle.Conversion.ConverterBuildID && manifest.DependencyLockHash == dependency && manifest.WriterConfigurationHash == writer && manifest.TargetPlatformContract == bundle.Conversion.TargetPlatformContract
}

func (scope ReplayPublicationScope) validate() error {
	for name, value := range map[string]string{
		"broker_server_fingerprint": scope.BrokerServerFingerprint,
		"dataset_id":                scope.DatasetID,
		"day_definition_id":         scope.DayDefinitionID,
		"exact_source_symbol":       scope.ExactSourceSymbol,
		"provider_id":               scope.ProviderID,
		"publisher_id":              scope.PublisherID,
		"scope_key":                 scope.ScopeKey,
		"settle_policy":             scope.SettlePolicy,
		"stable_feed_id":            scope.StableFeedID,
	} {
		if err := validatePublicationString(name, value); err != nil {
			return err
		}
	}
	parsed, err := time.Parse("2006-01-02", scope.Date)
	if err != nil || parsed.Format("2006-01-02") != scope.Date {
		return newError(ErrInvalidField, "scope date is not UTC YYYY-MM-DD")
	}
	if err := validateNonzeroHash("scope_config_hash", scope.ScopeConfigHash); err != nil {
		return err
	}
	if err := validatePrefix("immutable_prefix", scope.ImmutablePrefix); err != nil {
		return err
	}
	return nil
}

func (claim ReplayPublicationClaim) validate(scope ReplayPublicationScope) error {
	if claim.CanonicalJSON == "" || claim.FullKey == "" || claim.DomainDigest == "" {
		return newError(ErrRawClaimMissing, "M2 publisher claim is missing")
	}
	if err := validateNonzeroHash("claim domain_digest", claim.DomainDigest); err != nil {
		return err
	}
	wantDigest := PublisherClaimDomainDigest([]byte(claim.CanonicalJSON))
	if claim.DomainDigest != hex.EncodeToString(wantDigest[:]) {
		return newError(ErrWrongDomain, "publisher claim domain digest mismatch")
	}
	wantKey := scope.ImmutablePrefix + "/publisher-claims/epoch=" + strconv.FormatUint(scope.PublisherEpoch, 10) + ".json"
	if claim.FullKey != wantKey {
		return newError(ErrWrongKey, "publisher claim key mismatch")
	}
	value, err := decodePublicationCanonicalJSON([]byte(claim.CanonicalJSON))
	if err != nil {
		return err
	}
	object, err := publicationExactObject(value, publisherClaimKeys)
	if err != nil {
		return err
	}
	stringsToMatch := map[string]string{
		"broker_server_fingerprint": scope.BrokerServerFingerprint,
		"claim_version":             PublisherClaimVersion,
		"config_hash":               scope.ScopeConfigHash,
		"dataset_id":                scope.DatasetID,
		"day_definition_id":         scope.DayDefinitionID,
		"exact_source_symbol":       scope.ExactSourceSymbol,
		"provider_id":               scope.ProviderID,
		"publisher_id":              scope.PublisherID,
		"scope_key":                 scope.ScopeKey,
		"settle_policy":             scope.SettlePolicy,
		"stable_feed_id":            scope.StableFeedID,
	}
	for key, want := range stringsToMatch {
		actual, ok := object[key].(string)
		if !ok || actual != want {
			return newError(ErrScopeCollision, "publisher claim %s differs from bundle scope", key)
		}
	}
	epoch, ok := object["publisher_epoch"].(uint64)
	if !ok || epoch != scope.PublisherEpoch {
		return newError(ErrScopeCollision, "publisher claim epoch differs from bundle scope")
	}
	return nil
}

func (conversion ReplayPublicationConversion) validate() error {
	for name, value := range map[string]string{
		"conversion_id":            conversion.ConversionID,
		"converter_build_id":       conversion.ConverterBuildID,
		"format_id":                conversion.FormatID,
		"replay_contract_id":       conversion.ReplayContractID,
		"target_platform_contract": conversion.TargetPlatformContract,
	} {
		if err := validatePublicationString(name, value); err != nil {
			return err
		}
	}
	if conversion.FormatID != ReplayFormatID {
		return newError(ErrInvalidField, "conversion format is not ticks-parquet-v1")
	}
	if err := validateNonzeroHash("dependency_lock_hash", conversion.DependencyLockHash); err != nil {
		return err
	}
	if err := validateNonzeroHash("writer_configuration_hash", conversion.WriterConfigurationHash); err != nil {
		return err
	}
	if conversion.MaxRowsPerPart == 0 || conversion.MaxCanonicalBytesPerPart == 0 || conversion.MaxRowsPerRowGroup == 0 || conversion.MaxRowsPerRowGroup > conversion.MaxRowsPerPart {
		return newError(ErrInvalidField, "conversion limits are invalid")
	}
	return nil
}

func (limits ReplayPublicationLimits) validate() error {
	values := []struct {
		name  string
		value uint64
		bound uint64
	}{
		{"max_graph_nodes", limits.MaxGraphNodes, ReplayPublicationImplementationBounds.MaxGraphNodes},
		{"max_list_objects", limits.MaxListObjects, ReplayPublicationImplementationBounds.MaxListObjects},
		{"max_metadata_object_bytes", limits.MaxMetadataObjectBytes, ReplayPublicationImplementationBounds.MaxMetadataObjectBytes},
		{"max_observation_bytes", limits.MaxObservationBytes, ReplayPublicationImplementationBounds.MaxObservationBytes},
		{"max_observation_requests", limits.MaxObservationRequests, ReplayPublicationImplementationBounds.MaxObservationRequests},
		{"max_parquet_object_bytes", limits.MaxParquetObjectBytes, ReplayPublicationImplementationBounds.MaxParquetObjectBytes},
		{"max_parts", limits.MaxParts, ReplayPublicationImplementationBounds.MaxParts},
		{"max_publication_rounds", limits.MaxPublicationRounds, ReplayPublicationImplementationBounds.MaxPublicationRounds},
		{"max_total_metadata_bytes", limits.MaxTotalMetadataBytes, ReplayPublicationImplementationBounds.MaxTotalMetadataBytes},
		{"max_total_parquet_bytes", limits.MaxTotalParquetBytes, ReplayPublicationImplementationBounds.MaxTotalParquetBytes},
	}
	for _, item := range values {
		if item.value == 0 || item.value > item.bound {
			return newError(ErrResourceLimit, "%s is zero or exceeds implementation bound", item.name)
		}
	}
	if limits.MaxMetadataObjectBytes > limits.MaxTotalMetadataBytes || limits.MaxParquetObjectBytes > limits.MaxTotalParquetBytes || limits.MaxTotalParquetBytes > limits.MaxObservationBytes {
		return newError(ErrResourceLimit, "resource limit relationship is invalid")
	}
	return nil
}

// ValidateReplayPublicationLimits exposes the canonical Protocol V1 resource
// policy to adapters without duplicating its bounds or relationships.
func ValidateReplayPublicationLimits(limits ReplayPublicationLimits) error {
	return limits.validate()
}

func (manifest ReplayPublicationRawManifest) validate(scope ReplayPublicationScope, limits ReplayPublicationLimits) error {
	if manifest.Bytes == 0 || manifest.Bytes > limits.MaxMetadataObjectBytes {
		return newError(ErrResourceLimit, "raw manifest bytes exceed metadata object bound")
	}
	if manifest.Revision == 0 {
		return newError(ErrInvalidField, "raw manifest revision is zero")
	}
	if err := validateNonzeroHash("raw manifest domain_digest", manifest.DomainDigest); err != nil {
		return err
	}
	return validateObjectKeys(scope, manifest.RelativeKey, manifest.FullKey)
}

func (object ReplayPublicationRawObject) validate(scope ReplayPublicationScope) error {
	if object.Bytes == 0 {
		return newError(ErrInvalidField, "raw object bytes are zero")
	}
	if err := validateNonzeroHash("raw object sha256", object.SHA256); err != nil {
		return err
	}
	wantRelative := "objects/raw/wal-" + object.SHA256 + ".rtw"
	if object.RelativeKey != wantRelative {
		return newError(ErrWrongKey, "raw object relative key mismatch")
	}
	return validateObjectKeys(scope, object.RelativeKey, object.FullKey)
}

func (object ReplayPublicationParquetObject) validate(bundle ReplayPublicationBundle) error {
	if object.Bytes == 0 || object.Bytes > bundle.Limits.MaxParquetObjectBytes {
		return newError(ErrResourceLimit, "Parquet object bytes exceed limit")
	}
	if object.ObjectID == "" || object.LastStreamSequence < object.FirstStreamSequence {
		return newError(ErrInvalidField, "Parquet object identity or range is invalid")
	}
	partHash, err := ParseHashHex(object.SHA256)
	if err != nil || partHash == ([32]byte{}) {
		return newError(ErrZeroDigest, "Parquet object sha256 is invalid or zero")
	}
	scope := bundle.replayScope()
	wantRelative, err := ReplayPartObjectKey(scope, object.FirstStreamSequence, object.LastStreamSequence, partHash)
	if err != nil || object.RelativeKey != wantRelative {
		return newError(ErrWrongKey, "Parquet object relative key mismatch")
	}
	return validateObjectKeys(bundle.Scope, object.RelativeKey, object.FullKey)
}

func (manifest ReplayPublicationPartManifest) validate(bundle ReplayPublicationBundle) error {
	if manifest.Bytes == 0 || manifest.Bytes > bundle.Limits.MaxMetadataObjectBytes {
		return newError(ErrResourceLimit, "part manifest bytes exceed metadata object limit")
	}
	if manifest.ObjectID == "" {
		return newError(ErrInvalidField, "part manifest object ID is empty")
	}
	if err := validateNonzeroHash("part manifest domain_digest", manifest.DomainDigest); err != nil {
		return err
	}
	base, err := ReplayDerivativeBaseKey(bundle.replayScope())
	if err != nil {
		return newError(ErrWrongKey, "part manifest scope: %v", err)
	}
	wantRelative := fmt.Sprintf("%s/manifests/part-%08d-%s.json", base, manifest.PartSequence, manifest.DomainDigest)
	if manifest.RelativeKey != wantRelative {
		return newError(ErrWrongKey, "part manifest relative key mismatch")
	}
	return validateObjectKeys(bundle.Scope, manifest.RelativeKey, manifest.FullKey)
}

func (manifest ReplayPublicationReplayManifest) validate(bundle ReplayPublicationBundle) error {
	if manifest.Bytes == 0 || manifest.Bytes > bundle.Limits.MaxMetadataObjectBytes {
		return newError(ErrResourceLimit, "replay manifest bytes exceed metadata object limit")
	}
	if manifest.Revision == 0 {
		return newError(ErrInvalidField, "replay manifest revision is zero")
	}
	if err := validateNonzeroHash("replay manifest domain_digest", manifest.DomainDigest); err != nil {
		return err
	}
	base, err := ReplayDerivativeBaseKey(bundle.replayScope())
	if err != nil {
		return newError(ErrWrongKey, "replay manifest scope: %v", err)
	}
	wantRelative := fmt.Sprintf("%s/replay-day-%d-%s.json", base, manifest.Revision, manifest.DomainDigest)
	if manifest.RelativeKey != wantRelative {
		return newError(ErrWrongKey, "replay manifest relative key mismatch")
	}
	return validateObjectKeys(bundle.Scope, manifest.RelativeKey, manifest.FullKey)
}

func (bundle ReplayPublicationBundle) replayScope() ReplayScope {
	manifestHash, _ := ParseHashHex(bundle.RawManifest.DomainDigest)
	return ReplayScope{
		DatasetID:       bundle.Scope.DatasetID,
		DayDefinitionID: bundle.Scope.DayDefinitionID, Date: bundle.Scope.Date,
		ReplayContractID: bundle.Conversion.ReplayContractID, ConversionID: bundle.Conversion.ConversionID,
		RawDayManifestKey: bundle.RawManifest.RelativeKey, RawDayManifestSHA256: manifestHash,
	}
}

func (bundle ReplayPublicationBundle) expectedDerivatives() []ReplayObservedDerivativeObject {
	result := make([]ReplayObservedDerivativeObject, 0, len(bundle.ParquetObjects)+len(bundle.PartManifests)+1)
	for _, object := range bundle.ParquetObjects {
		result = append(result, ReplayObservedDerivativeObject{Bytes: object.Bytes, Digest: object.SHA256, DigestDomain: ReplayDerivativeDigestSHA256, FullKey: object.FullKey, Kind: ReplayDerivativeKindParquet})
	}
	for _, object := range bundle.PartManifests {
		result = append(result, ReplayObservedDerivativeObject{Bytes: object.Bytes, Digest: object.DomainDigest, DigestDomain: ReplayDerivativeDigestPart, FullKey: object.FullKey, Kind: ReplayDerivativeKindPartManifest})
	}
	result = append(result, ReplayObservedDerivativeObject{Bytes: bundle.ReplayManifest.Bytes, Digest: bundle.ReplayManifest.DomainDigest, DigestDomain: ReplayDerivativeDigestReplay, FullKey: bundle.ReplayManifest.FullKey, Kind: ReplayDerivativeKindReplay})
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].FullKey < result[j-1].FullKey; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}

func validateObjectKeys(scope ReplayPublicationScope, relative, full string) error {
	if err := validateRelativePublicationKey(relative); err != nil {
		return err
	}
	if full != scope.ImmutablePrefix+"/"+relative {
		return newError(ErrWrongKey, "full key is not the trusted-prefix derivation")
	}
	return nil
}

func validateRelativePublicationKey(value string) error {
	if value == "" || !utf8.ValidString(value) || len(value) > MaxPathBytes || strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\r\n") || strings.Contains(value, "//") {
		return newError(ErrWrongKey, "relative key is not canonical")
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." {
			return newError(ErrWrongKey, "relative key contains a forbidden component")
		}
	}
	return nil
}

func validatePrefix(name, value string) error {
	if value == "" || !utf8.ValidString(value) || len(value) > 4096 || strings.HasSuffix(value, "/") || strings.ContainsAny(value, "\r\n") {
		return newError(ErrWrongKey, "%s is not a canonical prefix", name)
	}
	return nil
}

func validatePublicationString(name, value string) error {
	if value == "" || !utf8.ValidString(value) || len([]byte(value)) > int(MaxStringBytes) {
		return newError(ErrInvalidField, "%s is not a Protocol V1 string", name)
	}
	return nil
}

func validateNonzeroHash(name, value string) error {
	hash, err := ParseHashHex(value)
	if err != nil {
		return newError(ErrInvalidField, "%s is not lowercase SHA-256", name)
	}
	if hash == ([32]byte{}) {
		return newError(ErrZeroDigest, "%s is zero", name)
	}
	return nil
}

func checkedPublicationTotal(total, next, limit uint64) (uint64, error) {
	if next > limit || total > limit-next {
		return 0, newError(ErrResourceLimit, "aggregate byte total exceeds limit")
	}
	return total + next, nil
}

func decodePublicationCanonicalJSON(data []byte) (any, error) {
	value, err := DecodeCanonicalJSON(data)
	if err == nil {
		return value, nil
	}
	code := ErrInvalidCanonicalJSON
	if strings.Contains(err.Error(), "duplicate JSON object key") {
		code = ErrDuplicateField
	}
	return nil, newError(code, "%v", err)
}

func canonicalStructJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	normalized, err := normalizeJSONNumbers(generic)
	if err != nil {
		return nil, err
	}
	return CanonicalJSON(normalized)
}

func normalizeJSONNumbers(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		if strings.HasPrefix(typed.String(), "-") {
			return strconv.ParseInt(typed.String(), 10, 64)
		}
		return strconv.ParseUint(typed.String(), 10, 64)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			converted, err := normalizeJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := normalizeJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return value, nil
	}
}

func publicationExactObject(value any, expected map[string]bool) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, newError(ErrInvalidField, "publication canonical value must be an object")
	}
	for key := range object {
		if !expected[key] {
			return nil, newError(ErrUnknownField, "unknown publication key %q", key)
		}
	}
	if len(object) != len(expected) {
		return nil, newError(ErrInvalidField, "publication object is missing a required key")
	}
	return object, nil
}

func validateObjectArrayShape(object map[string]any, key string, expected map[string]bool) error {
	items, ok := object[key].([]any)
	if !ok {
		return newError(ErrInvalidField, "%s must be an array", key)
	}
	for _, item := range items {
		if _, err := publicationExactObject(item, expected); err != nil {
			return err
		}
	}
	return nil
}

func validateBundleShape(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return newError(ErrInvalidField, "bundle must be an object")
	}
	if _, exists := object["claim"]; !exists {
		return newError(ErrRawClaimMissing, "bundle has no M2 publisher claim")
	}
	object, err := publicationExactObject(value, bundleKeys)
	if err != nil {
		return err
	}
	for key, expected := range map[string]map[string]bool{
		"claim": claimKeys, "conversion": conversionKeys, "limits": limitKeys,
		"raw_manifest": rawManifestKeys, "replay_manifest": replayManifestKeys,
		"scope": scopeKeys,
	} {
		if _, err := publicationExactObject(object[key], expected); err != nil {
			return err
		}
	}
	for key, expected := range map[string]map[string]bool{
		"raw_objects": rawObjectKeys, "parquet_objects": parquetObjectKeys, "part_manifests": partManifestPublicationKeys,
	} {
		if err := validateObjectArrayShape(object, key, expected); err != nil {
			return err
		}
	}
	return nil
}

func validateFinalObservationShape(value any) error {
	object, err := publicationExactObject(value, finalObservationKeys)
	if err != nil {
		return err
	}
	for key, expected := range map[string]map[string]bool{
		"claim": claimKeys, "raw_manifest": observedRawManifestKeys,
	} {
		if _, err := publicationExactObject(object[key], expected); err != nil {
			return err
		}
	}
	for key, expected := range map[string]map[string]bool{
		"raw_objects": observedRawObjectKeys, "derivative_objects": observedDerivativeKeys, "replay_edges": replayEdgeKeys,
	} {
		if err := validateObjectArrayShape(object, key, expected); err != nil {
			return err
		}
	}
	return nil
}

func keySet(values ...string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

var (
	bundleKeys                  = keySet("bundle_version", "canonical_stream_row_chain_root", "claim", "conversion", "limits", "parquet_objects", "part_manifests", "part_set_root", "raw_manifest", "raw_objects", "replay_manifest", "scope")
	claimKeys                   = keySet("canonical_json", "domain_digest", "full_key")
	conversionKeys              = keySet("conversion_id", "converter_build_id", "dependency_lock_hash", "format_id", "max_canonical_bytes_per_part", "max_rows_per_part", "max_rows_per_row_group", "replay_contract_id", "target_platform_contract", "writer_configuration_hash")
	limitKeys                   = keySet("max_graph_nodes", "max_list_objects", "max_metadata_object_bytes", "max_observation_bytes", "max_observation_requests", "max_parquet_object_bytes", "max_parts", "max_publication_rounds", "max_total_metadata_bytes", "max_total_parquet_bytes")
	rawManifestKeys             = keySet("bytes", "domain_digest", "full_key", "relative_key", "revision")
	rawObjectKeys               = keySet("bytes", "full_key", "relative_key", "sha256")
	parquetObjectKeys           = keySet("bytes", "first_stream_sequence", "full_key", "last_stream_sequence", "object_id", "relative_key", "sha256")
	partManifestPublicationKeys = keySet("bytes", "domain_digest", "full_key", "object_id", "part_sequence", "relative_key")
	replayManifestKeys          = keySet("bytes", "domain_digest", "full_key", "relative_key", "revision")
	scopeKeys                   = keySet("broker_server_fingerprint", "dataset_id", "date", "day_definition_id", "exact_source_symbol", "immutable_prefix", "provider_id", "publisher_epoch", "publisher_id", "scope_config_hash", "scope_key", "settle_policy", "stable_feed_id")
	finalObservationKeys        = keySet("bundle_digest", "claim", "complete", "derivative_objects", "observation_bytes", "observation_requests", "observation_version", "raw_manifest", "raw_objects", "replay_edges")
	observedRawManifestKeys     = keySet("bytes", "domain_digest", "full_key")
	observedRawObjectKeys       = keySet("bytes", "full_key", "sha256")
	observedDerivativeKeys      = keySet("bytes", "digest", "digest_domain", "full_key", "kind")
	replayEdgeKeys              = keySet("canonical_json", "canonical_stream_row_chain_root", "full_key", "manifest_digest", "part_count", "part_set_root", "previous_manifest_digest", "revision")
	publisherClaimKeys          = keySet("broker_server_fingerprint", "claim_version", "config_hash", "dataset_id", "day_definition_id", "exact_source_symbol", "provider_id", "publisher_epoch", "publisher_id", "scope_key", "settle_policy", "stable_feed_id")
)
