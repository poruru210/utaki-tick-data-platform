package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

var ErrReplayCheckDifferent = ErrRemoteCheckMismatch

type ReplayObservationLock interface {
	AssertHeld(bundle ReplayPublicationBundle) error
}

type ReplayBoundedObserver struct {
	remote ReplayRemoteReadBackend
	lock   ReplayObservationLock
}

func NewReplayBoundedObserver(remote ReplayRemoteReadBackend, lock ReplayObservationLock) (*ReplayBoundedObserver, error) {
	if remote == nil || lock == nil {
		return nil, fmt.Errorf("replay observer dependencies are incomplete")
	}
	return &ReplayBoundedObserver{remote: remote, lock: lock}, nil
}

// Observe performs a fresh bounded read campaign. Remote outcomes are returned
// as fail-closed observation classes; configuration and lock failures are
// returned as errors. No journal or prior event is accepted as remote truth.
func (o *ReplayBoundedObserver) Observe(ctx context.Context, bundle ReplayPublicationBundle) (ReplayRemoteObservation, error) {
	budget, err := NewReplayObservationBudget(bundle.Contract.Limits)
	if err != nil {
		return ReplayRemoteObservation{}, err
	}
	return o.ObserveWithBudget(ctx, bundle, budget)
}

// ObserveWithBudget lets the thin publisher share one aggregate budget across
// pre-action, post-action and final fresh observations. Counters are never
// reset by this method.
func (o *ReplayBoundedObserver) ObserveWithBudget(ctx context.Context, bundle ReplayPublicationBundle, budget *ReplayObservationBudget) (ReplayRemoteObservation, error) {
	if err := verifySealedReplayBundle(bundle); err != nil {
		return ReplayRemoteObservation{}, err
	}
	if budget == nil || budget.limits != bundle.Contract.Limits {
		return ReplayRemoteObservation{}, fmt.Errorf("replay observation budget does not match bundle limits")
	}
	if err := o.lock.AssertHeld(bundle); err != nil {
		return ReplayRemoteObservation{}, fmt.Errorf("replay campaign lock is not held: %w", err)
	}
	startSnapshot := budget.Snapshot()
	observation := ReplayRemoteObservation{BundleDigest: bundle.Digest}

	observation.Claim = o.observeMetadata(ctx, budget, bundle.Contract.Claim.FullKey, uint64(len(bundle.Contract.Claim.CanonicalJSON)), []byte(bundle.Contract.Claim.CanonicalJSON), nil)
	observation.RawManifest = o.observeRawManifest(ctx, budget, bundle)
	observation.RawObjects = make([]ReplayObjectObservation, len(bundle.Contract.RawObjects))
	for index, object := range bundle.Contract.RawObjects {
		observation.RawObjects[index] = ReplayObjectObservation{ObjectID: replayRawObjectID(object), Class: o.observeHashedObject(ctx, budget, object.FullKey, object.Bytes, object.SHA256, replayBudgetObservation, bundle.Contract.Limits.MaxObservationBytes)}
	}

	scope, conversion, err := replayVerificationInputs(bundle.Contract)
	if err != nil {
		return ReplayRemoteObservation{}, err
	}
	prefix := bundle.Contract.Scope.ImmutablePrefix + "/"
	derivativeBase, err := protocol.ReplayDerivativeBaseKey(scope)
	if err != nil {
		return ReplayRemoteObservation{}, err
	}
	prefix += derivativeBase + "/"
	inventory, inventoryClass := o.observeInventory(ctx, budget, prefix, bundle.Contract.Limits.MaxListObjects)
	observation.ParquetObjects = make([]ReplayObjectObservation, len(bundle.Contract.ParquetObjects))
	observation.PartManifests = make([]ReplayObjectObservation, len(bundle.Contract.PartManifests))
	for index, object := range bundle.Contract.ParquetObjects {
		class := inventoryClass
		if class == ObservationExact {
			class = o.observeParquetCandidate(ctx, budget, bundle, object, inventory)
		}
		observation.ParquetObjects[index] = ReplayObjectObservation{ObjectID: ReplayObjectID(object.ObjectID), Class: class}
	}

	parts := make([]protocol.PartManifest, 0, len(bundle.Contract.PartManifests))
	for index, object := range bundle.Contract.PartManifests {
		class := inventoryClass
		var part protocol.PartManifest
		if class == ObservationExact {
			class, part = o.observePartManifest(ctx, budget, object, inventory)
		}
		if class == ObservationExact {
			parts = append(parts, part)
		}
		observation.PartManifests[index] = ReplayObjectObservation{ObjectID: ReplayObjectID(object.ObjectID), Class: class}
	}
	var revisionGraph ReplayRevisionGraph
	if inventoryClass != ObservationExact {
		observation.PartChain = inventoryClass
		observation.ReplayManifest = inventoryClass
		observation.ReplayGraph = inventoryClass
	} else {
		partGraph, graphErr := VerifyReplayPartGraph(parts, scope, conversion, bundle.Contract.Limits.MaxGraphNodes)
		observation.PartChain = graphClass(graphErr, len(parts) == len(bundle.Contract.PartManifests))

		revisions, replayClass := o.observeReplayRevisions(ctx, budget, bundle, inventory, scope, conversion, partGraph)
		observation.ReplayManifest = replayClass
		var revisionErr error
		revisionGraph, revisionErr = VerifyReplayRevisionGraph(revisions, bundle.Contract.Limits.MaxGraphNodes)
		if revisionErr == nil {
			revisionGraph.Edges, revisionErr = replayObservedRevisionEdges(bundle.Layout, revisionGraph.Edges, revisionGraph.Revisions)
		}
		observation.ReplayGraph = graphClass(revisionErr, replayClass == ObservationExact)
		if revisionErr == nil && replayClass == ObservationExact && len(revisions) != 0 {
			last := revisions[len(revisions)-1]
			if last.ManifestSHA256 != mustHash(bundle.Contract.ReplayManifest.DomainDigest) || last.Revision != bundle.Contract.ReplayManifest.Revision {
				observation.ReplayGraph = ObservationAmbiguous
			}
		}
	}

	if err := o.lock.AssertHeld(bundle); err != nil {
		return ReplayRemoteObservation{}, fmt.Errorf("replay campaign lock was lost: %w", err)
	}
	snapshot := budget.Snapshot()
	observation.RequestCount = snapshot.Requests
	observation.ObservationBytes = snapshot.ObservationBytes
	observation.Complete = replayObservationAllExact(observation)
	if observation.Complete {
		passSnapshot, finalClass, finalErr := chargeReplayFinalObservationUplift(budget, startSnapshot, bundle, revisionGraph.Edges)
		snapshot = budget.Snapshot()
		observation.RequestCount = snapshot.Requests
		observation.ObservationBytes = snapshot.ObservationBytes
		if finalErr != nil {
			observation.Complete = false
			observation.ReplayGraph = ObservationAmbiguous
		} else if finalClass != ObservationExact {
			observation.Complete = false
			observation.ReplayGraph = finalClass
		} else {
			final, buildErr := makeProtocolFinalObservation(bundle, passSnapshot, revisionGraph.Edges)
			digest, digestErr := protocol.ReplayFinalObservationDigest(final, bundle.Contract)
			if buildErr != nil || digestErr != nil {
				observation.Complete = false
				observation.ReplayGraph = ObservationAmbiguous
			} else {
				observation.FinalObservation = &final
				observation.FinalDigest = digest
			}
		}
	}
	return observation, nil
}

func replayObservedRevisionEdges(layout Layout, edges []protocol.ReplayObservedRevisionEdge, revisions []protocol.ReplayDayManifest) ([]protocol.ReplayObservedRevisionEdge, error) {
	if len(edges) != len(revisions) {
		return nil, fmt.Errorf("replay edge and revision counts differ")
	}
	result := append([]protocol.ReplayObservedRevisionEdge(nil), edges...)
	for index := range revisions {
		fullKey, err := layout.ReplayDayManifestKey(revisions[index])
		if err != nil {
			return nil, fmt.Errorf("derive replay edge key from trusted layout: %w", err)
		}
		result[index].FullKey = fullKey
	}
	return result, nil
}

func verifySealedReplayBundle(bundle ReplayPublicationBundle) error {
	canonical, err := protocol.ReplayPublicationBundleCanonicalJSON(bundle.Contract)
	if err != nil {
		return err
	}
	digest, err := protocol.ReplayPublicationBundleDigest(bundle.Contract)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, bundle.CanonicalBytes) || digest != bundle.Digest {
		return fmt.Errorf("sealed replay bundle identity is inconsistent")
	}
	return nil
}

func (o *ReplayBoundedObserver) observeMetadata(ctx context.Context, budget *ReplayObservationBudget, key string, expectedBytes uint64, expected []byte, validate func([]byte) error) ObservationClass {
	read := o.readRemoteObject(ctx, budget, key, replayBudgetMetadata, budget.limits.MaxMetadataObjectBytes)
	if read.Class != ObservationExact {
		return read.Class
	}
	if validate != nil {
		if err := validate(read.Body); err != nil {
			return ObservationAmbiguous
		}
	} else {
		value, err := protocol.DecodeCanonicalJSON(read.Body)
		if err != nil || !validPublisherClaimShape(value) {
			return ObservationAmbiguous
		}
	}
	if uint64(len(read.Body)) != expectedBytes || !bytes.Equal(read.Body, expected) {
		return ObservationDifferent
	}
	return ObservationExact
}

func (o *ReplayBoundedObserver) observeRawManifest(ctx context.Context, budget *ReplayObservationBudget, bundle ReplayPublicationBundle) ObservationClass {
	expected := bundle.Contract.RawManifest
	read := o.readRemoteObject(ctx, budget, expected.FullKey, replayBudgetMetadata, bundle.Contract.Limits.MaxMetadataObjectBytes)
	if read.Class != ObservationExact {
		return read.Class
	}
	manifest, err := archive.VerifyRawDayManifest(read.Body)
	if err != nil {
		return ObservationAmbiguous
	}
	canonical, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, read.Body) {
		return ObservationAmbiguous
	}
	if manifest.DatasetID != bundle.Contract.Scope.DatasetID || manifest.CampaignID != bundle.Contract.Scope.CampaignID || manifest.DayDefinitionID != bundle.Contract.Scope.DayDefinitionID || manifest.Date != bundle.Contract.Scope.Date || manifest.PublisherID != bundle.Contract.Scope.PublisherID || manifest.PublisherEpoch != bundle.Contract.Scope.PublisherEpoch || manifest.SettlePolicy != bundle.Contract.Scope.SettlePolicy || manifest.ConfigHash != mustHash(bundle.Contract.Scope.ScopeConfigHash) {
		return ObservationAmbiguous
	}
	if uint64(len(read.Body)) != expected.Bytes || manifest.ManifestSHA256 != mustHash(expected.DomainDigest) {
		return ObservationDifferent
	}
	return ObservationExact
}

func validPublisherClaimShape(value any) bool {
	object, ok := value.(map[string]any)
	if !ok || len(object) != 13 {
		return false
	}
	for _, field := range []string{"broker_server_fingerprint", "campaign_id", "claim_version", "config_hash", "dataset_id", "day_definition_id", "exact_source_symbol", "provider_id", "publisher_id", "scope_key", "settle_policy", "stable_feed_id"} {
		if text, ok := object[field].(string); !ok || text == "" {
			return false
		}
	}
	_, ok = object["publisher_epoch"].(uint64)
	return ok
}

func (o *ReplayBoundedObserver) observeHashedObject(ctx context.Context, budget *ReplayObservationBudget, key string, expectedBytes uint64, expectedDigest string, kind replayBudgetByteKind, objectLimit uint64) ObservationClass {
	hash := sha256.New()
	consumed, class := o.consumeRemoteObject(ctx, budget, key, kind, objectLimit, hash)
	if class != ObservationExact {
		return class
	}
	if consumed != expectedBytes || hex.EncodeToString(hash.Sum(nil)) != expectedDigest {
		return ObservationDifferent
	}
	return ObservationExact
}

type replayRemoteReadResult struct {
	Body  []byte
	Class ObservationClass
}

func (o *ReplayBoundedObserver) readRemoteObject(ctx context.Context, budget *ReplayObservationBudget, key string, kind replayBudgetByteKind, objectLimit uint64) replayRemoteReadResult {
	buffer := bytes.NewBuffer(make([]byte, 0, minReplayReadCapacity(objectLimit)))
	_, class := o.consumeRemoteObject(ctx, budget, key, kind, objectLimit, buffer)
	if class != ObservationExact {
		return replayRemoteReadResult{Class: class}
	}
	return replayRemoteReadResult{Body: buffer.Bytes(), Class: ObservationExact}
}

func (o *ReplayBoundedObserver) consumeRemoteObject(ctx context.Context, budget *ReplayObservationBudget, key string, kind replayBudgetByteKind, objectLimit uint64, destination io.Writer) (uint64, ObservationClass) {
	consumption := o.consumeRemoteObjectDetailed(ctx, budget, key, kind, objectLimit, destination)
	return consumption.Bytes, consumption.Class
}

type replayRemoteConsumption struct {
	Bytes          uint64
	AdvertisedSize int64
	Class          ObservationClass
}

func (o *ReplayBoundedObserver) consumeRemoteObjectDetailed(ctx context.Context, budget *ReplayObservationBudget, key string, kind replayBudgetByteKind, objectLimit uint64, destination io.Writer) replayRemoteConsumption {
	if err := budget.ChargeRequest(); err != nil {
		return replayRemoteConsumption{Class: ObservationOversized}
	}
	capBytes, err := budget.ReadCap(kind, objectLimit)
	if err != nil {
		return replayRemoteConsumption{Class: ObservationOversized}
	}
	body, advertisedSize, err := o.remote.OpenLimited(ctx, key, capBytes)
	if err != nil {
		return replayRemoteConsumption{Class: classifyReplayRemoteError(err, false)}
	}
	consumed, readErr := io.Copy(destination, io.LimitReader(body, int64(capBytes)+1))
	closeErr := body.Close()
	chargeErr := chargeReplayReadBytes(budget, kind, uint64(consumed))
	result := replayRemoteConsumption{Bytes: uint64(consumed), AdvertisedSize: advertisedSize}
	if chargeErr != nil || uint64(consumed) > capBytes {
		result.Class = ObservationOversized
		return result
	}
	if readErr != nil || closeErr != nil || advertisedSize < 0 || consumed != advertisedSize {
		result.Class = ObservationUnavailable
		return result
	}
	result.Class = ObservationExact
	return result
}

func chargeReplayReadBytes(budget *ReplayObservationBudget, kind replayBudgetByteKind, consumed uint64) error {
	switch kind {
	case replayBudgetMetadata:
		return budget.ChargeMetadata(consumed)
	case replayBudgetParquet:
		return budget.ChargeParquet(consumed)
	case replayBudgetObservation:
		return budget.ChargeObservation(consumed)
	default:
		return ErrResourceLimit
	}
}

func minReplayReadCapacity(capBytes uint64) int {
	const maximumInitialCapacity = 64 * 1024
	if capBytes < maximumInitialCapacity {
		return int(capBytes)
	}
	return maximumInitialCapacity
}

func (o *ReplayBoundedObserver) observeInventory(ctx context.Context, budget *ReplayObservationBudget, prefix string, max uint64) (map[string]RemoteObject, ObservationClass) {
	if err := budget.ChargeRequest(); err != nil {
		return nil, ObservationOversized
	}
	listed, err := o.remote.ListLimited(ctx, prefix, max)
	if err != nil {
		return nil, classifyReplayRemoteError(err, false)
	}
	if !listed.Complete {
		return nil, ObservationAmbiguous
	}
	sort.Slice(listed.Objects, func(i, j int) bool { return listed.Objects[i].Key < listed.Objects[j].Key })
	result := make(map[string]RemoteObject, len(listed.Objects))
	for _, object := range listed.Objects {
		if object.Size < 0 || !strings.HasPrefix(object.Key, prefix) {
			return nil, ObservationAmbiguous
		}
		if !validReplayInventoryKey(prefix, object.Key) {
			return nil, ObservationAmbiguous
		}
		if _, duplicate := result[object.Key]; duplicate {
			return nil, ObservationAmbiguous
		}
		if err := budget.ChargeObservation(uint64(len(object.Key)) + 8); err != nil {
			return nil, ObservationOversized
		}
		result[object.Key] = object
	}
	return result, ObservationExact
}

func validReplayInventoryKey(prefix, key string) bool {
	relative := strings.TrimPrefix(key, prefix)
	if relative == key || strings.Contains(relative, "/../") || strings.Contains(relative, "//") {
		return false
	}
	return strings.HasPrefix(relative, "parquet/") && strings.HasSuffix(relative, ".parquet") ||
		strings.HasPrefix(relative, "manifests/part-") && strings.HasSuffix(relative, ".json") ||
		strings.HasPrefix(relative, "replay-day-") && strings.HasSuffix(relative, ".json")
}

func (o *ReplayBoundedObserver) observeParquetCandidate(ctx context.Context, budget *ReplayObservationBudget, bundle ReplayPublicationBundle, object protocol.ReplayPublicationParquetObject, inventory map[string]RemoteObject) ObservationClass {
	listed, ok := inventory[object.FullKey]
	if !ok {
		return ObservationAbsent
	}
	artifact, ok := bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)]
	if !ok || artifact.Path == "" || artifact.Bytes != object.Bytes || artifact.Digest != object.SHA256 {
		return ObservationAmbiguous
	}
	hash := sha256.New()
	consumption := o.consumeRemoteObjectDetailed(ctx, budget, object.FullKey, replayBudgetParquet, budget.limits.MaxParquetObjectBytes, hash)
	if consumption.Class != ObservationExact {
		return consumption.Class
	}
	if listed.Size != int64(object.Bytes) || consumption.AdvertisedSize != listed.Size || consumption.Bytes != object.Bytes {
		return ObservationAmbiguous
	}
	if hex.EncodeToString(hash.Sum(nil)) != object.SHA256 {
		return ObservationDifferent
	}
	return ObservationExact
}

func (o *ReplayBoundedObserver) observePartManifest(ctx context.Context, budget *ReplayObservationBudget, object protocol.ReplayPublicationPartManifest, inventory map[string]RemoteObject) (ObservationClass, protocol.PartManifest) {
	listed, ok := inventory[object.FullKey]
	if !ok {
		return ObservationAbsent, protocol.PartManifest{}
	}
	read := o.readRemoteObject(ctx, budget, object.FullKey, replayBudgetMetadata, budget.limits.MaxMetadataObjectBytes)
	if read.Class != ObservationExact {
		return read.Class, protocol.PartManifest{}
	}
	if int64(len(read.Body)) != listed.Size {
		return ObservationAmbiguous, protocol.PartManifest{}
	}
	part, err := archive.VerifyPartManifestObject(read.Body, object.RelativeKey, mustHash(object.DomainDigest))
	if err != nil {
		return ObservationAmbiguous, protocol.PartManifest{}
	}
	if uint64(len(read.Body)) != object.Bytes {
		return ObservationDifferent, protocol.PartManifest{}
	}
	return ObservationExact, part
}

func (o *ReplayBoundedObserver) observeReplayRevisions(ctx context.Context, budget *ReplayObservationBudget, bundle ReplayPublicationBundle, inventory map[string]RemoteObject, scope protocol.ReplayScope, conversion archive.ConversionTuple, partGraph ReplayPartGraph) ([]protocol.ReplayDayManifest, ObservationClass) {
	_, candidatePresent := inventory[bundle.Contract.ReplayManifest.FullKey]
	keys := make([]string, 0)
	for key := range inventory {
		if strings.Contains(key, "/replay-day-") && strings.HasSuffix(key, ".json") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil, ObservationAbsent
	}
	revisions := make([]protocol.ReplayDayManifest, 0, len(keys))
	for _, key := range keys {
		listed := inventory[key]
		read := o.readRemoteObject(ctx, budget, key, replayBudgetMetadata, budget.limits.MaxMetadataObjectBytes)
		if read.Class != ObservationExact {
			return nil, read.Class
		}
		if int64(len(read.Body)) != listed.Size {
			return nil, ObservationAmbiguous
		}
		manifest, err := protocol.VerifyReplayDayManifest(read.Body)
		if err != nil {
			return nil, ObservationAmbiguous
		}
		canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(canonical, read.Body) {
			return nil, ObservationAmbiguous
		}
		digest, err := protocol.ReplayDayManifestDigest(manifest)
		if err != nil {
			return nil, ObservationAmbiguous
		}
		fullKey, err := bundle.Layout.ReplayDayManifestKey(manifest)
		if err != nil || fullKey != key {
			return nil, ObservationDifferent
		}
		manifest.ManifestSHA256 = digest
		revisions = append(revisions, manifest)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i].Revision < revisions[j].Revision })
	graph, err := VerifyReplayRevisionGraph(revisions, budget.limits.MaxGraphNodes)
	if err != nil {
		return revisions, graphClass(err, false)
	}
	if !candidatePresent {
		return revisions, ObservationAbsent
	}
	last := graph.Revisions[len(graph.Revisions)-1]
	previous := (*protocol.ReplayDayManifest)(nil)
	if len(graph.Revisions) > 1 {
		previous = &graph.Revisions[len(graph.Revisions)-2]
	}
	verified, class := o.observeTerminalReplayCandidate(ctx, budget, bundle, scope, conversion, partGraph, previous)
	if class != ObservationExact {
		return revisions, class
	}
	if verified.ManifestSHA256 != last.ManifestSHA256 {
		return revisions, ObservationAmbiguous
	}
	return revisions, ObservationExact
}

// observeTerminalReplayCandidate preserves the typed outcome of the second
// fresh terminal read. It never collapses unavailable or oversized reads into
// malformed bytes.
func (o *ReplayBoundedObserver) observeTerminalReplayCandidate(ctx context.Context, budget *ReplayObservationBudget, bundle ReplayPublicationBundle, scope protocol.ReplayScope, conversion archive.ConversionTuple, partGraph ReplayPartGraph, previous *protocol.ReplayDayManifest) (protocol.ReplayDayManifest, ObservationClass) {
	object := bundle.Contract.ReplayManifest
	read := o.readRemoteObject(ctx, budget, object.FullKey, replayBudgetMetadata, budget.limits.MaxMetadataObjectBytes)
	if read.Class != ObservationExact {
		return protocol.ReplayDayManifest{}, read.Class
	}
	manifest, err := protocol.VerifyReplayDayManifest(read.Body)
	if err != nil {
		return protocol.ReplayDayManifest{}, ObservationAmbiguous
	}
	canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, read.Body) {
		return protocol.ReplayDayManifest{}, ObservationAmbiguous
	}
	digest, err := protocol.ReplayDayManifestDigest(manifest)
	if err != nil {
		return protocol.ReplayDayManifest{}, ObservationAmbiguous
	}
	if uint64(len(read.Body)) != object.Bytes || digest != mustHash(object.DomainDigest) {
		return protocol.ReplayDayManifest{}, ObservationDifferent
	}
	verified, err := archive.VerifyReplayDayManifestObject(read.Body, object.RelativeKey, scope, conversion, partGraph.Parts, previous, partGraph.CanonicalRowChainRoot)
	if err != nil || verified.ManifestSHA256 != digest {
		return protocol.ReplayDayManifest{}, ObservationDifferent
	}
	return verified, ObservationExact
}

func classifyReplayRemoteError(err error, candidateMissing bool) ObservationClass {
	if errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrMetadataTooLarge) {
		return ObservationOversized
	}
	if errors.Is(err, ErrObjectNotFound) {
		if candidateMissing {
			return ObservationAbsent
		}
		return ObservationAmbiguous
	}
	return ObservationUnavailable
}

func graphClass(err error, complete bool) ObservationClass {
	if err == nil && complete {
		return ObservationExact
	}
	if errors.Is(err, ErrResourceLimit) {
		return ObservationOversized
	}
	return ObservationAmbiguous
}

func replayVerificationInputs(bundle protocol.ReplayPublicationBundle) (protocol.ReplayScope, archive.ConversionTuple, error) {
	scope := protocol.ReplayScope{DatasetID: bundle.Scope.DatasetID, CampaignID: bundle.Scope.CampaignID, DayDefinitionID: bundle.Scope.DayDefinitionID, Date: bundle.Scope.Date, ReplayContractID: bundle.Conversion.ReplayContractID, ConversionID: bundle.Conversion.ConversionID, RawDayManifestKey: bundle.RawManifest.RelativeKey, RawDayManifestSHA256: mustHash(bundle.RawManifest.DomainDigest)}
	conversion := archive.ConversionTuple{ReplayContractID: bundle.Conversion.ReplayContractID, FormatID: bundle.Conversion.FormatID, ConversionID: bundle.Conversion.ConversionID, ConverterBuildID: bundle.Conversion.ConverterBuildID, DependencyLockHash: mustHash(bundle.Conversion.DependencyLockHash), WriterConfigurationHash: mustHash(bundle.Conversion.WriterConfigurationHash), TargetPlatformContract: bundle.Conversion.TargetPlatformContract}
	return scope, conversion, scope.Validate()
}

func mustHash(value string) [32]byte {
	decoded, _ := hex.DecodeString(value)
	var result [32]byte
	copy(result[:], decoded)
	return result
}

func replayObservationAllExact(observation ReplayRemoteObservation) bool {
	if observation.Claim != ObservationExact || observation.RawManifest != ObservationExact || observation.PartChain != ObservationExact || observation.ReplayManifest != ObservationExact || observation.ReplayGraph != ObservationExact {
		return false
	}
	for _, list := range [][]ReplayObjectObservation{observation.RawObjects, observation.ParquetObjects, observation.PartManifests} {
		for _, object := range list {
			if object.Class != ObservationExact {
				return false
			}
		}
	}
	return true
}

func chargeReplayFinalObservationUplift(budget *ReplayObservationBudget, start ReplayObservationBudgetSnapshot, bundle ReplayPublicationBundle, edges []protocol.ReplayObservedRevisionEdge) (ReplayObservationBudgetSnapshot, ObservationClass, error) {
	before := budget.Snapshot()
	pass, err := subtractReplayBudgetSnapshots(before, start)
	if err != nil {
		return ReplayObservationBudgetSnapshot{}, ObservationAmbiguous, err
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, edges)
	if err != nil {
		return ReplayObservationBudgetSnapshot{}, ObservationAmbiguous, err
	}
	if pass.ObservationBytes < required {
		uplift := required - pass.ObservationBytes
		if err := budget.ChargeObservation(uplift); err != nil {
			after, snapshotErr := subtractReplayBudgetSnapshots(budget.Snapshot(), start)
			return after, ObservationOversized, snapshotErr
		}
	}
	after, err := subtractReplayBudgetSnapshots(budget.Snapshot(), start)
	if err != nil {
		return ReplayObservationBudgetSnapshot{}, ObservationAmbiguous, err
	}
	return after, ObservationExact, nil
}

func subtractReplayBudgetSnapshots(current, start ReplayObservationBudgetSnapshot) (ReplayObservationBudgetSnapshot, error) {
	if current.Requests < start.Requests || current.MetadataBytes < start.MetadataBytes || current.ParquetBytes < start.ParquetBytes || current.ObservationBytes < start.ObservationBytes {
		return ReplayObservationBudgetSnapshot{}, fmt.Errorf("replay observation budget counters moved backwards")
	}
	return ReplayObservationBudgetSnapshot{
		Requests:         current.Requests - start.Requests,
		MetadataBytes:    current.MetadataBytes - start.MetadataBytes,
		ParquetBytes:     current.ParquetBytes - start.ParquetBytes,
		ObservationBytes: current.ObservationBytes - start.ObservationBytes,
	}, nil
}

func makeProtocolFinalObservation(bundle ReplayPublicationBundle, snapshot ReplayObservationBudgetSnapshot, edges []protocol.ReplayObservedRevisionEdge) (protocol.ReplayFinalObservation, error) {
	contract := bundle.Contract
	derivatives := make([]protocol.ReplayObservedDerivativeObject, 0, len(contract.ParquetObjects)+len(contract.PartManifests)+1)
	for _, object := range contract.ParquetObjects {
		derivatives = append(derivatives, protocol.ReplayObservedDerivativeObject{Bytes: object.Bytes, Digest: object.SHA256, DigestDomain: protocol.ReplayDerivativeDigestSHA256, FullKey: object.FullKey, Kind: protocol.ReplayDerivativeKindParquet})
	}
	for _, object := range contract.PartManifests {
		derivatives = append(derivatives, protocol.ReplayObservedDerivativeObject{Bytes: object.Bytes, Digest: object.DomainDigest, DigestDomain: protocol.ReplayDerivativeDigestPart, FullKey: object.FullKey, Kind: protocol.ReplayDerivativeKindPartManifest})
	}
	derivatives = append(derivatives, protocol.ReplayObservedDerivativeObject{Bytes: contract.ReplayManifest.Bytes, Digest: contract.ReplayManifest.DomainDigest, DigestDomain: protocol.ReplayDerivativeDigestReplay, FullKey: contract.ReplayManifest.FullKey, Kind: protocol.ReplayDerivativeKindReplay})
	sort.Slice(derivatives, func(i, j int) bool { return derivatives[i].FullKey < derivatives[j].FullKey })
	rawObjects := make([]protocol.ReplayObservedRawObject, len(contract.RawObjects))
	for index, object := range contract.RawObjects {
		rawObjects[index] = protocol.ReplayObservedRawObject{Bytes: object.Bytes, FullKey: object.FullKey, SHA256: object.SHA256}
	}
	observation := protocol.ReplayFinalObservation{ObservationVersion: protocol.ReplayFinalObservationVersion, BundleDigest: hex.EncodeToString(bundle.Digest[:]), Claim: contract.Claim, Complete: true, DerivativeObjects: derivatives, ObservationBytes: snapshot.ObservationBytes, ObservationRequests: snapshot.Requests, RawManifest: protocol.ReplayObservedRawManifest{Bytes: contract.RawManifest.Bytes, DomainDigest: contract.RawManifest.DomainDigest, FullKey: contract.RawManifest.FullKey}, RawObjects: rawObjects, ReplayEdges: append([]protocol.ReplayObservedRevisionEdge(nil), edges...)}
	required, err := protocol.ReplayFinalObservationRequiredBytes(contract, edges)
	if err != nil {
		return observation, err
	}
	if observation.ObservationBytes < required {
		return observation, fmt.Errorf("final observation bytes are below required budget")
	}
	return observation, nil
}
