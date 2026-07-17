package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"tick-data-platform/internal/archive"
)

type PublicationInput struct {
	Manifest      archive.RawDayManifest
	ManifestBytes []byte
	ManifestPath  string
	ObjectPaths   map[string]string
	ReceiptPath   string
}

type Publisher struct {
	layout   Layout
	backend  WriteBackend
	journal  *PublicationJournal
	lockPath string
	clock    func() time.Time
	hooks    publicationHooks
}

type publicationHooks struct {
	afterStage func(string) error
}

// NewPublisher constructs the publisher with an explicit clock.
// Production code supplies time.Now; tests supply a deterministic clock.
func NewPublisher(layout Layout, backend WriteBackend, journal *PublicationJournal, lockPath string, clock func() time.Time) (*Publisher, error) {
	if backend == nil || journal == nil {
		return nil, fmt.Errorf("publisher dependencies are incomplete")
	}
	if clock == nil {
		return nil, fmt.Errorf("publisher clock is required")
	}
	if lockPath == "" {
		var err error
		lockPath, err = PublicationLockPath(filepath.Dir(journal.Path()), layout.Scope)
		if err != nil {
			return nil, err
		}
	}
	return &Publisher{layout: layout, backend: backend, journal: journal, lockPath: lockPath, clock: clock}, nil
}

func (p *Publisher) Publish(ctx context.Context, input PublicationInput) (VerificationReceipt, error) {
	intent, err := p.prepareIntent(input)
	if err != nil {
		return VerificationReceipt{}, err
	}
	lock, err := AcquirePublicationLock(p.lockPath)
	if err != nil {
		return VerificationReceipt{}, err
	}
	defer lock.Close()

	record, err := p.journal.CreateOrGetIntent(intent)
	if err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.recordSealedLocalObjects(intent); err != nil {
		return VerificationReceipt{}, err
	}

	claimBytes, err := intent.Claim.CanonicalJSON()
	if err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.putPublisherClaim(ctx, intent.ClaimKey, claimBytes); err != nil {
		return VerificationReceipt{}, fmt.Errorf("put publisher claim: %w", err)
	}
	if err := p.advanceStage(intent.ManifestKey, StageClaimed); err != nil {
		return VerificationReceipt{}, err
	}

	if err := archive.VerifyRawDaySnapshot(intent.Manifest, objectPathMap(intent.Objects), intent.Scope); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.copyPathWithResume(ctx, intent.ScopeDescriptorPath, intent.ScopeDescriptorKey, intent.ScopeDescriptorSHA256, intent.ScopeDescriptorBytes); err != nil {
		return VerificationReceipt{}, fmt.Errorf("publish scope descriptor: %w", err)
	}

	stage := record.Stage
	if publicationStageRank(stage) < publicationStageRank(StageObjectsCopied) {
		for _, object := range intent.Objects {
			if err := p.copyWithResume(ctx, intent.ManifestKey, object); err != nil {
				return VerificationReceipt{}, fmt.Errorf("publish raw object %q: %w", object.Key, err)
			}
		}
		if err := p.advanceStage(intent.ManifestKey, StageObjectsCopied); err != nil {
			return VerificationReceipt{}, err
		}
	} else if err := p.recheckOrRepairObjects(ctx, intent.ManifestKey, intent.Objects); err != nil {
		return VerificationReceipt{}, fmt.Errorf("recheck or repair raw objects: %w", err)
	}
	if err := p.checkObjects(ctx, intent.ManifestKey, intent.Objects); err != nil {
		return VerificationReceipt{}, fmt.Errorf("verify raw objects: %w", err)
	}
	if err := p.advanceStage(intent.ManifestKey, StageObjectsVerified); err != nil {
		return VerificationReceipt{}, err
	}

	if err := p.checkObjects(ctx, intent.ManifestKey, intent.Objects); err != nil {
		return VerificationReceipt{}, fmt.Errorf("verify raw objects before manifest: %w", err)
	}
	if _, err := p.backend.VerifyFile(ctx, intent.ScopeDescriptorKey, intent.ScopeDescriptorPath, intent.ScopeDescriptorSHA256, intent.ScopeDescriptorBytes); err != nil {
		return VerificationReceipt{}, fmt.Errorf("verify scope descriptor: %w", err)
	}
	existing, err := LoadManifestRecords(ctx, p.backend, p.layout, intent.Manifest.Date)
	if err != nil {
		return VerificationReceipt{}, fmt.Errorf("load existing manifest records: %w", err)
	}
	same, err := ValidateRevisionGraph(intent.Manifest, intent.ManifestBytes, existing)
	if err != nil {
		return VerificationReceipt{}, err
	}
	manifestContentSHA256 := sha256.Sum256(intent.ManifestBytes)
	manifestBytes := uint64(len(intent.ManifestBytes))
	if same {
		if _, err := p.backend.VerifyFile(ctx, intent.ManifestKey, intent.ManifestPath, manifestContentSHA256, manifestBytes); err != nil {
			return VerificationReceipt{}, fmt.Errorf("verify existing manifest: %w", err)
		}
	} else {
		if err := p.copyManifestWithResume(ctx, intent.ManifestPath, intent.ManifestKey, manifestContentSHA256, manifestBytes, intent.ManifestBytes); err != nil {
			return VerificationReceipt{}, fmt.Errorf("publish manifest: %w", err)
		}
	}
	if err := p.advanceStage(intent.ManifestKey, StageManifestCopied); err != nil {
		return VerificationReceipt{}, err
	}
	if _, err := p.backend.VerifyFile(ctx, intent.ManifestKey, intent.ManifestPath, manifestContentSHA256, manifestBytes); err != nil {
		return VerificationReceipt{}, fmt.Errorf("verify manifest: %w", err)
	}
	if err := p.advanceStage(intent.ManifestKey, StageManifestVerified); err != nil {
		return VerificationReceipt{}, err
	}

	receipt := VerificationReceipt{
		ReceiptVersion:       "publication-verification-receipt-v1",
		ClaimHash:            intent.ClaimHash,
		ScopeConfigHash:      intent.Manifest.ConfigHash,
		ManifestKey:          intent.ManifestKey,
		ManifestSHA256:       intent.Manifest.ManifestSHA256,
		RawObjects:           intent.Objects,
		VerificationComplete: true,
	}
	if err := SaveVerificationReceipt(intent.ReceiptPath, receipt); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.advanceStage(intent.ManifestKey, StageReceiptSaved); err != nil {
		return VerificationReceipt{}, err
	}
	return receipt, nil
}

func (p *Publisher) advanceStage(identity, stage string) error {
	if err := p.journal.AdvanceStage(identity, stage); err != nil {
		return err
	}
	if p.hooks.afterStage != nil {
		return p.hooks.afterStage(stage)
	}
	return nil
}

func (p *Publisher) prepareIntent(input PublicationInput) (PublicationIntent, error) {
	if len(input.ManifestBytes) == 0 {
		var err error
		input.ManifestBytes, err = archive.ManifestCanonicalJSON(input.Manifest)
		if err != nil {
			return PublicationIntent{}, err
		}
	}
	decoded, err := archive.VerifyRawDayManifest(input.ManifestBytes)
	if err != nil {
		return PublicationIntent{}, fmt.Errorf("%w: manifest is not canonical", archive.ErrIntegrity)
	}
	canonical, err := archive.ManifestCanonicalJSON(decoded)
	if err != nil || !bytes.Equal(canonical, input.ManifestBytes) {
		return PublicationIntent{}, fmt.Errorf("%w: manifest bytes are not canonical", archive.ErrIntegrity)
	}
	if err := archive.ValidateRawDayManifest(decoded); err != nil {
		return PublicationIntent{}, fmt.Errorf("%w: manifest is invalid", archive.ErrIntegrity)
	}
	if err := manifestMatchesScope(decoded, p.layout.Scope); err != nil {
		return PublicationIntent{}, err
	}
	if len(input.ObjectPaths) == 0 && len(decoded.ChainObjects) != 0 {
		return PublicationIntent{}, fmt.Errorf("%w: chain object paths are missing", archive.ErrIntegrity)
	}
	if err := archive.VerifyRawDaySnapshot(decoded, input.ObjectPaths, p.layout.Scope); err != nil {
		return PublicationIntent{}, err
	}
	manifestKey, err := p.layout.ManifestKey(decoded)
	if err != nil {
		return PublicationIntent{}, err
	}
	claim, err := NewPublisherClaim(p.layout.Scope)
	if err != nil {
		return PublicationIntent{}, err
	}
	claimHash, err := claim.Digest()
	if err != nil {
		return PublicationIntent{}, err
	}
	claimKey, err := p.layout.ClaimKey(claim.PublisherEpoch)
	if err != nil {
		return PublicationIntent{}, err
	}
	manifestPath := input.ManifestPath
	if manifestPath == "" {
		manifestPath = filepath.Join(filepath.Dir(p.journal.Path()), ".manifest-"+fmt.Sprintf("%x", decoded.ManifestSHA256)+".json")
	}
	if err := saveNoClobberBytes(manifestPath, input.ManifestBytes, ".manifest-*.tmp"); err != nil {
		return PublicationIntent{}, err
	}
	receiptPath := input.ReceiptPath
	if receiptPath == "" {
		receiptPath = filepath.Join(filepath.Dir(p.journal.Path()), ".receipt-"+fmt.Sprintf("%x", decoded.ManifestSHA256)+".json")
	}
	configBytes, err := p.layout.Scope.CanonicalConfigJSON()
	if err != nil {
		return PublicationIntent{}, err
	}
	descriptorKey, err := p.layout.ScopeDescriptorKey()
	if err != nil {
		return PublicationIntent{}, err
	}
	descriptorHash := sha256.Sum256(configBytes)
	descriptorPath := filepath.Join(filepath.Dir(p.journal.Path()), ".scope-descriptor-"+archive.IdentityPathKey(string(configBytes))+".json")
	if err := saveNoClobberBytes(descriptorPath, configBytes, ".scope-descriptor-*.tmp"); err != nil {
		return PublicationIntent{}, err
	}
	objects := make([]PublicationObject, len(decoded.ChainObjects))
	for i, object := range decoded.ChainObjects {
		localPath := input.ObjectPaths[object.Key]
		remoteKey, err := p.layout.RemoteKey(object.Key)
		if err != nil {
			return PublicationIntent{}, err
		}
		localFile, md5Digest, err := openVerifiedLocalFile(localPath, object.SHA256, object.Bytes)
		if err != nil {
			return PublicationIntent{}, err
		}
		if err := localFile.Close(); err != nil {
			return PublicationIntent{}, fmt.Errorf("%w: close local object: %v", ErrLocalObjectChanged, err)
		}
		objects[i] = PublicationObject{
			Key:       object.Key,
			LocalPath: localPath,
			SHA256:    object.SHA256,
			MD5:       md5Digest,
			Bytes:     object.Bytes,
			RemoteKey: remoteKey,
		}
	}
	return PublicationIntent{
		Scope:                 p.layout.Scope,
		Claim:                 claim,
		ClaimKey:              claimKey,
		ClaimHash:             claimHash,
		ScopeDescriptorKey:    descriptorKey,
		ScopeDescriptorPath:   descriptorPath,
		ScopeDescriptorSHA256: descriptorHash,
		ScopeDescriptorBytes:  uint64(len(configBytes)),
		ManifestKey:           manifestKey,
		Manifest:              decoded,
		ManifestBytes:         append([]byte(nil), input.ManifestBytes...),
		ManifestPath:          manifestPath,
		Objects:               objects,
		ReceiptPath:           receiptPath,
	}, nil
}

func (p *Publisher) putPublisherClaim(ctx context.Context, key string, body []byte) error {
	err := p.backend.PutIfAbsent(ctx, key, body)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrObjectExists) {
		return err
	}
	existing, getErr := p.backend.Get(ctx, key)
	if getErr != nil {
		return getErr
	}
	if !bytes.Equal(existing, body) {
		return fmt.Errorf("%w: publisher claim content differs", ErrPublisherConflict)
	}
	return nil
}

func (p *Publisher) verifyBackendBytes(ctx context.Context, key string, want []byte) error {
	got, err := p.backend.Get(ctx, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%w: remote metadata content differs", archive.ErrIntegrity)
	}
	return nil
}

func (p *Publisher) recordSealedLocalObjects(intent PublicationIntent) error {
	for _, object := range intent.Objects {
		if err := p.journal.RecordObjectState(intent.ManifestKey, object, ObjectStateSealedLocal, "", time.Time{}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) copyWithResume(ctx context.Context, identity string, object PublicationObject) error {
	if err := p.journal.RecordObjectState(identity, object, ObjectStateUploading, "", time.Time{}); err != nil {
		return err
	}
	commit, err := p.backend.PutFileIfAbsent(ctx, object.RemoteKey, object.LocalPath, object.SHA256, object.Bytes)
	if err != nil {
		return err
	}
	if err := p.journal.RecordObjectState(identity, object, ObjectStateRemoteCommitted, commit.ETag, time.Time{}); err != nil {
		return err
	}
	verification, err := p.backend.VerifyFile(ctx, object.RemoteKey, object.LocalPath, object.SHA256, object.Bytes)
	if err != nil {
		return err
	}
	return p.journal.RecordObjectState(identity, object, ObjectStateRemoteVerified, verification.ETag, p.clock().UTC())
}

func (p *Publisher) copyPathWithResume(ctx context.Context, localPath, remoteKey string, digest [32]byte, bytes uint64) error {
	if _, err := p.backend.PutFileIfAbsent(ctx, remoteKey, localPath, digest, bytes); err != nil {
		return err
	}
	_, err := p.backend.VerifyFile(ctx, remoteKey, localPath, digest, bytes)
	return err
}

func (p *Publisher) recheckOrRepairObjects(ctx context.Context, identity string, objects []PublicationObject) error {
	for _, object := range objects {
		verification, err := p.backend.VerifyFile(ctx, object.RemoteKey, object.LocalPath, object.SHA256, object.Bytes)
		if err == nil {
			if err := p.journal.RecordObjectState(identity, object, ObjectStateRemoteVerified, verification.ETag, p.clock().UTC()); err != nil {
				return err
			}
			continue
		}
		if err := p.copyWithResume(ctx, identity, object); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) checkObjects(ctx context.Context, identity string, objects []PublicationObject) error {
	for _, object := range objects {
		verification, err := p.backend.VerifyFile(ctx, object.RemoteKey, object.LocalPath, object.SHA256, object.Bytes)
		if err != nil {
			return err
		}
		if err := p.journal.RecordObjectState(identity, object, ObjectStateRemoteVerified, verification.ETag, p.clock().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) copyManifestWithResume(ctx context.Context, localPath, remoteKey string, digest [32]byte, bytes uint64, body []byte) error {
	if _, err := p.backend.PutFileIfAbsent(ctx, remoteKey, localPath, digest, bytes); err != nil {
		return err
	}
	return p.verifyBackendBytes(ctx, remoteKey, body)
}

func objectPathMap(objects []PublicationObject) map[string]string {
	paths := make(map[string]string, len(objects))
	for _, object := range objects {
		paths[object.Key] = object.LocalPath
	}
	return paths
}

func manifestMatchesScope(manifest archive.RawDayManifest, scope archive.ScopeConfig) error {
	configHash, err := scope.ConfigHash()
	if err != nil {
		return err
	}
	if manifest.DatasetID != scope.DatasetID ||
		manifest.DayDefinitionID != scope.DayDefinitionID || manifest.PublisherID != scope.PublisherID ||
		manifest.PublisherEpoch != scope.PublisherEpoch || manifest.SettlePolicy != scope.SettlePolicy ||
		manifest.ConfigHash != configHash {
		return fmt.Errorf("%w: manifest scope does not match publisher scope", archive.ErrIntegrity)
	}
	return nil
}
