package r2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"

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
	backend  ObjectBackend
	rclone   *RcloneRunner
	journal  *PublicationJournal
	lockPath string
	hooks    publicationHooks
}

type publicationHooks struct {
	afterStage func(string) error
}

func NewPublisher(layout Layout, backend ObjectBackend, rclone *RcloneRunner, journal *PublicationJournal, lockPath string) (*Publisher, error) {
	if backend == nil || rclone == nil || journal == nil {
		return nil, fmt.Errorf("publisher dependencies are incomplete")
	}
	if lockPath == "" {
		var err error
		lockPath, err = PublicationLockPath(filepath.Dir(journal.Path()), layout.Scope)
		if err != nil {
			return nil, err
		}
	}
	return &Publisher{layout: layout, backend: backend, rclone: rclone, journal: journal, lockPath: lockPath}, nil
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

	claimBytes, err := intent.Claim.CanonicalJSON()
	if err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.putPublisherClaim(ctx, intent.ClaimKey, claimBytes); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.advanceStage(intent.ManifestKey, StageClaimed); err != nil {
		return VerificationReceipt{}, err
	}

	if err := archive.VerifyRawDaySnapshot(intent.Manifest, objectPathMap(intent.Objects)); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.copyPathWithResume(ctx, intent.ScopeDescriptorPath, intent.ScopeDescriptorRcloneKey); err != nil {
		return VerificationReceipt{}, err
	}

	stage := record.Stage
	if publicationStageRank(stage) < publicationStageRank(StageObjectsCopied) {
		for _, object := range intent.Objects {
			if err := p.copyWithResume(ctx, object); err != nil {
				return VerificationReceipt{}, err
			}
		}
		if err := p.advanceStage(intent.ManifestKey, StageObjectsCopied); err != nil {
			return VerificationReceipt{}, err
		}
	} else if err := p.recheckOrRepairObjects(ctx, intent.Objects); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.checkObjects(ctx, intent.Objects); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.advanceStage(intent.ManifestKey, StageObjectsVerified); err != nil {
		return VerificationReceipt{}, err
	}

	if err := p.checkObjects(ctx, intent.Objects); err != nil {
		return VerificationReceipt{}, err
	}
	existing, err := LoadManifestRecords(ctx, p.backend, p.layout, intent.Manifest.Date)
	if err != nil {
		return VerificationReceipt{}, err
	}
	same, err := ValidateRevisionGraph(intent.Manifest, intent.ManifestBytes, existing)
	if err != nil {
		return VerificationReceipt{}, err
	}
	manifestRcloneKey, err := p.layout.RcloneManifestKey(intent.Manifest)
	if err != nil {
		return VerificationReceipt{}, err
	}
	if same {
		if err := p.rclone.CheckDownload(ctx, intent.ManifestPath, manifestRcloneKey); err != nil {
			return VerificationReceipt{}, err
		}
	} else {
		if err := p.copyManifestWithResume(ctx, intent.ManifestPath, manifestRcloneKey, intent.ManifestKey, intent.ManifestBytes); err != nil {
			return VerificationReceipt{}, err
		}
	}
	if err := p.advanceStage(intent.ManifestKey, StageManifestCopied); err != nil {
		return VerificationReceipt{}, err
	}
	if err := p.rclone.CheckDownload(ctx, intent.ManifestPath, manifestRcloneKey); err != nil {
		return VerificationReceipt{}, err
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
		RcloneVersion:        RcloneVersion,
		RcloneGOOS:           p.rclone.Tool().GOOS,
		RcloneGOARCH:         p.rclone.Tool().GOARCH,
		RcloneBinarySHA256:   p.rclone.Tool().BinarySHA256,
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
	if err := archive.VerifyRawDaySnapshot(decoded, input.ObjectPaths); err != nil {
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
	descriptorRcloneKey, err := p.layout.RcloneKey("scope-descriptor-v1.json")
	if err != nil {
		return PublicationIntent{}, err
	}
	descriptorPath := filepath.Join(filepath.Dir(p.journal.Path()), ".scope-descriptor-"+archive.IdentityPathKey(string(configBytes))+".json")
	if err := saveNoClobberBytes(descriptorPath, configBytes, ".scope-descriptor-*.tmp"); err != nil {
		return PublicationIntent{}, err
	}
	objects := make([]PublicationObject, len(decoded.ChainObjects))
	for i, object := range decoded.ChainObjects {
		localPath := input.ObjectPaths[object.Key]
		rcloneKey, err := p.layout.RcloneRawObjectKey(object)
		if err != nil {
			return PublicationIntent{}, err
		}
		remoteKey, err := p.layout.RemoteKey(object.Key)
		if err != nil {
			return PublicationIntent{}, err
		}
		objects[i] = PublicationObject{
			Key:       object.Key,
			LocalPath: localPath,
			SHA256:    object.SHA256,
			Bytes:     object.Bytes,
			RemoteKey: remoteKey,
			RcloneKey: rcloneKey,
		}
	}
	return PublicationIntent{
		Scope:                    p.layout.Scope,
		Claim:                    claim,
		ClaimKey:                 claimKey,
		ClaimHash:                claimHash,
		ScopeDescriptorKey:       descriptorKey,
		ScopeDescriptorRcloneKey: descriptorRcloneKey,
		ScopeDescriptorPath:      descriptorPath,
		ManifestKey:              manifestKey,
		Manifest:                 decoded,
		ManifestBytes:            append([]byte(nil), input.ManifestBytes...),
		ManifestPath:             manifestPath,
		Objects:                  objects,
		ReceiptPath:              receiptPath,
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

func (p *Publisher) copyWithResume(ctx context.Context, object PublicationObject) error {
	return p.copyPathWithResume(ctx, object.LocalPath, object.RcloneKey)
}

func (p *Publisher) copyPathWithResume(ctx context.Context, localPath, rcloneKey string) error {
	if err := p.rclone.CopyToImmutable(ctx, localPath, rcloneKey); err != nil {
		if checkErr := p.rclone.CheckDownload(ctx, localPath, rcloneKey); checkErr != nil {
			return err
		}
	}
	return p.rclone.CheckDownload(ctx, localPath, rcloneKey)
}

func (p *Publisher) recheckOrRepairObjects(ctx context.Context, objects []PublicationObject) error {
	for _, object := range objects {
		if err := p.rclone.CheckDownload(ctx, object.LocalPath, object.RcloneKey); err != nil {
			if copyErr := p.rclone.CopyToImmutable(ctx, object.LocalPath, object.RcloneKey); copyErr != nil {
				return copyErr
			}
			if checkErr := p.rclone.CheckDownload(ctx, object.LocalPath, object.RcloneKey); checkErr != nil {
				return checkErr
			}
		}
	}
	return nil
}

func (p *Publisher) checkObjects(ctx context.Context, objects []PublicationObject) error {
	for _, object := range objects {
		if err := p.rclone.CheckDownload(ctx, object.LocalPath, object.RcloneKey); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) copyManifestWithResume(ctx context.Context, localPath, rcloneKey, remoteKey string, body []byte) error {
	if err := p.rclone.CopyToImmutable(ctx, localPath, rcloneKey); err != nil {
		if checkErr := p.rclone.CheckDownload(ctx, localPath, rcloneKey); checkErr == nil {
			return p.verifyBackendBytes(ctx, remoteKey, body)
		}
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
	if manifest.DatasetID != scope.DatasetID || manifest.CampaignID != scope.CampaignID ||
		manifest.DayDefinitionID != scope.DayDefinitionID || manifest.PublisherID != scope.PublisherID ||
		manifest.PublisherEpoch != scope.PublisherEpoch || manifest.SettlePolicy != scope.SettlePolicy ||
		manifest.ConfigHash != configHash {
		return fmt.Errorf("%w: manifest scope does not match publisher scope", archive.ErrIntegrity)
	}
	return nil
}
