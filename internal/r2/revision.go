package r2

import (
	"bytes"
	"context"
	"fmt"

	"tick-data-platform/internal/archive"
)

type ManifestRecord struct {
	Key      string
	Bytes    []byte
	Manifest archive.RawDayManifest
}

func LoadManifestRecords(ctx context.Context, backend ObjectBackend, layout Layout, date string) ([]ManifestRecord, error) {
	prefix, err := layout.ManifestPrefix(date)
	if err != nil {
		return nil, err
	}
	objects, err := backend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	records := make([]ManifestRecord, 0, len(objects))
	for _, object := range objects {
		body, err := backend.Get(ctx, object.Key)
		if err != nil {
			return nil, fmt.Errorf("read manifest metadata object: %w", err)
		}
		manifest, err := archive.VerifyRawDayManifest(body)
		if err != nil {
			return nil, fmt.Errorf("%w: remote manifest is not canonical", archive.ErrIntegrity)
		}
		canonical, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(canonical, body) {
			return nil, fmt.Errorf("%w: remote manifest bytes are not canonical", archive.ErrIntegrity)
		}
		wantKey, err := layout.ManifestKey(manifest)
		if err != nil || object.Key != wantKey {
			return nil, fmt.Errorf("%w: remote manifest key does not match its digest", archive.ErrIntegrity)
		}
		if object.Size >= 0 && int64(len(body)) != object.Size {
			return nil, fmt.Errorf("%w: remote manifest size changed during read", archive.ErrIntegrity)
		}
		records = append(records, ManifestRecord{Key: object.Key, Bytes: append([]byte(nil), body...), Manifest: manifest})
	}
	return records, nil
}

func ValidateRevisionGraph(candidate archive.RawDayManifest, candidateBytes []byte, existing []ManifestRecord) (bool, error) {
	decoded, err := archive.VerifyRawDayManifest(candidateBytes)
	if err != nil {
		return false, fmt.Errorf("%w: candidate manifest is not canonical", archive.ErrIntegrity)
	}
	wantBytes, err := archive.ManifestCanonicalJSON(decoded)
	if err != nil || !bytes.Equal(wantBytes, candidateBytes) || decoded.ManifestSHA256 != candidate.ManifestSHA256 {
		return false, fmt.Errorf("%w: candidate manifest digest or bytes differ", archive.ErrIntegrity)
	}
	if err := archive.ValidateRawDayManifest(candidate); err != nil {
		return false, fmt.Errorf("%w: candidate manifest is invalid", archive.ErrIntegrity)
	}

	byDigest := make(map[[32]byte]ManifestRecord, len(existing)+1)
	byRevision := make(map[uint64]ManifestRecord, len(existing))
	children := make(map[[32]byte][32]byte, len(existing))
	for _, record := range existing {
		if err := validateManifestRecord(record); err != nil {
			return false, err
		}
		if !sameManifestScope(record.Manifest, candidate) {
			return false, fmt.Errorf("%w: remote manifest scope or publisher differs", archive.ErrIntegrity)
		}
		digest := record.Manifest.ManifestSHA256
		if prior, ok := byDigest[digest]; ok && !bytes.Equal(prior.Bytes, record.Bytes) {
			return false, fmt.Errorf("%w: duplicate manifest digest has different bytes", archive.ErrIntegrity)
		}
		if prior, ok := byRevision[record.Manifest.Revision]; ok && prior.Manifest.ManifestSHA256 != digest {
			return false, fmt.Errorf("%w: duplicate revision has different digest", ErrPublisherConflict)
		}
		byDigest[digest] = record
		byRevision[record.Manifest.Revision] = record
	}
	for _, record := range existing {
		manifest := record.Manifest
		if manifest.Revision == 1 {
			if manifest.PreviousManifestSHA256 != nil {
				return false, fmt.Errorf("%w: genesis manifest has a predecessor", archive.ErrIntegrity)
			}
			continue
		}
		if manifest.PreviousManifestSHA256 == nil {
			return false, fmt.Errorf("%w: revision predecessor is missing", archive.ErrIntegrity)
		}
		predecessor, ok := byDigest[*manifest.PreviousManifestSHA256]
		if !ok || predecessor.Manifest.Revision+1 != manifest.Revision {
			return false, fmt.Errorf("%w: revision predecessor is absent or not adjacent", archive.ErrIntegrity)
		}
		if err := validateRevisionSuccessor(predecessor.Manifest, manifest); err != nil {
			return false, err
		}
		childDigest := manifest.ManifestSHA256
		if prior, ok := children[*manifest.PreviousManifestSHA256]; ok && prior != childDigest {
			return false, fmt.Errorf("%w: revision predecessor has two children", ErrPublisherConflict)
		}
		children[*manifest.PreviousManifestSHA256] = childDigest
	}

	digest := candidate.ManifestSHA256
	if prior, ok := byDigest[digest]; ok {
		if !bytes.Equal(prior.Bytes, candidateBytes) {
			return false, fmt.Errorf("%w: existing manifest identity has different bytes", ErrPublisherConflict)
		}
		if prior.Manifest.Revision != candidate.Revision {
			return false, fmt.Errorf("%w: manifest digest is bound to a different revision", archive.ErrIntegrity)
		}
		return true, nil
	}
	if prior, ok := byRevision[candidate.Revision]; ok && prior.Manifest.ManifestSHA256 != digest {
		return false, fmt.Errorf("%w: revision already has a different digest", ErrPublisherConflict)
	}
	if candidate.Revision == 1 {
		if candidate.PreviousManifestSHA256 != nil {
			return false, fmt.Errorf("%w: genesis manifest has a predecessor", archive.ErrIntegrity)
		}
	} else {
		if candidate.PreviousManifestSHA256 == nil {
			return false, fmt.Errorf("%w: candidate predecessor is missing", archive.ErrIntegrity)
		}
		predecessor, ok := byDigest[*candidate.PreviousManifestSHA256]
		if !ok || predecessor.Manifest.Revision+1 != candidate.Revision {
			return false, fmt.Errorf("%w: candidate predecessor is absent or not adjacent", archive.ErrIntegrity)
		}
		if err := validateRevisionSuccessor(predecessor.Manifest, candidate); err != nil {
			return false, err
		}
		if child, ok := children[*candidate.PreviousManifestSHA256]; ok && child != digest {
			return false, fmt.Errorf("%w: candidate would create a revision branch", ErrPublisherConflict)
		}
	}
	return false, nil
}

func validateRevisionSuccessor(previous, current archive.RawDayManifest) error {
	if previous.DatasetID != current.DatasetID || previous.CampaignID != current.CampaignID ||
		previous.DayDefinitionID != current.DayDefinitionID || previous.Date != current.Date ||
		previous.PublisherID != current.PublisherID || previous.PublisherEpoch != current.PublisherEpoch ||
		previous.ConfigHash != current.ConfigHash || previous.SettlePolicy != current.SettlePolicy {
		return fmt.Errorf("%w: revision scope or publisher differs", archive.ErrIntegrity)
	}
	if len(previous.Objects) > len(current.Objects) {
		return fmt.Errorf("%w: revision objects are not cumulative", archive.ErrIntegrity)
	}
	for i := range previous.Objects {
		if previous.Objects[i] != current.Objects[i] {
			return fmt.Errorf("%w: revision object prefix changed", archive.ErrIntegrity)
		}
	}
	if len(previous.ChainObjects) > len(current.ChainObjects) {
		return fmt.Errorf("%w: revision chain_objects are not cumulative", archive.ErrIntegrity)
	}
	for i := range previous.ChainObjects {
		if previous.ChainObjects[i] != current.ChainObjects[i] {
			return fmt.Errorf("%w: revision chain_objects prefix changed", archive.ErrIntegrity)
		}
	}
	if previous.AcceptedRecordCount > current.AcceptedRecordCount || previous.ErrorCount > current.ErrorCount ||
		previous.ObservedThroughSourceMSC > current.ObservedThroughSourceMSC ||
		previous.ObservedThroughCaptureSeq > current.ObservedThroughCaptureSeq {
		return fmt.Errorf("%w: revision counts or watermarks decreased", archive.ErrIntegrity)
	}
	if previous.ChainSliceStartSequence != current.ChainSliceStartSequence || previous.ChainSliceStartRoot != current.ChainSliceStartRoot {
		return fmt.Errorf("%w: revision chain start changed", archive.ErrIntegrity)
	}
	if previous.ChainSliceEndSequence > current.ChainSliceEndSequence {
		return fmt.Errorf("%w: revision chain end moved backwards", archive.ErrIntegrity)
	}
	if previous.ChainSliceEndSequence == current.ChainSliceEndSequence && previous.ChainSliceEndRoot != current.ChainSliceEndRoot {
		return fmt.Errorf("%w: revision chain end root changed without extension", archive.ErrIntegrity)
	}
	return nil
}

func validateManifestRecord(record ManifestRecord) error {
	manifest, err := archive.VerifyRawDayManifest(record.Bytes)
	if err != nil || manifest.ManifestSHA256 != record.Manifest.ManifestSHA256 {
		return fmt.Errorf("%w: invalid remote manifest record", archive.ErrIntegrity)
	}
	canonical, err := archive.ManifestCanonicalJSON(record.Manifest)
	if err != nil || !bytes.Equal(canonical, record.Bytes) {
		return fmt.Errorf("%w: remote manifest record is not canonical", archive.ErrIntegrity)
	}
	return nil
}

func sameManifestScope(left, right archive.RawDayManifest) bool {
	return left.ManifestVersion == right.ManifestVersion &&
		left.DatasetID == right.DatasetID && left.CampaignID == right.CampaignID &&
		left.DayDefinitionID == right.DayDefinitionID && left.Date == right.Date &&
		left.PublisherID == right.PublisherID && left.PublisherEpoch == right.PublisherEpoch &&
		left.ConfigHash == right.ConfigHash && left.SettlePolicy == right.SettlePolicy
}
