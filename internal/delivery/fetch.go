package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

func (r *archiveReaderV1) BuildFetchPlan(_ context.Context, snapshot ResolvedSnapshot) (FetchPlan, error) {
	if err := archive.ValidateRawDayManifest(snapshot.Manifest); err != nil {
		return FetchPlan{}, fmt.Errorf("%w: snapshot manifest is invalid", archive.ErrIntegrity)
	}
	if err := manifestMatchesScope(snapshot.Manifest, snapshot.Scope); err != nil {
		return FetchPlan{}, err
	}
	canonical, err := archive.ManifestCanonicalJSON(snapshot.Manifest)
	if err != nil || !bytes.Equal(canonical, snapshot.ManifestBytes) {
		return FetchPlan{}, fmt.Errorf("%w: snapshot manifest bytes are not canonical", archive.ErrIntegrity)
	}
	digest, err := archive.ManifestDigest(snapshot.Manifest)
	if err != nil || digest != snapshot.ManifestSHA256 {
		return FetchPlan{}, fmt.Errorf("%w: snapshot manifest digest is invalid", archive.ErrIntegrity)
	}
	if snapshot.ManifestKey == "" || snapshot.ManifestSHA256 == ([32]byte{}) {
		return FetchPlan{}, fmt.Errorf("%w: snapshot identity is incomplete", archive.ErrIntegrity)
	}
	layout, err := r2.NewLayout(r.config.ImmutableRoot, "", snapshot.Scope)
	if err != nil {
		return FetchPlan{}, fmt.Errorf("%w: snapshot scope layout is invalid", archive.ErrIntegrity)
	}
	wantManifestKey, err := layout.ManifestKey(snapshot.Manifest)
	if err != nil || wantManifestKey != snapshot.ManifestKey {
		return FetchPlan{}, fmt.Errorf("%w: manifest key does not match scope and digest", archive.ErrIntegrity)
	}
	objects := make([]FetchObject, len(snapshot.Manifest.ChainObjects))
	seen := make(map[string]struct{}, len(objects))
	for i, object := range snapshot.Manifest.ChainObjects {
		if object.Key != archive.RawWALObjectKey(object.SHA256) || object.Bytes == 0 || object.EndIngestSequence < object.StartIngestSequence {
			return FetchPlan{}, fmt.Errorf("%w: chain object identity is invalid", archive.ErrIntegrity)
		}
		if _, ok := seen[object.Key]; ok {
			return FetchPlan{}, fmt.Errorf("%w: chain object is duplicated", archive.ErrIntegrity)
		}
		seen[object.Key] = struct{}{}
		remoteKey, err := layout.RemoteKey(object.Key)
		if err != nil {
			return FetchPlan{}, fmt.Errorf("%w: chain object key is invalid", archive.ErrIntegrity)
		}
		objects[i] = FetchObject{
			Key:       object.Key,
			RemoteKey: remoteKey,
			SHA256:    object.SHA256,
			Bytes:     object.Bytes,
			CachePath: r.cacheObjectPath(object.SHA256),
		}
	}
	return FetchPlan{
		ManifestKey: snapshot.ManifestKey, ManifestSHA256: snapshot.ManifestSHA256,
		ManifestBytes:     append([]byte(nil), snapshot.ManifestBytes...),
		ManifestCachePath: r.cacheManifestPath(snapshot.Manifest), Objects: objects,
	}, nil
}

func (r *archiveReaderV1) Fetch(ctx context.Context, plan FetchPlan, destination string) (FetchResult, error) {
	if err := r.validateFetchPlan(plan); err != nil {
		return FetchResult{}, err
	}
	if err := r.fetchManifest(ctx, plan); err != nil {
		return FetchResult{}, err
	}
	objectPaths := make(map[string]string, len(plan.Objects))
	for _, object := range plan.Objects {
		if err := r.fetchRawObject(ctx, object); err != nil {
			return FetchResult{}, err
		}
		objectPaths[object.Key] = object.CachePath
	}
	entries, err := restoreEntries(planManifest(plan), objectPaths)
	if err != nil {
		return FetchResult{}, err
	}
	result := FetchResult{ManifestPath: plan.ManifestCachePath, ObjectPaths: objectPaths, Entries: entries}
	if destination == "" {
		return result, nil
	}
	manifestOutput := filepath.Join(destination, "manifest-"+hex.EncodeToString(plan.ManifestSHA256[:])+".json")
	if err := copyVerifiedFile(plan.ManifestCachePath, manifestOutput, plan.ManifestSHA256, uint64(len(plan.ManifestBytes)), false); err != nil {
		return FetchResult{}, err
	}
	result.ManifestPath = manifestOutput
	result.ObjectPaths = make(map[string]string, len(objectPaths))
	for _, object := range plan.Objects {
		output := filepath.Join(destination, "wal-"+hex.EncodeToString(object.SHA256[:])+".rtw")
		if err := copyVerifiedFile(object.CachePath, output, object.SHA256, object.Bytes, true); err != nil {
			return FetchResult{}, err
		}
		result.ObjectPaths[object.Key] = output
	}
	return result, nil
}

func (r *archiveReaderV1) validateFetchPlan(plan FetchPlan) error {
	if plan.ManifestKey == "" || len(plan.ManifestBytes) == 0 || plan.ManifestSHA256 == ([32]byte{}) {
		return fmt.Errorf("%w: fetch plan is incomplete", archive.ErrIntegrity)
	}
	manifest, err := archive.VerifyRawDayManifest(plan.ManifestBytes)
	if err != nil || manifest.ManifestSHA256 != plan.ManifestSHA256 {
		return fmt.Errorf("%w: fetch plan manifest is invalid", archive.ErrIntegrity)
	}
	if _, _, err := parseManifestSelector(plan.ManifestKey, r.config.ImmutableRoot); err != nil {
		return err
	}
	if len(manifest.ChainObjects) != len(plan.Objects) {
		return fmt.Errorf("%w: fetch plan chain object count differs", archive.ErrIntegrity)
	}
	for i, object := range plan.Objects {
		chainObject := manifest.ChainObjects[i]
		if object.Key != chainObject.Key || object.RemoteKey == "" || object.SHA256 != chainObject.SHA256 || object.Bytes != chainObject.Bytes || object.CachePath != r.cacheObjectPath(object.SHA256) {
			return fmt.Errorf("%w: fetch plan object differs from manifest", archive.ErrIntegrity)
		}
	}
	return nil
}

func planManifest(plan FetchPlan) archive.RawDayManifest {
	manifest, _ := archive.VerifyRawDayManifest(plan.ManifestBytes)
	return manifest
}

func (r *archiveReaderV1) fetchManifest(ctx context.Context, plan FetchPlan) error {
	body, err := r.metadata(ctx, plan.ManifestKey)
	if err != nil {
		return err
	}
	if !bytes.Equal(body, plan.ManifestBytes) {
		return fmt.Errorf("%w: remote manifest changed during fetch", archive.ErrIntegrity)
	}
	return publishVerifiedBytes(plan.ManifestCachePath, body)
}

func (r *archiveReaderV1) fetchRawObject(ctx context.Context, object FetchObject) error {
	if object.Bytes > r.config.MaxRawObjectBytes {
		return fmt.Errorf("%w: raw object exceeds configured limit", archive.ErrIntegrity)
	}
	body, size, err := r.backend.Open(ctx, object.RemoteKey)
	if err != nil {
		return err
	}
	if size >= 0 && uint64(size) != object.Bytes {
		_ = body.Close()
		return fmt.Errorf("%w: remote raw object size differs", archive.ErrIntegrity)
	}
	streamErr := streamVerifiedFile(body, object.CachePath, object.SHA256, object.Bytes, true)
	closeErr := body.Close()
	if streamErr != nil {
		return streamErr
	}
	if closeErr != nil {
		return fmt.Errorf("stream raw object close failed")
	}
	return nil
}

func (r *archiveReaderV1) cacheManifestPath(manifest archive.RawDayManifest) string {
	digest := hex.EncodeToString(manifest.ManifestSHA256[:])
	return filepath.Join(r.config.CacheRoot, "manifests", "raw-day-"+fmt.Sprintf("%d", manifest.Revision)+"-"+digest+".json")
}

func (r *archiveReaderV1) cacheObjectPath(hash [32]byte) string {
	return filepath.Join(r.config.CacheRoot, "objects", "raw", "wal-"+hex.EncodeToString(hash[:])+".rtw")
}

func publishVerifiedBytes(path string, body []byte) error {
	if path == "" {
		return fmt.Errorf("%w: cache path is empty", archive.ErrIntegrity)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create cache directory")
	}
	digest := sha256.Sum256(body)
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, body) {
			return nil
		}
		return fmt.Errorf("%w: existing cache bytes differ", archive.ErrIntegrity)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing cache")
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".delivery-metadata-*.tmp")
	if err != nil {
		return fmt.Errorf("create metadata temporary")
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set metadata temporary permissions")
	}
	if _, err := temp.Write(body); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write metadata temporary")
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync metadata temporary")
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close metadata temporary")
	}
	if err := os.Link(tempPath, path); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("publish metadata cache")
	}
	existing, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(existing, body) {
		return fmt.Errorf("%w: metadata cache collision %x", archive.ErrIntegrity, digest)
	}
	return nil
}

func streamVerifiedFile(source io.Reader, path string, expectedHash [32]byte, expectedBytes uint64, verifyWAL bool) error {
	if path == "" || expectedBytes == 0 {
		return fmt.Errorf("%w: streamed cache target is incomplete", archive.ErrIntegrity)
	}
	if err := verifyCachedFile(path, expectedHash, expectedBytes, verifyWAL); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create raw cache directory")
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".delivery-raw-*.tmp")
	if err != nil {
		return fmt.Errorf("create raw temporary")
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set raw temporary permissions")
	}
	digest := sha256.New()
	limited := io.LimitReader(source, int64(expectedBytes)+1)
	count, err := io.Copy(io.MultiWriter(temp, digest), limited)
	if err != nil {
		_ = temp.Close()
		return fmt.Errorf("stream raw object")
	}
	if uint64(count) != expectedBytes {
		_ = temp.Close()
		return fmt.Errorf("%w: streamed raw object size differs", archive.ErrIntegrity)
	}
	if !bytes.Equal(digest.Sum(nil), expectedHash[:]) {
		_ = temp.Close()
		return fmt.Errorf("%w: streamed raw object hash differs", archive.ErrIntegrity)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync raw temporary")
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close raw temporary")
	}
	if verifyWAL {
		if _, err := wal.VerifySealedSegment(tempPath); err != nil {
			return fmt.Errorf("%w: downloaded WAL failed verification", archive.ErrIntegrity)
		}
	}
	if err := os.Link(tempPath, path); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("publish raw cache")
	}
	return verifyCachedFile(path, expectedHash, expectedBytes, verifyWAL)
}

func verifyCachedFile(path string, expectedHash [32]byte, expectedBytes uint64, verifyWAL bool) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || uint64(stat.Size()) != expectedBytes {
		return fmt.Errorf("%w: cached file size differs", archive.ErrIntegrity)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read cached file")
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || !bytes.Equal(digest.Sum(nil), expectedHash[:]) {
		return fmt.Errorf("%w: cached file hash differs", archive.ErrIntegrity)
	}
	if verifyWAL {
		if _, err := wal.VerifySealedSegment(path); err != nil {
			return fmt.Errorf("%w: cached WAL failed verification", archive.ErrIntegrity)
		}
	}
	return nil
}

func copyVerifiedFile(source, destination string, expectedHash [32]byte, expectedBytes uint64, verifyWAL bool) error {
	if err := verifyCachedFile(destination, expectedHash, expectedBytes, verifyWAL); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open verified cache")
	}
	defer input.Close()
	return streamVerifiedFile(input, destination, expectedHash, expectedBytes, verifyWAL)
}

func restoreEntries(manifest archive.RawDayManifest, objectPaths map[string]string) ([]RestoredEntry, error) {
	if len(manifest.ChainObjects) == 0 {
		return []RestoredEntry{}, nil
	}
	segments := make([]wal.VerifiedSegment, len(manifest.ChainObjects))
	for i, object := range manifest.ChainObjects {
		path, ok := objectPaths[object.Key]
		if !ok {
			return nil, fmt.Errorf("%w: restored chain object is missing", archive.ErrIntegrity)
		}
		segment, err := wal.VerifySealedSegment(path)
		if err != nil || segment.ObjectSHA256 != object.SHA256 || uint64(segment.FileBytes) != object.Bytes || segment.StartSequence != object.StartIngestSequence || segment.LastSequence != object.EndIngestSequence {
			return nil, fmt.Errorf("%w: restored chain object differs from manifest", archive.ErrIntegrity)
		}
		segments[i] = segment
		if i > 0 {
			previous := segments[i-1]
			if previous.LastSequence+1 != segment.StartSequence || previous.ChainRoot != segment.ChainStart {
				return nil, fmt.Errorf("%w: restored WAL chain is discontinuous", archive.ErrIntegrity)
			}
		}
	}
	var result []RestoredEntry
	expectedSequence := manifest.ChainSliceStartSequence
	var expectedPrevious [32]byte = manifest.ChainSliceStartRoot
	reachedEnd := false
	for objectIndex, segment := range segments {
		for _, entry := range segment.Entries {
			if entry.Sequence < manifest.ChainSliceStartSequence {
				continue
			}
			if entry.Sequence > manifest.ChainSliceEndSequence {
				break
			}
			if entry.Sequence != expectedSequence || entry.PreviousEntryHash != expectedPrevious {
				return nil, fmt.Errorf("%w: restored entry chain is discontinuous", archive.ErrIntegrity)
			}
			frame, err := protocol.DecodeFrame(entry.Frame)
			if err != nil {
				return nil, fmt.Errorf("%w: restored frame is invalid", archive.ErrIntegrity)
			}
			message, err := protocol.DecodeMessage(frame)
			if err != nil {
				return nil, fmt.Errorf("%w: restored message is invalid", archive.ErrIntegrity)
			}
			batch, ok := message.(protocol.BatchFrameV1)
			if !ok {
				return nil, fmt.Errorf("%w: restored WAL entry is not BatchFrameV1", archive.ErrIntegrity)
			}
			result = append(result, RestoredEntry{
				ObjectKey: manifest.ChainObjects[objectIndex].Key, GatewayIngestSequence: entry.Sequence,
				Frame: append([]byte(nil), entry.Frame...), Batch: batch,
				PreviousEntryHash: entry.PreviousEntryHash, EntryHash: entry.EntryHash,
				SelectedRecordOrdinals: selectedOrdinals(manifest.Objects, entry.Sequence, len(batch.Records)),
			})
			expectedPrevious = entry.EntryHash
			if entry.Sequence == manifest.ChainSliceEndSequence {
				reachedEnd = true
				break
			}
			expectedSequence++
		}
		if reachedEnd {
			break
		}
	}
	if !reachedEnd || expectedPrevious != manifest.ChainSliceEndRoot {
		return nil, fmt.Errorf("%w: restored chain does not reach manifest end root", archive.ErrIntegrity)
	}
	return result, nil
}

func selectedOrdinals(ranges []archive.RawObjectRange, sequence uint64, recordCount int) []uint32 {
	set := make(map[uint32]struct{})
	for _, item := range ranges {
		if sequence < item.StartIngestSequence || sequence > item.EndIngestSequence {
			continue
		}
		first, last := item.FirstRecordOrdinal, item.LastRecordOrdinal
		if sequence > item.StartIngestSequence {
			first = 0
		}
		if sequence < item.EndIngestSequence {
			if recordCount == 0 {
				last = 0
			} else {
				last = uint32(recordCount - 1)
			}
		}
		if recordCount == 0 && first == 0 && last == 0 {
			set[0] = struct{}{}
			continue
		}
		for ordinal := first; ordinal <= last; ordinal++ {
			if int(ordinal) >= recordCount {
				break
			}
			set[ordinal] = struct{}{}
			if ordinal == ^uint32(0) {
				break
			}
		}
	}
	result := make([]uint32, 0, len(set))
	for ordinal := range set {
		result = append(result, ordinal)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
