package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/continuity"
	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
)

type m3NetworkFreeBackend struct {
	mu        sync.Mutex
	objects   map[string][]byte
	writes    int
	putIfNone []string
}

func newM3NetworkFreeBackend() *m3NetworkFreeBackend {
	return &m3NetworkFreeBackend{objects: make(map[string][]byte)}
}

func (b *m3NetworkFreeBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.objects[key]; exists {
		return r2.ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	b.writes++
	b.putIfNone = append(b.putIfNone, key)
	return nil
}

func (b *m3NetworkFreeBackend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[key]
	if !ok {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *m3NetworkFreeBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	body, err := b.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *m3NetworkFreeBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]r2.RemoteObject, 0)
	for key, body := range b.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, r2.RemoteObject{Key: key, Size: int64(len(body))})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (b *m3NetworkFreeBackend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
	body, err := b.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) > maxBytes {
		return nil, r2.ErrMetadataTooLarge
	}
	return body, nil
}

func (b *m3NetworkFreeBackend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]r2.RemoteObject, error) {
	objects, err := b.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if uint64(len(objects)) > maxObjects {
		return nil, r2.ErrResourceLimit
	}
	return objects, nil
}

func (b *m3NetworkFreeBackend) writeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writes
}

func (b *m3NetworkFreeBackend) putIfAbsentKeys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.putIfNone...)
}

type m3M2NetworkFreeRclone struct {
	backend         *m3NetworkFreeBackend
	immutablePrefix string
	rclonePrefix    string
	copyMutations   int
	copyKeys        []string
	checkKeys       []string
}

func (f *m3M2NetworkFreeRclone) fullKey(remoteKey string) (string, error) {
	prefix := f.rclonePrefix + "/"
	if !strings.HasPrefix(remoteKey, prefix) {
		return "", fmt.Errorf("fake M2 rclone key is outside the trusted prefix")
	}
	return f.immutablePrefix + "/" + strings.TrimPrefix(remoteKey, prefix), nil
}

func (f *m3M2NetworkFreeRclone) execute(ctx context.Context, _ string, args ...string) (string, error) {
	if len(args) == 1 && args[0] == "version" {
		return "rclone " + r2.RcloneVersion + "\n", nil
	}
	if len(args) != 4 {
		return "", errors.New("fake M2 rclone rejects an arbitrary operation shape")
	}
	localPath, remoteKey := args[2], args[3]
	fullKey, err := f.fullKey(remoteKey)
	if err != nil {
		return "", err
	}
	switch {
	case args[0] == "copyto" && args[1] == "--immutable":
		local, err := os.ReadFile(localPath)
		if err != nil {
			return "", err
		}
		f.backend.mu.Lock()
		defer f.backend.mu.Unlock()
		if existing, ok := f.backend.objects[fullKey]; ok {
			if !bytes.Equal(existing, local) {
				return "", r2.ErrRcloneCollision
			}
		} else {
			f.backend.objects[fullKey] = append([]byte(nil), local...)
			f.backend.writes++
			f.copyMutations++
			f.copyKeys = append(f.copyKeys, remoteKey)
		}
		return "", nil
	case args[0] == "check" && args[1] == "--download":
		local, err := os.ReadFile(localPath)
		if err != nil {
			return "", err
		}
		remote, err := f.backend.Get(ctx, fullKey)
		if err != nil || !bytes.Equal(local, remote) {
			return "", r2.ErrRcloneCheckMismatch
		}
		f.checkKeys = append(f.checkKeys, remoteKey)
		return "", nil
	default:
		return "", errors.New("fake M2 rclone rejects a forbidden operation")
	}
}

type m3NetworkFreeActionTool struct {
	backend         *m3NetworkFreeBackend
	immutablePrefix string
	rclonePrefix    string
	copyKeys        []string
	checkCalls      int
}

func (t *m3NetworkFreeActionTool) immutableKey(key string) (string, error) {
	prefix := t.rclonePrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("action key is outside the trusted prefix")
	}
	return t.immutablePrefix + "/" + strings.TrimPrefix(key, prefix), nil
}

func (t *m3NetworkFreeActionTool) CopyToImmutable(ctx context.Context, localPath, key string) error {
	fullKey, err := t.immutableKey(key)
	if err != nil {
		return err
	}
	body, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	if existing, getErr := t.backend.Get(ctx, fullKey); getErr == nil {
		if !bytes.Equal(existing, body) {
			return r2.ErrRcloneCollision
		}
	} else if err := t.backend.PutIfAbsent(ctx, fullKey, body); err != nil {
		return err
	}
	t.copyKeys = append(t.copyKeys, key)
	return nil
}

func (t *m3NetworkFreeActionTool) CheckDownload(ctx context.Context, localPath, key string) error {
	fullKey, err := t.immutableKey(key)
	if err != nil {
		return err
	}
	local, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	remote, err := t.backend.Get(ctx, fullKey)
	if err != nil || !bytes.Equal(local, remote) {
		return r2.ErrRcloneCheckMismatch
	}
	t.checkCalls++
	return nil
}

type m3FailingEventStore struct{ calls int }

func (s *m3FailingEventStore) Append(context.Context, r2.ReplayPublicationBundle, r2.ReplayPublicationEvent) error {
	s.calls++
	return errors.New("diagnostic event unavailable")
}

func (*m3FailingEventStore) Load(context.Context, r2.ReplayPublicationBundle) ([]r2.ReplayPublicationEvent, error) {
	return nil, errors.New("diagnostic event unavailable")
}

func TestM3ReplayDeliveryNetworkFreeEndToEnd(t *testing.T) {
	ctx := context.Background()
	producerInstanceID := "m3-e2e-producer"
	scope := archive.ScopeConfig{
		DatasetID: "m3-e2e-dataset", CampaignID: "m3-e2e-campaign", ProviderID: "m3-e2e-provider",
		StableFeedID: "m3-e2e-feed", ExactSourceSymbol: "EURUSD.e2e", BrokerServerFingerprint: "m3-e2e-server",
		GatewayBuildIdentity: "m3-e2e-gateway", ProducerBuildIdentity: "m3-e2e-producer-build",
		DayDefinitionID: "utc-day-v1", SettlePolicy: "manual-v1", PublisherID: "m3-e2e-publisher", PublisherEpoch: 1,
		ProtocolVersion: protocol.ProtocolVersion,
		ProtocolLimits:  archive.ProtocolLimits{MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: protocol.MaxRecords, MaxStringBytes: protocol.MaxStringBytes},
	}
	layout, err := r2.NewLayout("v1", "fake:v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	rawObject, manifest, manifestBytes := buildM3E2ERawTruth(t, scope, producerInstanceID)
	rawRelative, err := archive.RawDayManifestRelativeKey(scope, manifest)
	if err != nil {
		t.Fatal(err)
	}
	rawPaths := map[string]string{rawObject.Key: rawObject.Path}
	backend := newM3NetworkFreeBackend()
	m2BinaryBytes := []byte("network-free-m2-rclone-binary")
	m2BinaryPath := filepath.Join(t.TempDir(), "m2-rclone.exe")
	if err := os.WriteFile(m2BinaryPath, m2BinaryBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	m2BinaryHash := sha256.Sum256(m2BinaryBytes)
	m2ToolIdentity := r2.RcloneTool{GOOS: "windows", GOARCH: "amd64", BinarySHA256: fmt.Sprintf("%x", m2BinaryHash), BinaryBytes: uint64(len(m2BinaryBytes)), ExecutableName: "m2-rclone.exe"}
	m2FakeRclone := &m3M2NetworkFreeRclone{backend: backend, immutablePrefix: layout.ImmutableCampaignPrefix(), rclonePrefix: layout.RcloneCampaignPrefix()}
	if _, err := m2FakeRclone.execute(ctx, m2BinaryPath, "delete", "--immutable", "local", layout.RcloneCampaignPrefix()+"/forbidden"); err == nil {
		t.Fatal("fake M2 rclone accepted a forbidden operation")
	}
	if _, err := m2FakeRclone.execute(ctx, m2BinaryPath, "copyto", "--overwrite", "local", layout.RcloneCampaignPrefix()+"/forbidden"); err == nil {
		t.Fatal("fake M2 rclone accepted copyto without --immutable")
	}
	m2Runner, err := r2.NewRcloneRunnerWithExecutor(m2BinaryPath, m2ToolIdentity, m2FakeRclone.execute)
	if err != nil {
		t.Fatal(err)
	}
	m2Journal, err := r2.OpenPublicationJournal(filepath.Join(t.TempDir(), "m2-publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	m2Publisher, err := r2.NewPublisher(layout, backend, m2Runner, m2Journal, "")
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2Input := r2.PublicationInput{Manifest: manifest, ManifestBytes: manifestBytes, ObjectPaths: rawPaths, ReceiptPath: filepath.Join(t.TempDir(), "m2-receipt.json")}
	m2Receipt, err := m2Publisher.Publish(ctx, m2Input)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ManifestKey, err := layout.ManifestKey(manifest)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2Claim, err := r2.NewPublisherClaim(scope)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ClaimKey, err := layout.ClaimKey(scope.PublisherEpoch)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ClaimBytes, err := m2Claim.CanonicalJSON()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ClaimHash, err := m2Claim.Digest()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ScopeHash, err := scope.ConfigHash()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	if !m2Receipt.VerificationComplete || m2Receipt.ReceiptVersion != "publication-verification-receipt-v1" || m2Receipt.ManifestKey != m2ManifestKey || m2Receipt.ManifestSHA256 != manifest.ManifestSHA256 || m2Receipt.ClaimHash != m2ClaimHash || m2Receipt.ScopeConfigHash != m2ScopeHash || len(m2Receipt.RawObjects) != 1 || m2Receipt.RawObjects[0].SHA256 != rawObject.SHA256 || m2Receipt.RawObjects[0].Bytes != uint64(rawObject.Bytes) || m2Receipt.RcloneVersion != r2.RcloneVersion || m2Receipt.RcloneBinarySHA256 != m2ToolIdentity.BinarySHA256 {
		_ = m2Journal.Close()
		t.Fatalf("production M2 receipt identity mismatch: %+v", m2Receipt)
	}
	m2ScopeKey, err := layout.ScopeDescriptorKey()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2ScopeBytes, err := scope.CanonicalConfigJSON()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2RawKey, err := layout.RemoteKey(rawObject.Key)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2RawBytes, err := os.ReadFile(rawObject.Path)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	for key, want := range map[string][]byte{m2ScopeKey: m2ScopeBytes, m2ClaimKey: m2ClaimBytes, m2RawKey: m2RawBytes, m2ManifestKey: manifestBytes} {
		got, err := backend.Get(ctx, key)
		if err != nil || !bytes.Equal(got, want) {
			_ = m2Journal.Close()
			t.Fatalf("production M2 remote object %q differs: err=%v", key, err)
		}
	}
	if got := backend.putIfAbsentKeys(); len(got) != 1 || got[0] != m2ClaimKey {
		_ = m2Journal.Close()
		t.Fatalf("production M2 conditional claim writes = %v", got)
	}
	if m2FakeRclone.copyMutations != 3 {
		_ = m2Journal.Close()
		t.Fatalf("production M2 immutable copy mutations = %d, want scope/raw/manifest", m2FakeRclone.copyMutations)
	}
	m2ReceiptCanonical, err := m2Receipt.CanonicalJSON()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2Writes := backend.writeCount()
	m2Copies := m2FakeRclone.copyMutations
	m2ClaimWrites := len(backend.putIfAbsentKeys())
	m2RetryReceipt, err := m2Publisher.Publish(ctx, m2Input)
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	m2RetryCanonical, err := m2RetryReceipt.CanonicalJSON()
	if err != nil {
		_ = m2Journal.Close()
		t.Fatal(err)
	}
	if !bytes.Equal(m2RetryCanonical, m2ReceiptCanonical) || backend.writeCount() != m2Writes || m2FakeRclone.copyMutations != m2Copies || len(backend.putIfAbsentKeys()) != m2ClaimWrites {
		_ = m2Journal.Close()
		t.Fatal("same-content production M2 retry changed receipt identity, claim writes, or remote copy mutations")
	}
	if err := m2Journal.Close(); err != nil {
		t.Fatal(err)
	}

	spec, err := parquet.NewConversionSpec("m3-e2e-replay-v1", "m3-e2e-conversion-v1", "m3-e2e-converter", "windows-amd64-go1.24.13", 100, 1<<20, 100)
	if err != nil {
		t.Fatal(err)
	}
	source, err := archive.OpenVerifiedReplaySource(archive.ReplaySourceInput{
		Scope: scope, ProducerInstanceID: producerInstanceID, ManifestRelativeKey: rawRelative,
		ManifestBytes: manifestBytes, ObjectPaths: rawPaths, ReplayContractID: spec.ReplayContractID, ConversionID: spec.ConversionID,
		ResourceLimits: archive.ReplayResourceLimits{MaxChainObjects: 16, MaxObjectBytes: 1 << 30, MaxChainBytes: 2 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	replayScope := source.ReplayScope()
	generator, err := parquet.NewGenerator(spec, replayScope, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reduced, err := continuity.Reduce(source, generator.WriteRow)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := generator.Close()
	if err != nil {
		t.Fatal(err)
	}
	if reduced.RowCount == 0 || generated.RowCount != reduced.RowCount || generated.RowChainRoot != reduced.RowChainRoot || generated.RowChainRoot == ([32]byte{}) {
		t.Fatalf("reducer/generator roots differ: reduced=%+v generated=%+v", reduced, generated)
	}
	conversion, err := archive.ConversionTupleFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]protocol.PartManifest, len(generated.Parts))
	partBytes := make([][]byte, len(parts))
	var previous *protocol.PartManifest
	for index, artifact := range generated.Parts {
		input, err := archive.PartManifestInputFromArtifact(replayScope, conversion, artifact)
		if err != nil {
			t.Fatal(err)
		}
		part, err := archive.BuildPartManifest(input, previous)
		if err != nil {
			t.Fatal(err)
		}
		parts[index] = part
		partBytes[index], err = protocol.PartManifestCanonicalJSON(part)
		if err != nil {
			t.Fatal(err)
		}
		previous = &parts[index]
	}
	partRoot, err := protocol.PartSetRoot(parts)
	if err != nil {
		t.Fatal(err)
	}
	replayManifest, err := archive.BuildReplayDayManifest(archive.ReplayDayManifestInput{Scope: replayScope, Conversion: conversion, CompletenessStatus: "settled_snapshot", Parts: parts, CanonicalStreamRowChainRoot: generated.RowChainRoot})
	if err != nil {
		t.Fatal(err)
	}
	if replayManifest.PartSetRoot != partRoot || replayManifest.CanonicalStreamRowChainRoot != reduced.RowChainRoot || replayManifest.RawDayManifestKey != rawRelative || replayManifest.RawDayManifestSHA256 != m2Receipt.ManifestSHA256 || m2Receipt.ManifestKey != m2ManifestKey {
		t.Fatal("replay manifest did not preserve raw, part, and row-chain identity")
	}
	replayBytes, err := protocol.ReplayDayManifestCanonicalJSON(replayManifest)
	if err != nil {
		t.Fatal(err)
	}

	binaryBytes := []byte("network-free-rclone-binary")
	binaryPath := filepath.Join(t.TempDir(), "rclone.exe")
	if err := os.WriteFile(binaryPath, binaryBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	binaryHash := sha256.Sum256(binaryBytes)
	toolIdentity := r2.RcloneTool{GOOS: "windows", GOARCH: "amd64", BinarySHA256: fmt.Sprintf("%x", binaryHash), BinaryBytes: uint64(len(binaryBytes)), ExecutableName: "rclone.exe"}
	runner, err := r2.NewRcloneRunnerWithExecutor(binaryPath, toolIdentity, func(_ context.Context, _ string, args ...string) (string, error) {
		if len(args) == 1 && args[0] == "version" {
			return "rclone " + r2.RcloneVersion + "\n", nil
		}
		return "", errors.New("network-free runner rejects process operations")
	})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := r2.NewReplayRemoteReadAdapter(backend)
	if err != nil {
		t.Fatal(err)
	}
	actionTool := &m3NetworkFreeActionTool{backend: backend, immutablePrefix: layout.ImmutableCampaignPrefix(), rclonePrefix: layout.RcloneCampaignPrefix()}
	events := &m3FailingEventStore{}
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	publisher, err := r2.NewReplayPublisher(layout, remote, runner, actionTool, events, r2.FileReplayReceiptStore{}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	publicationInput := r2.ReplayPublicationInput{Conversion: spec, Limits: protocol.ReplayPublicationImplementationBounds, RawManifestBytes: manifestBytes, RawObjectPaths: rawPaths, Parts: generated.Parts, PartManifestBytes: partBytes, ReplayManifestBytes: replayBytes, ReceiptPath: receiptPath}
	bundle, err := r2.SealReplayPublicationBundle(r2.ReplayPublicationBundleInput{Layout: layout, Conversion: spec, Limits: publicationInput.Limits, RawManifest: manifestBytes, RawObjectPaths: rawPaths, Parts: generated.Parts, PartManifests: partBytes, ReplayManifest: replayBytes, ReceiptPath: receiptPath, RcloneBinaryPath: binaryPath, RcloneTool: toolIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Contract.RawManifest.FullKey != m2Receipt.ManifestKey || bundle.Contract.RawManifest.DomainDigest != fmt.Sprintf("%x", m2Receipt.ManifestSHA256) || bundle.Contract.RawManifest.RelativeKey != rawRelative || bundle.Contract.Claim.FullKey != m2ClaimKey || bundle.Contract.Claim.CanonicalJSON != string(m2ClaimBytes) || bundle.Contract.Claim.DomainDigest != fmt.Sprintf("%x", m2ClaimHash) {
		t.Fatalf("M2 receipt and M3 bundle identity graph differs: m2=%+v bundle=%+v", m2Receipt, bundle.Contract)
	}
	receipt, err := publisher.Publish(ctx, publicationInput)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.VerificationComplete || receipt.BundleDigest != bundle.Digest || receipt.FinalObservationDigest == ([32]byte{}) || receipt.PartSetRoot != fmt.Sprintf("%x", partRoot) || receipt.CanonicalRowChainRoot != fmt.Sprintf("%x", generated.RowChainRoot) || receipt.Claim != bundle.Contract.Claim || receipt.Claim.FullKey != m2ClaimKey || receipt.Claim.CanonicalJSON != string(m2ClaimBytes) || receipt.Claim.DomainDigest != fmt.Sprintf("%x", m2ClaimHash) {
		t.Fatalf("receipt identity mismatch: %+v", receipt)
	}
	expectedActionKeys := make([]string, 0, len(parts)*2+1)
	for _, part := range parts {
		key, _ := layout.RcloneReplayPartObjectKey(part)
		expectedActionKeys = append(expectedActionKeys, key)
	}
	for _, part := range parts {
		key, _ := layout.RcloneReplayPartManifestKey(part)
		expectedActionKeys = append(expectedActionKeys, key)
	}
	replayActionKey, _ := layout.RcloneReplayDayManifestKey(replayManifest)
	expectedActionKeys = append(expectedActionKeys, replayActionKey)
	if !equalM3E2EStrings(actionTool.copyKeys, expectedActionKeys) || actionTool.checkCalls != len(expectedActionKeys) || events.calls == 0 {
		t.Fatalf("approved actions/events differ: copies=%v checks=%d events=%d", actionTool.copyKeys, actionTool.checkCalls, events.calls)
	}
	writesAfterFirstPublish := backend.writeCount()
	copiesAfterFirstPublish := len(actionTool.copyKeys)
	retryReceipt, err := publisher.Publish(ctx, publicationInput)
	if err != nil {
		t.Fatal(err)
	}
	if retryReceipt.FinalObservationDigest != receipt.FinalObservationDigest || backend.writeCount() != writesAfterFirstPublish || len(actionTool.copyKeys) != copiesAfterFirstPublish {
		t.Fatal("same-content M3 retry duplicated a remote mutation")
	}

	cacheRoot := t.TempDir()
	reader, err := NewArchiveReaderV1WithBackend(testReaderConfig(cacheRoot), backend)
	if err != nil {
		t.Fatal(err)
	}
	readerWrites := backend.writeCount()
	replayDayScope := ReplayDayScope{DatasetID: scope.DatasetID, CampaignID: scope.CampaignID, Date: manifest.Date, ReplayContractID: spec.ReplayContractID, ConversionID: spec.ConversionID}
	snapshots, err := reader.ListReplaySnapshots(ctx, replayDayScope)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("replay list = %+v err=%v", snapshots, err)
	}
	resolved, err := reader.ResolveReplaySnapshot(ctx, ReplaySnapshotSelector{Manifest: snapshots[0].ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Manifest.RawDayManifestKey != rawRelative || resolved.Manifest.RawDayManifestSHA256 != m2Receipt.ManifestSHA256 || resolved.Manifest.PartSetRoot != partRoot || resolved.Manifest.CanonicalStreamRowChainRoot != generated.RowChainRoot {
		t.Fatalf("read-only resolution broke the M2-to-M3 identity graph: %+v", resolved.Manifest)
	}
	plan, err := reader.BuildReplayFetchPlan(ctx, resolved)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range append(append([]ReplayFetchObject{plan.Manifest}, plan.Parts...), plan.Parquet...) {
		if _, err := os.Stat(object.CachePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cache was not empty before fetch: %s", object.CachePath)
		}
	}
	fetched, err := reader.FetchReplay(ctx, plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fetched.ManifestPath, fmt.Sprintf("%x", replayManifest.ManifestSHA256)) || !strings.Contains(fetched.ParquetPaths[parts[0].PartKey], fmt.Sprintf("%x", parts[0].PartSHA256)) {
		t.Fatalf("cache paths are not hash-derived: %+v", fetched)
	}
	if err := parquet.VerifyPartFile(fetched.ParquetPaths[parts[0].PartKey], replayPartArtifact(parts[0], fetched.ParquetPaths[parts[0].PartKey]), replayScope); err != nil {
		t.Fatal(err)
	}
	report, err := reader.VerifyReplayDay(ctx, ReplaySnapshotSelector{Manifest: snapshots[0].ManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	if report.GenesisVerified || report.VerificationScope != VerificationScopeReplayAnchoredDay || report.ManifestSHA256 != replayManifest.ManifestSHA256 || !report.RawBindingVerified || !report.RawDaySemanticsVerified || !report.PartManifestChainVerified || !report.PartSetRootVerified || !report.ParquetSchemaVerified || !report.ParquetRowsVerified || !report.ParquetFileHashesVerified || !report.CanonicalRowChainRootVerified || report.RowCount != generated.RowCount || report.PartCount != uint64(len(parts)) {
		t.Fatalf("delivery report identity mismatch: %+v", report)
	}
	reused, err := reader.FetchReplay(ctx, plan, "")
	if err != nil || reused.ManifestPath != fetched.ManifestPath || reused.ParquetPaths[parts[0].PartKey] != fetched.ParquetPaths[parts[0].PartKey] {
		t.Fatalf("verified cache was not reused: %+v err=%v", reused, err)
	}
	if backend.writeCount() != readerWrites {
		t.Fatal("read-only selector/fetch/verify mutated remote state")
	}
}

func buildM3E2ERawTruth(t *testing.T, scope archive.ScopeConfig, producerInstanceID string) (archive.RawObject, archive.RawDayManifest, []byte) {
	t.Helper()
	fixture, err := fake.BatchFixture()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(fixture.Frame)
	if err != nil {
		t.Fatal(err)
	}
	message, err := protocol.DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	batch := message.(protocol.BatchFrameV1)
	batch.BatchSequence = 1
	batch.RequestedFromMSC = time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli()
	batch.Records[0].TimeMSC = batch.RequestedFromMSC + 100
	batch.Records[0].CaptureSequence = 1
	batch.SessionLeaseID = protocol.DeriveSessionLeaseID(producerInstanceID, batch.ProducerSessionID, scope.CampaignID, scope.ProviderID, scope.StableFeedID, scope.BrokerServerFingerprint, scope.ExactSourceSymbol)
	encoded, err := protocol.EncodeMessage(batch)
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(t.TempDir(), scope.GatewayBuildIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(encoded, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	sealed, err := store.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	rawObject, err := archive.PromoteSealedSegment(t.TempDir(), sealed.Path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{Scope: scope, Date: "2024-03-09", RawObjects: []archive.RawObject{rawObject}, TerminalSyncStatus: "complete", CompletenessStatus: "settled_snapshot", LogicalCloseTimeS: 1710028800})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return rawObject, manifest, manifestBytes
}

func equalM3E2EStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var _ r2.BoundedObjectBackend = (*m3NetworkFreeBackend)(nil)
var _ r2.ReadBackend = (*m3NetworkFreeBackend)(nil)
var _ r2.ReplayActionTool = (*m3NetworkFreeActionTool)(nil)
var _ r2.ReplayEventStore = (*m3FailingEventStore)(nil)
