package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/retention"
	"tick-data-platform/internal/testsupport"
	"tick-data-platform/producers/fake"
)

func TestPruneRemoteVerificationTruthTable(t *testing.T) {
	scope := pruneTestScope()
	layout, err := r2.NewLayout("v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256([]byte("raw-outbox"))
	root := t.TempDir()
	config := ingest.Config{RawOutboxRoot: root}
	limits := retention.ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100}
	base := pruneTestCandidate(scope, sha, limits)
	cases := []struct {
		name   string
		exact  bool
		action bool
	}{
		{name: "remote observation only", exact: false},
		{name: "journal intent only", exact: false},
		{name: "receipt only", exact: false},
		{name: "ETag only", exact: false},
		{name: "remote_verified exact identity", exact: true, action: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidates := []retention.CandidateFact{base}
			localPath := filepath.Join(config.RawOutboxRoot, filepath.FromSlash(base.Artifact.TrustedPath))
			journal := newPruneTruthJournal(t, scope, layout, sha, 10, test.name, localPath, test.exact)
			defer journal.Stop(context.Background())
			if err := bindPruneRemoteVerification(candidates, config, layout, journal); err != nil {
				t.Fatal(err)
			}
			input := retention.PlannerInput{
				Candidates: candidates, CurrentWallTimeUnixMS: 300,
				DurableWallTimeUnixMS: 300, WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10,
				ProofLimits: limits, Disk: retention.DiskHigh, RequireRemoteVerified: true,
			}
			plan, err := retention.BuildPrunePlan(input)
			if err != nil {
				t.Fatal(err)
			}
			if (len(plan.Actions) == 1) != test.action || (len(plan.Blocked) == 1) != !test.action {
				t.Fatalf("truth table plan = %+v", plan)
			}
		})
	}
}

func TestRunPruneLocalUsesFullCommandPathWithBoundedObserver(t *testing.T) {
	walRoot, rawRoot, artifact, _, rawPath := newPruneRawOutboxFixture(t)
	if err := retention.PublishWallClock(walRoot, 300); err != nil {
		t.Fatal(err)
	}
	scope := pruneTestScope()
	scope.ProtocolVersion = protocol.ProtocolVersion
	scope.ProtocolLimits = archive.ProtocolLimits{MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: protocol.MaxRecords, MaxStringBytes: protocol.MaxStringBytes}
	layout, err := r2.NewLayout("v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	journal := newPruneTruthJournal(t, scope, layout, artifact.ContentSHA256, artifact.Bytes, "command", rawPath, true)
	journalPath := journal.Path()
	if err := journal.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	retentionPath := filepath.Join(root, "retention.toml")
	retentionText := fmt.Sprintf(`retention_config_version = "tick-retention-v1"
endpoint = "https://account.r2.cloudflarestorage.com"
bucket = "tick-raw"
credentials_path = "unused-credentials.json"
region = "auto"
immutable_root = "v1"
dataset_id = "%s"
provider_id = "%s"
stable_feed_id = "%s"
exact_source_symbol = "%s"
broker_server_fingerprint = "%s"
gateway_build_identity = "%s"
producer_build_identity = "%s"
day_definition_id = "%s"
settle_policy = "%s"
publisher_id = "%s"
publisher_epoch = 1
max_frame_bytes = %d
max_records = %d
max_string_bytes = %d
date = "2024-03-09"
grace_ms = 100
`, scope.DatasetID, scope.ProviderID, scope.StableFeedID, scope.ExactSourceSymbol,
		scope.BrokerServerFingerprint, scope.GatewayBuildIdentity, scope.ProducerBuildIdentity,
		scope.DayDefinitionID, scope.SettlePolicy, scope.PublisherID, protocol.MaxFrameBytes, protocol.MaxRecords, protocol.MaxStringBytes)
	if err := os.WriteFile(retentionPath, []byte(retentionText), 0o600); err != nil {
		t.Fatal(err)
	}
	configValue := appconfig.Config{
		ListenAddress: "127.0.0.1:0", GatewayInstanceID: "gateway-prune-command-test",
		WALRoot: walRoot, RawOutboxRoot: rawRoot, JournalPath: filepath.Join(root, "gateway.sqlite"),
		MaxFrameBytes: protocol.MaxFrameBytes, MaxRecords: protocol.MaxRecords, InitialBatchCount: 1,
		MaximumBatchCount: 1, DenseBoundaryHardCap: 1, SessionLeaseTimeoutMS: 30000, HeartbeatIdleTimeoutMS: 60000,
		DiskHighFreeBytes: 512 << 20, DiskCriticalFreeBytes: 256 << 20, DiskEmergencyFreeBytes: 64 << 20,
		ProducerBuildID: scope.ProducerBuildIdentity, DatasetID: scope.DatasetID, ProviderID: scope.ProviderID, StableFeedID: scope.StableFeedID, BrokerServerFingerprint: scope.BrokerServerFingerprint,
		ExactSourceSymbol: scope.ExactSourceSymbol, GatewayBuildIdentity: scope.GatewayBuildIdentity,
		DayDefinitionID: scope.DayDefinitionID, SettlePolicy: scope.SettlePolicy, PublisherID: scope.PublisherID, PublisherEpoch: scope.PublisherEpoch,
		Credentials: appconfig.CredentialsConfig{Provider: "file", Path: "unused-credentials.json"},
		R2:          appconfig.R2Config{Endpoint: "https://account.r2.cloudflarestorage.com", Bucket: "tick-raw", Region: "auto", ImmutableRoot: "v1"},
		Publication: appconfig.PublicationConfig{
			CatalogPath: filepath.Join(root, "catalog.sqlite"), RemoteJournalPath: journalPath,
			ManifestRoot: filepath.Join(root, "manifests"), ReceiptRoot: filepath.Join(root, "receipts"),
			SealMaxBytes: 1, SealIntervalMS: 1000, ScanIntervalMS: 1000, RetryMinMS: 1, RetryMaxMS: 1000,
			MaxPendingSegments: 100, MaxPendingBytes: 1 << 20,
		},
	}
	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	commandErr := runPruneLocalWithObserver(configValue, []string{"--dry-run", "--retention-config", retentionPath}, pruneCommandObserver{scope: scope})
	_ = writer.Close()
	os.Stdout = oldStdout
	output, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if commandErr != nil {
		t.Fatalf("full prune-local dry-run error = %v; output=%s", commandErr, output)
	}
	var decoded map[string]any
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("prune-local output is not JSON: %v; output=%s", err, output)
	}
	canonicalPlan, ok := decoded["canonical_plan"].(string)
	if !ok || !strings.Contains(canonicalPlan, `"require_remote_verified":true`) || decoded["remote_verified_required"] != true {
		t.Fatalf("prune-local command did not preserve remote verification gate: %+v", decoded)
	}
}

func TestPruneLocalCompletionRecorderConvergesAfterCrashBeforeJournalRecord(t *testing.T) {
	_, rawRoot, artifact, candidate, rawPath := newPruneRawOutboxFixture(t)
	config := ingest.Config{RawOutboxRoot: rawRoot}
	scope := pruneTestScope()
	layout, err := r2.NewLayout("v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	journal := newPruneTruthJournal(t, scope, layout, artifact.ContentSHA256, artifact.Bytes, "crash", rawPath, true)
	journalPath := journal.Path()
	defer journal.Stop(context.Background())
	catalog, err := publication.NewCatalog(filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer catalog.Stop(context.Background())
	if err := catalog.UpsertSegment(context.Background(), publication.SegmentRecord{
		Identity:   publication.SegmentIdentity(artifact.ContentSHA256),
		SealedPath: filepath.Join(rawRoot, "sealed", "source.wal"), RawKey: artifact.TrustedPath,
		RawPath: rawPath, SHA256: artifact.ContentSHA256, Bytes: artifact.Bytes,
		StartSequence: artifact.WALRange.StartSequence, EndSequence: artifact.WALRange.EndSequence,
		AffectedDates: []string{"2024-03-09"}, State: publication.SegmentStatePublished, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	candidate.RemoteVerified = true
	limits := candidate.Proof.Limits
	plan, err := retention.BuildPrunePlan(retention.PlannerInput{
		Candidates: []retention.CandidateFact{candidate}, CurrentWallTimeUnixMS: 300,
		DurableWallTimeUnixMS: 300, WALExpectedStart: 1, GraceMS: 100,
		MaxCandidates: 10, ProofLimits: limits, Disk: retention.DiskHigh,
		RequireRemoteVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	faulty, err := retention.NewPruneExecutor(
		retention.PruneRoots{RawOutboxRoot: rawRoot},
		pruneTestFault{point: retention.FaultAfterUnlink},
	)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := faulty.Execute(context.Background(), plan, []retention.CandidateFact{candidate}); !errors.Is(err, retention.ErrPruneAvailability) || len(report.Completed) != 0 {
		t.Fatalf("executor crash seam report=%+v err=%v", report, err)
	}
	if _, err := os.Stat(rawPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("executor did not unlink raw source before crash: %v", err)
	}

	// A process restart must reconstruct the pending completion from disk and
	// reopen the remote verification journal before Catalog records local_pruned.
	if err := journal.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal, err = testsupport.NewStartedPublicationJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Stop(context.Background())
	recovered, err := retention.InventoryPruneCompletions(rawRoot, retention.ArtifactRawOutbox, 10, 1<<20)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("completion inventory after restart = %+v err=%v", recovered, err)
	}
	if err := bindPruneRemoteVerification(recovered, config, layout, journal); err != nil {
		t.Fatal(err)
	}
	retryPlan, err := retention.BuildPrunePlan(retention.PlannerInput{
		Candidates: recovered, CurrentWallTimeUnixMS: 400, DurableWallTimeUnixMS: 400,
		WALExpectedStart: 1, GraceMS: 100, MaxCandidates: 10,
		ProofLimits: limits, Disk: retention.DiskHigh, RequireRemoteVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	retryExecutor, err := retention.NewPruneExecutor(retention.PruneRoots{RawOutboxRoot: rawRoot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	report, err := retryExecutor.Execute(context.Background(), retryPlan, recovered)
	if err != nil || len(report.Completed) != 1 {
		t.Fatalf("restart retry plan=%+v recovered=%+v report=%+v err=%v", retryPlan, recovered, report, err)
	}
	if err := recordPruneLocalCompletions(context.Background(), config, report, recovered, catalog); err != nil {
		t.Fatal(err)
	}
	if err := recordPruneLocalCompletions(context.Background(), config, report, recovered, catalog); err != nil {
		t.Fatal(err)
	}
	remoteKey, err := layout.RemoteKey(archive.RawWALObjectKey(artifact.ContentSHA256))
	if err != nil {
		t.Fatal(err)
	}
	verified, found, err := journal.FindRemoteVerifiedObjectAtPath(remoteKey, rawPath, artifact.ContentSHA256, artifact.Bytes)
	if err != nil || !found || verified.State != r2.ObjectStateRemoteVerified {
		t.Fatalf("remote verification after retry = %+v found=%v err=%v", verified, found, err)
	}
	segments, err := catalog.ListSegments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].State != publication.SegmentStateLocalPruned {
		t.Fatalf("Catalog local_pruned state = %+v", segments)
	}
}

type pruneTestFault struct{ point retention.PruneFaultPoint }

func (f pruneTestFault) Inject(point retention.PruneFaultPoint, _ string) error {
	if point == f.point {
		return errors.New("injected prune crash")
	}
	return nil
}

func newPruneRawOutboxFixture(t *testing.T) (string, string, retention.LocalArtifact, retention.CandidateFact, string) {
	t.Helper()
	walRoot := t.TempDir()
	store, err := testsupport.NewStartedWAL(walRoot, "gateway-prune-test")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := fake.BatchFixture()
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	if _, err := store.Append(fixture.Frame, 1710000000, 42); err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	segment, err := store.Seal()
	if err != nil {
		_ = store.Stop(context.Background())
		t.Fatal(err)
	}
	if err := store.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	rawRoot := t.TempDir()
	rawPath := filepath.Join(rawRoot, filepath.FromSlash(archive.RawWALObjectKey(segment.ObjectSHA256)))
	if err := os.MkdirAll(filepath.Dir(rawPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(segment.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rawPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	artifact := retention.LocalArtifact{
		Kind: retention.ArtifactRawOutbox, TrustedPath: archive.RawWALObjectKey(segment.ObjectSHA256),
		Bytes: uint64(segment.FileBytes), ContentSHA256: segment.ObjectSHA256,
		WALRange: &retention.WALRange{StartSequence: segment.StartSequence, EndSequence: segment.LastSequence, StartChainRoot: segment.ChainStart, EndChainRoot: segment.ChainRoot},
	}
	scope := pruneTestScope()
	scopeHash, err := scope.ConfigHash()
	if err != nil {
		t.Fatal(err)
	}
	limits := retention.ProofLimits{MaxProofObjects: 100, MaxProofBytes: 1 << 20, MaxManifestNodes: 100}
	proof := &retention.RetentionProof{
		ProofVersion: retention.RetentionProofVersion, ArtifactKind: artifact.Kind,
		TrustedRelativePath: artifact.TrustedPath, Bytes: artifact.Bytes,
		ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: scopeHash,
		WALRange:            artifact.WALRange,
		Remote:              retention.RemoteObjectObservation{Class: retention.RemoteObservationExact, FullKey: "remote/raw", Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
		CoveringManifestKey: "remote/manifest", CoveringManifestDigest: [32]byte{2}, VerificationReportDigest: [32]byte{3},
		ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: limits,
	}
	return walRoot, rawRoot, artifact, retention.CandidateFact{Artifact: artifact, Proof: proof, FreshRemote: true, CoverageVerified: true}, rawPath
}

type pruneCommandObserver struct{ scope archive.ScopeConfig }

func (o pruneCommandObserver) Observe(_ context.Context, artifact retention.LocalArtifact, limits retention.ProofLimits) (retention.RemoteFact, error) {
	scopeHash, err := o.scope.ConfigHash()
	if err != nil {
		return retention.RemoteFact{}, err
	}
	var walRange *retention.WALRange
	if artifact.WALRange != nil {
		copyRange := *artifact.WALRange
		walRange = &copyRange
	}
	return retention.RemoteFact{
		Class: retention.RemoteObservationExact, CoverageVerified: true,
		Proof: &retention.RetentionProof{
			ProofVersion: retention.RetentionProofVersion, ArtifactKind: artifact.Kind,
			TrustedRelativePath: artifact.TrustedPath, Bytes: artifact.Bytes,
			ContentSHA256: artifact.ContentSHA256, ScopeConfigHash: scopeHash, WALRange: walRange,
			Remote:              retention.RemoteObjectObservation{Class: retention.RemoteObservationExact, FullKey: "remote/" + artifact.TrustedPath, Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256},
			CoveringManifestKey: "remote/manifest", CoveringManifestDigest: [32]byte{2}, VerificationReportDigest: [32]byte{3},
			ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: limits,
		},
	}, nil
}

func (o pruneCommandObserver) ObserveWithBudget(ctx context.Context, artifact retention.LocalArtifact, limits retention.ProofLimits, _ *retention.ProofObservationBudget) (retention.RemoteFact, error) {
	return o.Observe(ctx, artifact, limits)
}

func pruneTestCandidate(scope archive.ScopeConfig, sha [32]byte, limits retention.ProofLimits) retention.CandidateFact {
	scopeHash, _ := scope.ConfigHash()
	artifact := retention.LocalArtifact{
		Kind: retention.ArtifactRawOutbox, TrustedPath: archive.RawWALObjectKey(sha), Bytes: 10, ContentSHA256: sha,
		WALRange: &retention.WALRange{StartSequence: 1, EndSequence: 1, EndChainRoot: [32]byte{4}},
	}
	return retention.CandidateFact{
		Artifact: artifact, FreshRemote: true, CoverageVerified: true,
		Proof: &retention.RetentionProof{
			ProofVersion: retention.RetentionProofVersion, ArtifactKind: artifact.Kind,
			TrustedRelativePath: artifact.TrustedPath, Bytes: artifact.Bytes, ContentSHA256: artifact.ContentSHA256,
			ScopeConfigHash: scopeHash, Remote: retention.RemoteObjectObservation{
				Class: retention.RemoteObservationExact, FullKey: "remote/raw", Bytes: artifact.Bytes, SHA256: artifact.ContentSHA256,
			}, CoveringManifestKey: "remote/manifest", CoveringManifestDigest: [32]byte{2}, VerificationReportDigest: [32]byte{3},
			WALRange:               artifact.WALRange,
			ObservedWallTimeUnixMS: 100, GraceNotBeforeUnixMS: 200, Limits: limits,
		},
	}
}

func pruneTestScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID: "dataset-test", ProviderID: "provider-test", StableFeedID: "feed-test",
		ExactSourceSymbol: "EURUSD", BrokerServerFingerprint: "broker-test", GatewayBuildIdentity: "gateway-test",
		ProducerBuildIdentity: "producer-test", DayDefinitionID: "utc-day-v1", SettlePolicy: "manual-v1",
		PublisherID: "publisher-test", PublisherEpoch: 1,
	}
}

func newPruneTruthJournal(t *testing.T, scope archive.ScopeConfig, layout r2.Layout, sha [32]byte, bytes uint64, scenario, localPath string, exact bool) *r2.PublicationJournal {
	t.Helper()
	journal, err := testsupport.NewStartedPublicationJournal(filepath.Join(t.TempDir(), "remote.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope: scope, Date: "2024-03-09", Revision: 1,
		TerminalSyncStatus: "complete", CompletenessStatus: "provisional",
	})
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	claim, err := r2.NewPublisherClaim(scope)
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	claimHash, err := claim.Digest()
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	descriptor := []byte(`{"scope":"prune-test"}`)
	descriptorHash := sha256.Sum256(descriptor)
	intent := r2.PublicationIntent{
		Scope: scope, Claim: claim, ClaimKey: "claim/" + scenario, ClaimHash: claimHash,
		ScopeDescriptorKey: "descriptor/" + scenario, ScopeDescriptorPath: filepath.Join(t.TempDir(), "descriptor.json"),
		ScopeDescriptorSHA256: descriptorHash, ScopeDescriptorBytes: uint64(len(descriptor)),
		ManifestKey: "manifest/" + scenario, Manifest: manifest, ManifestBytes: manifestBytes,
		ManifestPath: filepath.Join(t.TempDir(), "manifest.json"), ReceiptPath: filepath.Join(t.TempDir(), "receipt.json"),
	}
	if _, err := journal.CreateOrGetIntent(intent); err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	remoteKey, err := layout.RemoteKey(archive.RawWALObjectKey(sha))
	if err != nil {
		_ = journal.Stop(context.Background())
		t.Fatal(err)
	}
	object := r2.PublicationObject{LocalPath: localPath, SHA256: sha, MD5: [16]byte{1}, Bytes: bytes, RemoteKey: remoteKey}
	switch {
	case exact:
		if err := journal.RecordObjectState(intent.ManifestKey, object, r2.ObjectStateRemoteVerified, "etag", time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)); err != nil {
			_ = journal.Stop(context.Background())
			t.Fatal(err)
		}
	case scenario == "receipt only":
		if err := journal.AdvanceStage(intent.ManifestKey, r2.StageReceiptSaved); err != nil {
			_ = journal.Stop(context.Background())
			t.Fatal(err)
		}
	case scenario == "ETag only":
		if err := journal.RecordObjectState(intent.ManifestKey, object, r2.ObjectStateRemoteCommitted, "etag", time.Time{}); err != nil {
			_ = journal.Stop(context.Background())
			t.Fatal(err)
		}
	case scenario == "journal intent only", scenario == "remote observation only":
		// The intent exists, but no object verification state exists.
	}
	return journal
}
