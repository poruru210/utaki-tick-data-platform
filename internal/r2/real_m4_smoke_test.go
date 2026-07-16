//go:build m4_real_r2_smoke

package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
)

const (
	m4RealR2SmokeEnv               = "TICK_M4_REAL_R2_SMOKE"
	m4RealR2PhaseEnv               = "TICK_M4_REAL_R2_PHASE"
	m4RealR2ConfirmEnv             = "TICK_M4_REAL_R2_CONFIRM"
	m4RealR2BucketEnv              = "TICK_M4_REAL_R2_BUCKET"
	m4RealR2PrefixEnv              = "TICK_M4_REAL_R2_PREFIX"
	m4RealR2EndpointEnv            = "TICK_M4_REAL_R2_ENDPOINT"
	m4RealR2RegionEnv              = "TICK_M4_REAL_R2_REGION"
	m4RealR2RunIDEnv               = "TICK_M4_REAL_R2_RUN_ID"
	m4RealR2OldAccessKeyEnv        = "TICK_M4_REAL_R2_OLD_ACCESS_KEY_ENV"
	m4RealR2OldSecretKeyEnv        = "TICK_M4_REAL_R2_OLD_SECRET_KEY_ENV"
	m4RealR2NewAccessKeyEnv        = "TICK_M4_REAL_R2_NEW_ACCESS_KEY_ENV"
	m4RealR2NewSecretKeyEnv        = "TICK_M4_REAL_R2_NEW_SECRET_KEY_ENV"
	m4RealR2ReadAccessKeyEnv       = "TICK_M4_REAL_R2_READ_ACCESS_KEY_ENV"
	m4RealR2ReadSecretKeyEnv       = "TICK_M4_REAL_R2_READ_SECRET_KEY_ENV"
	m4RealR2OldCredentialIDEnv     = "TICK_M4_REAL_R2_OLD_CREDENTIAL_ID"
	m4RealR2ProcessStopDigestEnv   = "TICK_M4_REAL_R2_PROCESS_STOP_EVIDENCE_DIGEST"
	m4RealR2ProcessStoppedAtEnv    = "TICK_M4_REAL_R2_PROCESS_STOPPED_AT_MS"
	m4RealR2CredentialRevokedAtEnv = "TICK_M4_REAL_R2_CREDENTIAL_REVOKED_AT_MS"
	m4RealR2OperatorIDEnv          = "TICK_M4_REAL_R2_OPERATOR_ID"
	m4RealR2OperatorConfirmEnv     = "TICK_M4_REAL_R2_OPERATOR_CONFIRM"
	m4RealR2OperatorConfirmPhrase  = "I_UNDERSTAND_OLD_PROCESS_STOPPED_AND_CREDENTIAL_REVOKED"
	m4RealR2ConfirmPhrase          = "I_UNDERSTAND_M4_NO_OVERWRITE"
)

var m4RealR2SafeIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var m4RealR2EnvName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// TestOptionalM4RealR2HandoverSmoke is intentionally opt-in and phase
// separated. Prepare writes only a synthetic prior claim. Verify requires the
// operator to have stopped the old writer and revoked its credential before it
// tests the old credential, then exercises the new writer and read-only path.
// The test never prints bucket names, endpoints, credential values, or tokens.
func TestOptionalM4RealR2HandoverSmoke(t *testing.T) {
	if os.Getenv(m4RealR2SmokeEnv) != "1" {
		t.Skip("set TICK_M4_REAL_R2_SMOKE=1 to run the isolated real-R2 handover smoke")
	}
	phase := requiredM4RealR2Env(t, m4RealR2PhaseEnv)
	if phase != "prepare" && phase != "verify" {
		t.Fatalf("%s must be prepare or verify", m4RealR2PhaseEnv)
	}

	settings := m4RealR2Settings(t)
	runID := m4RealR2Identity(t, m4RealR2RunIDEnv)
	prefix := requiredM4RealR2Env(t, m4RealR2PrefixEnv)
	isolatedPrefix := "m4-smoke/" + runID
	if (prefix != isolatedPrefix && !strings.HasPrefix(prefix, isolatedPrefix+"/")) || strings.HasPrefix(prefix, "/") || strings.Contains(prefix, "..") || strings.Contains(prefix, "//") {
		t.Fatalf("%s must be an isolated m4-smoke/ prefix without traversal", m4RealR2PrefixEnv)
	}

	scope := m4RealR2Scope(runID)
	layout, err := NewLayout(prefix, scope)
	if err != nil {
		t.Fatalf("construct isolated layout failed (%T)", err)
	}
	limits := operations.DefaultResourceLimits
	if err := limits.Validate(); err != nil {
		t.Fatalf("validate resource limits failed (%T)", err)
	}

	switch phase {
	case "prepare":
		m4RealR2Prepare(t, settings, layout, limits)
	case "verify":
		m4RealR2Verify(t, settings, layout, limits, runID)
	}
}

func m4RealR2Settings(t *testing.T) S3BackendConfig {
	t.Helper()
	endpoint := requiredM4RealR2Env(t, m4RealR2EndpointEnv)
	if err := ValidateHTTPSHostEndpoint(endpoint); err != nil {
		t.Fatalf("%s is invalid: %v", m4RealR2EndpointEnv, err)
	}
	return S3BackendConfig{
		Bucket:   requiredM4RealR2Env(t, m4RealR2BucketEnv),
		Endpoint: endpoint,
		Region:   os.Getenv(m4RealR2RegionEnv),
	}
}

func m4RealR2Prepare(t *testing.T, settings S3BackendConfig, layout Layout, limits operations.ResourceLimits) {
	t.Helper()
	if requiredM4RealR2Env(t, m4RealR2ConfirmEnv) != m4RealR2ConfirmPhrase {
		t.Fatalf("%s confirmation phrase is invalid", m4RealR2ConfirmEnv)
	}
	oldAccessEnv := m4RealR2CredentialEnv(t, m4RealR2OldAccessKeyEnv)
	oldSecretEnv := m4RealR2CredentialEnv(t, m4RealR2OldSecretKeyEnv)
	backend, err := NewS3BackendWithEnv(context.Background(), settings, oldAccessEnv, oldSecretEnv)
	if err != nil {
		t.Fatalf("construct old writer failed (%T)", err)
	}
	claim, err := NewPublisherClaim(layout.Scope)
	if err != nil {
		t.Fatalf("construct prior claim failed (%T)", err)
	}
	body, err := claim.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonicalize prior claim failed (%T)", err)
	}
	key, err := layout.ClaimKey(layout.Scope.PublisherEpoch)
	if err != nil {
		t.Fatalf("derive prior claim key failed (%T)", err)
	}
	ctx, cancel := m4RealR2Context(t, limits)
	err = backend.PutIfAbsent(ctx, key, body)
	cancel()
	if err == nil {
		t.Log("M4 real-R2 prepare wrote the synthetic prior claim")
		return
	}
	if !errors.Is(err, ErrObjectExists) {
		t.Fatalf("write prior claim failed (%T)", err)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	stored, getErr := backend.GetLimited(ctx, key, limits.MaxAPIRequestBytes)
	cancel()
	if getErr != nil || !bytes.Equal(stored, body) {
		t.Fatalf("existing prior claim was not the exact same content (%T)", getErr)
	}
	t.Log("M4 real-R2 prepare found the exact synthetic prior claim")
}

func m4RealR2Verify(t *testing.T, settings S3BackendConfig, layout Layout, limits operations.ResourceLimits, runID string) {
	t.Helper()
	if requiredM4RealR2Env(t, m4RealR2ConfirmEnv) != m4RealR2ConfirmPhrase {
		t.Fatalf("%s confirmation phrase is invalid", m4RealR2ConfirmEnv)
	}
	if requiredM4RealR2Env(t, m4RealR2OperatorConfirmEnv) != m4RealR2OperatorConfirmPhrase {
		t.Fatalf("%s confirmation phrase is invalid", m4RealR2OperatorConfirmEnv)
	}
	oldAccessEnv := m4RealR2CredentialEnv(t, m4RealR2OldAccessKeyEnv)
	oldSecretEnv := m4RealR2CredentialEnv(t, m4RealR2OldSecretKeyEnv)
	newAccessEnv := m4RealR2CredentialEnv(t, m4RealR2NewAccessKeyEnv)
	newSecretEnv := m4RealR2CredentialEnv(t, m4RealR2NewSecretKeyEnv)
	readAccessEnv := m4RealR2CredentialEnv(t, m4RealR2ReadAccessKeyEnv)
	readSecretEnv := m4RealR2CredentialEnv(t, m4RealR2ReadSecretKeyEnv)
	validateM4RealR2CredentialSeparation(t,
		m4RealR2CredentialPair{label: "old", accessEnv: oldAccessEnv, secretEnv: oldSecretEnv},
		m4RealR2CredentialPair{label: "new", accessEnv: newAccessEnv, secretEnv: newSecretEnv},
		m4RealR2CredentialPair{label: "read-only", accessEnv: readAccessEnv, secretEnv: readSecretEnv},
	)
	old, err := NewS3BackendWithEnv(context.Background(), settings, oldAccessEnv, oldSecretEnv)
	if err != nil {
		t.Fatalf("construct old writer for revocation probe failed (%T)", err)
	}
	newWriter, err := NewS3BackendWithEnv(context.Background(), settings, newAccessEnv, newSecretEnv)
	if err != nil {
		t.Fatalf("construct new writer failed (%T)", err)
	}
	readOnly, err := NewS3ReadBackendWithProvider(context.Background(), S3ReadBackendConfig{
		Bucket:   settings.Bucket,
		Endpoint: settings.Endpoint,
		Region:   settings.Region,
	}, m4StaticCredentialProvider{accessKey: os.Getenv(readAccessEnv), secretKey: os.Getenv(readSecretEnv)})
	if err != nil {
		t.Fatalf("construct read-only backend failed (%T)", err)
	}

	priorKey, err := layout.ClaimKey(layout.Scope.PublisherEpoch)
	if err != nil {
		t.Fatalf("derive prior claim key failed (%T)", err)
	}
	ctx, cancel := m4RealR2Context(t, limits)
	priorBytes, err := newWriter.GetLimited(ctx, priorKey, limits.MaxAPIRequestBytes)
	cancel()
	if err != nil {
		t.Fatalf("new writer could not read the prepared prior claim (%T)", err)
	}
	priorClaim, err := NewPublisherClaim(layout.Scope)
	if err != nil {
		t.Fatalf("construct expected prior claim failed (%T)", err)
	}
	expectedPriorBytes, err := priorClaim.CanonicalJSON()
	if err != nil || !bytes.Equal(priorBytes, expectedPriorBytes) {
		t.Fatalf("new writer read a different prior claim (%T)", err)
	}

	probeKey, err := layout.RemoteKey("handover-probes/old-write-" + runID + ".json")
	if err != nil {
		t.Fatalf("derive old-write probe key failed (%T)", err)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	probeErr := old.PutIfAbsent(ctx, probeKey, []byte("old-credential-must-be-revoked"))
	cancel()
	if probeErr == nil {
		t.Fatalf("old credential still had write permission; probe object was left for operator cleanup")
	}
	if !errors.Is(probeErr, ErrRemotePermission) {
		t.Fatalf("old credential failure was not classified as permission denied (%T)", probeErr)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	_, probeGetErr := readOnly.GetLimited(ctx, probeKey, limits.MaxAPIRequestBytes)
	cancel()
	if !errors.Is(probeGetErr, ErrObjectNotFound) {
		t.Fatalf("old write probe object was not absent (%T)", probeGetErr)
	}

	configDigest, err := layout.Scope.ConfigHash()
	if err != nil {
		t.Fatalf("hash handover scope failed (%T)", err)
	}
	now := uint64(time.Now().UnixMilli())
	processStoppedAt := m4RealR2Timestamp(t, m4RealR2ProcessStoppedAtEnv, now)
	credentialRevokedAt := m4RealR2Timestamp(t, m4RealR2CredentialRevokedAtEnv, now)
	if processStoppedAt > credentialRevokedAt {
		t.Fatalf("process stop evidence is later than credential revocation evidence")
	}
	runtimeIdentityDigest := m4RealR2Digest(t, m4RealR2ProcessStopDigestEnv)
	evidence := HandoverOperatorEvidence{
		EvidenceVersion: "operator-handover-evidence-v1",
		ScopeKey:        mustM4RealR2ScopeKey(t, layout.Scope),
		PriorEpoch:      layout.Scope.PublisherEpoch,
		Process: ProcessStopEvidence{
			EvidenceVersion:       "process-stop-evidence-v1",
			ScopeKey:              mustM4RealR2ScopeKey(t, layout.Scope),
			PriorPublisherEpoch:   layout.Scope.PublisherEpoch,
			RuntimeIdentityDigest: runtimeIdentityDigest,
			ObservedAtUnixMS:      processStoppedAt,
			Stopped:               true,
		},
		Credential: CredentialRevocationEvidence{
			EvidenceVersion:    "credential-revocation-evidence-v1",
			ScopeKey:           mustM4RealR2ScopeKey(t, layout.Scope),
			CredentialIDDigest: sha256.Sum256([]byte(requiredM4RealR2Env(t, m4RealR2OldCredentialIDEnv))),
			ScopeDigest:        configDigest,
			RevokedAtUnixMS:    credentialRevokedAt,
			Revoked:            true,
		},
	}
	nextEpoch := layout.Scope.PublisherEpoch + 1
	seal, err := SealHandover(layout, nextEpoch, evidence)
	if err != nil {
		t.Fatalf("seal handover failed (%T)", err)
	}
	confirmation := HandoverConfirmationRecord{
		ConfirmationVersion: "operator-handover-confirmation-v1",
		ScopeKey:            seal.ScopeKey,
		PriorEpoch:          seal.Artifact.PriorPublisherEpoch,
		SealDigest:          seal.Digest,
		OperatorIDDigest:    sha256.Sum256([]byte(requiredM4RealR2Env(t, m4RealR2OperatorIDEnv))),
		ConfirmedAtUnixMS:   uint64(time.Now().UnixMilli()),
		Confirmed:           true,
	}
	remote, err := NewReplayRemoteReadAdapter(newWriter)
	if err != nil {
		t.Fatalf("construct new bounded observer failed (%T)", err)
	}
	for step := 0; step < 3; step++ {
		ctx, cancel = m4RealR2Context(t, limits)
		observation, observeErr := ObserveHandover(ctx, remote, layout, seal, limits)
		cancel()
		if observeErr != nil {
			t.Fatalf("observe handover step %d failed (%T)", step, observeErr)
		}
		decision, reconcileErr := ReconcileHandover(seal, observation, evidence, confirmation)
		if reconcileErr != nil {
			t.Fatalf("reconcile handover step %d failed (%T)", step, reconcileErr)
		}
		if decision.Kind == HandoverReady {
			break
		}
		if decision.Kind != HandoverExecute || len(decision.Actions) != 1 {
			t.Fatalf("handover stopped at step %d (%s)", step, decision.Kind)
		}
		ctx, cancel = m4RealR2Context(t, limits)
		if executeErr := ExecuteHandoverActions(ctx, remote, newWriter, layout, seal, limits, evidence, confirmation, decision); executeErr != nil {
			cancel()
			t.Fatalf("execute handover step %d failed (%T)", step, executeErr)
		}
		cancel()
	}
	ctx, cancel = m4RealR2Context(t, limits)
	finalObservation, err := ObserveHandover(ctx, remote, layout, seal, limits)
	cancel()
	if err != nil {
		t.Fatalf("final handover observation failed (%T)", err)
	}
	finalDecision, err := ReconcileHandover(seal, finalObservation, evidence, confirmation)
	if err != nil || finalDecision.Kind != HandoverReady {
		t.Fatalf("handover did not reach ready (%s, %T)", finalDecision.Kind, err)
	}

	ctx, cancel = m4RealR2Context(t, limits)
	sameErr := newWriter.PutIfAbsent(ctx, seal.ArtifactKey, seal.CanonicalBytes)
	cancel()
	if !errors.Is(sameErr, ErrObjectExists) {
		t.Fatalf("same-content conditional collision was not rejected (%T)", sameErr)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	storedArtifact, err := newWriter.GetLimited(ctx, seal.ArtifactKey, limits.MaxAPIRequestBytes)
	cancel()
	if err != nil || !bytes.Equal(storedArtifact, seal.CanonicalBytes) {
		t.Fatalf("same-content collision changed the artifact (%T)", err)
	}
	different := []byte("different-content-must-not-overwrite")
	ctx, cancel = m4RealR2Context(t, limits)
	differentErr := newWriter.PutIfAbsent(ctx, seal.ArtifactKey, different)
	cancel()
	if !errors.Is(differentErr, ErrObjectExists) {
		t.Fatalf("different-content conditional collision was not rejected (%T)", differentErr)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	storedArtifact, err = newWriter.GetLimited(ctx, seal.ArtifactKey, limits.MaxAPIRequestBytes)
	cancel()
	if err != nil || !bytes.Equal(storedArtifact, seal.CanonicalBytes) {
		t.Fatalf("different-content collision changed the artifact (%T)", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, canceledErr := newWriter.GetLimited(ctx, seal.ArtifactKey, limits.MaxAPIRequestBytes)
	if canceledErr == nil || !errors.Is(canceledErr, context.Canceled) {
		t.Fatalf("canceled remote read did not preserve context cancellation (%T)", canceledErr)
	}

	readRemote, err := NewReplayRemoteReadAdapterFromReadBackend(readOnly)
	if err != nil {
		t.Fatalf("construct read-only bounded observer failed (%T)", err)
	}
	nextClaimKey, err := layout.ClaimKey(nextEpoch)
	if err != nil {
		t.Fatalf("derive next-claim key failed (%T)", err)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	nextBytes, err := readOnly.GetLimited(ctx, nextClaimKey, limits.MaxAPIRequestBytes)
	cancel()
	if err != nil || !bytes.Equal(nextBytes, seal.NextClaimBytes) {
		t.Fatalf("read-only credential could not read next claim (%T)", err)
	}
	readProbeKey, err := layout.RemoteKey("handover-probes/read-only-write-" + runID + ".json")
	if err != nil {
		t.Fatalf("derive read-only write probe key failed (%T)", err)
	}
	readProbeWriter, err := NewS3BackendWithEnv(context.Background(), settings, readAccessEnv, readSecretEnv)
	if err != nil {
		t.Fatalf("construct read-only permission probe failed (%T)", err)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	readProbeErr := readProbeWriter.PutIfAbsent(ctx, readProbeKey, []byte("read-only-credential-must-not-write"))
	cancel()
	if readProbeErr == nil {
		t.Fatalf("read-only credential still had write permission; probe object was left for operator cleanup")
	}
	if !errors.Is(readProbeErr, ErrRemotePermission) {
		t.Fatalf("read-only credential failure was not classified as permission denied (%T)", readProbeErr)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	_, readProbeGetErr := readOnly.GetLimited(ctx, readProbeKey, limits.MaxAPIRequestBytes)
	cancel()
	if !errors.Is(readProbeGetErr, ErrObjectNotFound) {
		t.Fatalf("read-only write probe object was not absent (%T)", readProbeGetErr)
	}
	candidatePrefix, err := layout.HandoverCandidatePrefix(nextEpoch)
	if err != nil {
		t.Fatalf("derive candidate prefix failed (%T)", err)
	}
	ctx, cancel = m4RealR2Context(t, limits)
	listed, err := readRemote.ListLimited(ctx, candidatePrefix, 2)
	cancel()
	if err != nil || !listed.Complete || len(listed.Objects) != 1 || listed.Objects[0].Key != nextClaimKey {
		t.Fatalf("read-only candidate inventory was not exact (%T)", err)
	}
	t.Logf("M4 real-R2 handover ready: run=%s seal=%s prior-claim=%s", runID, protocol.EncodeHashHex(seal.Digest), protocol.EncodeHashHex(mustM4RealR2ClaimDigest(t, expectedPriorBytes)))
}

type m4StaticCredentialProvider struct {
	accessKey string
	secretKey string
}

func (p m4StaticCredentialProvider) Load(context.Context) (credentials.Credentials, error) {
	return credentials.Credentials{AccessKeyID: p.accessKey, SecretAccessKey: p.secretKey}, nil
}

func m4RealR2Context(t *testing.T, limits operations.ResourceLimits) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout, err := limits.RequestTimeout()
	if err != nil {
		t.Fatalf("derive request timeout failed (%T)", err)
	}
	return context.WithTimeout(context.Background(), timeout)
}

func m4RealR2Scope(runID string) archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID:               "m4-real-r2",
		CampaignID:              "handover-" + runID,
		ProviderID:              "m4-provider",
		StableFeedID:            "m4-feed",
		ExactSourceSymbol:       "EURUSD.raw",
		BrokerServerFingerprint: "m4-broker",
		GatewayBuildIdentity:    "m4-gateway",
		ProducerBuildIdentity:   "m4-producer",
		DayDefinitionID:         "utc-day-v1",
		SettlePolicy:            "manual-v1",
		PublisherID:             "m4-old-publisher",
		PublisherEpoch:          1,
	}
}

func requiredM4RealR2Env(t *testing.T, name string) string {
	t.Helper()
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		t.Fatalf("required M4 real-R2 environment is missing: %s", name)
	}
	return value
}

func m4RealR2CredentialEnv(t *testing.T, name string) string {
	t.Helper()
	value := requiredM4RealR2Env(t, name)
	if !m4RealR2EnvName.MatchString(value) {
		t.Fatalf("credential environment name is invalid: %s", name)
	}
	return value
}

type m4RealR2CredentialPair struct {
	label     string
	accessEnv string
	secretEnv string
}

func validateM4RealR2CredentialSeparation(t *testing.T, pairs ...m4RealR2CredentialPair) {
	t.Helper()
	names := make(map[string]string, len(pairs)*2)
	fingerprints := make(map[[32]byte]string, len(pairs))
	for _, pair := range pairs {
		for _, name := range []string{pair.accessEnv, pair.secretEnv} {
			if prior, exists := names[name]; exists {
				t.Fatalf("credential environment names for %s and %s are shared", prior, pair.label)
			}
			names[name] = pair.label
		}
		access, accessOK := os.LookupEnv(pair.accessEnv)
		secret, secretOK := os.LookupEnv(pair.secretEnv)
		if !accessOK || !secretOK || access == "" || secret == "" {
			t.Fatalf("credential values for %s are unavailable", pair.label)
		}
		fingerprint := sha256.Sum256([]byte(access + "\x00" + secret))
		if prior, exists := fingerprints[fingerprint]; exists {
			t.Fatalf("credential values for %s and %s are shared", prior, pair.label)
		}
		fingerprints[fingerprint] = pair.label
	}
}

func m4RealR2Identity(t *testing.T, name string) string {
	t.Helper()
	value := requiredM4RealR2Env(t, name)
	if !m4RealR2SafeIdentity.MatchString(value) {
		t.Fatalf("safe M4 real-R2 identity is invalid: %s", name)
	}
	return value
}

func m4RealR2Digest(t *testing.T, name string) [32]byte {
	t.Helper()
	digest, err := protocol.ParseHashHex(requiredM4RealR2Env(t, name))
	if err != nil {
		t.Fatalf("evidence digest is invalid: %s", name)
	}
	return digest
}

func m4RealR2Timestamp(t *testing.T, name string, now uint64) uint64 {
	t.Helper()
	value := requiredM4RealR2Env(t, name)
	timestamp, err := strconv.ParseUint(value, 10, 64)
	if err != nil || timestamp == 0 || timestamp > now {
		t.Fatalf("evidence timestamp is invalid: %s", name)
	}
	return timestamp
}

func mustM4RealR2ScopeKey(t *testing.T, scope archive.ScopeConfig) string {
	t.Helper()
	key, err := archive.ScopePathKey(scope)
	if err != nil {
		t.Fatalf("derive scope key failed (%T)", err)
	}
	return key
}

func mustM4RealR2ClaimDigest(t *testing.T, body []byte) [32]byte {
	t.Helper()
	return protocol.PublisherClaimDomainDigest(body)
}
