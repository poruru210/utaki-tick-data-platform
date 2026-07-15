package r2

import (
	"fmt"
	"strings"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

func TestLayoutUsesImmutableRootExactlyOnce(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("v1", "r2:isolated-bucket/v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	objectKey := archive.RawWALObjectKey([32]byte{1})
	remoteObject, err := layout.RemoteKey(objectKey)
	if err != nil {
		t.Fatal(err)
	}
	rcloneObject, err := layout.RcloneKey(objectKey)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := "dataset=" + archive.IdentityPathKey(scope.DatasetID) +
		"/provider=" + archive.IdentityPathKey(scope.ProviderID) +
		"/feed=" + archive.IdentityPathKey(scope.StableFeedID) +
		"/symbol=" + archive.IdentityPathKey(scope.ExactSourceSymbol) +
		"/campaign=" + archive.IdentityPathKey(scope.CampaignID) + "/"
	if CampaignPrefix(scope) != wantPrefix {
		t.Fatalf("campaign prefix = %q, want %q", CampaignPrefix(scope), wantPrefix)
	}
	if remoteObject != "v1/"+wantPrefix+objectKey {
		t.Fatalf("S3 key = %q, want %q", remoteObject, "v1/"+wantPrefix+objectKey)
	}
	if rcloneObject != "r2:isolated-bucket/v1/"+wantPrefix+objectKey {
		t.Fatalf("rclone locator = %q, want %q", rcloneObject, "r2:isolated-bucket/v1/"+wantPrefix+objectKey)
	}
	if strings.Count(remoteObject, "v1/") != 1 || strings.Count(rcloneObject, "v1/") != 1 {
		t.Fatalf("immutable roots were duplicated: S3=%q rclone=%q", remoteObject, rcloneObject)
	}
}

func TestLayoutDerivesReplayBundlePrefixesWithoutCallerKeys(t *testing.T) {
	layout, err := NewLayout("v1", "r2:isolated-bucket/v1", layoutTestScope())
	if err != nil {
		t.Fatal(err)
	}
	wantImmutable := "v1/" + CampaignPrefix(layout.Scope)
	wantRclone := "r2:isolated-bucket/v1/" + CampaignPrefix(layout.Scope)
	if got := layout.ImmutableCampaignPrefix(); got != strings.TrimSuffix(wantImmutable, "/") {
		t.Fatalf("immutable campaign prefix = %q", got)
	}
	if got := layout.RcloneCampaignPrefix(); got != strings.TrimSuffix(wantRclone, "/") {
		t.Fatalf("rclone campaign prefix = %q", got)
	}
}

func TestLayoutSeparatesDatasetsWithTheSameCampaignIdentity(t *testing.T) {
	first := layoutTestScope()
	second := first
	second.DatasetID = "dataset-other"
	if CampaignPrefix(first) == CampaignPrefix(second) {
		t.Fatalf("campaign prefixes collide across dataset identities: %q", CampaignPrefix(first))
	}
	if !strings.Contains(CampaignPrefix(first), "dataset="+archive.IdentityPathKey(first.DatasetID)+"/") {
		t.Fatalf("campaign prefix does not bind dataset identity: %q", CampaignPrefix(first))
	}
}

func TestLayoutDerivesManifestLocatorFromOneRelativeKey(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("smoke/v1", "r2:isolated-bucket/smoke/v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	manifest := archive.RawDayManifest{
		Date:            "2024-03-09",
		Revision:        2,
		DayDefinitionID: scope.DayDefinitionID,
	}
	digest, err := archive.ManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	s3Key, err := layout.ManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rcloneKey, err := layout.RcloneManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	wantRelative := "snapshots/raw/day-definition=" + archive.IdentityPathKey(scope.DayDefinitionID) +
		"/date=2024-03-09/raw-day-2-" + fmt.Sprintf("%x", digest) + ".json"
	wantS3 := "smoke/v1/" + CampaignPrefix(scope) + wantRelative
	wantRclone := "r2:isolated-bucket/smoke/v1/" + CampaignPrefix(scope) + wantRelative
	if s3Key != wantS3 || rcloneKey != wantRclone {
		t.Fatalf("manifest locators differ: S3=%q rclone=%q want S3=%q rclone=%q", s3Key, rcloneKey, wantS3, wantRclone)
	}
	if strings.Count(s3Key, CampaignPrefix(scope)) != 1 || strings.Count(rcloneKey, CampaignPrefix(scope)) != 1 {
		t.Fatalf("campaign prefix was duplicated: S3=%q rclone=%q", s3Key, rcloneKey)
	}
}

func TestLayoutRejectsUnsafeRoots(t *testing.T) {
	for _, root := range []string{"/v1", "//server/share", "C:/v1", "v1/../x", "v1/./x"} {
		if _, err := NewLayout(root, "r2:bucket/v1", layoutTestScope()); err == nil {
			t.Fatalf("unsafe immutable root %q was accepted", root)
		}
	}
}

func TestLayoutRejectsForgedManifestDigestAndTraversalDate(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("v1", "r2:bucket/v1", scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, manifest := range []archive.RawDayManifest{
		{Date: "2024-03-09", Revision: 1, ManifestSHA256: [32]byte{1}},
		{Date: "../2024-03-09", Revision: 1},
	} {
		if _, err := layout.ManifestKey(manifest); err == nil {
			t.Fatalf("forged manifest key input was accepted: %+v", manifest)
		}
		if _, err := layout.RcloneManifestKey(manifest); err == nil {
			t.Fatalf("forged rclone manifest key input was accepted: %+v", manifest)
		}
	}
}

func TestLayoutValidatesTrustedFullDerivativeKeys(t *testing.T) {
	layout, err := NewLayout("v1", "r2:bucket/v1", layoutTestScope())
	if err != nil {
		t.Fatal(err)
	}
	part := layoutTestPart()
	fullObject, err := layout.ReplayPartObjectKey(part)
	if err != nil {
		t.Fatal(err)
	}
	if fullObject != "v1/"+CampaignPrefix(layout.Scope)+part.PartKey {
		t.Fatalf("full part object key = %q", fullObject)
	}
	if err := layout.VerifyReplayPartObjectKey(part, fullObject); err != nil {
		t.Fatal(err)
	}
	generic, err := layout.RemoteKey("objects/replay/part-" + fmt.Sprintf("%x", part.PartSHA256) + ".parquet")
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.VerifyReplayPartObjectKey(part, generic); err == nil {
		t.Fatal("generic replay object key was accepted by trusted derivative verifier")
	}
	foreignPart := part
	foreignPart.CampaignID = "campaign-other"
	if _, err := layout.ReplayPartObjectKey(foreignPart); err == nil {
		t.Fatal("cross-campaign part key was accepted by trusted layout")
	}
	fullManifest, err := layout.ReplayPartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.VerifyReplayPartManifestKey(part, fullManifest); err != nil {
		t.Fatal(err)
	}
	manifest := protocol.ReplayDayManifest{
		ManifestVersion: protocol.ReplayDayManifestVersion, ManifestID: "replay-layout-r1", DatasetID: part.DatasetID,
		CampaignID: part.CampaignID, DayDefinitionID: part.DayDefinitionID, Date: part.Date, Revision: 1,
		RawDayManifestKey: part.RawDayManifestKey, RawDayManifestSHA256: part.RawDayManifestSHA256,
		ReplayContractID: part.ReplayContractID, FormatID: protocol.ReplayFormatID, ConversionID: part.ConversionID,
		ConverterBuildID: part.ConverterBuildID, DependencyLockHash: part.DependencyLockHash,
		WriterConfigurationHash: part.WriterConfigurationHash, TargetPlatformContract: part.TargetPlatformContract,
		CompletenessStatus: "settled_snapshot",
	}
	fullReplay, err := layout.ReplayDayManifestKey(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.VerifyReplayDayManifestKey(manifest, fullReplay); err != nil {
		t.Fatal(err)
	}
}

func TestLayoutDerivesTrustedRcloneDerivativeKeys(t *testing.T) {
	layout, err := NewLayout("v1", "r2:bucket/v1", layoutTestScope())
	if err != nil {
		t.Fatal(err)
	}
	part := layoutTestPart()
	partRclone, err := layout.RcloneReplayPartObjectKey(part)
	if err != nil {
		t.Fatal(err)
	}
	if partRclone != "r2:bucket/v1/"+CampaignPrefix(layout.Scope)+part.PartKey {
		t.Fatalf("rclone Parquet key = %q", partRclone)
	}
	manifestRclone, err := layout.RcloneReplayPartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	partManifestKey, err := protocol.PartManifestKey(part)
	if err != nil {
		t.Fatal(err)
	}
	if manifestRclone != "r2:bucket/v1/"+CampaignPrefix(layout.Scope)+partManifestKey {
		t.Fatalf("rclone part manifest key = %q", manifestRclone)
	}
}

func layoutTestPart() protocol.PartManifest {
	scope := protocol.ReplayScope{
		DatasetID: layoutTestScope().DatasetID, CampaignID: layoutTestScope().CampaignID, DayDefinitionID: layoutTestScope().DayDefinitionID,
		Date: "2024-03-09", ReplayContractID: "replay-v1", ConversionID: "conversion-v1",
		RawDayManifestKey: "snapshots/raw/day-definition=utc-day-v1/date=2024-03-09/raw-day-1.json", RawDayManifestSHA256: [32]byte{0x55},
	}
	hash := [32]byte{0x11}
	key, err := protocol.ReplayPartObjectKey(scope, 0, 0, hash)
	if err != nil {
		panic(err)
	}
	return protocol.PartManifest{
		ManifestVersion: protocol.PartManifestVersion, DatasetID: scope.DatasetID, CampaignID: scope.CampaignID,
		DayDefinitionID: scope.DayDefinitionID, Date: scope.Date, ReplayContractID: scope.ReplayContractID,
		FormatID: protocol.ReplayFormatID, ConversionID: scope.ConversionID, ConverterBuildID: "converter-1",
		DependencyLockHash: [32]byte{0x44}, WriterConfigurationHash: [32]byte{0x55}, TargetPlatformContract: "parquet-v1",
		RawDayManifestKey: scope.RawDayManifestKey, RawDayManifestSHA256: scope.RawDayManifestSHA256,
		PartSequence: 0, PartKey: key, PartSHA256: hash, PartBytes: 1, RowCount: 1, CanonicalRowBytes: 1,
		FirstStreamSequence: 0, LastStreamSequence: 0, FirstRowChainHash: [32]byte{0x22}, LastRowChainHash: [32]byte{0x33},
	}
}

func layoutTestScope() archive.ScopeConfig {
	return archive.ScopeConfig{
		DatasetID:               "dataset-demo",
		CampaignID:              "campaign-demo",
		ProviderID:              "provider-demo",
		StableFeedID:            "feed-demo",
		ExactSourceSymbol:       "EURUSD.raw",
		BrokerServerFingerprint: "server-demo",
		GatewayBuildIdentity:    "gateway-demo",
		ProducerBuildIdentity:   "producer-demo",
		DayDefinitionID:         "utc-day-v1",
		SettlePolicy:            "manual-v1",
		PublisherID:             "publisher-demo",
		PublisherEpoch:          1,
	}
}
