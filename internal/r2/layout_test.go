package r2

import (
	"fmt"
	"strings"
	"testing"

	"tick-data-platform/internal/archive"
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
