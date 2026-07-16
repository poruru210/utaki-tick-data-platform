package operations

import (
	"bytes"
	"fmt"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

func testScopeProcessConfig(index int) ScopeProcessConfig {
	identity := fmt.Sprintf("%d", index)
	return ScopeProcessConfig{
		Scope: archive.ScopeConfig{
			DatasetID:               "dataset-" + identity,
			CampaignID:              "campaign-" + identity,
			ProviderID:              "provider-" + identity,
			StableFeedID:            "feed-" + identity,
			ExactSourceSymbol:       "EURUSD.raw",
			BrokerServerFingerprint: "broker-" + identity,
			GatewayBuildIdentity:    "gateway-build-1",
			ProducerBuildIdentity:   "producer-build-1",
			DayDefinitionID:         "utc-day-v1",
			SettlePolicy:            "manual-v1",
			PublisherID:             "publisher-" + identity,
			PublisherEpoch:          1,
			ProtocolVersion:         protocol.ProtocolVersion,
			ProtocolLimits: archive.ProtocolLimits{
				MaxFrameBytes:  protocol.MaxFrameBytes,
				MaxRecords:     4,
				MaxStringBytes: protocol.MaxStringBytes,
			},
		},
		GatewayInstanceID: "gateway-instance-" + identity,
		ListenAddress:     "127.0.0.1:" + fmt.Sprintf("%d", 18100+index),
		GatewayConfigPath: "/var/lib/tick/scope-" + identity + "/gateway.toml",
		MQLConfigPath:     "/var/lib/tick/scope-" + identity + "/mql.toml",
		WALRoot:           "/var/lib/tick/scope-" + identity + "/wal",
		JournalPath:       "/var/lib/tick/scope-" + identity + "/journal.sqlite",
		OutboxRoot:        "/var/lib/tick/scope-" + identity + "/outbox",
		ReceiptRoot:       "/var/lib/tick/scope-" + identity + "/receipts",
		LockRoot:          "/var/lib/tick/scope-" + identity + "/locks",
		CredentialPrefix:  "tick/scope/" + identity,
	}
}

func TestValidateScopeInventoryRejectsSharedIdentityAndResources(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ScopeProcessConfig, ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig)
	}{
		{
			name: "scope key",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.Scope.DatasetID = left.Scope.DatasetID
				right.Scope.CampaignID = left.Scope.CampaignID
				return left, right
			},
		},
		{
			name: "gateway instance",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.GatewayInstanceID = left.GatewayInstanceID
				return left, right
			},
		},
		{
			name: "publisher identity",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.Scope.PublisherID = left.Scope.PublisherID
				return left, right
			},
		},
		{
			name: "credential prefix",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.CredentialPrefix = left.CredentialPrefix
				return left, right
			},
		},
		{
			name: "same listener",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.ListenAddress = left.ListenAddress
				return left, right
			},
		},
		{
			name: "wildcard listener",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.ListenAddress = "0.0.0.0:18101"
				return left, right
			},
		},
		{
			name: "localhost listener alias",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.ListenAddress = "localhost:18101"
				return left, right
			},
		},
		{
			name: "nested writable root",
			mutate: func(left, right ScopeProcessConfig) (ScopeProcessConfig, ScopeProcessConfig) {
				right.WALRoot = left.WALRoot + "/nested"
				return left, right
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, right := test.mutate(testScopeProcessConfig(1), testScopeProcessConfig(2))
			if err := ValidateScopeInventory([]ScopeProcessConfig{left, right}); err == nil {
				t.Fatalf("expected %s collision", test.name)
			}
		})
	}
}

func TestListenOverlapNormalizesWildcardAndLoopbackAliases(t *testing.T) {
	overlap, err := normalizedListenOverlap("localhost:18101", "127.0.0.1:18101")
	if err != nil || !overlap {
		t.Fatalf("localhost and IPv4 loopback were not treated as one listener: overlap=%v err=%v", overlap, err)
	}
	overlap, err = normalizedListenOverlap("0.0.0.0:18101", "127.0.0.1:18101")
	if err != nil || !overlap {
		t.Fatalf("wildcard and loopback were not treated as overlapping: overlap=%v err=%v", overlap, err)
	}
}

func TestBuildSupervisorPlanIsStableAndSorted(t *testing.T) {
	left, right := testScopeProcessConfig(1), testScopeProcessConfig(2)
	forward, err := BuildSupervisorPlan([]ScopeProcessConfig{left, right})
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := BuildSupervisorPlan([]ScopeProcessConfig{right, left})
	if err != nil {
		t.Fatal(err)
	}
	forwardJSON, err := forward.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	reverseJSON, err := reverse.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forwardJSON, reverseJSON) {
		t.Fatalf("supervisor plan is input-order dependent:\n%s\n%s", forwardJSON, reverseJSON)
	}
	if len(forward.Units) != 2 || forward.Units[0].ScopeKey >= forward.Units[1].ScopeKey {
		t.Fatalf("supervisor units are not sorted: %+v", forward.Units)
	}
	if forward.Units[0].GatewayServiceName == forward.Units[1].GatewayServiceName || forward.Units[0].MQLServiceName == forward.Units[1].MQLServiceName {
		t.Fatalf("service names are not scope-specific: %+v", forward.Units)
	}
}

func TestScopeFailureStatusIsIsolated(t *testing.T) {
	left, right := testScopeProcessConfig(1), testScopeProcessConfig(2)
	plan, err := BuildSupervisorPlan([]ScopeProcessConfig{left, right})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Units[0].GatewayServiceName == plan.Units[1].GatewayServiceName || plan.Units[0].MQLServiceName == plan.Units[1].MQLServiceName || plan.Units[0].ListenAddress == plan.Units[1].ListenAddress {
		t.Fatalf("scope process units are not independent: %+v", plan.Units)
	}
	if left.WALRoot == right.WALRoot || left.JournalPath == right.JournalPath || left.LockRoot == right.LockRoot || left.OutboxRoot == right.OutboxRoot || left.ReceiptRoot == right.ReceiptRoot {
		t.Fatal("scope resource roots are shared")
	}
	leftKey, err := left.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	healthyRight := ScopeHealthStatus{
		ScopeKey: rightKey, PublisherEpoch: 1, TerminalSynchronization: "synced",
		CurrentSourceTimeMSC: 100, LastDurableSourceTimeMSC: 100,
	}
	aggregate, err := AggregateScopeHealth([]ScopeHealthStatus{
		{ScopeKey: leftKey, PublisherEpoch: 1, BlockedReason: "r2-outage", TerminalSynchronization: "blocked"},
		healthyRight,
	})
	if err != nil {
		t.Fatal(err)
	}
	var gotRight ScopeHealthStatus
	for _, status := range aggregate.Scopes {
		if status.ScopeKey == rightKey {
			gotRight = status
		}
	}
	if gotRight != healthyRight {
		t.Fatalf("failure in one scope changed another scope status: got=%+v want=%+v", gotRight, healthyRight)
	}
}

func TestAggregateScopeHealthIsStableAndRejectsDuplicates(t *testing.T) {
	left, right := testScopeProcessConfig(1), testScopeProcessConfig(2)
	leftKey, err := left.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	first, err := AggregateScopeHealth([]ScopeHealthStatus{
		{ScopeKey: rightKey, PublisherEpoch: 1, BlockedReason: "disk-high"},
		{ScopeKey: leftKey, PublisherEpoch: 1, TerminalSynchronization: "synced"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := AggregateScopeHealth([]ScopeHealthStatus{
		{ScopeKey: leftKey, PublisherEpoch: 1, TerminalSynchronization: "synced"},
		{ScopeKey: rightKey, PublisherEpoch: 1, BlockedReason: "disk-high"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.StatusVersion != ScopeOperationsVersion || len(first.Scopes) != 2 || first.Scopes[0].ScopeKey >= first.Scopes[1].ScopeKey {
		t.Fatalf("unexpected aggregate status: %+v", first)
	}
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("aggregate status is input-order dependent: %+v / %+v", first, second)
	}
	firstJSON, err := first.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := second.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("aggregate status JSON is input-order dependent: %s / %s", firstJSON, secondJSON)
	}
	if _, err := AggregateScopeHealth([]ScopeHealthStatus{{ScopeKey: leftKey, PublisherEpoch: 1}, {ScopeKey: leftKey, PublisherEpoch: 1}}); err == nil {
		t.Fatal("expected duplicate scope health identity to fail")
	}
	if _, err := AggregateScopeHealth([]ScopeHealthStatus{{ScopeKey: "", PublisherEpoch: 1}}); err == nil {
		t.Fatal("expected incomplete scope health identity to fail")
	}
}

func TestScopeInventoryCanonicalRoundTrip(t *testing.T) {
	inventory := ScopeInventory{
		InventoryVersion: ScopeInventoryVersion,
		Scopes:           []ScopeProcessConfig{testScopeProcessConfig(2), testScopeProcessConfig(1)},
	}
	canonical, err := inventory.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeScopeInventory(canonical)
	if err != nil {
		t.Fatal(err)
	}
	decodedCanonical, err := decoded.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, decodedCanonical) || len(decoded.Scopes) != 2 {
		t.Fatalf("scope inventory round trip changed bytes or entries: %s / %s", canonical, decodedCanonical)
	}
	value, err := protocol.DecodeCanonicalJSON(canonical)
	if err != nil {
		t.Fatal(err)
	}
	object := value.(map[string]any)
	object["unexpected"] = "field"
	withUnknown, err := protocol.CanonicalJSON(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeScopeInventory(withUnknown); err == nil {
		t.Fatal("expected unknown inventory field to fail")
	}
	delete(object, "unexpected")
	object["inventory_version"] = "wrong-version"
	wrongVersion, err := protocol.CanonicalJSON(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeScopeInventory(wrongVersion); err == nil {
		t.Fatal("expected wrong inventory version to fail")
	}
}
