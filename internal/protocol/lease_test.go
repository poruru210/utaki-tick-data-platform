package protocol

import "testing"

func TestDeriveSessionLeaseIDPreservesM2Fixture(t *testing.T) {
	got := DeriveSessionLeaseID(
		"fake-01", "session-test-01", "campaign-test-01", "provider-test", "feed-test", "broker-test", "EURUSD",
	)
	const want = "lease-1c8ebd0d187aa00f89d834003bb87c9c"
	if got != want {
		t.Fatalf("lease = %q, want %q", got, want)
	}
}

func TestDeriveSessionLeaseIDChangesWithEveryIdentityField(t *testing.T) {
	base := []string{"producer", "session", "campaign", "provider", "feed", "broker", "symbol"}
	want := DeriveSessionLeaseID(base[0], base[1], base[2], base[3], base[4], base[5], base[6])
	for index := range base {
		mutated := append([]string(nil), base...)
		mutated[index] += "-changed"
		got := DeriveSessionLeaseID(mutated[0], mutated[1], mutated[2], mutated[3], mutated[4], mutated[5], mutated[6])
		if got == want {
			t.Fatalf("identity field %d did not change lease", index)
		}
	}
}
