package protocol

import "testing"

func TestDeriveSessionLeaseIDPreservesM2Fixture(t *testing.T) {
	got := DeriveSessionLeaseID(
		"fake-01", "session-test-01", "provider-test", "feed-test", "broker-test", "EURUSD",
	)
	const want = "lease-185842e02b16a77514b5decebbcc9623"
	if got != want {
		t.Fatalf("lease = %q, want %q", got, want)
	}
}

func TestDeriveSessionLeaseIDChangesWithEveryIdentityField(t *testing.T) {
	base := []string{"producer", "session", "provider", "feed", "broker", "symbol"}
	want := DeriveSessionLeaseID(base[0], base[1], base[2], base[3], base[4], base[5])
	for index := range base {
		mutated := append([]string(nil), base...)
		mutated[index] += "-changed"
		got := DeriveSessionLeaseID(mutated[0], mutated[1], mutated[2], mutated[3], mutated[4], mutated[5])
		if got == want {
			t.Fatalf("identity field %d did not change lease", index)
		}
	}
}
