package r2

import (
	"errors"
	"math"
	"testing"

	"tick-data-platform/internal/protocol"
)

func TestReplayBudgetChargesAggregateDimensions(t *testing.T) {
	limits := protocol.ReplayPublicationImplementationBounds
	limits.MaxObservationRequests = 2
	limits.MaxTotalMetadataBytes = 5
	limits.MaxTotalParquetBytes = 7
	limits.MaxObservationBytes = 12
	budget, err := NewReplayObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeRequest(); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeMetadata(5); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeParquet(7); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeRequest(); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeRequest(); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("third request = %v", err)
	}
	snapshot := budget.Snapshot()
	if snapshot.Requests != 2 || snapshot.MetadataBytes != 5 || snapshot.ParquetBytes != 7 || snapshot.ObservationBytes != 12 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestReplayBudgetCheckedAdditionRejectsOverflow(t *testing.T) {
	if _, err := checkedReplayBudgetAdd(math.MaxUint64-1, 2, math.MaxUint64); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("overflow = %v", err)
	}
}

func TestReplayBudgetReadCapUsesSmallestRemainingBoundary(t *testing.T) {
	limits := protocol.ReplayPublicationImplementationBounds
	limits.MaxTotalMetadataBytes = 9
	limits.MaxObservationBytes = 10
	budget, err := NewReplayObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeObservation(4); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeMetadata(3); err != nil {
		t.Fatal(err)
	}
	capBytes, err := budget.ReadCap(replayBudgetMetadata, 8)
	if err != nil {
		t.Fatal(err)
	}
	if capBytes != 3 {
		t.Fatalf("read cap = %d, want aggregate observation remainder 3", capBytes)
	}
}

func TestReplayBudgetFailedActualChargeSaturatesBoundary(t *testing.T) {
	limits := protocol.ReplayPublicationImplementationBounds
	limits.MaxObservationBytes = 10
	budget, err := NewReplayObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeObservation(6); err != nil {
		t.Fatal(err)
	}
	if err := budget.ChargeObservation(5); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("cross-boundary charge = %v", err)
	}
	if got := budget.Snapshot().ObservationBytes; got != 10 {
		t.Fatalf("failed actual charge left reusable bytes: %d", got)
	}
	if _, err := budget.ReadCap(replayBudgetObservation, 1); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("saturated budget allowed another read: %v", err)
	}
}
