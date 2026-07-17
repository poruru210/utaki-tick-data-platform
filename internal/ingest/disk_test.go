package ingest

import (
	"errors"
	"testing"

	"tick-data-platform/internal/retention"
)

type fakeDiskUsage struct {
	usage DiskUsage
	err   error
}

func (f *fakeDiskUsage) Usage(string) (DiskUsage, error) {
	return f.usage, f.err
}

func TestDiskStateMachineWatermarksAndPoisonRecovery(t *testing.T) {
	provider := &fakeDiskUsage{usage: DiskUsage{FreeBytes: 300, TotalBytes: 1000}}
	machine, err := NewDiskStateMachine("/trusted", DiskWatermarks{HighFreeBytes: 500, CriticalFreeBytes: 250, EmergencyFreeBytes: 100}, provider)
	if err != nil {
		t.Fatal(err)
	}
	if state := machine.Refresh(); state.Class != retention.DiskHigh || !state.ACKAllowed || state.BlockedReason != "disk_high_watermark" {
		t.Fatalf("high state = %+v", state)
	}
	provider.usage.FreeBytes = 200
	if state := machine.Refresh(); state.Class != retention.DiskCritical || state.ACKAllowed || state.Ready != false {
		t.Fatalf("critical state = %+v", state)
	}
	provider.usage.FreeBytes = 800
	machine.MarkPoisoned()
	if state := machine.State(); state.Class != retention.DiskNormal || state.ACKAllowed || state.BlockedReason != "wal_poisoned_reopen_required" {
		t.Fatalf("poisoned state = %+v", state)
	}
	if state := machine.ClearPoisonedAfterVerification(); state.Class != retention.DiskNormal || !state.ACKAllowed {
		t.Fatalf("recovered state = %+v", state)
	}
}

func TestDiskStateMachineFailsClosedOnUsageError(t *testing.T) {
	provider := &fakeDiskUsage{err: errors.New("stat failed")}
	machine, err := NewDiskStateMachine("/trusted", DefaultDiskWatermarks(), provider)
	if err != nil {
		t.Fatal(err)
	}
	if state := machine.Refresh(); state.Class != retention.DiskEmergency || state.ACKAllowed || state.Ready || state.BlockedReason != "disk_usage_unavailable" {
		t.Fatalf("unavailable state = %+v", state)
	}
}

func TestDiskStateMachineAppliesPendingPublicationPressure(t *testing.T) {
	provider := &fakeDiskUsage{usage: DiskUsage{FreeBytes: 900, TotalBytes: 1000}}
	machine, err := NewDiskStateMachine("/trusted", DiskWatermarks{
		HighFreeBytes: 500, CriticalFreeBytes: 250, EmergencyFreeBytes: 100,
		MaxPendingSegments: 2, MaxPendingBytes: 100,
	}, provider)
	if err != nil {
		t.Fatal(err)
	}
	firstWakeup := machine.PriorityWakeups()
	secondWakeup := machine.PriorityWakeups()
	machine.SetPendingPublication(1, 99)
	if state := machine.State(); !state.ACKAllowed || state.WorkerPriority {
		t.Fatalf("below backlog limit state = %+v", state)
	}
	machine.SetPendingPublication(2, 99)
	for name, wakeup := range map[string]<-chan struct{}{"first": firstWakeup, "second": secondWakeup} {
		select {
		case <-wakeup:
		default:
			t.Fatalf("%s priority subscriber was not notified", name)
		}
	}
	if state := machine.State(); state.ACKAllowed || state.Ready || state.BlockedReason != "publication_backlog_limit" || !state.WorkerPriority {
		t.Fatalf("segment backlog limit state = %+v", state)
	}
	machine.SetPendingPublication(0, 0)
	if state := machine.State(); !state.ACKAllowed || state.BlockedReason != "" {
		t.Fatalf("cleared backlog state = %+v", state)
	}
}
