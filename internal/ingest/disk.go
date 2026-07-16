package ingest

import (
	"fmt"
	"math"
	"sync"

	"tick-data-platform/internal/retention"
)

type DiskUsage struct {
	FreeBytes  uint64
	TotalBytes uint64
}

type DiskUsageProvider interface {
	Usage(root string) (DiskUsage, error)
}

type OSDiskUsageProvider struct{}

type DiskWatermarks struct {
	HighFreeBytes      uint64
	CriticalFreeBytes  uint64
	EmergencyFreeBytes uint64
}

func DefaultDiskWatermarks() DiskWatermarks {
	return DiskWatermarks{
		HighFreeBytes:      512 << 20,
		CriticalFreeBytes:  256 << 20,
		EmergencyFreeBytes: 64 << 20,
	}
}

func (w DiskWatermarks) Validate() error {
	if w.HighFreeBytes == 0 || w.CriticalFreeBytes == 0 || w.EmergencyFreeBytes == 0 {
		return fmt.Errorf("disk free-space watermarks must be nonzero")
	}
	if w.HighFreeBytes <= w.CriticalFreeBytes || w.CriticalFreeBytes <= w.EmergencyFreeBytes {
		return fmt.Errorf("disk free-space watermarks must descend high > critical > emergency")
	}
	return nil
}

type DiskState struct {
	Class         retention.DiskClass `json:"disk_class"`
	FreeBytes     uint64              `json:"free_bytes"`
	TotalBytes    uint64              `json:"total_bytes"`
	Ready         bool                `json:"ready"`
	ACKAllowed    bool                `json:"ack_allowed"`
	Poisoned      bool                `json:"poisoned"`
	BlockedReason string              `json:"blocked_reason"`
}

type DiskStateMachine struct {
	root     string
	policy   DiskWatermarks
	provider DiskUsageProvider

	mu       sync.Mutex
	state    DiskState
	poisoned bool
}

func NewDiskStateMachine(root string, policy DiskWatermarks, provider DiskUsageProvider) (*DiskStateMachine, error) {
	if root == "" || provider == nil {
		return nil, fmt.Errorf("disk state dependencies are incomplete")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &DiskStateMachine{root: root, policy: policy, provider: provider, state: DiskState{Class: retention.DiskEmergency, BlockedReason: "disk_usage_unobserved"}}, nil
}

func (s *DiskStateMachine) Refresh() DiskState {
	usage, err := s.provider.Usage(s.root)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil || usage.TotalBytes == 0 || usage.FreeBytes > usage.TotalBytes {
		s.state = DiskState{Class: retention.DiskEmergency, Ready: false, ACKAllowed: false, Poisoned: s.poisoned, BlockedReason: "disk_usage_unavailable"}
		return s.state
	}
	class := classifyDisk(usage.FreeBytes, s.policy)
	ready := class == retention.DiskNormal || class == retention.DiskHigh
	reason := ""
	if class == retention.DiskHigh {
		reason = "disk_high_watermark"
	} else if class == retention.DiskCritical {
		reason = "disk_critical_watermark"
	} else if class == retention.DiskEmergency {
		reason = "disk_emergency_watermark"
	}
	if s.poisoned {
		ready = false
		reason = "wal_poisoned_reopen_required"
	}
	s.state = DiskState{Class: class, FreeBytes: usage.FreeBytes, TotalBytes: usage.TotalBytes, Ready: ready, ACKAllowed: ready, Poisoned: s.poisoned, BlockedReason: reason}
	return s.state
}

func classifyDisk(free uint64, policy DiskWatermarks) retention.DiskClass {
	switch {
	case free >= policy.HighFreeBytes:
		return retention.DiskNormal
	case free >= policy.CriticalFreeBytes:
		return retention.DiskHigh
	case free >= policy.EmergencyFreeBytes:
		return retention.DiskCritical
	default:
		return retention.DiskEmergency
	}
}

func (s *DiskStateMachine) State() DiskState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *DiskStateMachine) MarkPoisoned() DiskState {
	s.mu.Lock()
	s.poisoned = true
	s.mu.Unlock()
	return s.Refresh()
}

// ClearPoisonedAfterVerification is deliberately explicit: free space alone
// never reopens an append path after an OS append/sync failure.
func (s *DiskStateMachine) ClearPoisonedAfterVerification() DiskState {
	s.mu.Lock()
	s.poisoned = false
	s.mu.Unlock()
	return s.Refresh()
}

func (s *DiskStateMachine) ReadyForACK() bool {
	return s.Refresh().ACKAllowed
}

func checkedMultiply(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, fmt.Errorf("disk byte count overflows")
	}
	return left * right, nil
}
