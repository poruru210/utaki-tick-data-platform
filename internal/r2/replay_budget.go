package r2

import (
	"fmt"
	"sync"

	"tick-data-platform/internal/protocol"
)

// ReplayObservationBudget is shared by every logical remote operation in one
// publication. Requests are charged before I/O. Bytes are charged only after
// they are actually consumed, including failed and oversized reads.
type ReplayObservationBudget struct {
	mu sync.Mutex

	limits protocol.ReplayPublicationLimits

	requests         uint64
	metadataBytes    uint64
	parquetBytes     uint64
	observationBytes uint64
}

type ReplayObservationBudgetSnapshot struct {
	Requests         uint64
	MetadataBytes    uint64
	ParquetBytes     uint64
	ObservationBytes uint64
}

type replayBudgetByteKind uint8

const (
	replayBudgetMetadata replayBudgetByteKind = iota
	replayBudgetParquet
	replayBudgetObservation
)

func NewReplayObservationBudget(limits protocol.ReplayPublicationLimits) (*ReplayObservationBudget, error) {
	if limits.MaxObservationRequests == 0 || limits.MaxObservationBytes == 0 || limits.MaxTotalMetadataBytes == 0 || limits.MaxTotalParquetBytes == 0 {
		return nil, fmt.Errorf("replay observation limits are incomplete")
	}
	return &ReplayObservationBudget{limits: limits}, nil
}

func (b *ReplayObservationBudget) ChargeRequest() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	next, err := checkedReplayBudgetAdd(b.requests, 1, b.limits.MaxObservationRequests)
	if err != nil {
		return fmt.Errorf("%w: observation request budget", ErrResourceLimit)
	}
	b.requests = next
	return nil
}

// ReadCap returns the largest bounded prefix that may still be consumed for
// one object. The caller reads at most cap+1 bytes so a body crossing any
// per-object, category, or aggregate boundary is observable as Oversized.
func (b *ReplayObservationBudget) ReadCap(kind replayBudgetByteKind, objectLimit uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if objectLimit == 0 {
		return 0, fmt.Errorf("%w: zero object read limit", ErrResourceLimit)
	}
	observationRemaining := b.limits.MaxObservationBytes - b.observationBytes
	categoryRemaining := observationRemaining
	switch kind {
	case replayBudgetMetadata:
		categoryRemaining = b.limits.MaxTotalMetadataBytes - b.metadataBytes
	case replayBudgetParquet:
		categoryRemaining = b.limits.MaxTotalParquetBytes - b.parquetBytes
	case replayBudgetObservation:
	default:
		return 0, fmt.Errorf("%w: unknown observation byte category", ErrResourceLimit)
	}
	capBytes := minReplayBudgetValue(objectLimit, categoryRemaining, observationRemaining)
	if capBytes == 0 {
		return 0, fmt.Errorf("%w: observation byte budget exhausted", ErrResourceLimit)
	}
	return capBytes, nil
}

func (b *ReplayObservationBudget) ChargeMetadata(bytes uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	metadata, err := checkedReplayBudgetAdd(b.metadataBytes, bytes, b.limits.MaxTotalMetadataBytes)
	if err != nil {
		b.metadataBytes = saturatingReplayBudgetAdd(b.metadataBytes, bytes, b.limits.MaxTotalMetadataBytes)
		b.observationBytes = saturatingReplayBudgetAdd(b.observationBytes, bytes, b.limits.MaxObservationBytes)
		return fmt.Errorf("%w: metadata byte budget", ErrResourceLimit)
	}
	observation, err := checkedReplayBudgetAdd(b.observationBytes, bytes, b.limits.MaxObservationBytes)
	if err != nil {
		b.metadataBytes = metadata
		b.observationBytes = b.limits.MaxObservationBytes
		return fmt.Errorf("%w: observation byte budget", ErrResourceLimit)
	}
	b.metadataBytes = metadata
	b.observationBytes = observation
	return nil
}

func (b *ReplayObservationBudget) ChargeParquet(bytes uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	parquet, err := checkedReplayBudgetAdd(b.parquetBytes, bytes, b.limits.MaxTotalParquetBytes)
	if err != nil {
		b.parquetBytes = saturatingReplayBudgetAdd(b.parquetBytes, bytes, b.limits.MaxTotalParquetBytes)
		b.observationBytes = saturatingReplayBudgetAdd(b.observationBytes, bytes, b.limits.MaxObservationBytes)
		return fmt.Errorf("%w: parquet byte budget", ErrResourceLimit)
	}
	observation, err := checkedReplayBudgetAdd(b.observationBytes, bytes, b.limits.MaxObservationBytes)
	if err != nil {
		b.parquetBytes = parquet
		b.observationBytes = b.limits.MaxObservationBytes
		return fmt.Errorf("%w: observation byte budget", ErrResourceLimit)
	}
	b.parquetBytes = parquet
	b.observationBytes = observation
	return nil
}

// ChargeObservation accounts bytes such as list descriptors that are neither
// metadata object bodies nor Parquet bodies.
func (b *ReplayObservationBudget) ChargeObservation(bytes uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	next, err := checkedReplayBudgetAdd(b.observationBytes, bytes, b.limits.MaxObservationBytes)
	if err != nil {
		b.observationBytes = b.limits.MaxObservationBytes
		return fmt.Errorf("%w: observation byte budget", ErrResourceLimit)
	}
	b.observationBytes = next
	return nil
}

func (b *ReplayObservationBudget) Snapshot() ReplayObservationBudgetSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return ReplayObservationBudgetSnapshot{Requests: b.requests, MetadataBytes: b.metadataBytes, ParquetBytes: b.parquetBytes, ObservationBytes: b.observationBytes}
}

func checkedReplayBudgetAdd(current, increment, limit uint64) (uint64, error) {
	if increment > limit || current > limit-increment {
		return 0, ErrResourceLimit
	}
	return current + increment, nil
}

func minReplayBudgetValue(values ...uint64) uint64 {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func saturatingReplayBudgetAdd(current, increment, limit uint64) uint64 {
	if increment > limit || current > limit-increment {
		return limit
	}
	return current + increment
}
