package continuity

import "tick-data-platform/internal/protocol"

// VerifiedBatch is emitted by an M2 archive verifier. It contains WAL
// metadata and frame bytes that were read from a sealed object; callers do
// not construct replay entries directly.
type VerifiedBatch struct {
	ObjectKey              string
	ObjectSHA256           [32]byte
	GatewayIngestSequence  uint64
	PreviousEntryHash      [32]byte
	EntryHash              [32]byte
	ReceiveWallS           int64
	ReceiveMonotonicUS     uint64
	Frame                  []byte
	SelectedRecordOrdinals []uint32
}

// ProducerIdentity is the verified source identity used to recompute the M2
// session lease for every selected BatchFrameV1.
type ProducerIdentity struct {
	ProducerInstanceID      string
	ProviderID              string
	StableFeedID            string
	BrokerServerFingerprint string
	ExactSourceSymbol       string
}

// VerifiedBatchReader is the narrow handoff from M2 raw verification to the
// reducer. The production implementation is provided by internal/archive.
// Next returns entries in sealed-WAL order and marks only manifest.objects
// coordinates as selected replay data.
type VerifiedBatchReader interface {
	ReplayScope() protocol.ReplayScope
	ProducerIdentity() ProducerIdentity
	MaxRecords() uint32
	Next() (VerifiedBatch, bool, error)
}

// RowSink receives each canonical replay row in stream order. A test may use
// a collector; production callers should write to a bounded downstream sink.
type RowSink func(protocol.ReplayRow) error
