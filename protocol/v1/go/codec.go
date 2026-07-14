// Package protocolv1 exposes the Protocol V1 Go codec to producer packages.
package protocolv1

import internal "tick-data-platform/internal/protocol"

type (
	Frame         = internal.Frame
	Message       = internal.Message
	MessageType   = internal.MessageType
	ErrorCode     = internal.ErrorCode
	ProtocolError = internal.ProtocolError
	HelloV1       = internal.HelloV1
	ResumeV1      = internal.ResumeV1
	RawMqlTickV1  = internal.RawMqlTickV1
	BatchFrameV1  = internal.BatchFrameV1
	AckV1         = internal.AckV1
	ErrorV1       = internal.ErrorV1
)

const (
	MessageHello            = internal.MessageHello
	MessageResume           = internal.MessageResume
	MessageBatch            = internal.MessageBatch
	MessageAck              = internal.MessageAck
	MessageError            = internal.MessageError
	AckAcceptedAdvanced     = internal.AckAcceptedAdvanced
	AckAcceptedNoAdvance    = internal.AckAcceptedNoAdvance
	AckDuplicate            = internal.AckDuplicate
	AckDenseBoundary        = internal.AckDenseBoundary
	AckDenseUnresolved      = internal.AckDenseUnresolved
	AckRetryableError       = internal.AckRetryableError
	AckFatalProtocolError   = internal.AckFatalProtocolError
	AckSourceStateConflict  = internal.AckSourceStateConflict
	AckSessionLeaseConflict = internal.AckSessionLeaseConflict
	MaxFrameBytes           = internal.MaxFrameBytes
	MaxRecords              = internal.MaxRecords
	SourceSchemaMT5         = internal.SourceSchemaMT5
	ErrInvalidFrame         = internal.ErrInvalidFrame
	ErrUnsupportedVersion   = internal.ErrUnsupportedVersion
	ErrUnknownMessage       = internal.ErrUnknownMessage
	ErrTruncatedFrame       = internal.ErrTruncatedFrame
	ErrOversizedFrame       = internal.ErrOversizedFrame
	ErrCRCMismatch          = internal.ErrCRCMismatch
	ErrInvalidField         = internal.ErrInvalidField
	ErrSourceStateConflict  = internal.ErrSourceStateConflict
	ErrSessionLeaseConflict = internal.ErrSessionLeaseConflict
	ErrInternalRetryable    = internal.ErrInternalRetryable
)

var (
	EncodeFrame              = internal.EncodeFrame
	DecodeFrame              = internal.DecodeFrame
	EncodeMessage            = internal.EncodeMessage
	DecodeMessage            = internal.DecodeMessage
	ErrorCodeOf              = internal.ErrorCodeOf
	ErrorCodeNumber          = internal.ErrorCodeNumber
	SourcePayloadFingerprint = internal.SourcePayloadFingerprint
	ObservationHash          = internal.ObservationHash
	GatewayBatchSHA256       = internal.GatewayBatchSHA256
	WALEntryHash             = internal.WALEntryHash
	BoundaryDigest           = internal.BoundaryDigest
)
