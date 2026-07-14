// Package protocol implements Protocol V1 wire and message codecs.
package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"unicode/utf8"
)

const (
	Magic           = "TICK"
	ProtocolVersion = uint16(1)
	HeaderLength    = uint32(16)
	MinFrameBytes   = uint32(20)
	MaxFrameBytes   = uint32(1_048_576)
	MaxRecords      = uint32(4096)
	MaxStringBytes  = uint16(255)
	SourceSchemaMT5 = "mt5.mqltick.v1"
)

type MessageType uint16

const (
	MessageHello  MessageType = 1
	MessageResume MessageType = 2
	MessageBatch  MessageType = 3
	MessageAck    MessageType = 4
	MessageError  MessageType = 5
)

type ErrorCode string

const (
	ErrInvalidFrame         ErrorCode = "INVALID_FRAME"
	ErrUnsupportedVersion   ErrorCode = "UNSUPPORTED_PROTOCOL_VERSION"
	ErrUnknownMessage       ErrorCode = "UNKNOWN_MESSAGE_TYPE"
	ErrTruncatedFrame       ErrorCode = "TRUNCATED_FRAME"
	ErrOversizedFrame       ErrorCode = "OVERSIZED_FRAME"
	ErrCRCMismatch          ErrorCode = "CRC_MISMATCH"
	ErrInvalidField         ErrorCode = "INVALID_FIELD"
	ErrSourceStateConflict  ErrorCode = "SOURCE_STATE_CONFLICT"
	ErrSessionLeaseConflict ErrorCode = "SESSION_LEASE_CONFLICT"
	ErrInternalRetryable    ErrorCode = "INTERNAL_RETRYABLE"
)

type ProtocolError struct {
	Code   ErrorCode
	Detail string
}

func (e *ProtocolError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func newError(code ErrorCode, format string, args ...any) error {
	return &ProtocolError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	if protocolErr, ok := err.(*ProtocolError); ok {
		return protocolErr.Code
	}
	return ErrInvalidField
}

type Frame struct {
	Version     uint16
	MessageType MessageType
	Payload     []byte
}

func EncodeFrame(messageType MessageType, payload []byte) ([]byte, error) {
	if messageType < MessageHello || messageType > MessageError {
		return nil, newError(ErrUnknownMessage, "message type %d", messageType)
	}
	if len(payload) > int(MaxFrameBytes-MinFrameBytes) {
		return nil, newError(ErrOversizedFrame, "payload bytes %d", len(payload))
	}
	frameLength := uint32(20 + len(payload))
	frame := make([]byte, frameLength)
	copy(frame[0:4], Magic)
	binary.LittleEndian.PutUint16(frame[4:6], ProtocolVersion)
	binary.LittleEndian.PutUint16(frame[6:8], uint16(messageType))
	binary.LittleEndian.PutUint32(frame[8:12], frameLength)
	binary.LittleEndian.PutUint32(frame[12:16], HeaderLength)
	copy(frame[16:], payload)
	checksum := crc32.Checksum(frame[:len(frame)-4], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(frame[len(frame)-4:], checksum)
	return frame, nil
}

func DecodeFrame(raw []byte) (Frame, error) {
	if len(raw) < 12 {
		return Frame{}, newError(ErrTruncatedFrame, "frame has %d bytes", len(raw))
	}
	frameLength := binary.LittleEndian.Uint32(raw[8:12])
	if frameLength > MaxFrameBytes {
		return Frame{}, newError(ErrOversizedFrame, "frame length %d", frameLength)
	}
	if frameLength < MinFrameBytes {
		return Frame{}, newError(ErrInvalidFrame, "frame length %d", frameLength)
	}
	if len(raw) < int(frameLength) {
		return Frame{}, newError(ErrTruncatedFrame, "frame has %d bytes, needs %d", len(raw), frameLength)
	}
	if len(raw) != int(frameLength) {
		return Frame{}, newError(ErrInvalidFrame, "trailing bytes after frame")
	}
	if !bytes.Equal(raw[:4], []byte(Magic)) {
		return Frame{}, newError(ErrInvalidFrame, "invalid magic")
	}
	if binary.LittleEndian.Uint32(raw[12:16]) != HeaderLength {
		return Frame{}, newError(ErrInvalidFrame, "invalid header length")
	}
	version := binary.LittleEndian.Uint16(raw[4:6])
	if version != ProtocolVersion {
		return Frame{}, newError(ErrUnsupportedVersion, "protocol version %d", version)
	}
	messageType := MessageType(binary.LittleEndian.Uint16(raw[6:8]))
	if messageType < MessageHello || messageType > MessageError {
		return Frame{}, newError(ErrUnknownMessage, "message type %d", messageType)
	}
	want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	got := crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli))
	if want != got {
		return Frame{}, newError(ErrCRCMismatch, "want %08x, got %08x", want, got)
	}
	payload := append([]byte(nil), raw[16:len(raw)-4]...)
	return Frame{
		Version:     version,
		MessageType: messageType,
		Payload:     payload,
	}, nil
}

type HelloV1 struct {
	ProducerInstanceID      string
	ProducerSessionID       string
	ProducerBuildID         string
	MQLCompilerBuild        string
	TerminalBuild           string
	OSContract              string
	ClockAPIID              string
	CampaignID              string
	ProviderID              string
	StableFeedID            string
	BrokerServerFingerprint string
	ExactSourceSymbol       string
	SourceSchemaID          string
	AcquisitionMode         uint8
	InitialFromMSC          int64
	CapabilityFlags         uint32
}

type ResumeV1 struct {
	AcceptedProtocolVersion  uint16
	GatewayInstanceID        string
	SessionLeaseID           string
	CommittedCursorMSC       int64
	CommittedBoundaryDigest  [32]byte
	LastDurableBatchSequence uint64
	LastDurableBatchHash     [32]byte
	NextFromMSC              int64
	NextRequestedCount       uint32
	MaximumFrameBytes        uint32
	MaximumRecords           uint32
	HeartbeatIdleTimeoutMS   uint32
}

type RawMqlTickV1 struct {
	Time            int64
	BidBits         uint64
	AskBits         uint64
	LastBits        uint64
	Volume          uint64
	TimeMSC         int64
	Flags           uint32
	VolumeRealBits  uint64
	CaptureSequence uint64
}

type BatchFrameV1 struct {
	SessionLeaseID        string
	ProducerSessionID     string
	BatchSequence         uint64
	RequestedFromMSC      int64
	RequestedCount        uint32
	FetchWallStartS       int64
	FetchWallEndS         int64
	FetchMonotonicStartUS uint64
	FetchMonotonicEndUS   uint64
	ReturnedCount         int32
	CopyTicksError        int32
	SourceStatusFlags     uint32
	SourceSchemaID        string
	Records               []RawMqlTickV1
}

type AckV1 struct {
	ProducerSessionID       string
	BatchSequence           uint64
	GatewayBatchSHA256      [32]byte
	GatewayIngestSequence   uint64
	Status                  uint8
	CommittedCursorMSC      int64
	CommittedBoundaryDigest [32]byte
	NextFromMSC             int64
	NextRequestedCount      uint32
	RetryAfterMS            uint32
}

type ErrorV1 struct {
	Code                 uint16
	Retryable            uint8
	RelatedMessageType   MessageType
	RelatedBatchSequence uint64
	Message              string
}

type Message interface {
	messageType() MessageType
}

func (HelloV1) messageType() MessageType      { return MessageHello }
func (ResumeV1) messageType() MessageType     { return MessageResume }
func (BatchFrameV1) messageType() MessageType { return MessageBatch }
func (AckV1) messageType() MessageType        { return MessageAck }
func (ErrorV1) messageType() MessageType      { return MessageError }

type writer struct {
	bytes.Buffer
}

func (w *writer) u8(value uint8) { _ = w.WriteByte(value) }
func (w *writer) u16(value uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], value)
	w.Write(b[:])
}
func (w *writer) u32(value uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], value)
	w.Write(b[:])
}
func (w *writer) u64(value uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], value)
	w.Write(b[:])
}
func (w *writer) i32(value int32)     { w.u32(uint32(value)) }
func (w *writer) i64(value int64)     { w.u64(uint64(value)) }
func (w *writer) hash(value [32]byte) { w.Write(value[:]) }
func (w *writer) string(value string) error {
	if !utf8.ValidString(value) {
		return newError(ErrInvalidField, "string is not UTF-8")
	}
	if len(value) > int(MaxStringBytes) {
		return newError(ErrInvalidField, "string has %d bytes", len(value))
	}
	w.u16(uint16(len(value)))
	w.WriteString(value)
	return nil
}

type reader struct {
	data []byte
	off  int
}

func (r *reader) take(size int) ([]byte, error) {
	if size < 0 || r.off+size > len(r.data) {
		return nil, newError(ErrInvalidField, "field exceeds payload")
	}
	value := r.data[r.off : r.off+size]
	r.off += size
	return value, nil
}
func (r *reader) u8() (uint8, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}
func (r *reader) u16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}
func (r *reader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (r *reader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}
func (r *reader) i32() (int32, error) { value, err := r.u32(); return int32(value), err }
func (r *reader) i64() (int64, error) { value, err := r.u64(); return int64(value), err }
func (r *reader) hash() ([32]byte, error) {
	var result [32]byte
	b, err := r.take(32)
	if err != nil {
		return result, err
	}
	copy(result[:], b)
	return result, nil
}
func (r *reader) string() (string, error) {
	size, err := r.u16()
	if err != nil {
		return "", err
	}
	if size > MaxStringBytes {
		return "", newError(ErrInvalidField, "string has %d bytes", size)
	}
	b, err := r.take(int(size))
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", newError(ErrInvalidField, "string is not UTF-8")
	}
	return string(b), nil
}
func (r *reader) done() error {
	if r.off != len(r.data) {
		return newError(ErrInvalidField, "%d trailing payload bytes", len(r.data)-r.off)
	}
	return nil
}

func encodePayload(message Message) ([]byte, error) {
	w := &writer{}
	switch value := message.(type) {
	case HelloV1:
		fields := []string{
			value.ProducerInstanceID, value.ProducerSessionID, value.ProducerBuildID,
			value.MQLCompilerBuild, value.TerminalBuild, value.OSContract, value.ClockAPIID,
			value.CampaignID, value.ProviderID, value.StableFeedID,
			value.BrokerServerFingerprint, value.ExactSourceSymbol, value.SourceSchemaID,
		}
		for _, field := range fields {
			if err := w.string(field); err != nil {
				return nil, err
			}
		}
		if value.AcquisitionMode != 1 && value.AcquisitionMode != 2 {
			return nil, newError(ErrInvalidField, "acquisition mode %d", value.AcquisitionMode)
		}
		w.u8(value.AcquisitionMode)
		w.i64(value.InitialFromMSC)
		w.u32(value.CapabilityFlags)
	case ResumeV1:
		if value.AcceptedProtocolVersion != ProtocolVersion {
			return nil, newError(ErrInvalidField, "accepted protocol version %d", value.AcceptedProtocolVersion)
		}
		w.u16(value.AcceptedProtocolVersion)
		if err := w.string(value.GatewayInstanceID); err != nil {
			return nil, err
		}
		if err := w.string(value.SessionLeaseID); err != nil {
			return nil, err
		}
		w.i64(value.CommittedCursorMSC)
		w.hash(value.CommittedBoundaryDigest)
		w.u64(value.LastDurableBatchSequence)
		w.hash(value.LastDurableBatchHash)
		w.i64(value.NextFromMSC)
		w.u32(value.NextRequestedCount)
		w.u32(value.MaximumFrameBytes)
		w.u32(value.MaximumRecords)
		w.u32(value.HeartbeatIdleTimeoutMS)
	case BatchFrameV1:
		if len(value.Records) > int(MaxRecords) {
			return nil, newError(ErrInvalidField, "record count %d", len(value.Records))
		}
		if value.ReturnedCount >= 0 && value.ReturnedCount != int32(len(value.Records)) {
			return nil, newError(ErrInvalidField, "returned count does not match records")
		}
		if value.ReturnedCount < 0 && len(value.Records) != 0 {
			return nil, newError(ErrInvalidField, "negative returned count has records")
		}
		if err := w.string(value.SessionLeaseID); err != nil {
			return nil, err
		}
		if err := w.string(value.ProducerSessionID); err != nil {
			return nil, err
		}
		w.u64(value.BatchSequence)
		w.i64(value.RequestedFromMSC)
		w.u32(value.RequestedCount)
		w.i64(value.FetchWallStartS)
		w.i64(value.FetchWallEndS)
		w.u64(value.FetchMonotonicStartUS)
		w.u64(value.FetchMonotonicEndUS)
		w.i32(value.ReturnedCount)
		w.i32(value.CopyTicksError)
		w.u32(value.SourceStatusFlags)
		if err := w.string(value.SourceSchemaID); err != nil {
			return nil, err
		}
		w.u32(uint32(len(value.Records)))
		for _, record := range value.Records {
			w.i64(record.Time)
			w.u64(record.BidBits)
			w.u64(record.AskBits)
			w.u64(record.LastBits)
			w.u64(record.Volume)
			w.i64(record.TimeMSC)
			w.u32(record.Flags)
			w.u64(record.VolumeRealBits)
			w.u64(record.CaptureSequence)
		}
	case AckV1:
		if value.Status < 1 || value.Status > 9 {
			return nil, newError(ErrInvalidField, "ack status %d", value.Status)
		}
		if err := w.string(value.ProducerSessionID); err != nil {
			return nil, err
		}
		w.u64(value.BatchSequence)
		w.hash(value.GatewayBatchSHA256)
		w.u64(value.GatewayIngestSequence)
		w.u8(value.Status)
		w.i64(value.CommittedCursorMSC)
		w.hash(value.CommittedBoundaryDigest)
		w.i64(value.NextFromMSC)
		w.u32(value.NextRequestedCount)
		w.u32(value.RetryAfterMS)
	case ErrorV1:
		if value.Code < 1 || value.Code > 10 {
			return nil, newError(ErrInvalidField, "error code %d", value.Code)
		}
		if value.Retryable > 1 {
			return nil, newError(ErrInvalidField, "retryable %d", value.Retryable)
		}
		w.u16(value.Code)
		w.u8(value.Retryable)
		w.u16(uint16(value.RelatedMessageType))
		w.u64(value.RelatedBatchSequence)
		if err := w.string(value.Message); err != nil {
			return nil, err
		}
	default:
		return nil, newError(ErrInvalidField, "unsupported message %T", message)
	}
	return w.Bytes(), nil
}

func EncodeMessage(message Message) ([]byte, error) {
	payload, err := encodePayload(message)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(message.messageType(), payload)
}

func DecodeMessage(frame Frame) (Message, error) {
	r := &reader{data: frame.Payload}
	switch frame.MessageType {
	case MessageHello:
		value, err := decodeHello(r)
		if err != nil {
			return nil, err
		}
		return value, r.done()
	case MessageResume:
		value, err := decodeResume(r)
		if err != nil {
			return nil, err
		}
		return value, r.done()
	case MessageBatch:
		value, err := decodeBatch(r)
		if err != nil {
			return nil, err
		}
		return value, r.done()
	case MessageAck:
		value, err := decodeAck(r)
		if err != nil {
			return nil, err
		}
		return value, r.done()
	case MessageError:
		value, err := decodeError(r)
		if err != nil {
			return nil, err
		}
		return value, r.done()
	default:
		return nil, newError(ErrUnknownMessage, "message type %d", frame.MessageType)
	}
}

func decodeHello(r *reader) (HelloV1, error) {
	var value HelloV1
	fields := []*string{
		&value.ProducerInstanceID, &value.ProducerSessionID, &value.ProducerBuildID,
		&value.MQLCompilerBuild, &value.TerminalBuild, &value.OSContract, &value.ClockAPIID,
		&value.CampaignID, &value.ProviderID, &value.StableFeedID,
		&value.BrokerServerFingerprint, &value.ExactSourceSymbol, &value.SourceSchemaID,
	}
	for _, field := range fields {
		decoded, err := r.string()
		if err != nil {
			return value, err
		}
		*field = decoded
	}
	var err error
	value.AcquisitionMode, err = r.u8()
	if err != nil {
		return value, err
	}
	if value.AcquisitionMode != 1 && value.AcquisitionMode != 2 {
		return value, newError(ErrInvalidField, "acquisition mode %d", value.AcquisitionMode)
	}
	value.InitialFromMSC, err = r.i64()
	if err != nil {
		return value, err
	}
	value.CapabilityFlags, err = r.u32()
	return value, err
}

func decodeResume(r *reader) (ResumeV1, error) {
	var value ResumeV1
	var err error
	value.AcceptedProtocolVersion, err = r.u16()
	if err != nil {
		return value, err
	}
	if value.AcceptedProtocolVersion != ProtocolVersion {
		return value, newError(ErrInvalidField, "accepted protocol version %d", value.AcceptedProtocolVersion)
	}
	if value.GatewayInstanceID, err = r.string(); err != nil {
		return value, err
	}
	if value.SessionLeaseID, err = r.string(); err != nil {
		return value, err
	}
	if value.CommittedCursorMSC, err = r.i64(); err != nil {
		return value, err
	}
	if value.CommittedBoundaryDigest, err = r.hash(); err != nil {
		return value, err
	}
	if value.LastDurableBatchSequence, err = r.u64(); err != nil {
		return value, err
	}
	if value.LastDurableBatchHash, err = r.hash(); err != nil {
		return value, err
	}
	if value.NextFromMSC, err = r.i64(); err != nil {
		return value, err
	}
	if value.NextRequestedCount, err = r.u32(); err != nil {
		return value, err
	}
	if value.MaximumFrameBytes, err = r.u32(); err != nil {
		return value, err
	}
	if value.MaximumRecords, err = r.u32(); err != nil {
		return value, err
	}
	value.HeartbeatIdleTimeoutMS, err = r.u32()
	return value, err
}

func decodeBatch(r *reader) (BatchFrameV1, error) {
	var value BatchFrameV1
	var err error
	if value.SessionLeaseID, err = r.string(); err != nil {
		return value, err
	}
	if value.ProducerSessionID, err = r.string(); err != nil {
		return value, err
	}
	if value.BatchSequence, err = r.u64(); err != nil {
		return value, err
	}
	if value.RequestedFromMSC, err = r.i64(); err != nil {
		return value, err
	}
	if value.RequestedCount, err = r.u32(); err != nil {
		return value, err
	}
	if value.FetchWallStartS, err = r.i64(); err != nil {
		return value, err
	}
	if value.FetchWallEndS, err = r.i64(); err != nil {
		return value, err
	}
	if value.FetchMonotonicStartUS, err = r.u64(); err != nil {
		return value, err
	}
	if value.FetchMonotonicEndUS, err = r.u64(); err != nil {
		return value, err
	}
	if value.ReturnedCount, err = r.i32(); err != nil {
		return value, err
	}
	if value.CopyTicksError, err = r.i32(); err != nil {
		return value, err
	}
	if value.SourceStatusFlags, err = r.u32(); err != nil {
		return value, err
	}
	if value.SourceSchemaID, err = r.string(); err != nil {
		return value, err
	}
	if value.SourceSchemaID != SourceSchemaMT5 {
		return value, newError(ErrInvalidField, "source schema %q", value.SourceSchemaID)
	}
	count, err := r.u32()
	if err != nil {
		return value, err
	}
	if count > MaxRecords {
		return value, newError(ErrInvalidField, "record count %d", count)
	}
	value.Records = make([]RawMqlTickV1, count)
	for i := range value.Records {
		record := &value.Records[i]
		if record.Time, err = r.i64(); err != nil {
			return value, err
		}
		if record.BidBits, err = r.u64(); err != nil {
			return value, err
		}
		if record.AskBits, err = r.u64(); err != nil {
			return value, err
		}
		if record.LastBits, err = r.u64(); err != nil {
			return value, err
		}
		if record.Volume, err = r.u64(); err != nil {
			return value, err
		}
		if record.TimeMSC, err = r.i64(); err != nil {
			return value, err
		}
		if record.Flags, err = r.u32(); err != nil {
			return value, err
		}
		if record.VolumeRealBits, err = r.u64(); err != nil {
			return value, err
		}
		if record.CaptureSequence, err = r.u64(); err != nil {
			return value, err
		}
	}
	if value.ReturnedCount >= 0 && value.ReturnedCount != int32(count) {
		return value, newError(ErrInvalidField, "returned count %d does not match %d records", value.ReturnedCount, count)
	}
	if value.ReturnedCount < 0 && count != 0 {
		return value, newError(ErrInvalidField, "negative returned count has records")
	}
	return value, nil
}

func decodeAck(r *reader) (AckV1, error) {
	var value AckV1
	var err error
	if value.ProducerSessionID, err = r.string(); err != nil {
		return value, err
	}
	if value.BatchSequence, err = r.u64(); err != nil {
		return value, err
	}
	if value.GatewayBatchSHA256, err = r.hash(); err != nil {
		return value, err
	}
	if value.GatewayIngestSequence, err = r.u64(); err != nil {
		return value, err
	}
	if value.Status, err = r.u8(); err != nil {
		return value, err
	}
	if value.Status < 1 || value.Status > 9 {
		return value, newError(ErrInvalidField, "ack status %d", value.Status)
	}
	if value.CommittedCursorMSC, err = r.i64(); err != nil {
		return value, err
	}
	if value.CommittedBoundaryDigest, err = r.hash(); err != nil {
		return value, err
	}
	if value.NextFromMSC, err = r.i64(); err != nil {
		return value, err
	}
	if value.NextRequestedCount, err = r.u32(); err != nil {
		return value, err
	}
	value.RetryAfterMS, err = r.u32()
	return value, err
}

func decodeError(r *reader) (ErrorV1, error) {
	var value ErrorV1
	var err error
	if value.Code, err = r.u16(); err != nil {
		return value, err
	}
	if value.Code < 1 || value.Code > 10 {
		return value, newError(ErrInvalidField, "error code %d", value.Code)
	}
	if value.Retryable, err = r.u8(); err != nil {
		return value, err
	}
	if value.Retryable > 1 {
		return value, newError(ErrInvalidField, "retryable %d", value.Retryable)
	}
	messageType, err := r.u16()
	if err != nil {
		return value, err
	}
	value.RelatedMessageType = MessageType(messageType)
	if value.RelatedMessageType > MessageError {
		return value, newError(ErrInvalidField, "related message type %d", messageType)
	}
	if value.RelatedBatchSequence, err = r.u64(); err != nil {
		return value, err
	}
	value.Message, err = r.string()
	return value, err
}

func SourcePayloadFingerprint(record RawMqlTickV1) [32]byte {
	var w writer
	w.WriteString("tick-data-platform/source-payload/v1\x00")
	_ = w.string(SourceSchemaMT5)
	w.i64(record.Time)
	w.u64(record.BidBits)
	w.u64(record.AskBits)
	w.u64(record.LastBits)
	w.u64(record.Volume)
	w.i64(record.TimeMSC)
	w.u32(record.Flags)
	w.u64(record.VolumeRealBits)
	return sha256.Sum256(w.Bytes())
}

func ObservationHash(producerInstanceID, producerSessionID string, batchSequence uint64, recordOrdinal uint32, captureSequence uint64, sourcePayloadFingerprint [32]byte) [32]byte {
	var w writer
	w.WriteString("tick-data-platform/observation/v1\x00")
	_ = w.string(producerInstanceID)
	_ = w.string(producerSessionID)
	w.u64(batchSequence)
	w.u32(recordOrdinal)
	w.u64(captureSequence)
	w.hash(sourcePayloadFingerprint)
	return sha256.Sum256(w.Bytes())
}

func GatewayBatchSHA256(frame []byte) [32]byte {
	var w writer
	w.WriteString("tick-data-platform/batch/v1\x00")
	w.Write(frame)
	return sha256.Sum256(w.Bytes())
}

func WALEntryHash(sequence uint64, previous [32]byte, receiveWallS int64, receiveMonotonicUS uint64, batchHash [32]byte, frame []byte) [32]byte {
	var w writer
	w.WriteString("tick-data-platform/wal-entry/v1\x00")
	w.u64(sequence)
	w.hash(previous)
	w.i64(receiveWallS)
	w.u64(receiveMonotonicUS)
	w.hash(batchHash)
	w.Write(frame)
	return sha256.Sum256(w.Bytes())
}
