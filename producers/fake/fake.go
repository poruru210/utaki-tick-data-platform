// Package fake provides a deterministic, network-free Protocol V1 producer.
package fake

import (
	"bytes"
	"math"

	protocolv1 "tick-data-platform/protocol/v1/go"
)

type Result struct {
	Frame                    []byte
	SourcePayloadFingerprint [32]byte
	ObservationHash          [32]byte
	GatewayBatchSHA256       [32]byte
}

func BatchFixture() (Result, error) {
	batch := protocolv1.BatchFrameV1{
		SessionLeaseID:        "lease-0001",
		ProducerSessionID:     "session-0001",
		BatchSequence:         8,
		RequestedFromMSC:      1710000000000,
		RequestedCount:        1,
		FetchWallStartS:       1710000000,
		FetchWallEndS:         1710000001,
		FetchMonotonicStartUS: 100000,
		FetchMonotonicEndUS:   100500,
		ReturnedCount:         1,
		CopyTicksError:        0,
		SourceStatusFlags:     0,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time:            1710000000,
			BidBits:         math.Float64bits(1.2345),
			AskBits:         math.Float64bits(1.2347),
			LastBits:        math.Float64bits(1.2346),
			Volume:          7,
			TimeMSC:         1710000000123,
			Flags:           3,
			VolumeRealBits:  math.Float64bits(7.5),
			CaptureSequence: 42,
		}},
	}
	frame, err := protocolv1.EncodeMessage(batch)
	if err != nil {
		return Result{}, err
	}
	sourceHash := protocolv1.SourcePayloadFingerprint(batch.Records[0])
	observationHash := protocolv1.ObservationHash(
		"fake-01",
		batch.ProducerSessionID,
		batch.BatchSequence,
		0,
		batch.Records[0].CaptureSequence,
		sourceHash,
	)
	return Result{
		Frame:                    frame,
		SourcePayloadFingerprint: sourceHash,
		ObservationHash:          observationHash,
		GatewayBatchSHA256:       protocolv1.GatewayBatchSHA256(frame),
	}, nil
}

func VerifyFrame(raw []byte) protocolv1.ErrorCode {
	frame, err := protocolv1.DecodeFrame(raw)
	if err != nil {
		return protocolv1.ErrorCodeOf(err)
	}
	if _, err := protocolv1.DecodeMessage(frame); err != nil {
		return protocolv1.ErrorCodeOf(err)
	}
	return ""
}

func DuplicateIdentityStatus(first, second []byte) (uint8, protocolv1.ErrorCode) {
	firstFrame, err := protocolv1.DecodeFrame(first)
	if err != nil {
		return 0, protocolv1.ErrorCodeOf(err)
	}
	secondFrame, err := protocolv1.DecodeFrame(second)
	if err != nil {
		return 0, protocolv1.ErrorCodeOf(err)
	}
	firstMessage, err := protocolv1.DecodeMessage(firstFrame)
	if err != nil {
		return 0, protocolv1.ErrorCodeOf(err)
	}
	secondMessage, err := protocolv1.DecodeMessage(secondFrame)
	if err != nil {
		return 0, protocolv1.ErrorCodeOf(err)
	}
	firstBatch, firstOK := firstMessage.(protocolv1.BatchFrameV1)
	secondBatch, secondOK := secondMessage.(protocolv1.BatchFrameV1)
	if !firstOK || !secondOK {
		return 0, protocolv1.ErrInvalidField
	}
	if firstBatch.ProducerSessionID != secondBatch.ProducerSessionID ||
		firstBatch.BatchSequence != secondBatch.BatchSequence {
		return 0, ""
	}
	if bytes.Equal(first, second) {
		return 3, ""
	}
	return 0, protocolv1.ErrSourceStateConflict
}
