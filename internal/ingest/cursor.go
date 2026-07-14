package ingest

import (
	"tick-data-platform/internal/journal"
	"tick-data-platform/internal/protocol"
)

func outcomeForBatch(state journal.State, batch protocol.BatchFrameV1, config Config) journal.Outcome {
	config = config.withDefaults()
	outcome := journal.Outcome{
		Status:                  protocol.AckAcceptedNoAdvance,
		CommittedCursorMSC:      state.CommittedCursorMSC,
		CommittedBoundaryDigest: state.CommittedBoundaryDigest,
		NextFromMSC:             state.NextFromMSC,
		NextRequestedCount:      state.NextRequestedCount,
	}
	if outcome.NextRequestedCount == 0 {
		outcome.NextRequestedCount = config.InitialBatchCount
	}
	if outcome.NextFromMSC == 0 && state.CommittedCursorMSC != 0 {
		outcome.NextFromMSC = state.CommittedCursorMSC
	}
	if batch.CopyTicksError != 0 || batch.ReturnedCount < 0 || len(batch.Records) == 0 {
		return outcome
	}

	maxMSC := batch.Records[0].TimeMSC
	for _, record := range batch.Records[1:] {
		if record.TimeMSC > maxMSC {
			maxMSC = record.TimeMSC
		}
	}
	if maxMSC > state.CommittedCursorMSC {
		boundary := boundaryRecords(batch.Records, maxMSC)
		outcome.Status = protocol.AckAcceptedAdvanced
		outcome.CommittedCursorMSC = maxMSC
		outcome.CommittedBoundaryDigest = protocol.BoundaryDigest(maxMSC, [32]byte{}, boundary)
		outcome.NextFromMSC = maxMSC
		outcome.NextRequestedCount = config.InitialBatchCount
		return outcome
	}
	if maxMSC < state.CommittedCursorMSC {
		return outcome
	}

	boundary := boundaryRecords(batch.Records, maxMSC)
	if batch.RequestedCount > 0 && batch.ReturnedCount == int32(batch.RequestedCount) {
		currentCount := outcome.NextRequestedCount
		if batch.RequestedCount > currentCount {
			currentCount = batch.RequestedCount
		}
		if currentCount >= config.DenseBoundaryHardCap {
			outcome.Status = protocol.AckDenseUnresolved
			outcome.NextRequestedCount = config.DenseBoundaryHardCap
			outcome.NextFromMSC = state.CommittedCursorMSC
			return outcome
		}
		next := currentCount * 2
		if next < currentCount || next > config.DenseBoundaryHardCap {
			next = config.DenseBoundaryHardCap
		}
		outcome.Status = protocol.AckDenseBoundary
		outcome.NextRequestedCount = next
		outcome.NextFromMSC = state.CommittedCursorMSC
		return outcome
	}
	outcome.CommittedBoundaryDigest = protocol.BoundaryDigest(
		state.CommittedCursorMSC,
		state.CommittedBoundaryDigest,
		boundary,
	)
	outcome.NextRequestedCount = config.InitialBatchCount
	outcome.NextFromMSC = state.CommittedCursorMSC
	return outcome
}

func boundaryRecords(records []protocol.RawMqlTickV1, cursorMSC int64) []protocol.RawMqlTickV1 {
	result := make([]protocol.RawMqlTickV1, 0, len(records))
	for _, record := range records {
		if record.TimeMSC == cursorMSC {
			result = append(result, record)
		}
	}
	return result
}
