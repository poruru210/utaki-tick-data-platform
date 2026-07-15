package continuity

import (
	"fmt"

	"tick-data-platform/internal/protocol"
)

type Segment struct {
	ID                  string
	StartStreamSequence uint64
	StartReason         string
}

// Result is bounded summary state. It never retains the emitted row stream.
// A caller that needs rows for a test must collect them in its RowSink.
type Result struct {
	RowCount      uint64
	MarkerCount   uint64
	SegmentCount  uint64
	LastSegmentID string
	RowChainRoot  [32]byte
	TailSize      uint32
}

// Reduce verifies the reader contract again at the reducer boundary and
// emits replay rows in verified WAL order. Only a bounded fingerprint tail,
// the previous row-chain hash, and summary counters survive each emission.
func Reduce(reader VerifiedBatchReader, sink RowSink) (Result, error) {
	if reader == nil {
		return Result{}, fmt.Errorf("verified raw-day reader is required")
	}
	scope := reader.ReplayScope()
	if err := scope.Validate(); err != nil {
		return Result{}, fmt.Errorf("replay scope: %w", err)
	}
	identity := reader.ProducerIdentity()
	if identity.ProducerInstanceID == "" || identity.CampaignID == "" || identity.ProviderID == "" ||
		identity.StableFeedID == "" || identity.BrokerServerFingerprint == "" || identity.ExactSourceSymbol == "" {
		return Result{}, fmt.Errorf("verified producer identity is incomplete")
	}
	if identity.CampaignID != scope.CampaignID {
		return Result{}, fmt.Errorf("verified producer identity campaign mismatch")
	}
	maxRecords := reader.MaxRecords()
	if maxRecords == 0 || maxRecords > protocol.MaxRecords {
		return Result{}, fmt.Errorf("verified max records is invalid: %d", maxRecords)
	}
	if sink == nil {
		sink = func(protocol.ReplayRow) error { return nil }
	}

	result := Result{}
	var previousRowHash [32]byte
	var currentSegmentID string
	var tail []fingerprintOccurrence
	var previousOccurrence *fingerprintOccurrence
	var previousSession string
	var previousBatchSequence uint64
	var haveBatchSequence bool

	emit := func(row protocol.ReplayRow) error {
		canonical, err := row.CanonicalBytes()
		if err != nil {
			return err
		}
		streamSequence := row.StreamSequence()
		if streamSequence != result.RowCount {
			return fmt.Errorf("emitted row stream sequence %d, want %d", streamSequence, result.RowCount)
		}
		nextRowHash := protocol.RowChainStep(streamSequence, previousRowHash, canonical)
		if err := sink(row); err != nil {
			return err
		}
		result.RowCount++
		if row.Kind == protocol.ReplayRowMarker {
			result.MarkerCount++
		}
		previousRowHash = nextRowHash
		return nil
	}

	startSegment := func(code, reason, detail string, batch *VerifiedBatch, reference *fingerprintOccurrence) error {
		var gatewaySequence uint64
		var recordOrdinal uint32
		var rawKey string
		var rawHash [32]byte
		if batch != nil {
			gatewaySequence = batch.GatewayIngestSequence
			rawKey, rawHash = batch.ObjectKey, batch.ObjectSHA256
		}
		if reference != nil {
			gatewaySequence = reference.gatewaySequence
			recordOrdinal = reference.recordOrdinal
		}
		segmentID, err := protocol.SegmentID(scope, gatewaySequence, recordOrdinal, code, previousRowHash)
		if err != nil {
			return err
		}
		marker := protocol.ReplayMarkerRow{
			Scope:                          scope,
			StreamSequence:                 result.RowCount,
			ContinuitySegmentID:            segmentID,
			RawObjectKey:                   rawKey,
			RawObjectSHA256:                rawHash,
			MarkerCode:                     code,
			Reason:                         reason,
			Detail:                         detail,
			ReferenceGatewayIngestSequence: gatewaySequence,
			ReferenceRecordOrdinal:         recordOrdinal,
			PredecessorRowChainHash:        previousRowHash,
			ContinuitySegmentStartHash:     previousRowHash,
		}
		if err := emit(protocol.ReplayRow{Kind: protocol.ReplayRowMarker, Marker: &marker}); err != nil {
			return err
		}
		result.SegmentCount++
		result.LastSegmentID = segmentID
		currentSegmentID = segmentID
		tail = tail[:0]
		previousOccurrence = nil
		return nil
	}

	for {
		batch, ok, err := reader.Next()
		if err != nil {
			return Result{}, err
		}
		if !ok {
			break
		}
		_, sourceBatch, err := decodeBatch(batch.Frame)
		if err != nil {
			return Result{}, fmt.Errorf("verified WAL sequence %d: %w", batch.GatewayIngestSequence, err)
		}
		if len(sourceBatch.Records) > int(maxRecords) {
			return Result{}, fmt.Errorf("WAL sequence %d exceeds verified max records", batch.GatewayIngestSequence)
		}
		if sourceBatch.SourceSchemaID != protocol.SourceSchemaMT5 {
			return Result{}, fmt.Errorf("WAL sequence %d has unsupported source schema", batch.GatewayIngestSequence)
		}
		if sourceBatch.ProducerSessionID == "" {
			return Result{}, fmt.Errorf("WAL sequence %d has no producer session identity", batch.GatewayIngestSequence)
		}
		wantLease := protocol.DeriveSessionLeaseID(
			identity.ProducerInstanceID, sourceBatch.ProducerSessionID, identity.CampaignID,
			identity.ProviderID, identity.StableFeedID, identity.BrokerServerFingerprint, identity.ExactSourceSymbol,
		)
		if sourceBatch.SessionLeaseID != wantLease {
			return Result{}, fmt.Errorf("WAL sequence %d session lease does not match verified producer identity", batch.GatewayIngestSequence)
		}
		if len(batch.SelectedRecordOrdinals) == 0 {
			return Result{}, fmt.Errorf("verified WAL sequence %d has no manifest selection", batch.GatewayIngestSequence)
		}
		if !haveBatchSequence {
			if err := startSegment(protocol.MarkerSegmentStart, protocol.ReasonInitial, "first accepted source occurrence", &batch, nil); err != nil {
				return Result{}, err
			}
		} else if sourceBatch.ProducerSessionID == previousSession && sourceBatch.BatchSequence != previousBatchSequence+1 {
			if err := startSegment(protocol.MarkerGap, protocol.ReasonWALSequenceGap, "producer batch sequence gap", &batch, previousOccurrence); err != nil {
				return Result{}, err
			}
		}
		previousSession = sourceBatch.ProducerSessionID
		previousBatchSequence = sourceBatch.BatchSequence
		haveBatchSequence = true

		occurrences := make([]fingerprintOccurrence, 0, len(batch.SelectedRecordOrdinals))
		for _, ordinal := range batch.SelectedRecordOrdinals {
			if ordinal >= uint32(len(sourceBatch.Records)) {
				if len(sourceBatch.Records) == 0 && ordinal == 0 {
					continue
				}
				return Result{}, fmt.Errorf("manifest selected ordinal %d outside WAL sequence %d", ordinal, batch.GatewayIngestSequence)
			}
			record := sourceBatch.Records[ordinal]
			fingerprint := protocol.SourcePayloadFingerprint(record)
			occurrences = append(occurrences, fingerprintOccurrence{
				fingerprint: fingerprint, timeMSC: record.TimeMSC, gatewaySequence: batch.GatewayIngestSequence,
				recordOrdinal: ordinal, record: record, batch: sourceBatch, rawKey: batch.ObjectKey,
				rawHash: batch.ObjectSHA256, producerInstanceID: identity.ProducerInstanceID,
			})
		}
		if sourceBatch.CopyTicksError != 0 {
			if err := startSegment(protocol.MarkerSourceError, protocol.ReasonSourceReportedError, fmt.Sprintf("copy_ticks_error=%d", sourceBatch.CopyTicksError), &batch, previousOccurrence); err != nil {
				return Result{}, err
			}
			continue
		}
		if len(occurrences) == 0 {
			continue
		}
		if previousOccurrence != nil && occurrences[0].timeMSC < previousOccurrence.timeMSC {
			if err := startSegment(protocol.MarkerTimestampRegression, protocol.ReasonTimeMSCRegression, "source time_msc moved backwards", &batch, previousOccurrence); err != nil {
				return Result{}, err
			}
		}

		overlap := 0
		if changed, reference := uniqueHistoryChange(tail, occurrences); changed && previousOccurrence != nil &&
			previousOccurrence.producerInstanceID == identity.ProducerInstanceID {
			if err := startSegment(protocol.MarkerSourceHistoryChanged, protocol.ReasonSamePositionChanged, "one changed payload has exact surrounding fingerprint evidence", &batch, reference); err != nil {
				return Result{}, err
			}
		} else {
			var ambiguous bool
			overlap, ambiguous = overlapLength(tail, occurrences)
			if ambiguous || overlap == 0 && len(tail) > 0 {
				detail := "no uniquely provable suffix/prefix overlap"
				if ambiguous {
					detail = "longest suffix/prefix candidate has multiple positions"
				}
				if err := startSegment(protocol.MarkerAmbiguousOverlap, protocol.ReasonNoUniqueOverlap, detail, &batch, previousOccurrence); err != nil {
					return Result{}, err
				}
				overlap = 0
			}
		}
		for _, occurrence := range occurrences[overlap:] {
			row := makeDataRow(scope, currentSegmentID, occurrence)
			row.Data.StreamSequence = result.RowCount
			if err := emit(row); err != nil {
				return Result{}, err
			}
			tail = append(tail, occurrence)
			if len(tail) > int(maxRecords) {
				tail = tail[len(tail)-int(maxRecords):]
			}
			copyOccurrence := occurrence
			previousOccurrence = &copyOccurrence
		}
	}
	result.RowChainRoot = previousRowHash
	result.TailSize = uint32(len(tail))
	return result, nil
}

type fingerprintOccurrence struct {
	fingerprint        [32]byte
	timeMSC            int64
	gatewaySequence    uint64
	recordOrdinal      uint32
	record             protocol.RawMqlTickV1
	batch              protocol.BatchFrameV1
	rawKey             string
	rawHash            [32]byte
	producerInstanceID string
}

// overlapLength examines only the bounded tail. It chooses the longest exact
// suffix/prefix match and then checks multiplicity for that longest sequence.
// Shorter matches are normal consequences of a longer match and do not make a
// unique longest match ambiguous.
func overlapLength(previous, incoming []fingerprintOccurrence) (int, bool) {
	maxLength := len(previous)
	if len(incoming) < maxLength {
		maxLength = len(incoming)
	}
	candidates := make([]int, 0, maxLength)
	for length := 1; length <= maxLength; length++ {
		if equalSuffixPrefix(previous, incoming, length) {
			candidates = append(candidates, length)
		}
	}
	if len(candidates) == 0 {
		return 0, false
	}
	longest := candidates[len(candidates)-1]
	occurrenceCount := 0
	for start := 0; start+longest <= len(previous); start++ {
		matches := true
		for index := 0; index < longest; index++ {
			if previous[start+index].fingerprint != incoming[index].fingerprint {
				matches = false
				break
			}
		}
		if matches {
			occurrenceCount++
		}
	}
	if occurrenceCount != 1 {
		return 0, true
	}
	return longest, false
}

// uniqueHistoryChange proves one changed position at the authoritative batch
// boundary. It considers only a suffix of the retained tail against the
// incoming prefix, chooses the longest valid boundary length, and then proves
// that no other tail window of that length is wildcard-compatible. It never
// uses capture_sequence or time_msc as an identity coordinate.
func uniqueHistoryChange(previous, incoming []fingerprintOccurrence) (bool, *fingerprintOccurrence) {
	maxLength := len(previous)
	if len(incoming) < maxLength {
		maxLength = len(incoming)
	}
	var boundary historyAlignment
	for length := maxLength; length >= 3; length-- {
		candidate, ok := oneSubstitutionAlignment(previous, len(previous)-length, incoming, 0, length)
		if !ok {
			continue
		}
		boundary = candidate
		break
	}
	if boundary.length == 0 {
		return false, nil
	}

	// The boundary candidate is only accepted when the same incoming prefix
	// cannot be explained by another position in the retained tail. This is a
	// proof check, not an alternative alignment used to accept an interior
	// window.
	compatiblePositions := 0
	for start := 0; start+boundary.length <= len(previous); start++ {
		if _, ok := oneSubstitutionAlignment(previous, start, incoming, 0, boundary.length); ok {
			compatiblePositions++
		}
	}
	if compatiblePositions != 1 {
		return false, nil
	}
	reference := previous[boundary.previousStart+boundary.changedOffset]
	return true, &reference
}

type historyAlignment struct {
	previousStart int
	changedOffset int
	length        int
}

func oneSubstitutionAlignment(previous []fingerprintOccurrence, previousStart int, incoming []fingerprintOccurrence, incomingStart int, length int) (historyAlignment, bool) {
	if length < 3 || previousStart < 0 || incomingStart < 0 || previousStart+length > len(previous) || incomingStart+length > len(incoming) {
		return historyAlignment{}, false
	}
	changedOffset := -1
	for offset := 0; offset < length; offset++ {
		if previous[previousStart+offset].fingerprint == incoming[incomingStart+offset].fingerprint {
			continue
		}
		if offset == 0 || offset == length-1 || changedOffset >= 0 {
			return historyAlignment{}, false
		}
		changedOffset = offset
	}
	if changedOffset < 0 {
		return historyAlignment{}, false
	}
	return historyAlignment{previousStart: previousStart, changedOffset: changedOffset, length: length}, true
}

func equalSuffixPrefix(previous, incoming []fingerprintOccurrence, length int) bool {
	if length > len(previous) || length > len(incoming) {
		return false
	}
	for index := 0; index < length; index++ {
		if previous[len(previous)-length+index].fingerprint != incoming[index].fingerprint {
			return false
		}
	}
	return true
}

func makeDataRow(scope protocol.ReplayScope, segmentID string, occurrence fingerprintOccurrence) protocol.ReplayRow {
	observation := protocol.ObservationHash(occurrence.producerInstanceID, occurrence.batch.ProducerSessionID, occurrence.batch.BatchSequence, occurrence.recordOrdinal, occurrence.record.CaptureSequence, occurrence.fingerprint)
	data := protocol.ReplayDataRow{
		Scope: scope, StreamSequence: 0, ContinuitySegmentID: segmentID,
		RawObjectKey: occurrence.rawKey, RawObjectSHA256: occurrence.rawHash,
		GatewayIngestSequence: occurrence.gatewaySequence, ProducerInstanceID: occurrence.producerInstanceID,
		ProducerSessionID: occurrence.batch.ProducerSessionID, BatchSequence: occurrence.batch.BatchSequence,
		RecordOrdinal: occurrence.recordOrdinal, CaptureSequence: occurrence.record.CaptureSequence,
		Record: occurrence.record, SourcePayloadFingerprint: occurrence.fingerprint, ObservationHash: observation,
		FetchWallStartS: occurrence.batch.FetchWallStartS, FetchWallEndS: occurrence.batch.FetchWallEndS,
		FetchMonotonicStartUS: occurrence.batch.FetchMonotonicStartUS, FetchMonotonicEndUS: occurrence.batch.FetchMonotonicEndUS,
		CopyTicksError: occurrence.batch.CopyTicksError, SourceStatusFlags: occurrence.batch.SourceStatusFlags,
	}
	return protocol.ReplayRow{Kind: protocol.ReplayRowData, Data: &data}
}

func decodeBatch(frame []byte) ([]byte, protocol.BatchFrameV1, error) {
	decodedFrame, err := protocol.DecodeFrame(frame)
	if err != nil {
		return nil, protocol.BatchFrameV1{}, err
	}
	message, err := protocol.DecodeMessage(decodedFrame)
	if err != nil {
		return nil, protocol.BatchFrameV1{}, err
	}
	batch, ok := message.(protocol.BatchFrameV1)
	if !ok {
		return nil, protocol.BatchFrameV1{}, fmt.Errorf("WAL entry is not BatchFrameV1")
	}
	canonical, err := protocol.EncodeMessage(batch)
	if err != nil {
		return nil, protocol.BatchFrameV1{}, err
	}
	if string(canonical) != string(frame) {
		return nil, protocol.BatchFrameV1{}, fmt.Errorf("BatchFrameV1 bytes are not canonical")
	}
	return frame, batch, nil
}
