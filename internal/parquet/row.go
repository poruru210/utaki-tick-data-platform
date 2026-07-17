package parquet

import "tick-data-platform/internal/protocol"

// parquetRow is intentionally flat at the top level and uses nullable data
// and marker groups so one physical schema represents both replay row kinds.
// The field declaration order is the ticks-parquet-v1 column order.
type parquetRow struct {
	RowKind              uint8      `parquet:"row_kind,uint(8),uncompressed,plain"`
	StreamSequence       uint64     `parquet:"stream_sequence,uint(64),uncompressed,plain"`
	DatasetID            string     `parquet:"dataset_id,uncompressed,plain"`
	DayDefinitionID      string     `parquet:"day_definition_id,uncompressed,plain"`
	Date                 string     `parquet:"date,uncompressed,plain"`
	ReplayContractID     string     `parquet:"replay_contract_id,uncompressed,plain"`
	ConversionID         string     `parquet:"conversion_id,uncompressed,plain"`
	RawDayManifestKey    string     `parquet:"raw_day_manifest_key,uncompressed,plain"`
	RawDayManifestSHA256 [32]byte   `parquet:"raw_day_manifest_sha256,uncompressed,plain"`
	ContinuitySegmentID  string     `parquet:"continuity_segment_id,uncompressed,plain"`
	PreviousRowChainHash [32]byte   `parquet:"previous_row_chain_hash,uncompressed,plain"`
	RowChainHash         [32]byte   `parquet:"row_chain_hash,uncompressed,plain"`
	Data                 *dataRow   `parquet:"data,optional"`
	Marker               *markerRow `parquet:"marker,optional"`
}

type dataRow struct {
	RawObjectKey             string   `parquet:"raw_object_key,uncompressed,plain"`
	RawObjectSHA256          [32]byte `parquet:"raw_object_sha256,uncompressed,plain"`
	GatewayIngestSequence    uint64   `parquet:"gateway_ingest_sequence,uint(64),uncompressed,plain"`
	ProducerInstanceID       string   `parquet:"producer_instance_id,uncompressed,plain"`
	ProducerSessionID        string   `parquet:"producer_session_id,uncompressed,plain"`
	BatchSequence            uint64   `parquet:"batch_sequence,uint(64),uncompressed,plain"`
	RecordOrdinal            uint32   `parquet:"record_ordinal,uint(32),uncompressed,plain"`
	CaptureSequence          uint64   `parquet:"capture_sequence,uint(64),uncompressed,plain"`
	Time                     int64    `parquet:"time,int(64),uncompressed,plain"`
	BidBits                  uint64   `parquet:"bid_bits,uint(64),uncompressed,plain"`
	AskBits                  uint64   `parquet:"ask_bits,uint(64),uncompressed,plain"`
	LastBits                 uint64   `parquet:"last_bits,uint(64),uncompressed,plain"`
	Volume                   uint64   `parquet:"volume,uint(64),uncompressed,plain"`
	TimeMSC                  int64    `parquet:"time_msc,int(64),uncompressed,plain"`
	Flags                    uint32   `parquet:"flags,uint(32),uncompressed,plain"`
	VolumeRealBits           uint64   `parquet:"volume_real_bits,uint(64),uncompressed,plain"`
	SourcePayloadFingerprint [32]byte `parquet:"source_payload_fingerprint,uncompressed,plain"`
	ObservationHash          [32]byte `parquet:"observation_hash,uncompressed,plain"`
	FetchWallStartS          int64    `parquet:"fetch_wall_start_s,int(64),uncompressed,plain"`
	FetchWallEndS            int64    `parquet:"fetch_wall_end_s,int(64),uncompressed,plain"`
	FetchMonotonicStartUS    uint64   `parquet:"fetch_monotonic_start_us,uint(64),uncompressed,plain"`
	FetchMonotonicEndUS      uint64   `parquet:"fetch_monotonic_end_us,uint(64),uncompressed,plain"`
	CopyTicksError           int32    `parquet:"copy_ticks_error,int(32),uncompressed,plain"`
	SourceStatusFlags        uint32   `parquet:"source_status_flags,uint(32),uncompressed,plain"`
}

type markerRow struct {
	RawObjectKey                   string   `parquet:"raw_object_key,uncompressed,plain"`
	RawObjectSHA256                [32]byte `parquet:"raw_object_sha256,uncompressed,plain"`
	MarkerCode                     string   `parquet:"marker_code,uncompressed,plain"`
	Reason                         string   `parquet:"reason,uncompressed,plain"`
	Detail                         string   `parquet:"detail,uncompressed,plain"`
	ReferenceGatewayIngestSequence uint64   `parquet:"reference_gateway_ingest_sequence,uint(64),uncompressed,plain"`
	ReferenceRecordOrdinal         uint32   `parquet:"reference_record_ordinal,uint(32),uncompressed,plain"`
	PredecessorRowChainHash        [32]byte `parquet:"predecessor_row_chain_hash,uncompressed,plain"`
	ContinuitySegmentStartHash     [32]byte `parquet:"continuity_segment_start_hash,uncompressed,plain"`
}

func makeParquetRow(row protocol.ReplayRow, previous [32]byte) (parquetRow, []byte, [32]byte, error) {
	if row.Kind == protocol.ReplayRowData && (row.Data == nil || row.Marker != nil) || row.Kind == protocol.ReplayRowMarker && (row.Marker == nil || row.Data != nil) {
		return parquetRow{}, nil, [32]byte{}, errInvalidParquetRow
	}
	canonical, err := row.CanonicalBytes()
	if err != nil {
		return parquetRow{}, nil, [32]byte{}, err
	}
	if row.StreamSequence() == ^uint64(0) {
		return parquetRow{}, nil, [32]byte{}, errStreamSequenceOverflow
	}
	next := protocol.RowChainStep(row.StreamSequence(), previous, canonical)
	scope := row.Scope()
	result := parquetRow{
		RowKind:              row.Kind,
		StreamSequence:       row.StreamSequence(),
		DatasetID:            scope.DatasetID,
		DayDefinitionID:      scope.DayDefinitionID,
		Date:                 scope.Date,
		ReplayContractID:     scope.ReplayContractID,
		ConversionID:         scope.ConversionID,
		RawDayManifestKey:    scope.RawDayManifestKey,
		RawDayManifestSHA256: scope.RawDayManifestSHA256,
		PreviousRowChainHash: previous,
		RowChainHash:         next,
	}
	if row.Data != nil {
		record := row.Data.Record
		result.ContinuitySegmentID = row.Data.ContinuitySegmentID
		result.Data = &dataRow{
			RawObjectKey: row.Data.RawObjectKey, RawObjectSHA256: row.Data.RawObjectSHA256,
			GatewayIngestSequence: row.Data.GatewayIngestSequence, ProducerInstanceID: row.Data.ProducerInstanceID,
			ProducerSessionID: row.Data.ProducerSessionID, BatchSequence: row.Data.BatchSequence,
			RecordOrdinal: row.Data.RecordOrdinal, CaptureSequence: row.Data.CaptureSequence,
			Time: record.Time, BidBits: record.BidBits, AskBits: record.AskBits, LastBits: record.LastBits,
			Volume: record.Volume, TimeMSC: record.TimeMSC, Flags: record.Flags, VolumeRealBits: record.VolumeRealBits,
			SourcePayloadFingerprint: row.Data.SourcePayloadFingerprint, ObservationHash: row.Data.ObservationHash,
			FetchWallStartS: row.Data.FetchWallStartS, FetchWallEndS: row.Data.FetchWallEndS,
			FetchMonotonicStartUS: row.Data.FetchMonotonicStartUS, FetchMonotonicEndUS: row.Data.FetchMonotonicEndUS,
			CopyTicksError: row.Data.CopyTicksError, SourceStatusFlags: row.Data.SourceStatusFlags,
		}
	}
	if row.Marker != nil {
		result.ContinuitySegmentID = row.Marker.ContinuitySegmentID
		result.Marker = &markerRow{
			RawObjectKey: row.Marker.RawObjectKey, RawObjectSHA256: row.Marker.RawObjectSHA256,
			MarkerCode: row.Marker.MarkerCode, Reason: row.Marker.Reason, Detail: row.Marker.Detail,
			ReferenceGatewayIngestSequence: row.Marker.ReferenceGatewayIngestSequence,
			ReferenceRecordOrdinal:         row.Marker.ReferenceRecordOrdinal,
			PredecessorRowChainHash:        row.Marker.PredecessorRowChainHash,
			ContinuitySegmentStartHash:     row.Marker.ContinuitySegmentStartHash,
		}
	}
	return result, canonical, next, nil
}

func (r parquetRow) replayRow() (protocol.ReplayRow, error) {
	scope := protocol.ReplayScope{
		DatasetID: r.DatasetID, DayDefinitionID: r.DayDefinitionID,
		Date: r.Date, ReplayContractID: r.ReplayContractID, ConversionID: r.ConversionID,
		RawDayManifestKey: r.RawDayManifestKey, RawDayManifestSHA256: r.RawDayManifestSHA256,
	}
	result := protocol.ReplayRow{Kind: r.RowKind}
	if r.Data != nil {
		result.Data = &protocol.ReplayDataRow{
			Scope: scope, StreamSequence: r.StreamSequence, ContinuitySegmentID: r.ContinuitySegmentID,
			RawObjectKey: r.Data.RawObjectKey, RawObjectSHA256: r.Data.RawObjectSHA256,
			GatewayIngestSequence: r.Data.GatewayIngestSequence, ProducerInstanceID: r.Data.ProducerInstanceID,
			ProducerSessionID: r.Data.ProducerSessionID, BatchSequence: r.Data.BatchSequence,
			RecordOrdinal: r.Data.RecordOrdinal, CaptureSequence: r.Data.CaptureSequence,
			Record: protocol.RawMqlTickV1{Time: r.Data.Time, BidBits: r.Data.BidBits, AskBits: r.Data.AskBits,
				LastBits: r.Data.LastBits, Volume: r.Data.Volume, TimeMSC: r.Data.TimeMSC, Flags: r.Data.Flags,
				VolumeRealBits: r.Data.VolumeRealBits, CaptureSequence: r.Data.CaptureSequence},
			SourcePayloadFingerprint: r.Data.SourcePayloadFingerprint, ObservationHash: r.Data.ObservationHash,
			FetchWallStartS: r.Data.FetchWallStartS, FetchWallEndS: r.Data.FetchWallEndS,
			FetchMonotonicStartUS: r.Data.FetchMonotonicStartUS, FetchMonotonicEndUS: r.Data.FetchMonotonicEndUS,
			CopyTicksError: r.Data.CopyTicksError, SourceStatusFlags: r.Data.SourceStatusFlags,
		}
	}
	if r.Marker != nil {
		result.Marker = &protocol.ReplayMarkerRow{
			Scope: scope, StreamSequence: r.StreamSequence, ContinuitySegmentID: r.ContinuitySegmentID,
			RawObjectKey: r.Marker.RawObjectKey, RawObjectSHA256: r.Marker.RawObjectSHA256,
			MarkerCode: r.Marker.MarkerCode, Reason: r.Marker.Reason, Detail: r.Marker.Detail,
			ReferenceGatewayIngestSequence: r.Marker.ReferenceGatewayIngestSequence,
			ReferenceRecordOrdinal:         r.Marker.ReferenceRecordOrdinal,
			PredecessorRowChainHash:        r.Marker.PredecessorRowChainHash,
			ContinuitySegmentStartHash:     r.Marker.ContinuitySegmentStartHash,
		}
	}
	if (r.RowKind != protocol.ReplayRowData && r.RowKind != protocol.ReplayRowMarker) || (r.RowKind == protocol.ReplayRowData) != (r.Data != nil) || (r.RowKind == protocol.ReplayRowMarker) != (r.Marker != nil) {
		return protocol.ReplayRow{}, errInvalidParquetRow
	}
	return result, nil
}
