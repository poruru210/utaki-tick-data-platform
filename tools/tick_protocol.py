"""Independent Python implementation of the Protocol V1 contract."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from datetime import date as date_type
from typing import Any

MAGIC = b"TICK"
PROTOCOL_VERSION = 1
HEADER_LENGTH = 16
MIN_FRAME_BYTES = 20
MAX_FRAME_BYTES = 1_048_576
MAX_RECORDS = 4096
MAX_STRING_BYTES = 255
MAX_PATH_BYTES = 1024
SOURCE_SCHEMA_MT5 = "mt5.mqltick.v1"

REPLAY_ROW_DATA = 1
REPLAY_ROW_MARKER = 2
REPLAY_ROW_DOMAIN = b"tick-data-platform/replay-row/v1\0"
REPLAY_MARKER_DOMAIN = b"tick-data-platform/replay-marker/v1\0"
REPLAY_ROW_CHAIN_DOMAIN = b"tick-data-platform/replay-row-chain/v1\0"
CONTINUITY_SEGMENT_DOMAIN = b"tick-data-platform/continuity-segment/v1\0"
PART_MANIFEST_DOMAIN = b"tick-data-platform/part-manifest/v1\0"
PART_SET_ROOT_DOMAIN = b"tick-data-platform/part-set/v1\0"
REPLAY_MANIFEST_DOMAIN = b"tick-data-platform/replay-day-manifest/v1\0"
RAW_DAY_MANIFEST_DOMAIN = b"tick-data-platform/raw-day-manifest/v1\0"
PUBLISHER_CLAIM_DOMAIN = b"tick-data-platform/publisher-claim/v1\0"
REPLAY_PUBLICATION_BUNDLE_DOMAIN = b"tick-data-platform/replay-publication-bundle/v1\0"
REPLAY_FINAL_OBSERVATION_DOMAIN = b"tick-data-platform/replay-publication-final-observation/v1\0"
HANDOVER_ARTIFACT_DOMAIN = b"tick-data-platform/publisher-handover/v1\0"
HANDOVER_ARTIFACT_VERSION = "publisher-handover-v1"
REPLAY_PUBLICATION_BUNDLE_VERSION = "replay-publication-bundle-v1"
REPLAY_FINAL_OBSERVATION_VERSION = "replay-publication-final-observation-v1"

REPLAY_PUBLICATION_IMPLEMENTATION_BOUNDS = {
    "max_graph_nodes": 50_000,
    "max_list_objects": 50_000,
    "max_metadata_object_bytes": 16_777_216,
    "max_observation_bytes": 70_368_744_177_664,
    "max_observation_requests": 100_000,
    "max_parquet_object_bytes": 1_099_511_627_776,
    "max_parts": 10_000,
    "max_publication_rounds": 20_002,
    "max_total_metadata_bytes": 268_435_456,
    "max_total_parquet_bytes": 17_592_186_044_416,
}

MESSAGE_TYPES = {
    1: "HelloV1",
    2: "ResumeV1",
    3: "BatchFrameV1",
    4: "AckV1",
    5: "ErrorV1",
}

ERROR_CODES = {
    1: "INVALID_FRAME",
    2: "UNSUPPORTED_PROTOCOL_VERSION",
    3: "UNKNOWN_MESSAGE_TYPE",
    4: "TRUNCATED_FRAME",
    5: "OVERSIZED_FRAME",
    6: "CRC_MISMATCH",
    7: "INVALID_FIELD",
    8: "SOURCE_STATE_CONFLICT",
    9: "SESSION_LEASE_CONFLICT",
    10: "INTERNAL_RETRYABLE",
}


class ProtocolError(ValueError):
    """Protocol V1 rejection with a stable error code."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        super().__init__(f"{code}: {detail}" if detail else code)


def raw_day_manifest_digest(canonical_bytes: bytes) -> bytes:
    """Return the M2 raw-day domain digest, never plain SHA-256."""

    return hashlib.sha256(RAW_DAY_MANIFEST_DOMAIN + canonical_bytes).digest()


@dataclass(frozen=True)
class Frame:
    version: int
    message_type: int
    payload: bytes


class _Reader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def take(self, size: int) -> bytes:
        if size < 0 or self.offset + size > len(self.payload):
            raise ProtocolError("INVALID_FIELD", "field exceeds payload")
        value = self.payload[self.offset : self.offset + size]
        self.offset += size
        return value

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i32(self) -> int:
        return struct.unpack("<i", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def hash32(self) -> bytes:
        return self.take(32)

    def string(self) -> str:
        size = self.u16()
        if size > MAX_STRING_BYTES:
            raise ProtocolError("INVALID_FIELD", f"string has {size} bytes")
        value = self.take(size)
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("INVALID_FIELD", "string is not UTF-8") from exc

    def done(self) -> None:
        if self.offset != len(self.payload):
            raise ProtocolError(
                "INVALID_FIELD", f"{len(self.payload) - self.offset} trailing payload bytes"
            )


def _crc32c(data: bytes) -> int:
    """Return CRC32C Castagnoli using a small table-free reference loop."""

    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFF


def encode_frame(message_type: int, payload: bytes) -> bytes:
    if message_type not in MESSAGE_TYPES:
        raise ProtocolError("UNKNOWN_MESSAGE_TYPE", str(message_type))
    frame_length = 20 + len(payload)
    if frame_length > MAX_FRAME_BYTES:
        raise ProtocolError("OVERSIZED_FRAME", str(frame_length))
    body = (
        MAGIC
        + struct.pack("<HHII", PROTOCOL_VERSION, message_type, frame_length, HEADER_LENGTH)
        + payload
    )
    return body + struct.pack("<I", _crc32c(body))


def decode_frame(raw: bytes) -> Frame:
    if len(raw) < 12:
        raise ProtocolError("TRUNCATED_FRAME", f"frame has {len(raw)} bytes")
    frame_length = struct.unpack_from("<I", raw, 8)[0]
    if frame_length > MAX_FRAME_BYTES:
        raise ProtocolError("OVERSIZED_FRAME", str(frame_length))
    if frame_length < MIN_FRAME_BYTES:
        raise ProtocolError("INVALID_FRAME", str(frame_length))
    if len(raw) < frame_length:
        raise ProtocolError("TRUNCATED_FRAME", f"frame has {len(raw)} bytes")
    if len(raw) != frame_length:
        raise ProtocolError("INVALID_FRAME", "trailing bytes")
    if raw[:4] != MAGIC:
        raise ProtocolError("INVALID_FRAME", "magic")
    if struct.unpack_from("<I", raw, 12)[0] != HEADER_LENGTH:
        raise ProtocolError("INVALID_FRAME", "header length")
    version, message_type = struct.unpack_from("<HH", raw, 4)
    if version != PROTOCOL_VERSION:
        raise ProtocolError("UNSUPPORTED_PROTOCOL_VERSION", str(version))
    if message_type not in MESSAGE_TYPES:
        raise ProtocolError("UNKNOWN_MESSAGE_TYPE", str(message_type))
    expected = struct.unpack_from("<I", raw, frame_length - 4)[0]
    actual = _crc32c(raw[: frame_length - 4])
    if expected != actual:
        raise ProtocolError("CRC_MISMATCH", f"{expected:08x} != {actual:08x}")
    return Frame(version, message_type, raw[16 : frame_length - 4])


def _read_strings(reader: _Reader, names: tuple[str, ...]) -> dict[str, Any]:
    return {name: reader.string() for name in names}


def decode_message(frame: Frame) -> dict[str, Any]:
    reader = _Reader(frame.payload)
    if frame.message_type == 1:
        value = _read_strings(
            reader,
            (
                "producer_instance_id",
                "producer_session_id",
                "producer_build_id",
                "mql_compiler_build",
                "terminal_build",
                "os_contract",
                "clock_api_id",
                "campaign_id",
                "provider_id",
                "stable_feed_id",
                "broker_server_fingerprint",
                "exact_source_symbol",
                "source_schema_id",
            ),
        )
        value["acquisition_mode"] = reader.u8()
        if value["acquisition_mode"] not in (1, 2):
            raise ProtocolError("INVALID_FIELD", "acquisition mode")
        value["initial_from_msc"] = reader.i64()
        value["capability_flags"] = reader.u32()
    elif frame.message_type == 2:
        value = {
            "accepted_protocol_version": reader.u16(),
            "gateway_instance_id": reader.string(),
            "session_lease_id": reader.string(),
            "committed_cursor_msc": reader.i64(),
            "committed_boundary_digest": reader.hash32(),
            "last_durable_batch_sequence": reader.u64(),
            "last_durable_batch_hash": reader.hash32(),
            "next_from_msc": reader.i64(),
            "next_requested_count": reader.u32(),
            "maximum_frame_bytes": reader.u32(),
            "maximum_records": reader.u32(),
            "heartbeat_idle_timeout_ms": reader.u32(),
        }
        if value["accepted_protocol_version"] != PROTOCOL_VERSION:
            raise ProtocolError("INVALID_FIELD", "accepted protocol version")
    elif frame.message_type == 3:
        value = {
            "session_lease_id": reader.string(),
            "producer_session_id": reader.string(),
            "batch_sequence": reader.u64(),
            "requested_from_msc": reader.i64(),
            "requested_count": reader.u32(),
            "fetch_wall_start_s": reader.i64(),
            "fetch_wall_end_s": reader.i64(),
            "fetch_monotonic_start_us": reader.u64(),
            "fetch_monotonic_end_us": reader.u64(),
            "returned_count": reader.i32(),
            "copy_ticks_error": reader.i32(),
            "source_status_flags": reader.u32(),
            "source_schema_id": reader.string(),
        }
        if value["source_schema_id"] != SOURCE_SCHEMA_MT5:
            raise ProtocolError("INVALID_FIELD", "source schema")
        count = reader.u32()
        if count > MAX_RECORDS:
            raise ProtocolError("INVALID_FIELD", "record count")
        records = []
        for _ in range(count):
            records.append(
                {
                    "time": reader.i64(),
                    "bid_bits": reader.u64(),
                    "ask_bits": reader.u64(),
                    "last_bits": reader.u64(),
                    "volume": reader.u64(),
                    "time_msc": reader.i64(),
                    "flags": reader.u32(),
                    "volume_real_bits": reader.u64(),
                    "capture_sequence": reader.u64(),
                }
            )
        value["records"] = records
        if value["returned_count"] >= 0 and value["returned_count"] != count:
            raise ProtocolError("INVALID_FIELD", "returned count")
        if value["returned_count"] < 0 and count:
            raise ProtocolError("INVALID_FIELD", "negative returned count")
    elif frame.message_type == 4:
        value = {
            "producer_session_id": reader.string(),
            "batch_sequence": reader.u64(),
            "gateway_batch_sha256": reader.hash32(),
            "gateway_ingest_sequence": reader.u64(),
            "status": reader.u8(),
            "committed_cursor_msc": reader.i64(),
            "committed_boundary_digest": reader.hash32(),
            "next_from_msc": reader.i64(),
            "next_requested_count": reader.u32(),
            "retry_after_ms": reader.u32(),
        }
        if not 1 <= value["status"] <= 9:
            raise ProtocolError("INVALID_FIELD", "ack status")
    elif frame.message_type == 5:
        value = {
            "code": reader.u16(),
            "retryable": reader.u8(),
            "related_message_type": reader.u16(),
            "related_batch_sequence": reader.u64(),
            "message": reader.string(),
        }
        if not 1 <= value["code"] <= 10:
            raise ProtocolError("INVALID_FIELD", "error code")
        if value["retryable"] > 1 or value["related_message_type"] > 5:
            raise ProtocolError("INVALID_FIELD", "error field")
    else:
        raise ProtocolError("UNKNOWN_MESSAGE_TYPE", str(frame.message_type))
    reader.done()
    return value


def _hash_domain(prefix: bytes, *parts: bytes) -> bytes:
    return hashlib.sha256(prefix + b"".join(parts)).digest()


def _lp(value: str) -> bytes:
    data = value.encode("utf-8")
    if len(data) > MAX_STRING_BYTES:
        raise ProtocolError("INVALID_FIELD", "string length")
    return struct.pack("<H", len(data)) + data


def _path(value: str) -> bytes:
    data = value.encode("utf-8")
    if len(data) > MAX_PATH_BYTES:
        raise ProtocolError("INVALID_FIELD", "path length")
    return struct.pack("<I", len(data)) + data


def source_payload_fingerprint(record: dict[str, int]) -> bytes:
    return _hash_domain(
        b"tick-data-platform/source-payload/v1\0",
        _lp(SOURCE_SCHEMA_MT5),
        struct.pack(
            "<qQQQQqIQ",
            record["time"],
            record["bid_bits"],
            record["ask_bits"],
            record["last_bits"],
            record["volume"],
            record["time_msc"],
            record["flags"],
            record["volume_real_bits"],
        ),
    )


def observation_hash(
    producer_instance_id: str,
    producer_session_id: str,
    batch_sequence: int,
    record_ordinal: int,
    capture_sequence: int,
    source_hash: bytes,
) -> bytes:
    return _hash_domain(
        b"tick-data-platform/observation/v1\0",
        _lp(producer_instance_id),
        _lp(producer_session_id),
        struct.pack("<QI", batch_sequence, record_ordinal),
        struct.pack("<Q", capture_sequence),
        source_hash,
    )


def _hash32(value: bytes) -> bytes:
    if len(value) != 32:
        raise ProtocolError("INVALID_FIELD", "hash must be 32 bytes")
    return value


def _replay_common(scope: dict[str, Any], row: dict[str, Any], kind: int) -> bytearray:
    required_scope = {
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "replay_contract_id",
        "conversion_id",
        "raw_day_manifest_sha256",
    }
    if set(scope) != required_scope:
        raise ProtocolError("INVALID_FIELD", "replay scope keys differ")
    result = bytearray(REPLAY_ROW_DOMAIN)
    result.extend(struct.pack("<BQ", kind, row["stream_sequence"]))
    for key in (
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "replay_contract_id",
        "conversion_id",
        "continuity_segment_id",
    ):
        result.extend(_lp(scope[key] if key != "continuity_segment_id" else row[key]))
    result.extend(_hash32(scope["raw_day_manifest_sha256"]))
    result.extend(_lp(row["raw_object_key"]))
    result.extend(_hash32(row["raw_object_sha256"]))
    return result


def canonical_replay_data_row(scope: dict[str, Any], row: dict[str, Any]) -> bytes:
    record = row["record"]
    source_hash = source_payload_fingerprint(record)
    if row["source_payload_fingerprint"] != source_hash:
        raise ProtocolError("INVALID_FIELD", "source payload fingerprint mismatch")
    expected_observation = observation_hash(
        row["producer_instance_id"],
        row["producer_session_id"],
        row["batch_sequence"],
        row["record_ordinal"],
        row["capture_sequence"],
        source_hash,
    )
    if row["observation_hash"] != expected_observation:
        raise ProtocolError("INVALID_FIELD", "observation hash mismatch")
    result = _replay_common(scope, row, REPLAY_ROW_DATA)
    result.extend(
        struct.pack(
            "<Q",
            row["gateway_ingest_sequence"],
        )
    )
    result.extend(_lp(row["producer_instance_id"]))
    result.extend(_lp(row["producer_session_id"]))
    result.extend(
        struct.pack("<QIQ", row["batch_sequence"], row["record_ordinal"], row["capture_sequence"])
    )
    result.extend(
        struct.pack(
            "<qQQQQqIQ",
            record["time"],
            record["bid_bits"],
            record["ask_bits"],
            record["last_bits"],
            record["volume"],
            record["time_msc"],
            record["flags"],
            record["volume_real_bits"],
        )
    )
    result.extend(source_hash)
    result.extend(expected_observation)
    result.extend(
        struct.pack(
            "<qqQQiI",
            row["fetch_wall_start_s"],
            row["fetch_wall_end_s"],
            row["fetch_monotonic_start_us"],
            row["fetch_monotonic_end_us"],
            row["copy_ticks_error"],
            row["source_status_flags"],
        )
    )
    return bytes(result)


def canonical_replay_marker_row(scope: dict[str, Any], row: dict[str, Any]) -> bytes:
    expected_reasons = {
        "SEGMENT_START": "INITIAL",
        "AMBIGUOUS_OVERLAP": "NO_UNIQUE_OVERLAP",
        "SOURCE_HISTORY_CHANGED": "SAME_POSITION_PAYLOAD_CHANGED",
        "SOURCE_ERROR": "SOURCE_REPORTED_ERROR",
        "GAP": "WAL_SEQUENCE_GAP",
        "TIMESTAMP_REGRESSION": "TIME_MSC_REGRESSION",
        "CAMPAIGN_BOUNDARY": "CAMPAIGN_CHANGED",
    }
    if expected_reasons.get(row["marker_code"]) != row["reason"]:
        raise ProtocolError("INVALID_FIELD", "marker code/reason pair")
    result = _replay_common(scope, row, REPLAY_ROW_MARKER)
    result.extend(REPLAY_MARKER_DOMAIN)
    result.extend(_lp(row["marker_code"]))
    result.extend(_lp(row["reason"]))
    result.extend(_lp(row["detail"]))
    result.extend(
        struct.pack(
            "<QI", row["reference_gateway_ingest_sequence"], row["reference_record_ordinal"]
        )
    )
    result.extend(_hash32(row["predecessor_row_chain_hash"]))
    result.extend(_hash32(row["continuity_segment_start_hash"]))
    return bytes(result)


def row_chain_step(stream_sequence: int, previous_hash: bytes, canonical_row: bytes) -> bytes:
    return _hash_domain(
        REPLAY_ROW_CHAIN_DOMAIN,
        struct.pack("<Q", stream_sequence),
        _hash32(previous_hash),
        struct.pack("<I", len(canonical_row)),
        canonical_row,
    )


def segment_id(
    scope: dict[str, Any],
    gateway_sequence: int,
    record_ordinal: int,
    marker_code: str,
    predecessor: bytes,
) -> str:
    result = bytearray(CONTINUITY_SEGMENT_DOMAIN)
    for key in (
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "replay_contract_id",
        "conversion_id",
        "raw_day_manifest_key",
    ):
        result.extend(_lp(scope[key]))
    result.extend(_hash32(scope["raw_day_manifest_sha256"]))
    result.extend(struct.pack("<QI", gateway_sequence, record_ordinal))
    result.extend(_lp(marker_code))
    result.extend(_hash32(predecessor))
    return hashlib.sha256(result).hexdigest()


def exact_identity_path_key(value: str) -> str:
    if not isinstance(value, str):
        raise ProtocolError("INVALID_FIELD", "identity path component")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def replay_derivative_base_key(scope: dict[str, Any]) -> str:
    required = {
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "replay_contract_id",
        "conversion_id",
    }
    if not required.issubset(scope):
        raise ProtocolError("INVALID_FIELD", "replay derivative scope")
    for key in required:
        value = scope[key]
        if not isinstance(value, str) or not value:
            raise ProtocolError("INVALID_FIELD", f"replay derivative {key}")
    try:
        if date_type.fromisoformat(scope["date"]).isoformat() != scope["date"]:
            raise ValueError("noncanonical date")
    except ValueError as exc:
        raise ProtocolError("INVALID_FIELD", "replay derivative date") from exc
    return (
        f"derivatives/stream={exact_identity_path_key(scope['replay_contract_id'])}"
        f"/format=ticks-parquet-v1/conversion={exact_identity_path_key(scope['conversion_id'])}"
        f"/day-definition={exact_identity_path_key(scope['day_definition_id'])}/date={scope['date']}"
    )


def replay_part_object_key(part: dict[str, Any]) -> str:
    if not isinstance(part, dict):
        raise ProtocolError("INVALID_FIELD", "replay part key input")
    try:
        part_hash = bytes.fromhex(part["part_sha256"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError("INVALID_FIELD", "replay part hash") from exc
    _hash32(part_hash)
    if part["last_stream_sequence"] < part["first_stream_sequence"]:
        raise ProtocolError("INVALID_FIELD", "replay part range")
    base = replay_derivative_base_key(part)
    return (
        f"{base}/parquet/{part['first_stream_sequence']}-{part['last_stream_sequence']}-"
        f"{part_hash.hex()}.parquet"
    )


def part_manifest_value(part: dict[str, Any]) -> dict[str, Any]:
    required = {
        "campaign_id",
        "manifest_version",
        "conversion_id",
        "converter_build_id",
        "dataset_id",
        "date",
        "day_definition_id",
        "dependency_lock_hash",
        "format_id",
        "part_sequence",
        "part_key",
        "part_sha256",
        "part_bytes",
        "row_count",
        "canonical_row_bytes",
        "first_stream_sequence",
        "last_stream_sequence",
        "first_row_chain_hash",
        "last_row_chain_hash",
        "previous_manifest_sha256",
        "previous_row_chain_hash",
        "raw_day_manifest_key",
        "raw_day_manifest_sha256",
        "replay_contract_id",
        "target_platform_contract",
        "writer_configuration_hash",
    }
    if set(part) != required:
        raise ProtocolError("INVALID_FIELD", "part manifest keys differ")
    if part["manifest_version"] != "part-manifest-v1" or part["part_key"] != replay_part_object_key(
        part
    ):
        raise ProtocolError("INVALID_FIELD", "part manifest identity")
    for key in (
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "replay_contract_id",
        "conversion_id",
        "converter_build_id",
        "target_platform_contract",
        "raw_day_manifest_key",
    ):
        if (
            not isinstance(part[key], str)
            or not part[key]
            or len(part[key].encode("utf-8")) > MAX_STRING_BYTES
        ):
            raise ProtocolError("INVALID_FIELD", f"part manifest {key}")
    try:
        if date_type.fromisoformat(part["date"]).isoformat() != part["date"]:
            raise ValueError("date is not canonical")
    except ValueError as exc:
        raise ProtocolError("INVALID_FIELD", "part manifest date") from exc
    raw_key = part["raw_day_manifest_key"]
    if "//" in raw_key or any(character in raw_key for character in "\\\r\n"):
        raise ProtocolError("INVALID_FIELD", "part manifest raw key")
    if any(component in {"", ".", ".."} for component in raw_key.split("/")):
        raise ProtocolError("INVALID_FIELD", "part manifest raw key")
    if part["format_id"] != "ticks-parquet-v1":
        raise ProtocolError("INVALID_FIELD", "part manifest format")
    if (
        part["row_count"] <= 0
        or part["last_stream_sequence"] < part["first_stream_sequence"]
        or part["last_stream_sequence"] - part["first_stream_sequence"] != part["row_count"] - 1
    ):
        raise ProtocolError("INVALID_FIELD", "part manifest range")
    if part["part_bytes"] <= 0 or part["canonical_row_bytes"] <= 0:
        raise ProtocolError("INVALID_FIELD", "part manifest sizes")

    def parse_hash(key: str, value: Any) -> bytes:
        if isinstance(value, bytes):
            data = value
        elif isinstance(value, str) and len(value) == 64 and value == value.lower():
            try:
                data = bytes.fromhex(value)
            except ValueError as exc:
                raise ProtocolError("INVALID_FIELD", f"part manifest {key}") from exc
        else:
            raise ProtocolError("INVALID_FIELD", f"part manifest {key}")
        return _hash32(data)

    hashes = {
        key: parse_hash(key, part[key])
        for key in (
            "part_sha256",
            "first_row_chain_hash",
            "last_row_chain_hash",
            "dependency_lock_hash",
            "writer_configuration_hash",
            "raw_day_manifest_sha256",
            "previous_row_chain_hash",
        )
    }
    previous_manifest = part["previous_manifest_sha256"]
    previous_manifest_hex = None
    if previous_manifest is not None:
        previous_manifest_bytes = parse_hash("previous_manifest_sha256", previous_manifest)
        if previous_manifest_bytes == bytes(32):
            raise ProtocolError("INVALID_FIELD", "part manifest predecessor is zero")
        previous_manifest_hex = previous_manifest_bytes.hex()
    for key, value in hashes.items():
        if key != "previous_row_chain_hash" and value == bytes(32):
            raise ProtocolError("INVALID_FIELD", "part manifest hash is zero")
    if hashes["previous_row_chain_hash"] == bytes(32) and part["part_sequence"] > 0:
        raise ProtocolError("INVALID_FIELD", "successor row-chain predecessor")
    if part["part_sequence"] == 0 and hashes["previous_row_chain_hash"] != bytes(32):
        raise ProtocolError("INVALID_FIELD", "first part row-chain predecessor")
    if part["part_sequence"] == 0 and part["previous_manifest_sha256"] is not None:
        raise ProtocolError("INVALID_FIELD", "first part predecessor")
    if part["part_sequence"] > 0 and part["previous_manifest_sha256"] is None:
        raise ProtocolError("INVALID_FIELD", "part predecessor missing")
    normalized = dict(part)
    for key, value in hashes.items():
        normalized[key] = value.hex()
    normalized["previous_manifest_sha256"] = previous_manifest_hex
    return normalized


def part_manifest_canonical_json(part: dict[str, Any]) -> bytes:
    return canonical_json(part_manifest_value(part)).encode("utf-8")


def part_manifest_digest(part: dict[str, Any]) -> bytes:
    return _hash_domain(PART_MANIFEST_DOMAIN, part_manifest_canonical_json(part))


def part_manifest_key(part: dict[str, Any]) -> str:
    return (
        f"{replay_derivative_base_key(part)}/manifests/part-"
        f"{part['part_sequence']:08d}-{part_manifest_digest(part).hex()}.json"
    )


def part_set_root(parts: list[dict[str, Any]]) -> bytes:
    if not parts:
        return bytes(32)
    result = bytearray(PART_SET_ROOT_DOMAIN)
    result.extend(struct.pack("<I", len(parts)))
    for index, part in enumerate(parts):
        if part["part_sequence"] != index:
            raise ProtocolError("INVALID_FIELD", "part sequence is not contiguous")
        part_manifest_value(part)
        if index > 0:
            previous = parts[index - 1]
            if any(
                part[key] != previous[key]
                for key in (
                    "dataset_id",
                    "campaign_id",
                    "day_definition_id",
                    "date",
                    "replay_contract_id",
                    "format_id",
                    "conversion_id",
                    "converter_build_id",
                    "dependency_lock_hash",
                    "writer_configuration_hash",
                    "target_platform_contract",
                    "raw_day_manifest_key",
                    "raw_day_manifest_sha256",
                )
            ):
                raise ProtocolError("INVALID_FIELD", "part provenance or conversion differs")
            if part["first_stream_sequence"] != previous["last_stream_sequence"] + 1:
                raise ProtocolError("INVALID_FIELD", "part stream ranges are not contiguous")
            if part["previous_row_chain_hash"] != previous["last_row_chain_hash"]:
                raise ProtocolError("INVALID_FIELD", "part row-chain predecessor differs")
            if part["previous_manifest_sha256"] != part_manifest_digest(previous).hex():
                raise ProtocolError("INVALID_FIELD", "part manifest predecessor differs")
        elif part["first_stream_sequence"] != 0:
            raise ProtocolError("INVALID_FIELD", "first part stream start")
        digest = part_manifest_digest(part)
        result.extend(_path(part_manifest_key(part)))
        result.extend(digest)
        result.extend(
            struct.pack("<QQ", part["first_stream_sequence"], part["last_stream_sequence"])
        )
    return hashlib.sha256(result).digest()


def replay_day_manifest_canonical_json(manifest: dict[str, Any]) -> bytes:
    required = {
        "manifest_version",
        "manifest_id",
        "dataset_id",
        "campaign_id",
        "day_definition_id",
        "date",
        "revision",
        "raw_day_manifest_key",
        "raw_day_manifest_sha256",
        "replay_contract_id",
        "format_id",
        "conversion_id",
        "converter_build_id",
        "dependency_lock_hash",
        "writer_configuration_hash",
        "target_platform_contract",
        "completeness_status",
        "part_manifest_keys",
        "part_set_root",
        "canonical_stream_row_chain_root",
        "previous_manifest_sha256",
    }
    if set(manifest) != required or manifest["manifest_version"] != "replay-day-manifest-v1":
        raise ProtocolError("INVALID_FIELD", "replay manifest keys differ")
    if manifest["format_id"] != "ticks-parquet-v1" or manifest["revision"] < 1:
        raise ProtocolError("INVALID_FIELD", "replay manifest identity")
    if manifest["revision"] == 1 and manifest["previous_manifest_sha256"] is not None:
        raise ProtocolError("INVALID_FIELD", "genesis replay predecessor")
    if manifest["revision"] > 1 and manifest["previous_manifest_sha256"] is None:
        raise ProtocolError("INVALID_FIELD", "successor replay predecessor")
    for key in (
        "raw_day_manifest_sha256",
        "dependency_lock_hash",
        "writer_configuration_hash",
        "part_set_root",
        "canonical_stream_row_chain_root",
    ):
        _require_publication_hash(
            key,
            manifest[key],
            allow_zero=key in {"part_set_root", "canonical_stream_row_chain_root"},
        )
    if manifest["previous_manifest_sha256"] is not None:
        _require_publication_hash("previous_manifest_sha256", manifest["previous_manifest_sha256"])
    base = replay_derivative_base_key(manifest)
    if not isinstance(manifest["part_manifest_keys"], list):
        raise ProtocolError("INVALID_FIELD", "replay part manifest keys")
    zero = "00" * 32
    if not manifest["part_manifest_keys"]:
        if manifest["part_set_root"] != zero or manifest["canonical_stream_row_chain_root"] != zero:
            raise ProtocolError("INVALID_FIELD", "empty replay manifest roots")
    elif manifest["part_set_root"] == zero or manifest["canonical_stream_row_chain_root"] == zero:
        raise ProtocolError("INVALID_FIELD", "non-empty replay manifest roots")
    seen: set[str] = set()
    for key in manifest["part_manifest_keys"]:
        if (
            not isinstance(key, str)
            or not key.startswith(base + "/manifests/part-")
            or not key.endswith(".json")
        ):
            raise ProtocolError("INVALID_FIELD", "replay part manifest key")
        suffix = key[len(base + "/manifests/part-") : -len(".json")]
        if (
            len(suffix) != 8 + 1 + 64
            or suffix[8] != "-"
            or not suffix[:8].isdigit()
            or any(c not in "0123456789abcdef" for c in suffix[9:])
        ):
            raise ProtocolError("INVALID_FIELD", "replay part manifest key shape")
        if key in seen:
            raise ProtocolError("INVALID_FIELD", "duplicate replay part manifest key")
        seen.add(key)
    return canonical_json(manifest).encode("utf-8")


def replay_day_manifest_digest(manifest: dict[str, Any]) -> bytes:
    return _hash_domain(REPLAY_MANIFEST_DOMAIN, replay_day_manifest_canonical_json(manifest))


def replay_day_manifest_key(manifest: dict[str, Any]) -> str:
    return (
        f"{replay_derivative_base_key(manifest)}/replay-day-"
        f"{manifest['revision']}-{replay_day_manifest_digest(manifest).hex()}.json"
    )


_BUNDLE_KEYS = {
    "bundle_version",
    "canonical_stream_row_chain_root",
    "claim",
    "conversion",
    "limits",
    "parquet_objects",
    "part_manifests",
    "part_set_root",
    "raw_manifest",
    "raw_objects",
    "replay_manifest",
    "rclone_identity",
    "scope",
}
_CLAIM_KEYS = {"canonical_json", "domain_digest", "full_key"}
_CONVERSION_KEYS = {
    "conversion_id",
    "converter_build_id",
    "dependency_lock_hash",
    "format_id",
    "max_canonical_bytes_per_part",
    "max_rows_per_part",
    "max_rows_per_row_group",
    "replay_contract_id",
    "target_platform_contract",
    "writer_configuration_hash",
}
_LIMIT_KEYS = set(REPLAY_PUBLICATION_IMPLEMENTATION_BOUNDS)
_RAW_MANIFEST_KEYS = {
    "bytes",
    "domain_digest",
    "full_key",
    "relative_key",
    "rclone_key",
    "revision",
}
_RAW_OBJECT_KEYS = {"bytes", "full_key", "relative_key", "rclone_key", "sha256"}
_PARQUET_OBJECT_KEYS = {
    "bytes",
    "first_stream_sequence",
    "full_key",
    "last_stream_sequence",
    "object_id",
    "relative_key",
    "rclone_key",
    "sha256",
}
_PART_PUBLICATION_KEYS = {
    "bytes",
    "domain_digest",
    "full_key",
    "object_id",
    "part_sequence",
    "relative_key",
    "rclone_key",
}
_REPLAY_PUBLICATION_MANIFEST_KEYS = {
    "bytes",
    "domain_digest",
    "full_key",
    "relative_key",
    "rclone_key",
    "revision",
}
_RCLONE_IDENTITY_KEYS = {"binary_sha256", "goarch", "goos", "version"}
_PUBLICATION_SCOPE_KEYS = {
    "broker_server_fingerprint",
    "campaign_id",
    "dataset_id",
    "date",
    "day_definition_id",
    "exact_source_symbol",
    "immutable_prefix",
    "provider_id",
    "publisher_epoch",
    "publisher_id",
    "rclone_prefix",
    "scope_config_hash",
    "scope_key",
    "settle_policy",
    "stable_feed_id",
}
_FINAL_OBSERVATION_KEYS = {
    "bundle_digest",
    "claim",
    "complete",
    "derivative_objects",
    "observation_bytes",
    "observation_requests",
    "observation_version",
    "raw_manifest",
    "raw_objects",
    "replay_edges",
}
_OBSERVED_RAW_MANIFEST_KEYS = {"bytes", "domain_digest", "full_key"}
_OBSERVED_RAW_OBJECT_KEYS = {"bytes", "full_key", "sha256"}
_OBSERVED_DERIVATIVE_KEYS = {"bytes", "digest", "digest_domain", "full_key", "kind"}
_REPLAY_EDGE_KEYS = {
    "canonical_json",
    "canonical_stream_row_chain_root",
    "full_key",
    "manifest_digest",
    "part_count",
    "part_set_root",
    "previous_manifest_digest",
    "revision",
}
_PUBLISHER_CLAIM_KEYS = {
    "broker_server_fingerprint",
    "campaign_id",
    "claim_version",
    "config_hash",
    "dataset_id",
    "day_definition_id",
    "exact_source_symbol",
    "provider_id",
    "publisher_epoch",
    "publisher_id",
    "scope_key",
    "settle_policy",
    "stable_feed_id",
}
_HANDOVER_ARTIFACT_KEYS = {
    "campaign_id",
    "dataset_id",
    "expected_next_claim_domain_digest",
    "expected_next_claim_key",
    "handover_version",
    "next_publisher_epoch",
    "operator_evidence_digest",
    "prior_claim_domain_digest",
    "prior_claim_key",
    "prior_publisher_epoch",
    "scope_key",
    "transition_key",
}


def _publication_exact_object(value: Any, expected: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError("INVALID_FIELD", "publication value must be an object")
    unknown = set(value) - expected
    if unknown:
        raise ProtocolError("UNKNOWN_FIELD", f"unknown publication key {sorted(unknown)[0]!r}")
    if set(value) != expected:
        raise ProtocolError("INVALID_FIELD", "publication object is missing a required key")
    return value


def _decode_publication_canonical_json(data: bytes) -> Any:
    try:
        return decode_canonical_json(data)
    except ValueError as exc:
        code = (
            "DUPLICATE_FIELD"
            if "duplicate JSON object key" in str(exc)
            else "INVALID_CANONICAL_JSON"
        )
        raise ProtocolError(code, str(exc)) from exc


def _require_publication_string(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > MAX_STRING_BYTES:
        raise ProtocolError("INVALID_FIELD", f"{name} is not a Protocol V1 string")
    return value


def _require_publication_uint(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > (1 << 64) - 1:
        raise ProtocolError("INVALID_FIELD", f"{name} is not a U64")
    return value


def _require_publication_hash(name: str, value: Any, allow_zero: bool = False) -> bytes:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ProtocolError("INVALID_FIELD", f"{name} is not lowercase SHA-256")
    decoded = bytes.fromhex(value)
    if not allow_zero and decoded == bytes(32):
        raise ProtocolError("ZERO_DIGEST", f"{name} is zero")
    return decoded


def _validate_publication_prefix(name: str, value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 4096
        or value.endswith("/")
        or "\r" in value
        or "\n" in value
    ):
        raise ProtocolError("WRONG_KEY", f"{name} is not a canonical prefix")
    return value


def _validate_handover_full_key(name: str, value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_PATH_BYTES
        or value.startswith("/")
        or "\\" in value
        or "\r" in value
        or "\n" in value
        or "//" in value
    ):
        raise ProtocolError("WRONG_KEY", f"{name} is not a canonical full key")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ProtocolError("WRONG_KEY", f"{name} contains a forbidden key component")
    return value


def handover_artifact_key(immutable_prefix: str, next_epoch: int) -> str:
    prefix = _validate_publication_prefix("immutable_prefix", immutable_prefix)
    if isinstance(next_epoch, bool) or not isinstance(next_epoch, int) or next_epoch <= 0:
        raise ProtocolError("INVALID_FIELD", "next publisher epoch is invalid")
    return f"{prefix}/handover/next-epoch={next_epoch}.json"


def handover_transition_key(immutable_prefix: str, next_epoch: int) -> str:
    prefix = _validate_publication_prefix("immutable_prefix", immutable_prefix)
    if isinstance(next_epoch, bool) or not isinstance(next_epoch, int) or next_epoch <= 0:
        raise ProtocolError("INVALID_FIELD", "next publisher epoch is invalid")
    return f"{prefix}/handover-transitions/next-epoch={next_epoch}.json"


def _validate_handover_artifact(value: Any) -> dict[str, Any]:
    artifact = _publication_exact_object(value, _HANDOVER_ARTIFACT_KEYS)
    if artifact["handover_version"] != HANDOVER_ARTIFACT_VERSION:
        raise ProtocolError("INVALID_FIELD", "invalid handover version")
    for name in ("dataset_id", "campaign_id"):
        _require_publication_string(name, artifact[name])
    prior_epoch = _require_publication_uint(
        "prior_publisher_epoch", artifact["prior_publisher_epoch"]
    )
    next_epoch = _require_publication_uint("next_publisher_epoch", artifact["next_publisher_epoch"])
    if prior_epoch == 0 or next_epoch == 0 or next_epoch <= prior_epoch:
        raise ProtocolError("INVALID_FIELD", "handover epochs must increase strictly")
    _require_publication_hash("scope_key", artifact["scope_key"])
    for name in (
        "prior_claim_domain_digest",
        "expected_next_claim_domain_digest",
        "operator_evidence_digest",
    ):
        _require_publication_hash(name, artifact[name])
    prior_key = _validate_handover_full_key("prior_claim_key", artifact["prior_claim_key"])
    next_key = _validate_handover_full_key(
        "expected_next_claim_key", artifact["expected_next_claim_key"]
    )
    transition_key = _validate_handover_full_key("transition_key", artifact["transition_key"])
    if not prior_key.endswith(f"/publisher-claims/epoch={prior_epoch}.json"):
        raise ProtocolError("WRONG_KEY", "prior claim key does not bind prior epoch")
    if not next_key.endswith(f"/publisher-claims/epoch={next_epoch}.json"):
        raise ProtocolError("WRONG_KEY", "expected next claim key does not bind next epoch")
    if not transition_key.endswith(f"/handover-transitions/next-epoch={next_epoch}.json"):
        raise ProtocolError("WRONG_KEY", "transition key does not bind next epoch")
    return artifact


def handover_artifact_canonical_json(artifact: dict[str, Any]) -> bytes:
    _validate_handover_artifact(artifact)
    return canonical_json(artifact).encode("utf-8")


def handover_artifact_digest(artifact: dict[str, Any]) -> bytes:
    return hashlib.sha256(
        HANDOVER_ARTIFACT_DOMAIN + handover_artifact_canonical_json(artifact)
    ).digest()


def verify_handover_artifact(data: bytes) -> tuple[dict[str, Any], bytes]:
    value = _decode_publication_canonical_json(data)
    _validate_handover_artifact(value)
    if handover_artifact_canonical_json(value) != data:
        raise ProtocolError("INVALID_CANONICAL_JSON", "handover bytes are not canonical")
    return value, hashlib.sha256(HANDOVER_ARTIFACT_DOMAIN + data).digest()


def verify_handover_artifact_binding(
    data: bytes, immutable_prefix: str, expected_scope_key: str, expected_artifact_key: str
) -> tuple[dict[str, Any], bytes]:
    artifact, digest = verify_handover_artifact(data)
    prefix = _validate_publication_prefix("immutable_prefix", immutable_prefix)
    expected_scope = _require_publication_hash("expected scope_key", expected_scope_key)
    if expected_scope != bytes.fromhex(artifact["scope_key"]):
        raise ProtocolError("SCOPE_COLLISION", "handover scope key differs from trusted scope")
    want_artifact_key = handover_artifact_key(prefix, artifact["next_publisher_epoch"])
    if expected_artifact_key != want_artifact_key:
        raise ProtocolError(
            "WRONG_KEY", "expected handover artifact key is not the trusted derivation"
        )
    want_prior = f"{prefix}/publisher-claims/epoch={artifact['prior_publisher_epoch']}.json"
    want_next = f"{prefix}/publisher-claims/epoch={artifact['next_publisher_epoch']}.json"
    want_transition = handover_transition_key(prefix, artifact["next_publisher_epoch"])
    if (
        artifact["prior_claim_key"] != want_prior
        or artifact["expected_next_claim_key"] != want_next
        or artifact["transition_key"] != want_transition
    ):
        raise ProtocolError("WRONG_KEY", "handover keys are outside the trusted campaign prefix")
    return artifact, digest


def _validate_publication_relative_key(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_PATH_BYTES
        or value.startswith("/")
        or "\\" in value
        or "\r" in value
        or "\n" in value
        or "//" in value
        or any(component in {"", ".", ".."} for component in value.split("/"))
    ):
        raise ProtocolError("WRONG_KEY", "relative key is not canonical")
    return value


def _validate_publication_keys(scope: dict[str, Any], item: dict[str, Any]) -> None:
    relative = _validate_publication_relative_key(item["relative_key"])
    if item["full_key"] != f"{scope['immutable_prefix']}/{relative}" or item["rclone_key"] != (
        f"{scope['rclone_prefix']}/{relative}"
    ):
        raise ProtocolError("WRONG_KEY", "full or rclone key is not the trusted-prefix derivation")


def _checked_publication_total(total: int, next_value: int, limit: int) -> int:
    if next_value > limit or total > limit - next_value:
        raise ProtocolError("RESOURCE_LIMIT", "aggregate byte total exceeds limit")
    return total + next_value


def _validate_publication_scope(scope: Any) -> dict[str, Any]:
    scope = _publication_exact_object(scope, _PUBLICATION_SCOPE_KEYS)
    for key in (
        "broker_server_fingerprint",
        "campaign_id",
        "dataset_id",
        "day_definition_id",
        "exact_source_symbol",
        "provider_id",
        "publisher_id",
        "scope_key",
        "settle_policy",
        "stable_feed_id",
    ):
        _require_publication_string(key, scope[key])
    try:
        if date_type.fromisoformat(scope["date"]).isoformat() != scope["date"]:
            raise ValueError("noncanonical date")
    except (TypeError, ValueError) as exc:
        raise ProtocolError("INVALID_FIELD", "scope date is not UTC YYYY-MM-DD") from exc
    _require_publication_uint("publisher_epoch", scope["publisher_epoch"])
    _require_publication_hash("scope_config_hash", scope["scope_config_hash"])
    _validate_publication_prefix("immutable_prefix", scope["immutable_prefix"])
    _validate_publication_prefix("rclone_prefix", scope["rclone_prefix"])
    return scope


def _validate_publication_claim(claim: Any, scope: dict[str, Any]) -> dict[str, Any]:
    claim = _publication_exact_object(claim, _CLAIM_KEYS)
    if not claim["canonical_json"] or not claim["domain_digest"] or not claim["full_key"]:
        raise ProtocolError("RAW_CLAIM_MISSING", "M2 publisher claim is missing")
    _require_publication_hash("claim domain_digest", claim["domain_digest"])
    expected_digest = hashlib.sha256(
        PUBLISHER_CLAIM_DOMAIN + claim["canonical_json"].encode("utf-8")
    ).hexdigest()
    if claim["domain_digest"] != expected_digest:
        raise ProtocolError("WRONG_DOMAIN", "publisher claim domain digest mismatch")
    expected_key = (
        f"{scope['immutable_prefix']}/publisher-claims/epoch={scope['publisher_epoch']}.json"
    )
    if claim["full_key"] != expected_key:
        raise ProtocolError("WRONG_KEY", "publisher claim key mismatch")
    claim_value = _publication_exact_object(
        _decode_publication_canonical_json(claim["canonical_json"].encode("utf-8")),
        _PUBLISHER_CLAIM_KEYS,
    )
    expected_strings = {
        "broker_server_fingerprint": scope["broker_server_fingerprint"],
        "campaign_id": scope["campaign_id"],
        "claim_version": "publisher-claim-v1",
        "config_hash": scope["scope_config_hash"],
        "dataset_id": scope["dataset_id"],
        "day_definition_id": scope["day_definition_id"],
        "exact_source_symbol": scope["exact_source_symbol"],
        "provider_id": scope["provider_id"],
        "publisher_id": scope["publisher_id"],
        "scope_key": scope["scope_key"],
        "settle_policy": scope["settle_policy"],
        "stable_feed_id": scope["stable_feed_id"],
    }
    for key, expected in expected_strings.items():
        if claim_value[key] != expected:
            raise ProtocolError(
                "SCOPE_COLLISION", f"publisher claim {key} differs from bundle scope"
            )
    if claim_value["publisher_epoch"] != scope["publisher_epoch"]:
        raise ProtocolError("SCOPE_COLLISION", "publisher claim epoch differs from bundle scope")
    return claim


def _validate_publication_conversion(conversion: Any) -> dict[str, Any]:
    conversion = _publication_exact_object(conversion, _CONVERSION_KEYS)
    for key in (
        "conversion_id",
        "converter_build_id",
        "format_id",
        "replay_contract_id",
        "target_platform_contract",
    ):
        _require_publication_string(key, conversion[key])
    if conversion["format_id"] != "ticks-parquet-v1":
        raise ProtocolError("INVALID_FIELD", "conversion format is not ticks-parquet-v1")
    _require_publication_hash("dependency_lock_hash", conversion["dependency_lock_hash"])
    _require_publication_hash("writer_configuration_hash", conversion["writer_configuration_hash"])
    for key in (
        "max_canonical_bytes_per_part",
        "max_rows_per_part",
        "max_rows_per_row_group",
    ):
        if _require_publication_uint(key, conversion[key]) == 0:
            raise ProtocolError("INVALID_FIELD", "conversion limits are zero")
    if conversion["max_rows_per_row_group"] > conversion["max_rows_per_part"]:
        raise ProtocolError("INVALID_FIELD", "conversion row group exceeds part")
    return conversion


def _validate_publication_limits(limits: Any) -> dict[str, Any]:
    limits = _publication_exact_object(limits, _LIMIT_KEYS)
    for key, bound in REPLAY_PUBLICATION_IMPLEMENTATION_BOUNDS.items():
        value = _require_publication_uint(key, limits[key])
        if value == 0 or value > bound:
            raise ProtocolError("RESOURCE_LIMIT", f"{key} is zero or exceeds implementation bound")
    if (
        limits["max_metadata_object_bytes"] > limits["max_total_metadata_bytes"]
        or limits["max_parquet_object_bytes"] > limits["max_total_parquet_bytes"]
        or limits["max_total_parquet_bytes"] > limits["max_observation_bytes"]
    ):
        raise ProtocolError("RESOURCE_LIMIT", "resource limit relationship is invalid")
    return limits


def _publication_replay_scope(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": bundle["scope"]["dataset_id"],
        "campaign_id": bundle["scope"]["campaign_id"],
        "day_definition_id": bundle["scope"]["day_definition_id"],
        "date": bundle["scope"]["date"],
        "replay_contract_id": bundle["conversion"]["replay_contract_id"],
        "conversion_id": bundle["conversion"]["conversion_id"],
        "raw_day_manifest_key": bundle["raw_manifest"]["relative_key"],
        "raw_day_manifest_sha256": bundle["raw_manifest"]["domain_digest"],
    }


def publication_bundle_value(bundle: Any) -> dict[str, Any]:
    bundle = _publication_exact_object(bundle, _BUNDLE_KEYS)
    if bundle["bundle_version"] != REPLAY_PUBLICATION_BUNDLE_VERSION:
        raise ProtocolError("INVALID_FIELD", "invalid bundle version")
    scope = _validate_publication_scope(bundle["scope"])
    _validate_publication_claim(bundle["claim"], scope)
    _validate_publication_conversion(bundle["conversion"])
    limits = _validate_publication_limits(bundle["limits"])
    part_set_root = _require_publication_hash("part_set_root", bundle["part_set_root"], True)
    row_chain_root = _require_publication_hash(
        "canonical_stream_row_chain_root", bundle["canonical_stream_row_chain_root"], True
    )
    raw_manifest = _publication_exact_object(bundle["raw_manifest"], _RAW_MANIFEST_KEYS)
    if (
        _require_publication_uint("raw manifest bytes", raw_manifest["bytes"]) == 0
        or raw_manifest["bytes"] > limits["max_metadata_object_bytes"]
    ):
        raise ProtocolError("RESOURCE_LIMIT", "raw manifest bytes exceed metadata object bound")
    if _require_publication_uint("raw manifest revision", raw_manifest["revision"]) == 0:
        raise ProtocolError("INVALID_FIELD", "raw manifest revision is zero")
    _require_publication_hash("raw manifest domain_digest", raw_manifest["domain_digest"])
    _validate_publication_keys(scope, raw_manifest)
    raw_objects = bundle["raw_objects"]
    parquet_objects = bundle["parquet_objects"]
    part_manifests = bundle["part_manifests"]
    if not all(isinstance(items, list) for items in (raw_objects, parquet_objects, part_manifests)):
        raise ProtocolError("INVALID_FIELD", "bundle inventories must be arrays")
    if len(parquet_objects) != len(part_manifests):
        raise ProtocolError("SCOPE_COLLISION", "Parquet and part manifest counts differ")
    required_rounds = 2 * len(part_manifests) + 2
    if limits["max_publication_rounds"] < required_rounds:
        raise ProtocolError(
            "RESOURCE_LIMIT", "publication round budget is too small for the bundle"
        )
    if len(part_manifests) > limits["max_parts"]:
        raise ProtocolError("RESOURCE_LIMIT", "part count exceeds limit")
    if len(part_manifests) + 1 > limits["max_graph_nodes"]:
        raise ProtocolError("RESOURCE_LIMIT", "graph node count exceeds limit")
    if len(parquet_objects) + len(part_manifests) + 1 > limits["max_list_objects"]:
        raise ProtocolError("RESOURCE_LIMIT", "derivative inventory exceeds list limit")
    claim_bytes = len(bundle["claim"]["canonical_json"].encode("utf-8"))
    metadata_total = _checked_publication_total(
        claim_bytes,
        raw_manifest["bytes"],
        limits["max_total_metadata_bytes"],
    )
    observation_total = _checked_publication_total(
        claim_bytes,
        raw_manifest["bytes"],
        limits["max_observation_bytes"],
    )
    previous_raw_key = ""
    for item in raw_objects:
        item = _publication_exact_object(item, _RAW_OBJECT_KEYS)
        if _require_publication_uint("raw object bytes", item["bytes"]) == 0:
            raise ProtocolError("INVALID_FIELD", "raw object bytes are zero")
        digest = _require_publication_hash("raw object sha256", item["sha256"])
        if item["relative_key"] != f"objects/raw/wal-{digest.hex()}.rtw":
            raise ProtocolError("WRONG_KEY", "raw object relative key mismatch")
        _validate_publication_keys(scope, item)
        if item["full_key"] <= previous_raw_key:
            raise ProtocolError("INVALID_FIELD", "raw objects are not key-sorted")
        previous_raw_key = item["full_key"]
        observation_total = _checked_publication_total(
            observation_total, item["bytes"], limits["max_observation_bytes"]
        )
    seen_object_ids: set[str] = set()
    parquet_total = 0
    replay_scope = _publication_replay_scope(bundle)
    for item in parquet_objects:
        item = _publication_exact_object(item, _PARQUET_OBJECT_KEYS)
        object_id = _require_publication_string("Parquet object_id", item["object_id"])
        if object_id in seen_object_ids:
            raise ProtocolError("INVALID_FIELD", "duplicate bundle object ID")
        seen_object_ids.add(object_id)
        bytes_value = _require_publication_uint("Parquet bytes", item["bytes"])
        if bytes_value == 0 or bytes_value > limits["max_parquet_object_bytes"]:
            raise ProtocolError("RESOURCE_LIMIT", "Parquet object bytes exceed limit")
        first = _require_publication_uint("first stream sequence", item["first_stream_sequence"])
        last = _require_publication_uint("last stream sequence", item["last_stream_sequence"])
        if last < first:
            raise ProtocolError("INVALID_FIELD", "Parquet stream range is reversed")
        digest = _require_publication_hash("Parquet sha256", item["sha256"])
        expected_relative = (
            f"{replay_derivative_base_key(replay_scope)}/parquet/"
            f"{first}-{last}-{digest.hex()}.parquet"
        )
        if item["relative_key"] != expected_relative:
            raise ProtocolError("WRONG_KEY", "Parquet object relative key mismatch")
        _validate_publication_keys(scope, item)
        parquet_total = _checked_publication_total(
            parquet_total, bytes_value, limits["max_total_parquet_bytes"]
        )
        observation_total = _checked_publication_total(
            observation_total, bytes_value, limits["max_observation_bytes"]
        )
    for index, item in enumerate(part_manifests):
        item = _publication_exact_object(item, _PART_PUBLICATION_KEYS)
        if item["part_sequence"] != index:
            raise ProtocolError("INVALID_FIELD", "part manifest sequence is not contiguous")
        object_id = _require_publication_string("part manifest object_id", item["object_id"])
        if object_id in seen_object_ids:
            raise ProtocolError("INVALID_FIELD", "duplicate bundle object ID")
        seen_object_ids.add(object_id)
        bytes_value = _require_publication_uint("part manifest bytes", item["bytes"])
        if bytes_value == 0 or bytes_value > limits["max_metadata_object_bytes"]:
            raise ProtocolError("RESOURCE_LIMIT", "part manifest bytes exceed limit")
        digest = _require_publication_hash("part manifest domain_digest", item["domain_digest"])
        expected_relative = (
            f"{replay_derivative_base_key(replay_scope)}/manifests/part-{index:08d}-"
            f"{digest.hex()}.json"
        )
        if item["relative_key"] != expected_relative:
            raise ProtocolError("WRONG_KEY", "part manifest relative key mismatch")
        _validate_publication_keys(scope, item)
        metadata_total = _checked_publication_total(
            metadata_total, bytes_value, limits["max_total_metadata_bytes"]
        )
        observation_total = _checked_publication_total(
            observation_total, bytes_value, limits["max_observation_bytes"]
        )
    replay_manifest = _publication_exact_object(
        bundle["replay_manifest"], _REPLAY_PUBLICATION_MANIFEST_KEYS
    )
    replay_bytes = _require_publication_uint("replay manifest bytes", replay_manifest["bytes"])
    if replay_bytes == 0 or replay_bytes > limits["max_metadata_object_bytes"]:
        raise ProtocolError("RESOURCE_LIMIT", "replay manifest bytes exceed limit")
    revision = _require_publication_uint("replay manifest revision", replay_manifest["revision"])
    if revision == 0:
        raise ProtocolError("INVALID_FIELD", "replay manifest revision is zero")
    if revision > limits["max_graph_nodes"]:
        raise ProtocolError("RESOURCE_LIMIT", "replay revision exceeds graph node limit")
    replay_digest = _require_publication_hash(
        "replay manifest domain_digest", replay_manifest["domain_digest"]
    )
    expected_replay_relative = (
        f"{replay_derivative_base_key(replay_scope)}/replay-day-{revision}-"
        f"{replay_digest.hex()}.json"
    )
    if replay_manifest["relative_key"] != expected_replay_relative:
        raise ProtocolError("WRONG_KEY", "replay manifest relative key mismatch")
    _validate_publication_keys(scope, replay_manifest)
    metadata_total = _checked_publication_total(
        metadata_total, replay_bytes, limits["max_total_metadata_bytes"]
    )
    observation_total = _checked_publication_total(
        observation_total, replay_bytes, limits["max_observation_bytes"]
    )
    if len(part_manifests) == 0:
        if part_set_root != bytes(32) or row_chain_root != bytes(32):
            raise ProtocolError("ZERO_DIGEST", "empty bundle roots are inconsistent")
    elif part_set_root == bytes(32) or row_chain_root == bytes(32):
        raise ProtocolError("ZERO_DIGEST", "non-empty bundle root is zero")
    minimum_requests = 3 + len(raw_objects) + len(parquet_objects) + len(part_manifests)
    minimum_requests += revision
    if minimum_requests > limits["max_observation_requests"]:
        raise ProtocolError("RESOURCE_LIMIT", "complete observation request budget is too small")
    rclone = _publication_exact_object(bundle["rclone_identity"], _RCLONE_IDENTITY_KEYS)
    _require_publication_hash("rclone binary_sha256", rclone["binary_sha256"])
    for key in ("goarch", "goos", "version"):
        _require_publication_string(f"rclone {key}", rclone[key])
    return bundle


def publication_bundle_canonical_json(bundle: dict[str, Any]) -> bytes:
    return canonical_json(publication_bundle_value(bundle)).encode("utf-8")


def publication_bundle_digest(bundle: dict[str, Any]) -> bytes:
    return hashlib.sha256(
        REPLAY_PUBLICATION_BUNDLE_DOMAIN + publication_bundle_canonical_json(bundle)
    ).digest()


def verify_publication_bundle(data: bytes) -> tuple[dict[str, Any], bytes]:
    value = _decode_publication_canonical_json(data)
    if not isinstance(value, dict) or "claim" not in value:
        raise ProtocolError("RAW_CLAIM_MISSING", "bundle has no M2 publisher claim")
    bundle = publication_bundle_value(value)
    return bundle, publication_bundle_digest(bundle)


def _expected_derivative_observations(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    values = [
        {
            "bytes": item["bytes"],
            "digest": item["sha256"],
            "digest_domain": "sha256",
            "full_key": item["full_key"],
            "kind": "parquet",
        }
        for item in bundle["parquet_objects"]
    ]
    values.extend(
        {
            "bytes": item["bytes"],
            "digest": item["domain_digest"],
            "digest_domain": "part-manifest-v1",
            "full_key": item["full_key"],
            "kind": "part_manifest",
        }
        for item in bundle["part_manifests"]
    )
    values.append(
        {
            "bytes": bundle["replay_manifest"]["bytes"],
            "digest": bundle["replay_manifest"]["domain_digest"],
            "digest_domain": "replay-day-manifest-v1",
            "full_key": bundle["replay_manifest"]["full_key"],
            "kind": "replay_manifest",
        }
    )
    return sorted(values, key=lambda item: item["full_key"].encode("utf-8"))


def final_observation_value(observation: Any, bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = publication_bundle_value(bundle)
    observation = _publication_exact_object(observation, _FINAL_OBSERVATION_KEYS)
    if (
        observation["observation_version"] != REPLAY_FINAL_OBSERVATION_VERSION
        or observation["complete"] is not True
    ):
        raise ProtocolError("INCOMPLETE_OBSERVATION", "final observation is not complete")
    expected_bundle_digest = publication_bundle_digest(bundle).hex()
    if observation["bundle_digest"] != expected_bundle_digest:
        raise ProtocolError("WRONG_DOMAIN", "bundle digest mismatch")
    if observation["claim"] != bundle["claim"]:
        raise ProtocolError("INCOMPLETE_OBSERVATION", "claim is not Exact")
    expected_raw_manifest = {
        "bytes": bundle["raw_manifest"]["bytes"],
        "domain_digest": bundle["raw_manifest"]["domain_digest"],
        "full_key": bundle["raw_manifest"]["full_key"],
    }
    if observation["raw_manifest"] != expected_raw_manifest:
        raise ProtocolError("INCOMPLETE_OBSERVATION", "raw manifest is not Exact")
    expected_raw_objects = [
        {"bytes": item["bytes"], "full_key": item["full_key"], "sha256": item["sha256"]}
        for item in bundle["raw_objects"]
    ]
    if observation["raw_objects"] != expected_raw_objects:
        raise ProtocolError("INCOMPLETE_OBSERVATION", "raw object inventory is not Exact")
    expected_derivatives = _expected_derivative_observations(bundle)
    if observation["derivative_objects"] != expected_derivatives:
        raise ProtocolError(
            "INCOMPLETE_OBSERVATION", "derivative inventory is not Exact or key-sorted"
        )
    edges = observation["replay_edges"]
    if not isinstance(edges, list) or not edges:
        raise ProtocolError("INCOMPLETE_OBSERVATION", "replay graph is empty")
    previous_manifest: dict[str, Any] | None = None
    for index, edge in enumerate(edges):
        edge = _publication_exact_object(edge, _REPLAY_EDGE_KEYS)
        revision = _require_publication_uint("replay edge revision", edge["revision"])
        canonical_text = edge["canonical_json"]
        if not isinstance(canonical_text, str) or not canonical_text:
            raise ProtocolError("INCOMPLETE_OBSERVATION", "replay edge identity is incomplete")
        if not isinstance(edge["full_key"], str) or not edge["full_key"]:
            raise ProtocolError("INCOMPLETE_OBSERVATION", "replay edge identity is incomplete")
        if revision == 0:
            raise ProtocolError("INCOMPLETE_OBSERVATION", "replay edge identity is incomplete")
        try:
            manifest = _decode_publication_canonical_json(canonical_text.encode("utf-8"))
            canonical = replay_day_manifest_canonical_json(manifest)
        except (ProtocolError, UnicodeError, TypeError, ValueError) as exc:
            raise ProtocolError(
                "INVALID_FIELD", "replay edge manifest is invalid or noncanonical"
            ) from exc
        if canonical != canonical_text.encode("utf-8"):
            raise ProtocolError("INVALID_FIELD", "replay edge manifest is noncanonical")
        digest = replay_day_manifest_digest(manifest).hex()
        if edge["manifest_digest"] != digest:
            raise ProtocolError("WRONG_DOMAIN", "replay edge manifest digest mismatch")
        expected_full_key = (
            bundle["scope"]["immutable_prefix"] + "/" + replay_day_manifest_key(manifest)
        )
        if edge["full_key"] != expected_full_key:
            raise ProtocolError("WRONG_KEY", "replay edge manifest key mismatch")
        part_count = _require_publication_uint("replay edge part_count", edge["part_count"])
        if (
            revision != manifest["revision"]
            or part_count != len(manifest["part_manifest_keys"])
            or edge["part_set_root"] != manifest["part_set_root"]
            or edge["canonical_stream_row_chain_root"]
            != manifest["canonical_stream_row_chain_root"]
        ):
            raise ProtocolError(
                "INCOMPLETE_OBSERVATION", "replay edge differs from canonical manifest"
            )
        if edge["previous_manifest_digest"] != manifest["previous_manifest_sha256"]:
            raise ProtocolError(
                "INCOMPLETE_OBSERVATION",
                "replay edge predecessor differs from canonical manifest",
            )
        conversion = bundle["conversion"]
        scope = bundle["scope"]
        if any(
            manifest[key] != expected
            for key, expected in {
                "dataset_id": scope["dataset_id"],
                "campaign_id": scope["campaign_id"],
                "day_definition_id": scope["day_definition_id"],
                "date": scope["date"],
                "replay_contract_id": conversion["replay_contract_id"],
                "format_id": conversion["format_id"],
                "conversion_id": conversion["conversion_id"],
                "converter_build_id": conversion["converter_build_id"],
                "dependency_lock_hash": conversion["dependency_lock_hash"],
                "writer_configuration_hash": conversion["writer_configuration_hash"],
                "target_platform_contract": conversion["target_platform_contract"],
            }.items()
        ):
            raise ProtocolError("SCOPE_COLLISION", "replay edge scope or conversion mismatch")
        if index == 0:
            if revision != 1 or edge["previous_manifest_digest"] is not None:
                raise ProtocolError("INCOMPLETE_OBSERVATION", "genesis replay edge is incomplete")
        else:
            previous = edges[index - 1]
            if (
                revision != previous["revision"] + 1
                or edge["previous_manifest_digest"] != previous["manifest_digest"]
                or previous_manifest is None
                or previous_manifest["manifest_id"] != manifest["manifest_id"]
                or previous_manifest["raw_day_manifest_key"] == manifest["raw_day_manifest_key"]
                or previous_manifest["raw_day_manifest_sha256"]
                == manifest["raw_day_manifest_sha256"]
            ):
                raise ProtocolError("INCOMPLETE_OBSERVATION", "replay edge chain is incomplete")
        previous_manifest = manifest
    last_edge = edges[-1]
    if (
        last_edge["revision"] != bundle["replay_manifest"]["revision"]
        or last_edge["manifest_digest"] != bundle["replay_manifest"]["domain_digest"]
        or last_edge["full_key"] != bundle["replay_manifest"]["full_key"]
        or last_edge["part_count"] != len(bundle["part_manifests"])
        or last_edge["part_set_root"] != bundle["part_set_root"]
        or last_edge["canonical_stream_row_chain_root"] != bundle["canonical_stream_row_chain_root"]
    ):
        raise ProtocolError("INCOMPLETE_OBSERVATION", "final replay edge differs from bundle")
    minimum_requests = 2 + len(expected_raw_objects) + len(expected_derivatives)
    minimum_requests += len(observation["replay_edges"])
    requests = _require_publication_uint(
        "observation_requests", observation["observation_requests"]
    )
    if requests < minimum_requests or requests > bundle["limits"]["max_observation_requests"]:
        raise ProtocolError("RESOURCE_LIMIT", "observation request counter is outside budget")
    minimum_bytes = len(bundle["claim"]["canonical_json"])
    minimum_bytes = _checked_publication_total(
        minimum_bytes,
        expected_raw_manifest["bytes"],
        bundle["limits"]["max_observation_bytes"],
    )
    for item in [*expected_raw_objects, *expected_derivatives]:
        minimum_bytes = _checked_publication_total(
            minimum_bytes, item["bytes"], bundle["limits"]["max_observation_bytes"]
        )
    for edge in edges:
        minimum_bytes = _checked_publication_total(
            minimum_bytes,
            len(edge["full_key"].encode("utf-8")),
            bundle["limits"]["max_observation_bytes"],
        )
        minimum_bytes = _checked_publication_total(
            minimum_bytes,
            len(edge["canonical_json"].encode("utf-8")),
            bundle["limits"]["max_observation_bytes"],
        )
    minimum_bytes = _checked_publication_total(
        minimum_bytes,
        len(canonical_json(observation).encode("utf-8")),
        bundle["limits"]["max_observation_bytes"],
    )
    observed_bytes = _require_publication_uint(
        "observation_bytes", observation["observation_bytes"]
    )
    if observed_bytes < minimum_bytes or observed_bytes > bundle["limits"]["max_observation_bytes"]:
        raise ProtocolError("RESOURCE_LIMIT", "observation byte counter is outside budget")
    return observation


def final_observation_canonical_json(observation: dict[str, Any], bundle: dict[str, Any]) -> bytes:
    return canonical_json(final_observation_value(observation, bundle)).encode("utf-8")


def final_observation_digest(observation: dict[str, Any], bundle: dict[str, Any]) -> bytes:
    return hashlib.sha256(
        REPLAY_FINAL_OBSERVATION_DOMAIN + final_observation_canonical_json(observation, bundle)
    ).digest()


def verify_final_observation(data: bytes, bundle: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    observation = _decode_publication_canonical_json(data)
    value = final_observation_value(observation, bundle)
    return value, final_observation_digest(value, bundle)


def gateway_batch_sha256(frame: bytes) -> bytes:
    return _hash_domain(b"tick-data-platform/batch/v1\0", frame)


def wal_entry_hash(
    sequence: int,
    previous_hash: bytes,
    receive_wall_s: int,
    receive_monotonic_us: int,
    batch_hash: bytes,
    frame: bytes,
) -> bytes:
    return _hash_domain(
        b"tick-data-platform/wal-entry/v1\0",
        struct.pack("<Q", sequence),
        previous_hash,
        struct.pack("<qQ", receive_wall_s, receive_monotonic_us),
        batch_hash,
        frame,
    )


def canonical_json(value: Any) -> str:
    def check(item: Any) -> Any:
        if isinstance(item, bool) or item is None or isinstance(item, str):
            if isinstance(item, str):
                try:
                    item.encode("utf-8", "strict")
                except UnicodeEncodeError as exc:
                    raise ValueError("canonical JSON strings must be valid UTF-8") from exc
            return item
        if isinstance(item, int):
            if item < -(1 << 63) or item > (1 << 64) - 1:
                raise ValueError("canonical JSON integer is outside 64-bit range")
            return item
        if isinstance(item, float):
            raise ValueError("canonical JSON accepts integers only")
        if isinstance(item, list):
            return [check(child) for child in item]
        if isinstance(item, dict):
            for key in item:
                if not isinstance(key, str):
                    raise ValueError("object keys must be strings")
            ordered: dict[str, Any] = {}
            for key in sorted(item, key=lambda child: child.encode("utf-8")):
                try:
                    key.encode("utf-8", "strict")
                except UnicodeEncodeError as exc:
                    raise ValueError("canonical JSON keys must be valid UTF-8") from exc
                ordered[key] = check(item[key])
            return ordered
        raise ValueError(f"unsupported JSON value: {type(item)!r}")

    checked = check(value)
    return json.dumps(
        checked,
        ensure_ascii=True,
        sort_keys=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def decode_canonical_json(data: bytes) -> Any:
    if data.startswith(b"\xef\xbb\xbf"):
        raise ValueError("canonical JSON must not contain a BOM")
    try:
        text = data.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ValueError("canonical JSON is not valid UTF-8") from exc

    def parse_int(token: str) -> int:
        if (
            token == "-0"
            or (len(token) > 1 and token[0] == "0")
            or (len(token) > 2 and token[:2] == "-0")
        ):
            raise ValueError("non-canonical integer")
        value = int(token, 10)
        if value < -(1 << 63) or value > (1 << 64) - 1:
            raise ValueError("canonical JSON integer is outside 64-bit range")
        return value

    def parse_float(_: str) -> Any:
        raise ValueError("canonical JSON accepts integers only")

    def parse_constant(token: str) -> Any:
        raise ValueError(f"invalid JSON constant {token}")

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(
            text,
            parse_int=parse_int,
            parse_float=parse_float,
            parse_constant=parse_constant,
            object_pairs_hook=object_pairs,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid canonical JSON: {exc}") from exc
    if canonical_json(value).encode("utf-8") != data:
        raise ValueError("canonical JSON bytes are not canonical")
    return value


def raw_set_root(objects: list[dict[str, Any]]) -> bytes:
    if len(objects) > 0xFFFFFFFF:
        raise ValueError("raw set has too many ranges")
    payload = bytearray(b"tick-data-platform/raw-set/v1\0")
    payload.extend(struct.pack("<I", len(objects)))
    previous: tuple[int, int] | None = None
    for item in objects:
        required = {
            "key",
            "sha256",
            "bytes",
            "start_ingest_sequence",
            "end_ingest_sequence",
            "first_record_ordinal",
            "last_record_ordinal",
        }
        if set(item) != required:
            raise ValueError("raw set range keys differ")
        if not isinstance(item["key"], str) or not item["key"]:
            raise ValueError("raw set key is empty")
        digest = bytes.fromhex(item["sha256"])
        if len(digest) != 32 or item["bytes"] <= 0:
            raise ValueError("raw set object hash or size is invalid")
        start = (item["start_ingest_sequence"], item["first_record_ordinal"])
        end = (item["end_ingest_sequence"], item["last_record_ordinal"])
        if start[0] == 0 or end < start:
            raise ValueError("raw set range is empty or reversed")
        if previous is not None and start <= previous:
            raise ValueError("raw set ranges are not strictly ascending")
        payload.extend(
            struct.pack(
                "<32sQQQII",
                digest,
                item["bytes"],
                item["start_ingest_sequence"],
                item["end_ingest_sequence"],
                item["first_record_ordinal"],
                item["last_record_ordinal"],
            )
        )
        previous = end
    return hashlib.sha256(payload).digest()


def raw_wal_object_key(sha256: bytes) -> str:
    if len(sha256) != 32:
        raise ValueError("raw WAL object SHA-256 must be 32 bytes")
    return f"objects/raw/wal-{sha256.hex()}.rtw"


def duplicate_identity_status(first: bytes, second: bytes) -> str:
    first_frame = decode_frame(first)
    second_frame = decode_frame(second)
    first_message = decode_message(first_frame)
    second_message = decode_message(second_frame)
    if first_frame.message_type != 3 or second_frame.message_type != 3:
        raise ProtocolError("INVALID_FIELD", "duplicate identity requires BatchFrameV1")
    same_identity = (
        first_message["producer_session_id"] == second_message["producer_session_id"]
        and first_message["batch_sequence"] == second_message["batch_sequence"]
    )
    if not same_identity:
        return "NOT_DUPLICATE"
    if gateway_batch_sha256(first) == gateway_batch_sha256(second):
        return "DUPLICATE"
    return "SOURCE_STATE_CONFLICT"


def apply_mutation(raw: bytes, mutation: dict[str, Any]) -> bytes:
    kind = mutation["type"]
    if kind == "truncate":
        return raw[: int(mutation["size"])]
    result = bytearray(raw)
    if kind == "xor":
        offset = int(mutation["offset"])
        result[offset] ^= int(mutation["value"])
        return bytes(result)
    if kind == "set_u16":
        struct.pack_into("<H", result, int(mutation["offset"]), int(mutation["value"]))
        return bytes(result)
    if kind == "set_u32":
        struct.pack_into("<I", result, int(mutation["offset"]), int(mutation["value"]))
        return bytes(result)
    if kind == "duplicate_identity":
        return raw
    raise ValueError(f"unknown mutation: {kind}")
