"""Independent Python implementation of the Protocol V1 contract."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any

MAGIC = b"TICK"
PROTOCOL_VERSION = 1
HEADER_LENGTH = 16
MIN_FRAME_BYTES = 20
MAX_FRAME_BYTES = 1_048_576
MAX_RECORDS = 4096
MAX_STRING_BYTES = 255
SOURCE_SCHEMA_MT5 = "mt5.mqltick.v1"

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
