"""Verify Protocol V1 golden fixtures independently of the Go implementation."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import struct
from pathlib import Path
from typing import Any

from tick_protocol import (
    _crc32c,
    apply_mutation,
    canonical_json,
    decode_frame,
    decode_message,
    duplicate_identity_status,
    gateway_batch_sha256,
    observation_hash,
    source_payload_fingerprint,
    wal_entry_hash,
)

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "testdata" / "tickdata" / "golden"


def _load_fixture(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _verify_frame(fixture: dict[str, Any]) -> None:
    raw = bytes.fromhex(fixture["wire_hex"])
    frame = decode_frame(raw)
    if frame.message_type != fixture["decoded_message_type"]:
        raise AssertionError(f"{fixture['fixture_id']}: message type mismatch")
    message = decode_message(frame)
    expected = fixture.get("expected_hashes", {})
    if frame.message_type == 3 and expected:
        record = message["records"][0]
        source_hash = source_payload_fingerprint(record).hex()
        if source_hash != expected["source_payload_fingerprint"]:
            raise AssertionError(f"{fixture['fixture_id']}: source hash mismatch")
        observation = observation_hash(
            "fake-01",
            message["producer_session_id"],
            message["batch_sequence"],
            0,
            record["capture_sequence"],
            bytes.fromhex(source_hash),
        ).hex()
        if observation != expected["observation_hash"]:
            raise AssertionError(f"{fixture['fixture_id']}: observation hash mismatch")
        batch_hash = gateway_batch_sha256(raw).hex()
        if batch_hash != expected["gateway_batch_sha256"]:
            raise AssertionError(f"{fixture['fixture_id']}: batch hash mismatch")
    elif expected:
        raise AssertionError(f"{fixture['fixture_id']}: unexpected hashes")


def _verify_manifest(fixture: dict[str, Any]) -> None:
    canonical = fixture["canonical_json"]
    decoded = json.loads(canonical)
    if canonical_json(decoded) != canonical:
        raise AssertionError(f"{fixture['fixture_id']}: non-canonical JSON")
    _verify_manifest_schema(fixture["fixture_id"], decoded)
    if fixture["fixture_id"].startswith("raw-"):
        prefix = b"tick-data-platform/raw-day-manifest/v1\0"
    else:
        prefix = b"tick-data-platform/replay-day-manifest/v1\0"
    actual = hashlib.sha256(prefix + canonical.encode("utf-8")).hexdigest()
    if actual != fixture["manifest_sha256"]:
        raise AssertionError(f"{fixture['fixture_id']}: manifest hash mismatch")


def _verify_manifest_schema(fixture_id: str, value: dict[str, Any]) -> None:
    def is_integer(item: Any) -> bool:
        return isinstance(item, int) and not isinstance(item, bool)

    def require_keys(expected: set[str]) -> None:
        if set(value) != expected:
            raise AssertionError(f"{fixture_id}: manifest keys differ")

    def require_hash(name: str, nullable: bool = False) -> None:
        item = value[name]
        if nullable and item is None:
            return
        if not isinstance(item, str) or not re.fullmatch(r"[0-9a-f]{64}", item):
            raise AssertionError(f"{fixture_id}: invalid {name}")

    if fixture_id == "raw-day-manifest-v1":
        require_keys(
            {
                "manifest_version",
                "manifest_id",
                "dataset_id",
                "campaign_id",
                "day_definition_id",
                "date",
                "publisher_id",
                "publisher_epoch",
                "config_hash",
                "protocol_version",
                "source_schema_id",
                "wal_schema_id",
                "observed_through_source_msc",
                "observed_through_capture_sequence",
                "terminal_sync_status",
                "settle_policy",
                "completeness_status",
                "objects",
                "accepted_record_count",
                "error_count",
                "chain_slice_start_sequence",
                "chain_slice_start_root",
                "chain_slice_end_sequence",
                "chain_slice_end_root",
                "raw_set_root",
                "previous_manifest_sha256",
                "logical_close_time_s",
            }
        )
        if not isinstance(value["publisher_epoch"], int) or isinstance(
            value["publisher_epoch"], bool
        ):
            raise AssertionError(f"{fixture_id}: publisher_epoch must be integer")
        if value["publisher_epoch"] < 0 or value["protocol_version"] != 1:
            raise AssertionError(f"{fixture_id}: invalid version or epoch")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value["date"]):
            raise AssertionError(f"{fixture_id}: invalid date")
        if value["source_schema_id"] != "mt5.mqltick.v1":
            raise AssertionError(f"{fixture_id}: invalid source schema")
        if value["wal_schema_id"] != "gateway-wal-v1":
            raise AssertionError(f"{fixture_id}: invalid WAL schema")
        for name in (
            "config_hash",
            "chain_slice_start_root",
            "chain_slice_end_root",
            "raw_set_root",
        ):
            require_hash(name)
        require_hash("previous_manifest_sha256", nullable=True)
        for name in (
            "observed_through_source_msc",
            "observed_through_capture_sequence",
            "accepted_record_count",
            "error_count",
            "chain_slice_start_sequence",
            "chain_slice_end_sequence",
            "logical_close_time_s",
        ):
            if not is_integer(value[name]):
                raise AssertionError(f"{fixture_id}: {name} must be integer")
        if any(
            not isinstance(value[name], int) or isinstance(value[name], bool) or value[name] < 0
            for name in (
                "observed_through_capture_sequence",
                "accepted_record_count",
                "error_count",
                "chain_slice_start_sequence",
                "chain_slice_end_sequence",
            )
        ):
            raise AssertionError(f"{fixture_id}: non-negative field violation")
        if not isinstance(value["objects"], list):
            raise AssertionError(f"{fixture_id}: objects must be array")
        object_keys = {
            "key",
            "sha256",
            "bytes",
            "start_ingest_sequence",
            "end_ingest_sequence",
            "first_record_ordinal",
            "last_record_ordinal",
        }
        for item in value["objects"]:
            if set(item) != object_keys:
                raise AssertionError(f"{fixture_id}: object keys differ")
            if not isinstance(item["key"], str):
                raise AssertionError(f"{fixture_id}: object key type")
            require_object_hash = item["sha256"]
            if not isinstance(require_object_hash, str) or not re.fullmatch(
                r"[0-9a-f]{64}", require_object_hash
            ):
                raise AssertionError(f"{fixture_id}: object hash")
            for name in object_keys - {"key", "sha256"}:
                if not is_integer(item[name]) or item[name] < 0:
                    raise AssertionError(f"{fixture_id}: object integer")
    else:
        require_keys(
            {
                "manifest_version",
                "manifest_id",
                "dataset_id",
                "campaign_id",
                "day_definition_id",
                "date",
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
            }
        )
        for name in (
            "raw_day_manifest_sha256",
            "dependency_lock_hash",
            "writer_configuration_hash",
        ):
            require_hash(name)
        if value["format_id"] != "ticks-parquet-v1":
            raise AssertionError(f"{fixture_id}: invalid format")
        if value["completeness_status"] not in {"provisional", "settled_snapshot"}:
            raise AssertionError(f"{fixture_id}: invalid completeness")
        if value["part_manifest_keys"] != []:
            raise AssertionError(f"{fixture_id}: part manifests are not empty in M0")
        if (
            value["part_set_root"] is not None
            or value["canonical_stream_row_chain_root"] is not None
        ):
            raise AssertionError(f"{fixture_id}: M0 roots must be null")


def _verify_wal(fixture: dict[str, Any]) -> None:
    header = bytes.fromhex(fixture["file_header_hex"])
    entry = bytes.fromhex(fixture["entry_hex"])
    trailer = bytes.fromhex(fixture["trailer_hex"])
    if header[:4] != b"TWAL" or trailer[:4] != b"TWTR":
        raise AssertionError(f"{fixture['fixture_id']}: WAL magic")
    entry_length = struct.unpack_from("<I", entry, 0)[0]
    if entry_length != len(entry):
        raise AssertionError(f"{fixture['fixture_id']}: entry length")
    frame_length = struct.unpack_from("<I", entry, 64)[0]
    frame = entry[68 : 68 + frame_length]
    stored_batch = entry[68 + frame_length : 100 + frame_length]
    stored_entry = entry[100 + frame_length : 132 + frame_length]
    if struct.unpack_from("<I", entry, 132 + frame_length)[0] != 0x434F4D4D:
        raise AssertionError(f"{fixture['fixture_id']}: commit marker")
    if struct.unpack_from("<I", entry, 136 + frame_length)[0] != _crc32c(
        entry[: 136 + frame_length]
    ):
        raise AssertionError(f"{fixture['fixture_id']}: entry CRC")
    batch_hash = gateway_batch_sha256(frame)
    if stored_batch != batch_hash:
        raise AssertionError(f"{fixture['fixture_id']}: batch hash")
    expected_entry = wal_entry_hash(
        struct.unpack_from("<Q", entry, 8)[0],
        entry[32:64],
        struct.unpack_from("<q", entry, 16)[0],
        struct.unpack_from("<Q", entry, 24)[0],
        batch_hash,
        frame,
    )
    if stored_entry != expected_entry:
        raise AssertionError(f"{fixture['fixture_id']}: entry hash")
    pre_trailer = header + entry
    trailer_file_hash = trailer[60:92]
    if trailer_file_hash != hashlib.sha256(pre_trailer).digest():
        raise AssertionError(f"{fixture['fixture_id']}: file hash")
    if struct.unpack_from("<I", trailer, 92)[0] != _crc32c(trailer[:92]):
        raise AssertionError(f"{fixture['fixture_id']}: trailer CRC")
    expected = fixture["expected_hashes"]
    if expected["gateway_batch_sha256"] != batch_hash.hex():
        raise AssertionError(f"{fixture['fixture_id']}: expected batch hash")
    if expected["wal_entry_hash"] != stored_entry.hex():
        raise AssertionError(f"{fixture['fixture_id']}: expected entry hash")
    if expected["file_sha256"] != trailer_file_hash.hex():
        raise AssertionError(f"{fixture['fixture_id']}: expected file hash")


def _verify_invalid(fixture: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> None:
    base = by_id[fixture["base_fixture_id"]]
    raw = apply_mutation(bytes.fromhex(base["wire_hex"]), fixture["mutation"])
    if fixture["mutation"]["type"] == "duplicate_identity":
        decode_message(decode_frame(raw))
        if fixture["expected_error_code"] != "SOURCE_STATE_CONFLICT":
            raise AssertionError(f"{fixture['fixture_id']}: duplicate expectation")
        return
    try:
        decode_frame(raw)
    except Exception as exc:
        code = getattr(exc, "code", "")
        if code != fixture["expected_error_code"]:
            raise AssertionError(
                f"{fixture['fixture_id']}: expected {fixture['expected_error_code']}, got {code}"
            ) from exc
    else:
        raise AssertionError(f"{fixture['fixture_id']}: mutation was accepted")


def _verify_stateful(fixture: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> None:
    base = by_id[fixture["base_fixture_id"]]
    scenario = fixture["scenario"]
    if scenario in {"ACK_LOSS_RETRY", "DUPLICATE_RETRANSMISSION"}:
        raw = bytes.fromhex(base["wire_hex"])
        if duplicate_identity_status(raw, raw) != fixture["expected_ack_status"]:
            raise AssertionError(f"{fixture['fixture_id']}: Ack status mismatch")
        return
    if scenario == "WAL_RECOVERY":
        _verify_wal(base)
        if fixture["expected_recovery"] != "REPLAY_COMMITTED_ENTRY":
            raise AssertionError(f"{fixture['fixture_id']}: recovery expectation")
        return
    raise AssertionError(f"{fixture['fixture_id']}: unknown stateful scenario")


def verify_all() -> int:
    index = _load_fixture(GOLDEN / "index.json")
    by_id = {item["fixture_id"]: _load_fixture(GOLDEN / item["path"]) for item in index["fixtures"]}
    for item in index["fixtures"]:
        fixture = by_id[item["fixture_id"]]
        if fixture["expected_result"] != item["expected_result"]:
            raise AssertionError(f"{item['fixture_id']}: index result mismatch")
        if item["kind"] == "valid_frame":
            _verify_frame(fixture)
        elif item["kind"] == "canonical_json":
            _verify_manifest(fixture)
        elif item["kind"] == "wal_entry":
            _verify_wal(fixture)
        elif item["kind"] == "invalid_frame":
            _verify_invalid(fixture, by_id)
        elif item["kind"] == "stateful_scenario":
            _verify_stateful(fixture, by_id)
        else:
            raise AssertionError(f"{item['fixture_id']}: unknown fixture kind")
    print(f"verified {len(index['fixtures'])} Protocol V1 fixtures")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, help="reserved for single-fixture verification")
    args = parser.parse_args(argv)
    if args.fixture is not None:
        raise SystemExit("single-fixture verification is not implemented; omit --fixture")
    return verify_all()


if __name__ == "__main__":
    raise SystemExit(main())
