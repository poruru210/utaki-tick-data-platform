"""Verify Protocol V1 golden fixtures independently of the Go implementation."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import struct
from pathlib import Path
from typing import Any

try:
    from tick_protocol import (
        PUBLISHER_CLAIM_DOMAIN,
        ProtocolError,
        _crc32c,
        apply_mutation,
        canonical_json,
        canonical_replay_data_row,
        canonical_replay_marker_row,
        decode_canonical_json,
        decode_frame,
        decode_message,
        duplicate_identity_status,
        final_observation_canonical_json,
        gateway_batch_sha256,
        observation_hash,
        part_manifest_canonical_json,
        part_manifest_digest,
        part_manifest_key,
        part_set_root,
        publication_bundle_canonical_json,
        raw_day_manifest_digest,
        raw_set_root,
        raw_wal_object_key,
        replay_day_manifest_canonical_json,
        replay_day_manifest_digest,
        replay_day_manifest_key,
        replay_derivative_base_key,
        row_chain_step,
        source_payload_fingerprint,
        verify_final_observation,
        verify_publication_bundle,
        wal_entry_hash,
    )
except ModuleNotFoundError:
    from tools.tick_protocol import (
        PUBLISHER_CLAIM_DOMAIN,
        ProtocolError,
        _crc32c,
        apply_mutation,
        canonical_json,
        canonical_replay_data_row,
        canonical_replay_marker_row,
        decode_canonical_json,
        decode_frame,
        decode_message,
        duplicate_identity_status,
        final_observation_canonical_json,
        gateway_batch_sha256,
        observation_hash,
        part_manifest_canonical_json,
        part_manifest_digest,
        part_manifest_key,
        part_set_root,
        publication_bundle_canonical_json,
        raw_day_manifest_digest,
        raw_set_root,
        raw_wal_object_key,
        replay_day_manifest_canonical_json,
        replay_day_manifest_digest,
        replay_day_manifest_key,
        replay_derivative_base_key,
        row_chain_step,
        source_payload_fingerprint,
        verify_final_observation,
        verify_publication_bundle,
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
    try:
        decoded = decode_canonical_json(canonical.encode("utf-8"))
    except ValueError as exc:
        raise AssertionError(f"{fixture['fixture_id']}: non-canonical JSON: {exc}") from exc
    _verify_manifest_schema(fixture["fixture_id"], decoded)
    if fixture["fixture_id"].startswith("raw-"):
        actual = raw_day_manifest_digest(canonical.encode("utf-8")).hex()
    else:
        actual = hashlib.sha256(
            b"tick-data-platform/replay-day-manifest/v1\0" + canonical.encode("utf-8")
        ).hexdigest()
    if actual != fixture["manifest_sha256"]:
        raise AssertionError(f"{fixture['fixture_id']}: manifest hash mismatch")
    if fixture["fixture_id"].startswith("raw-"):
        if raw_set_root(decoded["objects"]).hex() != decoded["raw_set_root"]:
            raise AssertionError(f"{fixture['fixture_id']}: raw set root mismatch")


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

    if fixture_id.startswith("raw-"):
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
                "chain_objects",
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
                "revision",
            }
        )
        if not isinstance(value["publisher_epoch"], int) or isinstance(
            value["publisher_epoch"], bool
        ):
            raise AssertionError(f"{fixture_id}: publisher_epoch must be integer")
        if value["publisher_epoch"] < 0 or value["protocol_version"] != 1:
            raise AssertionError(f"{fixture_id}: invalid version or epoch")
        if not is_integer(value["revision"]) or value["revision"] < 1:
            raise AssertionError(f"{fixture_id}: revision must be at least 1")
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
        if not isinstance(value["chain_objects"], list):
            raise AssertionError(f"{fixture_id}: chain_objects must be array")
        chain_object_keys = {
            "key",
            "sha256",
            "bytes",
            "start_ingest_sequence",
            "end_ingest_sequence",
        }
        previous_chain_end = None
        seen_chain_keys: set[str] = set()
        for item in value["chain_objects"]:
            if set(item) != chain_object_keys:
                raise AssertionError(f"{fixture_id}: chain object keys differ")
            if not isinstance(item["key"], str):
                raise AssertionError(f"{fixture_id}: chain object key type")
            if not isinstance(item["sha256"], str) or not re.fullmatch(
                r"[0-9a-f]{64}", item["sha256"]
            ):
                raise AssertionError(f"{fixture_id}: chain object hash")
            if item["key"] != raw_wal_object_key(bytes.fromhex(item["sha256"])):
                raise AssertionError(f"{fixture_id}: noncanonical chain object key")
            if item["key"] in seen_chain_keys:
                raise AssertionError(f"{fixture_id}: duplicate chain object")
            seen_chain_keys.add(item["key"])
            for name in chain_object_keys - {"key", "sha256"}:
                if not is_integer(item[name]) or item[name] < 0:
                    raise AssertionError(f"{fixture_id}: chain object integer")
            if item["bytes"] == 0 or item["start_ingest_sequence"] == 0:
                raise AssertionError(f"{fixture_id}: empty chain object")
            if item["end_ingest_sequence"] < item["start_ingest_sequence"]:
                raise AssertionError(f"{fixture_id}: reversed chain object")
            if (
                previous_chain_end is not None
                and item["start_ingest_sequence"] != previous_chain_end + 1
            ):
                raise AssertionError(f"{fixture_id}: chain object gap or overlap")
            previous_chain_end = item["end_ingest_sequence"]
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
            if item["bytes"] == 0:
                raise AssertionError(f"{fixture_id}: object bytes must be nonzero")
            start = (item["start_ingest_sequence"], item["first_record_ordinal"])
            end = (item["end_ingest_sequence"], item["last_record_ordinal"])
            if start[0] == 0 or end < start:
                raise AssertionError(f"{fixture_id}: empty or reversed object range")
            if item["key"] != raw_wal_object_key(bytes.fromhex(item["sha256"])):
                raise AssertionError(f"{fixture_id}: noncanonical object key")
        if not value["objects"]:
            if value["chain_objects"] or any(
                value[name] != 0
                for name in ("chain_slice_start_sequence", "chain_slice_end_sequence")
            ):
                raise AssertionError(f"{fixture_id}: empty raw manifest has a chain")
        else:
            if not value["chain_objects"]:
                raise AssertionError(f"{fixture_id}: selected objects lack chain objects")
            start_sequence = value["chain_slice_start_sequence"]
            end_sequence = value["chain_slice_end_sequence"]
            if start_sequence == 0 or end_sequence < start_sequence:
                raise AssertionError(f"{fixture_id}: invalid chain slice")
            first = value["chain_objects"][0]
            last = value["chain_objects"][-1]
            if not first["start_ingest_sequence"] <= start_sequence <= first["end_ingest_sequence"]:
                raise AssertionError(f"{fixture_id}: chain start is not contained")
            if not last["start_ingest_sequence"] <= end_sequence <= last["end_ingest_sequence"]:
                raise AssertionError(f"{fixture_id}: chain end is not contained")
            for item in value["objects"]:
                matches = [
                    chain
                    for chain in value["chain_objects"]
                    if (item["key"], item["sha256"], item["bytes"])
                    == (chain["key"], chain["sha256"], chain["bytes"])
                ]
                if len(matches) != 1:
                    raise AssertionError(f"{fixture_id}: object range chain binding")
                chain = matches[0]
                if not (
                    chain["start_ingest_sequence"]
                    <= item["start_ingest_sequence"]
                    <= item["end_ingest_sequence"]
                    <= chain["end_ingest_sequence"]
                    and start_sequence <= item["start_ingest_sequence"]
                    and item["end_ingest_sequence"] <= end_sequence
                ):
                    raise AssertionError(f"{fixture_id}: object range outside chain slice")
    else:
        m0_keys = {
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
        m3_keys = m0_keys | {
            "revision",
            "raw_day_manifest_key",
            "previous_manifest_sha256",
        }
        if set(value) == m0_keys:
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
                raise AssertionError(f"{fixture_id}: M0 parts are not empty")
            if (
                value["part_set_root"] is not None
                or value["canonical_stream_row_chain_root"] is not None
            ):
                raise AssertionError(f"{fixture_id}: M0 roots must be null")
            return
        require_keys(m3_keys)
        for name in (
            "raw_day_manifest_sha256",
            "dependency_lock_hash",
            "writer_configuration_hash",
            "part_set_root",
            "canonical_stream_row_chain_root",
        ):
            require_hash(name)
        if value["format_id"] != "ticks-parquet-v1":
            raise AssertionError(f"{fixture_id}: invalid format")
        if value["completeness_status"] not in {"provisional", "settled_snapshot"}:
            raise AssertionError(f"{fixture_id}: invalid completeness")
        if not is_integer(value["revision"]) or value["revision"] < 1:
            raise AssertionError(f"{fixture_id}: invalid replay revision")
        if not isinstance(value["raw_day_manifest_key"], str) or not value["raw_day_manifest_key"]:
            raise AssertionError(f"{fixture_id}: missing raw manifest key")
        previous = value["previous_manifest_sha256"]
        if value["revision"] == 1 and previous is not None:
            raise AssertionError(f"{fixture_id}: genesis has a predecessor")
        if value["revision"] > 1:
            require_hash("previous_manifest_sha256")
        if not isinstance(value["part_manifest_keys"], list) or len(
            set(value["part_manifest_keys"])
        ) != len(value["part_manifest_keys"]):
            raise AssertionError(f"{fixture_id}: duplicate part manifest key")
        base = replay_derivative_base_key(value)
        for key in value["part_manifest_keys"]:
            if (
                not isinstance(key, str)
                or not key.startswith(base + "/manifests/part-")
                or not key.endswith(".json")
            ):
                raise AssertionError(f"{fixture_id}: invalid part manifest key")


def _verify_replay_contract(fixture: dict[str, Any]) -> None:
    fixture_id = fixture["fixture_id"]
    scope = dict(fixture["row_scope"])
    scope["raw_day_manifest_sha256"] = bytes.fromhex(scope["raw_day_manifest_sha256"])
    marker = dict(fixture["marker_row"])
    for name in (
        "raw_object_sha256",
        "predecessor_row_chain_hash",
        "continuity_segment_start_hash",
    ):
        marker[name] = bytes.fromhex(marker[name])
    data = dict(fixture["data_row"])
    for name in ("raw_object_sha256", "source_payload_fingerprint", "observation_hash"):
        data[name] = bytes.fromhex(data[name])
    data["record"] = dict(data["record"])
    marker_bytes = canonical_replay_marker_row(scope, marker)
    data_bytes = canonical_replay_data_row(scope, data)
    if marker_bytes.hex() != fixture["marker_canonical_bytes"]:
        raise AssertionError(f"{fixture_id}: marker canonical bytes mismatch")
    if data_bytes.hex() != fixture["data_canonical_bytes"]:
        raise AssertionError(f"{fixture_id}: data canonical bytes mismatch")
    marker_hash = row_chain_step(0, bytes(32), marker_bytes)
    root = row_chain_step(1, marker_hash, data_bytes)
    if root.hex() != fixture["row_chain_root"]:
        raise AssertionError(f"{fixture_id}: row-chain root mismatch")
    part = fixture["part_manifest"]
    if (
        part_manifest_canonical_json(part).decode("utf-8")
        != fixture["part_manifest_canonical_json"]
    ):
        raise AssertionError(f"{fixture_id}: part manifest canonical bytes mismatch")
    if part_manifest_digest(part).hex() != fixture["part_manifest_digest"]:
        raise AssertionError(f"{fixture_id}: part manifest digest mismatch")
    part_root = part_set_root([part])
    if part_root.hex() != fixture["part_set_root"]:
        raise AssertionError(f"{fixture_id}: part set root mismatch")
    if part["previous_row_chain_hash"] != "00" * 32:
        raise AssertionError(f"{fixture_id}: first part predecessor row-chain hash")
    if part["last_row_chain_hash"] != fixture["row_chain_root"]:
        raise AssertionError(f"{fixture_id}: final part row-chain anchor mismatch")
    manifest = fixture["replay_manifest"]
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
    ):
        if part[key] != manifest[key]:
            raise AssertionError(f"{fixture_id}: part outer binding mismatch for {key}")
    if manifest["part_manifest_keys"] != [part_manifest_key(part)]:
        raise AssertionError(f"{fixture_id}: replay part key mismatch")
    if replay_day_manifest_key(manifest) != fixture["replay_manifest_key"]:
        raise AssertionError(f"{fixture_id}: replay manifest key mismatch")
    if manifest["part_set_root"] != part_root.hex():
        raise AssertionError(f"{fixture_id}: replay part-set root mismatch")
    if manifest["canonical_stream_row_chain_root"] != part["last_row_chain_hash"]:
        raise AssertionError(f"{fixture_id}: replay final row-chain anchor mismatch")
    if (
        replay_day_manifest_canonical_json(manifest).decode("utf-8")
        != fixture["replay_manifest_canonical_json"]
    ):
        raise AssertionError(f"{fixture_id}: replay manifest canonical bytes mismatch")
    if replay_day_manifest_digest(manifest).hex() != fixture["replay_manifest_digest"]:
        raise AssertionError(f"{fixture_id}: replay manifest digest mismatch")


def _verify_part_contract(fixture: dict[str, Any]) -> None:
    fixture_id = fixture["fixture_id"]
    part = fixture["part_manifest"]
    if part_manifest_canonical_json(part).decode("utf-8") != fixture["canonical_json"]:
        raise AssertionError(f"{fixture_id}: part manifest canonical bytes mismatch")
    digest = part_manifest_digest(part)
    if digest.hex() != fixture["manifest_sha256"]:
        raise AssertionError(f"{fixture_id}: part manifest digest mismatch")
    if part_manifest_key(part) != fixture["manifest_key"]:
        raise AssertionError(f"{fixture_id}: part manifest key mismatch")


def _verify_key_contract(fixture: dict[str, Any]) -> None:
    fixture_id = fixture["fixture_id"]
    part = fixture["part_manifest"]
    manifest = fixture["replay_manifest"]
    if part_manifest_key(part) != fixture["part_manifest_key"]:
        raise AssertionError(f"{fixture_id}: exact part manifest key mismatch")
    if replay_day_manifest_key(manifest) != fixture["replay_manifest_key"]:
        raise AssertionError(f"{fixture_id}: exact replay manifest key mismatch")
    base = replay_derivative_base_key(part)
    if not part["part_key"].startswith(base + "/parquet/"):
        raise AssertionError(f"{fixture_id}: exact part object base mismatch")
    if any(
        legacy in part["part_key"]
        or legacy in fixture["part_manifest_key"]
        or legacy in fixture["replay_manifest_key"]
        for legacy in ("objects/replay", "manifests/replay", "snapshots/replay", "hour=")
    ):
        raise AssertionError(f"{fixture_id}: legacy derivative key was emitted")


def _verify_publication_contract(fixture: dict[str, Any]) -> None:
    fixture_id = fixture["fixture_id"]
    bundle = fixture["bundle"]
    bundle_bytes = publication_bundle_canonical_json(bundle)
    _, bundle_digest = verify_publication_bundle(bundle_bytes)
    if bundle_digest.hex() != fixture["publication_bundle_digest"]:
        raise AssertionError(f"{fixture_id}: publication bundle digest mismatch")
    observation = fixture["final_observation"]
    observation_bytes = final_observation_canonical_json(observation, bundle)
    _, observation_digest = verify_final_observation(observation_bytes, bundle)
    if observation_digest.hex() != fixture["final_observation_digest"]:
        raise AssertionError(f"{fixture_id}: final observation digest mismatch")
    for case in fixture["negative_cases"]:
        case_id = case["case_id"]
        try:
            if case["target"] == "raw":
                verify_publication_bundle(bytes.fromhex(case["raw_hex"]))
            elif case["target"] == "bundle":
                verify_publication_bundle(
                    _publication_bundle_mutation(bundle, bundle_bytes, case_id)
                )
            elif case["target"] == "observation":
                verify_final_observation(
                    _publication_observation_mutation(observation, case_id), bundle
                )
            else:
                raise AssertionError(f"{fixture_id}: unknown publication target")
        except ProtocolError as exc:
            if exc.code != case["expected_error"]:
                raise AssertionError(
                    f"{fixture_id}/{case_id}: expected {case['expected_error']}, got {exc.code}"
                ) from exc
        else:
            raise AssertionError(f"{fixture_id}/{case_id}: negative case was accepted")


def _publication_bundle_mutation(bundle: dict[str, Any], canonical: bytes, case_id: str) -> bytes:
    if case_id == "duplicate_key":
        return canonical.replace(b"{", b'{"bundle_version":"replay-publication-bundle-v1",', 1)
    if case_id == "noncanonical_bytes":
        return canonical.replace(b"{", b"{ ", 1)
    value = copy.deepcopy(bundle)
    if case_id == "unknown_key":
        value["unknown"] = 1
    elif case_id == "zero_digest":
        value["part_set_root"] = "00" * 32
    elif case_id == "wrong_domain":
        value["claim"]["domain_digest"] = "cc" * 32
    elif case_id == "wrong_key":
        value["claim"]["full_key"] = "i/publisher-claims/epoch=8.json"
    elif case_id == "scope_collision":
        claim = json.loads(value["claim"]["canonical_json"])
        claim["dataset_id"] = "x"
        claim_bytes = canonical_json(claim).encode("utf-8")
        value["claim"]["canonical_json"] = claim_bytes.decode("utf-8")
        value["claim"]["domain_digest"] = hashlib.sha256(
            PUBLISHER_CLAIM_DOMAIN + claim_bytes
        ).hexdigest()
    elif case_id == "raw_claim_missing":
        value["claim"] = {"canonical_json": "", "domain_digest": "", "full_key": ""}
    elif case_id == "oversized_aggregate":
        value["limits"]["max_metadata_object_bytes"] = 1000
        value["limits"]["max_total_metadata_bytes"] = 1200
    else:
        raise AssertionError(f"unknown publication bundle mutation {case_id!r}")
    return canonical_json(value).encode("utf-8")


def _publication_observation_mutation(observation: dict[str, Any], case_id: str) -> bytes:
    value = copy.deepcopy(observation)
    if case_id == "incomplete_observation":
        value["complete"] = False
    elif case_id == "missing_derivative":
        value["derivative_objects"] = []
    elif case_id == "missing_edge_manifest":
        value["replay_edges"][0]["canonical_json"] = ""
    elif case_id == "invalid_edge_manifest":
        value["replay_edges"][0]["canonical_json"] = "{}"
    elif case_id == "noncanonical_edge_manifest":
        value["replay_edges"][0]["canonical_json"] = (
            " " + value["replay_edges"][0]["canonical_json"]
        )
    elif case_id == "edge_key_mismatch":
        value["replay_edges"][0]["full_key"] = "i/wrong.json"
    elif case_id == "edge_digest_mismatch":
        value["replay_edges"][0]["manifest_digest"] = "aa" * 32
    elif case_id == "edge_revision_mismatch":
        value["replay_edges"][0]["revision"] = 2
    elif case_id == "edge_root_mismatch":
        value["replay_edges"][0]["part_set_root"] = "aa" * 32
    elif case_id == "terminal_shape_mismatch":
        value["replay_edges"][0]["part_count"] += 1
    elif case_id in {"nonempty_zero_roots", "mixed_root"}:
        manifest = json.loads(value["replay_edges"][0]["canonical_json"])
        manifest["part_set_root"] = "00" * 32
        if case_id == "nonempty_zero_roots":
            manifest["canonical_stream_row_chain_root"] = "00" * 32
        value["replay_edges"][0]["canonical_json"] = canonical_json(manifest)
    else:
        raise AssertionError(f"unknown publication observation mutation {case_id!r}")
    return canonical_json(value).encode("utf-8")


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
        elif item["kind"] == "replay_contract":
            _verify_replay_contract(fixture)
        elif item["kind"] == "part_contract":
            _verify_part_contract(fixture)
        elif item["kind"] == "key_contract":
            _verify_key_contract(fixture)
        elif item["kind"] == "publication_contract":
            _verify_publication_contract(fixture)
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
