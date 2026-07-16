from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from tools.tick_fixture_verify import _verify_publication_contract, _verify_replay_contract
from tools.tick_protocol import (
    ProtocolError,
    canonical_json,
    canonical_replay_data_row,
    canonical_replay_marker_row,
    decode_canonical_json,
    decode_frame,
    decode_message,
    duplicate_identity_status,
    exact_identity_path_key,
    final_observation_canonical_json,
    gateway_batch_sha256,
    observation_hash,
    part_manifest_canonical_json,
    part_manifest_digest,
    part_manifest_key,
    part_set_root,
    publication_bundle_digest,
    raw_day_manifest_digest,
    raw_set_root,
    raw_wal_object_key,
    replay_day_manifest_canonical_json,
    replay_day_manifest_digest,
    replay_day_manifest_key,
    replay_derivative_base_key,
    replay_part_object_key,
    row_chain_step,
    source_payload_fingerprint,
)

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "testdata" / "tickdata" / "golden"


def load_fixture(name: str) -> dict:
    return json.loads((GOLDEN / name).read_text(encoding="utf-8"))


def test_python_decoder_accepts_all_valid_frames() -> None:
    index = load_fixture("index.json")
    for item in index["fixtures"]:
        if item["kind"] != "valid_frame":
            continue
        fixture = load_fixture(item["path"])
        frame = decode_frame(bytes.fromhex(fixture["wire_hex"]))
        message = decode_message(frame)
        assert frame.message_type == fixture["decoded_message_type"]
        if frame.message_type == 3 and fixture["expected_hashes"]:
            record = message["records"][0]
            expected = fixture["expected_hashes"]
            source_hash = source_payload_fingerprint(record)
            assert source_hash.hex() == expected["source_payload_fingerprint"]
            assert (
                observation_hash(
                    "fake-01",
                    message["producer_session_id"],
                    message["batch_sequence"],
                    0,
                    record["capture_sequence"],
                    source_hash,
                ).hex()
                == expected["observation_hash"]
            )
            assert (
                gateway_batch_sha256(bytes.fromhex(fixture["wire_hex"])).hex()
                == expected["gateway_batch_sha256"]
            )


@pytest.mark.parametrize(
    ("fixture_name", "expected_code"),
    [
        ("batch-truncated.json", "TRUNCATED_FRAME"),
        ("batch-crc-mismatch.json", "CRC_MISMATCH"),
        ("batch-unknown-version.json", "UNSUPPORTED_PROTOCOL_VERSION"),
        ("batch-unknown-message.json", "UNKNOWN_MESSAGE_TYPE"),
        ("batch-oversized.json", "OVERSIZED_FRAME"),
    ],
)
def test_python_decoder_rejects_mutations(fixture_name: str, expected_code: str) -> None:
    fixture = load_fixture(fixture_name)
    base = load_fixture("batch-v1.json")
    mutation = fixture["mutation"]
    raw = bytearray(bytes.fromhex(base["wire_hex"]))
    if mutation["type"] == "truncate":
        raw = raw[: mutation["size"]]
    elif mutation["type"] == "xor":
        raw[mutation["offset"]] ^= mutation["value"]
    elif mutation["type"] == "set_u16":
        raw[mutation["offset"] : mutation["offset"] + 2] = mutation["value"].to_bytes(2, "little")
    elif mutation["type"] == "set_u32":
        raw[mutation["offset"] : mutation["offset"] + 4] = mutation["value"].to_bytes(4, "little")
    with pytest.raises(ProtocolError) as raised:
        decode_frame(bytes(raw))
    assert raised.value.code == expected_code


def test_canonical_json_fixture_is_stable() -> None:
    for name in (
        "raw-day-manifest-v1.json",
        "raw-day-manifest-chain-slice-v1.json",
        "replay-day-manifest-v1.json",
    ):
        fixture = load_fixture(name)
        value = json.loads(fixture["canonical_json"])
        assert canonical_json(value) == fixture["canonical_json"]


def test_canonical_json_strict_decoder_rejects_duplicate_float_and_noncanonical_bytes() -> None:
    for raw in (
        b'{"a":1,"a":2}',
        b'{"a":1.0}',
        b'{"a":1} ',
        b'{"a":18446744073709551616}',
        '{"a":"日本"}'.encode(),
    ):
        with pytest.raises(ValueError):
            decode_canonical_json(raw)
    with pytest.raises(ValueError):
        canonical_json({"value": 1 << 64})


def test_raw_set_root_matches_golden_manifest() -> None:
    for name in ("raw-day-manifest-v1.json", "raw-day-manifest-chain-slice-v1.json"):
        fixture = load_fixture(name)
        manifest = decode_canonical_json(fixture["canonical_json"].encode("utf-8"))
        assert raw_set_root(manifest["objects"]).hex() == manifest["raw_set_root"]


def test_raw_manifest_domain_digest_rejects_plain_sha256_binding() -> None:
    fixture = load_fixture("raw-day-manifest-v1.json")
    canonical = fixture["canonical_json"].encode("utf-8")
    expected = bytes.fromhex(fixture["manifest_sha256"])
    assert raw_day_manifest_digest(canonical) == expected
    assert hashlib.sha256(canonical).digest() != expected


def test_raw_wal_object_key_is_content_addressed() -> None:
    assert raw_wal_object_key(bytes.fromhex("aa" * 32)) == "objects/raw/wal-" + "aa" * 32 + ".rtw"


def test_derivative_keys_are_exact_date_local_and_reject_generic_or_range_mismatch() -> None:
    fixture = load_fixture("replay-v1-conformance.json")
    part = fixture["part_manifest"]
    manifest = fixture["replay_manifest"]
    assert exact_identity_path_key("EURUSD") != exact_identity_path_key("eurusd")
    assert exact_identity_path_key("é") == hashlib.sha256("é".encode()).hexdigest()
    base = replay_derivative_base_key(part)
    assert part["part_key"].startswith(base + "/parquet/0-1-")
    assert part_manifest_key(part) == fixture["part_manifest_key"]
    assert replay_day_manifest_key(manifest) == fixture["replay_manifest_key"]
    generic_part = dict(part)
    generic_part["part_key"] = "objects/replay/part-" + generic_part["part_sha256"] + ".parquet"
    with pytest.raises(ProtocolError):
        part_manifest_canonical_json(generic_part)
    generic_manifest = dict(manifest)
    generic_manifest["part_manifest_keys"] = ["manifests/replay/part-00000000-deadbeef.json"]
    with pytest.raises(ProtocolError):
        replay_day_manifest_canonical_json(generic_manifest)
    wrong_range = dict(part)
    wrong_range["first_stream_sequence"] = 1
    with pytest.raises(ProtocolError):
        part_manifest_canonical_json(wrong_range)


def test_duplicate_identity_returns_duplicate_ack_status() -> None:
    fixture = load_fixture("batch-v1.json")
    raw = bytes.fromhex(fixture["wire_hex"])
    assert duplicate_identity_status(raw, raw) == "DUPLICATE"


def test_replay_v1_conformance_matches_golden() -> None:
    fixture = load_fixture("replay-v1-conformance.json")
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
    marker_bytes = canonical_replay_marker_row(scope, marker)
    data_bytes = canonical_replay_data_row(scope, data)
    assert marker_bytes.hex() == fixture["marker_canonical_bytes"]
    assert data_bytes.hex() == fixture["data_canonical_bytes"]
    marker_hash = row_chain_step(0, bytes(32), marker_bytes)
    assert row_chain_step(1, marker_hash, data_bytes).hex() == fixture["row_chain_root"]
    part = fixture["part_manifest"]
    assert part_manifest_canonical_json(part).decode() == fixture["part_manifest_canonical_json"]
    assert part_manifest_digest(part).hex() == fixture["part_manifest_digest"]
    assert part_set_root([part]).hex() == fixture["part_set_root"]
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
        assert part[key] == manifest[key]
    assert part["previous_row_chain_hash"] == "00" * 32
    assert part["last_row_chain_hash"] == manifest["canonical_stream_row_chain_root"]
    assert (
        replay_day_manifest_canonical_json(manifest).decode()
        == fixture["replay_manifest_canonical_json"]
    )
    assert replay_day_manifest_digest(manifest).hex() == fixture["replay_manifest_digest"]
    unknown_version = dict(manifest)
    unknown_version["manifest_version"] = "replay-day-manifest-v9"
    with pytest.raises(ProtocolError):
        replay_day_manifest_canonical_json(unknown_version)
    unknown_key = dict(manifest)
    unknown_key["unexpected"] = 1
    with pytest.raises(ProtocolError):
        replay_day_manifest_canonical_json(unknown_key)


def test_part_manifest_fixture_matches_canonical_key_and_digest() -> None:
    fixture = load_fixture("part-manifest-v1.json")
    part = fixture["part_manifest"]
    assert part_manifest_canonical_json(part).decode() == fixture["canonical_json"]
    assert part_manifest_digest(part).hex() == fixture["manifest_sha256"]
    assert part_manifest_key(part) == fixture["manifest_key"]


def test_replay_publication_contract_fixture() -> None:
    _verify_publication_contract(load_fixture("replay-publication-v1-conformance.json"))


def test_python_publication_round_budget_scales_with_part_count() -> None:
    fixture = load_fixture("replay-publication-v1-conformance.json")
    bundle = copy.deepcopy(fixture["bundle"])
    bundle["limits"]["max_publication_rounds"] = 3
    with pytest.raises(ProtocolError, match="publication round budget"):
        publication_bundle_digest(bundle)


def test_python_final_observation_accepts_proven_empty_terminal_and_predecessor() -> None:
    fixture = load_fixture("replay-publication-v1-conformance.json")
    bundle = copy.deepcopy(fixture["bundle"])
    manifest = json.loads(fixture["final_observation"]["replay_edges"][0]["canonical_json"])
    empty = copy.deepcopy(manifest)
    empty["part_manifest_keys"] = []
    empty["part_set_root"] = "00" * 32
    empty["canonical_stream_row_chain_root"] = "00" * 32

    def edge(value: dict) -> dict:
        canonical = replay_day_manifest_canonical_json(value).decode()
        relative = replay_day_manifest_key(value)
        return {
            "canonical_json": canonical,
            "canonical_stream_row_chain_root": value["canonical_stream_row_chain_root"],
            "full_key": bundle["scope"]["immutable_prefix"] + "/" + relative,
            "manifest_digest": replay_day_manifest_digest(value).hex(),
            "part_count": len(value["part_manifest_keys"]),
            "part_set_root": value["part_set_root"],
            "previous_manifest_digest": value["previous_manifest_sha256"],
            "revision": value["revision"],
        }

    empty_edge = edge(empty)
    bundle["parquet_objects"] = []
    bundle["part_manifests"] = []
    bundle["part_set_root"] = "00" * 32
    bundle["canonical_stream_row_chain_root"] = "00" * 32
    bundle["replay_manifest"].update(
        bytes=len(empty_edge["canonical_json"].encode()),
        domain_digest=empty_edge["manifest_digest"],
        full_key=empty_edge["full_key"],
        relative_key=empty_edge["full_key"][len(bundle["scope"]["immutable_prefix"]) + 1 :],
        revision=1,
    )
    observation = copy.deepcopy(fixture["final_observation"])
    observation["derivative_objects"] = [
        item for item in observation["derivative_objects"] if item["kind"] == "replay_manifest"
    ]
    observation["derivative_objects"][0].update(
        bytes=bundle["replay_manifest"]["bytes"],
        digest=empty_edge["manifest_digest"],
        full_key=empty_edge["full_key"],
    )
    observation["replay_edges"] = [empty_edge]
    observation["bundle_digest"] = publication_bundle_digest(bundle).hex()
    observation["observation_bytes"] = bundle["limits"]["max_observation_bytes"]
    final_observation_canonical_json(observation, bundle)

    predecessor = copy.deepcopy(empty)
    predecessor["raw_day_manifest_key"] = "old.json"
    predecessor["raw_day_manifest_sha256"] = "aa" * 32
    predecessor_edge = edge(predecessor)
    successor = copy.deepcopy(manifest)
    successor["revision"] = 2
    successor["previous_manifest_sha256"] = predecessor_edge["manifest_digest"]
    successor["manifest_id"] = predecessor["manifest_id"]
    successor_edge = edge(successor)
    bundle = copy.deepcopy(fixture["bundle"])
    relative = successor_edge["full_key"][len(bundle["scope"]["immutable_prefix"]) + 1 :]
    bundle["replay_manifest"].update(
        bytes=len(successor_edge["canonical_json"].encode()),
        domain_digest=successor_edge["manifest_digest"],
        full_key=successor_edge["full_key"],
        relative_key=relative,
        revision=2,
    )
    observation = copy.deepcopy(fixture["final_observation"])
    for item in observation["derivative_objects"]:
        if item["kind"] == "replay_manifest":
            item.update(
                bytes=bundle["replay_manifest"]["bytes"],
                digest=successor_edge["manifest_digest"],
                full_key=successor_edge["full_key"],
            )
    observation["replay_edges"] = [predecessor_edge, successor_edge]
    observation["bundle_digest"] = publication_bundle_digest(bundle).hex()
    observation["observation_bytes"] = bundle["limits"]["max_observation_bytes"]
    final_observation_canonical_json(observation, bundle)
    observation["replay_edges"][0]["canonical_json"] = ""
    with pytest.raises(ProtocolError):
        final_observation_canonical_json(observation, bundle)


@pytest.mark.parametrize(
    "key",
    (
        "part_sha256",
        "first_row_chain_hash",
        "last_row_chain_hash",
        "raw_day_manifest_sha256",
        "dependency_lock_hash",
        "writer_configuration_hash",
    ),
)
def test_python_part_manifest_rejects_zero_identity_and_anchor_hashes(key: str) -> None:
    fixture = load_fixture("part-manifest-v1.json")
    part = dict(fixture["part_manifest"])
    part[key] = "00" * 32
    with pytest.raises(ProtocolError):
        part_manifest_canonical_json(part)


def test_python_part_manifest_rejects_zero_bytes_and_successor_zero_predecessor() -> None:
    fixture = load_fixture("part-manifest-v1.json")
    zero_bytes = dict(fixture["part_manifest"])
    zero_bytes["part_bytes"] = 0
    with pytest.raises(ProtocolError):
        part_manifest_canonical_json(zero_bytes)
    successor = dict(fixture["part_manifest"])
    successor["part_sequence"] = 1
    successor["previous_manifest_sha256"] = fixture["manifest_sha256"]
    with pytest.raises(ProtocolError):
        part_manifest_canonical_json(successor)


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("date", "2024-03-10"),
        ("campaign_id", "campaign-other"),
        ("conversion_id", "conversion-v2"),
        (
            "raw_day_manifest_key",
            "snapshots/raw/day-definition=utc-day-v1/date=2024-03-09/raw-day-2-demo.json",
        ),
        ("raw_day_manifest_sha256", "88" * 32),
    ),
)
def test_python_fixture_verifier_rejects_part_outer_binding_mismatch(key: str, value: str) -> None:
    fixture = load_fixture("replay-v1-conformance.json")
    part = dict(fixture["part_manifest"])
    part[key] = value
    part["part_key"] = replay_part_object_key(part)
    fixture["part_manifest"] = part
    fixture["part_manifest_canonical_json"] = part_manifest_canonical_json(part).decode()
    fixture["part_manifest_digest"] = part_manifest_digest(part).hex()
    fixture["part_set_root"] = part_set_root([part]).hex()
    fixture["replay_manifest"]["part_manifest_keys"] = [part_manifest_key(part)]
    fixture["replay_manifest"]["part_set_root"] = fixture["part_set_root"]
    with pytest.raises(AssertionError):
        _verify_replay_contract(fixture)
