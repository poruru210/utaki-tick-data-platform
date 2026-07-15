from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.tick_protocol import (
    ProtocolError,
    canonical_json,
    decode_canonical_json,
    decode_frame,
    decode_message,
    duplicate_identity_status,
    gateway_batch_sha256,
    observation_hash,
    raw_set_root,
    raw_wal_object_key,
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


def test_raw_wal_object_key_is_content_addressed() -> None:
    assert raw_wal_object_key(bytes.fromhex("aa" * 32)) == "objects/raw/wal-" + "aa" * 32 + ".rtw"


def test_duplicate_identity_returns_duplicate_ack_status() -> None:
    fixture = load_fixture("batch-v1.json")
    raw = bytes.fromhex(fixture["wire_hex"])
    assert duplicate_identity_status(raw, raw) == "DUPLICATE"
