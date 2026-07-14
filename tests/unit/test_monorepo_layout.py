from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_monorepo_component_boundaries_exist() -> None:
    expected = (
        "protocol/v1",
        "protocol/v1/schemas",
        "protocol/v1/fixtures",
        "protocol/v1/conformance",
        "producers/mt5",
        "producers/fake",
        "cmd/tick-gateway",
        "internal/protocol",
    )

    missing = [path for path in expected if not (ROOT / path).is_dir()]
    assert missing == []


def test_mt5_producer_does_not_import_gateway_internals() -> None:
    source = (ROOT / "producers" / "mt5" / "TickCaptureService.mq5").read_text(encoding="utf-8")

    assert "internal/" not in source
    assert "R2" not in source
    assert "SQLite" not in source
