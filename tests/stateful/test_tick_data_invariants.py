from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_fixture_verifier_is_independent_of_gateway_source() -> None:
    verifier = ROOT / "tools" / "tick_fixture_verify.py"
    source = verifier.read_text(encoding="utf-8")

    assert "internal" not in source
    assert "chishiki_logic" not in source
