from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_planned_monorepo_component_roots_exist() -> None:
    expected = (
        "cmd/tick-gateway",
        "cmd/tickctl",
        "cmd/tick-verify",
        "cmd/tick-api",
        "internal/protocol",
        "internal/ingest",
        "internal/wal",
        "internal/journal",
        "internal/continuity",
        "internal/archive",
        "internal/parquet",
        "internal/catalog",
        "internal/delivery",
        "internal/r2",
        "protocol/v1",
        "protocol/v1/schemas",
        "protocol/v1/fixtures",
        "protocol/v1/conformance",
        "producers/mt5",
        "producers/fake",
        "tools",
        "testdata/tickdata",
        "docs/architecture",
    )

    missing = [path for path in expected if not (ROOT / path).is_dir()]
    assert missing == []


def test_runtime_artifacts_are_not_part_of_the_initial_fixture_area() -> None:
    fixture_root = ROOT / "testdata" / "tickdata"
    forbidden_suffixes = (".wal", ".rtw", ".parquet")

    runtime_files = [
        path
        for path in fixture_root.rglob("*")
        if path.is_file() and path.suffix.lower() in forbidden_suffixes
    ]
    assert runtime_files == []
