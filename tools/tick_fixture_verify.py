"""Independent fixture-verifier entry point for the M0 contract milestone.

The decoder and hash checks are intentionally not implemented in the initial
repository scaffold. This command only establishes the stable tool location
without importing the Go gateway or any trading code.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture", type=Path, help="fixture file to verify once M0 is implemented"
    )
    args = parser.parse_args(argv)

    if args.fixture is None:
        print("tick_fixture_verify scaffold: no fixture selected")
        return 0

    if not args.fixture.is_file():
        parser.error(f"fixture does not exist: {args.fixture}")

    print(f"tick_fixture_verify scaffold: verification pending for {args.fixture}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
