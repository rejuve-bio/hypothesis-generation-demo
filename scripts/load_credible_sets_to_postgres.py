#!/usr/bin/env python3
"""
Deprecated: credible sets and OT studies are now loaded together, atomically,
by scripts/bootstrap_ot_db.py (which also runs automatically on deployment).

This shim loads an explicit release from the EBI FTP (no-op if already current):

  python scripts/load_credible_sets_to_postgres.py --release 25.09
"""

import argparse
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.services.ot_db_bootstrap import BootstrapConfig, run_bootstrap  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release", required=True, help="pinned OpenTargets release, e.g. 25.09 ('latest' is rejected)")
    args = parser.parse_args()
    return run_bootstrap(BootstrapConfig(release=args.release, mode="ebi-only"))


if __name__ == "__main__":
    raise SystemExit(main())
