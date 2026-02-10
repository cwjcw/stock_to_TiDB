from __future__ import annotations

"""
Fix inconsistent minute_5m schema across AS_5MIN_P1/P2/P3.

Problem we saw:
- AS_5MIN_P3.minute_5m had open/high/low/close/amount as VARCHAR(64) while P1/P2 used FLOAT.

This script can print and optionally apply ALTER TABLE statements to normalize types.
"""

# Ensure repo root is importable even when launched from non-repo CWD.
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse

from sqlalchemy import text

from stock_to_tidb.env import load_settings
from stock_to_tidb.tidb import load_tidb_config, make_engine


def _show_cols(engine, table: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with engine.begin() as conn:
        rows = conn.execute(text(f"SHOW COLUMNS FROM `{table}`")).fetchall()
    for r in rows:
        out[str(r[0])] = str(r[1])
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Apply ALTER TABLE (default: dry-run only).")
    ap.add_argument("--table", default="minute_5m")
    ap.add_argument("--clusters", default="AS_5MIN_P1,AS_5MIN_P2,AS_5MIN_P3")
    args = ap.parse_args(argv)

    clusters = [x.strip() for x in str(args.clusters).split(",") if x.strip()]
    table = str(args.table).strip()
    apply = bool(args.apply)

    settings = load_settings()
    for cluster in clusters:
        eng = make_engine(load_tidb_config(settings, cluster))  # type: ignore[arg-type]
        cols = _show_cols(eng, table)

        want_float = ["open", "high", "low", "close", "amount"]
        bad = [c for c in want_float if c in cols and cols[c].lower().startswith("varchar")]

        print(f"\n== {cluster}.{table}")
        for c in want_float:
            if c in cols:
                print(f"{c}: {cols[c]}")
            else:
                print(f"{c}: (missing)")

        if not bad:
            print("schema ok (no varchar numeric columns detected)")
            continue

        alters = ", ".join(f"MODIFY COLUMN `{c}` FLOAT NULL" for c in bad)
        sql = f"ALTER TABLE `{table}` {alters}"
        print("need:", sql)
        if apply:
            with eng.begin() as conn:
                conn.execute(text(sql))
            print("applied")
        else:
            print("(dry-run; re-run with --apply to execute)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

