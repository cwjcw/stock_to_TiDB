from __future__ import annotations

"""
Normalize AS_MASTER.moneyflow_hsgt numeric columns from VARCHAR to DECIMAL.

Background:
- Tushare moneyflow_hsgt often returns numeric fields as strings.
- If the table was auto-created from string dtypes, TiDB ends up with VARCHAR columns.
- That makes analytics (avg/quantile/change rate) harder and error-prone.
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


NUM_COLS = ["ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Apply ALTER TABLE (default: dry-run).")
    ap.add_argument("--precision", type=int, default=24)
    ap.add_argument("--scale", type=int, default=2)
    args = ap.parse_args(argv)

    settings = load_settings()
    eng = make_engine(load_tidb_config(settings, "AS_MASTER"))

    with eng.begin() as conn:
        cols = conn.execute(text("SHOW COLUMNS FROM `moneyflow_hsgt`")).fetchall()
    col_types = {str(r[0]): str(r[1]).lower() for r in cols}

    bad_cols = [c for c in NUM_COLS if c in col_types and col_types[c].startswith("varchar")]
    print("moneyflow_hsgt varchar numeric cols:", bad_cols)

    # Validate convertibility: any non-numeric chars -> would coerce to 0 on ALTER, so we NULL them first.
    non_numeric_counts = {}
    with eng.begin() as conn:
        for c in bad_cols:
            n = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM `moneyflow_hsgt`
                    WHERE `{c}` IS NOT NULL AND `{c}` <> ''
                      AND `{c}` REGEXP '[^0-9\\\\.\\-]'
                    """
                )
            ).scalar()
            non_numeric_counts[c] = int(n or 0)
    print("non-numeric regex counts:", non_numeric_counts)

    prec = int(args.precision)
    scale = int(args.scale)
    alter_cols = ", ".join(f"MODIFY COLUMN `{c}` DECIMAL({prec},{scale}) NULL" for c in bad_cols)
    if not alter_cols:
        print("schema ok (no VARCHAR numeric columns)")
        return 0

    sql = f"ALTER TABLE `moneyflow_hsgt` {alter_cols}"
    print("need:", sql)

    if not args.apply:
        print("(dry-run; re-run with --apply to execute)")
        return 0

    with eng.begin() as conn:
        for c, n in non_numeric_counts.items():
            if n > 0:
                conn.execute(
                    text(
                        f"""
                        UPDATE `moneyflow_hsgt`
                        SET `{c}` = NULL
                        WHERE `{c}` IS NOT NULL AND `{c}` <> ''
                          AND `{c}` REGEXP '[^0-9\\\\.\\-]'
                        """
                    )
                )
        conn.execute(text(sql))

    print("applied")
    with eng.begin() as conn:
        cols2 = conn.execute(text("SHOW COLUMNS FROM `moneyflow_hsgt`")).fetchall()
    for r in cols2:
        if str(r[0]) in NUM_COLS or str(r[0]) == "trade_date":
            print(r[0], r[1], r[2], r[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

