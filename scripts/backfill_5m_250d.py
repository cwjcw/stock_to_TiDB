from __future__ import annotations

# Ensure repo root is importable even when launched from non-repo CWD.
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json
import logging
from datetime import date

from stock_to_tidb.env import load_settings
from stock_to_tidb.xtquant_5m import backfill_minute_5m_market_250d


def _parse_date(s: str) -> date:
    s = s.strip()
    if "-" in s:
        return date.fromisoformat(s)
    return date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default=None, help="End date (YYYY-MM-DD). Default: today.")
    ap.add_argument("--keep-open-days", type=int, default=250, help="Retention window (open trading days).")
    ap.add_argument("--chunk-size", type=int, default=400, help="Symbols per QMT call (reduce memory / avoid timeouts).")
    ap.add_argument("--write-mode", choices=["upsert", "ignore"], default="ignore", help="Write strategy (ignore saves RU on retries).")
    ap.add_argument("--no-delete", action="store_true", help="Skip retention deletes (debug/testing).")
    ap.add_argument("--sleep-s", type=float, default=0.0, help="Sleep between chunks (can reduce QMT load).")
    ap.add_argument("--max-codes", type=int, default=0, help="Limit number of symbols for a controlled run.")
    ap.add_argument(
        "--resume",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Resume from shard cursors (default: true). Use --no-resume to reprocess the whole window.",
    )
    ap.add_argument("--max-days", type=int, default=0, help="Process only first N trading days (use with --resume for chunked backfill).")
    ap.add_argument(
        "--reset-cursor",
        action="store_true",
        help="Reset shard cursor to start of backfill window (typically paired with --no-resume).",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = load_settings()
    end = _parse_date(args.end) if args.end else None
    out = backfill_minute_5m_market_250d(
        settings=settings,
        end_date=end,
        keep_open_days=int(args.keep_open_days),
        chunk_size=int(args.chunk_size),
        no_delete=bool(args.no_delete),
        write_mode=str(args.write_mode),
        sleep_s=float(args.sleep_s),
        max_codes=int(args.max_codes or 0),
        resume=bool(args.resume),
        max_days=int(args.max_days or 0),
        reset_cursor=bool(args.reset_cursor),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
