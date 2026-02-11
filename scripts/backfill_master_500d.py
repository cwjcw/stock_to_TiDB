from __future__ import annotations

# Ensure repo root is importable even when launched from non-repo CWD.
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import inspect

from stock_to_tidb.env import load_settings
from stock_to_tidb.tidb import load_tidb_config, make_engine
from stock_to_tidb.trade_cal import cutoff_by_last_open_days
from stock_to_tidb.tushare_jobs import MASTER_TABLES, update_table
from stock_to_tidb.sql_utils import delete_older_than_chunked, ensure_index


def _parse_date(s: str) -> date:
    s = s.strip()
    if "-" in s:
        return date.fromisoformat(s)
    return datetime.strptime(s, "%Y%m%d").date()


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _month_end(d: date) -> date:
    # inclusive end
    if d.month == 12:
        nxt = date(d.year + 1, 1, 1)
    else:
        nxt = date(d.year, d.month + 1, 1)
    return nxt - timedelta(days=1)


def _iter_month_ranges(start: date, end: date):
    cur = _month_start(start)
    while cur <= end:
        ms = cur
        me = min(_month_end(cur), end)
        yield (max(ms, start), me)
        # next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def _setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler()]
    try:
        handlers.insert(0, logging.FileHandler(log_path, encoding="utf-8"))
    except Exception:
        # If log file can't be opened (e.g. redirected/locked), fall back to stdout only.
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)


def _date_col_for_table(table_name: str) -> str:
    if table_name == "trade_cal":
        return "cal_date"
    if table_name == "st_list":
        return "start_date"
    return "trade_date"


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill AS_MASTER tables for last N open trading days (rolling window).")
    ap.add_argument("--keep-open-days", type=int, default=500)
    ap.add_argument("--exchange", default="SSE")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD or YYYYMMDD (default: today)")
    ap.add_argument("--write-mode", choices=["upsert", "ignore"], default="upsert")
    ap.add_argument("--sleep-sec", type=float, default=0.0, help="Sleep between API/table ops (rudimentary throttling)")
    ap.add_argument("--log", default="logs/backfill_master_500d.log")
    ap.add_argument("--tables", default="all", help="Comma-separated, or 'all'. Excludes minute_5m.")
    args = ap.parse_args()

    _setup_logging(Path(args.log))

    settings = load_settings()
    engine = make_engine(load_tidb_config(settings, "AS_MASTER"))

    end = _parse_date(args.end_date) if args.end_date else date.today()

    # Always ensure trade_cal and stock_basic first.
    logging.info("Ensuring trade_cal/stock_basic...")
    update_table(
        settings=settings,
        engine_master=engine,
        cluster="AS_MASTER",
        spec=MASTER_TABLES["trade_cal"],
        start_date=end - timedelta(days=365 * 5),
        end_date=end,
        lookback_days=0,
        ts_codes=None,
        write_mode=args.write_mode,
        no_delete=True,
    )
    update_table(
        settings=settings,
        engine_master=engine,
        cluster="AS_MASTER",
        spec=MASTER_TABLES["stock_basic"],
        start_date=None,
        end_date=end,
        lookback_days=0,
        ts_codes=None,
        write_mode=args.write_mode,
        no_delete=True,
    )

    cutoff = cutoff_by_last_open_days(engine, exchange=args.exchange, end=end, keep_open_days=int(args.keep_open_days))
    if cutoff is None:
        raise SystemExit("trade_cal not sufficient to compute cutoff for keep-open-days")
    logging.info("Computed cutoff=%s for last %s open days (end=%s).", cutoff, args.keep_open_days, end)

    if args.tables == "all":
        tables = [t for t in MASTER_TABLES.keys() if t not in ("trade_cal", "stock_basic")]
    else:
        tables = [x.strip() for x in str(args.tables).split(",") if x.strip()]
        tables = [t for t in tables if t not in ("trade_cal", "stock_basic")]

    # A stable order: heavier/frequent tables first.
    preferred = [
        "daily_raw",
        "adj_factor",
        "moneyflow_ind",
        "moneyflow_sector",
        "moneyflow_mkt",
        "moneyflow_hsgt",
        "suspend_d",
        "st_list",
        "index_daily",
    ]
    ordered = [t for t in preferred if t in tables] + [t for t in tables if t not in preferred]

    # Backfill month-by-month within [cutoff, end], without deleting until the end.
    for ms, me in _iter_month_ranges(cutoff, end):
        logging.info("Backfill month range %s -> %s", ms, me)
        for t in ordered:
            spec = MASTER_TABLES[t]
            logging.info("  table=%s", t)
            res = update_table(
                settings=settings,
                engine_master=engine,
                cluster="AS_MASTER",
                spec=spec,
                start_date=ms,
                end_date=me,
                lookback_days=0,
                ts_codes=None,
                write_mode=args.write_mode,
                no_delete=True,  # avoid repeated deletes (RU), do once at end
            )
            logging.info("    fetched=%s affected=%s cursor=%s cutoff=%s", res["rows_fetched"], res["rows_affected"], res["cursor_value"], res["retention_cutoff"])

    # Enforce retention once at end (delete rows older than cutoff).
    logging.info("Enforcing retention deletes once (cutoff=%s)...", cutoff)
    insp = inspect(engine)
    for t in ordered:
        spec = MASTER_TABLES[t]
        if not spec.retention_open_days:
            continue
        if not insp.has_table(spec.table_name):
            continue
        col = _date_col_for_table(spec.table_name)
        try:
            ensure_index(engine, spec.table_name, f"idx_{spec.table_name}_{col}", [col])
        except Exception:
            pass
        try:
            deleted = delete_older_than_chunked(engine, spec.table_name, col, cutoff)
        except Exception as e:
            logging.warning("delete failed table=%s err=%s", t, e)
            deleted = 0
        logging.info("  retention table=%s deleted=%s", t, deleted)

    logging.info("Backfill done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
