from __future__ import annotations

import argparse
import json
from datetime import date, datetime

from .env import load_settings
from .tidb import load_tidb_config, make_engine
from .tushare_jobs import MASTER_TABLES, update_master
from .xtquant_5m import update_minute_5m


def _parse_date(s: str) -> date:
    s = s.strip()
    if "-" in s:
        return date.fromisoformat(s)
    return datetime.strptime(s, "%Y%m%d").date()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="stock_to_tidb")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_up = sub.add_parser("update", help="Create/fetch/incrementally update AS_MASTER tables (Tushare) with retention.")
    p_up.add_argument("--tables", default="all", help="Comma-separated table names, or 'all'.")
    p_up.add_argument("--since", default=None, help="Override start date (YYYY-MM-DD or YYYYMMDD).")
    p_up.add_argument("--until", default=None, help="Override end date (YYYY-MM-DD or YYYYMMDD).")
    p_up.add_argument("--lookback-days", type=int, default=0, help="Re-fetch last N days (still enforces retention).")
    p_up.add_argument("--ts-codes", default=None, help="Comma-separated ts_code list to limit fetch size (best-effort).")
    p_up.add_argument("--sample-tscodes", type=int, default=0, help="Randomly sample N stocks from Tushare (reduces RU).")
    p_up.add_argument("--write-mode", choices=["upsert", "ignore"], default="upsert", help="Write strategy (ignore uses INSERT IGNORE).")
    p_up.add_argument("--no-delete", action="store_true", help="Skip retention deletes (safe for testing).")

    p_5m = sub.add_parser("update-5m", help="Update minute_5m via xtquant (runs a QMT pythonw worker; QMT行情服务需可连接).")
    p_5m.add_argument("--ts-codes", required=False, default="", help="Comma-separated ts_code list, e.g. 000001.SZ,600000.SH")
    p_5m.add_argument("--since", default=None)
    p_5m.add_argument("--until", default=None)
    p_5m.add_argument("--lookback-days", type=int, default=0)
    p_5m.add_argument("--sample-tscodes", type=int, default=0, help="Randomly sample N stocks from Tushare.")
    p_5m.add_argument("--no-delete", action="store_true", help="Skip retention deletes (safe for testing).")
    p_5m.add_argument("--write-mode", choices=["upsert", "ignore"], default="upsert")

    args = p.parse_args(argv)

    settings = load_settings()

    if args.cmd == "update":
        if args.tables == "all":
            tables = list(MASTER_TABLES.keys())
        else:
            tables = [x.strip() for x in str(args.tables).split(",") if x.strip()]

        since = _parse_date(args.since) if args.since else None
        until = _parse_date(args.until) if args.until else None

        ts_codes = None
        if args.ts_codes:
            ts_codes = [x.strip() for x in str(args.ts_codes).split(",") if x.strip()]
        elif int(args.sample_tscodes or 0) > 0:
            import random
            import tushare as ts

            ts.set_token(settings.tushare_token)
            pro = ts.pro_api()
            df = pro.query("stock_basic", exchange="", list_status="L", fields="ts_code")
            all_codes = [x for x in df["ts_code"].dropna().astype(str).tolist()]
            random.shuffle(all_codes)
            ts_codes = all_codes[: int(args.sample_tscodes)]

        cfg = load_tidb_config(settings, "AS_MASTER")
        engine = make_engine(cfg)
        out = update_master(
            settings=settings,
            engine_master=engine,
            tables=tables,
            start_date=since,
            end_date=until,
            lookback_days=int(args.lookback_days or 0),
            ts_codes=ts_codes,
            write_mode=str(args.write_mode),
            no_delete=bool(args.no_delete),
        )
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "update-5m":
        ts_codes = [x.strip() for x in str(args.ts_codes).split(",") if x.strip()]
        if int(args.sample_tscodes or 0) > 0:
            import random
            import tushare as ts

            ts.set_token(settings.tushare_token)
            pro = ts.pro_api()
            df = pro.query("stock_basic", exchange="", list_status="L", fields="ts_code")
            all_codes = [x for x in df["ts_code"].dropna().astype(str).tolist()]
            random.shuffle(all_codes)
            ts_codes = all_codes[: int(args.sample_tscodes)]
        if not ts_codes:
            raise SystemExit("Need --ts-codes or --sample-tscodes for update-5m")

        since = _parse_date(args.since) if args.since else None
        until = _parse_date(args.until) if args.until else None
        res = update_minute_5m(
            settings=settings,
            ts_codes=ts_codes,
            start_date=since,
            end_date=until,
            lookback_days=int(args.lookback_days or 0),
            keep_open_days=250,
            no_delete=bool(args.no_delete),
            write_mode=str(args.write_mode),
        )
        print(json.dumps({"table": res.table, "clusters": res.clusters, "retention_cutoff": res.retention_cutoff}, ensure_ascii=False, indent=2))
        return 0

    raise SystemExit(f"Unknown cmd: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
