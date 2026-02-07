from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import os
import subprocess
import tempfile

import pandas as pd
from sqlalchemy import text

from .env import Settings
from .sql_utils import delete_older_than_chunked, ensure_index, normalize_yyyymmddhhmmss_dt, upsert_df
from .state import ensure_state_table, get_cursor, set_cursor
from .tidb import ClusterName, load_tidb_config, make_engine, route_5m_cluster
from .trade_cal import cutoff_by_last_open_days, get_open_trade_dates, parse_date_any


def _find_xtquant_site_packages() -> Path | None:
    # Default install path from your DB_SCHEMA.md.
    base = Path(r"E:\Software\stock\gjzqqmt")
    if not base.exists():
        return None
    for p in base.rglob("xtquant"):
        if p.is_dir() and p.parent.name == "site-packages":
            return p.parent
    return None


def _find_qmt_pythonw() -> Path | None:
    candidates = [
        Path(r"E:\Software\stock\gjzqqmt\国金证券QMT交易端\bin.x64\pythonw.exe"),
        Path(r"E:\Software\stock\gjzqqmt\国金证券QMT交易端\bin.x64\python.exe"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def _run_xtquant_worker(
    *,
    python_exe: Path,
    worker_path: Path,
    site_packages: Path,
    codes: list[str],
    start: str,
    end: str,
    out_csv_gz: Path,
) -> None:
    # Avoid Windows command-line length issues: pass codes via file.
    with tempfile.TemporaryDirectory(prefix="qmt_worker_") as td:
        codes_file = Path(td) / "codes.txt"
        codes_file.write_text("\n".join(codes) + "\n", encoding="utf-8")

        # pythonw.exe sometimes won't run a script path directly; use runpy wrapper.
        code = (
            "import runpy,sys\n"
            f"sys.argv=[r'{worker_path}',"
            f"'--site-packages',r'{site_packages}',"
            f"'--codes-file',r'{codes_file}',"
            f"'--start','{start}','--end','{end}','--out',r'{out_csv_gz}']\n"
            "runpy.run_path(sys.argv[0], run_name='__main__')\n"
        )
        # pythonw.exe may return before file is fully materialized; we poll afterwards.
        # Remove old error file if present.
        try:
            err_file = Path(str(out_csv_gz) + ".err.txt")
            if err_file.exists():
                err_file.unlink()
        except Exception:
            err_file = Path(str(out_csv_gz) + ".err.txt")

        res = subprocess.run([str(python_exe), "-c", code], capture_output=False)
        if res.returncode != 0:
            msg = f"QMT worker failed with code {res.returncode}"
            try:
                err_file = Path(str(out_csv_gz) + ".err.txt")
                if err_file.exists():
                    msg += "\n" + err_file.read_text(encoding="utf-8", errors="replace")
            except Exception:
                pass
            raise RuntimeError(msg)
        # Wait until output file exists and has data.
        import time as _time

        deadline = _time.time() + 120.0
        while _time.time() < deadline:
            try:
                if out_csv_gz.exists() and out_csv_gz.stat().st_size > 0:
                    return
            except Exception:
                pass
            _time.sleep(0.5)
        raise RuntimeError(f"QMT worker did not produce output file in time: {out_csv_gz}")


def _day_session_range(d: date) -> tuple[str, str]:
    start = datetime.combine(d, time(9, 30, 0))
    end = datetime.combine(d, time(15, 0, 0))
    return start.strftime("%Y%m%d%H%M%S"), end.strftime("%Y%m%d%H%M%S")


@dataclass(frozen=True)
class Update5mResult:
    table: str
    clusters: dict[str, int]
    retention_cutoff: str | None


def _get_cutoff_from_master(settings: Settings, *, end: date, keep_open_days: int) -> date:
    cfg = load_tidb_config(settings, "AS_MASTER")
    engine = make_engine(cfg)
    cutoff = cutoff_by_last_open_days(engine, exchange="SSE", end=end, keep_open_days=keep_open_days)
    if cutoff is None:
        raise RuntimeError(
            f"trade_cal doesn't have enough open days to compute 5m retention cutoff for keep_open_days={keep_open_days}. "
            "Run `python -m stock_to_tidb update --tables trade_cal` first (with enough history)."
        )
    return cutoff


def update_minute_5m(
    *,
    settings: Settings,
    ts_codes: list[str],
    start_date: date | None,
    end_date: date | None,
    lookback_days: int = 0,
    keep_open_days: int = 250,
    table_name: str = "minute_5m",
    no_delete: bool = False,
    write_mode: str = "upsert",
) -> Update5mResult:
    end = end_date or date.today()

    # Retention cutoff driven by AS_MASTER trade_cal.
    cutoff = _get_cutoff_from_master(settings, end=end, keep_open_days=int(keep_open_days)) if not no_delete else None

    # Use AS_MASTER trade_cal to drive open trading days.
    cfgm = load_tidb_config(settings, "AS_MASTER")
    engm = make_engine(cfgm)

    # Group by shard.
    shard_map: dict[ClusterName, list[str]] = {"AS_5MIN_P1": [], "AS_5MIN_P2": [], "AS_5MIN_P3": []}
    for code in ts_codes:
        shard_map[route_5m_cluster(code)].append(code)

    primary_keys = ["ts_code", "trade_time"]
    inserted: dict[str, int] = {}

    qmt_python = _find_qmt_pythonw()
    site_pkgs = _find_xtquant_site_packages()
    worker_path = Path(__file__).resolve().parent / "xtquant_worker.py"
    if qmt_python is None:
        raise RuntimeError("Cannot find QMT python executable (pythonw.exe).")
    if site_pkgs is None:
        raise RuntimeError("Cannot find xtquant site-packages under E:\\Software\\stock\\gjzqqmt")

    for cluster, codes in shard_map.items():
        if not codes:
            continue

        cfg = load_tidb_config(settings, cluster)
        engine = make_engine(cfg)
        ensure_state_table(engine)

        cur_raw = get_cursor(engine, cluster, table_name, "trade_date")
        cur_date = parse_date_any(cur_raw) if cur_raw else None

        if start_date is not None:
            start = start_date
        elif cur_date is not None:
            start = cur_date + timedelta(days=1)
        else:
            start = cutoff or end

        if cutoff:
            start = max(start, cutoff)

        if lookback_days and lookback_days > 0:
            start = min(start, end - timedelta(days=int(lookback_days) * 2))
            if cutoff:
                start = max(start, cutoff)

        total_affected = 0
        days = get_open_trade_dates(engm, exchange="SSE", start=start, end=end)
        max_day: date | None = None

        buf: list[pd.DataFrame] = []
        buf_rows = 0
        flush_rows = 200000

        for d in days:
            st, et = _day_session_range(d)
            out_csv = Path(tempfile.gettempdir()) / f"qmt_5m_{cluster}_{d.isoformat()}.csv.gz"
            _run_xtquant_worker(
                python_exe=qmt_python,
                worker_path=worker_path,
                site_packages=site_pkgs,
                codes=codes,
                start=st,
                end=et,
                out_csv_gz=out_csv,
            )
            df = pd.read_csv(out_csv, compression="gzip")
            # Keep the intermediate csv.gz if debugging is needed.
            if not (os.environ.get("STOCK_TO_TIDB_KEEP_QMT_CSV") == "1"):
                try:
                    out_csv.unlink(missing_ok=True)
                except Exception:
                    pass

            if df is None or df.empty:
                continue

            df = normalize_yyyymmddhhmmss_dt(df, "trade_time")

            # volume (手) -> vol_share (股)
            if "volume" in df.columns:
                df["vol_share"] = pd.to_numeric(df["volume"], errors="coerce") * 100.0
                df = df.drop(columns=["volume"])

            buf.append(df)
            buf_rows += int(len(df))
            if buf_rows >= flush_rows:
                all_df = pd.concat(buf, ignore_index=True)
                total_affected += int(upsert_df(engine, table_name, all_df, primary_keys, mode=write_mode))
                buf = []
                buf_rows = 0
            max_day = d if (max_day is None or d > max_day) else max_day

        if buf_rows > 0:
            all_df = pd.concat(buf, ignore_index=True)
            total_affected += int(upsert_df(engine, table_name, all_df, primary_keys, mode=write_mode))

        # Cursor and retention delete on this shard.
        if max_day is not None:
            set_cursor(engine, cluster, table_name, "trade_date", max_day.isoformat())
        if cutoff:
            # Ensure index exists so retention deletes don't full-scan (saves RU).
            try:
                ensure_index(engine, table_name, f"idx_{table_name}_trade_time", ["trade_time"])
            except Exception:
                pass
            cutoff_dt = datetime.combine(cutoff, time(0, 0, 0))
            delete_older_than_chunked(engine, table_name, "trade_time", cutoff_dt)

        # Cleanup: remove any bad zero-datetime rows from older buggy runs.
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f"DELETE FROM `{table_name}` WHERE trade_time = '0000-00-00 00:00:00' LIMIT 10000")
                )
        except Exception:
            pass

        inserted[cluster] = int(total_affected)

    return Update5mResult(table=table_name, clusters=inserted, retention_cutoff=cutoff.isoformat() if cutoff else None)
