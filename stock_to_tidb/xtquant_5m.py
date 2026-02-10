from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import os
import subprocess
import tempfile

import pandas as pd
from pandas.errors import EmptyDataError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError

from .env import Settings
from .sql_utils import delete_older_than_chunked, ensure_index, ensure_table_from_df, normalize_yyyymmddhhmmss_dt, upsert_df
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


def _is_transient_mysql_disconnect(e: Exception) -> bool:
    code = None
    orig = getattr(e, "orig", None)
    if orig is not None:
        try:
            code = orig.args[0]
        except Exception:
            code = None
    if code in {2006, 2013, 2055}:
        return True
    msg = str(e).lower()
    return ("lost connection" in msg) or ("server has gone away" in msg) or ("connection was killed" in msg)


def _max_trade_date(engine, table_name: str) -> date | None:
    """
    Best-effort: infer latest trading day present in table.
    Used to fast-forward cursors when data exists but etl_state is behind.
    """
    sql = text(f"SELECT DATE(MAX(trade_time)) AS d FROM `{table_name}`")
    last_err: Exception | None = None
    for attempt in range(5):
        try:
            with engine.begin() as conn:
                row = conn.execute(sql).fetchone()
            last_err = None
            break
        except (OperationalError, DBAPIError) as e:
            if not _is_transient_mysql_disconnect(e):
                raise
            last_err = e
            import time as _time

            _time.sleep(min(8.0, 0.5 * (2**attempt)))
    if last_err is not None:
        raise last_err
    if not row or row[0] is None:
        return None
    v = row[0]
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    try:
        # v may be datetime in some drivers.
        if isinstance(v, datetime):
            return v.date()
    except Exception:
        pass
    try:
        return parse_date_any(str(v))
    except Exception:
        return None


def _maybe_adjust_end_to_last_completed_open_day(engine_master, *, end: date, end_date_was_explicit: bool) -> date:
    """
    xtquant can return partial intraday bars, but this project uses a date cursor that advances by day.
    If we ingest "today" before the session is complete and then advance the cursor, we'd skip the
    remainder of the day. So, when end_date is not explicitly provided, only process fully completed
    trading days (i.e., exclude today before 15:00).
    """
    if end_date_was_explicit:
        return end
    today = date.today()
    if end != today:
        return end
    # A small buffer after close to avoid edge cases around 15:00:00.
    if datetime.now().time() >= time(15, 5, 0):
        return end
    # Find the previous open trading day from AS_MASTER.trade_cal.
    days = get_open_trade_dates(engine_master, exchange="SSE", start=end - timedelta(days=10), end=end)
    if len(days) >= 2 and days[-1] == end:
        return days[-2]
    for d in reversed(days):
        if d < end:
            return d
    # Fallback: exclude today even if trade_cal is missing.
    return end - timedelta(days=1)


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


def _load_all_tscodes_from_master(engine_master) -> list[str]:
    # Prefer TiDB AS_MASTER.stock_basic (already backfilled by scripts/backfill_master_500d.py)
    # to avoid consuming Tushare quota and extra latency.
    with engine_master.connect() as conn:
        # Use a tolerant query (schema may evolve).
        rows = conn.execute(text("SELECT ts_code FROM stock_basic WHERE ts_code IS NOT NULL")).fetchall()
    out = []
    for (c,) in rows:
        if not c:
            continue
        out.append(str(c).strip())
    # Keep deterministic order; shard routing depends on ts_code hash anyway.
    out = sorted(set(out))
    return out


def backfill_minute_5m_market_250d(
    *,
    settings: Settings,
    end_date: date | None = None,
    keep_open_days: int = 250,
    chunk_size: int = 400,
    table_name: str = "minute_5m",
    no_delete: bool = False,
    write_mode: str = "ignore",
    sleep_s: float = 0.0,
    max_codes: int = 0,
    resume: bool = False,
    max_days: int = 0,
    reset_cursor: bool = False,
) -> dict[str, int]:
    """
    Backfill full-market 5m bars for the last `keep_open_days` open trading days.

    Design goals:
    - Correct granularity: one row per (ts_code, trade_time)
    - RU-friendly: chunked writes, optional INSERT IGNORE, retention delete after backfill
    - Resumable (optional): advances shard cursor only after a full trading day finishes for that shard
    """
    import logging

    log = logging.getLogger(__name__)

    # Use AS_MASTER trade_cal as source-of-truth.
    cfgm = load_tidb_config(settings, "AS_MASTER")
    engm = make_engine(cfgm)

    end = end_date or date.today()
    end = _maybe_adjust_end_to_last_completed_open_day(engm, end=end, end_date_was_explicit=(end_date is not None))

    # Always compute the window cutoff; `no_delete` only controls whether we execute retention deletes.
    cutoff_window = cutoff_by_last_open_days(engm, exchange="SSE", end=end, keep_open_days=int(keep_open_days))
    if cutoff_window is None:
        raise RuntimeError(f"Cannot compute cutoff for keep_open_days={keep_open_days} (insufficient trade_cal).")
    cutoff_retention = None if no_delete else cutoff_window

    # Trading days we plan to cover.
    days = get_open_trade_dates(engm, exchange="SSE", start=cutoff_window, end=end)
    if keep_open_days and len(days) > int(keep_open_days):
        days = days[-int(keep_open_days) :]
    if not days:
        return {}

    # Load all codes (and optionally cap for controlled runs).
    codes_all = _load_all_tscodes_from_master(engm)
    if max_codes and int(max_codes) > 0:
        codes_all = codes_all[: int(max_codes)]
    if not codes_all:
        raise RuntimeError("No ts_code found in AS_MASTER.stock_basic. Run `python -m stock_to_tidb update --tables stock_basic` first.")

    qmt_python = _find_qmt_pythonw()
    site_pkgs = _find_xtquant_site_packages()
    worker_path = Path(__file__).resolve().parent / "xtquant_worker.py"
    if qmt_python is None:
        raise RuntimeError("Cannot find QMT python executable (pythonw.exe).")
    if site_pkgs is None:
        raise RuntimeError("Cannot find xtquant site-packages under E:\\Software\\stock\\gjzqqmt")

    # Probe the earliest available minute day to avoid spending hours on empty ranges.
    probe_code = "000001.SZ" if "000001.SZ" in codes_all else codes_all[0]

    def _probe_has_data(d: date) -> bool:
        import gzip

        st, et = _day_session_range(d)
        out_csv = Path(tempfile.gettempdir()) / f"qmt_5m_probe_{d.isoformat()}.csv.gz"
        # Ensure we don't accidentally read a stale file if a previous run crashed mid-probe.
        try:
            out_csv.unlink(missing_ok=True)
        except Exception:
            pass
        _run_xtquant_worker(
            python_exe=qmt_python,
            worker_path=worker_path,
            site_packages=site_pkgs,
            codes=[probe_code],
            start=st,
            end=et,
            out_csv_gz=out_csv,
        )
        try:
            with gzip.open(out_csv, "rt", encoding="utf-8", errors="replace") as f:
                # header + at least one row
                _ = f.readline()
                # Some environments occasionally materialize a blank line before data;
                # be tolerant and scan a few lines.
                for _i in range(5):
                    row = f.readline()
                    if row and row.strip():
                        return True
                return False
        finally:
            if not (os.environ.get("STOCK_TO_TIDB_KEEP_QMT_CSV") == "1"):
                try:
                    out_csv.unlink(missing_ok=True)
                except Exception:
                    pass

    # Availability probing/binary-search was useful for first-time backfills (avoid spending hours on empty ranges),
    # but for steady-state incremental updates we want to be cursor-driven and avoid extra QMT calls.
    # Therefore: only probe when NOT resuming from cursors.
    if not resume:
        # Find a recent day with any data. "Today" may be empty (or incomplete) depending on when the job runs.
        hi = None
        for i in range(min(10, len(days))):
            d = days[-1 - i]
            if _probe_has_data(d):
                hi = len(days) - 1 - i
                break
        if hi is None:
            log.warning("No 5m data available for probe_code=%s in window %s..%s", probe_code, days[0], days[-1])
            return {}
        if hi < len(days) - 1:
            days = days[: hi + 1]

        # Binary search for first available day (assumes availability is monotonic with time).
        lo, hi2 = 0, len(days) - 1
        if not _probe_has_data(days[0]):
            while lo < hi2:
                mid = (lo + hi2) // 2
                if _probe_has_data(days[mid]):
                    hi2 = mid
                else:
                    lo = mid + 1
        first_available = days[lo]
        if lo > 0:
            log.info(
                "Detected minute data availability starts at %s (probe_code=%s); skipping %s earlier open days.",
                first_available,
                probe_code,
                lo,
            )
            days = days[lo:]

    # Partition codes by shard.
    shard_codes: dict[ClusterName, list[str]] = {"AS_5MIN_P1": [], "AS_5MIN_P2": [], "AS_5MIN_P3": []}
    for c in codes_all:
        shard_codes[route_5m_cluster(c)].append(c)

    primary_keys = ["ts_code", "trade_time"]
    affected_total: dict[str, int] = {}

    # Ensure tables exist (even if first chunk is empty) and initialize/reset cursors if requested.
    for cluster in ["AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3"]:
        cfg = load_tidb_config(settings, cluster)
        engine = make_engine(cfg)
        ensure_state_table(engine)
        # Use typed empty columns; otherwise ensure_table_from_df would infer VARCHAR for numeric fields.
        sample = pd.DataFrame(
            {
                "ts_code": pd.Series(dtype="string"),
                "trade_time": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "amount": pd.Series(dtype="float64"),
                "vol_share": pd.Series(dtype="float64"),
            }
        )
        ensure_table_from_df(engine, table_name, sample, primary_keys)
        if reset_cursor:
            # Set to the day before the first backfill day so resume will start from days[0].
            set_cursor(engine, cluster, table_name, "trade_date", (days[0] - timedelta(days=1)).isoformat())
            try:
                set_cursor(engine, cluster, table_name, "trade_date_next_i", None)
            except Exception:
                pass

    # Process day-by-day so each worker call stays small (48 bars per day per symbol).
    import time as _time

    log.info(
        "Starting 5m backfill: end=%s keep_open_days=%s cutoff=%s open_days=%s codes=%s (P1=%s P2=%s P3=%s) chunk_size=%s mode=%s resume=%s",
        end.isoformat(),
        int(keep_open_days),
        cutoff_window.isoformat() if cutoff_window else None,
        len(days),
        len(codes_all),
        len(shard_codes["AS_5MIN_P1"]),
        len(shard_codes["AS_5MIN_P2"]),
        len(shard_codes["AS_5MIN_P3"]),
        int(chunk_size),
        write_mode,
        bool(resume),
    )

    for cluster in ["AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3"]:
        codes = shard_codes[cluster]
        if not codes:
            affected_total[cluster] = 0
            continue

        cfg = load_tidb_config(settings, cluster)
        engine = make_engine(cfg)

        # Resume per shard from cursor, but never before cutoff.
        cur_raw = get_cursor(engine, cluster, table_name, "trade_date")
        cur_date = parse_date_any(cur_raw) if cur_raw else None

        # If the data already exists in TiDB but etl_state cursor is behind (e.g. previous run crashed after writes),
        # fast-forward the cursor so we DON'T re-download from QMT.
        if resume and not reset_cursor:
            try:
                # Helps make MAX(trade_time) cheap if this index already exists or can be created quickly.
                ensure_index(engine, table_name, f"idx_{table_name}_trade_time", ["trade_time"])
            except Exception:
                pass
            try:
                max_d = _max_trade_date(engine, table_name)
            except Exception:
                max_d = None
            if max_d is not None and (cur_date is None or max_d > cur_date):
                # Only fast-forward within our intended window (cutoff_window..end) to avoid surprising jumps
                # if the table contains far-future garbage timestamps.
                if max_d <= end and max_d >= cutoff_window:
                    set_cursor(engine, cluster, table_name, "trade_date", max_d.isoformat())
                    try:
                        set_cursor(engine, cluster, table_name, "trade_date_next_i", None)
                    except Exception:
                        pass
                    cur_date = max_d
                    log.info(
                        "%s %s: fast-forward cursor to existing max trade_date=%s (skip QMT downloads for older days)",
                        table_name,
                        cluster,
                        max_d.isoformat(),
                    )

        # Optional per-day progress cursor to avoid re-downloading earlier chunks after a crash.
        # Format: YYYY-MM-DD@<next_code_index>
        prog_raw = get_cursor(engine, cluster, table_name, "trade_date_next_i")
        prog_day: date | None = None
        prog_next_i: int = 0
        if prog_raw and "@" in prog_raw:
            try:
                d0, n0 = prog_raw.split("@", 1)
                prog_day = parse_date_any(d0)
                prog_next_i = max(0, int(n0))
            except Exception:
                prog_day = None
                prog_next_i = 0

        shard_days = days
        if resume and cur_date is not None:
            shard_days = [d for d in days if d > cur_date]
        shard_days = [d for d in shard_days if d >= cutoff_window]
        if max_days and int(max_days) > 0:
            shard_days = shard_days[: int(max_days)]

        total_affected = 0
        did_work = False

        for d in shard_days:
            did_work = True
            log.info("%s %s: day=%s codes=%s", table_name, cluster, d.isoformat(), len(codes))
            st, et = _day_session_range(d)

            # Chunk codes to avoid huge dicts/dataframes.
            n_chunks = max(1, (len(codes) + int(chunk_size) - 1) // int(chunk_size))
            start_i = 0
            if resume and prog_day == d and prog_next_i > 0:
                start_i = min(int(prog_next_i), len(codes))
                if start_i > 0:
                    log.info(
                        "%s %s: day=%s resuming within-day at code_index=%s/%s (avoids re-downloading earlier chunks)",
                        table_name,
                        cluster,
                        d.isoformat(),
                        start_i,
                        len(codes),
                    )

            for i in range(start_i, len(codes), int(chunk_size)):
                chunk_idx = (i // int(chunk_size)) + 1
                chunk = codes[i : i + int(chunk_size)]
                out_csv = Path(tempfile.gettempdir()) / f"qmt_5m_{cluster}_{d.isoformat()}_{chunk_idx}of{n_chunks}.csv.gz"

                t0 = _time.perf_counter()
                _run_xtquant_worker(
                    python_exe=qmt_python,
                    worker_path=worker_path,
                    site_packages=site_pkgs,
                    codes=chunk,
                    start=st,
                    end=et,
                    out_csv_gz=out_csv,
                )
                t_worker = _time.perf_counter() - t0

                t0 = _time.perf_counter()
                try:
                    df = pd.read_csv(out_csv, compression="gzip")
                except EmptyDataError:
                    df = pd.DataFrame()
                t_read = _time.perf_counter() - t0

                if not (os.environ.get("STOCK_TO_TIDB_KEEP_QMT_CSV") == "1"):
                    try:
                        out_csv.unlink(missing_ok=True)
                    except Exception:
                        pass

                if df is None or df.empty:
                    if chunk_idx == 1 or chunk_idx == n_chunks or (chunk_idx % 10 == 0):
                        log.info(
                            "%s %s: day=%s chunk=%s/%s rows=0 t_worker=%.2fs t_read=%.2fs",
                            table_name,
                            cluster,
                            d.isoformat(),
                            chunk_idx,
                            n_chunks,
                            t_worker,
                            t_read,
                        )
                    # Mark chunk as processed so a retry won't re-download earlier chunks.
                    try:
                        set_cursor(
                            engine,
                            cluster,
                            table_name,
                            "trade_date_next_i",
                            f"{d.isoformat()}@{min(i + int(chunk_size), len(codes))}",
                        )
                    except Exception:
                        pass
                    if sleep_s and float(sleep_s) > 0:
                        _time.sleep(float(sleep_s))
                    continue

                t0 = _time.perf_counter()
                df = normalize_yyyymmddhhmmss_dt(df, "trade_time")
                if "volume" in df.columns:
                    df["vol_share"] = pd.to_numeric(df["volume"], errors="coerce") * 100.0
                    df = df.drop(columns=["volume"])
                t_xform = _time.perf_counter() - t0

                t0 = _time.perf_counter()
                affected = int(upsert_df(engine, table_name, df, primary_keys, mode=write_mode))
                t_write = _time.perf_counter() - t0
                total_affected += affected
                # Mark chunk as processed so a retry won't re-download earlier chunks.
                try:
                    set_cursor(
                        engine,
                        cluster,
                        table_name,
                        "trade_date_next_i",
                        f"{d.isoformat()}@{min(i + int(chunk_size), len(codes))}",
                    )
                except Exception:
                    pass
                if chunk_idx == 1 or chunk_idx == n_chunks or (chunk_idx % 10 == 0):
                    log.info(
                        "%s %s: day=%s chunk=%s/%s rows=%s affected=%s total_affected=%s t_worker=%.2fs t_read=%.2fs t_xform=%.2fs t_write=%.2fs",
                        table_name,
                        cluster,
                        d.isoformat(),
                        chunk_idx,
                        n_chunks,
                        len(df),
                        affected,
                        total_affected,
                        t_worker,
                        t_read,
                        t_xform,
                        t_write,
                    )

                if sleep_s and float(sleep_s) > 0:
                    _time.sleep(float(sleep_s))

            # Only mark the day as done after all chunks completed.
            set_cursor(engine, cluster, table_name, "trade_date", d.isoformat())
            # Clear within-day progress once the day is complete.
            try:
                set_cursor(engine, cluster, table_name, "trade_date_next_i", None)
                prog_day = None
                prog_next_i = 0
            except Exception:
                pass

        # Retention delete after backfill to reduce RU spikes.
        if did_work and cutoff_retention is not None:
            try:
                ensure_index(engine, table_name, f"idx_{table_name}_trade_time", ["trade_time"])
            except Exception:
                pass
            cutoff_dt = datetime.combine(cutoff_retention, time(0, 0, 0))
            delete_older_than_chunked(engine, table_name, "trade_time", cutoff_dt)

        affected_total[cluster] = int(total_affected)

    return affected_total


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
    end = _maybe_adjust_end_to_last_completed_open_day(engm, end=end, end_date_was_explicit=(end_date is not None))

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
            try:
                df = pd.read_csv(out_csv, compression="gzip")
            except EmptyDataError:
                # Worker may have produced an empty file for suspended/non-trading symbols.
                df = pd.DataFrame()
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
