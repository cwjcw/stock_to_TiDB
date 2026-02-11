from __future__ import annotations

import time
import signal
from collections import deque
from dataclasses import dataclass
from datetime import date, timedelta
from threading import Lock
from typing import Any, Callable
import sys
from datetime import datetime

import pandas as pd
import tushare as ts
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy import inspect

from .env import Settings
from .sql_utils import (
    delete_older_than_chunked,
    ensure_index,
    normalize_yyyymmdd_date,
    upsert_df,
)
from .state import ensure_state_table, get_cursor, set_cursor
from .trade_cal import cutoff_by_last_open_days, date_to_yyyymmdd, fallback_cutoff, get_open_trade_dates, parse_date_any


class RateLimiter:
    """
    Simple per-process sliding-window rate limiter (e.g. 300 calls/min).
    """

    def __init__(self, max_calls_per_minute: int):
        self._max = int(max_calls_per_minute)
        self._lock = Lock()
        self._ts: deque[float] = deque()

    def wait(self) -> None:
        if self._max <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0
                while self._ts and self._ts[0] <= cutoff:
                    self._ts.popleft()
                if len(self._ts) < self._max:
                    self._ts.append(now)
                    return
                sleep_s = max(0.0, (self._ts[0] + 60.0) - now) + 0.02
            time.sleep(sleep_s)


_RATE_LIMITER: RateLimiter | None = None
_SETTINGS_ENV: dict[str, str] | None = None
_TUSHARE_QUERY_TIMEOUT_S: float = 45.0


def _on_tushare_retry(retry_state) -> None:
    # Log concise retry context to pinpoint which day/type is stalling.
    try:
        args = tuple(getattr(retry_state, "args", ()) or ())
        kwargs = dict(getattr(retry_state, "kwargs", {}) or {})
        api = str(args[1]) if len(args) > 1 else "unknown_api"
        attempt = int(getattr(retry_state, "attempt_number", 0) or 0)
        next_sleep_s = None
        na = getattr(retry_state, "next_action", None)
        if na is not None:
            next_sleep_s = getattr(na, "sleep", None)
        exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
        fields = [f"api={api}", f"attempt={attempt}"]
        for k in ["trade_date", "start_date", "end_date", "limit_type", "offset", "limit", "ts_code", "index_code"]:
            v = kwargs.get(k)
            if v not in (None, ""):
                fields.append(f"{k}={v}")
        if exc is not None:
            fields.append(f"err={type(exc).__name__}:{exc}")
        if next_sleep_s is not None:
            fields.append(f"next_sleep_s={float(next_sleep_s):.1f}")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] TUSHARE retry {' '.join(fields)}", file=sys.stderr, flush=True)
    except Exception:
        pass


def make_pro(settings: Settings):
    ts.set_token(settings.tushare_token)
    global _RATE_LIMITER
    global _SETTINGS_ENV
    _SETTINGS_ENV = dict(settings.env or {})
    try:
        max_cpm = int((settings.env.get("TUSHARE_MAX_CALLS_PER_MIN") or "300").strip())
    except Exception:
        max_cpm = 300
    global _TUSHARE_QUERY_TIMEOUT_S
    try:
        _TUSHARE_QUERY_TIMEOUT_S = float((settings.env.get("TUSHARE_QUERY_TIMEOUT_S") or "45").strip())
    except Exception:
        _TUSHARE_QUERY_TIMEOUT_S = 45.0
    _TUSHARE_QUERY_TIMEOUT_S = max(5.0, _TUSHARE_QUERY_TIMEOUT_S)
    if _RATE_LIMITER is None or getattr(_RATE_LIMITER, "_max", None) != max_cpm:
        _RATE_LIMITER = RateLimiter(max_calls_per_minute=max_cpm)
    return ts.pro_api()


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20), before_sleep=_on_tushare_retry)
def _pro_query(pro, api: str, **params) -> pd.DataFrame:
    if _RATE_LIMITER is not None:
        _RATE_LIMITER.wait()
    timeout_s = float(_TUSHARE_QUERY_TIMEOUT_S or 0.0)
    if timeout_s <= 0:
        return pro.query(api, **params)

    # Guard against upstream sockets hanging forever.
    def _raise_timeout(_signum, _frame):
        raise TimeoutError(f"Tushare query timeout api={api} timeout_s={timeout_s}")

    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_s)
    try:
        return pro.query(api, **params)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old_handler)


@dataclass(frozen=True)
class TableSpec:
    table_name: str
    primary_keys: list[str]
    cursor_col: str | None
    exchange: str | None = "SSE"  # used for trade_cal driven loops
    retention_open_days: int | None = None  # keep last N open trading days (delete older)
    by_trade_date: bool = False  # fetch per trade_date via trade_cal
    fetch_range: Callable[[Any, str, str], pd.DataFrame] | None = None  # (pro, start_yyyymmdd, end_yyyymmdd) -> df
    fetch_day: Callable[[Any, str], pd.DataFrame] | None = None  # (pro, trade_date_yyyymmdd) -> df
    post: Callable[[pd.DataFrame], pd.DataFrame] | None = None


LIMIT_MAX = 6000


def _pro_query_paged(pro, api: str, *, limit: int, **params) -> pd.DataFrame:
    """
    Query with (limit, offset) paging until exhausted.
    Keeps each API call under a configured row cap (e.g. 6000 rows/call).
    """
    limit = int(limit)
    offset = 0
    frames: list[pd.DataFrame] = []
    while True:
        df = _pro_query(pro, api, limit=limit, offset=offset, **params)
        if df is None or df.empty:
            break
        frames.append(df)
        if int(len(df)) < limit:
            break
        offset += limit
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _post_stock_basic(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "list_date")
    df = normalize_yyyymmdd_date(df, "delist_date")
    return df


def _post_trade_cal(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "cal_date")
    if "pretrade_date" in df.columns:
        df = normalize_yyyymmdd_date(df, "pretrade_date")
    if "is_open" in df.columns:
        df["is_open"] = pd.to_numeric(df["is_open"], errors="coerce").fillna(0).astype("int64")
    return df


def _post_daily_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "trade_date")
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce") * 1000.0  # 千元 -> 元
    if "vol" in df.columns:
        df["vol_share"] = pd.to_numeric(df["vol"], errors="coerce") * 100.0  # 手 -> 股
        df = df.drop(columns=["vol"])
    return df


def _post_trade_date(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_yyyymmdd_date(df, "trade_date")


def _post_moneyflow_hsgt(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "trade_date")
    # Tushare often returns these numeric fields as strings; normalize for easier analytics.
    for c in ["ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _post_index_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "trade_date")
    if "vol" in df.columns:
        df["vol_share"] = pd.to_numeric(df["vol"], errors="coerce") * 100.0
        df = df.drop(columns=["vol"])
    return df


def _post_st_list(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "start_date")
    df = normalize_yyyymmdd_date(df, "end_date")
    df = normalize_yyyymmdd_date(df, "ann_date")
    return df


def _post_index_basic(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "base_date")
    df = normalize_yyyymmdd_date(df, "list_date")
    df = normalize_yyyymmdd_date(df, "exp_date")
    return df


def _post_share_float(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "ann_date")
    df = normalize_yyyymmdd_date(df, "float_date")
    return df


def _post_dividend(df: pd.DataFrame) -> pd.DataFrame:
    for c in [
        "end_date",
        "ann_date",
        "record_date",
        "ex_date",
        "pay_date",
        "div_listdate",
        "imp_ann_date",
        "base_date",
    ]:
        df = normalize_yyyymmdd_date(df, c)
    return df


def _post_index_member_all(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_yyyymmdd_date(df, "in_date")
    df = normalize_yyyymmdd_date(df, "out_date")
    return df


def _merge_daily_raw(daily: pd.DataFrame, basic: pd.DataFrame) -> pd.DataFrame:
    if daily.empty and basic.empty:
        return pd.DataFrame()
    if daily.empty:
        return basic
    if basic.empty:
        return daily
    overlap = set(daily.columns) & set(basic.columns)
    overlap -= {"ts_code", "trade_date"}
    basic2 = basic.drop(columns=sorted(overlap), errors="ignore")
    return daily.merge(basic2, on=["ts_code", "trade_date"], how="outer")


def _fetch_stock_basic(pro, _sd: str, _ed: str) -> pd.DataFrame:
    return _pro_query(pro, "stock_basic", exchange="", list_status="L")


def _fetch_trade_cal(pro, sd: str, ed: str) -> pd.DataFrame:
    return _pro_query(pro, "trade_cal", exchange="SSE", start_date=sd, end_date=ed)


def _fetch_daily_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "daily", trade_date=td)


def _fetch_daily_basic_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "daily_basic", trade_date=td)


def _fetch_adj_factor_day(pro, td: str) -> pd.DataFrame:
    # adj_factor supports trade_date in Tushare.
    return _pro_query(pro, "adj_factor", trade_date=td)


def _fetch_moneyflow_dc_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "moneyflow_dc", trade_date=td)


def _fetch_moneyflow_ind_dc_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "moneyflow_ind_dc", trade_date=td)


def _fetch_moneyflow_mkt_dc_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "moneyflow_mkt_dc", trade_date=td)


def _fetch_moneyflow_hsgt_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "moneyflow_hsgt", trade_date=td)


def _fetch_suspend_d_day(pro, td: str) -> pd.DataFrame:
    return _pro_query(pro, "suspend_d", trade_date=td)


def _fetch_stk_namechange_range(pro, sd: str, ed: str) -> pd.DataFrame:
    # Tushare: query namechange (doc_id=397 is stk_namechange in docs, but API name is `namechange`)
    return _pro_query(pro, "namechange", start_date=sd, end_date=ed)


def _fetch_index_daily_range(pro, sd: str, ed: str) -> pd.DataFrame:
    # Some environments see long date-range calls hang; chunk into smaller ranges.
    from datetime import datetime, timedelta

    start = datetime.strptime(sd, "%Y%m%d").date()
    end = datetime.strptime(ed, "%Y%m%d").date()
    step = timedelta(days=90)

    frames = []
    for code in ["000001.SH", "000300.SH", "399001.SZ", "399006.SZ"]:
        cur = start
        while cur <= end:
            cur_end = min(end, cur + step)
            df = _pro_query(
                pro,
                "index_daily",
                ts_code=code,
                start_date=cur.strftime("%Y%m%d"),
                end_date=cur_end.strftime("%Y%m%d"),
            )
            if df is not None and not df.empty:
                frames.append(df)
            cur = cur_end + timedelta(days=1)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_index_basic(_pro, _sd: str, _ed: str) -> pd.DataFrame:
    # doc_id=94 index_basic
    return _pro_query(_pro, "index_basic")


def _fetch_index_classify(_pro, _sd: str, _ed: str) -> pd.DataFrame:
    # doc_id=181 index_classify
    # PDF里没有列出参数要求，默认不带参数调用；如遇到必填参数，可在这里补充。
    return _pro_query(_pro, "index_classify")


def _fetch_index_member_all(_pro, _sd: str, _ed: str) -> pd.DataFrame:
    # doc_id=335 index_member_all: 单次最大2000行
    return _pro_query_paged(_pro, "index_member_all", limit=2000, is_new="Y")


def _fetch_index_weight_range(_pro, sd: str, ed: str) -> pd.DataFrame:
    # doc_id=96 index_weight: 指数成分和权重（月度）。
    # 为避免拉全市场指数导致数据爆炸，这里只抓取配置的指数代码集合。
    env = _SETTINGS_ENV or {}
    codes_raw = (env.get("INDEX_WEIGHT_CODES") or "").strip()
    if not codes_raw:
        return pd.DataFrame()
    codes = [x.strip() for x in codes_raw.split(",") if x.strip()]
    if not codes:
        return pd.DataFrame()

    start = pd.to_datetime(sd, format="%Y%m%d").date().replace(day=1)
    end = pd.to_datetime(ed, format="%Y%m%d").date()

    def month_start(d: date) -> date:
        return d.replace(day=1)

    def next_month(d: date) -> date:
        if d.month == 12:
            return date(d.year + 1, 1, 1)
        return date(d.year, d.month + 1, 1)

    frames: list[pd.DataFrame] = []
    cur = month_start(start)
    while cur <= end:
        nm = next_month(cur)
        rng_start = cur.strftime("%Y%m%d")
        rng_end = (min(end, nm - timedelta(days=1))).strftime("%Y%m%d")
        for code in codes:
            df = _pro_query_paged(_pro, "index_weight", limit=LIMIT_MAX, index_code=code, start_date=rng_start, end_date=rng_end)
            if df is not None and not df.empty:
                frames.append(df)
        cur = nm
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_share_float_range(_pro, sd: str, ed: str) -> pd.DataFrame:
    # doc_id=160 share_float: 单次最大6000行
    return _pro_query_paged(_pro, "share_float", limit=LIMIT_MAX, start_date=sd, end_date=ed)


def _fetch_stk_limit_day(_pro, td: str) -> pd.DataFrame:
    # doc_id=183 stk_limit: 单次最多约5800行
    return _pro_query(_pro, "stk_limit", trade_date=td, limit=5800, offset=0)


def _fetch_limit_list_d_day(_pro, td: str) -> pd.DataFrame:
    # doc_id=298 limit_list_d: 单次最大2500行
    frames: list[pd.DataFrame] = []
    for lt in ["U", "D", "Z"]:
        try:
            df = _pro_query_paged(_pro, "limit_list_d", limit=2500, trade_date=td, limit_type=lt)
        except Exception as ex:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{ts}] AS_MASTER.limit_list_d fetch_failed trade_date={td} limit_type={lt} err={type(ex).__name__}:{ex}",
                file=sys.stderr,
                flush=True,
            )
            raise
        if df is not None and not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_dividend_range(_pro, sd: str, ed: str) -> pd.DataFrame:
    # doc_id=103 dividend：PDF里只有 ann_date/record_date/ex_date/imp_ann_date 等点查询参数
    # 这里用 ann_date 按自然日循环，适合滚动窗口+增量，不建议一次性拉全历史。
    start = pd.to_datetime(sd, format="%Y%m%d").date()
    end = pd.to_datetime(ed, format="%Y%m%d").date()
    frames: list[pd.DataFrame] = []
    cur = start
    while cur <= end:
        ann = cur.strftime("%Y%m%d")
        df = _pro_query_paged(_pro, "dividend", limit=LIMIT_MAX, ann_date=ann)
        if df is not None and not df.empty:
            frames.append(df)
        cur += timedelta(days=1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


MASTER_TABLES: dict[str, TableSpec] = {
    "stock_basic": TableSpec(
        table_name="stock_basic",
        primary_keys=["ts_code"],
        cursor_col=None,
        retention_open_days=None,
        by_trade_date=False,
        fetch_range=_fetch_stock_basic,
        post=_post_stock_basic,
    ),
    "trade_cal": TableSpec(
        table_name="trade_cal",
        primary_keys=["exchange", "cal_date"],
        cursor_col="cal_date",
        retention_open_days=None,  # do not delete trade_cal
        by_trade_date=False,
        fetch_range=_fetch_trade_cal,
        post=_post_trade_cal,
    ),
    # Index metadata + classification (small tables; keep all rows).
    "index_basic": TableSpec(
        table_name="index_basic",
        primary_keys=["ts_code"],
        cursor_col=None,
        retention_open_days=None,
        by_trade_date=False,
        fetch_range=_fetch_index_basic,
        post=_post_index_basic,
    ),
    "index_classify": TableSpec(
        table_name="index_classify",
        primary_keys=["index_code"],
        cursor_col=None,
        retention_open_days=None,
        by_trade_date=False,
        fetch_range=_fetch_index_classify,
        post=None,
    ),
    "index_member_all": TableSpec(
        table_name="index_member_all",
        # Minimal local DB: keep latest membership only; use (ts_code, l3_code) as stable non-null key.
        primary_keys=["ts_code", "l3_code"],
        cursor_col=None,
        retention_open_days=None,
        by_trade_date=False,
        fetch_range=_fetch_index_member_all,
        post=_post_index_member_all,
    ),
    # Keep last 500 open trading days
    "daily_raw": TableSpec(
        table_name="daily_raw",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=lambda pro, td: _merge_daily_raw(_fetch_daily_day(pro, td), _fetch_daily_basic_day(pro, td)),
        post=_post_daily_raw,
    ),
    "adj_factor": TableSpec(
        table_name="adj_factor",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_adj_factor_day,
        post=_post_trade_date,
    ),
    "index_daily": TableSpec(
        table_name="index_daily",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=False,  # few rows, range is fine
        fetch_range=_fetch_index_daily_range,
        post=_post_index_daily,
    ),
    "index_weight": TableSpec(
        table_name="index_weight",
        primary_keys=["index_code", "con_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=2000,
        by_trade_date=False,
        fetch_range=_fetch_index_weight_range,
        post=_post_trade_date,
    ),
    "stk_limit": TableSpec(
        table_name="stk_limit",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_stk_limit_day,
        post=_post_trade_date,
    ),
    "limit_list_d": TableSpec(
        table_name="limit_list_d",
        primary_keys=["ts_code", "trade_date", "limit"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_limit_list_d_day,
        post=_post_trade_date,
    ),
    "share_float": TableSpec(
        table_name="share_float",
        primary_keys=["ts_code", "ann_date", "float_date", "holder_name", "share_type"],
        cursor_col="float_date",
        retention_open_days=500,
        by_trade_date=False,
        fetch_range=_fetch_share_float_range,
        post=_post_share_float,
    ),
    "dividend": TableSpec(
        table_name="dividend",
        # record_date/ex_date can be missing for未实施的预案; avoid nullable PK parts.
        primary_keys=["ts_code", "ann_date", "end_date"],
        cursor_col="ann_date",
        retention_open_days=500,
        by_trade_date=False,
        fetch_range=_fetch_dividend_range,
        post=_post_dividend,
    ),
    "moneyflow_ind": TableSpec(
        table_name="moneyflow_ind",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_moneyflow_dc_day,
        post=_post_trade_date,
    ),
    "moneyflow_sector": TableSpec(
        table_name="moneyflow_sector",
        primary_keys=["ts_code", "trade_date", "content_type"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_moneyflow_ind_dc_day,
        post=_post_trade_date,
    ),
    "moneyflow_mkt": TableSpec(
        table_name="moneyflow_mkt",
        primary_keys=["trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_moneyflow_mkt_dc_day,
        post=_post_trade_date,
    ),
    "moneyflow_hsgt": TableSpec(
        table_name="moneyflow_hsgt",
        primary_keys=["trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_moneyflow_hsgt_day,
        post=_post_moneyflow_hsgt,
    ),
    "st_list": TableSpec(
        table_name="st_list",
        primary_keys=["ts_code", "start_date"],
        cursor_col="start_date",
        retention_open_days=500,
        by_trade_date=False,
        fetch_range=_fetch_stk_namechange_range,
        post=_post_st_list,
    ),
    "suspend_d": TableSpec(
        table_name="suspend_d",
        primary_keys=["ts_code", "trade_date"],
        cursor_col="trade_date",
        retention_open_days=500,
        by_trade_date=True,
        fetch_day=_fetch_suspend_d_day,
        post=_post_trade_date,
    ),
}


def _retention_cutoff(engine_master, *, exchange: str, end: date, keep_open_days: int) -> date:
    cutoff = cutoff_by_last_open_days(engine_master, exchange=exchange, end=end, keep_open_days=keep_open_days)
    # Only delete based on real trading days. If trade_cal isn't long enough, skip retention delete.
    if cutoff is None:
        raise RuntimeError(
            f"trade_cal doesn't have enough open days to compute retention cutoff for keep_open_days={keep_open_days}. "
            "Run `update --tables trade_cal` with a longer history first."
        )
    return cutoff


def update_table(
    *,
    settings: Settings,
    engine_master,
    cluster: str,
    spec: TableSpec,
    start_date: date | None,
    end_date: date | None,
    lookback_days: int = 0,
    ts_codes: list[str] | None = None,
    write_mode: str = "upsert",
    no_delete: bool = False,
) -> dict[str, Any]:
    ensure_state_table(engine_master)
    pro = make_pro(settings)

    end = end_date or date.today()

    # Compute retention cutoff if needed (rolling window).
    cutoff = None
    if (not no_delete) and spec.retention_open_days:
        cutoff = _retention_cutoff(engine_master, exchange=spec.exchange or "SSE", end=end, keep_open_days=spec.retention_open_days)

    cur_raw = get_cursor(engine_master, cluster, spec.table_name, spec.cursor_col) if spec.cursor_col else None
    cur_date = parse_date_any(cur_raw) if cur_raw else None

    # Determine start.
    if start_date is not None:
        start = start_date
    elif cur_date is not None:
        start = cur_date + timedelta(days=1)
    else:
        # First run: backfill only the retention window if configured; else a conservative 20y.
        start = cutoff if cutoff else (end - timedelta(days=365 * 20))

    if cutoff:
        start = max(start, cutoff)

    if lookback_days and lookback_days > 0:
        start = min(start, end - timedelta(days=int(lookback_days) * 2))
        if cutoff:
            start = max(start, cutoff)

    rows = 0
    affected = 0
    max_cursor: date | None = None
    max_cursor_with_data: date | None = None

    if spec.by_trade_date:
        if spec.fetch_day is None:
            raise RuntimeError(f"{spec.table_name} is by_trade_date but fetch_day is missing")
        tds = get_open_trade_dates(engine_master, exchange=spec.exchange or "SSE", start=start, end=end)
        # Progress log to stderr so CLI JSON output (stdout) remains machine-readable.
        # These tables can be slow (many trade dates), so emit periodic logs for visibility.
        try:
            progress_every_td = int((settings.env.get("PROGRESS_LOG_EVERY_TD") or "20").strip())
        except Exception:
            progress_every_td = 20
        try:
            progress_interval_s = float((settings.env.get("PROGRESS_LOG_INTERVAL_S") or "30").strip())
        except Exception:
            progress_interval_s = 30.0
        progress_every_td = max(1, progress_every_td)
        progress_interval_s = max(5.0, progress_interval_s)

        start_ts = time.monotonic()
        last_log_ts = start_ts
        total = int(len(tds))
        ts0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{ts0}] AS_MASTER.{spec.table_name} by_trade_date start={start.isoformat()} end={end.isoformat()} trade_dates={total}",
            file=sys.stderr,
            flush=True,
        )

        # Batch multiple trade_dates into one upsert to reduce RU (fewer transactions).
        buf: list[pd.DataFrame] = []
        buf_rows = 0
        flush_rows = 50000
        for i, d in enumerate(tds, start=1):
            td = date_to_yyyymmdd(d)
            if spec.table_name == "daily_raw" and ts_codes:
                daily = _fetch_daily_day_codes(pro, td, ts_codes)
                basic = _fetch_daily_basic_day_codes(pro, td, ts_codes)
                df = _merge_daily_raw(daily, basic)
            else:
                df = spec.fetch_day(pro, td)
            if spec.post:
                df = spec.post(df)
            if not df.empty:
                buf.append(df)
                buf_rows += int(len(df))
                rows += int(len(df))
                max_cursor_with_data = d if (max_cursor_with_data is None or d > max_cursor_with_data) else max_cursor_with_data
            max_cursor = d if (max_cursor is None or d > max_cursor) else max_cursor

            if buf_rows >= flush_rows:
                all_df = pd.concat(buf, ignore_index=True)
                affected += int(upsert_df(engine_master, spec.table_name, all_df, spec.primary_keys, mode=write_mode))
                buf = []
                buf_rows = 0

                # Log flushes since they can be large and take time.
                now = time.monotonic()
                tsf = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elapsed_s = int(now - start_ts)
                print(
                    f"[{tsf}] AS_MASTER.{spec.table_name} flush i={i}/{total} td={td} rows={rows} affected={affected} elapsed_s={elapsed_s}",
                    file=sys.stderr,
                    flush=True,
                )
                last_log_ts = now

            # Periodic progress logs (not too chatty).
            now = time.monotonic()
            if i == total or (i % progress_every_td == 0) or (now - last_log_ts >= progress_interval_s):
                tsp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elapsed_s = max(1.0, now - start_ts)
                rate = float(i) / elapsed_s
                eta_s = int((total - i) / rate) if rate > 0 else -1
                print(
                    f"[{tsp}] AS_MASTER.{spec.table_name} progress i={i}/{total} td={td} rows={rows} buf_rows={buf_rows} elapsed_s={int(elapsed_s)} eta_s={eta_s}",
                    file=sys.stderr,
                    flush=True,
                )
                last_log_ts = now
        if buf_rows > 0:
            tsf0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            elapsed_s = int(time.monotonic() - start_ts)
            print(
                f"[{tsf0}] AS_MASTER.{spec.table_name} final_flush rows={buf_rows} elapsed_s={elapsed_s}",
                file=sys.stderr,
                flush=True,
            )
            all_df = pd.concat(buf, ignore_index=True)
            affected += int(upsert_df(engine_master, spec.table_name, all_df, spec.primary_keys, mode=write_mode))
            tsf1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            elapsed_s = int(time.monotonic() - start_ts)
            print(
                f"[{tsf1}] AS_MASTER.{spec.table_name} final_flush_done affected={affected} elapsed_s={elapsed_s}",
                file=sys.stderr,
                flush=True,
            )
    else:
        if spec.fetch_range is None:
            raise RuntimeError(f"{spec.table_name} is range-based but fetch_range is missing")
        df = spec.fetch_range(pro, date_to_yyyymmdd(start), date_to_yyyymmdd(end))
        if spec.post:
            df = spec.post(df)
        rows = int(len(df))
        affected = int(upsert_df(engine_master, spec.table_name, df, spec.primary_keys, mode=write_mode)) if rows else 0
        if spec.cursor_col and spec.cursor_col in df.columns and not df.empty:
            series = df[spec.cursor_col].dropna()
            if not series.empty:
                mx = series.max()
                if isinstance(mx, date):
                    max_cursor = mx
                    max_cursor_with_data = mx
                else:
                    # If cursor col isn't a date type, store as str.
                    max_cursor = None

    # Update cursor.
    cursor_value = None
    if spec.cursor_col:
        # Cursor rules:
        # - If we saw any data with a usable cursor value, advance to that max.
        # - Else if we wrote rows but cannot infer a cursor value (range-based without cursor col), advance to `end`.
        # - Else keep previous cursor so transient API issues don't "skip" data forever.
        if max_cursor_with_data is not None:
            cursor_value = max_cursor_with_data.isoformat()
        elif rows > 0:
            cursor_value = end.isoformat()
        else:
            cursor_value = cur_raw

        # Never move cursor backwards.
        if cur_date and cursor_value:
            try:
                new_d = parse_date_any(cursor_value)
                if new_d < cur_date:
                    cursor_value = cur_date.isoformat()
            except Exception:
                pass
        # Avoid creating a state row with NULL cursor_value on a first run that fetched nothing.
        if cursor_value is not None or cur_raw is not None:
            set_cursor(engine_master, cluster, spec.table_name, spec.cursor_col, cursor_value)

    # Enforce retention (delete old).
    deleted = 0
    if (not no_delete) and cutoff and spec.retention_open_days:
        tsr0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{tsr0}] AS_MASTER.{spec.table_name} retention cutoff={cutoff.isoformat()} start",
            file=sys.stderr,
            flush=True,
        )
        # If table was never created (e.g. upstream returned empty frames), skip retention delete.
        if not inspect(engine_master).has_table(spec.table_name):
            return {
                "table": spec.table_name,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "rows_fetched": rows,
                "rows_affected": affected,
                "cursor_col": spec.cursor_col,
                "cursor_value": cursor_value,
                "retention_cutoff": cutoff.isoformat() if cutoff else None,
                "rows_deleted": 0,
            }
        # Pick date column to delete on.
        date_col = "trade_date"
        if spec.table_name == "st_list":
            date_col = "start_date"
        if spec.table_name == "trade_cal":
            date_col = "cal_date"
        if spec.table_name == "share_float":
            date_col = "float_date"
        if spec.table_name == "dividend":
            date_col = "ann_date"
        # Ensure index exists so retention deletes don't full-scan (saves RU).
        try:
            tsi0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{tsi0}] AS_MASTER.{spec.table_name} ensuring index idx_{spec.table_name}_{date_col} ...",
                file=sys.stderr,
                flush=True,
            )
            ensure_index(engine_master, spec.table_name, f"idx_{spec.table_name}_{date_col}", [date_col])
            tsi1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{tsi1}] AS_MASTER.{spec.table_name} ensure_index done",
                file=sys.stderr,
                flush=True,
            )
        except Exception:
            pass
        deleted = delete_older_than_chunked(engine_master, spec.table_name, date_col, cutoff)
        tsr1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{tsr1}] AS_MASTER.{spec.table_name} retention done deleted={int(deleted)}",
            file=sys.stderr,
            flush=True,
        )

    return {
        "table": spec.table_name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "rows_fetched": rows,
        "rows_affected": affected,
        "cursor_col": spec.cursor_col,
        "cursor_value": cursor_value,
        "retention_cutoff": cutoff.isoformat() if cutoff else None,
        "rows_deleted": int(deleted),
    }


def update_master(
    *,
    settings: Settings,
    engine_master,
    tables: list[str],
    start_date: date | None,
    end_date: date | None,
    lookback_days: int = 0,
    ts_codes: list[str] | None = None,
    write_mode: str = "upsert",
    no_delete: bool = False,
) -> list[dict[str, Any]]:
    # Always update trade_cal first if any table needs retention/trading-day loops.
    ordered: list[str] = []
    if "trade_cal" not in tables:
        tables = ["trade_cal"] + [t for t in tables if t != "trade_cal"]
    ordered.append("trade_cal")

    # Keep user order but without duplicates, and ensure stock_basic early.
    for t in ["stock_basic"]:
        if t in tables and t not in ordered:
            ordered.append(t)
    for t in tables:
        if t not in ordered:
            ordered.append(t)

    out = []
    # Ensure trade_cal has enough history to compute retention cutoffs for *all* selected tables.
    # Approximation: 1 open day ~= 2 calendar days; add an extra buffer.
    max_keep_open_days = 0
    for t in ordered:
        spec = MASTER_TABLES.get(t)
        if spec is None:
            continue
        if spec.retention_open_days:
            max_keep_open_days = max(max_keep_open_days, int(spec.retention_open_days))
    for name in ordered:
        spec = MASTER_TABLES.get(name)
        if spec is None:
            raise RuntimeError(f"Unknown table: {name}")

        # Ensure trade_cal has enough history to compute "last N trading days" retention cutoffs.
        # Ignore user `since` if it would make trade_cal too short.
        tc_start = start_date
        if name == "trade_cal":
            # Default to 5y, but extend when other tables require longer retention windows (e.g. 2000 open days).
            # calendar_years ~= keep_open_days/250 trading_days_per_year, plus buffer.
            end_ref = end_date or date.today()
            years = max(5, int(max_keep_open_days / 200) + 2) if max_keep_open_days > 0 else 5
            tc_min_start = end_ref - timedelta(days=365 * years)
            tc_start = tc_min_start if (tc_start is None or tc_start > tc_min_start) else tc_start

        # Progress log to stderr so CLI JSON output (stdout) remains machine-readable.
        ts0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts0}] updating AS_MASTER.{name} ...", file=sys.stderr, flush=True)

        res = update_table(
            settings=settings,
            engine_master=engine_master,
            cluster="AS_MASTER",
            spec=spec,
            start_date=tc_start if name == "trade_cal" else start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            ts_codes=ts_codes,
            write_mode=write_mode,
            no_delete=no_delete,
        )
        out.append(res)

        ts1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{ts1}] done AS_MASTER.{name} fetched={res.get('rows_fetched')} affected={res.get('rows_affected')} deleted={res.get('rows_deleted')}",
            file=sys.stderr,
            flush=True,
        )
    return out
def _fetch_daily_day_codes(pro, td: str, ts_codes: list[str]) -> pd.DataFrame:
    frames = []
    for code in ts_codes:
        frames.append(_pro_query(pro, "daily", ts_code=code, trade_date=td))
    frames = [f for f in frames if f is not None and not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_daily_basic_day_codes(pro, td: str, ts_codes: list[str]) -> pd.DataFrame:
    frames = []
    for code in ts_codes:
        frames.append(_pro_query(pro, "daily_basic", ts_code=code, trade_date=td))
    frames = [f for f in frames if f is not None and not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
