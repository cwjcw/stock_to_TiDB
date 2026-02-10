from __future__ import annotations

from datetime import date, datetime, timedelta
import time

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError


def _is_transient_mysql_disconnect(e: Exception) -> bool:
    """
    Detect retryable MySQL/TiDB connection drop conditions.
    We intentionally keep this conservative to avoid masking real config/auth errors.
    """
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


def parse_date_any(s: str) -> date:
    s = s.strip()
    if "-" in s:
        return date.fromisoformat(s)
    return datetime.strptime(s, "%Y%m%d").date()


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_open_trade_dates(engine: Engine, *, exchange: str, start: date, end: date) -> list[date]:
    sql = text(
        "SELECT cal_date FROM trade_cal "
        "WHERE exchange=:exchange AND is_open=1 AND cal_date BETWEEN :start AND :end "
        "ORDER BY cal_date"
    )
    # TiDB Cloud connections can transiently drop (e.g. network hiccups).
    # These queries are read-only and safe to retry.
    last_err: Exception | None = None
    for attempt in range(5):
        try:
            with engine.begin() as conn:
                rows = conn.execute(sql, {"exchange": exchange, "start": start, "end": end}).fetchall()
            last_err = None
            break
        except (OperationalError, DBAPIError) as e:
            if not _is_transient_mysql_disconnect(e):
                raise
            last_err = e
            time.sleep(min(8.0, 0.5 * (2**attempt)))
    if last_err is not None:
        raise last_err
    return [r[0] for r in rows]


def cutoff_by_last_open_days(engine: Engine, *, exchange: str, end: date, keep_open_days: int) -> date | None:
    """
    Return the earliest date among the last N open trading days (inclusive).
    If trade_cal doesn't have enough rows, return None.
    """
    keep_open_days = int(keep_open_days)
    if keep_open_days <= 0:
        return None

    # LIMIT cannot be bound reliably; inline integer.
    sql = text(
        "SELECT cal_date FROM trade_cal "
        "WHERE exchange=:exchange AND is_open=1 AND cal_date <= :end "
        f"ORDER BY cal_date DESC LIMIT {keep_open_days}"
    )
    last_err: Exception | None = None
    for attempt in range(5):
        try:
            with engine.begin() as conn:
                rows = conn.execute(sql, {"exchange": exchange, "end": end}).fetchall()
            last_err = None
            break
        except (OperationalError, DBAPIError) as e:
            if not _is_transient_mysql_disconnect(e):
                raise
            last_err = e
            time.sleep(min(8.0, 0.5 * (2**attempt)))
    if last_err is not None:
        raise last_err
    if len(rows) < keep_open_days:
        return None
    # rows are desc; cutoff is min among them.
    return min(r[0] for r in rows)


def fallback_cutoff(end: date, keep_open_days: int) -> date:
    # Approximate: 5 open days per week -> buffer * 2 calendar days.
    return end - timedelta(days=int(keep_open_days) * 2)
