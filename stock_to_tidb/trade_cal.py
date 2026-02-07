from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine


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
    with engine.begin() as conn:
        rows = conn.execute(sql, {"exchange": exchange, "start": start, "end": end}).fetchall()
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
    with engine.begin() as conn:
        rows = conn.execute(sql, {"exchange": exchange, "end": end}).fetchall()
    if len(rows) < keep_open_days:
        return None
    # rows are desc; cutoff is min among them.
    return min(r[0] for r in rows)


def fallback_cutoff(end: date, keep_open_days: int) -> date:
    # Approximate: 5 open days per week -> buffer * 2 calendar days.
    return end - timedelta(days=int(keep_open_days) * 2)

