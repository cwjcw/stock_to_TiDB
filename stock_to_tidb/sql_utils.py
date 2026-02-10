from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import Date, DateTime, Float, Integer, MetaData, String, Table, Text, inspect, text
from sqlalchemy.dialects.mysql import BIGINT, DECIMAL, insert as mysql_insert
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class EnsureTableResult:
    created: bool
    added_columns: list[str]


def _is_nan(v: Any) -> bool:
    return isinstance(v, float) and math.isnan(v)


def df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        clean: dict[str, Any] = {}
        for k, v in row.items():
            if v is None or _is_nan(v):
                clean[k] = None
            else:
                clean[k] = v
        out.append(clean)
    return out


def normalize_yyyymmdd_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    s = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
    df[col] = pd.to_datetime(s, format="%Y%m%d", errors="coerce").dt.date
    return df


def normalize_yyyymmddhhmmss_dt(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    # xtquant may include millisecond suffix like "20260205093000.000"
    # or pandas may stringify floats like "20260205093000.0".
    s = (
        df[col]
        .astype(str)
        .str.replace(r"\.\d+$", "", regex=True)
        .str.replace(r"\s+", "", regex=True)
        .str.zfill(14)
    )
    dt = pd.to_datetime(s, format="%Y%m%d%H%M%S", errors="coerce")
    # Fallback for unexpected formats (still deterministic, but better than all-NaT).
    if dt.isna().all():
        dt = pd.to_datetime(df[col], errors="coerce")
    df[col] = dt
    return df


def _infer_type(col: str, series: pd.Series):
    c = col.lower()
    # Moneyflow (hsgt) numbers are returned as strings by Tushare in some environments.
    # Use DECIMAL in schema to make analytics safe and avoid string casts in SQL.
    if c in {"ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"}:
        return DECIMAL(24, 2)
    if c in {"ts_code", "symbol", "exchange", "market", "content_type"}:
        return String(32)
    if c.endswith("_date") or c in {"cal_date", "trade_date", "list_date", "delist_date", "start_date", "end_date"}:
        return Date()
    if c in {"time", "trade_time"}:
        return DateTime()

    if pd.api.types.is_integer_dtype(series.dtype):
        return BIGINT()
    if pd.api.types.is_float_dtype(series.dtype):
        return Float()
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return DateTime()

    # Default string; choose TEXT if long.
    try:
        max_len = int(series.dropna().astype(str).map(len).max())
    except Exception:
        max_len = 0
    if max_len <= 64:
        return String(64)
    if max_len <= 255:
        return String(255)
    return Text()


def ensure_table_from_df(engine: Engine, table_name: str, df: pd.DataFrame, primary_keys: list[str]) -> EnsureTableResult:
    for pk in primary_keys:
        if pk not in df.columns:
            raise ValueError(f"Primary key column `{pk}` missing from df for `{table_name}`")

    insp = inspect(engine)
    if not insp.has_table(table_name):
        md = MetaData()
        cols = []
        from sqlalchemy import Column

        for col in df.columns:
            cols.append(Column(col, _infer_type(col, df[col]), primary_key=(col in primary_keys)))
        Table(table_name, md, *cols, mysql_charset="utf8mb4")
        md.create_all(engine)
        return EnsureTableResult(created=True, added_columns=[])

    existing = {c["name"] for c in insp.get_columns(table_name)}
    missing = [c for c in df.columns if c not in existing]
    if not missing:
        return EnsureTableResult(created=False, added_columns=[])

    stmts = []
    for col in missing:
        typ = _infer_type(col, df[col])
        typ_sql = typ.compile(dialect=engine.dialect)
        stmts.append(f"ADD COLUMN `{col}` {typ_sql}")
    sql = f"ALTER TABLE `{table_name}` " + ", ".join(stmts)
    with engine.begin() as conn:
        conn.execute(text(sql))
    return EnsureTableResult(created=False, added_columns=missing)


_TABLE_CACHE: dict[tuple[int, str], Table] = {}


def _get_table(engine: Engine, table_name: str) -> Table:
    key = (id(engine), table_name)
    tbl = _TABLE_CACHE.get(key)
    if tbl is not None:
        return tbl
    tbl = Table(table_name, MetaData(), autoload_with=engine)
    _TABLE_CACHE[key] = tbl
    return tbl


def ensure_index(engine: Engine, table_name: str, index_name: str, columns: list[str]) -> bool:
    """
    Create a secondary index if missing.
    Returns True if created.
    """
    cols_sql = ", ".join(f"`{c}`" for c in columns)
    with engine.begin() as conn:
        rows = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
        existing = {str(r[2]) for r in rows}  # Key_name
        if index_name in existing:
            return False
        conn.execute(text(f"CREATE INDEX `{index_name}` ON `{table_name}` ({cols_sql})"))
        return True


def upsert_df(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    primary_keys: list[str],
    chunk_size: int = 2000,
    mode: str = "upsert",  # upsert|ignore
) -> int:
    if df.empty:
        return 0
    ensure_table_from_df(engine, table_name, df, primary_keys)
    table = _get_table(engine, table_name)

    records = df_to_records(df)
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            batch = records[i : i + chunk_size]
            stmt = mysql_insert(table).values(batch)
            if mode == "ignore":
                stmt = stmt.prefix_with("IGNORE")
            else:
                update_cols = {c.name: stmt.inserted[c.name] for c in table.columns if c.name not in primary_keys}
                stmt = stmt.on_duplicate_key_update(**update_cols)
            res = conn.execute(stmt)
            total += int(res.rowcount or 0)
    return total


def delete_older_than(engine: Engine, table_name: str, col: str, cutoff: date | Any) -> int:
    sql = text(f"DELETE FROM `{table_name}` WHERE `{col}` < :cutoff")
    with engine.begin() as conn:
        res = conn.execute(sql, {"cutoff": cutoff})
        return int(res.rowcount or 0)


def delete_older_than_chunked(
    engine: Engine,
    table_name: str,
    col: str,
    cutoff: date | Any,
    *,
    chunk_rows: int = 20000,
    max_loops: int = 5000,
) -> int:
    """
    Delete in chunks to reduce RU spikes/transaction size.
    """
    total = 0
    for _ in range(max_loops):
        with engine.begin() as conn:
            res = conn.execute(
                text(f"DELETE FROM `{table_name}` WHERE `{col}` < :cutoff LIMIT {int(chunk_rows)}"),
                {"cutoff": cutoff},
            )
            n = int(res.rowcount or 0)
        total += n
        if n <= 0:
            break
    return total
