from __future__ import annotations

from sqlalchemy import Column, DateTime, MetaData, String, Table, text
from sqlalchemy.engine import Engine


def ensure_state_table(engine: Engine) -> None:
    md = MetaData()
    Table(
        "etl_state",
        md,
        Column("cluster", String(32), primary_key=True),
        Column("table_name", String(64), primary_key=True),
        Column("cursor_col", String(64), primary_key=True),
        Column("cursor_value", String(64), nullable=True),
        Column("updated_at", DateTime, nullable=False),
        mysql_charset="utf8mb4",
    )
    md.create_all(engine)

    # Best-effort: set default updated_at. Some TiDB variants may reject ON UPDATE; ignore.
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE etl_state "
                    "MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
                    "ON UPDATE CURRENT_TIMESTAMP"
                )
            )
    except Exception:
        pass


def get_cursor(engine: Engine, cluster: str, table_name: str, cursor_col: str) -> str | None:
    sql = text(
        "SELECT cursor_value FROM etl_state "
        "WHERE cluster=:cluster AND table_name=:table_name AND cursor_col=:cursor_col"
    )
    with engine.begin() as conn:
        row = conn.execute(sql, {"cluster": cluster, "table_name": table_name, "cursor_col": cursor_col}).fetchone()
    if not row or row[0] is None:
        return None
    v = str(row[0]).strip()
    return v or None


def set_cursor(engine: Engine, cluster: str, table_name: str, cursor_col: str, cursor_value: str | None) -> None:
    sql = text(
        "INSERT INTO etl_state(cluster, table_name, cursor_col, cursor_value, updated_at) "
        "VALUES(:cluster, :table_name, :cursor_col, :cursor_value, CURRENT_TIMESTAMP) "
        "ON DUPLICATE KEY UPDATE cursor_value=VALUES(cursor_value), updated_at=CURRENT_TIMESTAMP"
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {"cluster": cluster, "table_name": table_name, "cursor_col": cursor_col, "cursor_value": cursor_value},
        )

