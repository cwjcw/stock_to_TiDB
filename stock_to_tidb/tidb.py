from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .env import Settings

ClusterName = Literal["AS_MASTER", "AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3"]


@dataclass(frozen=True)
class TiDBConfig:
    cluster: ClusterName
    host: str
    port: int
    user: str
    password: str
    dbname: str
    ca_path: Path


def load_tidb_config(settings: Settings, cluster: ClusterName) -> TiDBConfig:
    host = settings.tidb_shared_host
    port = settings.tidb_shared_port

    prefix = cluster
    user = (settings.env.get(f"{prefix}_USER") or "").strip()
    pwd = (settings.env.get(f"{prefix}_PWD") or "").strip()
    dbname = (settings.env.get(f"{prefix}_DBNAME") or "").strip()
    ca_name = (settings.env.get(f"{prefix}_CA") or "").strip()

    if not user or not pwd or not dbname or not ca_name:
        raise RuntimeError(
            f"Missing TiDB credentials for {cluster} in .env "
            f"(need {prefix}_USER/{prefix}_PWD/{prefix}_DBNAME/{prefix}_CA)"
        )

    ca_path = Path(ca_name)
    if not ca_path.is_absolute():
        ca_path = settings.repo_root / "CA" / ca_name
    if not ca_path.exists():
        raise RuntimeError(f"CA file not found: {ca_path}")

    return TiDBConfig(cluster=cluster, host=host, port=port, user=user, password=pwd, dbname=dbname, ca_path=ca_path)


def make_engine(cfg: TiDBConfig) -> Engine:
    # TiDB Cloud is MySQL-compatible; use CA to enforce TLS.
    url = f"mysql+pymysql://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.dbname}"
    # Avoid "infinite hang" when network/gateway stalls. Defaults are conservative and can be
    # tuned via .env (seconds).
    env = getattr(cfg, "_env", None)
    # cfg doesn't carry env; read from process env via Settings would be cleaner, but keep this local:
    # users can still override via SQLAlchemy URL params if needed.
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "ssl": {"ca": str(cfg.ca_path)},
            # PyMySQL timeouts (seconds). If unset, they can block forever.
            "connect_timeout": int((__import__("os").environ.get("TIDB_CONNECT_TIMEOUT_S") or "10").strip()),
            "read_timeout": int((__import__("os").environ.get("TIDB_READ_TIMEOUT_S") or "300").strip()),
            "write_timeout": int((__import__("os").environ.get("TIDB_WRITE_TIMEOUT_S") or "300").strip()),
        },
        future=True,
    )


def route_5m_cluster(ts_code: str) -> ClusterName:
    h = hashlib.md5(ts_code.encode("utf-8")).hexdigest()
    mod = int(h[:8], 16) % 3
    return ("AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3")[mod]
