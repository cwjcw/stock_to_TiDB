from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    # .env in this repo sometimes contains GBK comments; keys are ASCII.
    for enc in ("utf-8", "gbk"):
        try:
            vals = dotenv_values(path, encoding=enc)
            out: dict[str, str] = {}
            for k, v in vals.items():
                if k is None or v is None:
                    continue
                out[str(k).strip()] = str(v).strip()
            return out
        except UnicodeDecodeError:
            continue
    # Last resort: let python-dotenv decide.
    vals = dotenv_values(path)
    out2: dict[str, str] = {}
    for k, v in vals.items():
        if k is None or v is None:
            continue
        out2[str(k).strip()] = str(v).strip()
    return out2


@dataclass(frozen=True)
class Settings:
    repo_root: Path
    env: dict[str, str]

    @property
    def tushare_token(self) -> str:
        token = self.env.get("tushare_API_KEY") or self.env.get("TUSHARE_API_KEY") or ""
        if not token:
            raise RuntimeError("Missing Tushare token: set `tushare_API_KEY` in .env")
        return token

    @property
    def tidb_shared_host(self) -> str:
        host = (self.env.get("TIDB_SHARED_HOST") or "").strip()
        if not host:
            raise RuntimeError("Missing `TIDB_SHARED_HOST` in .env")
        return host

    @property
    def tidb_shared_port(self) -> int:
        port = (self.env.get("TIDB_SHARED_PORT") or "").strip()
        if not port:
            raise RuntimeError("Missing `TIDB_SHARED_PORT` in .env")
        return int(port)


def load_settings(repo_root: Path | None = None) -> Settings:
    root = repo_root or Path(__file__).resolve().parents[1]
    env = _load_dotenv(root / ".env")
    return Settings(repo_root=root, env=env)

