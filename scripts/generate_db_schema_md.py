from __future__ import annotations

"""
Generate schema docs by introspecting TiDB clusters and embedding into README.md.

Requires a working .env with AS_MASTER / AS_5MIN_P1..P3 credentials.
"""


# Ensure repo root is importable even when launched from non-repo CWD.
import sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import inspect

from stock_to_tidb.env import load_settings
from stock_to_tidb.tidb import ClusterName, load_tidb_config, make_engine
from stock_to_tidb.tushare_jobs import MASTER_TABLES


@dataclass(frozen=True)
class ColInfo:
    name: str
    type_sql: str
    nullable: bool


def _token_cn(token: str) -> str | None:
    t = token.lower()
    token_map = {
        "ts": "TS",
        "code": "代码",
        "tscode": "证券代码",
        "trade": "交易",
        "date": "日期",
        "time": "时间",
        "open": "开盘",
        "high": "最高价",
        "low": "最低价",
        "close": "收盘价",
        "pre": "昨",
        "chg": "涨跌",
        "pct": "百分比",
        "change": "涨跌额",
        "vol": "成交量",
        "share": "股",
        "amount": "成交额",
        "turnover": "换手",
        "rate": "率",
        "ratio": "比",
        "pe": "市盈率",
        "pb": "市净率",
        "ps": "市销率",
        "ttm": "TTM",
        "dv": "股息",
        "total": "总",
        "float": "流通",
        "free": "自由流通",
        "mv": "市值",
        "circ": "流通",
        "cal": "日历",
        "exchange": "交易所",
        "is": "是否",
        # NOTE: "open" already mapped above.
        "list": "上市",
        "delist": "退市",
        "status": "状态",
        "symbol": "证券代码",
        "name": "名称",
        "fullname": "全称",
        "enname": "英文名",
        "cnspell": "拼音",
        "area": "地区",
        "industry": "行业",
        "market": "市场",
        "curr": "货币",
        "type": "类型",
        "hs": "沪深港通",
        # moneyflow
        "buy": "买入",
        "sell": "卖出",
        "sm": "小单",
        "md": "中单",
        "lg": "大单",
        "elg": "特大单",
        "net": "净",
        "mf": "资金流",
        # misc
        "start": "开始",
        "end": "结束",
        "ann": "公告",
        "reason": "原因",
        "content": "内容",
        "suspend": "停牌",
        "resume": "复牌",
        "limit": "涨跌停",
    }
    return token_map.get(t)


def cn_label(col: str) -> str:
    explicit = {
        "ts_code": "证券代码",
        "trade_date": "交易日期",
        "trade_time": "交易时间",
        "cal_date": "日历日期",
        "pretrade_date": "上一交易日",
        "is_open": "是否开市(1/0)",
        "open": "开盘价",
        "high": "最高价",
        "low": "最低价",
        "close": "收盘价",
        "pre_close": "昨收价",
        "pct_chg": "涨跌幅(%)",
        "vol": "成交量(手)",
        "vol_share": "成交量(股)",
        "volume": "成交量(手)",
        "amount": "成交额",
        "turnover_rate": "换手率(%)",
        "turnover_rate_f": "换手率(自由流通, %)",
        "volume_ratio": "量比",
        "dv_ratio": "股息率(%)",
        "dv_ttm": "股息率(TTM, %)",
        "total_share": "总股本(股)",
        "float_share": "流通股本(股)",
        "free_share": "自由流通股本(股)",
        "total_mv": "总市值",
        "circ_mv": "流通市值",
        "adj_factor": "复权因子",
        # moneyflow_hsgt units: 万元 (per current Tushare return)
        "ggt_ss": "港股通(沪)净流入(万元)",
        "ggt_sz": "港股通(深)净流入(万元)",
        "hgt": "沪股通净流入(万元)",
        "sgt": "深股通净流入(万元)",
        "north_money": "北向资金净流入(万元)",
        "south_money": "南向资金净流入(万元)",
        # limit_list common fields
        "amp": "振幅(%)",
        "fc_ratio": "封成比",
        "fl_ratio": "封流比",
        "fd_amount": "封单金额",
        "first_time": "首次涨跌停时间",
        "last_time": "最后涨跌停时间",
        "open_times": "打开次数",
        "strth": "强度",
        "limit": "涨跌停标志",
        "etl_state": "ETL游标状态表",
        "cluster": "集群",
        "table_name": "表名",
        "cursor_col": "游标列",
        "cursor_value": "游标值",
        "updated_at": "更新时间",
    }
    if col in explicit:
        return explicit[col]

    toks = [t for t in col.split("_") if t]
    parts: list[str] = []
    unknown = False
    for t in toks:
        cn = _token_cn(t)
        if cn is None:
            unknown = True
            break
        parts.append(cn)
    if not toks:
        return f"同英文({col})"
    if unknown:
        return f"同英文({col})"

    # Some common composites.
    s = "".join(parts)
    s = s.replace("交易日期日期", "交易日期").replace("交易时间时间", "交易时间")
    s = s.replace("昨收盘价", "昨收价")
    return s


def _freq_for_table(table: str) -> tuple[str, str]:
    """
    Returns (source_granularity, update_frequency_hint).
    """
    if table == "minute_5m":
        return ("5分钟", "增量：按游标逐交易日更新；默认只处理已收盘交易日")
    if table in {"daily_raw", "adj_factor", "moneyflow_ind", "moneyflow_sector", "moneyflow_mkt", "moneyflow_hsgt", "limit_list", "suspend_d"}:
        return ("日频", "增量：按游标逐交易日更新")
    if table in {"stk_limit", "limit_list_d"}:
        return ("日频", "增量：按游标逐交易日更新")
    if table in {"index_daily"}:
        return ("日频", "增量：按游标更新(范围抓取)")
    if table in {"index_weight"}:
        return ("月度", "增量：按月份范围抓取(按配置的指数集合)")
    if table in {"share_float", "dividend"}:
        return ("事件/区间", "增量：按日期范围循环抓取")
    if table in {"stock_basic"}:
        return ("不定期", "建议每日/每周更新一次")
    if table in {"index_basic", "index_classify", "index_member_all"}:
        return ("不定期", "建议每日/每周更新一次")
    if table in {"trade_cal"}:
        return ("日历", "建议每日更新")
    if table in {"st_list"}:
        return ("事件/区间", "建议每日更新(区间抓取)")
    if table == "etl_state":
        return ("元数据", "由程序自动维护")
    return ("未知", "未知")


def _source_for_table(table: str) -> str:
    if table == "minute_5m":
        return "国金QMT/xtquant (download_history_data2 + get_market_data)"
    src_map = {
        "stock_basic": "Tushare(stock_basic)",
        "trade_cal": "Tushare(trade_cal)",
        "index_basic": "Tushare(index_basic)",
        "index_classify": "Tushare(index_classify)",
        "index_member_all": "Tushare(index_member_all)",
        "daily_raw": "Tushare(daily + daily_basic)",
        "adj_factor": "Tushare(adj_factor)",
        "index_daily": "Tushare(index_daily)",
        "index_weight": "Tushare(index_weight)",
        "moneyflow_ind": "Tushare(moneyflow_dc)",
        "moneyflow_sector": "Tushare(moneyflow_ind_dc)",
        "moneyflow_mkt": "Tushare(moneyflow_mkt_dc)",
        "moneyflow_hsgt": "Tushare(moneyflow_hsgt)",
        "limit_list": "Tushare(limit_list)",
        "stk_limit": "Tushare(stk_limit)",
        "limit_list_d": "Tushare(limit_list_d)",
        "share_float": "Tushare(share_float)",
        "dividend": "Tushare(dividend)",
        "st_list": "Tushare(namechange)",
        "suspend_d": "Tushare(suspend_d)",
        "etl_state": "Internal(etl_state)",
    }
    return src_map.get(table, "未知")


def _retention_for_table(table: str) -> str:
    if table == "minute_5m":
        return "保留最近 250 个开市日(按 trade_time 删除)"
    if table in {
        "daily_raw",
        "adj_factor",
        "index_daily",
        "moneyflow_ind",
        "moneyflow_sector",
        "moneyflow_mkt",
        "moneyflow_hsgt",
        "limit_list",
        "stk_limit",
        "limit_list_d",
        "st_list",
        "suspend_d",
    }:
        return "保留最近 500 个开市日(按 trade_date/start_date 删除)"
    if table in {"share_float"}:
        return "保留最近 500 个开市日(按 float_date 删除)"
    if table in {"dividend"}:
        return "保留最近 500 个开市日(按 ann_date 删除)"
    if table in {"index_weight"}:
        return "保留最近 2000 个开市日(按 trade_date 删除)"
    if table in {"stock_basic", "trade_cal", "etl_state"}:
        return "不做自动删除"
    if table in {"index_basic", "index_classify", "index_member_all"}:
        return "不做自动删除"
    return "未知"


def _notes_for_table(table: str) -> list[str]:
    if table == "daily_raw":
        return [
            "`amount` 入库前按约定换算为元(原始接口常见为千元)。",
            "`vol` 入库后存为 `vol_share` (股)，按约定由“手”转换。",
        ]
    if table == "minute_5m":
        return [
            "逻辑表 `minute_5m` 按 `ts_code` 哈希路由分片到 `AS_5MIN_P1/P2/P3`。",
            "`volume` 入库后存为 `vol_share` (股)，按约定由“手”转换。",
        ]
    if table == "moneyflow_hsgt":
        return [
            "单位：万元（以 Tushare 当前返回为准）。",
        ]
    if table == "index_weight":
        return [
            "月度数据：建议按月设置 start_date/end_date（当月第一天与最后一天）。",
            "本项目默认只抓取 `.env` 的 `INDEX_WEIGHT_CODES` 指定的指数集合。",
        ]
    return []


def _render_table(*, cluster: str, table: str, cols: list[ColInfo], pks: list[str], exists: bool) -> str:
    src_freq, upd_freq = _freq_for_table(table)
    md: list[str] = []
    md.append(f"### {table}  ({cluster})")
    md.append(f"- **数据源**：{_source_for_table(table)}")
    md.append(f"- **频率(数据粒度)**：{src_freq}")
    md.append(f"- **更新频率(建议)**：{upd_freq}")
    md.append(f"- **保留策略**：{_retention_for_table(table)}")
    md.append(f"- **已建表**：{'是' if exists else '否(尚未落库，字段来自接口探测/约定)'}")
    if pks:
        md.append(f"- **主键**：`({', '.join(pks)})`")
    for n in _notes_for_table(table):
        md.append(f"- **备注**：{n}")
    md.append("")
    md.append("| Column (EN) | 字段(中文) | Type | Null |")
    md.append("|---|---|---:|:---:|")
    for c in cols:
        typ = c.type_sql or "-"
        md.append(f"| `{c.name}` | {cn_label(c.name)} | `{typ}` | {'YES' if c.nullable else 'NO'} |")
    md.append("")
    return "\n".join(md)


def _get_probe_trade_date_yyyymmdd(engine_master) -> str:
    # Prefer AS_MASTER.trade_cal to pick a recent open day.
    try:
        with engine_master.begin() as conn:
            row = conn.execute(
                """
                SELECT DATE_FORMAT(MAX(cal_date), '%Y%m%d') AS td
                FROM trade_cal
                WHERE exchange='SSE' AND is_open=1 AND cal_date <= CURRENT_DATE()
                """
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    # Fallback: yesterday (may be non-trading; still returns columns for many APIs).
    from datetime import date, timedelta

    return (date.today() - timedelta(days=1)).strftime("%Y%m%d")


def _probe_master_columns(settings, *, probe_td: str) -> dict[str, list[str]]:
    """
    Return table_name -> column list (EN) for AS_MASTER logical tables using light Tushare probes.
    """
    out: dict[str, list[str]] = {}
    try:
        import tushare as ts

        ts.set_token(settings.tushare_token)
        pro = ts.pro_api()
    except Exception:
        return out

    def q(api: str, **params) -> list[str]:
        try:
            df = pro.query(api, **params)
            return [str(c) for c in list(getattr(df, "columns", []))]
        except Exception:
            return []

    # Minimal probes (prefer ts_code filters to avoid large responses).
    code = "000001.SZ"
    idx = "000001.SH"

    out["trade_cal"] = q("trade_cal", exchange="SSE", start_date=probe_td, end_date=probe_td)
    out["stock_basic"] = q("stock_basic", exchange="", list_status="L")
    out["index_basic"] = q("index_basic")
    out["index_classify"] = q("index_classify")
    out["index_member_all"] = q("index_member_all", ts_code=code, is_new="Y") or q("index_member_all", ts_code=code)

    # daily_raw = daily + daily_basic merged
    daily_cols = q("daily", ts_code=code, trade_date=probe_td) or q("daily", trade_date=probe_td)
    basic_cols = q("daily_basic", ts_code=code, trade_date=probe_td) or q("daily_basic", trade_date=probe_td)
    merged: list[str] = []
    for c in daily_cols:
        if c not in merged:
            merged.append(c)
    for c in basic_cols:
        if c not in merged:
            merged.append(c)
    if "vol" in merged and "vol_share" not in merged:
        merged = [("vol_share" if c == "vol" else c) for c in merged]
    out["daily_raw"] = merged

    out["adj_factor"] = q("adj_factor", ts_code=code, trade_date=probe_td) or q("adj_factor", trade_date=probe_td)
    out["index_daily"] = q("index_daily", ts_code=idx, start_date=probe_td, end_date=probe_td) or q(
        "index_daily", start_date=probe_td, end_date=probe_td
    )
    # index_weight needs index_code; pick from .env if possible, else fallback to CSI300.
    try:
        codes_raw = (settings.env.get("INDEX_WEIGHT_CODES") or "").strip()
        index_code = (codes_raw.split(",")[0].strip() if codes_raw else "") or "000300.SH"
    except Exception:
        index_code = "000300.SH"
    out["index_weight"] = q("index_weight", index_code=index_code, start_date=probe_td, end_date=probe_td)

    out["moneyflow_ind"] = q("moneyflow_dc", ts_code=code, trade_date=probe_td) or q("moneyflow_dc", trade_date=probe_td)
    out["moneyflow_sector"] = q("moneyflow_ind_dc", ts_code=code, trade_date=probe_td) or q("moneyflow_ind_dc", trade_date=probe_td)
    out["moneyflow_mkt"] = q("moneyflow_mkt_dc", trade_date=probe_td)
    out["moneyflow_hsgt"] = q("moneyflow_hsgt", trade_date=probe_td)

    out["limit_list"] = q("limit_list", ts_code=code, trade_date=probe_td) or q("limit_list", trade_date=probe_td)
    out["stk_limit"] = q("stk_limit", ts_code=code, trade_date=probe_td) or q("stk_limit", trade_date=probe_td)
    out["limit_list_d"] = q("limit_list_d", trade_date=probe_td, limit_type="U") or q("limit_list_d", trade_date=probe_td)
    out["share_float"] = q("share_float", ts_code=code, start_date=probe_td, end_date=probe_td) or q(
        "share_float", start_date=probe_td, end_date=probe_td
    )
    out["dividend"] = q("dividend", ts_code=code, ann_date=probe_td) or q("dividend", ann_date=probe_td)
    out["suspend_d"] = q("suspend_d", ts_code=code, trade_date=probe_td) or q("suspend_d", trade_date=probe_td)

    out["st_list"] = q("namechange", ts_code=code, start_date=probe_td, end_date=probe_td) or q(
        "namechange", start_date=probe_td, end_date=probe_td
    )

    return out


def _bump_headings(md: str, bump: int = 1) -> str:
    """
    Embed a standalone markdown doc into another one by bumping heading levels.
    """
    out: list[str] = []
    for line in md.splitlines():
        if line.startswith("#"):
            n = 0
            while n < len(line) and line[n] == "#":
                n += 1
            if 1 <= n <= 6 and (n < len(line) and line[n] == " "):
                nn = min(6, n + int(bump))
                out.append("#" * nn + line[n:])
                continue
        out.append(line)
    return "\n".join(out) + ("\n" if md.endswith("\n") else "")


def _update_readme_block(*, repo_root: Path, schema_md: str) -> None:
    begin = "<!-- BEGIN DB_SCHEMA -->"
    end = "<!-- END DB_SCHEMA -->"
    readme_path = repo_root / "README.md"
    readme = readme_path.read_text(encoding="utf-8")

    if begin in readme and end in readme and readme.index(begin) < readme.index(end):
        pre = readme[: readme.index(begin) + len(begin)]
        post = readme[readme.index(end) :]
        new_text = pre.rstrip() + "\n\n" + schema_md.rstrip() + "\n\n" + post.lstrip()
    else:
        new_text = readme.rstrip() + "\n\n" + begin + "\n\n" + schema_md.rstrip() + "\n\n" + end + "\n"

    readme_path.write_text(new_text, encoding="utf-8")


def main() -> int:
    settings = load_settings()
    repo_root = Path(settings.repo_root)

    clusters: list[ClusterName] = ["AS_MASTER", "AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3"]
    cluster_tables: dict[str, dict[str, dict[str, Any]]] = {}
    errors: list[str] = []
    engine_master = None

    for c in clusters:
        try:
            eng = make_engine(load_tidb_config(settings, c))
            if c == "AS_MASTER":
                engine_master = eng
            insp = inspect(eng)
            tables = sorted(insp.get_table_names())
            tbl_map: dict[str, dict[str, Any]] = {}
            for t in tables:
                cols = insp.get_columns(t)
                pk = insp.get_pk_constraint(t).get("constrained_columns") or []
                ci = []
                for col in cols:
                    typ = col.get("type")
                    type_sql = str(typ) if typ is not None else ""
                    ci.append(ColInfo(name=str(col["name"]), type_sql=type_sql, nullable=bool(col.get("nullable", True))))
                tbl_map[t] = {"cols": ci, "pk": [str(x) for x in pk]}
            cluster_tables[c] = tbl_map
        except Exception as e:
            errors.append(f"{c}: {e}")
            cluster_tables[c] = {}

    probe_cols: dict[str, list[str]] = {}
    if engine_master is not None:
        probe_td = _get_probe_trade_date_yyyymmdd(engine_master)
        probe_cols = _probe_master_columns(settings, probe_td=probe_td)

    lines: list[str] = []
    lines.append("# 量化交易数据库表结构（自动生成）")
    lines.append("")
    lines.append(f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("- 说明：字段名(EN)来自实际 TiDB 表结构；中文字段名为本项目约定的解释或自动推断。")
    lines.append("- 设计原则：除明确的字段改名/单位换算外，其余字段保持与源接口一致。")
    lines.append("")
    lines.append("## 集群与表概览")
    lines.append("")
    for c in clusters:
        tbls = sorted(cluster_tables.get(c, {}).keys())
        lines.append(f"- `{c}`: {', '.join(f'`{t}`' for t in tbls) if tbls else '(无/连接失败)'}")
    # Also show the logical AS_MASTER table set expected by code, to avoid doc drift even if tables aren't created yet.
    master_actual_names = set(cluster_tables.get('AS_MASTER', {}).keys())
    master_expected_names = sorted(set(MASTER_TABLES.keys()) | {'etl_state'} | master_actual_names)
    lines.append(f"- `AS_MASTER(expected)`: {', '.join(f'`{t}`' for t in master_expected_names)}")
    lines.append("")
    if errors:
        lines.append("## 连接错误")
        lines.append("")
        for e in errors:
            lines.append(f"- {e}")
        lines.append("")

    # AS_MASTER tables (include all tables defined in code, even if not created yet)
    lines.append("## AS_MASTER")
    lines.append("")
    master_actual = cluster_tables.get("AS_MASTER", {})
    master_expected = sorted(set(MASTER_TABLES.keys()) | {"etl_state"} | set(master_actual.keys()))
    for t in master_expected:
        if t in master_actual:
            info = master_actual[t]
            lines.append(_render_table(cluster="AS_MASTER", table=t, cols=info["cols"], pks=info["pk"], exists=True))
            continue
        cols = [ColInfo(name=c, type_sql="", nullable=True) for c in (probe_cols.get(t) or [])]
        pks = MASTER_TABLES.get(t).primary_keys if t in MASTER_TABLES else []
        lines.append(_render_table(cluster="AS_MASTER", table=t, cols=cols, pks=pks, exists=False))

    # Shards (document each cluster separately; minute_5m is sharded)
    for c in ["AS_5MIN_P1", "AS_5MIN_P2", "AS_5MIN_P3"]:
        lines.append(f"## {c}")
        lines.append("")
        tbls = cluster_tables.get(c, {})
        for t in sorted(tbls.keys()):
            info = tbls[t]
            lines.append(_render_table(cluster=c, table=t, cols=info["cols"], pks=info["pk"], exists=True))

    schema_doc = "\n".join(lines).rstrip() + "\n"
    # README already has its own headings; embed schema as a nested section.
    schema_doc = _bump_headings(schema_doc, bump=2)
    _update_readme_block(repo_root=repo_root, schema_md=schema_doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
