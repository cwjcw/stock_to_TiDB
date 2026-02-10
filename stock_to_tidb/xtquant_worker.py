"""
Runs under QMT's embedded Python (often 3.6/3.7/...) to fetch 5m bars via xtquant.xtdata.

This file is intentionally Python 3.6 compatible (no modern typing syntax).
It writes a gzip CSV that Python 3.12 then ingests into TiDB.
"""

import argparse
import os
import sys


def _add_site_packages(site_packages):
    if site_packages and site_packages not in sys.path:
        sys.path.insert(0, site_packages)


def _read_codes(path):
    codes = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            codes.append(s)
    return codes


def _market_dict_to_long(mkt):
    import pandas as pd

    if not mkt:
        return pd.DataFrame()

    def _looks_like_tscode(s):
        if not isinstance(s, str):
            return False
        s = s.strip().upper()
        return s.endswith(".SH") or s.endswith(".SZ") or s.endswith(".BJ")

    def _normalize_trade_time_col(df, col):
        # Downstream expects yyyymmddHHMMSS strings.
        if col not in df.columns:
            return df
        v = df[col]
        if pd.api.types.is_datetime64_any_dtype(v):
            df[col] = v.dt.strftime("%Y%m%d%H%M%S")
        else:
            df[col] = v.astype(str)
            # common artifact: "20260205093500.0"
            # pandas on embedded Python may be old; avoid str.replace(..., regex=...)
            df[col] = df[col].str.replace(r"\\.0$", "")
        return df

    sample_key = None
    for k in mkt.keys():
        sample_key = k
        break

    # Case 1: {ts_code -> DataFrame(index=time, columns=field)}
    if _looks_like_tscode(sample_key):
        out = []
        for code, df in mkt.items():
            if df is None or getattr(df, "empty", False):
                continue
            if not hasattr(df, "reset_index"):
                continue
            dfx = df.copy().reset_index()
            if "index" in dfx.columns and "trade_time" not in dfx.columns:
                dfx = dfx.rename(columns={"index": "trade_time"})
            dfx["ts_code"] = str(code)
            dfx["ts_code"] = dfx["ts_code"].astype(str)
            dfx = _normalize_trade_time_col(dfx, "trade_time")
            out.append(dfx)
        if not out:
            return pd.DataFrame()
        df_all = pd.concat(out, ignore_index=True)
        cols = list(df_all.columns)
        for c in ["ts_code", "trade_time"]:
            if c in cols:
                cols.remove(c)
        return df_all[["ts_code", "trade_time"] + cols]

    # Case 2: {field -> DataFrame(index=time, columns=ts_code)}
    parts = []
    for field, df in mkt.items():
        if df is None or getattr(df, "empty", False):
            continue
        if not hasattr(df, "stack"):
            continue
        s = df.stack(dropna=False)
        s.name = str(field)
        parts.append(s)
    if not parts:
        return pd.DataFrame()
    wide = pd.concat(parts, axis=1).reset_index()
    # xtquant docs for Kline: DataFrame index=stock_list, columns=time_list
    # but some variants are transposed. Detect which level is ts_code vs trade_time.
    col0 = "level_0"
    col1 = "level_1"
    v0 = wide[col0].iloc[0] if len(wide) else None
    v1 = wide[col1].iloc[0] if len(wide) else None
    if _looks_like_tscode(v0) and (not _looks_like_tscode(v1)):
        wide = wide.rename(columns={col0: "ts_code", col1: "trade_time"})
    elif _looks_like_tscode(v1) and (not _looks_like_tscode(v0)):
        wide = wide.rename(columns={col0: "trade_time", col1: "ts_code"})
    else:
        # Fallback: assume doc orientation.
        wide = wide.rename(columns={col0: "ts_code", col1: "trade_time"})

    wide["ts_code"] = wide["ts_code"].astype(str)
    wide = _normalize_trade_time_col(wide, "trade_time")
    return wide


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-packages", default="", help="xtquant site-packages path")
    ap.add_argument("--codes-file", required=True, help="UTF-8 file with one ts_code per line")
    ap.add_argument("--start", required=True, help="YYYYMMDDhhmmss")
    ap.add_argument("--end", required=True, help="YYYYMMDDhhmmss")
    ap.add_argument("--out", required=True, help="Output .csv.gz path")
    ap.add_argument("--period", default="5m")
    args = ap.parse_args(argv)

    _add_site_packages(args.site_packages)

    import pandas as pd
    from xtquant import xtdata  # noqa

    codes = _read_codes(args.codes_file)
    if not codes:
        raise SystemExit("empty codes list")

    # Ensure local history exists (QMT cache). QMT行情服务有时需要等待启动。
    import time
    last_err = None
    deadline = time.time() + 90.0
    while True:
        try:
            xtdata.download_history_data2(codes, args.period, start_time=args.start, end_time=args.end)
            break
        except Exception as e:
            last_err = e
            msg = str(e)
            if ("无法连接行情服务" in msg) and time.time() < deadline:
                time.sleep(1.0)
                continue
            raise
    # Use the documented API from "获取行情数据.pdf".
    mkt = xtdata.get_market_data(
        # Explicit fields keeps returned structure more stable across QMT versions.
        field_list=["open", "high", "low", "close", "volume", "amount"],
        stock_list=codes,
        period=args.period,
        start_time=args.start,
        end_time=args.end,
        count=-1,
        dividend_type="none",
        fill_data=True,
    )

    df = _market_dict_to_long(mkt)
    if df is None or getattr(df, "empty", False):
        # Always write a CSV with a header so the upstream pandas reader doesn't
        # throw EmptyDataError on blank files.
        df = pd.DataFrame(columns=["ts_code", "trade_time", "open", "high", "low", "close", "volume", "amount"])
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    # gzip CSV is the lowest-common-denominator format for embedded Python.
    import gzip

    with gzip.open(args.out, "wt", encoding="utf-8") as f:
        df.to_csv(f, index=False)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        import traceback

        # Best-effort error report (pythonw.exe has no stdout).
        out = None
        for i, a in enumerate(sys.argv):
            if a == "--out" and i + 1 < len(sys.argv):
                out = sys.argv[i + 1]
                break
        err_path = (out + ".err.txt") if out else os.path.abspath("xtquant_worker.err.txt")
        try:
            with open(err_path, "w", encoding="utf-8") as f:
                f.write(traceback.format_exc())
        except Exception:
            pass
        raise
