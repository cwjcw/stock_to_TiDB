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
    parts = []
    for field, df in mkt.items():
        if not hasattr(df, "stack"):
            continue
        s = df.stack(dropna=False)
        s.name = field
        parts.append(s)
    if not parts:
        return pd.DataFrame()
    wide = pd.concat(parts, axis=1).reset_index()
    wide = wide.rename(columns={"level_0": "ts_code", "level_1": "trade_time"})
    # Ensure trade_time is string (usually yyyymmddHHMMSS)
    wide["trade_time"] = wide["trade_time"].astype(str)
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
    mkt = xtdata.get_market_data_ex(
        field_list=[],
        stock_list=codes,
        period=args.period,
        start_time=args.start,
        end_time=args.end,
        count=-1,
        dividend_type="none",
        fill_data=True,
    )

    df = _market_dict_to_long(mkt)
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
