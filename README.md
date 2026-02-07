# stock_to_TiDB

通过国金 QMT/xtquant 以及 Tushare，将历史与增量数据写入 TiDB。

## 快速开始
1. 安装依赖（使用仓库自带 `venv`）：

```powershell
Set-Location e:\New_Code\stock_to_TiDB
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

2. 配置：
- `.env`：`tushare_API_KEY`
- `.env`：TiDB 连接信息（`TIDB_SHARED_HOST/PORT` + 各 cluster 的 `*_USER/*_PWD/*_DBNAME/*_CA`）
- `CA/`：对应 pem 文件存在

3. 增量更新（会自动建表与补列；即使你不是每天更新，也会从上次 cursor 的下一天补齐）：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update --tables all
```

仅更新部分表：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update --tables stock_basic,trade_cal,daily_raw
```

回补或重跑指定区间：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update --tables daily_raw --since 2024-01-01 --until 2024-12-31
```

如果 Tushare 可能对最近数据有修订，可加 `--lookback-days` 重新拉取最近 N 天并 upsert：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update --tables daily_raw --lookback-days 30
```

## RU 省配（TiDB Cloud）
为减少 RU 消耗：
- `daily_raw` 等按交易日逐日抓取的表会按天累积后批量写入（减少事务次数）
- 保留窗口删除会先尝试为日期列建二级索引（避免全表扫描），并按块 `DELETE ... LIMIT ...` 执行
- 测试建议使用 `--sample-tscodes N` + `--write-mode ignore` + `--no-delete` 控制写入规模与避免误删

## 一键回补（AS_MASTER 最近 500 交易日）
按表按月循环回补最近 500 个开市交易日（先回补数据，最后统一做一次保留窗口删除以减少 RU）：

```powershell
.\venv\Scripts\python.exe scripts\backfill_master_500d.py --keep-open-days 500 --write-mode upsert
```

## 说明
当前实现覆盖 `DB_SCHEMA.md` 中的主要表，并做“循环增量 + 删除旧数据”：
- 日线/资金流/风险类（`daily_raw/adj_factor/index_daily/moneyflow_*/limit_list/st_list/suspend_d`）：始终保留最近 **500 个交易日**，更老的数据会被删除
- 5分钟线（`minute_5m`）：始终保留最近 **250 个交易日**，更老的数据会被删除

删除规则是“按交易日窗口裁剪”，不是清空历史：程序会从 `trade_cal` 计算出“最近 N 个开市交易日”中的最早日期 `cutoff`，然后执行 `DELETE ... WHERE trade_date < cutoff`（5分钟线用 `trade_time < cutoff 00:00:00`）。

5分钟分时 `minute_5m`（xtquant）通过 `update-5m` 命令，按 `ts_code` 哈希路由写入 `AS_5MIN_P1/P2/P3` 分片。
注意：`update-5m` 仍然由你当前的 Python 3.12.1 启动，但内部会调用 QMT 自带的 `pythonw.exe` 来执行 xtquant 抓取（因为 xtquant 二进制扩展不支持在 Python 3.12 里直接 import）。
还需要确保 QMT 行情服务已启动并可连接（否则会报 “无法连接行情服务！”）。

示例：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update-5m --ts-codes 000001.SZ,600000.SH --since 2025-01-01
```
