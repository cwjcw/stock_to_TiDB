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
- `.env`：可选 `TUSHARE_MAX_CALLS_PER_MIN`（默认 300，用于限制抓取频率，避免回补时触发服务端限流）
- `.env`：可选 `INDEX_WEIGHT_CODES`（逗号分隔，比如 `000300.SH,000905.SH,399300.SZ`；为空则跳过抓取 `index_weight`）
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

## 5分钟线（全市场：国金QMT/xtquant）
`minute_5m` 是全市场 5 分钟数据，按 `ts_code` 哈希路由分片写入 `AS_5MIN_P1/P2/P3`。

前置条件：
- 已配置 `.env` 的 TiDB + CA
- QMT 行情服务可连接
- `AS_MASTER.stock_basic` 和 `AS_MASTER.trade_cal` 已有数据（用来取全市场代码、计算保留窗口）

```powershell
# 确保元数据齐全（只需跑一次，之后增量即可）
.\venv\Scripts\python.exe -m stock_to_tidb update --tables stock_basic,trade_cal
```

全市场回补（最近 250 个开市日，首次跑/修洞用）：

```powershell
.\venv\Scripts\python.exe scripts\backfill_5m_250d.py --keep-open-days 250 --write-mode ignore
```

全市场增量（日常每天跑一次；默认 `--resume`，只处理游标之后的交易日，然后删掉窗口外数据）：

```powershell
.\venv\Scripts\python.exe scripts\backfill_5m_250d.py
```

强制重跑窗口（会重置游标并重新回补整个窗口）：

```powershell
.\venv\Scripts\python.exe scripts\backfill_5m_250d.py --no-resume --reset-cursor
```

## 说明
当前实现覆盖本 README 的“数据库表结构（自动生成）”中的主要表，并做“循环增量 + 删除旧数据”：
- 日频滚动窗口类（`daily_raw/adj_factor/index_daily/moneyflow_*/limit_list_d/stk_limit/st_list/suspend_d/share_float/dividend`）：默认始终保留最近 **500 个交易日**（`index_weight` 默认保留最近 **2000 个交易日**）
- 5分钟线（`minute_5m`）：默认始终保留最近 **250 个交易日**

删除规则是“按交易日窗口裁剪”，不是清空历史：程序会从 `trade_cal` 计算出“最近 N 个开市交易日”中的最早日期 `cutoff`，然后执行 `DELETE ... WHERE trade_date < cutoff`（5分钟线用 `trade_time < cutoff 00:00:00`）。

5分钟分时 `minute_5m`（xtquant）通过 `update-5m` 命令，按 `ts_code` 哈希路由写入 `AS_5MIN_P1/P2/P3` 分片。
注意：`update-5m` 仍然由你当前的 Python 3.12.1 启动，但内部会调用 QMT 自带的 `pythonw.exe` 来执行 xtquant 抓取（因为 xtquant 二进制扩展不支持在 Python 3.12 里直接 import）。
还需要确保 QMT 行情服务已启动并可连接（否则会报 “无法连接行情服务！”）。

示例：

```powershell
.\venv\Scripts\python.exe -m stock_to_tidb update-5m --ts-codes 000001.SZ,600000.SH --since 2025-01-01
```

## 数据库表结构（自动生成）
运行 `.\venv\Scripts\python.exe scripts\generate_db_schema_md.py` 会自动把最新 schema 写回本 README（包含实际 TiDB 表结构 + 未落库表的轻量探测字段）。

<!-- BEGIN DB_SCHEMA -->

### 量化交易数据库表结构（自动生成）

- 生成时间：2026-02-11 08:35:16
- 说明：字段名(EN)来自实际 TiDB 表结构；中文字段名为本项目约定的解释或自动推断。
- 设计原则：除明确的字段改名/单位换算外，其余字段保持与源接口一致。

#### 集群与表概览

- `AS_MASTER`: `adj_factor`, `daily_raw`, `etl_state`, `index_basic`, `index_classify`, `index_daily`, `index_member_all`, `moneyflow_hsgt`, `moneyflow_ind`, `moneyflow_mkt`, `moneyflow_sector`, `st_list`, `stock_basic`, `suspend_d`, `trade_cal`
- `AS_5MIN_P1`: `etl_state`, `minute_5m`
- `AS_5MIN_P2`: `etl_state`, `minute_5m`
- `AS_5MIN_P3`: `etl_state`, `minute_5m`
- `AS_MASTER(expected)`: `adj_factor`, `daily_raw`, `dividend`, `etl_state`, `index_basic`, `index_classify`, `index_daily`, `index_member_all`, `index_weight`, `limit_list_d`, `moneyflow_hsgt`, `moneyflow_ind`, `moneyflow_mkt`, `moneyflow_sector`, `share_float`, `st_list`, `stk_limit`, `stock_basic`, `suspend_d`, `trade_cal`

#### 表用途速查

下面是每张表的用途说明（先看用途再看字段，方便你判断是否需要常驻落库）。

- `adj_factor`: 复权因子。用于前复权/后复权价格序列、复权收益计算。
- `daily_raw`: A股日线行情 + 每日指标（合并表）。用于技术面/因子研究、回测与风控。
- `dividend`: 分红送股事件。用于分红/股息因子、除权除息事件回测、红利策略与现金流分配研究。
- `etl_state`: ETL 游标状态：记录每张表抓取到哪一天。用于增量更新与断点续跑。
- `index_basic`: 指数基础信息维表。用于指数池管理、发布方/类别筛选、指数元数据查询。
- `index_classify`: 申万行业分类字典（行业代码与层级）。用于行业维度分析与行业中性化。
- `index_daily`: 指数日线行情。用于基准对比、指数择时、指数增强评估。
- `index_member_all`: 申万行业成分映射（股票属于哪个行业/分级）。用于行业轮动、行业中性、多因子分组。
- `index_weight`: 指数成分与权重（月度）。用于指数增强、成分约束组合、指数复刻与归因。
- `limit_list_d`: 涨跌停/炸板名单（新接口，含连板/炸板统计）。用于情绪/打板/连板研究与风险控制。
- `moneyflow_hsgt`: 沪深港通资金流向（北向/南向净流入）。用于风格/风险开关、资金面择时。
- `moneyflow_ind`: 个股/行业资金流向（按 Tushare 口径）。用于资金驱动选股、拥挤度/风格切换。
- `moneyflow_mkt`: 全市场资金流向统计。用于市场情绪与资金面监控。
- `moneyflow_sector`: 板块/概念维度资金流向（按 Tushare 口径）。用于主题轮动与资金扩散分析。
- `share_float`: 限售股解禁。用于解禁冲击事件研究、供给压力因子与风险提示。
- `st_list`: ST/更名等状态变更区间。用于风险过滤、退市/ST 策略约束与事件研究。
- `stk_limit`: 每日涨跌停价（上/下限）。用于回测可成交性约束、涨跌停风险控制。
- `stock_basic`: 股票基础信息维表：代码、名称、行业、上市日期等。用于全市场股票池、行业分组、数据对齐。
- `suspend_d`: 停复牌信息。用于可交易性过滤、回测约束与事件分析。
- `trade_cal`: 交易日历维表：开市/休市、上一交易日。用于滚动窗口、按交易日回补、避免自然日偏差。

#### AS_MASTER

##### adj_factor  (AS_MASTER)
- **数据源**：Tushare(adj_factor)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_date` | 交易日期 | `DATE` | NO |
| `adj_factor` | 复权因子 | `FLOAT` | YES |

##### daily_raw  (AS_MASTER)
- **数据源**：Tushare(daily + daily_basic)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_date)`
- **备注**：`amount` 入库前按约定换算为元(原始接口常见为千元)。
- **备注**：`vol` 入库后存为 `vol_share` (股)，按约定由“手”转换。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_date` | 交易日期 | `DATE` | NO |
| `open` | 开盘价 | `FLOAT` | YES |
| `high` | 最高价 | `FLOAT` | YES |
| `low` | 最低价 | `FLOAT` | YES |
| `close` | 收盘价 | `FLOAT` | YES |
| `pre_close` | 昨收价 | `FLOAT` | YES |
| `change` | 涨跌额 | `FLOAT` | YES |
| `pct_chg` | 涨跌幅(%) | `FLOAT` | YES |
| `amount` | 成交额 | `FLOAT` | YES |
| `turnover_rate` | 换手率(%) | `FLOAT` | YES |
| `turnover_rate_f` | 换手率(自由流通, %) | `FLOAT` | YES |
| `volume_ratio` | 量比 | `FLOAT` | YES |
| `pe` | 市盈率 | `VARCHAR(64)` | YES |
| `pe_ttm` | 市盈率TTM | `VARCHAR(64)` | YES |
| `pb` | 市净率 | `VARCHAR(64)` | YES |
| `ps` | 市销率 | `FLOAT` | YES |
| `ps_ttm` | 市销率TTM | `FLOAT` | YES |
| `dv_ratio` | 股息率(%) | `VARCHAR(64)` | YES |
| `dv_ttm` | 股息率(TTM, %) | `VARCHAR(64)` | YES |
| `total_share` | 总股本(股) | `FLOAT` | YES |
| `float_share` | 流通股本(股) | `FLOAT` | YES |
| `free_share` | 自由流通股本(股) | `FLOAT` | YES |
| `total_mv` | 总市值 | `FLOAT` | YES |
| `circ_mv` | 流通市值 | `FLOAT` | YES |
| `vol_share` | 成交量(股) | `FLOAT` | YES |

##### dividend  (AS_MASTER)
- **数据源**：Tushare(dividend)
- **频率(数据粒度)**：事件/区间
- **更新频率(建议)**：增量：按日期范围循环抓取
- **保留策略**：保留最近 500 个开市日(按 ann_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(ts_code, ann_date, end_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `-` | YES |
| `end_date` | 结束日期 | `-` | YES |
| `ann_date` | 公告日期 | `-` | YES |
| `div_proc` | 同英文(div_proc) | `-` | YES |
| `stk_div` | 同英文(stk_div) | `-` | YES |
| `stk_bo_rate` | 同英文(stk_bo_rate) | `-` | YES |
| `stk_co_rate` | 同英文(stk_co_rate) | `-` | YES |
| `cash_div` | 同英文(cash_div) | `-` | YES |
| `cash_div_tax` | 同英文(cash_div_tax) | `-` | YES |
| `record_date` | 同英文(record_date) | `-` | YES |
| `ex_date` | 同英文(ex_date) | `-` | YES |
| `pay_date` | 同英文(pay_date) | `-` | YES |
| `div_listdate` | 同英文(div_listdate) | `-` | YES |
| `imp_ann_date` | 同英文(imp_ann_date) | `-` | YES |

##### etl_state  (AS_MASTER)
- **数据源**：Internal(etl_state)
- **频率(数据粒度)**：元数据
- **更新频率(建议)**：由程序自动维护
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(cluster, table_name, cursor_col)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `cluster` | 集群 | `VARCHAR(32)` | NO |
| `table_name` | 表名 | `VARCHAR(64)` | NO |
| `cursor_col` | 游标列 | `VARCHAR(64)` | NO |
| `cursor_value` | 游标值 | `VARCHAR(64)` | YES |
| `updated_at` | 更新时间 | `DATETIME` | NO |

##### index_basic  (AS_MASTER)
- **数据源**：Tushare(index_basic)
- **频率(数据粒度)**：不定期
- **更新频率(建议)**：建议每日/每周更新一次
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(ts_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `market` | 市场 | `VARCHAR(32)` | YES |
| `publisher` | 同英文(publisher) | `VARCHAR(64)` | YES |
| `category` | 同英文(category) | `VARCHAR(64)` | YES |
| `base_date` | 同英文(base_date) | `DATE` | YES |
| `base_point` | 同英文(base_point) | `FLOAT` | YES |
| `list_date` | 上市日期 | `DATE` | YES |

##### index_classify  (AS_MASTER)
- **数据源**：Tushare(index_classify)
- **频率(数据粒度)**：不定期
- **更新频率(建议)**：建议每日/每周更新一次
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(index_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `index_code` | 同英文(index_code) | `VARCHAR(64)` | NO |
| `industry_name` | 行业名称 | `VARCHAR(64)` | YES |
| `level` | 同英文(level) | `VARCHAR(64)` | YES |
| `industry_code` | 行业代码 | `VARCHAR(64)` | YES |
| `is_pub` | 同英文(is_pub) | `VARCHAR(64)` | YES |
| `parent_code` | 同英文(parent_code) | `VARCHAR(64)` | YES |
| `src` | 同英文(src) | `VARCHAR(64)` | YES |

##### index_daily  (AS_MASTER)
- **数据源**：Tushare(index_daily)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标更新(范围抓取)
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_date` | 交易日期 | `DATE` | NO |
| `close` | 收盘价 | `FLOAT` | YES |
| `open` | 开盘价 | `FLOAT` | YES |
| `high` | 最高价 | `FLOAT` | YES |
| `low` | 最低价 | `FLOAT` | YES |
| `pre_close` | 昨收价 | `FLOAT` | YES |
| `change` | 涨跌额 | `FLOAT` | YES |
| `pct_chg` | 涨跌幅(%) | `FLOAT` | YES |
| `amount` | 成交额 | `FLOAT` | YES |
| `vol_share` | 成交量(股) | `FLOAT` | YES |

##### index_member_all  (AS_MASTER)
- **数据源**：Tushare(index_member_all)
- **频率(数据粒度)**：不定期
- **更新频率(建议)**：建议每日/每周更新一次
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(l3_code, ts_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `l1_code` | 同英文(l1_code) | `VARCHAR(64)` | YES |
| `l1_name` | 同英文(l1_name) | `VARCHAR(64)` | YES |
| `l2_code` | 同英文(l2_code) | `VARCHAR(64)` | YES |
| `l2_name` | 同英文(l2_name) | `VARCHAR(64)` | YES |
| `l3_code` | 同英文(l3_code) | `VARCHAR(64)` | NO |
| `l3_name` | 同英文(l3_name) | `VARCHAR(64)` | YES |
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `in_date` | 同英文(in_date) | `DATE` | YES |
| `out_date` | 同英文(out_date) | `DATE` | YES |
| `is_new` | 同英文(is_new) | `VARCHAR(64)` | YES |

##### index_weight  (AS_MASTER)
- **数据源**：Tushare(index_weight)
- **频率(数据粒度)**：月度
- **更新频率(建议)**：增量：按月份范围抓取(按配置的指数集合)
- **保留策略**：保留最近 2000 个开市日(按 trade_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(index_code, con_code, trade_date)`
- **备注**：月度数据：建议按月设置 start_date/end_date（当月第一天与最后一天）。
- **备注**：本项目默认只抓取 `.env` 的 `INDEX_WEIGHT_CODES` 指定的指数集合。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `index_code` | 同英文(index_code) | `-` | YES |
| `con_code` | 同英文(con_code) | `-` | YES |
| `trade_date` | 交易日期 | `-` | YES |
| `weight` | 同英文(weight) | `-` | YES |

##### limit_list_d  (AS_MASTER)
- **数据源**：Tushare(limit_list_d)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(ts_code, trade_date, limit)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `-` | YES |
| `ts_code` | 证券代码 | `-` | YES |
| `industry` | 行业 | `-` | YES |
| `name` | 名称 | `-` | YES |
| `close` | 收盘价 | `-` | YES |
| `pct_chg` | 涨跌幅(%) | `-` | YES |
| `amount` | 成交额 | `-` | YES |
| `limit_amount` | 涨跌停成交额 | `-` | YES |
| `float_mv` | 流通市值 | `-` | YES |
| `total_mv` | 总市值 | `-` | YES |
| `turnover_ratio` | 换手比 | `-` | YES |
| `fd_amount` | 封单金额 | `-` | YES |
| `first_time` | 首次涨跌停时间 | `-` | YES |
| `last_time` | 最后涨跌停时间 | `-` | YES |
| `open_times` | 打开次数 | `-` | YES |
| `up_stat` | 同英文(up_stat) | `-` | YES |
| `limit_times` | 同英文(limit_times) | `-` | YES |
| `limit` | 涨跌停标志 | `-` | YES |

##### moneyflow_hsgt  (AS_MASTER)
- **数据源**：Tushare(moneyflow_hsgt)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(trade_date)`
- **备注**：单位：万元（以 Tushare 当前返回为准）。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `DATE` | NO |
| `ggt_ss` | 港股通(沪)净流入(万元) | `DECIMAL(24, 2)` | YES |
| `ggt_sz` | 港股通(深)净流入(万元) | `DECIMAL(24, 2)` | YES |
| `hgt` | 沪股通净流入(万元) | `DECIMAL(24, 2)` | YES |
| `sgt` | 深股通净流入(万元) | `DECIMAL(24, 2)` | YES |
| `north_money` | 北向资金净流入(万元) | `DECIMAL(24, 2)` | YES |
| `south_money` | 南向资金净流入(万元) | `DECIMAL(24, 2)` | YES |

##### moneyflow_ind  (AS_MASTER)
- **数据源**：Tushare(moneyflow_dc)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(trade_date, ts_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `DATE` | NO |
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `pct_change` | 百分比涨跌额 | `FLOAT` | YES |
| `close` | 收盘价 | `FLOAT` | YES |
| `net_amount` | 净成交额 | `FLOAT` | YES |
| `net_amount_rate` | 净成交额率 | `FLOAT` | YES |
| `buy_elg_amount` | 买入特大单成交额 | `FLOAT` | YES |
| `buy_elg_amount_rate` | 买入特大单成交额率 | `FLOAT` | YES |
| `buy_lg_amount` | 买入大单成交额 | `FLOAT` | YES |
| `buy_lg_amount_rate` | 买入大单成交额率 | `FLOAT` | YES |
| `buy_md_amount` | 买入中单成交额 | `FLOAT` | YES |
| `buy_md_amount_rate` | 买入中单成交额率 | `FLOAT` | YES |
| `buy_sm_amount` | 买入小单成交额 | `FLOAT` | YES |
| `buy_sm_amount_rate` | 买入小单成交额率 | `FLOAT` | YES |

##### moneyflow_mkt  (AS_MASTER)
- **数据源**：Tushare(moneyflow_mkt_dc)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `DATE` | NO |
| `close_sh` | 同英文(close_sh) | `FLOAT` | YES |
| `pct_change_sh` | 同英文(pct_change_sh) | `FLOAT` | YES |
| `close_sz` | 同英文(close_sz) | `FLOAT` | YES |
| `pct_change_sz` | 同英文(pct_change_sz) | `FLOAT` | YES |
| `net_amount` | 净成交额 | `FLOAT` | YES |
| `net_amount_rate` | 净成交额率 | `FLOAT` | YES |
| `buy_elg_amount` | 买入特大单成交额 | `FLOAT` | YES |
| `buy_elg_amount_rate` | 买入特大单成交额率 | `FLOAT` | YES |
| `buy_lg_amount` | 买入大单成交额 | `FLOAT` | YES |
| `buy_lg_amount_rate` | 买入大单成交额率 | `FLOAT` | YES |
| `buy_md_amount` | 买入中单成交额 | `FLOAT` | YES |
| `buy_md_amount_rate` | 买入中单成交额率 | `FLOAT` | YES |
| `buy_sm_amount` | 买入小单成交额 | `FLOAT` | YES |
| `buy_sm_amount_rate` | 买入小单成交额率 | `FLOAT` | YES |

##### moneyflow_sector  (AS_MASTER)
- **数据源**：Tushare(moneyflow_ind_dc)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(trade_date, content_type, ts_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `DATE` | NO |
| `content_type` | 内容类型 | `VARCHAR(32)` | NO |
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `pct_change` | 百分比涨跌额 | `FLOAT` | YES |
| `close` | 收盘价 | `FLOAT` | YES |
| `net_amount` | 净成交额 | `FLOAT` | YES |
| `net_amount_rate` | 净成交额率 | `FLOAT` | YES |
| `buy_elg_amount` | 买入特大单成交额 | `FLOAT` | YES |
| `buy_elg_amount_rate` | 买入特大单成交额率 | `FLOAT` | YES |
| `buy_lg_amount` | 买入大单成交额 | `FLOAT` | YES |
| `buy_lg_amount_rate` | 买入大单成交额率 | `FLOAT` | YES |
| `buy_md_amount` | 买入中单成交额 | `FLOAT` | YES |
| `buy_md_amount_rate` | 买入中单成交额率 | `FLOAT` | YES |
| `buy_sm_amount` | 买入小单成交额 | `FLOAT` | YES |
| `buy_sm_amount_rate` | 买入小单成交额率 | `FLOAT` | YES |
| `buy_sm_amount_stock` | 同英文(buy_sm_amount_stock) | `VARCHAR(64)` | YES |
| `rank` | 同英文(rank) | `BIGINT` | YES |

##### share_float  (AS_MASTER)
- **数据源**：Tushare(share_float)
- **频率(数据粒度)**：事件/区间
- **更新频率(建议)**：增量：按日期范围循环抓取
- **保留策略**：保留最近 500 个开市日(按 float_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(ts_code, ann_date, float_date, holder_name, share_type)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `-` | YES |
| `ann_date` | 公告日期 | `-` | YES |
| `float_date` | 流通日期 | `-` | YES |
| `float_share` | 流通股本(股) | `-` | YES |
| `float_ratio` | 流通比 | `-` | YES |
| `holder_name` | 同英文(holder_name) | `-` | YES |
| `share_type` | 股类型 | `-` | YES |

##### st_list  (AS_MASTER)
- **数据源**：Tushare(namechange)
- **频率(数据粒度)**：事件/区间
- **更新频率(建议)**：建议每日更新(区间抓取)
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(ts_code, start_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `start_date` | 开始日期 | `DATE` | NO |
| `end_date` | 结束日期 | `DATE` | YES |
| `ann_date` | 公告日期 | `DATE` | YES |
| `change_reason` | 涨跌额原因 | `VARCHAR(64)` | YES |

##### stk_limit  (AS_MASTER)
- **数据源**：Tushare(stk_limit)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(ts_code, trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `-` | YES |
| `ts_code` | 证券代码 | `-` | YES |
| `up_limit` | 同英文(up_limit) | `-` | YES |
| `down_limit` | 同英文(down_limit) | `-` | YES |

##### stock_basic  (AS_MASTER)
- **数据源**：Tushare(stock_basic)
- **频率(数据粒度)**：不定期
- **更新频率(建议)**：建议每日/每周更新一次
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(ts_code)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `symbol` | 证券代码 | `VARCHAR(32)` | YES |
| `name` | 名称 | `VARCHAR(64)` | YES |
| `area` | 地区 | `VARCHAR(64)` | YES |
| `industry` | 行业 | `VARCHAR(64)` | YES |
| `cnspell` | 拼音 | `VARCHAR(64)` | YES |
| `market` | 市场 | `VARCHAR(32)` | YES |
| `list_date` | 上市日期 | `DATE` | YES |
| `act_name` | 同英文(act_name) | `VARCHAR(64)` | YES |
| `act_ent_type` | 同英文(act_ent_type) | `VARCHAR(64)` | YES |

##### suspend_d  (AS_MASTER)
- **数据源**：Tushare(suspend_d)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_date` | 交易日期 | `DATE` | NO |
| `suspend_timing` | 同英文(suspend_timing) | `VARCHAR(64)` | YES |
| `suspend_type` | 停牌类型 | `VARCHAR(64)` | YES |

##### trade_cal  (AS_MASTER)
- **数据源**：Tushare(trade_cal)
- **频率(数据粒度)**：日历
- **更新频率(建议)**：建议每日更新
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(exchange, cal_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `exchange` | 交易所 | `VARCHAR(32)` | NO |
| `cal_date` | 日历日期 | `DATE` | NO |
| `is_open` | 是否开市(1/0) | `BIGINT` | YES |
| `pretrade_date` | 上一交易日 | `DATE` | YES |

#### AS_5MIN_P1

##### etl_state  (AS_5MIN_P1)
- **数据源**：Internal(etl_state)
- **频率(数据粒度)**：元数据
- **更新频率(建议)**：由程序自动维护
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(cluster, table_name, cursor_col)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `cluster` | 集群 | `VARCHAR(32)` | NO |
| `table_name` | 表名 | `VARCHAR(64)` | NO |
| `cursor_col` | 游标列 | `VARCHAR(64)` | NO |
| `cursor_value` | 游标值 | `VARCHAR(64)` | YES |
| `updated_at` | 更新时间 | `DATETIME` | NO |

##### minute_5m  (AS_5MIN_P1)
- **数据源**：国金QMT/xtquant (download_history_data2 + get_market_data)
- **频率(数据粒度)**：5分钟
- **更新频率(建议)**：增量：按游标逐交易日更新；默认只处理已收盘交易日
- **保留策略**：保留最近 250 个开市日(按 trade_time 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_time)`
- **备注**：逻辑表 `minute_5m` 按 `ts_code` 哈希路由分片到 `AS_5MIN_P1/P2/P3`。
- **备注**：`volume` 入库后存为 `vol_share` (股)，按约定由“手”转换。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_time` | 交易时间 | `DATETIME` | NO |
| `amount` | 成交额 | `FLOAT` | YES |
| `close` | 收盘价 | `FLOAT` | YES |
| `high` | 最高价 | `FLOAT` | YES |
| `low` | 最低价 | `FLOAT` | YES |
| `open` | 开盘价 | `FLOAT` | YES |
| `vol_share` | 成交量(股) | `FLOAT` | YES |
| `volume` | 成交量(手) | `VARCHAR(64)` | YES |

#### AS_5MIN_P2

##### etl_state  (AS_5MIN_P2)
- **数据源**：Internal(etl_state)
- **频率(数据粒度)**：元数据
- **更新频率(建议)**：由程序自动维护
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(cluster, table_name, cursor_col)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `cluster` | 集群 | `VARCHAR(32)` | NO |
| `table_name` | 表名 | `VARCHAR(64)` | NO |
| `cursor_col` | 游标列 | `VARCHAR(64)` | NO |
| `cursor_value` | 游标值 | `VARCHAR(64)` | YES |
| `updated_at` | 更新时间 | `DATETIME` | NO |

##### minute_5m  (AS_5MIN_P2)
- **数据源**：国金QMT/xtquant (download_history_data2 + get_market_data)
- **频率(数据粒度)**：5分钟
- **更新频率(建议)**：增量：按游标逐交易日更新；默认只处理已收盘交易日
- **保留策略**：保留最近 250 个开市日(按 trade_time 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_time)`
- **备注**：逻辑表 `minute_5m` 按 `ts_code` 哈希路由分片到 `AS_5MIN_P1/P2/P3`。
- **备注**：`volume` 入库后存为 `vol_share` (股)，按约定由“手”转换。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_time` | 交易时间 | `DATETIME` | NO |
| `amount` | 成交额 | `FLOAT` | YES |
| `close` | 收盘价 | `FLOAT` | YES |
| `high` | 最高价 | `FLOAT` | YES |
| `low` | 最低价 | `FLOAT` | YES |
| `open` | 开盘价 | `FLOAT` | YES |
| `vol_share` | 成交量(股) | `FLOAT` | YES |
| `volume` | 成交量(手) | `VARCHAR(64)` | YES |

#### AS_5MIN_P3

##### etl_state  (AS_5MIN_P3)
- **数据源**：Internal(etl_state)
- **频率(数据粒度)**：元数据
- **更新频率(建议)**：由程序自动维护
- **保留策略**：不做自动删除
- **已建表**：是
- **主键**：`(cluster, table_name, cursor_col)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `cluster` | 集群 | `VARCHAR(32)` | NO |
| `table_name` | 表名 | `VARCHAR(64)` | NO |
| `cursor_col` | 游标列 | `VARCHAR(64)` | NO |
| `cursor_value` | 游标值 | `VARCHAR(64)` | YES |
| `updated_at` | 更新时间 | `DATETIME` | NO |

##### minute_5m  (AS_5MIN_P3)
- **数据源**：国金QMT/xtquant (download_history_data2 + get_market_data)
- **频率(数据粒度)**：5分钟
- **更新频率(建议)**：增量：按游标逐交易日更新；默认只处理已收盘交易日
- **保留策略**：保留最近 250 个开市日(按 trade_time 删除)
- **已建表**：是
- **主键**：`(ts_code, trade_time)`
- **备注**：逻辑表 `minute_5m` 按 `ts_code` 哈希路由分片到 `AS_5MIN_P1/P2/P3`。
- **备注**：`volume` 入库后存为 `vol_share` (股)，按约定由“手”转换。

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `ts_code` | 证券代码 | `VARCHAR(32)` | NO |
| `trade_time` | 交易时间 | `DATETIME` | NO |
| `open` | 开盘价 | `VARCHAR(64)` | YES |
| `high` | 最高价 | `VARCHAR(64)` | YES |
| `low` | 最低价 | `VARCHAR(64)` | YES |
| `close` | 收盘价 | `VARCHAR(64)` | YES |
| `volume` | 成交量(手) | `VARCHAR(64)` | YES |
| `amount` | 成交额 | `VARCHAR(64)` | YES |
| `vol_share` | 成交量(股) | `FLOAT` | YES |

<!-- END DB_SCHEMA -->
