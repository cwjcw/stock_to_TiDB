# 量化交易数据库表结构（自动生成）

- 生成时间：2026-02-10 10:56:37
- 说明：字段名(EN)来自实际 TiDB 表结构；中文字段名为本项目约定的解释或自动推断。
- 设计原则：除明确的字段改名/单位换算外，其余字段保持与源接口一致。

## 集群与表概览

- `AS_MASTER`: `adj_factor`, `daily_raw`, `etl_state`, `index_daily`, `moneyflow_hsgt`, `moneyflow_ind`, `moneyflow_mkt`, `moneyflow_sector`, `st_list`, `stock_basic`, `suspend_d`, `trade_cal`
- `AS_5MIN_P1`: `etl_state`, `minute_5m`
- `AS_5MIN_P2`: `etl_state`, `minute_5m`
- `AS_5MIN_P3`: `etl_state`, `minute_5m`

## AS_MASTER

### adj_factor  (AS_MASTER)
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

### daily_raw  (AS_MASTER)
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

### etl_state  (AS_MASTER)
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

### index_daily  (AS_MASTER)
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

### limit_list  (AS_MASTER)
- **数据源**：Tushare(limit_list)
- **频率(数据粒度)**：日频
- **更新频率(建议)**：增量：按游标逐交易日更新
- **保留策略**：保留最近 500 个开市日(按 trade_date/start_date 删除)
- **已建表**：否(尚未落库，字段来自接口探测/约定)
- **主键**：`(ts_code, trade_date)`

| Column (EN) | 字段(中文) | Type | Null |
|---|---|---:|:---:|
| `trade_date` | 交易日期 | `-` | YES |
| `ts_code` | 证券代码 | `-` | YES |
| `name` | 名称 | `-` | YES |
| `close` | 收盘价 | `-` | YES |
| `pct_chg` | 涨跌幅(%) | `-` | YES |
| `amp` | 振幅(%) | `-` | YES |
| `fc_ratio` | 封成比 | `-` | YES |
| `fl_ratio` | 封流比 | `-` | YES |
| `fd_amount` | 封单金额 | `-` | YES |
| `first_time` | 首次涨跌停时间 | `-` | YES |
| `last_time` | 最后涨跌停时间 | `-` | YES |
| `open_times` | 打开次数 | `-` | YES |
| `strth` | 强度 | `-` | YES |
| `limit` | 涨跌停标志 | `-` | YES |

### moneyflow_hsgt  (AS_MASTER)
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

### moneyflow_ind  (AS_MASTER)
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

### moneyflow_mkt  (AS_MASTER)
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

### moneyflow_sector  (AS_MASTER)
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

### st_list  (AS_MASTER)
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

### stock_basic  (AS_MASTER)
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

### suspend_d  (AS_MASTER)
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

### trade_cal  (AS_MASTER)
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

## AS_5MIN_P1

### etl_state  (AS_5MIN_P1)
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

### minute_5m  (AS_5MIN_P1)
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

## AS_5MIN_P2

### etl_state  (AS_5MIN_P2)
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

### minute_5m  (AS_5MIN_P2)
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

## AS_5MIN_P3

### etl_state  (AS_5MIN_P3)
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

### minute_5m  (AS_5MIN_P3)
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
