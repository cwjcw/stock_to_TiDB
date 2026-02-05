# 数据库表逻辑说明（S_ 前缀）

> 所有 MySQL 表均以 `S_` 前缀区分。  
> 表为“全量字段入库”，除明确的字段改名/单位换算外，其余字段保持与源接口一致。

## 1. MySQL 表

### 1.1 S_daily_raw（日线行情 + 每日指标合并表）
- 数据源：Tushare 日线（doc_id=27）+ 每日指标（doc_id=32）
- 用途：日线基础行情 + 衍生指标的统一查询表
- 字段变更：
  - `amount`：Tushare 原始单位为“千元”，入库前 **×1000 转为元**
  - `vol`：Tushare 原始单位为“手”，入库后改名为 **`vol_share`**，并 **/100 转为股**
- 取数频率：手工启动；每次运行补齐“最近 N 个交易日窗口”内缺失日期
- 每次返回数据量（典型）：每交易日约等于 A 股数量（约 4-6 千行/日）

主键：`(ts_code, trade_date)`

---

### 1.2 S_adj_factor（复权因子）
- 数据源：Tushare 复权因子（doc_id=28）
- 用途：前复权计算（与 `S_daily_raw` / `S_minute_5m` 关联）
- 字段变更：无
- 取数频率：手工启动；随日线一起补缺
- 每次返回数据量（典型）：每交易日约等于 A 股数量（约 4-6 千行/日）

主键：`(ts_code, trade_date)`

---

### 1.3 S_minute_5m（5分钟原始行情，不复权）
- 数据源：国金证券 xtquant（`download_history_data2` + `get_market_data_ex`，period=5m）
- 用途：分钟级别回测/信号分析
- 字段变更：
  - `time`：统一格式为 `YYYYMMDDHHMMSS`，与 `trade_time` 一致
  - `volume/vol`：原始单位为“手”，入库后改名为 **`vol_share`**，并 **/100 转为股**
- 取数频率：手工启动；按“最近 N 个交易日窗口”补缺
- 每次返回数据量（典型）：每股每交易日约 48 根（5分钟 × 240 分钟/日）

主键：`(ts_code, trade_time)`

---

### 1.4 S_moneyflow_ind（个股资金流）
- 数据源：Tushare 资金流（doc_id=349，接口 `moneyflow_dc`）
- 用途：个股级资金流向分析
- 字段变更：无
- 取数频率：手工启动；按“最近 N 个交易日窗口”补缺
- 每次返回数据量（典型）：每交易日约等于 A 股数量（约 4-6 千行/日）
- 单位：金额类字段为 **万元**（Tushare 口径）

主键：`(ts_code, trade_date)`

---

### 1.5 S_moneyflow_sector（板块资金流）
- 数据源：Tushare 板块资金流（doc_id=344，接口 `moneyflow_ind_dc`，按 `content_type`=行业/概念/地域）
- 用途：板块资金流向/强弱分析
- 字段变更：无
- 取数频率：手工启动；按“最近 N 个交易日窗口”补缺
- 每次返回数据量（典型）：每交易日 * 每类板块数量（通常数百行/日）
- 单位：金额类字段为 **万元**（Tushare 口径）

主键：`(ts_code, trade_date, content_type)`

---

### 1.6 S_moneyflow_mkt（大盘资金流）
- 数据源：Tushare 大盘资金流（doc_id=345，接口 `moneyflow_mkt_dc`）
- 用途：大盘层面资金流向分析
- 字段变更：无
- 取数频率：手工启动；按“最近 N 个交易日窗口”补缺
- 每次返回数据量（典型）：**1 行/交易日**
- 单位：金额类字段为 **万元**（Tushare 口径）

主键：`(trade_date)`

---

### 1.7 S_moneyflow_hsgt（沪深港通资金流）
- 数据源：Tushare 沪深港通（doc_id=47，接口 `moneyflow_hsgt`）
- 用途：北向/南向资金统计分析
- 字段变更：无
- 取数频率：手工启动；按“最近 N 个交易日窗口”补缺
- 每次返回数据量（典型）：**1 行/交易日**
- 单位：金额类字段为 **万元**（Tushare 口径）

主键：`(trade_date)`

---

## 2. SQLite 实时缓存（temp_cache/realtime_cache.db）

### S_realtime_quote（实时行情缓存）
- 数据源：国金证券 xtquant 实时接口（自定义采集逻辑）
- 用途：短线监控/实时信号的轻量缓存
- 字段变更：
  - `time`：`YYYYMMDDHHMMSS`
  - `vol_share`：原始单位“手”，入库 **/100 转为股**
- 取数频率：取决于你实时抓取频率（秒/分钟级）
- 每次返回数据量：不固定（按实时采样频率与标的数量决定）

主键：`(ts_code, time)`

---

## 3. 参考文档
- Tushare：doc_id=27/32/28/349/344/345/47  
- 国金接口文档：`E:\\Software\\stock\\gjzqqmt\\QMT操作说明文档`

### Tushare 文档链接（doc_id）
```text
https://tushare.pro/document/2?doc_id=27
https://tushare.pro/document/2?doc_id=32
https://tushare.pro/document/2?doc_id=28
https://tushare.pro/document/2?doc_id=349
https://tushare.pro/document/2?doc_id=344
https://tushare.pro/document/2?doc_id=345
https://tushare.pro/document/2?doc_id=47
```
