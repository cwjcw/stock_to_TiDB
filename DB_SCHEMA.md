# 量化交易数据库表逻辑说明

> **设计原则**：
> 1. 表为“全量字段入库”，除明确的字段改名/单位换算外，其余字段保持与源接口一致。
> 2. 架构分布：基础设施、日线、资金流存放在 `AS_MASTER`；5分钟分时线分布在 `P1/P2/P3` 集群。

---

## 1. 基础设施与元数据 (存储于 AS_MASTER)

### 1.1 stock_basic (股票列表元数据)
- **数据源**：Tushare 股票列表 (doc_id=25)
- **用途**：基础信息检索、行业/地域分类统计。所有查询的“导航地图”。
- **主键**：`ts_code`
- **核心逻辑**：包含 `list_status` (L上市 D退市 P暂停)，用于过滤无效标的。

### 1.2 trade_cal (交易日历)
- **数据源**：Tushare 交易日历 (doc_id=26)
- **用途**：判断交易日、补齐缺失日期、生成回测序列。
- **主键**：`(exchange, cal_date)`

---

## 2. 日线级别数据 (存储于 AS_MASTER)

### 2.1 daily_raw (日线行情 + 每日指标合并表)
- **数据源**：Tushare 日线 (doc_id=27) + 每日指标 (doc_id=32)
- **用途**：日线基础行情 + 衍生指标（PE/PB/换手率等）统一查询。
- **字段变更**：
  - `amount`：Tushare 原始单位为“千元”，入库前 **×1000 转为元**
  - `vol`：Tushare 原始单位为“手”，入库后改名为 **`vol_share`**，并 **/100 转为股**
- **主键**：`(ts_code, trade_date)`

### 2.2 adj_factor (复权因子)
- **数据源**：Tushare 复权因子 (doc_id=28)
- **用途**：计算前复权/后复权价格，保证行情连续。
- **主键**：`(ts_code, trade_date)`

### 2.3 index_daily (指数日线行情)
- **数据源**：Tushare 指数日线 (doc_id=95)
- **用途**：存储上证、沪深300、创业板等基准指数。计算超额收益 (Alpha)。
- **字段变更**：`vol` 改名为 **`vol_share`**，单位转为 **股**。
- **主键**：`(ts_code, trade_date)`

---

## 3. 分时高频行情 (分布式存储于 P1, P2, P3)

### 3.1 minute_5m (5分钟原始行情，不复权)
- **数据源**：国金 xtquant (`download_history_data2` + `get_market_data_ex`)
- **用途**：分钟级别信号分析。
- **字段变更**：
  - `time`：统一格式为 `YYYYMMDDHHMMSS`
  - `volume/vol`：原始单位为“手”，入库后改名为 **`vol_share`**，并 **/100 转为股**
- **主键**：`(ts_code, trade_time)`
- **存储建议**：按 `ts_code` 哈希路由分布至三个分片集群。

---

## 4. 资金流向分析 (存储于 AS_MASTER)

### 4.1 moneyflow_ind (个股资金流)
- **数据源**：Tushare 资金流 (doc_id=349, 接口 `moneyflow_dc`)
- **单位**：金额类字段为 **万元**
- **主键**：`(ts_code, trade_date)`

### 4.2 moneyflow_sector (板块资金流)
- **数据源**：Tushare 板块资金流 (doc_id=344, 接口 `moneyflow_ind_dc`)
- **主键**：`(ts_code, trade_date, content_type)` (行业/概念/地域)

### 4.3 moneyflow_mkt (大盘资金流)
- **数据源**：Tushare 大盘资金流 (doc_id=345, 接口 `moneyflow_mkt_dc`)
- **主键**：`(trade_date)`

### 4.4 moneyflow_hsgt (沪深港通资金流)
- **数据源**：Tushare 沪深港通 (doc_id=47, 接口 `moneyflow_hsgt`)
- **用途**：北向/南向资金统计分析。
- **主键**：`(trade_date)`

---

## 5. 风险监控与状态过滤 (存储于 AS_MASTER)

### 5.1 limit_list (每日涨跌停统计)
- **数据源**：Tushare 涨跌停 (doc_id=298)
- **主键**：`(ts_code, trade_date)`

### 5.2 st_list (ST 状态记录)
- **数据源**：Tushare 股票更名/ST 记录 (doc_id=397)
- **用途**：回测时动态剔除风险警示股。
- **主键**：`(ts_code, start_date)`

### 5.3 suspend_d (停复牌记录)
- **数据源**：Tushare (doc_id=214)
- **用途**：过滤当日无法买卖的标的。
- **主键**：`(ts_code, trade_date)`

---

## 6. 实时缓存 (SQLite: realtime_cache.db)

### realtime_quote (实时行情缓存)
- **数据源**：xtquant 实时接口
- **字段变更**：`vol_share` 原始单位“手”，入库 **/100 转为股**。
- **主键**：`(ts_code, time)`

---

## 7. 参考文档及链接

### 国金QMT接口文档：E:\\Software\\stock\\gjzqqmt\\QMT操作说明文档

### Tushare 文档链接 (doc_id)
```text
[https://tushare.pro/document/2?doc_id=25](https://tushare.pro/document/2?doc_id=25) (股票列表)
[https://tushare.pro/document/2?doc_id=26](https://tushare.pro/document/2?doc_id=26) (交易日历)
[https://tushare.pro/document/2?doc_id=27](https://tushare.pro/document/2?doc_id=27) (日线行情)
[https://tushare.pro/document/2?doc_id=32](https://tushare.pro/document/2?doc_id=32) (每日指标)
[https://tushare.pro/document/2?doc_id=28](https://tushare.pro/document/2?doc_id=28) (复权因子)
[https://tushare.pro/document/2?doc_id=95](https://tushare.pro/document/2?doc_id=95) (指数日线)
[https://tushare.pro/document/2?doc_id=349](https://tushare.pro/document/2?doc_id=349) (个股资金流)
[https://tushare.pro/document/2?doc_id=344](https://tushare.pro/document/2?doc_id=344) (板块资金流)
[https://tushare.pro/document/2?doc_id=345](https://tushare.pro/document/2?doc_id=345) (大盘资金流)
[https://tushare.pro/document/2?doc_id=47](https://tushare.pro/document/2?doc_id=47) (沪深港通)
[https://tushare.pro/document/2?doc_id=298](https://tushare.pro/document/2?doc_id=298) (涨跌停)
[https://tushare.pro/document/2?doc_id=214](https://tushare.pro/document/2?doc_id=214) (停复牌)
[https://tushare.pro/document/2?doc_id=397](https://tushare.pro/document/2?doc_id=397) (ST股票列表)
