# 现代 CDC 湖仓架构下的数仓与数据平台演进

> 目标：把前面讨论的“流批一体、CDC、数据湖/湖仓、数仓、数据平台”整理成一份可用于面试和架构讨论的复习资料。  
> 核心要求：不仅讲“流和批合成一套”，还要说明数据湖加持后，数仓建模、开发方式、指标产出、修复回溯、数据服务，以及数据平台的开发、调度、治理、资源、观测到底发生了什么实质变化。

## 1. 先给结论

在现代 CDC + 湖仓 + 流批一体架构下，最大的变化不是“实时任务和离线任务合并”，而是数据资产的组织方式发生了变化。

一句话：

> 数仓从“结果表建设”升级为“实体状态、事件事实、变更历史、指标资产和数据服务”的统一语义层；数据平台从“任务开发调度系统”升级为“围绕数据资产全生命周期的操作系统”。

如果只把 Lambda 架构里的实时链路和离线链路合并成一套技术栈，本质提升有限。真正有价值的变化应该体现在：

- ODS 不再只是业务库快照，而是同时承载 `latest-state` 和 `changelog`。
- DWD 不再按实时/离线拆两套，而是围绕实体、事件、关系、维度统一建模。
- 指标不再是某张报表结果表，而是可定义、可版本、可追踪、可服务化的指标资产。
- 数据修复不再只是重跑分区，而是基于 snapshot、tag、changelog、主键范围做局部回放和可控修复。
- 数据平台不再只是提交 SQL 和调度 DAG，而是管理数据契约、CDC 位点、schema 变更、模型版本、指标口径、血缘影响、质量闸口、资源成本和端到端 SLA。

## 2. 为什么 Lambda 架构下这些事做不好

Lambda 架构的典型形态是：

```text
业务库 / 日志
  -> 离线链路：T+1 抽取 -> Hive/Spark -> 离线数仓 -> 离线报表
  -> 实时链路：binlog/Kafka -> Flink/Storm -> 实时结果表/OLAP/Redis -> 实时看板
```

它解决了一个阶段的问题：既要离线准确性，也要实时新鲜度。但问题也很明显：

1. 双链路导致双口径
   同一个 GMV，实时链路一套 Flink 逻辑，离线链路一套 Spark SQL 逻辑。订单状态、退款、取消、维表口径、迟到数据处理稍有差异，结果就不一致。

2. ODS 以快照为主，变更历史不完整
   离线 ODS 常常只保留每日快照。后来发现某个订单金额变化了，只知道今天和昨天不同，但不知道中间经历了哪些 update/delete，也难以解释实时指标为什么波动。

3. 结果表导向，模型资产弱
   很多 ADS 表是为某个报表临时建设的。表越来越多，但实体、事件、指标之间的关系不清楚，复用性差。

4. 补数和修复成本高
   一旦发现实时链路逻辑有 bug，通常要离线重刷，再手工对齐实时结果。修复粒度经常是天分区或整张表，而不是某个 snapshot、某批变更、某组主键。

5. 平台只知道任务，不知道数据产品
   调度平台知道某个任务失败了，但不知道它影响哪个指标、哪个报表、哪个 API、哪个人群包。平台观测停留在 job 级，缺少数据资产级影响面。

## 3. 数据湖/湖仓带来的关键能力

现代湖仓表格式，比如 Paimon、Iceberg、Hudi、Delta，带来的不是“换个存储路径”，而是把数据表变成可版本、可增量、可回溯、可多引擎访问的资产。

关键能力包括：

- Snapshot：每次提交形成表版本，读写隔离，支持历史版本查询。
- Manifest/Data File：元数据和数据文件分层管理，可以支撑大规模文件裁剪和变更追踪。
- Primary Key Table：支持按主键维护最新状态，承载 CDC update/delete。
- Changelog：可以保留变更流，让湖表不仅能批查，也能继续被流式消费。
- Time Travel/Tag：可以回看或固化某个业务时点的数据版本。
- Schema Evolution：支持字段新增、类型变更等结构演进，但生产要有治理策略。
- Multi-Engine：Flink 写、Spark 补数、Trino 查询、StarRocks/Doris 加速可以围绕同一张表协作。
- Object Storage/HDFS：支持低成本长期存储和冷热分层。

所以，流批一体只是计算侧变化；湖仓表格式让“数据资产本身”变得可管理，这是架构升级的根本。

## 4. 数仓层面的变化：从“表”到“实体状态 + 事件事实 + 变更历史”

这是最重要的一点。过去说数仓建模，很多时候是在说“从源表抽到 ODS，再加工成 DWD/DWS/ADS 表”。在 CDC 湖仓架构下，建模对象应该重新拆分。

### 4.1 原来只围绕“表”建模的问题

以订单 GMV 为例。传统做法可能有这些表：

```text
ods_order_df              每日订单快照
dwd_order_detail_df       订单明细宽表
dws_trade_gmv_1d          每日 GMV 汇总
ads_gmv_report            GMV 报表结果
```

实时链路可能又有：

```text
kafka_order_binlog
flink_realtime_gmv
clickhouse_gmv_realtime
```

这样的问题是：

- 离线 GMV 可能从 `ods_order_df` 的最终状态算。
- 实时 GMV 可能从支付 binlog 或订单状态变更算。
- 退款、取消、金额改写、测试订单过滤、维表归属变化，两边很容易不一致。
- 如果只看每日快照，无法知道订单在一天内经历了哪些变化。
- 如果实时链路错算，补偿往往要靠离线结果覆盖，而不是解释每一笔差异。

### 4.2 新建模方式一：实体状态表

实体状态表回答的是：

> 这个业务对象现在是什么状态？

以订单为例，可以建：

```text
ods_order_latest
dwd_order_state_current
```

典型字段：

```text
order_id                 订单主键
user_id                  用户
merchant_id              商户
channel_id               渠道
order_status             当前订单状态
pay_status               当前支付状态
refund_status            当前退款状态
order_amount             下单金额
pay_amount               实付金额
refund_amount            已退款金额
net_amount               净支付金额
create_time              下单时间
pay_time                 支付时间
refund_time              最近退款时间
update_time              业务更新时间
op_ts                    源端变更时间
ingest_ts                入湖时间
source_table             来源表
schema_version           源端 schema 版本
sequence                 CDC 顺序字段
snapshot_id              湖表可见版本
```

这张表适合回答：

- 当前订单是不是已支付？
- 当前订单是否已退款？
- 当前订单最终金额是多少？
- 某个用户当前有效订单有哪些？
- 当前业务状态和源库是否一致？

但是，**GMV 不应该只依赖最新状态表计算**。因为最新状态会覆盖过程。

例如：

```text
10:00 订单创建，金额 100
10:05 支付成功，支付 100
11:00 退款 30
```

最新状态表最终可能是：

```text
pay_status = PAID
refund_status = PARTIAL_REFUNDED
pay_amount = 100
refund_amount = 30
net_amount = 70
```

它适合算“当前净额”，但不适合解释“10:05 的支付 GMV 事件”和“11:00 的退款冲减事件”。如果只从最新状态算 GMV，很容易把过程抹掉。

### 4.3 新建模方式二：事件事实表

事件事实表回答的是：

> 业务上发生了什么事件？

以 GMV 为例，应该从订单变更里识别业务事件，形成：

```text
dwd_trade_event_fact
```

典型字段：

```text
event_id                 事件唯一键
order_id                 订单 ID
user_id                  用户 ID
merchant_id              商户 ID
event_type               事件类型：ORDER_CREATE / PAY_SUCCESS / REFUND_SUCCESS / CANCEL
event_time               业务事件时间
event_amount             事件金额
gmv_amount               对 GMV 的影响金额
currency                 币种
channel_id               渠道
is_test_order            是否测试订单
is_valid                 是否有效事件
source_op_ts             源端变更时间
ingest_ts                入湖时间
dedup_key                去重键
trace_id                 链路追踪 ID
```

GMV 的核心应该来自事件事实，而不是直接来自最新状态：

```text
PAY_SUCCESS     -> gmv_amount = +pay_amount
REFUND_SUCCESS  -> 如果算净 GMV，gmv_amount = -refund_amount
CANCEL_UNPAID   -> gmv_amount = 0
ORDER_CREATE    -> 通常不计入 GMV，除非业务定义下单 GMV
```

这样，GMV 从“查某张订单表当前状态”变成“对交易事件事实进行聚合”。

优势是：

- 支付、退款、取消都有独立事件。
- 实时和离线都基于同一张事件事实表。
- 迟到事件可以按事件时间修正窗口。
- 补数可以按事件范围重放。
- 指标波动可以解释到具体事件。

### 4.4 新建模方式三：变更历史表

变更历史表回答的是：

> 源系统这条记录是怎么变过来的？

可以建：

```text
ods_order_changelog
dwd_order_change_history
```

典型字段：

```text
order_id                 订单主键
row_kind                 INSERT / UPDATE_BEFORE / UPDATE_AFTER / DELETE
before_image             变更前字段快照
after_image              变更后字段快照
changed_columns          变化字段列表
op_ts                    源端操作时间
binlog_file              binlog 文件
binlog_pos               binlog 位点
transaction_id           事务 ID
schema_version           schema 版本
ingest_ts                入湖时间
source_table             来源表
```

变更历史表不一定直接给业务报表查询，但它非常关键：

- 审计：谁在什么时候把订单金额从 100 改成 80？
- 追因：为什么昨天 GMV 从 100 万变成 98 万？
- 修复：某段时间 CDC 逻辑错了，可以按 changelog 重放。
- 对账：源端 binlog 位点和湖表 snapshot 可以关联。
- 质量：识别异常字段变化，比如订单金额变负数。

### 4.5 GMV 在新模型里的完整链路

推荐链路：

```text
MySQL order / payment / refund
    -> Flink CDC
    -> ODS changelog：原始变更历史
    -> ODS latest-state：订单当前状态
    -> DWD trade event fact：支付、退款、取消等业务事件
    -> DWS GMV aggregate：分钟/小时/天级 GMV
    -> ADS GMV service：报表、API、看板、经营分析
```

具体表设计：

```text
ods_order_changelog
  保存订单表每次 CDC 变化，保留 before/after、row_kind、op_ts、binlog 位点。

ods_order_latest
  Paimon/Iceberg/Hudi 主键表，按 order_id 保留订单最新状态。

dwd_trade_event_fact
  从 changelog/latest-state 中识别业务事件，如支付成功、退款成功。

dws_gmv_1m / dws_gmv_1h / dws_gmv_1d
  按事件时间、业务维度聚合 GMV。

ads_gmv_dashboard
  面向报表、经营看板或 API 的服务表。
```

### 4.6 用一笔订单解释 GMV 如何计算

假设订单生命周期如下：

```text
10:00  创建订单，order_amount = 100
10:05  支付成功，pay_amount = 100
11:00  部分退款，refund_amount = 30
11:30  商户归属从 A 类目调整到 B 类目
```

在 `ods_order_changelog` 中会保留所有变更：

```text
10:00 INSERT        order_status=CREATED, pay_status=UNPAID, amount=100
10:05 UPDATE_AFTER  pay_status=PAID, pay_amount=100
11:00 UPDATE_AFTER  refund_status=PARTIAL_REFUNDED, refund_amount=30
11:30 UPDATE_AFTER  category=A -> B
```

在 `ods_order_latest` 中只保留当前状态：

```text
order_id=1
pay_status=PAID
refund_status=PARTIAL_REFUNDED
pay_amount=100
refund_amount=30
net_amount=70
category=B
```

在 `dwd_trade_event_fact` 中应该识别出业务事件：

```text
10:05 PAY_SUCCESS     gmv_amount = +100
11:00 REFUND_SUCCESS  net_gmv_amount = -30
```

于是指标可以分开定义：

```text
支付 GMV      = sum(PAY_SUCCESS.pay_amount)
净 GMV        = sum(PAY_SUCCESS.pay_amount) - sum(REFUND_SUCCESS.refund_amount)
当前有效 GMV  = 基于 latest-state 的当前有效订单净额
财务确认 GMV  = T+1 校准后、排除测试单/异常单/对账失败单的版本
```

这里体现了现代数仓的关键变化：

- `事件事实` 用来算过程指标。
- `实体状态` 用来查当前状态和做最终对账。
- `变更历史` 用来审计、追溯和修复。
- `指标资产` 用来统一业务口径。

### 4.7 为什么 GMV 不能只从最新订单表算

只从最新状态表算 GMV 会遇到几个问题：

1. 过程被覆盖
   支付成功后又退款，最新状态只看到退款后的状态，看不到支付发生时的 GMV 事件。

2. 口径不清
   到底算支付 GMV、下单 GMV、净 GMV、结算 GMV，不能靠一张订单最新状态表隐含表达。

3. 维度归属会漂移
   商户类目、用户等级、渠道归属发生变化后，用当前维度回算历史 GMV 可能改变历史口径。要明确是按事件发生时维度，还是按当前维度。

4. 迟到和修正难处理
   如果支付事件 23:59 发生，CDC 00:01 到达，实时和离线按哪个日期算？必须基于事件时间、处理时间和入湖时间明确口径。

5. 审计无法解释
   财务或业务问“为什么昨天 GMV 修正了 2 万”，只看最新状态很难解释，需要 changelog 和事件事实。

### 4.8 GMV 的指标体系应该怎么设计

指标中心里不应该只有一个 `gmv` 字段，而要拆成层次：

原子指标：

```text
支付金额 pay_amount
退款金额 refund_amount
订单数 order_count
支付订单数 paid_order_count
```

派生指标：

```text
支付 GMV = sum(pay_amount where event_type='PAY_SUCCESS')
净 GMV = 支付 GMV - 退款金额
客单价 = 支付 GMV / 支付订单数
退款率 = 退款金额 / 支付 GMV
```

复合指标：

```text
实时经营 GMV
财务结算 GMV
渠道归因 GMV
商户经营 GMV
```

每个指标都要有：

- 业务定义。
- 统计口径。
- 时间口径：event_time、pay_time、settle_time、ingest_time。
- 维度范围。
- 过滤规则：测试单、内部单、异常单、取消单。
- 修正规则：退款、冲正、补单。
- SLA：秒级、分钟级、小时级、T+1。
- 负责人和审批记录。
- 指标版本。

### 4.9 指标产出方式如何变化

以前：

```text
写一张 ads_gmv_day 表给报表用。
```

现在：

```text
同一个 GMV 指标定义，可以产出多个服务形态。
```

例如：

```text
dws_gmv_1m_realtime
  分钟级实时看板，允许短暂修正。

dws_gmv_1d_offline
  T+1 离线校准，用于正式经营日报。

ads_gmv_olap
  同步到 StarRocks/Doris，承接高并发多维分析。

api_gmv_summary
  面向业务系统的数据 API。

feature_user_trade_7d
  面向推荐/风控的人群或特征。
```

这不是多套口径，而是同一个指标在不同 SLA 和服务场景下的多个产出版本。

## 5. 数据湖加持后，数仓还要发生的其他变化

### 5.1 从分区重跑到版本化修复

传统修复：

```text
删除某天分区 -> 重跑当天任务 -> 覆盖结果表
```

湖仓修复：

```text
定位问题 snapshot/tag
  -> 找到影响的 changelog 范围、主键范围、分区范围
  -> 启动补数/回放任务
  -> 生成新的 snapshot
  -> 对账通过后发布
  -> 保留修复记录和回滚点
```

这让数据修复从粗粒度重跑变成可审计的变更操作。

### 5.2 从静态分层到 SLA 分层

现代数仓不应该只按 ODS/DWD/DWS/ADS 静态分层，还应该按 SLA 分层：

```text
秒级：风控、实时监控、实时推荐特征
分钟级：经营看板、活动监控、实时 GMV
小时级：主题宽表、常规运营分析
T+1：财务、审计、复杂归因、正式报表
```

同一套模型可以有不同 SLA 的产出，但指标口径和血缘必须统一。

### 5.3 从表服务到数据产品

现代数仓产物不只是表，还包括：

- 表：湖仓表、OLAP 表、宽表、聚合表。
- 指标：GMV、订单数、转化率、留存率。
- 标签：用户标签、商户标签、风险标签。
- 特征：推荐/风控/搜索特征。
- API：面向业务系统的数据服务。
- 人群包：DMP 圈选结果。
- 报告：经营日报、监管报送、财务对账。

这些都应该有 owner、SLA、血缘、权限、质量和成本。

## 6. 数据平台层面的变化：治理不只是 Schema 变更

你指出得对：如果数据平台治理只讲 schema evolution，那太窄了。Schema 只是治理的一部分。现代 CDC 湖仓平台的治理至少要包含以下几个方向。

### 6.1 元数据治理

治理对象：

```text
Catalog
Database
Table
Column
Partition
Bucket
Snapshot
Tag
Schema Version
Metric
Model
Job
API
Dashboard
Owner
SLA
```

Lambda 架构下，元数据更多是“表在哪里、字段是什么”。现代平台要进一步管理运行态元数据：

- 这张表当前最新 snapshot 是多少？
- 这个指标由哪个模型产出？
- 这个 API 背后依赖哪些表和任务？
- 这张表的小文件、compaction、snapshot 保留是否健康？
- 这个字段是从源库哪个字段变换来的？

元数据不再是静态目录，而是数据资产运行状态的索引。

### 6.2 数据契约治理

CDC 架构下，源系统不是随便改表就结束。上游业务系统要和数据平台形成契约：

- 主键是什么，是否稳定？
- 哪些字段是核心字段，是否允许为空？
- 字段语义是什么，金额单位是什么？
- DDL 变更是否需要提前通知？
- 删除字段、重命名字段、主键变化是否允许？
- 数据延迟 SLA 是多少？
- 谁是 owner，谁负责解释字段？

数据契约的价值是把“源端随便变，下游被动炸”变成“变更可预期、可审批、可影响分析”。

### 6.3 血缘和影响面治理

现代平台血缘不能只停留在表级，要覆盖：

- 表级血缘：A 表产出 B 表。
- 字段级血缘：`pay_amount` 从哪个源字段来，经过哪些转换。
- 指标血缘：GMV 依赖哪些事件、过滤规则、维度。
- 任务血缘：哪些 Flink/Spark/SQL 任务参与产出。
- 服务血缘：哪些报表、API、人群包、特征依赖这个指标。
- 版本血缘：某个指标版本对应哪个模型版本、哪个 snapshot。

例如 `pay_amount` 字段类型从 decimal(18,2) 改成 decimal(20,4)，平台应该能告诉你：

```text
影响 dwd_trade_event_fact
影响 dws_gmv_1m / dws_gmv_1d
影响 ads_gmv_dashboard
影响 3 个 API
影响 2 个经营看板
影响 财务 T+1 对账任务
```

这才是治理的价值。

### 6.4 指标治理

指标治理要解决“一个公司到底有几个 GMV”的问题。

平台要管理：

- 原子指标、派生指标、复合指标。
- 指标口径、过滤条件、时间口径、维度口径。
- 实时版本、离线版本、财务确认版本。
- 指标 owner、审批人、使用方。
- 指标变更历史和影响面。
- 指标质量规则和 SLA。
- 指标下线和替代关系。

核心原则：

> 同一个业务指标只能有一个权威定义，但可以有多个 SLA 产出和多个服务形态。

### 6.5 数据质量治理

质量治理不能只在 ADS 结果表做校验，应该贯穿全链路：

源端质量：

- 主键是否为空。
- 主键是否重复。
- binlog/LSN 是否连续。
- 源表行数是否异常波动。

ODS 质量：

- CDC 延迟是否超阈值。
- delete/update 是否正确落入 latest-state。
- changelog 和 latest-state 是否能对账。
- schema_version 是否漂移。

DWD 质量：

- 事件识别是否正确。
- pay/refund/cancel 状态转换是否合法。
- 金额是否为负。
- 时间字段是否异常。

DWS/ADS 质量：

- GMV 波动是否超过阈值。
- 实时 GMV 和离线校准差异是否在范围内。
- 维度聚合是否漏值。
- 报表/API 是否按 SLA 产出。

质量规则还应该有动作：

```text
告警
阻断发布
隔离脏数据
降级产出
自动补偿
进入人工审批
```

### 6.6 权限和隐私治理

湖仓统一后，数据更集中，权限治理反而更重要。

平台要支持：

- 表级权限。
- 列级权限。
- 行级权限。
- 动态脱敏。
- 敏感字段识别。
- 权限申请审批。
- 使用目的记录。
- 查询审计。
- API 调用审计。
- 数据导出审计。
- 人群包导出审批。

例如 GMV 指标本身可能不敏感，但用户维度、手机号、设备号、支付账户、地理位置可能敏感。平台要能做到：分析师能看聚合 GMV，但不能随便导出明细用户列表。

### 6.7 生命周期治理

湖仓里 snapshot、changelog、tag、小文件、OLAP 副本都会产生长期成本。生命周期治理要管：

- 明细数据保留多久。
- changelog 保留多久。
- snapshot expire 策略。
- tag 长期保留策略。
- 热数据、温数据、冷数据分层。
- OLAP 加速表是否还被使用。
- 无人访问的数据产品是否下线。
- 废弃指标和废弃表如何归档。

传统数仓生命周期多是删分区；现代湖仓生命周期还要关注 snapshot、manifest、changelog、compaction 和下游 consumer。

### 6.8 变更治理

变更治理不等于 schema 变更，还包括：

- 源表 DDL 变更。
- 指标口径变更。
- 模型字段变更。
- 任务逻辑变更。
- 数据修复变更。
- 补数变更。
- 权限策略变更。
- 服务 API 变更。
- OLAP 物化视图变更。

每类变更都应该有：

```text
变更申请
影响分析
审批
灰度
回滚点
发布记录
质量校验
通知机制
```

例如 GMV 口径从“支付成功金额”改为“支付成功金额扣除退款”，这不是改一段 SQL，而是指标定义变化，必须影响分析、版本升级和使用方通知。

### 6.9 模型治理

模型治理解决的是数仓长期腐化问题。

平台要管：

- 表命名规范。
- 分层规范。
- 主键规范。
- 时间字段规范。
- 金额字段单位规范。
- 公共维度和公共事实复用。
- 临时表和实验表生命周期。
- 重复模型识别。
- 下线流程。

没有模型治理，流批一体只会更快地产生更多混乱表。

### 6.10 成本治理

成本治理既属于资源，也属于治理。现代平台需要知道一个数据产品的总成本：

```text
CDC 采集成本
Flink 长跑成本
Checkpoint/Savepoint 成本
湖表存储成本
Snapshot/Changelog 保留成本
Compaction 成本
Spark 补数成本
Trino 查询成本
OLAP 副本成本
API 服务成本
```

平台应该能回答：

- 这个 GMV 看板每天花多少钱？
- 某个指标是否还被使用？
- 是否有低价值高成本任务？
- 某张大表是否被重复同步到多个 OLAP？
- 小文件和 compaction 是否导致成本异常？

这也是和 Lambda 架构明显不同的地方。以前成本多按集群/队列看，现在要按数据产品、指标、表、租户看。

## 7. 数据平台五大能力如何升级

### 7.1 开发：从任务开发到模型/指标/数据产品开发

旧平台：

```text
写 SQL
提交任务
配置调度
看日志
```

新平台：

```text
定义数据源和契约
配置 CDC 入湖
定义实体模型和事件模型
定义指标
生成实时/离线/补数任务
发布前自动校验
产出表、API、指标、标签、特征
```

提升点：

- 一个开发入口覆盖 CDC、SQL、模型、指标、补数、服务。
- 一个模型可以生成多个 SLA 任务。
- 一个指标可以产出实时、离线、API、OLAP 多种形态。
- 发布前自动检查 schema、血缘、权限、资源、质量、savepoint。

### 7.2 调度：从时间 DAG 到数据资产状态调度

旧调度：

```text
每天 01:00 跑 A
A 成功后跑 B
B 成功后跑 C
```

新调度：

```text
snapshot 生成后触发下游
watermark 到达后触发窗口产出
quality check 通过后发布指标
tag 固化后触发财务报表
schema 变更审批后触发模型升级
补数完成后触发重算和对账
```

调度对象从“任务是否成功”升级为“数据资产是否可用”。

### 7.3 治理：从登记元数据到全生命周期治理

旧治理：

```text
表说明
字段说明
血缘展示
权限申请
```

新治理：

```text
数据契约
模型规范
指标口径
字段级血缘
质量闸口
变更审批
隐私合规
生命周期
成本归因
影响面分析
```

治理要参与生产链路，而不是只做展示。

### 7.4 资源：从队列资源到 SLA/成本资源

旧资源管理：

```text
YARN 队列
Flink 集群
Spark 队列
```

新资源管理：

```text
实时链路资源
批回刷资源
湖表 compaction 资源
OLAP 查询资源
对象存储成本
CDC 源库压力
按指标/表/租户归因
按 SLA 动态保障
```

资源调度要知道业务优先级。核心 GMV 链路应该优先保障，低价值临时报表可以降级、延后或限流。

### 7.5 观测：从 job running 到端到端数据可用性

旧观测：

```text
任务成功/失败
任务耗时
日志
重试次数
```

新观测：

```text
源库 binlog lag
CDC split 进度
Flink checkpoint
Sink commit
湖表 snapshot
Compaction backlog
小文件数量
DWD 事件产出
DWS 指标波动
ADS 查询延迟
API SLA
业务影响面
```

对 GMV 来说，观测不应该只说“任务成功了”，而要能回答：

- 支付事件有没有进来？
- 退款事件有没有冲减？
- 实时 GMV 和离线 GMV 差异多少？
- 差异来自延迟、迟到、维度变化、过滤规则，还是补数？
- 当前异常影响哪些看板、API、业务方？

## 8. GMV 端到端样例：现代架构应该长什么样

### 8.1 数据流

```text
MySQL order/payment/refund
  -> Flink CDC
  -> Paimon ODS changelog
  -> Paimon ODS latest-state
  -> DWD trade event fact
  -> DWS GMV realtime/offline aggregate
  -> StarRocks/Doris/Trino/API
```

### 8.2 表和职责

| 层级 | 表/资产 | 职责 |
|---|---|---|
| ODS | `ods_order_changelog` | 保留原始 CDC 变更，支持审计、回放、问题定位 |
| ODS | `ods_order_latest` | 按订单主键维护当前状态，支持查询当前订单状态和对账 |
| DWD | `dwd_trade_event_fact` | 把技术变更转成业务事件，如支付成功、退款成功 |
| DWD | `dwd_order_state_current` | 标准化后的订单实体状态，屏蔽源表结构差异 |
| DWS | `dws_gmv_1m` | 分钟级实时 GMV |
| DWS | `dws_gmv_1d` | 天级正式 GMV，可由离线校准 |
| ADS | `ads_gmv_dashboard` | 面向经营看板 |
| API | `api_gmv_summary` | 面向业务系统服务 |
| Metric | `metric.gmv` | GMV 权威定义和版本 |

### 8.3 指标定义

```text
metric: GMV
definition: 支付成功订单金额
event_source: dwd_trade_event_fact
event_type: PAY_SUCCESS
time_column: pay_time / event_time
amount_column: pay_amount
filters:
  - exclude test orders
  - exclude internal orders
  - include valid paid orders
correction:
  - refund affects net_gmv
  - cancellation before payment does not affect gmv
sli:
  realtime: 1 minute
  offline_confirmed: T+1
owner: trade data team
```

### 8.4 实时和离线如何共用口径

实时任务：

```text
dwd_trade_event_fact streaming read
  -> group by tumble(event_time, 1 minute), channel, merchant
  -> dws_gmv_1m
```

离线任务：

```text
dwd_trade_event_fact snapshot/tag read
  -> group by biz_date, channel, merchant
  -> dws_gmv_1d_confirmed
```

它们的区别是执行方式和 SLA，不是指标定义。

### 8.5 对账和修复

对账逻辑：

```text
事件事实累计支付金额
  vs
订单最新状态中的 paid/net amount
  vs
支付系统结算金额
```

如果发现差异：

```text
定位异常日期/维度
  -> 找到对应 event_id/order_id
  -> 回看 changelog 和 source binlog 位点
  -> 判断是迟到、重复、漏采、退款冲减、维度漂移还是口径变更
  -> 触发局部补数或指标重算
  -> 生成新 snapshot/tag
  -> 更新指标版本或修复记录
```

这就是现代湖仓数仓比 Lambda 更强的地方：不是简单用离线结果覆盖实时结果，而是能解释差异、重放差异、审计差异。

## 9. 和 Lambda 架构的本质差异总结

| 维度 | Lambda 架构 | CDC 湖仓流批一体架构 |
|---|---|---|
| 数据入口 | 批抽取 + 实时流两套 | CDC/change event 统一进入湖仓 |
| ODS | 每日快照为主 | latest-state + changelog + snapshot |
| 建模对象 | 源表/结果表 | 实体状态、事件事实、变更历史 |
| 指标 | 实时和离线容易两套口径 | 一个指标定义，多 SLA 产出 |
| 开发 | 写 SQL/任务 | 定义契约、模型、指标、服务 |
| 调度 | 时间 DAG | 时间、数据、事件、状态混合触发 |
| 治理 | 元数据登记、权限申请 | 契约、指标、质量、血缘、隐私、生命周期、变更影响 |
| 修复 | 重跑分区/覆盖结果 | snapshot/tag/changelog 局部回放 |
| 资源 | 按引擎/队列看 | 按链路、表、指标、SLA、租户归因 |
| 观测 | job 成功失败 | 数据新鲜度、质量、版本、SLA、业务影响面 |
| 服务 | 报表表 | 湖仓 + OLAP + API + Feature Store |

## 10. 面试表达

如果面试官问“在 CDC + 湖仓 + 流批一体架构下，数仓和数据平台应该怎么发展”，可以这样答：

> 我认为不能只理解成实时和离线合并。流批一体只是计算层统一，数据湖/湖仓真正带来的变化是数据资产可版本、可回放、可增量、可多引擎访问。所以数仓层面要从面向表和结果表，升级成实体状态、事件事实、变更历史和指标资产的统一建模。以 GMV 为例，不能只从订单最新状态表计算，而应该同时有订单 latest-state、订单 changelog、交易事件事实表和 GMV 指标定义。支付成功是 GMV 正向事件，退款是净 GMV 冲减事件，最新状态用于对账，变更历史用于审计和修复。  
>   
> 数据平台层面也不是把 Flink、Spark、Paimon、StarRocks 接到一个页面就结束。相比 Lambda 架构，平台要从任务开发调度系统升级成数据资产生命周期平台。开发上支持数据契约、模型、指标、CDC、补数和服务一体化；调度上从时间 DAG 升级为基于 snapshot、watermark、质量结果、schema 变更和补数状态的混合调度；治理上不只是 schema evolution，还包括元数据、数据契约、字段级血缘、指标口径、质量闸口、权限隐私、生命周期、成本归因和变更影响面；资源上从队列资源升级为按链路、表、指标、SLA 的成本治理；观测上从 job running 升级为端到端数据可用性和业务影响面。这个变化才是现代湖仓 CDC 架构相对 Lambda 架构的核心提升。

## 11. 参考资料

- Apache Flink：https://flink.apache.org/
- Apache Paimon Overview：https://paimon.apache.org/docs/1.4/concepts/overview/
- Apache Paimon Basic Concepts：https://paimon.apache.org/docs/1.4/concepts/basic-concepts/
- Apache Paimon Changelog Producer：https://paimon.apache.org/docs/1.4/primary-key-table/changelog-producer/
- Apache Flink CDC Schema Evolution：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/core-concept/schema-evolution/
