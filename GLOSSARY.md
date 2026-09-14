# Lakehouse Flow 中文术语表

本文档解释 Lakehouse Flow 文档、API 和代码中反复出现的领域术语。面向中文读者的文档应在术语第一次出现时使用“中文名称（English Term）”，后续可使用中文简称；类名、字段名、枚举值和协议键保持代码中的英文原文。

## A

### Action（调度操作）

用户或平台显式发起的一次调度侧操作，例如重跑、补数、取消、跳过、暂停补数、恢复补数或重新检查 snapshot。

Action 只改变 Lakehouse Flow 的调度记录和意图，不直接操作 Flink、Spark 等计算引擎。需要启动或重启常驻流作业时，Lakehouse Flow 会发布 `JobControlIntent`，仍由外部平台执行。

### Admission（调度准入）

Lakehouse Flow 在发布意图前执行的持久化互斥判断。当前包括：

- `targetAssetKey + bizDate`：防止正常调度、补数和重跑同时推进同一目标区间。
- Flow 版本并发：限制同一已发布 Flow 版本的活跃实例数。
- Writer 绑定：保证一个物理表只有一个受管 writer，并用 epoch 隔离新旧 writer 实例。

准入只约束 Lakehouse Flow 自己发布意图，不能锁住平台外部已经运行的作业。

### Asset / AssetState（数据资产 / 数据资产状态）

Asset 是可被观察和依赖的湖仓表或分区。`AssetState` 是 Lakehouse Flow 从持久化 snapshot 事件投影出的当前事实，供依赖判断使用。

`AssetState` 区分：

- `latestSnapshotId`：最近观察到的物理 snapshot，包括 compaction 等维护提交。
- `latestDataSnapshotId`：最近产生业务数据变化的 snapshot。

因此物理 snapshot 推进不一定代表业务数据已经就绪。

### Attribution（归因）

判断某个 snapshot 是否由指定调度意图对应的 writer 产生。Lakehouse Flow 不因为“目标表出现了更新”就确认任务，而是要求 snapshot 携带完整匹配的 intent、writer、epoch、目标资产、业务日期和逻辑完成标记。

## B

### Backfill（补数）

针对一个历史业务时间区间，重新推进选定 Node 及其下游资产。补数不是回退整个表到历史 snapshot，也不是在历史版本上分叉执行；它基于当前可见数据和当前受管 writer，为指定业务日期重新产生新的目标 snapshot。

补数起始 Node 可以绕过其上游 DAG 依赖直接进入 `READY_TO_SCHEDULE`，但仍必须通过自身目标 snapshot 推进确认结果。后续 Node 必须等待同一补数实例中的直接上游目标 snapshot 确认。

### BackfillBatch / BackfillItem（补数批次 / 补数项）

`BackfillBatch` 是一次补数请求的统一批次记录，冻结日期范围、入口 Node、选中子图、推进模式和恢复关系。`BackfillItem` 表示批次内一个“业务日期 + Node”的调度项。

批次状态是调度聚合结果，不是下游作业运行状态。

### Baseline Snapshot（基线 Snapshot）

意图第一次发布前冻结的目标资产 snapshot 坐标。只有基线之后的合格 snapshot 才能确认本次意图。基线用于避免把任务发布前已经存在的目标数据误认为本次产出。

## C

### CDC（变更数据捕获，Change Data Capture）

持续读取业务数据库变更日志并写入湖仓的方式。LF-1.0 的真实闭环使用 Flink CDC 将订单数据流式写入 ODS，并在 checkpoint 完成时提交 Paimon snapshot。

### Checkpoint（检查点）

流处理引擎保存一致性状态和 source offset 的恢复点。在本项目中，checkpoint 是否成功不是 Lakehouse Flow 的任务结果；只有 checkpoint 对应的 Paimon 数据 snapshot 被 source 观察并通过归因后，才构成调度证据。

### Claim / Lease / Fencing（认领 / 租约 / 隔离令牌）

用于多 scheduler 可靠投递：

- Claim：某个 scheduler 原子认领一批待投递记录。
- Lease：认领在一段时间内有效，进程中断后允许其他节点重占。
- Fencing token：每次认领生成的新令牌，阻止旧 owner 的迟到 ACK 覆盖新 owner 结果。

这些机制保证投递协调，不代表下游只会收到一次。下游仍必须按 `intentKey` 幂等消费。

### Compaction（文件合并）

湖仓为了优化文件布局执行的维护操作。Compaction 可能创建新 snapshot，但通常不代表业务数据变化。适配器必须将其标记为 `dataChange=false`；它可以推进物理观察坐标和 source offset，不能推进业务分区状态、触发下游或确认任务。

## D

### DAG（有向无环图，Directed Acyclic Graph）

Flow 中 Node 的依赖拓扑。一个 DAG 可以混合 `STREAMING` 和 `BATCH` Node。批式下游必须等待当前实例全部直接父节点的 snapshot 证据；流式父节点则通过持续产生的资产 snapshot 或 watermark 提供依赖证据。

### Data Snapshot / Maintenance Snapshot（业务数据 Snapshot / 维护 Snapshot）

业务数据 snapshot 表示表的业务内容发生变化，规范化为 `dataChange=true`。维护 snapshot 表示 compaction、统计分析或其他不改变业务结果的提交，规范化为 `dataChange=false`。

不同湖格式的原生 operation 名称不同，Lakehouse Flow 只依赖 source adapter 给出的统一分类。

### Delivery（投递）

把不可变意图送到外部平台的传输过程。LF-1.0 正式支持：

- `DATABASE_TABLE`：意图在数据库表中原子可见，由下游轮询消费。
- `HTTP`：Lakehouse Flow 主动 POST 到下游 endpoint。

`MQ` 保留中立 SPI，需要部署侧提供具体 broker gateway。`PUBLISHED` 只表示数据库可见、HTTP 2xx 或 broker 接受，不表示作业成功。

### Delivery Exhausted（投递耗尽）

`DELIVERY_EXHAUSTED` 表示意图超过传输重试上限。它是死信和运维告警，不是下游执行失败，也不能替代 snapshot 结果。

## F

### Flow / FlowPlan（数据流 / 数据流计划）

`FlowPlan` 是用户配置的调度隔离和版本管理边界，包含基本信息及多个不可变版本。文档中的 Flow 是对 `FlowPlan` 的简称。

Flow 不区分 Flink、Spark 或其他执行引擎；它只描述 Node 的流式或批式处理模式、资产依赖和输出资产。

### FlowPlanVersion（数据流计划版本）

一个 Flow 的不可变 DAG 版本。草稿版本可以编辑 Node，发布前必须通过图校验；发布后的实例、意图、补数和审计记录都引用明确版本，避免定义漂移。

## I

### Idempotency / Idempotency Key（幂等 / 幂等键）

同一请求或投递重复发生时只产生一个逻辑结果。主要幂等键包括：

- `actionKey`：用户调度操作。
- `triggerKey`：snapshot 驱动的自然触发。
- `intentKey`：下游消费、writer commit 和 snapshot 归因。
- `requestKey`：writer start/restart 平台请求。

### Intent（意图）

Lakehouse Flow 对外发布的声明式、不可变指令。它表达“希望下游推进什么”，而不是“Lakehouse Flow 已经执行了什么”。

中文文档通常写作“调度意图”；协议、类名和字段仍使用 `Intent`。意图至少包含：

- 稳定的幂等键和 schema 版本。
- Flow、Node、业务日期和触发来源。
- 冻结的输入 snapshot/watermark 向量。
- 目标资产和目标基线 snapshot。
- writer 身份与 `writerEpoch`。
- 下游提交目标 snapshot 时必须写入的归因属性。

### SchedulingIntent（数据处理调度意图）

要求下游运行一次有界数据处理逻辑并推进某个目标资产。它用于普通批节点、重跑和补数。下游接收后自行选择执行引擎和资源，Lakehouse Flow 等待可归因的目标 snapshot。

### JobControlIntent（作业控制意图）

要求平台启动或重启一个受管常驻 writer。它不创建虚假的业务日期任务，也不使用作业状态回调确认结果；新的 writer generation 产生可归因业务 snapshot 后，Lakehouse Flow 才确认该控制意图的 snapshot 结果。

## L

### LakehouseEvent（湖仓事件）

Source adapter 读取一个原生 snapshot 后生成的持久化原始证据。事件记录 source、表、snapshot、operation、业务分区、snapshot properties 和观察时间，并按 `eventId` 去重。

事件本身不是最终状态。摄取事务会用它推进 `AssetState`、路由触发并提交 source offset。

## N

### Node / ScheduleNode（节点 / 调度节点）

`ScheduleNode` 是 FlowPlanVersion 中的一个资产产出单元，描述：

- `processingMode=STREAMING|BATCH`
- 直接父 Node
- 额外资产条件
- 模板化目标资产
- 唯一 writer 绑定
- snapshot 确认策略

Node 不保存引擎类型、SQL、JAR 或资源队列信息；这些属于外部执行平台的 writer 配置。

## O

### Offset（来源消费位点）

Lakehouse Flow 对每个 snapshot source identity 持久化的连续消费位置。只有当前 snapshot 的事件和状态投影事务成功后，offset 才能推进；发生缺口或投影失败时必须停在最后连续成功位置。

### Outbox（事务发件箱）

先在业务事务中持久化待发布记录，再由后台 scanner 投递的可靠消息模式。Lakehouse Flow 使用 outbox 将“调度决策已经持久化”和“外部传输可能重试”分离。

## P

### Paimon

Apache Paimon 是 LF-1.0 真实闭环使用的湖表格式。Lakehouse Flow 通过 Catalog API 顺序读取 snapshot，通过 writer-side adapter 在 commit 时写入意图归因属性并执行 writer epoch fencing。

Paimon 是当前验证实现，不是核心模型的唯一格式；snapshot source 和变化分类通过 SPI 与 Iceberg、Hudi 等未来适配器隔离。

### Processing Mode（处理模式）

- `STREAMING`：常驻流式 writer 持续消费数据并周期性产生 snapshot。
- `BATCH`：收到一次调度意图后处理有界输入并产生逻辑完成 snapshot。

处理模式描述调度语义，不描述具体执行引擎。

## R

### Rerun（重跑）

对既有 WorkflowInstance、TaskInstance 或已发布 Node 再创建一次独立调度尝试。重跑会冻结新的目标 baseline 并生成新的 intent，不会修改历史 snapshot 证据，也不会把旧任务状态直接改回待运行。

## S

### Snapshot（快照）

湖表的一次不可变提交版本。Lakehouse Flow 把 snapshot ID 存为字符串以兼容不同湖格式，并通过 source 提供的顺序规则比较；不能使用普通字符串字典序比较数字 snapshot ID。

Snapshot 是 Lakehouse Flow 判断数据推进的核心证据，但“出现新 snapshot”本身不足以确认调度成功，还要满足数据变更、目标范围和归因要求。

### Snapshot Confirmation（Snapshot 确认）

在意图目标基线之后查找合格目标 snapshot 的过程。确认结果包括：

- `SNAPSHOT_CONFIRMED`：发现可归因的业务数据 snapshot。
- `SNAPSHOT_NOT_ADVANCED`：source 健康且确认窗口结束，仍没有目标证据。
- `SOURCE_BLOCKED`：source 缺口、不可用或投影不可信，暂时不能下结论。

### Snapshot Source（Snapshot 来源）

按顺序枚举某个湖表 snapshot 的格式适配器。核心 SPI 负责 identity、offset ordering、earliest/latest 和批量读取；Paimon 是 LF-1.0 的正式实现。

### Source Reconciliation（来源对账）

周期核对湖表 earliest/latest、持久化 offset、原始事件和 AssetState 投影是否一致。可修复的偏差通过重放持久化事件恢复；无法安全跨越的 retention gap 必须进入 `SOURCE_BLOCKED`。

## T

### TaskInstance（任务实例）

某个 Node 在一次 WorkflowInstance 中的调度记录。它保存依赖等待、意图发布、目标 baseline、观察到的 snapshot 和确认结果。

`TaskInstance` 不是执行引擎任务实例，不包含 `RUNNING/SUCCESS/FAILED` 生命周期。主要状态是：

- `CREATED`
- `WAITING_SNAPSHOT`
- `READY_TO_SCHEDULE`
- `SCHEDULED`
- `SNAPSHOT_CONFIRMED`
- `SNAPSHOT_NOT_ADVANCED`
- `CANCELLED`
- `SKIPPED`

### Trigger / TriggerKey（触发 / 触发幂等键）

Trigger 是 snapshot 变化经过依赖评估后形成的一次调度决策。`triggerKey` 将 Flow 版本、依赖和输入证据编码为唯一键，使多 scheduler 重复扫描时只创建一个逻辑实例。

## W

### Watermark（水位线）

流式数据的事件时间进度。它表达“在 source 约定下，早于该时间的数据已经处理到某个程度”，可以与 snapshot 一起组成依赖证据，但不能脱离 source 语义解释为绝对完整。

### WorkflowInstance（数据流实例）

某个已发布 FlowPlanVersion 在一个业务日期和触发上下文中的调度聚合记录。它包含同一 DAG 尝试的 TaskInstance，用于审计和聚合 snapshot 结果。

WorkflowInstance 不是 Flink Job、Spark Application 或外部 workflow 的镜像，也不跟踪下游运行状态。

### Writer / WriterJobKey / WriterEpoch（写入作业 / 写入作业键 / 写入世代）

Writer 是唯一有权写入某个受管物理表的外部作业配置。`writerJobKey` 是稳定身份，`writerEpoch` 是每次启动、重启或有界写入准入分配的单调世代。

Writer-side adapter 在 commit 期间检查当前 epoch；旧实例即使迟到，也不能提交一个会被 Lakehouse Flow 接受的 snapshot。

## 常见数仓分层

| 缩写 | 英文 | 常见含义 |
| --- | --- | --- |
| ODS | Operational Data Store | 原始业务数据接入层；本项目示例由 Flink CDC 流式写入 |
| DWD | Data Warehouse Detail | 清洗和标准化后的明细层 |
| DWS | Data Warehouse Summary/Service | 面向主题的汇总层 |
| ADS | Application Data Service | 面向应用或指标消费的结果层 |

## 易混淆概念

| 不要混为一谈 | 区别 |
| --- | --- |
| intent 已投递 vs 任务成功 | 投递只说明指令可见或被接收；成功只由目标 snapshot 证据确认 |
| 新 snapshot vs 新业务数据 | compaction 也会生成 snapshot；只有 `dataChange=true` 才可能推进业务状态 |
| 外部作业写入 vs 本次意图产出 | 必须匹配完整 snapshot attribution，不能只比较目标表和 snapshot ID |
| TaskInstance vs Flink Job | 前者是调度与证据记录，后者由外部平台运行 |
| 补数 vs source 历史回放 | 补数创建新的调度批次；source 回放用于恢复观察事实，可能触发历史数据且需谨慎启用 |
| `SOURCE_BLOCKED` vs `SNAPSHOT_NOT_ADVANCED` | 前者表示证据不可信、不能下结论；后者表示在健康 source 上确认没有推进 |
