# Architecture

Lakehouse Flow 的目标是构建一个独立的湖仓资产状态调度决策层。它不应该先假设 cron 时间点已经可靠，而是先判断上游表或分区的 snapshot、watermark、quality、schema 状态是否真的满足下游任务要求。

## 架构边界

当前推荐边界：

```text
Lakehouse Flow 平台控制面
  负责：事件摄入、资产状态推进、FlowPlan DAG 决策、依赖门禁、作业启动/重启控制意图、幂等触发审计、snapshot 进展确认、调度意图交付

平台执行面
  负责：被动接收调度意图、按 writer 路由到实际计算引擎、资源队列、执行重试、运行日志、writer 适配；LF-1.0 参考实现使用 Flink CDC/Flink
```

Lakehouse Flow 只做调度，不真正执行任务；平台上的启动和重启操作由控制面形成独立 `JobControlIntent`，执行面根据 `writerJobKey` 调用实际引擎，`LF-1.0` 使用 Flink 验收。业务数据范围继续使用绑定 task 的 `SchedulingIntent`，不能为作业生命周期伪造 workflow/task instance。两类对象都只记录控制、投递和 snapshot 证据，不建立传统执行状态机。操作后的结果来自目标资产 snapshot 是否相对 baseline 推进，而不是任务运行状态回调。

## 当前实现

```text
Business Database
  -> Flink CDC continuous streaming
  -> Paimon ODS data snapshots
  -> LakehouseSnapshotScanner

LakehouseSnapshotScanner
  -> LakehouseSnapshotSourceProvider[*]
     -> PaimonSnapshotSource / future IcebergSnapshotSource / HudiSnapshotSource
  -> EventIngestionService
  -> SnapshotIngestionTransactionService
  -> LakehouseEventRepository + AssetStateService + EventConsumerOffsetRepository
  -> FlowPlanEvaluationService
  -> WorkflowInstance / TaskInstance / TriggerHistory
  -> SchedulingIntentOutboxScanner
  -> SchedulingIntent / SchedulingIntentDelivery
  -> database table / HTTP publisher / optional MQ gateway
  -> downstream Flink/Paimon stream-batch writer-side adapter
  -> SnapshotConfirmationService
  -> DagProgressionService

LF-1.0 G20 target branch
  -> platform START_JOB / RESTART_JOB operation
  -> WriterJobBinding + next writerEpoch
  -> JobControlIntent / JobControlIntentDelivery
  -> database table / HTTP publisher
  -> platform execution plane operates configured engine (Flink in LF-1.0)
  -> target data snapshot carries writer epoch evidence
```

已实现模块：

- `lakehouse-flow-model`: `LakehouseEvent`, `AssetState`, `DependencyCondition`, `FlowPlan`, `FlowPlanVersion`, `ScheduleNode`, `WorkflowInstance`, `TaskInstance`, `SchedulingIntent`, `SchedulingIntentDelivery`, `TriggerHistory`, `SchedulingAction`, `EventConsumerOffset`
- `lakehouse-flow-dao`: Spring Data JPA repository 与 PostgreSQL Flyway schema
- `lakehouse-flow-service`: 事件摄入辅助、资产状态推进、条件评估、snapshot 进展确认、FlowPlan 管理、调度意图交付、action、实例服务、触发审计服务
- `lakehouse-flow-api`: FlowPlan/Node、action、instance evidence、task scheduling intent 只读审计 API
- `lakehouse-flow-scheduler`: 内部 outbox 发布和 snapshot 确认扫描循环
- `lakehouse-flow-integration`: 格式无关 snapshot source SPI、通用扫描器与 Paimon Catalog API 适配器
- `lakehouse-flow-boot`: Spring Boot 启动入口

待完成的 `LF-1.0` 边界：

- 接入真实业务库到 Flink CDC 持续流式 ODS，并实现同时覆盖流式 checkpoint 与批式结束提交的 Paimon writer-side adapter
- 增加 `WriterJobBinding`、独立 `JobControlIntent`、writer epoch 和单表单写入者约束；当前 task-bound `SchedulingIntent` 不承担常驻作业生命周期
- 整体 Testcontainers E2E 验证 PostgreSQL 多 scheduler 竞争、事务回滚、claim 重占、fencing、DB/HTTP 投递和 offset 恢复
- snapshot 超时前校验 source 证据完整性，并补齐阻塞原因与 source 对账的最小运维 API

Iceberg/Hudi source、具体 MQ 产品绑定、可信身份/RBAC、Web UI 和高级并发策略不属于 `LF-1.0`。

### Flow 流批边界

Lakehouse Flow 必须区分“计算模式”和“调度激活方式”，不能把二者混成传统任务状态：

- ODS 数据入口固定为平台执行面托管的 Flink CDC 常驻流作业。平台控制面负责启动/重启意图，但不为每条 CDC 变更重复发送 intent；Lakehouse Flow 只摄入 Paimon ODS 的业务 snapshot，并把 snapshot/watermark 作为根依赖事实。
- DWD、DWS、ADS 不按层级强制模式。`ScheduleNode` 只声明 `processingMode=STREAMING|BATCH`，不保存 `FLINK`、`SPARK` 或其他执行引擎类型。`writerJobKey` 是平台作业的透明引用，由执行面解析实际引擎和作业定义。
- `STREAMING` 表示作业启动后自行持续消费上游新增 snapshot/changelog；Lakehouse Flow 不为普通输入推进逐次发送数据处理 intent，只观察其输出业务 snapshot。
- `BATCH` 表示依赖边界满足后由 Lakehouse Flow 发送一次数据处理 intent，平台执行面据此运行有界处理。
- 对补数、重跑等显式数据范围，流式 writer 在处理到冻结输入向量的提交上写入完成属性，批式 writer 在有界范围的最终数据提交上写入完成属性。具体 checkpoint、job-end 或 commit hook 由执行适配器解释，不成为 Flow 字段。
- `lakehouse-flow.final=true` 是“该 snapshot 已完成本次 intent 的逻辑输入范围”的兼容属性名，不表示执行引擎作业已结束。普通中间提交不得携带它。

因此 Flow 定义只需要冻结 `processingMode`。`STREAMING` 的持续激活和 `BATCH` 的按意图激活由该模式直接派生，不再额外持久化 `activationMode` 或可配置的 `completionBoundary`。运行时数据处理 intent 仍必须冻结完整 input snapshot/watermark vector，然后等待可归因目标 snapshot。流式与批式不得复制两套 DAG、补数、重跑、互斥或结果状态机。当前 Java 模型尚未携带 `processingMode` 和输入向量，这是 `LF-1.0` 的 G19，而不是已完成能力。

对 `STREAMING` 节点执行补数或重跑时，不把常驻作业重启状态纳入 Lakehouse Flow。Action 派生一个有明确输入向量的数据处理回放意图，可由当前流式 writer 消费，或按单写入者规则受控切换到批式运行，并以新的可归因目标 snapshot 确认。这使普通持续推进和历史回放共享同一个 BackfillBatch、DAG 门禁与 snapshot 结果模型。

### 平台作业生命周期与单写入者

一个 FlowPlan DAG 可以混合 `STREAMING` 和 `BATCH` 节点，但同一物理表只能由一个稳定作业写入。这里的唯一性按去除分区模板后的规范化 `tableAssetKey` 判断，不按 `targetAssetKey + bizDate` 判断：

```text
tableAssetKey -> exactly one writerJobKey
writerJobKey  -> one active writerEpoch at a time
```

- `WriterJobBinding` 是跨 Flow 共享的平台级对象，只保存平台作业标识、目标物理表和允许的流/批运行配置，不保存任何执行引擎的 JobStatus。FlowPlan/Node 只能引用它，不能在 Flow 隔离范围内重新声明同表 writer。
- 发布 FlowPlanVersion 时必须对全部输出做跨 Flow 校验：同一 `tableAssetKey` 绑定不同 `writerJobKey` 直接拒绝；同一 writer 的版本升级可以沿用绑定。
- 平台上的启动和重启操作分别产生独立的 `JobControlIntent(START_JOB)`、`JobControlIntent(RESTART_JOB)`。控制意图绑定 `WriterJobBinding` 而不是 task 或业务日期；重启会推进 `writerEpoch`，执行面必须先 fencing 旧 epoch，再启动新 epoch；Lakehouse Flow 只观察新 epoch 产生的可归因数据 snapshot。
- 流式和批式节点可以出现在同一 DAG。不同表可以绑定不同 writer；多个节点或版本若引用同一物理表，则必须引用同一个 `writerJobKey`。同一 writer 若支持流批一体运行，也只能在一个 epoch 内选择一种活动写入模式，不能让流式实例和批式实例同时写目标表。
- 已有 `SchedulingTargetAdmission(targetAssetKey, bizDate)` 仍用于同一逻辑日期的补数/正常/重跑互斥，但它不能替代物理表 writer 唯一绑定与 epoch fencing。

连续 writer 所属表需要补数时只允许两种策略：由当前流作业在同一 epoch 内消费回放意图，或由平台先 fencing 当前流 epoch，再以同一个 `writerJobKey` 的批式 profile 创建新 epoch，补数确认后再以更新 epoch 恢复流式 profile。禁止另建第二个 writer 作业与常驻流并发写表。对于普通有界批节点，绑定 task 的 `SchedulingIntent` 本身就是一次平台启动有界运行的授权，不需要再叠加 `JobControlIntent(START_JOB)`。

`JobControlIntent` 的 delivery ACK 只表示执行面收到操作。首次出现携带 `jobControlIntentKey + writerJobKey + writerEpoch` 且满足业务数据条件的目标 snapshot，才证明新写入世代已经推进数据；确认窗口内没有数据时只能得到 `SNAPSHOT_NOT_ADVANCED`，不能据此读取或推断执行引擎的 RUNNING/FAILED 状态。

## 领域模型

### LakehouseEvent

原始湖仓事件。事件是证据，用来记录“某个 source 观察到了某个 snapshot 或状态变化”。

关键字段：

- `eventId`: 去重键
- `sourceType`: `PAIMON`、后续可扩展 `ICEBERG` / `HUDI`
- `catalogName`, `databaseName`, `tableName`, `partitionName`
- `snapshotId`, `schemaId`, `watermark`, `commitTime`, `dataChange`
- `payloadJson`: 原始事件细节

### AssetState

资产调度事实表。调度判断应该以它为准，而不是以单个事件为准。

关键字段：

- `assetKey`: `catalog.database.table[.partition]`
- `latestSnapshotId`: 最新物理 snapshot，包含数据与维护提交
- `latestDataSnapshotId`: 最新业务数据 snapshot
- `latestWatermark`, `latestCommitTime`: 最新物理观察事实
- `latestDataWatermark`, `latestDataCommitTime`: 最新业务数据事实
- `qualityStatus`, `schemaStatus`, `backfillStatus`, `readinessStatus`
- `version`: JPA 乐观锁版本

单调性规则：

- 物理观察轨和业务数据轨分别单调推进，旧 snapshot 不能回退任一轨道。
- `COMPACT` / `ANALYZE` 只推进表级物理观察轨，不推进业务数据轨，也不创建分区业务状态。
- snapshot ID 字段虽然是字符串，但比较时数字 ID 按数字顺序处理。
- snapshot、schema、watermark、commit time 各自只允许单调推进；较新的 snapshot 不能携带较旧 watermark 覆盖现值。

### DependencySpec

依赖定义只存在于发布的 `FlowPlanVersion.dependencySpecJson` 和 `ScheduleNode.inputDependencySpecJson` 中，由 `FlowPlanConditionService` 解释 AND/OR 分组条件。旧 `AssetDependency` 运行时路径已经移除，避免同一资产变化进入两套自然触发模型；初始 schema 中保留的旧表仅用于兼容已有数据库升级，不再由 JPA 或调度服务读取。

当前条件评估器支持：

- `SNAPSHOT_EXISTS`
- `SNAPSHOT_ID_GTE`
- `WATERMARK_GTE`
- `QUALITY_PASSED`
- `SCHEMA_COMPATIBLE`

混合流批 DAG 的直接父边必须按父节点模式读取不同形式的 snapshot 证据：

- 流式父节点不为每个输出 snapshot 创建 `TaskInstance`。它的 `outputAssetSpec` 指向持续产出的资产，Lakehouse Flow 从 `AssetState` 和事件账本取得触发本次下游判断的 snapshot/watermark。
- 批式父节点使用同一调度实例内已经 `SNAPSHOT_CONFIRMED` 的 task 输出 snapshot，不能拿历史实例或任意最新状态替代。
- 下游批式节点只有在全部直接父边证据和自身 `inputDependencySpec` 同时满足后才进入 `READY_TO_SCHEDULE`，并把这些证据统一冻结为 `inputSnapshotVector`。
- 下游流式节点启动后自行监听所有声明输入的推进，不按每个父 snapshot 重复生成 task intent；Lakehouse Flow 继续观察它的输出供更下游使用。

当前 `DagProgressionService` 只会检查同一 workflow 内父 `TaskInstance.SNAPSHOT_CONFIRMED`，尚不能表达没有逐 snapshot task 的流式父节点。G19 必须把门禁从“父 task 状态集合”提升为“父节点输出 snapshot 证据向量”，同时保留批式父节点的同实例约束。

### FlowPlan / FlowPlanVersion / ScheduleNode

FlowPlan 是当前推荐的定义侧对象模型和轻量隔离边界。它把 workflow 概念收敛为可版本化的调度计划：

- `FlowPlan`: Flow code、名称、owner、space 和当前发布版本。
- `FlowPlanVersion`: 发布版本、图结构、依赖策略、触发策略、snapshot 确认策略和并发策略。
- `ScheduleNode`: 节点 code、`STREAMING|BATCH` 处理模式、writer binding、节点依赖、输入依赖条件、输出目标资产和节点级确认策略。

这些对象只描述调度图和 snapshot 确认策略，不描述 executor、资源队列或任务运行参数。发布版本只影响后续调度决策，不立即创建实例或执行任务。

发布前必须完成图校验：节点 code 唯一、父节点存在、不能自依赖、不能成环。自然触发、workflow 重跑和补数都从同一个已发布版本生成 task intents，避免定义路径分叉。

### TriggerHistory

触发审计表。它记录“为什么触发或跳过”，同时通过 `triggerKey` 为未来的触发动作提供幂等基础。

当前 `evaluationPayloadJson` 使用 PostgreSQL JSONB 存储结构化评估结果，不再写入 Java `toString()`。

### WorkflowInstance / TaskInstance

实例表只记录调度意图和 snapshot 结果确认，不记录外部执行状态。

当前核心状态：

- `CREATED`
- `WAITING_SNAPSHOT`
- `READY_TO_SCHEDULE`
- `SCHEDULED`
- `SNAPSHOT_CONFIRMED`
- `SNAPSHOT_NOT_ADVANCED`
- `CANCELLED`
- `SKIPPED`

其中 `SCHEDULED` 表示 Lakehouse Flow 已经发出调度意图，不表示任务已经开始运行；`SNAPSHOT_CONFIRMED` / `SNAPSHOT_NOT_ADVANCED` 来自目标资产 snapshot 证据。

`LF-1.0` 对外结果由三个正交维度组成：

```text
snapshotResult = WAITING | SNAPSHOT_CONFIRMED | SNAPSHOT_NOT_ADVANCED
deliveryStatus = PENDING | PUBLISHING | RETRY_WAIT | PUBLISHED | DELIVERY_EXHAUSTED
sourceHealth   = HEALTHY | REPAIRABLE | SOURCE_BLOCKED
```

`DELIVERY_EXHAUSTED` 是存储状态 `SchedulingIntentDelivery.EXHAUSTED` 的对外语义，只表示传输失败。`SOURCE_BLOCKED` 是 source 对账 `BLOCKED` 的对外语义，只表示 snapshot 证据链当前不可信。两者都不是 task 执行失败，也不应加入 `TaskInstance` 执行状态机。

`SNAPSHOT_NOT_ADVANCED` 只能在确认窗口结束，且 source 已追平到可判定该窗口、无 retention gap、offset 冲突或投影缺口时产生。若 source 不可用或证据链尚未修复，snapshot 结果保持待定并暴露 `SOURCE_BLOCKED`。暂时中断可以在无缺口补扫完成后恢复判定。

### EventConsumerOffset

事件源扫描位点。当前语义为：

```text
(source_type, source_name) -> offset_value
```

对 Paimon 来说，`offset_value` 是最后一个连续处理成功的 snapshot ID。对其他格式来说它是适配器定义的原生扫描位置，事务内推进时必须使用同一个 source 的 `offsetComparator`，不能套用 Paimon 的数值规则。

`(source_type, source_name)` 由数据库唯一约束保护；已有 offset 在推进事务中使用悲观行锁，避免多个调度实例把较新的位点覆盖为旧值。首次初始化的并发插入由唯一约束失败关闭并在下一轮扫描重试。

## 核心循环设计

### 1. 湖格式适配边界

`lakehouse-flow-integration` 通过 `LakehouseSnapshotSourceProvider` 动态提供受管表 source。调度核心只认识以下格式无关契约：

```text
LakehouseSourceIdentity
  -> sourceType + sourceName + catalog/database/table

LakehouseSnapshotSource
  -> scanAfter(opaqueOffset)
  -> offsetComparator()

LakehouseSnapshot
  -> opaque sourceOffset + normalized monotonic snapshotId + native schemaId
  -> watermark + commitTime + native commitKind
  -> dataChange + snapshotProperties + changedPartitions
```

`snapshotId` 是调度侧 baseline 与推进比较坐标，不一定等于格式的原生 snapshot id。Paimon 直接使用连续 snapshot id；Iceberg 应使用 sequence number 并把原生 snapshot id 留在 payload；Hudi 可使用 timeline instant。`schemaId` 跟随产生当前状态的 snapshot 更新，不假设它能跨格式或按字典序比较。

`commitKind` 是原生格式审计字段，不能作为跨湖统一语义。每个适配器必须把原生 operation 明确分类为 `dataChange=true/false`；snapshot 结果确认和自然触发只读取这个统一分类。为兼容契约 1.1 之前已落库的 Paimon 事件，无 `dataChange` 字段时暂时回退到 `APPEND/OVERWRITE` 判断，新适配器不得依赖该回退。

当前 Paimon 适配器使用官方 Catalog API 逐个读取保留的 snapshot，采集 snapshot properties，并读取 delta manifest list 与 manifest entry 生成 changed-partition 集合。offset 落后于最早保留 snapshot 时失败关闭，不能静默跨过已过期证据。Iceberg/Hudi 后续只新增 provider 和 adapter，不修改事件摄取、事务投影或调度服务。

source 首次没有 offset 时默认使用 `LATEST`，只以最新 snapshot 建立当前事实并做一次正常评估，避免接入一张存量表时自动回放全部历史。`EARLIEST` 必须显式配置；常规历史补数继续使用统一 `BackfillBatch`，不把 metadata source 回放当成补数替代品。

统一模型把 `snapshotProperties` 和 `changedPartitions` 作为 `LakehouseSnapshot` 的强类型字段，而不是要求每个 adapter 自行拼装 payload 键。事件 mapper 负责把统一字段投影到 durable event payload。不同 source 不允许声明相同的逻辑 `catalog.database.table`，避免多种湖格式写入同一个 `AssetState`。

### 2. 事件摄入循环

当前实现：`LakehouseSnapshotScanner` 定时调用 `EventIngestionService.ingestAllSources()`；每张表按 `(sourceType, sourceName)` 保存独立 offset。

原型流程：

```text
read offset_value
scan snapshots with snapshot_id > offset
sort snapshots by snapshot ordering
for each snapshot in order:
  begin transaction
  insert-or-load LakehouseEvent
  project every snapshot into table observed state
  if dataChange=true, also project table data state and changed partitions
  classify snapshot origin from immutable intent evidence
  if natural progression is allowed:
    release waiting DAG nodes affected by changed asset scopes
    evaluate published snapshot-driven FlowPlan roots once
  else:
    retain facts for intent confirmation without natural triggering
  persist this snapshot as source offset
  commit transaction
  if unexpected error: rollback this snapshot and stop batch
```

重要约束：offset 只能推进到连续成功处理的前缀。中间 snapshot 失败时必须停住，下一轮重试，否则会永久漏采。

### 3. 资产状态推进

当前实现：`AssetStateService.projectAssetStatesFromEvent()`；兼容入口 `updateAssetStateFromEvent()` 只更新事件自身表示的单一 scope。

规则：

```text
if asset_state 不存在:
  create observed state from event
  initialize data state only when dataChange=true
else:
  independently advance observed and data snapshot/watermark/commit-time fields
  never overwrite any field with older evidence
  ignore the event when no field advances
```

每个 snapshot 都更新表级物理观察状态；只有 `dataChange=true` 才更新表级业务数据状态，并从 `changedPartitions` 派生独立的分区状态。`ConditionEvaluator` 的 snapshot/watermark 条件只读取业务数据轨。例如历史 `dt=2026-09-01` 补数不会推进 `dt=2026-09-13` 的分区状态，compaction 也不能满足任何分区数据依赖。日期型 FlowPlan 必须使用带 `${bizDate}` 的分区资产；显式整表依赖会观察任意业务数据分区提交，这是整表语义而不是隔离缺陷。

### 4. 条件评估

当前实现由两层组成：`ConditionEvaluator` 评估单资产条件，`FlowPlanConditionService` 解析 FlowPlan 的 AND/OR 分组、`${bizDate}` 模板并聚合等待原因。空依赖表示没有外部资产门禁；DAG 父节点门禁由 `DagProgressionService` 独立负责。

### 5. 触发审计

当前实现：`FlowPlanEvaluationService` 扫描 `PUBLISHED` 且允许 snapshot trigger 的版本；发生变化的资产必须被 root 输入引用，并且全部 root 外部条件满足后才生成一次完整 DAG：

```text
AssetState changed
  -> find published FlowPlanVersion roots referencing changed asset
  -> evaluate all root dependency groups
  -> compute trigger_key and deduplicate
  -> create root READY + downstream WAITING task intents
  -> record TriggerHistory
```

当前 task-bound DAG 中，下游节点只有在同一 workflow、同一业务日期内的全部直接父 task 达到 `SNAPSHOT_CONFIRMED`，并且自身外部输入条件满足时，才会进入 `READY_TO_SCHEDULE`。混合流批 DAG 按前述规则把流式父节点替换为其输出资产 snapshot/watermark 证据。

### 6. 补数 DAG 推进

Node 补数以用户选中的 `startNode` 作为唯一显式边界：

```text
startNode -> READY_TO_SCHEDULE
selected downstream -> WAITING_SNAPSHOT
parent target snapshot all confirmed -> downstream READY_TO_SCHEDULE
any parent missing or not confirmed -> keep waiting
```

`startNode` 可以跳过它在完整 DAG 中的上游；其他选中节点不能跳过任何直接父节点。若级联子图包含汇聚节点但遗漏另一路父节点，创建阶段直接拒绝该范围，避免形成无法满足的等待实例。

Node 补数还支持日期级推进策略：`PARALLEL` 一次准入全部日期，`SERIAL` 一次准入一个日期，`PARALLEL_WITH_LIMIT` 最多准入 `maxActiveDates` 个日期。有限模式下，后续日期起点保持 `WAITING_CONCURRENCY`；只有较早日期的整张选中子图全部由目标 snapshot 推进确认后才补充槽位。该配额只控制调度意图可见性，不表示下游执行并发。

批次和意图交付使用相同的 batch-first 锁顺序。暂停批次不会释放新日期，恢复时重算槽位；任一节点确认窗口内 snapshot 未推进，批次转为 `FAILED` 并停止继续准入。

### 7. 跨实例目标日期准入

`SchedulingTargetAdmission` 为同一 `targetAssetKey + bizDate` 保存一个可复用的持久化槽位。正常调度、补数、恢复和重跑在首次发布前都必须获取该槽位；获取成功后才冻结 baseline 并写入 outbox，冲突任务保持 `READY_TO_SCHEDULE` 等待后续内部扫描。

槽位从 intent 发布持续到严格 snapshot 归因确认或确认窗口超时。确认事务先收口 task/DAG 调度状态，再校验 holder 并释放槽位；旧 intent 的晚到 snapshot 不能释放已经被新 task 重占的槽位。租约过期表示 Lakehouse Flow 可再次发布，不表示能够停止或确认旧的下游执行。

该机制与补数批次的日期配额相互独立：批次配额控制一个补数批次允许同时推进多少个日期，目标日期槽位控制所有来源是否可以同时向同一目标日期发布。未经过 Lakehouse Flow 的外部任务无法被本系统互斥，只能依靠严格 snapshot 属性归因避免结果串扰。

### 8. Snapshot 结果确认

当前实现：`SnapshotProgressService` 捕获 publication baseline，`SnapshotEvidenceService` 从持久化 snapshot 事件序列匹配调度意图。

`AssetState.latestSnapshotId` 吸收所有物理 snapshot，供 baseline、source 连续性和运维判断使用；资产依赖读取 `latestDataSnapshotId/latestDataWatermark`。task 结果确认不直接使用任一 latest 字段：

```text
scan every durable snapshot event after baseline
  -> require format adapter classification dataChange=true
  -> require exact source + intentKey + targetAsset + bizDate properties
  -> require lakehouse-flow.final=true as logical intent completion
  -> for partition targets, require changedPartitions evidence
  -> matching event confirms; unrelated events remain asset progress only
```

因此补数、正常调度、外部任务和 Paimon 维护提交可以交错产生 snapshot，而不会互相确认。即使匹配 snapshot 后又出现了更晚的外部 snapshot，历史事件仍保留匹配证据。

这条路径是 Lakehouse Flow 判断调度结果的唯一推荐入口。它不读取外部任务状态，不要求执行系统回调。完整属性约定见 [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md)。

### 9. Snapshot 触发路由

当前实现：`SnapshotTriggerRoutingService` 在事件完成持久化和 AssetState 投影后决定是否进入全局自然触发。

```text
external dataChange=true -> natural progression
valid logically-complete SNAPSHOT_DRIVEN intent -> natural progression
BACKFILL / BACKFILL_RECOVERY / RERUN / RERUN_TASK -> owning instance only
intermediate / orphan / forged / maintenance snapshot -> suppress natural progression
```

Action-owned snapshot 仍由 `SnapshotEvidenceService` 确认其 intent，并由 `DagProgressionService` 推进同一 workflow；它不会再次创建一条正常 `SNAPSHOT_DRIVEN` workflow。Lakehouse Flow 标记的正常逻辑完成 snapshot 使用 intent 冻结的 `bizDate`，不使用可能偏移的物理 commit 时间。

### 10. 调度意图主动发布

当前实现：`SchedulingIntentService` / `SchedulingIntentOutboxScanner` / `SchedulingIntentDeliveryScanner` / `SchedulingIntentDeliveryService` / `SchedulingIntentController`。

最小发布流程：

```text
SchedulingIntentOutboxScanner
  -> 内部读取 READY_TO_SCHEDULE task decision
  -> lock batch (when backfill), workflow, task
  -> 捕获受管 target asset baseline snapshot
  -> 构造版本化 instructionPayload 和 requiredSnapshotProperties
  -> 原子写入不可变 SchedulingIntent + 单一选定 route delivery
  -> task 进入 SCHEDULED 审计状态

SchedulingIntentDeliveryScanner (HTTP/MQ)
  -> 短事务 claim + fencing token
  -> 事务外调用 publisher SPI
  -> ACK: PUBLISHED
  -> failure: RETRY_WAIT + exponential backoff
  -> max attempts / admission deadline: EXHAUSTED dead letter

downstream
  -> 轮询 scheduling_intent.instruction_payload_json
  -> 或等待 HTTP publisher 调用
  -> 或消费 MQ gateway 发布的消息
  -> 幂等接收 taskCode + taskVersion 对应的流式或批式处理意图
  -> 处理到 intent 冻结的 input snapshot/watermark vector
  -> Flink/Paimon writer-side adapter 在逻辑完成 snapshot 写入 requiredSnapshotProperties

SnapshotConfirmationScanner
  -> 扫描 baseline 后的 target asset snapshot 事件
  -> 匹配 intent properties、commit kind 和目标分区
```

对外 REST 只提供按 task 查询已发布 intent 的审计能力，不提供 `/ready`、`/claim` 或 `/deliver`。任何 HTTP 响应、MQ broker ACK 或数据库可见性都只属于传输证据；Lakehouse Flow 不读取、不接收、不依赖下游执行结果，后续结果只由可归属于该 intent 的受管目标资产 snapshot 推进决定。

## 数据库

当前 SQL 以 PostgreSQL 为准，使用 `BIGSERIAL` 和 `JSONB`。不要再把 MySQL DDL 示例当作可运行事实。

核心表：

- `lakehouse_event`
- `asset_state`
- `asset_dependency`
- `workflow_definition`
- `task_definition`
- `workflow_instance`
- `task_instance`
- `trigger_history`
- `scheduler_lease`
- `event_consumer_offset`
- `backfill_spec`
- `flow_plan`
- `flow_plan_version`
- `schedule_node`
- `scheduling_action`
- `backfill_batch`
- `backfill_item`
- `scheduling_intent`
- `scheduling_intent_delivery`
- `scheduling_target_admission`
- `writer_job_binding`（LF-1.0 G20 目标表）
- `writer_job_epoch`（LF-1.0 G20 目标表；保存当前 epoch、活动模式、holder intent 和租约）
- `job_control_intent`（LF-1.0 G20 目标表；绑定 writer，不绑定 task/workflow/bizDate）
- `job_control_intent_delivery`（LF-1.0 G20 目标表；复用现有 delivery 算法，保持明确外键）

注意：

- `workflow_instance` 和 `task_instance` 使用 `scheduled_at` / `last_snapshot_check_at` 记录调度与 snapshot 检查时间，不记录执行开始/结束时间。
- `workflow_instance.biz_date` 和 `task_instance.biz_date` 使用 `TIMESTAMP`，与实体中的 `LocalDateTime` 对齐。
- `event_consumer_offset.offset_value` 与实体字段名一致。
- `trigger_history.evaluation_payload_json` 是 JSONB。
- `scheduling_intent.instruction_payload_json` 是跨数据库、HTTP 和 MQ 通道保持一致的版本化下游契约。
- `job_control_intent.instruction_payload_json` 使用独立契约；不得通过放宽 `scheduling_intent.task_instance_id` 约束来复用 task intent 表。

## 一致性策略

### 事件去重

`lakehouse_event.event_id` 唯一。重复事件不应重复创建事实，但可以重新投影已有事件以修复“事件已落库、状态投影失败”的边界情况。

### 状态单调推进

`AssetState` 的物理观察轨和业务数据轨都不回退。对于数字 snapshot ID，必须使用数字比较；字符串字典序会导致 `"99"` 被错误认为大于 `"100"`。

### Offset 保守推进

offset 代表“已连续处理到的位置”，不是“本批次见过的最大 snapshot”。如果 snapshot 100 失败但 101 成功，offset 仍不能推进到 101。

### Source 对账与补偿

每个 lakehouse adapter 必须实现 `inspectPosition(durableOffset)`，在适配器内部解释 earliest/latest、offset 顺序和 retention gap。公共对账服务只消费统一的 `SnapshotSourcePosition`，并核对以下证据链：

```text
live source range
  -> event_consumer_offset
  -> latest durable lakehouse_event
  -> table-level asset_state.latestSnapshotId
  -> latest durable data-change event
  -> table-level asset_state.latestDataSnapshotId
```

`UNINITIALIZED` 和 `LAGGING` 通过现有连续摄入事务补偿。V20 只从可确定分类的 durable data event 回建已有表/分区的业务轨，不会把语义不明的旧 `latestSnapshotId` 猜成业务数据状态；启动后物理或业务数据投影缺失/落后时，分别重放最新物理事件或最新 typed data event。幂等约束和双轨单调比较继续保护调度决定。`RETENTION_GAP`、`OFFSET_AHEAD`、offset 有值但事件缺失、source latest 与事件不一致，或 AssetState 声称存在但事件账本没有对应数据事实时必须 `BLOCKED`。

对账指标记录 source lag、retention gap 和投影一致性；delivery 指标记录 publisher 尝试、耗时和各 channel/status 记录数。所有这些都是调度观察与传输证据，不参与 task 成功失败判断。

`LF-1.0` 的 snapshot 超时扫描必须在写入 `SNAPSHOT_NOT_ADVANCED` 前消费这条对账证据链。如果 source 当前为 `REPAIRABLE/BLOCKED`，或无法证明 durable offset 已追平确认窗口，扫描器只能记录 `SOURCE_BLOCKED` 运维原因并保留 snapshot 待定。对账恢复且连续证据可用后，扫描器再判断是否存在可归因 snapshot。

为了让该门禁跨进程、跨 scheduler 实例可恢复，`LF1-01` 引入持久化的表级 `SnapshotSourceHealth`，而不让确认服务直接依赖 Paimon 适配器或内存扫描结果。最小证据包含：

- `sourceType + sourceName`：与 durable offset 一致的 source 主键。
- `tableAssetKey`：受管表级资产键，与 source registry 的唯一性规则一致。
- `outcome + offsetStatus + projectionStatus`：source、offset、event 和 AssetState 的归一化健康结果。
- `durableOffset + latestSourceOffset`：证明是否追平的扫描位点。
- `evidenceCheckedAt`：最后一次实际访问 source 并验证证据完整性的时间。
- `detail + updatedAt`：用于运维查询和并发更新审计。

分区 intent 必须通过 `targetAssetKey` 和确切的 `expectedPartition` 解析出 `tableAssetKey`；整表 intent 直接使用 `targetAssetKey`。无法唯一绑定受管 source 时不得产生“source 健康”的假证据，应以 `SOURCE_BLOCKED/UNMANAGED_TARGET_SOURCE` 失败关闭。

超时判定需要 `SnapshotSourceHealth` 已在确认截止时间之后成功检查，且结果为 `HEALTHY`。这允许 source 暂时中断后通过连续补扫恢复，同时防止用一份过期健康记录证明整个确认窗口没有目标 snapshot。

### 调度决策可观测性

`FlowPlanDecisionMetrics` 将自然 snapshot 触发拆成两层低基数指标：发布版本检查结果为 `TRIGGER_POLICY_FILTERED` / `ASSET_UNMATCHED` / `MATCHED`，匹配后的决策结果为 `EMITTED` / `BLOCKED_CONDITION` / `DEDUPLICATED` / `FAILED`，并记录决策耗时。`SchedulingBacklogMetrics` 通过分组查询统计固定的 task 非终态等待阶段，以及补数 `DATE_CONCURRENCY` / `DAG_DEPENDENCY` 阻塞类别。Flow code、asset key、snapshot id、trigger key、workflow id 和自由文本原因不进入 metric tag，而是写入结构化决策日志；已提交的高维审计事实仍以 `TriggerHistory` 为准。这里记录的是调度评估尝试和调度侧积压，不是下游运行结果，也不能替代 target snapshot 确认。

### 触发与发布幂等

FlowPlan trigger 使用稳定 `trigger_key`；task intent 使用 `task_instance_id` 唯一约束和稳定 `intent_key`，并在事务内按 batch、受限 FlowPlanVersion、workflow、task、writer binding/epoch、target-admission 顺序加锁。只有配置有限 `maxActiveInstances` 时才锁版本行，以原子完成活跃 workflow 计数和首次 intent 发布；无上限的 `PARALLEL` 不被版本锁串行化。`writer_job_binding` 以规范化 `table_asset_key` 唯一键保护单表单写入作业，`scheduling_target_admission` 以 `(target_asset_key, biz_date)` 保护逻辑日期首次发布。

作业启动/重启不走 workflow/task 锁链，而是锁 `writer_job_binding` 后原子分配下一 epoch，并以 `writerJobKey + writerEpoch` 唯一生成 `JobControlIntent`。task intent 和 job-control intent 的 HTTP/MQ delivery lease 都只能存在于各自 delivery 基础设施模型，不能进入 `TaskInstance`、`WriterJobBinding` 的运行状态，或参与 snapshot 结果判断；两类 delivery 服务应复用同一状态机算法与 publisher SPI，而不是共享可空多态外键。

## 后续架构路线

推荐按以下顺序推进：

1. Source-aware confirmation：将 source 对账健康纳入 snapshot 超时门禁，分开 snapshot、delivery 和 source 结果。
2. 作业与单写入者模型：增加 writer job binding、发布期物理表唯一校验、writer epoch，以及不绑定 task 的 `JobControlIntent` / delivery；平台启动/重启通过该控制对象交付。
3. 流批混合契约：节点只冻结 processing mode；运行时 intent 冻结由流式资产证据、批式同实例确认证据和额外依赖组成的 input snapshot/watermark vector。
4. Flink/Paimon 闭环：接入 Flink CDC 流式 ODS，并让统一 writer adapter 覆盖流式 checkpoint、批式结束提交和 epoch fencing 的归因、DAG 与补数验证。
5. 整体 E2E：使用 Testcontainers 验证 DB/HTTP、PostgreSQL 多 scheduler 锁竞争、中断、重占、fencing、回滚和 offset 恢复。
6. 最小运维面：补齐阻塞原因与 source 对账 API，随后冻结 1.0 REST、intent 和 migration 契约。
7. 1.1+ 扩展：再考虑高级并发策略、具体 MQ、可信身份/RBAC、Flow/Node 聚合与 UI、Iceberg/Hudi。
