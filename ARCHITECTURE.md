# Architecture

Lakehouse Flow 的目标是构建一个独立的湖仓资产状态调度决策层。它不应该先假设 cron 时间点已经可靠，而是先判断上游表或分区的 snapshot、watermark、quality、schema 状态是否真的满足下游任务要求。

## 架构边界

当前推荐边界：

```text
Lakehouse Flow
  负责：事件摄入、资产状态推进、FlowPlan DAG 决策、依赖门禁、幂等触发审计、snapshot 进展确认、调度意图交付

外部执行系统
  负责：被动接收调度意图、资源队列、任务执行、执行重试、运行日志、执行器适配
```

Lakehouse Flow 只做调度，不真正执行任务；因此 workflow/task instance 是调度与确认的审计记录，不是传统执行状态机。调度后的成功/失败来自目标资产 snapshot 是否相对 baseline 推进，而不是任务运行状态回调。

## 当前实现

```text
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
  -> database table / future HTTP or MQ publisher
  -> SnapshotConfirmationService
  -> DagProgressionService
```

已实现模块：

- `lakehouse-flow-model`: `LakehouseEvent`, `AssetState`, `AssetDependency`, `FlowPlan`, `FlowPlanVersion`, `ScheduleNode`, `WorkflowInstance`, `TaskInstance`, `SchedulingIntent`, `SchedulingIntentDelivery`, `TriggerHistory`, `SchedulingAction`, `EventConsumerOffset`
- `lakehouse-flow-dao`: Spring Data JPA repository 与 PostgreSQL Flyway schema
- `lakehouse-flow-service`: 事件摄入辅助、资产状态推进、条件评估、snapshot 进展确认、FlowPlan 管理、调度意图交付、action、实例服务、触发审计服务
- `lakehouse-flow-api`: FlowPlan/Node、action、instance evidence、task scheduling intent 只读审计 API
- `lakehouse-flow-scheduler`: 内部 outbox 发布和 snapshot 确认扫描循环
- `lakehouse-flow-integration`: 格式无关 snapshot source SPI、通用扫描器与 Paimon Catalog API 适配器
- `lakehouse-flow-boot`: Spring Boot 启动入口

占位或未实现模块：

- Paimon 真实环境整体 E2E、Iceberg/Hudi source、HTTP/MQ publisher、内部投递退避/死信、统一审计查询、UI 均未完成

## 领域模型

### LakehouseEvent

原始湖仓事件。事件是证据，用来记录“某个 source 观察到了某个 snapshot 或状态变化”。

关键字段：

- `eventId`: 去重键
- `sourceType`: `PAIMON`、后续可扩展 `ICEBERG` / `HUDI`
- `catalogName`, `databaseName`, `tableName`, `partitionName`
- `snapshotId`, `schemaId`, `watermark`, `commitTime`
- `payloadJson`: 原始事件细节

### AssetState

资产调度事实表。调度判断应该以它为准，而不是以单个事件为准。

关键字段：

- `assetKey`: `catalog.database.table[.partition]`
- `latestSnapshotId`: 最新 snapshot
- `latestWatermark`: 最新事件时间水位
- `qualityStatus`, `schemaStatus`, `backfillStatus`, `readinessStatus`
- `version`: JPA 乐观锁版本

单调性规则：

- 新 snapshot 可以推进状态，旧 snapshot 不能回退状态。
- snapshot ID 字段虽然是字符串，但比较时数字 ID 按数字顺序处理。
- snapshot、schema、watermark、commit time 各自只允许单调推进；较新的 snapshot 不能携带较旧 watermark 覆盖现值。

### AssetDependency

`AssetDependency` 是旧兼容模型，不再是推荐的自然触发入口。新定义使用 `FlowPlanVersion.dependencySpecJson` 和 `ScheduleNode.inputDependencySpecJson`，由 `FlowPlanConditionService` 解释 AND/OR 分组条件。

当前条件评估器支持：

- `SNAPSHOT_EXISTS`
- `SNAPSHOT_ID_GTE`
- `WATERMARK_GTE`
- `QUALITY_PASSED`
- `SCHEMA_COMPATIBLE`

### FlowPlan / FlowPlanVersion / ScheduleNode

FlowPlan 是当前推荐的定义侧对象模型和轻量隔离边界。它把 workflow 概念收敛为可版本化的调度计划：

- `FlowPlan`: Flow code、名称、owner、space 和当前发布版本。
- `FlowPlanVersion`: 发布版本、图结构、依赖策略、触发策略、snapshot 确认策略和并发策略。
- `ScheduleNode`: 节点 code、节点依赖、输入依赖条件、输出目标资产和节点级确认策略。

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
  project table and changed-partition AssetState monotonically
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
  create state from event
else:
  independently advance snapshot/schema/watermark/commit time fields
  never overwrite any field with older evidence
  ignore the event when no field advances
```

一个 snapshot 始终更新表级状态，并从 `changedPartitions` 派生独立的分区级状态。例如历史 `dt=2026-09-01` 补数不会推进 `dt=2026-09-13` 的分区状态。日期型 FlowPlan 必须使用带 `${bizDate}` 的分区资产；显式整表依赖会观察任意分区提交，这是整表语义而不是隔离缺陷。

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

下游节点只有在同一 workflow、同一业务日期内的全部直接父节点达到 `SNAPSHOT_CONFIRMED`，并且自身外部输入条件满足时，才会进入 `READY_TO_SCHEDULE`。

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

`AssetState` 继续吸收所有 snapshot 推进，供资产依赖和最新状态判断使用；task 结果确认不再只读取 `asset_state.latest_snapshot_id`：

```text
scan every durable snapshot event after baseline
  -> require format adapter classification dataChange=true
  -> require exact source + intentKey + targetAsset + bizDate properties
  -> require lakehouse-flow.final=true
  -> for partition targets, require changedPartitions evidence
  -> matching event confirms; unrelated events remain asset progress only
```

因此补数、正常调度、外部任务和 Paimon 维护提交可以交错产生 snapshot，而不会互相确认。即使匹配 snapshot 后又出现了更晚的外部 snapshot，历史事件仍保留匹配证据。

这条路径是 Lakehouse Flow 判断调度结果的唯一推荐入口。它不读取外部任务状态，不要求执行系统回调。完整属性约定见 [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md)。

### 9. Snapshot 触发路由

当前实现：`SnapshotTriggerRoutingService` 在事件完成持久化和 AssetState 投影后决定是否进入全局自然触发。

```text
external dataChange=true -> natural progression
valid final SNAPSHOT_DRIVEN intent -> natural progression
BACKFILL / BACKFILL_RECOVERY / RERUN / RERUN_TASK -> owning instance only
intermediate / orphan / forged / maintenance snapshot -> suppress natural progression
```

Action-owned snapshot 仍由 `SnapshotEvidenceService` 确认其 intent，并由 `DagProgressionService` 推进同一 workflow；它不会再次创建一条正常 `SNAPSHOT_DRIVEN` workflow。Lakehouse Flow 标记的正常最终 snapshot 使用 intent 冻结的 `bizDate`，不使用可能偏移的物理 commit 时间。

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
  -> 幂等执行 taskCode + taskVersion 对应工作
  -> 在最终目标 snapshot 写入 requiredSnapshotProperties

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

注意：

- `workflow_instance` 和 `task_instance` 使用 `scheduled_at` / `last_snapshot_check_at` 记录调度与 snapshot 检查时间，不记录执行开始/结束时间。
- `workflow_instance.biz_date` 和 `task_instance.biz_date` 使用 `TIMESTAMP`，与实体中的 `LocalDateTime` 对齐。
- `event_consumer_offset.offset_value` 与实体字段名一致。
- `trigger_history.evaluation_payload_json` 是 JSONB。
- `scheduling_intent.instruction_payload_json` 是跨数据库、HTTP 和 MQ 通道保持一致的版本化下游契约。

## 一致性策略

### 事件去重

`lakehouse_event.event_id` 唯一。重复事件不应重复创建事实，但可以重新投影已有事件以修复“事件已落库、状态投影失败”的边界情况。

### 状态单调推进

`AssetState` 的 snapshot 不回退。对于数字 snapshot ID，必须使用数字比较；字符串字典序会导致 `"99"` 被错误认为大于 `"100"`。

### Offset 保守推进

offset 代表“已连续处理到的位置”，不是“本批次见过的最大 snapshot”。如果 snapshot 100 失败但 101 成功，offset 仍不能推进到 101。

### Source 对账与补偿

每个 lakehouse adapter 必须实现 `inspectPosition(durableOffset)`，在适配器内部解释 earliest/latest、offset 顺序和 retention gap。公共对账服务只消费统一的 `SnapshotSourcePosition`，并核对以下证据链：

```text
live source range
  -> event_consumer_offset
  -> latest durable lakehouse_event
  -> table-level asset_state
```

`UNINITIALIZED` 和 `LAGGING` 通过现有连续摄入事务补偿。事件已持久化但表级 `AssetState` 缺失或落后时，可重放该事件的投影；幂等约束和单调 snapshot 比较继续保护调度决定。`RETENTION_GAP`、`OFFSET_AHEAD`、offset 有值但事件缺失、source latest 与事件不一致时必须 `BLOCKED`，因为自动推进会跳过或制造 snapshot 事实。

对账指标记录 source lag、retention gap 和投影一致性；delivery 指标记录 publisher 尝试、耗时和各 channel/status 记录数。所有这些都是调度观察与传输证据，不参与 task 成功失败判断。

### 触发与发布幂等

FlowPlan trigger 使用稳定 `trigger_key`；task intent 使用 `task_instance_id` 唯一约束和稳定 `intent_key`，并在事务内按 batch、受限 FlowPlanVersion、workflow、task、target-admission 顺序加锁。只有配置有限 `maxActiveInstances` 时才锁版本行，以原子完成活跃 workflow 计数和首次 intent 发布；无上限的 `PARALLEL` 不被版本锁串行化。`scheduling_target_admission` 通过 `(target_asset_key, biz_date)` 唯一键和行锁保护首次发布。后续 HTTP/MQ publisher 的 lease 只允许存在于 `SchedulingIntentDelivery` 基础设施模型，不能进入 `TaskInstance` 或参与 snapshot 结果判断。

## 后续架构路线

推荐按以下顺序推进：

1. Flow 隔离：以 Flow 为授权和配额边界，owner/space 只作归属元数据，身份必须来自可信认证适配层。
2. 观测性：继续为条件阻塞和触发决策增加指标与结构化日志。
3. MQ 部署：选定实际消息产品后实现 broker gateway。
4. 查询展示：Flow/Node 聚合、稳定游标和 Web 运维视图按当前决策后移。
5. 整体 Testcontainers E2E 与 Iceberg/Hudi source adapter 按当前阶段决策暂缓。
