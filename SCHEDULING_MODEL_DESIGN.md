# Snapshot 推进式调度对象模型设计

## 目标和边界

本文是 Lakehouse Flow 当前对象模型与调度语义的权威设计基线。实现进度、版本目标和未完成项以 `PHASE2_PROGRESS.md` 为准；本文中的模型约束不是执行器设计，也不能被下游任务状态回调替代。

Lakehouse Flow 的核心目标不是做一个新的 DolphinScheduler、Airflow 或任务执行平台，而是做一个面向湖仓 snapshot 推进的调度决策系统：

```text
观察资产 snapshot -> 归并资产状态 -> 评估依赖条件 -> 发出调度意图 -> 用目标资产 snapshot 推进确认结果
```

不可越过的边界：

- 不提交 Shell、SQL、Spark、Flink、HTTP 等任务。
- 不接管 Worker、队列、资源池或执行环境。
- 不依赖任务运行状态回调判断成功或失败。
- 不把 workflow/task instance 设计成传统执行状态机。

Lakehouse Flow 应该回答的问题：

- 哪些资产版本已经到达？
- 哪些调度计划已经满足触发条件？
- 这次调度意图基于哪些 snapshot 证据？
- 调度后目标资产 snapshot 是否相对 baseline 推进？
- 重跑、补数、恢复、取消这些操作如何保持幂等、可审计、可隔离？

## 对 DolphinScheduler Action 的参考方式

参考资料来自 Apache DolphinScheduler 官方仓库和文档：

- [README](https://github.com/apache/dolphinscheduler/blob/dev/README.md)
- [Workflow Definition](https://github.com/apache/dolphinscheduler/blob/dev/docs/docs/en/guide/project/workflow-definition.md)
- [Workflow Instance](https://github.com/apache/dolphinscheduler/blob/dev/docs/docs/en/guide/project/workflow-instance.md)

DolphinScheduler 这里只作为“调度动作集合”的参照，不作为对象模型、组织模型、实例状态机或执行架构的参照。Lakehouse Flow 不需要照搬它的 Project、执行资源、权限中心、资源中心、任务插件等设计。

Lakehouse Flow 参考的 action 只有这些：

| DolphinScheduler Action | Lakehouse Flow 转译 |
| --- | --- |
| Edit | 修改 `FlowPlan` 草稿；已发布版本不可变 |
| Online / Offline | 启用或暂停 `FlowPlan` 的自然触发 |
| Timing / Timing Online / Timing Offline / Timing Management | 管理 `CalendarGate`；只作为触发窗口或兜底扫描，不作为主要事实来源 |
| Run Workflow | 手动生成 `ScheduleInstance`，只发出调度意图 |
| Rerun | 为已有实例生成新的调度操作批次 |
| Recovery Failed | 从 `SNAPSHOT_NOT_ADVANCED`、`EXPIRED` 或依赖阻塞点恢复 |
| Complement / Backfill | 按业务时间、分区或 snapshot 范围批量生成补数实例 |
| Serial Complement | 补数批次串行推进 |
| Parallel Complement | 补数批次并行推进，并受并发上限控制 |
| Dependency Mode | 补数时可选择是否级联直接下游或所有下游 |
| Pause | 暂停计划或实例的后续调度判断 |
| Stop | 取消尚未完成的调度实例，不 kill 外部任务 |
| Resume | 恢复暂停的计划或实例 |
| Delete | 归档定义或实例；审计记录保留 |
| Copy / Export / Download | 复制或导出 `FlowPlan` 定义，不复制运行态事实 |
| Batch Delete / Batch Export / Batch Copy | 批量归档、导出、复制定义 |
| Version Info / Switch Version | 查看、发布、回滚 `FlowPlanVersion` |
| Tree Diagram / Gantt / Log | 转译为拓扑图、调度证据时间线、blocked reason、snapshot 确认时间线 |

除上述调度 action 外，平台还需要两个作业生命周期操作：

| 平台操作 | Lakehouse Flow 语义 |
| --- | --- |
| `START_JOB` | 为 `WriterJobBinding` 分配新的 writer epoch 并发布独立 `JobControlIntent`；执行面实际启动对应引擎作业，首个可归因数据 snapshot 才能确认数据推进 |
| `RESTART_JOB` | 原子推进 writer epoch，发布独立 `JobControlIntent` 并要求执行面 fencing 旧 epoch；不读取引擎 JobStatus |

明确不参考的部分：

- 不参考 DolphinScheduler 的 Project / Tenant / Worker Group / Resource Center / Alert Group 等平台组织模型。
- 不参考任务类型、任务插件、执行队列、worker kill、任务日志采集等执行能力。
- 不复用它的 workflow/task instance 状态机；Lakehouse Flow 的状态只描述调度意图和 snapshot 确认。

## 推荐对象模型

### 1. 调度域与用户协作

多用户或多团队访问不应在第一版设计中过度展开。Lakehouse Flow 的 Flow 本身已经有天然隔离性：一个 `FlowPlan` 的定义、依赖、调度实例、证据集合和操作记录都应该围绕这个 Flow 收敛。

因此第一隔离对象应该是 `FlowPlan`，而不是 `Tenant` 或复杂 Workspace。`FlowSpace` 只作为轻量协作分组使用，第一版甚至可以先实现为 Flow 上的命名空间字段，等权限和 UI 需要明确后再独立成表。

#### FlowSpace

一个轻量协作域，通常对应一个团队、项目、业务域或数据产品。它只负责分组、查询和权限继承，不承担强隔离语义。

建议字段：

- `flowSpaceId`
- `spaceCode`
- `spaceName`
- `ownerUserId`
- `visibility`: `PRIVATE` / `TEAM_VISIBLE`
- `status`
- `createdAt`
- `updatedAt`

设计原则：

- `FlowPlan` 是调度语义和运行审计的第一隔离边界。
- `FlowSpace` 只是协作和查询分组，不能把模型带回重型多租户。
- 不设计跨组织共享协议；需要共享时，优先复制 Flow 或引用公共资产。
- 如果未来做 SaaS 级多组织隔离，再在 `FlowSpace` 上方增加组织边界，不在当前模型里提前复杂化。

#### User / Group / ServiceAccount

用户、人群组、系统账号。

建议原则：

- 人操作使用 `User`。
- CI、外部系统、数据平台集成使用 `ServiceAccount`。
- 权限授予给 `User` 或 `Group`，服务账号单独授权。

#### RoleBinding / AccessPolicy

权限模型建议先做 Flow 级 RBAC，并允许从 `FlowSpace` 继承默认权限。

核心权限：

- `VIEW`: 查看定义、资产状态、调度实例。
- `OPERATE`: 手动触发、重跑、补数、恢复、取消。
- `EDIT`: 修改草稿定义。
- `PUBLISH`: 发布/下线调度计划。
- `ADMIN`: 管理成员、角色、项目设置。

资源范围：

- `FLOW_PLAN`
- `ASSET`
- `SCHEDULE_INSTANCE`

### 2. 资产与 snapshot 证据

#### Asset

逻辑数据资产注册。一个物理表可以被多个 Flow 引用，但 Lakehouse Flow 不需要复杂的跨组织共享模型；每个 Flow 可以维护自己的资产别名和依赖声明。

建议字段：

- `assetId`
- `flowSpaceId`（可选协作分组）
- `assetKey`
- `physicalAssetRef`
- `format`: `PAIMON` / `ICEBERG` / `HUDI`
- `assetType`: `TABLE` / `PARTITION`
- `owner`
- `visibility`

#### PhysicalAssetRef

物理资产引用，用于表达同一个 catalog/table 被多个 Flow 引用。

建议字段：

- `physicalAssetRefId`
- `format`
- `catalogName`
- `databaseName`
- `tableName`
- `partitionExpression`
- `connectionRef`

共享规则：

- `PhysicalAssetRef` 可以作为公共引用，不承载操作权限。
- `Asset` 是 Flow 内的逻辑资产别名；如果启用 `FlowSpace`，可以从协作域继承默认命名空间。
- `AssetState` 可以按 `physicalAssetRefId` 归并事实，也可以按 `assetId` 隔离投影；第一版建议按 `assetId` 隔离，减少共享歧义。
- 如多个 Flow 依赖同一物理表，可以通过各自 `Asset` 引用同一个 `PhysicalAssetRef`，不需要单独设计复杂授权对象。

#### SnapshotObservation

原始 snapshot 观察事件，当前代码中的 `LakehouseEvent` 可演进为这个概念。

职责：

- 保存 source 观察到的事实。
- 支持去重、回放、审计。
- 不直接触发调度。

关键字段：

- `observationId`
- `sourceType`
- `assetId`
- `snapshotId`
- `schemaId`
- `watermark`
- `commitTime`
- `payloadJson`
- `observedAt`

#### AssetState

调度判断事实来源。

关键字段：

- `assetId`
- `latestSnapshotId`: 最新物理 snapshot，包含维护提交
- `latestDataSnapshotId`: 最新业务数据 snapshot
- `latestWatermark`, `latestCommitTime`: 最新物理观察事实
- `latestDataWatermark`, `latestDataCommitTime`: 最新业务数据事实
- `qualityStatus`
- `schemaStatus`
- `readinessStatus`
- `version`

原则：

- 单调推进，不回退。
- 物理观察轨供 source 连续性、baseline 和运维使用；业务数据轨供依赖判断使用。
- `COMPACT` / `ANALYZE` 不推进业务数据轨，也不创建或推进分区状态。
- snapshot ID 比较统一走 `SnapshotIds`。
- 旧事件只能成为审计证据，不能覆盖新状态。

### 3. 调度定义

#### FlowPlan

取代传统 `WorkflowDefinition` 的核心概念：一个可被 snapshot 推进触发的调度计划。

建议字段：

- `flowPlanId`
- `flowSpaceId`（可选协作分组）
- `flowCode`
- `flowName`
- `currentVersion`
- `status`: `DRAFT` / `PUBLISHED` / `SUSPENDED` / `RETIRED`
- `owner`

语义：

- 它不是执行工作流定义。
- 它是调度决策模板，声明依赖、输出资产、确认策略、并发策略和操作策略。

#### FlowPlanVersion

发布后的不可变版本。

建议字段：

- `flowPlanVersionId`
- `flowPlanId`
- `version`
- `graphJson`
- `dependencySpecJson`
- `triggerPolicyJson`
- `confirmationPolicyJson`
- `concurrencyPolicyJson`
- `publishedBy`
- `publishedAt`

原则：

- 已产生实例的版本不可修改。
- 重跑和补数必须绑定具体版本。
- 可以从旧版本复制为新草稿。

#### ScheduleGraph / ScheduleNode

表达计划内部结构。

`ScheduleNode` 建议字段：

- `nodeCode`
- `nodeName`
- `nodeType`: `ASSET_OUTPUT` / `CHECKPOINT` / `SUB_FLOW`
- `dependsOnNodes`
- `inputDependencySpec`
- `writerJobKey`
- `processingMode`: `STREAMING` / `BATCH`
- `outputAssetSpec`
- `confirmationPolicy`

注意：

- `ScheduleNode` 不配置 Shell、Spark、SQL 这类执行器。
- `ScheduleNode` 只通过 `processingMode=STREAMING|BATCH` 表达调度所需差异，不保存 `engineType=FLINK|SPARK|...`。
- `writerJobKey` 是平台作业的透明引用。实际执行引擎、程序包、入口类和运行参数由平台执行面解析，不进入 FlowPlan。

#### WriterJobBinding

`WriterJobBinding` 表示平台中唯一允许写入某张物理湖表的稳定作业身份，而不是某个执行引擎的一次运行实例。它是跨 Flow 共享的平台级对象，FlowPlan/Node 只能引用 binding，不能在自己的隔离范围内重新声明一个同表 writer。

建议字段：

- `writerJobKey`: 平台内稳定且全局唯一的作业键
- `tableAssetKey`: 去除 `${bizDate}` 和具体分区后的规范化物理表键
- `allowedProcessingModes`: 同一套作业代码允许的 `STREAMING` / `BATCH` 运行配置
- `deliveryRoute`: 平台执行面的被动接收路由
- `currentWriterEpoch`: 每次启动、重启或安全模式切换时单调递增
- `activeProcessingMode`: 当前 epoch 唯一允许的写入模式
- `holderIntentKey`, `leaseExpiresAt`: 防止同一 writer 的不同运行实例并发占用物理表
- `createdAt`, `updatedAt`

发布不变量：

1. 一个 FlowPlan DAG 可以同时存在流式和批式节点。
2. 一个 `tableAssetKey` 只能绑定一个 `writerJobKey`；跨 FlowPlan 和跨版本校验都不能只看节点 ID 或分区日期。
3. 同一个 writer 可以复用流批一体代码，但同一 epoch 只能激活一种写入模式，不能让流式实例和批式实例同时写表。
4. 已发布版本升级可以继续引用原 `writerJobKey`；把目标表换绑到另一个 writer 必须经过显式迁移，不允许普通发布悄悄覆盖。
5. Lakehouse Flow 保存 binding、epoch 和控制意图，不保存 `RUNNING/FAILED/CANCELLED` 等引擎状态。

绑定 task 的批式 `SchedulingIntent` 本身授权执行面启动一次有界作业，并占用一个 writer epoch。连续 writer 的补数可以由现有流作业消费回放意图；如果必须切到批式 profile，则平台必须先 fencing 流式 epoch，再用同一个 `writerJobKey` 启动批式 epoch，完成后再分配新 epoch 恢复流式运行。任何情况下都不能出现第二个 writer 作业并发写同表。

#### JobControlIntent

`JobControlIntent` 表示从平台发起的一次常驻作业启动或重启操作。它与数据处理 `SchedulingIntent` 是两个领域对象：前者绑定 `WriterJobBinding`，后者绑定 `TaskInstance`。二者共享可靠投递语义，但不能为了复用现有表而让作业控制伪造 workflow、task 或业务日期。

建议字段：

- `controlIntentKey`: 稳定幂等键，建议由 `writerJobKey + writerEpoch` 生成
- `writerJobKey`, `tableAssetKey`
- `operationType`: `START_JOB` / `RESTART_JOB`
- `writerEpoch`, `previousWriterEpoch`
- `processingMode`: 常驻作业当前只允许 `STREAMING`；有界批运行由数据处理 intent 授权
- `baselineSnapshotId`, `confirmationTimeout`: 用于观察新 epoch 是否产生目标业务数据
- `deliverBefore`, `requestedBy`, `reason`, `createdAt`
- `instructionPayloadJson`: 独立版本化控制契约，不包含 task/workflow/bizDate/input vector

`JobControlIntentDelivery` 复用现有 claim lease、fencing token、指数退避、最大尝试和死信算法，但保留到控制意图的明确外键。当前 `SchedulingIntent` 已由 `task_instance_id NOT NULL` 和 task 唯一约束定义为数据处理意图，G20 不应把它改造成带大量可空字段的多态对象。

控制意图的 delivery ACK 只说明操作已送达。首个匹配 `jobControlIntentKey + writerJobKey + writerEpoch` 的目标业务 snapshot 可以确认该 writer 世代产生了数据；没有业务数据变化时不得制造 snapshot，也不能把 `SNAPSHOT_NOT_ADVANCED` 解释为执行引擎作业启动失败。

平台最小操作入口应包括 `POST /api/v1/writer-jobs/{writerJobKey}/start`、`POST /api/v1/writer-jobs/{writerJobKey}/restart` 和控制意图审计查询。start/restart 请求只创建并可靠投递 `JobControlIntent`；API 返回控制意图与投递证据，不同步等待执行引擎，也不返回伪造的 RUNNING/SUCCESS 状态。

#### DependencySpec

声明调度等待条件。

结构建议：

```json
{
  "groups": [
    {
      "groupName": "default",
      "operator": "AND",
      "conditions": [
        {
          "assetKey": "paimon.prod.orders.dt=${bizDate}",
          "type": "SNAPSHOT_ADVANCED",
          "required": true
        },
        {
          "assetKey": "paimon.prod.payments.dt=${bizDate}",
          "type": "WATERMARK_GTE",
          "value": "${bizDate}T23:59:59"
        }
      ]
    }
  ],
  "groupOperator": "OR"
}
```

条件类型：

- `SNAPSHOT_EXISTS`
- `SNAPSHOT_ID_GTE`
- `SNAPSHOT_ADVANCED`
- `WATERMARK_GTE`
- `QUALITY_PASSED`
- `SCHEMA_COMPATIBLE`

#### ProcessingMode

定义节点是流式还是批式。Flow 不关心实际使用 Flink、Spark 或其他计算引擎，也不持有任何执行状态。

建议字段：

- `processingMode`: `STREAMING` / `BATCH`，这是 Flow 层唯一的计算方式分类

`STREAMING` 节点的作业由平台启动后自行持续消费上游新增 snapshot/changelog，不为每次普通输入推进创建 task intent；其输出业务 snapshot 作为下游依赖事实。`BATCH` 节点在全部依赖满足后生成一次数据处理 `SchedulingIntent`，平台执行面据此运行有界范围。流式和批式的提交钩子差异由执行适配器处理，不在 FlowPlan 中保存 `activationMode`、`completionBoundary` 或引擎枚举。

`inputSnapshotVector` 是运行时 SchedulingIntent 的冻结证据，不是节点定义字段。对 `STREAMING` 节点执行补数或重跑时，Action 仍派生一次有明确输入向量的数据处理回放意图，而不是把常驻作业状态带进调度模型。回放可由当前流式 writer 消费，或按单写入者规则受控切换到批式运行，并只用新的可归因目标 snapshot 推进原有 BackfillBatch/DAG。

常驻作业的首次启动和重启由平台操作触发：Lakehouse Flow 为对应 `writerJobKey` 分配 writer epoch 并投递独立 `JobControlIntent(START_JOB|RESTART_JOB)`，平台执行面根据 writer 路由完成真实引擎操作。该控制对象不进入 Flow DAG，也不创建 task instance；旧 epoch 产生的迟到 snapshot 可以保留为物理事实，但不能确认新 epoch 的控制意图或数据处理 intent。

#### MixedDependencyEvidence

混合流批 DAG 的边统一依赖“父节点输出资产证据”，但证据来源不同：

- 父节点为 `STREAMING`：记录触发本次判断的父输出 `assetKey + snapshotId/watermark`，不要求存在逐 snapshot `TaskInstance`。
- 父节点为 `BATCH`：记录同一 ScheduleInstance 内父 task 已确认的 `taskInstanceId + targetAssetKey + observedSnapshotId`。
- 节点自身 `inputDependencySpec`：继续记录额外资产条件对应的 snapshot/watermark 证据。

建议统一冻结为 `InputSnapshotEvidence`：`parentNodeCode`、`parentProcessingMode`、`assetKey`、`snapshotId`、`watermark`、可选 `upstreamTaskInstanceId` 和 `observedAt`。下游批式节点只有在全部直接父边和自身额外依赖都满足后才创建 SchedulingIntent；下游流式节点自行持续消费这些输入，Lakehouse Flow 不为每次父 snapshot 创建 task intent。

当前 `DagProgressionService.dependenciesConfirmed` 只接受“同一 workflow 的父 task 已确认”，这是纯批式/意图驱动 DAG 的已有实现，不是混合流批 DAG 的最终语义。G19 需要用上述证据向量替代纯 task 状态门禁，同时保留批式父节点的同实例隔离。

#### OutputAssetSpec

定义调度后应该观察哪个目标资产的 snapshot 变化。

建议字段：

- `targetAssetKey`
- `baselinePolicy`: `READ_BEFORE_SCHEDULE`
- `confirmationType`: `INTENT_CORRELATED_SNAPSHOT_ADVANCED`
- `requiredSnapshotProperties`: intent/source/target/bizDate/final 属性约定，其中 final 表示逻辑 intent 完成而不是引擎作业结束
- `requiredSnapshotChangeType`: 当前固定为格式无关的 `DATA`；原生 operation 由 source adapter 映射
- `expectedPartition`: 分区目标的实际变更范围
- `timeout`: ISO-8601 Duration；节点策略覆盖版本策略，旧字段名 `confirmationTimeout` 只作兼容读取
- `staleAction`: 当前仅支持 `MARK_NOT_ADVANCED`

这是 Lakehouse Flow 和传统执行调度器最不同的地方：结果不来自任务状态，而来自可归属于当前调度意图的 output asset 数据 snapshot 推进。任何物理提交都可以推进观察轨，但维护 snapshot 不能推进业务数据轨，更不能替代当前 intent 的结果证据。

#### TriggerPolicy

触发策略。

类型：

- `SNAPSHOT_DRIVEN`: 资产状态变化触发。
- `CALENDAR_GATED`: 在日历窗口内才允许 snapshot 触发。
- `MANUAL`: 用户手动触发。
- `BACKFILL`: 补数批次触发。
- `RERUN`: 重跑触发。

建议原则：

- cron 只能是 gate 或兜底扫描，不应成为主要事实来源。
- 同一 snapshot 组合只应产生一次默认调度实例。
- 人工操作必须生成新的 `operationId`，不能伪装成自然触发。

#### ConcurrencyPolicy

调度实例并发策略只描述同一 `FlowPlan` 如何产生和排队 `ScheduleInstance`，不表达外部任务执行方式。

当前运行时取值：

- `PARALLEL`: 同一版本可并行发布实例；可用 `maxActiveInstances` 设置正整数上限。
- `SERIAL_WAIT`: 默认上限为 1；有活跃实例时，新实例保持 `READY_TO_SCHEDULE`，不发布 intent、不冻结 baseline。

当前字段：

- `maxActiveInstances`
- `targetAdmissionLease`: ISO-8601 Duration，不得短于当前节点有效确认窗口；缺省时等于确认窗口

`SERIAL_DISCARD`、`SERIAL_PRIORITY`、`priority` 和 `dedupeWindow` 需要先定义丢弃/排队的审计模型，当前发布校验会拒绝未实现的 mode，避免配置被静默忽略。版本活跃数只统计调度状态为 `SCHEDULED` 的 workflow；同一 workflow 的多个 root 和后续 DAG 节点不重复占用名额，最终名额由确定的 snapshot 终态释放。source 阻塞不能伪造超时失败来提前释放名额。

### 4. 调度运行时与审计

#### ScheduleInstance

一次调度意图。它是当前 `WorkflowInstance` 的目标语义。

建议字段：

- `scheduleInstanceId`
- `flowPlanId`
- `flowPlanVersion`
- `bizTime`
- `triggerType`
- `operationId`
- `triggerKey`
- `inputEvidenceSetId`
- `state`
- `createdBy`
- `createdAt`

推荐状态：

```text
CREATED
WAITING_SNAPSHOT
READY_TO_SCHEDULE
SCHEDULED
CONFIRMING_SNAPSHOT
SNAPSHOT_CONFIRMED
SNAPSHOT_NOT_ADVANCED
CANCELLED
SKIPPED
EXPIRED
```

其中：

- `SCHEDULED` 表示调度意图已经发出或记录成功。
- `CONFIRMING_SNAPSHOT` 表示等待目标资产 snapshot 推进。
- `SNAPSHOT_CONFIRMED` 表示找到了相对 baseline 推进且与 intent 精确匹配的目标资产 snapshot。
- `SNAPSHOT_NOT_ADVANCED` 表示确认窗口已结束、source 证据链完整且没有观察到可归属的推进；期间可能存在其他来源的 snapshot。source 不可用、未追平或存在 retention/projection 缺口时不得进入该状态。

`LF-1.0` 不将传输和 source 健康塞入节点状态机，而是同时暴露三个正交维度：

```text
snapshotResult = WAITING | SNAPSHOT_CONFIRMED | SNAPSHOT_NOT_ADVANCED
deliveryStatus = PENDING | PUBLISHING | RETRY_WAIT | PUBLISHED | DELIVERY_EXHAUSTED
sourceHealth   = HEALTHY | REPAIRABLE | SOURCE_BLOCKED
```

`DELIVERY_EXHAUSTED` 和 `SOURCE_BLOCKED` 都是运维与决策证据，不是下游执行失败。在 `SOURCE_BLOCKED` 期间 snapshot 结果保持待定，恢复且证据连续后才重新判定确认或未推进。

#### NodeScheduleInstance

节点级调度记录。它是当前 `TaskInstance` 的目标语义。

建议字段：

- `nodeScheduleInstanceId`
- `scheduleInstanceId`
- `nodeCode`
- `state`
- `waitingReason`
- `targetAssetId`
- `baselineSnapshotId`
- `observedSnapshotId`
- `scheduledAt`
- `lastSnapshotCheckAt`

#### EvidenceSet

一次调度判断使用的证据集合。

建议字段：

- `evidenceSetId`
- `flowPlanId`
- `flowPlanVersion`
- `bizTime`
- `assetSnapshotsJson`
- `conditionResultsJson`
- `createdAt`

价值：

- 解释为什么触发。
- 支持重跑时选择“重用原证据”或“重新采样证据”。
- 支持审计和排障。

#### SnapshotConfirmation

目标资产 snapshot 推进确认记录。

建议字段：

- `confirmationId`
- `scheduleInstanceId`
- `nodeScheduleInstanceId`
- `targetAssetId`
- `baselineSnapshotId`
- `observedSnapshotId`
- `intentKey`
- `matchedEventId`
- `matchPolicyVersion`
- `result`: `CONFIRMED` / `NOT_ADVANCED` / `WAITING`
- `checkedAt`

#### DecisionRecord

决策审计。当前 `TriggerHistory` 可演进为这个概念。

建议字段：

- `decisionId`
- `flowPlanId`
- `triggerKey`
- `decisionType`: `TRIGGERED` / `SKIPPED` / `DEDUPED` / `BLOCKED`
- `reason`
- `evaluationPayloadJson`
- `relatedScheduleInstanceId`
- `createdAt`

#### OperationRequest

人工或系统操作请求。

类型：

- `MANUAL_TRIGGER`
- `RERUN`
- `BACKFILL`
- `RECOVER`
- `CANCEL`
- `PAUSE_PLAN`
- `RESUME_PLAN`

建议字段：

- `operationId`
- `targetFlowPlanId`
- `operatorUserId`
- `operationType`
- `scope`
- `parametersJson`
- `status`
- `createdAt`

原则：

- 所有人工操作都必须有 `operationId`。
- 操作实例的幂等键必须包含 `operationId`，否则重跑/补数会被自然触发的去重逻辑误伤。

## 隔离与共享

### 隔离原则

第一版只保留 Flow 级隔离，不做平台级组织隔离：

```text
FlowPlan
  -> ScheduleInstance
    -> NodeScheduleInstance / DecisionRecord / SnapshotConfirmation
```

核心规则：

- `FlowPlan` 隔离调度语义、版本、并发、实例、操作和审计。
- `FlowSpace` 只是可选分组，用于列表、默认权限和配额继承。
- `ScheduleInstance` 隔离一次具体调度意图。
- `Asset` 在 Flow 内命名，避免不同业务流程对同一物理表有不同业务含义时互相污染。
- `PhysicalAssetRef` 可以复用，但不引入复杂共享授权。

### 共享方式

推荐只保留三种简单共享方式：

1. 公共物理资产引用：多个 Flow 可以引用同一个 `PhysicalAssetRef`。
2. Flow 复制：从其他 Flow 复制为本空间草稿，再独立发布和运维。
3. 只读查看：管理员或拥有 `VIEW` 权限的用户查看其他 Flow 的定义和实例。

暂不设计：

- 跨组织或跨空间复杂授权对象。
- 跨空间共同操作同一个生产 Flow。
- 细粒度原始事件 payload 共享。
- 资产状态写权限共享。

### 权限矩阵

| 操作 | VIEW | OPERATE | EDIT | PUBLISH | ADMIN |
| --- | --- | --- | --- | --- | --- |
| 查看资产状态 | Y | Y | Y | Y | Y |
| 查看调度实例 | Y | Y | Y | Y | Y |
| 查看证据链 | Y | Y | Y | Y | Y |
| 手动触发 | N | Y | Y | Y | Y |
| 重跑 | N | Y | Y | Y | Y |
| 补数 | N | Y | Y | Y | Y |
| 恢复/取消实例 | N | Y | Y | Y | Y |
| 修改草稿 | N | N | Y | Y | Y |
| 发布/下线计划 | N | N | N | Y | Y |
| 成员管理 | N | N | N | N | Y |

### 配额与公平性

虽然 Lakehouse Flow 不执行任务，但仍需要调度侧配额：

- 每 `FlowPlan` 最大活跃 `ScheduleInstance` 数。
- 每 `FlowPlan` 最大补数并发。
- 每 FlowSpace 最大总活跃实例数，可选。
- 每 plan 最大排队实例数。
- 每分钟最大触发决策数。
- snapshot 确认扫描频率上限。

这些配额防止某个 Flow 的补数、重跑淹没调度决策层；FlowSpace 级配额只是团队公平性的补充，不是多租户强隔离。

## 能力设计

### 发布、上线和下线

推荐流程：

```text
DRAFT -> REVIEWED -> PUBLISHED -> SUSPENDED -> RETIRED
```

规则：

- 只有 `PUBLISHED` 版本会产生自然调度实例。
- `SUSPENDED` 后不再产生新实例，但已有实例继续保留审计。
- `RETIRED` 后只能查询历史，不能操作。

### 手动触发

手动触发用于立即生成一个 `ScheduleInstance`。

参数：

- `flowPlanVersion`
- `bizTime`
- `triggerReason`
- `evidenceMode`: `REUSE_CURRENT_STATE` / `RE_SAMPLE`

结果：

- 生成 `OperationRequest(MANUAL_TRIGGER)`。
- 生成新的 `triggerKey`。
- 按依赖条件决定进入 `READY_TO_SCHEDULE` 或 `WAITING_SNAPSHOT`。

### 重跑

重跑不是重新执行任务，而是重新发出调度意图，并等待目标 snapshot 再次推进。

重跑范围：

- 整个 `ScheduleInstance`。
- 单个 `NodeScheduleInstance`。
- 从某个未确认节点开始的子图。

重跑模式：

- `REUSE_EVIDENCE`: 使用原输入 snapshot 证据，适合修复外部执行系统问题后重新推动产出。
- `RE_SAMPLE_EVIDENCE`: 重新读取当前 `AssetState`，适合源数据已经继续推进的情况。

幂等规则：

```text
rerun_trigger_key =
flow_plan_id + version + biz_time + original_instance_id + operation_id + rerun_scope
```

这样自然触发仍然去重，人工重跑可以合法产生新实例。

当前代码进展：

- 已新增 `SchedulingAction` 审计模型，使用 `actionKey` 做操作幂等。
- 已新增 `SchedulingActionService.rerunWorkflowInstance`，当前实现支持 workflow instance 级重跑。
- 当前重跑只生成新的 workflow 调度侧实例并推进到 `SCHEDULED`，不执行外部任务。
- 已支持 task instance 级重跑和 published `FlowPlanVersion` + `ScheduleNode` 级重跑，都会创建新的 action-owned 调度意图并保留原证据。

### 失败恢复

Lakehouse Flow 没有任务失败回调，因此“失败恢复”应定义为：

- 依赖条件长期不满足后的恢复。
- snapshot 确认超时后的恢复。
- 某个节点 `SNAPSHOT_NOT_ADVANCED` 后从该节点重新产生调度意图。

恢复入口：

- `RECOVER_FROM_NOT_ADVANCED`
- `RECOVER_FROM_EXPIRED`
- `RECOVER_FROM_BLOCKED_DEPENDENCY`

恢复结果：

- 新增 `OperationRequest(RECOVER)`。
- 创建新的 `ScheduleInstance` 或 `NodeScheduleInstance`。
- baseline snapshot 重新采样或沿用原 baseline，由操作参数决定；当前 backfill 恢复固定在新 intent `deliver` 时重新采样。

当前 backfill 失败恢复采用替代批次模型：

1. `RECOVER_BACKFILL` 只接受 `FAILED` 且存在 `SNAPSHOT_NOT_ADVANCED` item 的批次。
2. 原批次、item 和 task 作为历史证据保持不变；恢复创建新的 `BackfillBatch`，记录 `sourceBackfillBatchId` 和递增的 `recoveryAttempt`。
3. 新批次从最早失败业务日期推进到原结束日期。已在更早日期完成的内容不会重复生成意图。
4. 每个失败批次最多创建一个直接替代批次；再次失败时应以新的失败批次为来源继续形成线性恢复链，避免同一来源产生重复调度意图。
5. 恢复会从原补数起点重新展开完整选中子图。这样起点仍是唯一 DAG bypass，级联节点只能依赖新 workflow 实例内的父节点 snapshot 确认。
6. 恢复不复用旧实例的 `SNAPSHOT_CONFIRMED` 作为新实例 DAG 证据，也不读取下游执行状态。

### 补数 / Backfill

补数是 Lakehouse Flow 必须支持的一等能力。

补数维度：

- 按业务时间范围：`bizTimeStart` 到 `bizTimeEnd`。
- 按 snapshot 范围：`snapshotStart` 到 `snapshotEnd`。
- 按分区范围：如 `dt=2026-09-01..2026-09-10`。

补数模式：

- `SERIAL`: 一次只允许一个业务日期暴露可交付意图，整日子图确认后再释放下一日期。
- `PARALLEL`: 所有业务日期可并行产生调度意图，保持原有兼容行为。
- `PARALLEL_WITH_LIMIT`: 最多同时允许 `maxActiveDates` 个业务日期暴露可交付意图。

这里的并发是调度侧“日期准入窗口”，不是下游执行线程数或资源配额。日期被准入后，内部节点仍严格遵守 DAG snapshot 门禁。

完整 Flow 与 Node 子图补数已经统一进入 `BackfillBatch`。`BACKFILL_WORKFLOW` 和 `BACKFILL_NODE` 只是两个请求入口，不再代表两套内部生命周期。

统一批次范围：

- `scopeType=FULL_FLOW`：`selectedNodeCodes` 是发布版本的完整 DAG，`entryNodeCodes` 是全部根节点；不存在伪造的单一 `startNode` 或 `cascadePolicy`。
- `scopeType=NODE_SUBGRAPH`：`selectedNodeCodes` 是经过依赖闭包校验的子图，`entryNodeCodes` 只包含用户选择的 `startNode`。
- `entryNodeCodes` 和 `selectedNodeCodes` 在批次创建时冻结。失败恢复继承这两个集合，不能根据后续请求重新解释源批次范围。
- 多个完整 Flow 根节点共同构成一个业务日期的入口组。日期准入时一起释放，但只占用一个日期槽位。

级联策略：

- `NO_CASCADE`: 只补选中的起始节点。
- `DIRECT_DOWNSTREAM`: 补起始节点和直接下游节点。
- `TRANSITIVE_DOWNSTREAM`: 补起始节点和所有可达下游节点，生产化阶段必须加审批和并发上限。

DAG 依赖原则：

1. 每个业务日期形成独立的调度实例，不能跨日期复用父节点确认结果。
2. 用户选中的 `startNode` 是唯一允许跳过其 DAG 上游的节点，这是 node backfill 的显式边界。
3. 其余被级联选中的节点先进入 `WAITING_SNAPSHOT`；只有同一实例内全部直接父节点都达到 `SNAPSHOT_CONFIRMED`，并且自身外部资产条件满足，才进入 `READY_TO_SCHEDULE`。
4. 父节点存在但 snapshot 未推进、父节点 task 缺失、或父节点只处于 `SCHEDULED`，均不能释放下游。
5. 若级联到汇聚节点但选择范围遗漏另一路直接父节点，当前实现直接拒绝该补数请求。不能把不完整子图变成永久等待，更不能把父节点已有历史数据等同于本次 snapshot 已推进。

安全跳过策略：

1. `skipPolicy=NONE` 是默认值，表示即使历史日期已经成功，也按请求重新生成调度意图。
2. `skipPolicy=SKIP_FULLY_CONFIRMED_DATES` 只允许跳过完整业务日期，不支持在新 workflow 内按单节点跳过。
3. 一个日期只有在全部选中节点都存在同一 immutable `FlowPlanVersion`、同一业务日期、同一 `ScheduleNode`、同一日期解析目标资产的 `SNAPSHOT_CONFIRMED` task 证据，且 observed snapshot 非空时才能跳过。
4. 任一节点证据缺失、目标资产不匹配或只存在当前 `AssetState` 时，该日期都必须完整展开；`AssetState` 的存在不代表这次历史调度已经成功。
5. 历史确认仅证明整日可省略，绝不作为新 workflow 的父节点确认。只要创建了新实例，级联下游仍必须等待该实例内父节点 snapshot 推进。
6. 被跳过的日期不创建 workflow、task 或 backfill item，也不占用 `SERIAL` / `PARALLEL_WITH_LIMIT` 的日期槽位；整个区间都被跳过时批次直接完成。
7. `BackfillBatch.skipEvidenceJson` 保存逐日逐节点的 task、目标资产、baseline 和 observed snapshot 证据，保证跳过决策可审计。

补数实例生成：

```text
BackfillRequest
  -> BackfillBatch
    -> ScheduleInstance(bizTime=day1): start READY, downstream WAITING
    -> ScheduleInstance(bizTime=day2): start WAITING_CONCURRENCY
    -> ScheduleInstance(bizTime=day3): start WAITING_CONCURRENCY

start intent delivered
  -> capture start target snapshot baseline
  -> observe target snapshot advancement
  -> mark start SNAPSHOT_CONFIRMED
  -> re-evaluate direct downstream DAG + external asset gates
  -> release eligible downstream intent
  -> all selected nodes for day1 SNAPSHOT_CONFIRMED
  -> release day2 start intent into the available date slot
```

补数幂等键：

```text
backfill_trigger_key =
flow_plan_id + version + backfill_batch_id + biz_time + node_scope
```

当前代码进展：

- `SchedulingActionService.backfillWorkflow` 已改为通过 `FULL_FLOW` 统一批次生成完整 DAG 调度意图，并支持与 Node 补数相同的日期推进和批次控制。
- 已新增 `SchedulingActionService.backfillScheduleNode`，支持按 published `FlowPlanVersion`、起始 `ScheduleNode`、业务日期闭区间和 `NO_CASCADE` / `DIRECT_DOWNSTREAM` / `TRANSITIVE_DOWNSTREAM` 生成 task scheduling intents。
- 当前 node 补数会为每个业务日期创建 action-owned workflow wrapper；仅选中起点为 READY，范围内其他节点先等待 DAG snapshot 依赖。
- 当前 node 补数已持久化 `BackfillBatch` 和 `BackfillItem`，可按 actionKey 查询批次，并按 batchId 查询 node/date 明细。
- 已新增 `DagProgressionService`：当前针对拥有 task instance 的父节点，在目标 snapshot 确认后重算下游，并对缺失或未推进父 task 保持阻塞；流式父节点的资产证据门禁尚待 G19 补齐。
- 已新增 `SnapshotTriggerRoutingService`：补数、恢复和重跑产生的 snapshot 只确认所属 intent 并推进所属 workflow，不再进入全局自然触发通道。
- 一个物理表 snapshot 会同时投影表级状态和 `changedPartitions` 对应的分区级状态；历史分区补数不能推进当前日期的分区资产键。
- Lakehouse Flow 标记但不存在 intent、属性不完整、中间提交和维护提交均失败关闭，不产生正常调度实例。
- 已在子图展开前校验依赖闭包；选中起点可绕过自己的父节点，但下游汇聚节点不能遗漏其他父节点。
- 节点输出资产支持 `${bizDate}` / `${biz_date}`，按每个补数日期独立解析。
- 已支持补数批次暂停、恢复和取消：暂停会阻断内部 outbox publication，恢复后待发布 intent 重新进入候选，取消只终止未发布 intent。
- intent 成功写入 outbox 后，`BackfillItem` 会记录为 `INTENT_DELIVERED`；这是历史命名，准确含义是调度承诺已经持久化。HTTP/MQ 是否 ACK 必须查看独立 delivery 状态。已承诺 intent 不受批次暂停影响，也不会被批次取消伪装成已撤回。
- 批次控制和 intent publication 对同一批次加行锁，避免暂停/取消与发布发生竞态穿透。
- 已新增 `BackfillProgressionService`，支持 `PARALLEL` / `SERIAL` / `PARALLEL_WITH_LIMIT`；有限模式按业务日期顺序补位，并以整日选中子图 snapshot 全部确认为释放条件。
- 日期起点等待配额时记录为 `WAITING_CONCURRENCY`；外部资产事件不能绕过该状态，暂停期间不补位，恢复时重新计算槽位。
- 任一节点进入 `SNAPSHOT_NOT_ADVANCED` 后批次转为 `FAILED`，不会继续释放后续日期。
- 已支持 `RECOVER_BACKFILL`：从最早 snapshot 未推进日期创建单链替代批次，保留原证据，并按原级联和日期准入策略重新生成意图。
- 已支持显式安全整日跳过：默认强制补数；只有完整、同版本、同日期、同节点和同目标资产的 snapshot 确认证据才能省略日期，证据持久化到批次并通过查询 API 暴露。
- 完整 Flow 与 Node 子图恢复都继承源批次冻结的 scope、入口节点和选中节点，不复用旧实例 DAG 确认。
- 当前补数还未支持审批上限，该能力按当前阶段决策暂缓。
- 已新增跨正常、补数、恢复和重跑的 `targetAssetKey + bizDate` 持久化准入槽；未获槽位的任务保持 `READY_TO_SCHEDULE`，由后续内部扫描重试。
- 准入槽在 baseline 捕获前获取，在匹配 snapshot 确认或发布租约过期后释放；过期重占只代表允许再次发布，不保证旧下游进程已经停止，也不等于旧 task 已得出 `SNAPSHOT_NOT_ADVANCED`。source 阻塞时可允许调度槽位过期，但旧 task 的 snapshot 结果仍待定。
- 准入槽释放校验当前 holder，旧 intent 的晚到 snapshot 不能释放已经换给新 intent 的槽位。外部系统自行启动且未经过 Lakehouse Flow 的任务不受该调度互斥控制，但其未归因 snapshot 不能确认 Lakehouse Flow intent。
- `FlowPlanVersion.confirmationPolicyJson` 与 `concurrencyPolicyJson` 已进入 intent 发布、确认窗口、目标准入租约和版本级活跃实例准入决策。

### 暂停、取消和恢复

Lakehouse Flow 的暂停/取消只作用于调度层；作业启动和重启是独立的平台生命周期操作。

| 操作 | 语义 |
| --- | --- |
| `PAUSE_PLAN` | 暂停自然触发，不影响历史实例 |
| `PAUSE_INSTANCE` | 暂停实例后续节点调度和 snapshot 确认扫描 |
| `CANCEL_INSTANCE` | 取消尚未确认的调度实例 |
| `RESUME_INSTANCE` | 恢复暂停实例的调度判断 |
| `PAUSE_BACKFILL` | 暂停补数批次中尚未交付的 intent，不影响已交付 intent |
| `RESUME_BACKFILL` | 重新开放暂停批次中尚未交付的 intent |
| `CANCEL_BACKFILL` | 取消尚未交付的 intent，保留已交付 intent 及其 snapshot 确认链路 |
| `RECOVER_BACKFILL` | 保留失败批次，从最早未推进日期创建新的替代批次和调度意图 |

边界：

- Lakehouse Flow 进程不直接 kill worker，也不直接调用 Spark/Flink/Yarn/K8s 控制接口。
- 平台 `START_JOB` / `RESTART_JOB` 只产生不绑定 task 的可靠 `JobControlIntent`，真实操作由执行面完成。
- 普通 `PAUSE/CANCEL` 不隐式停止外部作业；任何停止能力都必须是独立、显式且可审计的生命周期操作。
- 不清理外部执行日志。

当前代码进展：

- 已新增 workflow/task cancel action，取消仅改变调度侧实例状态。
- 已新增 task skip action，适用于未确认成功的调度节点。
- 已新增 task snapshot recheck action，用于重新读取目标资产 snapshot 推进情况。
- 已新增 backfill pause/resume/cancel action；控制范围止于 Lakehouse Flow 的意图交付边界。

### 调度时间窗口

时间不再是主要触发源，但仍有价值：

- 控制某些计划只在指定时间窗口内允许触发。
- 做兜底扫描。
- 做补数范围展开。
- 做 SLA 和超时告警。

因此推荐把 cron 设计为 `CalendarGate`，而不是唯一触发器。

### 可观测性

传统 Gantt 图在执行系统里展示任务运行时间。Lakehouse Flow 应展示：

- 依赖条件时间线。
- 输入 snapshot 证据集。
- 调度意图生成时间。
- baseline snapshot。
- 目标 snapshot 观察时间线。
- 最终确认结果。

建议视图：

- Asset State Timeline
- Schedule Instance Timeline
- Evidence Diff
- Backfill Batch Progress
- Blocked Reason List

## 与当前代码的映射建议

| 当前名称 | 目标对象 | 调整建议 |
| --- | --- | --- |
| `LakehouseEvent` | `SnapshotObservation` | 可以先保留类名，文档中明确事件是证据 |
| `AssetState` | `AssetState` | 保留 |
| `FlowPlanVersion.dependencySpecJson` / `ScheduleNode.inputDependencySpecJson` | `DependencySpec` | 已统一为发布版本内的可组合 DSL，不再保留单表依赖运行时路径 |
| `WorkflowDefinition` | `FlowPlan` | 建议重命名或至少重新定义语义 |
| `TaskDefinition` | `ScheduleNode` | 节点定义，不包含执行器 |
| `WorkflowInstance` | `ScheduleInstance` | 建议重命名，避免执行语义 |
| `TaskInstance` | `NodeScheduleInstance` | 建议重命名，避免任务执行语义 |
| `TriggerHistory` | `DecisionRecord` | 可保留历史表名，但语义应是决策审计 |
| `BackfillSpec` | `BackfillRequest` / `BackfillBatch` | 需要拆分请求和批次实例 |
| `EventConsumerOffset` | `SourceCursor` | 语义更通用 |

短期如果不立刻重命名代码，至少要在 API 和文档上采用目标语义：

- 对外称 `FlowPlan`，内部临时映射到 `WorkflowDefinition`。
- 对外称 `ScheduleInstance`，内部临时映射到 `WorkflowInstance`。
- 对外称 `NodeScheduleInstance`，内部临时映射到 `TaskInstance`。

## 需要继续讨论的问题

1. 对外产品命名采用 `FlowPlan` 还是 `SchedulePlan`？
2. `FlowSpace` 第一版是否只作为 Flow 的命名空间字段，而不独立成表？
3. `AssetState` 第一版按 `assetId` 隔离，还是按 `physicalAssetRefId` 共享事实再做逻辑投影？
4. 重跑默认使用原证据还是重新采样当前 `AssetState`？
5. `TRANSITIVE_DOWNSTREAM` 在生产环境是否强制审批，以及按多少节点/日期触发阈值？

已确认：`LF-1.0` 的数据处理 intent 和 `JobControlIntent` 都必须支持 Database Table 与 HTTP 两种正式交付方式；单条意图仍只选择一个实际路由。

## 建议的下一步文档工作

1. 确认命名：`FlowPlan / ScheduleInstance / NodeScheduleInstance` 是否接受。
2. 补一版 ERD 草图和核心表清单。
3. 为重跑、补数、恢复各写一个时序流程。
4. 再进入代码改造，避免当前 `Workflow/Task` 命名继续扩散执行语义。
