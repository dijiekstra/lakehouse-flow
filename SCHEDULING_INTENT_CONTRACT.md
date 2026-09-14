# Outbound Intent 下游契约

**当前 SchedulingIntent 契约版本**: 1.3；字段与兼容策略已完成 LF-1.0 冻结
**LF-1.0 目标 SchedulingIntent 契约版本**: 1.3
**LF-1.0 目标 JobControlIntent 契约版本**: 1.0；字段与兼容策略已完成 LF-1.0 冻结
**适用通道**: Database Outbox、HTTP、MQ
**LF-1.0 状态**: 两类契约的 JSON Schema、REST surface、未知字段和 migration 兼容回归已完成；DATABASE_TABLE、HTTP、action、流批边界及 V22 到 V23 升级已通过整体 E2E

## 1. 契约目标

Lakehouse Flow 只负责产生和投递出站意图，不执行任务，也不读取下游任务状态。1.0 明确区分两种领域对象：

- `SchedulingIntent` / `DATA_PROCESSING`：绑定一个 `TaskInstance`，表达“处理这次冻结的数据范围”，并等待目标业务 snapshot 确认。
- `JobControlIntent` / `JOB_CONTROL`：绑定一个 `WriterJobBinding`，表达平台对常驻作业的显式启动或重启操作，不绑定 `WorkflowInstance`、`TaskInstance` 或 `bizDate`。

两者可以复用数据库、HTTP 和 MQ publisher SPI 以及相同的至少一次投递状态机，但必须保持独立的领域记录和审计接口。当前 `SchedulingIntent` 受 `task_instance_id NOT NULL` 和 task 唯一约束保护，G20 不得通过伪造 task 或把这些字段改为可空来承载作业生命周期。

下游接收调度意图后负责实际运行，并在已经完整覆盖本次冻结输入边界的目标资产 snapshot 中写入意图指定的属性。Lakehouse Flow 通过目标 snapshot 的属性、格式适配器给出的数据变更分类和变更分区完成归因与结果确认，不接收执行成功或失败回调。

`STREAMING` 或 `BATCH` 不会改变 Lakehouse Flow 的 DAG 与结果模型，也不暴露实际使用 Flink、Spark 或其他执行引擎。批式 intent 在有界范围的最终目标数据 snapshot 上形成完成证据；流式回放 intent 在某次数据提交已处理到冻结的 input snapshot/watermark vector 后形成完成证据。兼容属性 `lakehouse-flow.final=true` 表示“本次逻辑调度范围完成”，不表示引擎作业结束。

数据库、HTTP 和 MQ 必须传输完全相同的 `instructionPayload`。传输 ACK 只证明消息送达，不能确认任务成功。

`LF-1.0` 正式交付仅承诺 `DATABASE_TABLE + HTTP`。MQ 继续遵守同一 payload 和 publisher SPI，但具体 broker 绑定不属于 1.0 必选实现。

机器可读 Schema 位于：

- [SchedulingIntent 1.3](./lakehouse-flow-boot/src/main/resources/contracts/lakehouse-flow/lf-1.0/scheduling-intent-1.3.schema.json)
- [JobControlIntent 1.0](./lakehouse-flow-boot/src/main/resources/contracts/lakehouse-flow/lf-1.0/job-control-intent-1.0.schema.json)

同 major 只允许增加可选字段，接收方必须忽略未知字段；删除、重命名、类型变化或幂等/归因语义变化必须升级 major。完整 REST、payload、下游幂等与 migration 兼容策略见 [LF1_COMPATIBILITY.md](./LF1_COMPATIBILITY.md)。

## 2. 数据处理指令结构

```json
{
  "contractVersion": "1.3",
  "source": "LAKEHOUSE_FLOW",
  "intentKind": "DATA_PROCESSING",
  "intentKey": "task-instance:42",
  "issuedAt": "2026-09-13T10:30:00",
  "identity": {
    "taskInstanceId": 42,
    "workflowInstanceId": 21,
    "flowPlanVersionId": 7,
    "scheduleNodeId": 9
  },
  "definition": {
    "workflowCode": "orders-daily",
    "workflowVersion": 3,
    "taskCode": "build-dwd-orders",
    "taskVersion": 2
  },
  "writer": {
    "writerJobKey": "writer.dwd.orders",
    "writerEpoch": 17,
    "tableAssetKey": "paimon.dwd.orders"
  },
  "schedule": {
    "triggerType": "BACKFILL",
    "triggerEventId": "backfill:12:2026-09-01",
    "triggerReason": "repair historical orders",
    "bizDate": "2026-09-01",
    "backfillBatchId": 12,
    "backfillItemId": 35
  },
  "processing": {
    "processingMode": "STREAMING",
    "inputSnapshotVector": [
      {
        "parentNodeCode": "orders-stream",
        "parentProcessingMode": "STREAMING",
        "evidenceSource": "ASSET_SNAPSHOT",
        "assetKey": "paimon.ods.orders.dt=2026-09-01",
        "snapshotId": "812",
        "watermark": "2026-09-01T23:59:59",
        "upstreamTaskInstanceId": null,
        "observedAt": "2026-09-13T10:29:58"
      },
      {
        "parentNodeCode": "payments-daily",
        "parentProcessingMode": "BATCH",
        "evidenceSource": "SCHEDULE_INSTANCE_OUTPUT",
        "upstreamTaskInstanceId": 41,
        "assetKey": "paimon.ods.payments.dt=2026-09-01",
        "snapshotId": "433",
        "watermark": "2026-09-01T23:59:59",
        "observedAt": "2026-09-13T10:29:55"
      }
    ]
  },
  "schedulingPolicy": {
    "concurrencyMode": "SERIAL_WAIT",
    "maxActiveInstances": 1
  },
  "publicationAdmission": {
    "targetAssetKey": "paimon.dwd.orders.dt=2026-09-01",
    "bizDate": "2026-09-01",
    "holderIntentKey": "task-instance:42",
    "leaseExpiresAt": "2026-09-13T11:30:00"
  },
  "snapshotEvidence": {
    "mode": "INTENT_CORRELATED",
    "targetAssetKey": "paimon.dwd.orders.dt=2026-09-01",
    "baselineSnapshotId": "100",
    "confirmationTimeout": "PT1H",
    "expectedPartition": "dt=2026-09-01",
    "requiredSnapshotChangeType": "DATA",
    "requiredSnapshotProperties": {
      "lakehouse-flow.source": "LAKEHOUSE_FLOW",
      "lakehouse-flow.intent-key": "task-instance:42",
      "lakehouse-flow.writer-job-key": "writer.dwd.orders",
      "lakehouse-flow.writer-epoch": "17",
      "lakehouse-flow.target-asset": "paimon.dwd.orders.dt=2026-09-01",
      "lakehouse-flow.biz-date": "2026-09-01",
      "lakehouse-flow.final": "true"
    }
  }
}
```

普通调度、重跑、恢复任务的 `backfillBatchId` 和 `backfillItemId` 可以为空。`baselineSnapshotId` 为空表示目标资产此前没有可见 snapshot，不表示可以省略归因属性。

每条 SchedulingIntent 的 `intentKind` 固定为 `DATA_PROCESSING`，只携带 Flow 层理解的 `processingMode=STREAMING|BATCH`，不携带执行引擎类型。流式节点的普通 snapshot 推进不产生逐 snapshot intent；对流式节点执行补数或重跑时，Lakehouse Flow 才派生一条带冻结输入范围的数据处理回放意图。

混合流批 DAG 的 `inputSnapshotVector` 统一保存父节点输出资产证据。`parentProcessingMode=STREAMING` 时，`evidenceSource=ASSET_SNAPSHOT` 表示证据来自流式父节点持续产出的 AssetState/事件账本，不要求父 task；`parentProcessingMode=BATCH` 时，`evidenceSource=SCHEDULE_INSTANCE_OUTPUT` 必须同时携带同一调度实例内的 `upstreamTaskInstanceId`。下游只有在所有直接父边和自身额外资产依赖都满足后才能形成该向量。

有界批节点的 `SchedulingIntent` 本身就是平台执行面启动一次有界运行的授权，不再额外发送 `START_JOB`。常驻流节点使用独立的 `JobControlIntent` 管理生命周期；其日常数据推进不重复下发启动意图。连续 writer 所属表补数时，执行面必须让当前流作业消费回放意图，或先 fencing 当前流 epoch 后以同一个 `writerJobKey` 切换到批式 epoch，禁止第二个作业并发写表。

`schedulingPolicy` 和 `snapshotEvidence.confirmationTimeout` 是 Lakehouse Flow 本次发布实际采用的冻结策略证据。下游不据此回传任务状态；真正与下游启动相关的截止时间仍以 `publicationAdmission.leaseExpiresAt` 为准。

### 2.1 作业控制指令结构

```json
{
  "contractVersion": "1.0",
  "source": "LAKEHOUSE_FLOW",
  "intentKind": "JOB_CONTROL",
  "intentKey": "job-control:writer.ods.orders:17",
  "issuedAt": "2026-09-13T10:30:00",
  "writer": {
    "writerJobKey": "writer.ods.orders",
    "tableAssetKey": "paimon.ods.orders",
    "writerEpoch": 17,
    "previousWriterEpoch": 16,
    "processingMode": "STREAMING"
  },
  "control": {
    "operationType": "RESTART_JOB",
    "reason": "restore from latest completed checkpoint",
    "requestedBy": "operator@example.com",
    "deliverBefore": "2026-09-13T10:40:00"
  },
  "snapshotObservation": {
    "targetTableAssetKey": "paimon.ods.orders",
    "baselineSnapshotId": "811",
    "confirmationTimeout": "PT10M",
    "requiredWriterProperties": {
      "lakehouse-flow.source": "LAKEHOUSE_FLOW",
      "lakehouse-flow.job-control-intent-key": "job-control:writer.ods.orders:17",
      "lakehouse-flow.writer-job-key": "writer.ods.orders",
      "lakehouse-flow.writer-epoch": "17"
    }
  }
}
```

`JobControlIntent.control.operationType` 只支持 `START_JOB` 和 `RESTART_JOB`。平台控制面在同一个事务中锁定 `WriterJobBinding`、分配单调递增的 `writerEpoch` 并持久化控制意图；平台执行面被动接收意图后根据 `writerJobKey` 解析实际引擎和作业定义，完成真实操作及旧 epoch fencing。它不是数据处理任务，因此不得携带伪造的 workflow、task、DAG 节点、业务日期、输入 snapshot 向量或 `FLINK/SPARK` 引擎枚举。

控制意图的 delivery ACK 只表示执行面收到操作。首个携带全部 `requiredWriterProperties` 的目标业务数据 snapshot 可以形成 `SNAPSHOT_CONFIRMED` 证据，说明新 writer epoch 确实推进了数据；确认窗口内没有目标数据只能形成 `SNAPSHOT_NOT_ADVANCED`，不能据此断言实际引擎启动失败。`COMPACT` 等维护 snapshot 不得确认控制意图。

## 3. 数据处理下游规则

1. 以 `intentKey` 作为消费和启动幂等键。同一个意图重复送达时，不得重复启动等价工作。
2. 使用 `taskCode + taskVersion` 解析下游维护的任务定义；Lakehouse Flow 不传递 executor 实现或运行状态。
3. 使用 `bizDate` 绑定本次业务日期，并只写入 `targetAssetKey` 表示的目标范围。
4. 校验 `writerJobKey` 与规范化 `tableAssetKey` 的平台唯一绑定，并保证同一物理表只有这个 writer 作业能够写入。
5. 在启动、重启或流批运行模式切换前 fencing 旧 `writerEpoch`；旧 epoch 的进程不得继续向该表提交数据。每个可归因 snapshot 必须写入当前 `writerJobKey + writerEpoch`。
6. 严格处理 `processing.inputSnapshotVector` 冻结的每个输入资产边界，不得把“读到任意较新数据”当作本次调度范围已经完成。
7. `processing.processingMode=BATCH` 时，以有界方式处理冻结输入，并在该范围的最终业务数据 snapshot 写入 `requiredSnapshotProperties`。
8. `processing.processingMode=STREAMING` 时，由持续运行的 writer 处理冻结输入；只有某次业务数据提交已完整覆盖全部输入边界后，才能写入 `requiredSnapshotProperties`。
9. `processingMode` 是 Flow 与通用意图唯一理解的计算方式分类。具体使用 checkpoint、job-end 或其他 commit hook 由执行适配器决定，不得向通用契约增加执行引擎枚举。
10. `lakehouse-flow.final=true` 表示本次 intent 的逻辑范围完成。普通中间提交、尚未覆盖完整输入向量的提交不得写入该值；流式作业不需要因为写入该值而停止。
11. 逻辑完成输出必须是真实业务数据变更，满足 `requiredSnapshotChangeType=DATA`；`COMPACT`、`ANALYZE` 等维护 snapshot 不得作为完成证据。原生 operation 的分类由对应湖格式适配器负责。
12. 不向 Lakehouse Flow 回传 SUCCESS、FAILED、RUNNING 或 executor 状态。下游异常时可以不产生逻辑完成标记；确认窗口结束且 source 证据完整时，Lakehouse Flow 才判定未获得 snapshot 证据。
13. 消费时检查 `publicationAdmission.leaseExpiresAt`；当前时间达到或超过该值时不得再启动这条旧 intent。该判断不需要向 Lakehouse Flow 回报状态。

### 3.1 作业控制下游规则

1. 以 `JobControlIntent.intentKey` 作为操作幂等键，重复投递不得重复启动或重启同一 epoch。
2. 只有 `WriterJobBinding.currentWriterEpoch` 对应的控制意图可以激活 writer；低于当前值的迟到意图必须拒绝。
3. `RESTART_JOB` 必须先 fencing `previousWriterEpoch`，确认旧进程不能再提交后才能激活新 epoch。这里的确认由平台执行面完成，不回写为 Lakehouse Flow 的任务状态。
4. 作业的实际启动、checkpoint 恢复、资源申请和日志都在平台执行面完成；Lakehouse Flow 只保存控制、投递和 snapshot 观察证据。
5. 当前 epoch 的每个业务数据 snapshot 都必须携带 `writerJobKey`、`writerEpoch` 和 `job-control-intent-key`。没有业务数据变化时不得为了证明作业存活而制造 snapshot。
6. 消费时检查 `control.deliverBefore`；过期的旧控制意图不得启动。更高 epoch 已分配后，旧意图即使仍在投递窗口内也必须拒绝。

## 4. 发布互斥与租约边界

FlowPlanVersion 发布前先按规范化 `tableAssetKey` 校验唯一 `writerJobKey`，防止不同 Flow、节点或模式成为同表 writer。发布 `SchedulingIntent` 时，Lakehouse Flow 先完成发布版本的 `maxActiveInstances` 准入，再取得 `targetAssetKey + bizDate` 槽并校验 writer binding/epoch；writer 不可用时立即释放本次目标槽。正常调度、补数、恢复和重跑共享这些调度侧互斥；任一准入未通过时都不会冻结 baseline，也不会产生 outbox 记录，而是保持 READY 等待内部扫描重试。

发布 `JobControlIntent` 时不进入 workflow 并发或目标日期准入，而是锁定对应 `WriterJobBinding` 并原子分配下一 epoch。同一 writer 的两个启动/重启请求只能串行形成控制意图；不同 writer 可以并行。数据库唯一键必须保证一个 `tableAssetKey` 只存在一个有效 binding，不能仅依赖发布前内存校验。

槽位在匹配逻辑完成 snapshot 确认或 `publicationAdmission.leaseExpiresAt` 到期后可释放。租约过期后可能发布新的 intent，因此下游仍必须以 `intentKey` 保证启动幂等，并把对应的 `requiredSnapshotProperties` 写入各自逻辑完成 snapshot。旧 intent 晚到的 snapshot 只能归属于旧 intent，不能确认新 intent，也不能释放新 holder 的槽位。租约到期只改变调度准入；source 阻塞时，旧 task 的 snapshot 结果仍然待定。

目标日期槽是调度意图发布互斥，不是单表 writer 锁。单表单作业首先由 `tableAssetKey -> writerJobKey` 发布约束保证，启动/重启竞态再由平台执行面按 `writerEpoch` fencing。Lakehouse Flow 不读取执行引擎状态，也不能仅凭租约超时证明旧进程已经停止；执行面必须拒绝旧 epoch commit，并拒绝在 `leaseExpiresAt` 后才准备启动的旧 intent。

### 4.1 传输状态与至少一次投递

每条不可变 outbound intent 只选择一个 route，避免同一部署同时从数据库、HTTP 和 MQ 重复下发。`SchedulingIntent` 与 `JobControlIntent` 使用各自独立的领域记录和 delivery 外键，但复用同一 claim、fencing、退避、死信算法及 publisher SPI，不采用可空多态外键。数据处理路由由 `lakehouse-flow.scheduling-intent-delivery.channel` 和 `destination` 配置，作业控制路由使用对应的 job-control 配置：

- `DATABASE_TABLE`：intent 与 delivery 在同一事务提交，delivery 立即为 `PUBLISHED`；执行面分别轮询 `scheduling_intent` 或 `job_control_intent`。
- `HTTP`：delivery 初始为 `PENDING`，内部 scanner 使用 Java 17 HTTP client 主动 POST；只有 2xx 算传输 ACK，响应 body 不作为任务状态读取。
- `MQ`：delivery 初始为 `PENDING`，内部 scanner 调用 broker-neutral `SchedulingIntentMessageGateway`；具体 Kafka、Pulsar 或 RabbitMQ 绑定由部署适配器提供。

外部路由的状态机为：

```text
PENDING -> PUBLISHING -> PUBLISHED
                      -> RETRY_WAIT -> PUBLISHING
                      -> EXHAUSTED
```

`PUBLISHING` 使用短数据库 claim lease 和随机 fencing token。进程退出后，过期 claim 可以由其他 scheduler 实例重占；旧进程的迟到 ACK 无法覆盖新 claim。失败按指数退避重试，达到最大尝试次数或该意图自己的投递截止时间后进入 `EXHAUSTED`。`SchedulingIntent` 使用 `publicationAdmission.leaseExpiresAt`，`JobControlIntent` 使用 `control.deliverBefore`。`EXHAUSTED` 是传输死信，不是下游任务或作业失败。

数据处理侧 `SCHEDULED` 表示 intent 已经在 Lakehouse Flow 内持久化并形成不可撤销的调度承诺，不等于 HTTP/MQ 已 ACK，更不等于下游启动。确认窗口从该承诺开始，传输重试包含在窗口内。意图一旦形成，暂停或取消操作不会伪装撤回它；publisher 到截止时间前仍可能至少一次投递。因此下游必须以 `intentKey` 幂等，并在真正启动前检查对应截止时间。

`BackfillItem.INTENT_DELIVERED` 是兼容已有批次模型保留的名称，其精确语义同样是 intent 已持久化；HTTP/MQ 是否真正 ACK 必须读取 `SchedulingIntentDelivery.status`。批次所谓“尚未交付”指尚未形成不可变 intent，而不是已经 committed 但仍处于 `PENDING/RETRY_WAIT` 的外部传输。

HTTP 请求会携带 `Idempotency-Key`、`X-Lakehouse-Flow-Intent-Key`、`X-Lakehouse-Flow-Contract-Version` 和 `X-Lakehouse-Flow-Delivery-Id`。MQ gateway 使用 intent key 作为 message key 并携带等价元数据。两种通道的消息 body 都只包含本契约定义的 `instructionPayload`，不会混入 delivery 重试状态或 executor 状态。

## 5. Lakehouse Flow 的确认规则

对于 `SchedulingIntent`，Lakehouse Flow 保存 baseline 之后的完整 snapshot 事件序列，并查找满足以下全部条件的事件：

```text
snapshotId > baselineSnapshotId
AND adapter classified dataChange = true
AND snapshotProperties 包含全部 requiredSnapshotProperties
AND 目标为分区资产时，changedPartitions 包含 expectedPartition
```

未携带本次 `intentKey` 的补数、正常任务和外部数据写入仍会更新 `AssetState` 的业务数据轨，但不会确认当前调度意图；维护提交只更新物理观察轨。即使匹配 snapshot 之后又出现了其他 snapshot，历史匹配证据也不会被最新状态覆盖。

对于 `JobControlIntent`，确认条件改为 baseline 之后的业务数据 snapshot 同时匹配 `job-control-intent-key + writer-job-key + writer-epoch`。该证据只确认“平台下发的新 writer 世代产生了数据”，不建立或读取实际引擎运行状态；没有数据变化时保持同样的失败关闭边界。

### 5.1 超时与 Source 可观测性

`LF-1.0` 必须将 snapshot 结果、delivery 状态和 source 健康作为三个正交维度对外暴露：

```text
snapshotResult = WAITING | SNAPSHOT_CONFIRMED | SNAPSHOT_NOT_ADVANCED
deliveryStatus = PENDING | PUBLISHING | RETRY_WAIT | PUBLISHED | DELIVERY_EXHAUSTED
sourceHealth   = HEALTHY | REPAIRABLE | SOURCE_BLOCKED
```

`DELIVERY_EXHAUSTED` 只是存储层 `EXHAUSTED` 的稳定对外名称，不表示下游执行失败。`SOURCE_BLOCKED` 只是 source 对账 `BLOCKED` 的稳定对外名称，不表示下游任务没有成功。

确认窗口结束时，Lakehouse Flow 只有在目标 source 已追平、durable offset 与事件/AssetState 证据一致，且不存在 retention gap 时，才能把“没有匹配 snapshot”结束为 `SNAPSHOT_NOT_ADVANCED`。source 不可用、落后未补齐、offset 异常或投影证据不完整时，snapshot 结果保持 `WAITING`，并单独记录 `SOURCE_BLOCKED`。source 恢复后先连续补扫，再恢复 snapshot 结果判定。

## 6. Snapshot 触发路由

所有 snapshot 都进入事件账本、推进 source offset 并更新表级 `AssetState.latestSnapshotId`。只有 typed `dataChange=true` 才更新 `latestDataSnapshotId/latestDataWatermark/latestDataCommitTime`，并按 `changedPartitions` 更新分区级 `AssetState`。是否进入全局自然触发还要由 snapshot 来源决定：

```text
外部 dataChange=true snapshot
  -> 允许评估 published FlowPlan

仅携带 JobControlIntent writer 世代属性的 dataChange=true snapshot
  -> 可以确认 writer epoch 已产生数据
  -> 仍按自然资产推进评估 published FlowPlan

Lakehouse Flow 非 action intent 的合法逻辑完成 snapshot
  -> 允许确认原 intent，并评估依赖它的其他 FlowPlan

BACKFILL / BACKFILL_RECOVERY / RERUN / RERUN_TASK
  -> 只确认所属 intent，并推进所属 workflow/backfill DAG
  -> 禁止额外生成 SNAPSHOT_DRIVEN 正常实例

中间提交、孤儿 intent、伪造属性、维护提交
  -> 更新事实但禁止自然触发
```

Action-owned snapshot 的路由依据来自 `intentKey -> SchedulingIntent.triggerType` 反查，不能由下游自行声明 trigger type。`job-control-intent-key` 只证明 writer 世代，不改变业务数据的自然触发来源；同一 snapshot 若还携带数据处理 `intent-key`，则优先按该 SchedulingIntent 的 trigger type 路由。这样补数会改变真实表 snapshot，但不会串入当前正常调度实例。

日期型 FlowPlan 必须把输入依赖和输出目标声明为包含 `${bizDate}` 的分区资产。整表资产语义本来就代表任意分区提交；如果计划选择依赖整表，历史补数更新整表 `AssetState` 属于预期行为，不能获得日期级隔离。

## 7. 湖格式适配要求

Lakehouse Flow 的摄入核心只消费统一 `LakehouseSnapshot`，其中：

- `sourceOffset` 是适配器持久化的原生扫描位置，其比较规则由适配器提供；
- `snapshotId` 是适配器归一化后的单调调度坐标，用于 baseline 和推进比较；原生 snapshot id 不具备顺序语义时必须保留在 payload，不能直接填入该字段；
- `schemaId` 是当前 snapshot 关联的原生 schema 标识，只作为事实，不进行跨格式大小比较；
- `commitKind` 保留原生 operation，仅用于审计；
- `dataChange` 是适配器给出的跨格式统一分类，并持久化为事件 typed 字段；
- `snapshotProperties` 提供 intent 归因；
- `changedPartitions` 提供分区目标证据。

新增 Iceberg、Hudi 或其他湖格式时，只能在适配器内解释原生 metadata 和 operation，不能把格式枚举带入调度服务。适配器必须逐个读取 offset 之后的 snapshot/commit，不能只读取 latest；发生保留历史缺口时必须失败关闭并进入对账，不能静默跳过。

推荐的坐标映射为：Paimon 使用连续 snapshot id；Iceberg 使用单调 sequence number，并把原生 snapshot id 放入 payload；Hudi 使用可排序 timeline instant。新格式若无法提供稳定的全序推进坐标，在定义对账与缺口语义前不得接入调度确认链路。

首次接入的默认启动位置为 `LATEST`，用于建立当前事实而不是隐式历史补数；只有运维显式选择 `EARLIEST` 时才允许回放仍被保留的历史。无论哪种模式，一旦存在 durable offset，后续都必须从其严格后继连续扫描。

### 7.1 Flink CDC 到 Paimon ODS

业务库增量入湖固定为 Flink CDC 持续流式写入 Paimon ODS。这条常驻数据入口不是 Lakehouse Flow 按 snapshot 重复启动的 task：

1. 平台控制面提供 Flink CDC 作业启动和重启操作并投递独立 `JobControlIntent`；平台执行面负责真实 Flink 生命周期、checkpoint、状态恢复和资源。
2. 每次包含业务数据变化的 Paimon ODS snapshot 作为外部 `dataChange=true` 事实进入 Lakehouse Flow。
3. 当前 writer epoch 的 ODS 业务 snapshot 携带 `job-control-intent-key + writer-job-key + writer-epoch` 以证明平台托管来源，并可附带 CDC watermark、source offset 等审计信息；没有数据处理 intent 时不得伪造 `lakehouse-flow.intent-key` 或 `lakehouse-flow.final`。
4. Lakehouse Flow 以 ODS snapshot/watermark 推进评估 DWD 等下游依赖，不读取 Flink CDC job 状态。

DWD、DWS、ADS 的常驻流作业如果只是持续自然加工，同样以外部 snapshot 身份推进资产；其 snapshot 可以触发下游，但不能确认不存在的 task intent。只有普通调度、补数或重跑明确产生了 intent，流式 writer 才在完整覆盖该 intent 输入向量的 checkpoint snapshot 上写入归因属性。

### 7.2 Paimon 与 Flink 流批 writer

Paimon source 必须按持久化扫描游标读取每一个新 snapshot，而不是只读取 latest snapshot，并采集：

- `snapshotId`
- `commitUser`
- `commitIdentifier`
- `commitKind`
- `snapshotProperties`
- 从 delta manifests 推导出的 `changedPartitions`

Paimon 的 `APPEND` / `OVERWRITE` 映射为 `dataChange=true`，`COMPACT` / `ANALYZE` 等映射为 false。`commitUser` 表示写入作业身份，`commitIdentifier` 表示写入事务或 checkpoint，二者只用于补充审计。Lakehouse Flow 以 `requiredSnapshotProperties` 作为调度意图归因依据。

当前实现基于 Apache Paimon 1.3 Catalog API。Paimon snapshot id 连续，snapshot 元数据包含 commit identity 和 delta manifest list；对应格式说明见 [Paimon Snapshot](https://paimon.apache.org/docs/1.3/concepts/spec/snapshot/)。

Paimon 1.3 的标准 commit 与普通 SQL 写入链路不能假设会自动为单次 commit 注入任意 `requiredSnapshotProperties`。接入方必须提供 writer-side adapter 或 connector 扩展，在逻辑完成 commit 构造 `ManifestCommittable` 时通过其 property 能力注入这些属性；Lakehouse Flow 的 Paimon source 只负责读取和验证，不参与写入。

`LF-1.0` 的参考实现固定为下游侧 Flink/Paimon 流批一体 writer-side adapter：

1. 下游接收 intent 后将 `intentKey`、`writerJobKey`、`writerEpoch`、`bizDate`、`targetAssetKey`、`leaseExpiresAt`、`processing` 和完整 `requiredSnapshotProperties` 作为本次逻辑处理范围的不可变输入。
2. 批式路径以 `processing.inputSnapshotVector` 创建有界输入，在 `JOB_END` 对应业务数据 commit 注入 `lakehouse-flow.final=true`。
3. 流式路径持续运行，在某次 checkpoint 已完整处理到 `processing.inputSnapshotVector` 后，才在该 checkpoint 对应业务数据 commit 注入 `lakehouse-flow.final=true`；后续 checkpoint 不重复携带该 intent 的完成标记。
4. 下游以 `intentKey` 保证消费幂等，并在真正接受处理前拒绝已过 `leaseExpiresAt` 的旧 intent。
5. adapter 是可供下游引入的独立边界，Flink/Paimon 依赖不得泄漏到 Lakehouse Flow 的 service、scheduler 或通用 source SPI。
6. 部署验收分别完成流式 checkpoint 和批式 job-end 的带属性探针提交，再验证 source 完整读回数据处理意图的七个约定属性、`dataChange=true` 和目标 `changedPartitions`；常驻作业还要验证四个 writer 世代属性。

当前可复用实现位于 `lakehouse-flow-flink-paimon`。`WriterCommitContext` 在打开写入路径前校验 intent 类型、物理表、writer 身份、epoch 和完整 snapshot properties；`FlinkPaimonWriterAdapter` 统一准备 checkpoint/批式 committable 并注入属性；`WriterEpochFence` 是执行面可替换的 fencing SPI。受信单团队部署提供的 `JdbcWriterEpochFence` 会对 Lakehouse Flow PostgreSQL 中的 `writer_job_binding` 执行 `SELECT ... FOR UPDATE`，精确校验当前 holder，并把该行锁保持到 Paimon commit 成功或失败后，防止旧 writer 在检查和物理提交之间跨过新 epoch 分配。

平台执行面负责 Flink 状态恢复，不由 Lakehouse Flow 读取 job 状态。当前整体 E2E 将 CDC checkpoint 外部化并在取消旧 JobID 后保留最终 checkpoint，`RESTART_JOB` 以新 epoch 从该 checkpoint 恢复 source offset；恢复是否成功仍只由后续可归因 Paimon 业务 snapshot 证明。生产执行面可以使用等价的 checkpoint/savepoint 策略，但不得退化为从 `LATEST` 重新启动并跳过重启窗口内的数据。

流式场景的完成对象是 intent 冻结的输入 snapshot/watermark vector，不是无界作业本身。一条 intent 在 LF-1.0 中只能由一个目标业务 snapshot 确认；writer adapter 必须保证同一 intent 不跨多个 checkpoint 重复写完成标记。无法安装该写入扩展的 Paimon 任务不能使用 `INTENT_CORRELATED` 成功确认；`commitUser` / `commitIdentifier` 不得降级替代强归因。

### 7.3 Source reconciliation

适配器必须实现当前位置检查，并明确返回 `EMPTY`、`UNINITIALIZED`、`IN_SYNC`、`LAGGING`、`RETENTION_GAP`、`OFFSET_AHEAD` 或 `ERROR`。是否存在 retention gap 只能由格式适配器依据其原生 offset 语义判断，公共调度代码不得猜测。

Lakehouse Flow 同时对账物理轨 `source range -> durable offset -> latest event -> AssetState.latestSnapshotId` 和业务轨 `latest data event -> AssetState.latestDataSnapshotId`。只允许两类自动补偿：从 durable offset 严格后继继续读取仍保留的 snapshot，以及重放已经持久化的物理或数据事件投影。不得修改 snapshot、伪造事件、把 offset 跳到 latest，或因对账异常确认任何调度意图。

同一 source 的摄入事务必须在写入事件之前初始化并锁定 `(sourceType, sourceName)` offset 行，并把该 PostgreSQL 行锁保持到事件、AssetState、DAG/Flow 评估和 offset 一起提交。并发 scheduler 因此只能串行投影同一 source；事务在任意中间切点回滚时，首次 offset 初始化也随事务回滚，下一节点可从同一 snapshot 重放。offset 比较必须使用格式适配器的顺序语义，迟到的较小 offset 不得覆盖较大 durable offset。

`RETENTION_GAP`、`OFFSET_AHEAD`、offset 缺少事件、source latest 缺少对应事件等状态必须失败关闭。运维可通过 Prometheus source 指标查看 lag、gap 和 projection inconsistency；这些指标不会替代 snapshot 证据。

## 8. 失败关闭原则

以下情况不得推断成功：

- snapshot 推进但没有意图属性；
- 属性中的 `intentKey` 属于另一条意图；
- 只有 `commitUser` 或 `commitIdentifier` 相似；
- snapshot 属于维护提交；
- 分区资产缺少目标分区变更证据；
- 仅收到 HTTP/MQ/数据库投递确认。

这些情况不得推断成功。确认窗口结束后，只有 source 证据链完整时才记录 `SNAPSHOT_NOT_ADVANCED`；source 缺口或不可用时继续保留 snapshot 待定并暴露 `SOURCE_BLOCKED`。
