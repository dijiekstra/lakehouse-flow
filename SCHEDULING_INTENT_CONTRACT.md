# Scheduling Intent 下游契约

**契约版本**: 1.2
**适用通道**: Database Outbox、HTTP、MQ

## 1. 契约目标

Lakehouse Flow 只负责产生和投递调度意图，不执行任务，也不读取下游任务状态。

下游接收调度意图后负责实际运行，并在最终目标资产 snapshot 中写入意图指定的属性。Lakehouse Flow 通过目标 snapshot 的属性、格式适配器给出的数据变更分类和变更分区完成归因与结果确认，不接收执行成功或失败回调。

数据库、HTTP 和 MQ 必须传输完全相同的 `instructionPayload`。传输 ACK 只证明消息送达，不能确认任务成功。

## 2. 指令结构

```json
{
  "contractVersion": "1.2",
  "source": "LAKEHOUSE_FLOW",
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
  "schedule": {
    "triggerType": "BACKFILL",
    "triggerEventId": "backfill:12:2026-09-01",
    "triggerReason": "repair historical orders",
    "bizDate": "2026-09-01",
    "backfillBatchId": 12,
    "backfillItemId": 35
  },
  "schedulingPolicy": {
    "concurrencyMode": "SERIAL_WAIT",
    "maxActiveInstances": 1
  },
  "publicationAdmission": {
    "targetAssetKey": "paimon.prod.orders.dt=2026-09-01",
    "bizDate": "2026-09-01",
    "holderIntentKey": "task-instance:42",
    "leaseExpiresAt": "2026-09-13T11:30:00"
  },
  "snapshotEvidence": {
    "mode": "INTENT_CORRELATED",
    "targetAssetKey": "paimon.prod.orders.dt=2026-09-01",
    "baselineSnapshotId": "100",
    "confirmationTimeout": "PT1H",
    "expectedPartition": "dt=2026-09-01",
    "requiredSnapshotChangeType": "DATA",
    "requiredSnapshotProperties": {
      "lakehouse-flow.source": "LAKEHOUSE_FLOW",
      "lakehouse-flow.intent-key": "task-instance:42",
      "lakehouse-flow.target-asset": "paimon.prod.orders.dt=2026-09-01",
      "lakehouse-flow.biz-date": "2026-09-01",
      "lakehouse-flow.final": "true"
    }
  }
}
```

普通调度、重跑、恢复任务的 `backfillBatchId` 和 `backfillItemId` 可以为空。`baselineSnapshotId` 为空表示目标资产此前没有可见 snapshot，不表示可以省略归因属性。

`schedulingPolicy` 和 `snapshotEvidence.confirmationTimeout` 是 Lakehouse Flow 本次发布实际采用的冻结策略证据。下游不据此回传任务状态；真正与下游启动相关的截止时间仍以 `publicationAdmission.leaseExpiresAt` 为准。

## 3. 下游必须遵守的规则

1. 以 `intentKey` 作为消费和启动幂等键。同一个意图重复送达时，不得重复启动等价工作。
2. 使用 `taskCode + taskVersion` 解析下游维护的任务定义；Lakehouse Flow 不传递 executor 实现或运行状态。
3. 使用 `bizDate` 绑定本次业务日期，并只写入 `targetAssetKey` 表示的目标范围。
4. 将 `requiredSnapshotProperties` 原样写入本次最终数据 commit 的 snapshot properties。
5. 中间提交不得写 `lakehouse-flow.final=true`。一次运行存在多个提交时，仅最终业务数据 snapshot 写入该值。
6. 最终输出必须是真实业务数据变更，满足 `requiredSnapshotChangeType=DATA`；`COMPACT`、`ANALYZE` 等维护 snapshot 不得作为最终输出。原生 operation 的分类由对应湖格式适配器负责。
7. 不向 Lakehouse Flow 回传 SUCCESS、FAILED、RUNNING 或 executor 状态。下游异常时可以不产生最终标记，Lakehouse Flow 将按确认窗口判定未获得 snapshot 证据。
8. 消费时检查 `publicationAdmission.leaseExpiresAt`；当前时间达到或超过该值时不得再启动这条旧 intent。该判断不需要向 Lakehouse Flow 回报状态。

## 4. 发布互斥与租约边界

Lakehouse Flow 先按发布版本的 `maxActiveInstances` 完成 workflow 准入，再按 `targetAssetKey + bizDate` 获取持久化目标槽。正常调度、补数、恢复和重跑共享这些调度侧互斥；任一准入未通过时都不会冻结 baseline，也不会产生 outbox 记录，而是保持 READY 等待内部扫描重试。

槽位在匹配最终 snapshot 确认或确认窗口超时后释放。租约过期后可能发布新的 intent，因此下游仍必须以 `intentKey` 保证启动幂等，并把对应的 `requiredSnapshotProperties` 写入各自最终 snapshot。旧 intent 晚到的 snapshot 只能归属于旧 intent，不能确认新 intent，也不能释放新 holder 的槽位。

这是调度意图发布互斥，不是外部执行锁。Lakehouse Flow 无法阻止未经过本系统启动的任务，也不能在租约超时后证明旧下游进程已经停止。下游必须拒绝在 `leaseExpiresAt` 后才准备启动的旧 intent；若运行可能跨过租约且需要更强的写入排他，还应由下游写入平台基于 `holderIntentKey` 和目标分区实现 fencing 或事务冲突控制。

### 4.1 传输状态与至少一次投递

每条不可变 `SchedulingIntent` 只选择一个 outbound route，避免同一部署同时从数据库、HTTP 和 MQ 重复下发。路由由 `lakehouse-flow.scheduling-intent-delivery.channel` 和 `destination` 配置：

- `DATABASE_TABLE`：intent 与 delivery 在同一事务提交，delivery 立即为 `PUBLISHED`，下游轮询 `scheduling_intent`。
- `HTTP`：delivery 初始为 `PENDING`，内部 scanner 使用 Java 17 HTTP client 主动 POST；只有 2xx 算传输 ACK，响应 body 不作为任务状态读取。
- `MQ`：delivery 初始为 `PENDING`，内部 scanner 调用 broker-neutral `SchedulingIntentMessageGateway`；具体 Kafka、Pulsar 或 RabbitMQ 绑定由部署适配器提供。

外部路由的状态机为：

```text
PENDING -> PUBLISHING -> PUBLISHED
                      -> RETRY_WAIT -> PUBLISHING
                      -> EXHAUSTED
```

`PUBLISHING` 使用短数据库 claim lease 和随机 fencing token。进程退出后，过期 claim 可以由其他 scheduler 实例重占；旧进程的迟到 ACK 无法覆盖新 claim。失败按指数退避重试，达到最大尝试次数或 `publicationAdmission.leaseExpiresAt` 后进入 `EXHAUSTED`。`EXHAUSTED` 是传输死信，不是下游任务失败。

调度侧 `SCHEDULED` 表示 intent 已经在 Lakehouse Flow 内持久化并形成不可撤销的调度承诺，不等于 HTTP/MQ 已 ACK，更不等于下游启动。确认窗口从该承诺开始，传输重试包含在窗口内。意图一旦形成，暂停或取消操作不会伪装撤回它；publisher 到截止时间前仍可能至少一次投递。因此下游必须以 `intentKey` 幂等，并在真正启动前检查 `leaseExpiresAt`。

`BackfillItem.INTENT_DELIVERED` 是兼容已有批次模型保留的名称，其精确语义同样是 intent 已持久化；HTTP/MQ 是否真正 ACK 必须读取 `SchedulingIntentDelivery.status`。批次所谓“尚未交付”指尚未形成不可变 intent，而不是已经 committed 但仍处于 `PENDING/RETRY_WAIT` 的外部传输。

HTTP 请求会携带 `Idempotency-Key`、`X-Lakehouse-Flow-Intent-Key`、`X-Lakehouse-Flow-Contract-Version` 和 `X-Lakehouse-Flow-Delivery-Id`。MQ gateway 使用 intent key 作为 message key 并携带等价元数据。两种通道的消息 body 都只包含本契约定义的 `instructionPayload`，不会混入 delivery 重试状态或 executor 状态。

## 5. Lakehouse Flow 的确认规则

Lakehouse Flow 保存 baseline 之后的完整 snapshot 事件序列，并查找满足以下全部条件的事件：

```text
snapshotId > baselineSnapshotId
AND adapter classified dataChange = true
AND snapshotProperties 包含全部 requiredSnapshotProperties
AND 目标为分区资产时，changedPartitions 包含 expectedPartition
```

未携带本次 `intentKey` 的补数、正常任务和外部数据写入仍会更新 `AssetState` 的业务数据轨，但不会确认当前调度意图；维护提交只更新物理观察轨。即使匹配 snapshot 之后又出现了其他 snapshot，历史匹配证据也不会被最新状态覆盖。

## 6. Snapshot 触发路由

所有 snapshot 都进入事件账本、推进 source offset 并更新表级 `AssetState.latestSnapshotId`。只有 typed `dataChange=true` 才更新 `latestDataSnapshotId/latestDataWatermark/latestDataCommitTime`，并按 `changedPartitions` 更新分区级 `AssetState`。是否进入全局自然触发还要由 snapshot 来源决定：

```text
外部 dataChange=true snapshot
  -> 允许评估 published FlowPlan

Lakehouse Flow 非 action intent 的合法最终 snapshot
  -> 允许确认原 intent，并评估依赖它的其他 FlowPlan

BACKFILL / BACKFILL_RECOVERY / RERUN / RERUN_TASK
  -> 只确认所属 intent，并推进所属 workflow/backfill DAG
  -> 禁止额外生成 SNAPSHOT_DRIVEN 正常实例

中间提交、孤儿 intent、伪造属性、维护提交
  -> 更新事实但禁止自然触发
```

Action-owned snapshot 的路由依据来自 `intentKey -> SchedulingIntent.triggerType` 反查，不能由下游自行声明 trigger type。这样补数会改变真实表 snapshot，但不会串入当前正常调度实例。

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

### 7.1 Paimon

Paimon source 必须按持久化扫描游标读取每一个新 snapshot，而不是只读取 latest snapshot，并采集：

- `snapshotId`
- `commitUser`
- `commitIdentifier`
- `commitKind`
- `snapshotProperties`
- 从 delta manifests 推导出的 `changedPartitions`

Paimon 的 `APPEND` / `OVERWRITE` 映射为 `dataChange=true`，`COMPACT` / `ANALYZE` 等映射为 false。`commitUser` 表示写入作业身份，`commitIdentifier` 表示写入事务或 checkpoint，二者只用于补充审计。Lakehouse Flow 以 `requiredSnapshotProperties` 作为调度意图归因依据。

当前实现基于 Apache Paimon 1.3 Catalog API。Paimon snapshot id 连续，snapshot 元数据包含 commit identity 和 delta manifest list；对应格式说明见 [Paimon Snapshot](https://paimon.apache.org/docs/1.3/concepts/spec/snapshot/)。

Paimon 1.3 的标准 `BatchTableCommit` 与普通 SQL 写入链路没有暴露“为单次 commit 注入任意 snapshot properties”的公共接口，不能假设一条普通 `INSERT` 会自动携带 `requiredSnapshotProperties`。接入方必须提供 writer-side adapter 或 connector 扩展，在最终 commit 构造 `ManifestCommittable` 时通过其 property 能力注入这些属性；Lakehouse Flow 的 Paimon source 只负责读取和验证，不参与写入。部署验收必须先做一笔带属性的探针提交并确认 source 能完整读回五个约定属性。无法安装该写入扩展的 Paimon 任务不能使用 `INTENT_CORRELATED` 成功确认；`commitUser` / `commitIdentifier` 不得降级替代强归因。

### 7.2 Source reconciliation

适配器必须实现当前位置检查，并明确返回 `EMPTY`、`UNINITIALIZED`、`IN_SYNC`、`LAGGING`、`RETENTION_GAP`、`OFFSET_AHEAD` 或 `ERROR`。是否存在 retention gap 只能由格式适配器依据其原生 offset 语义判断，公共调度代码不得猜测。

Lakehouse Flow 同时对账物理轨 `source range -> durable offset -> latest event -> AssetState.latestSnapshotId` 和业务轨 `latest data event -> AssetState.latestDataSnapshotId`。只允许两类自动补偿：从 durable offset 严格后继继续读取仍保留的 snapshot，以及重放已经持久化的物理或数据事件投影。不得修改 snapshot、伪造事件、把 offset 跳到 latest，或因对账异常确认任何调度意图。

`RETENTION_GAP`、`OFFSET_AHEAD`、offset 缺少事件、source latest 缺少对应事件等状态必须失败关闭。运维可通过 Prometheus source 指标查看 lag、gap 和 projection inconsistency；这些指标不会替代 snapshot 证据。

## 8. 失败关闭原则

以下情况不得推断成功：

- snapshot 推进但没有意图属性；
- 属性中的 `intentKey` 属于另一条意图；
- 只有 `commitUser` 或 `commitIdentifier` 相似；
- snapshot 属于维护提交；
- 分区资产缺少目标分区变更证据；
- 仅收到 HTTP/MQ/数据库投递确认。

这些情况保持等待，确认窗口结束后记录为未获得可归属的 snapshot 证据。
