# Lakehouse Flow 生产运维手册

**适用版本**: LF-1.0，数据库 schema V23

本文档是单团队受信环境下的生产接入和故障处置手册。数据库升级、恢复与数据保留见 [DATABASE_OPERATIONS.md](./DATABASE_OPERATIONS.md)，下游 payload 和幂等要求见 [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md)。

## 1. 运维边界

Lakehouse Flow 只负责生成、持久化和投递调度意图，并通过目标业务 snapshot 判断结果。运维时必须保持以下边界：

- `deliveryStatus` 只描述传输，`DELIVERY_EXHAUSTED` 不代表下游执行失败。
- `snapshotResult` 只描述是否出现可归因的目标业务 snapshot。
- `sourceHealth` 描述 snapshot 证据链是否可信，`SOURCE_BLOCKED` 时不能产生 `SNAPSHOT_NOT_ADVANCED`。
- Lakehouse Flow 不读取 Flink、Spark 或其他引擎的运行状态，不接受执行结果回调。
- 补数、重跑和恢复只控制尚未形成的调度意图。已经持久化的 intent 不会被伪装撤回。
- 一个物理表只能绑定一个 `writerJobKey`；START/RESTART 必须通过 writer epoch fencing 阻止旧 writer 继续提交。

## 2. 投产前检查

每次部署必须记录应用版本、Git commit、镜像 digest、数据库备份位置和当班操作人，并完成以下检查：

1. 使用 JDK 17 构建的制品，仓库验证命令为 `./mvnw clean verify -DskipITs`。
2. PostgreSQL 15 或已完成兼容性验证的更高版本可用，所有 scheduler 节点连接同一数据库和 schema。
3. `flyway_schema_history` 没有失败记录，目标制品包含的最高 migration 与发布清单一致。
4. 已按 [DATABASE_OPERATIONS.md](./DATABASE_OPERATIONS.md) 完成升级前逻辑备份和恢复抽检。
5. 所有 scheduler 节点使用相同的 source、投递通道、destination、时区和重试参数。
6. Paimon catalog、metastore 和对象存储凭据由 Secret 管理，不写入仓库、镜像或日志。
7. HTTP 消费端已实现 `intentKey` 幂等；数据库消费端还需持久化自己的递增游标。
8. Paimon writer 已接入 `lakehouse-flow-flink-paimon`，能够写入 intent 和 writer epoch 归因属性。
9. 常驻流 writer 的 checkpoint/savepoint 存储位于进程外部，RESTART 不会从 `LATEST` 跳过增量。
10. `/actuator/health`、`/actuator/prometheus` 和 `/api/v1/operations/*` 仅暴露给受信运维网络。

## 3. 生产配置

### 3.1 公共配置

数据库连接使用部署环境注入的 `SPRING_DATASOURCE_URL`、`SPRING_DATASOURCE_USERNAME` 和 `SPRING_DATASOURCE_PASSWORD`。生产环境不得使用仓库中的本地默认口令。

Flyway 生产默认必须保持：

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true
    baseline-on-migrate: false
    validate-on-migrate: true
    clean-disabled: true
```

`baseline-on-migrate` 只有在接管经过核验并已备份的历史 schema 时才能单次显式开启，不能用于绕过 migration 失败。

### 3.2 DATABASE_TABLE 投递

数据处理和作业控制使用独立表及独立通道配置：

```bash
export LAKEHOUSE_FLOW_INTENT_CHANNEL=DATABASE_TABLE
export LAKEHOUSE_FLOW_INTENT_DESTINATION=scheduling_intent
export LAKEHOUSE_FLOW_JOB_CONTROL_CHANNEL=DATABASE_TABLE
export LAKEHOUSE_FLOW_JOB_CONTROL_DESTINATION=job_control_intent
```

`DATABASE_TABLE` intent 与 delivery 在同一事务提交，delivery 立即为 `PUBLISHED`。执行面分别轮询 `scheduling_intent` 和 `job_control_intent`，只消费与 `DATABASE_TABLE` delivery 关联的记录。消费者必须：

1. 按 intent 主键递增读取，并在自己的数据库中持久化游标。
2. 以 `intent_key` 做最终幂等，游标不能替代幂等记录。
3. 在同一消费者事务中记录幂等键和本地接收结果。
4. 不修改 Lakehouse Flow 的 intent、delivery、task 或 workflow 表。
5. 不把本地执行状态写回 Lakehouse Flow。

一个基本的数据 intent 读取条件如下，生产消费者需使用参数化 SQL：

```sql
SELECT i.id, i.intent_key, i.contract_version, i.instruction_payload_json, i.created_at
FROM scheduling_intent i
JOIN scheduling_intent_delivery d ON d.scheduling_intent_id = i.id
WHERE i.id > :last_seen_id
  AND d.channel = 'DATABASE_TABLE'
  AND d.status = 'PUBLISHED'
ORDER BY i.id
LIMIT :batch_size;
```

`job_control_intent` 使用相同原则，通过 `job_control_intent_delivery.job_control_intent_id` 关联读取。

### 3.3 HTTP 投递

两个 intent 家族可以独立选择通道，但一条不可变 intent 只能选择一个 route：

```bash
export LAKEHOUSE_FLOW_INTENT_CHANNEL=HTTP
export LAKEHOUSE_FLOW_INTENT_DESTINATION=https://execution-plane.example/api/data-intents
export LAKEHOUSE_FLOW_JOB_CONTROL_CHANNEL=HTTP
export LAKEHOUSE_FLOW_JOB_CONTROL_DESTINATION=https://execution-plane.example/api/job-control-intents
```

生产 endpoint 必须使用 TLS，并在网关或服务网格完成受信身份校验。当前 LF-1.0 面向受信单团队环境，应用本身不提供租户级 RBAC。

HTTP 只有 2xx 被视为传输 ACK。下游必须使用 `Idempotency-Key` 或 `X-Lakehouse-Flow-Intent-Key` 去重，重复请求返回 2xx 和同一接收结果。响应 body 不参与 Lakehouse Flow 的结果判断。默认 claim、退避与重试参数位于 `application.yml`，调整前必须保证：

- `claim-lease` 长于单次 HTTP 请求的正常上界；
- `maximum-backoff` 不小于 `initial-backoff`；
- 重试总窗口不超过 intent 自身的 `deliverBefore`；
- 多 scheduler 节点配置完全一致。

系统不会在 HTTP 失败后自动切换到数据库或 MQ，以免同一 intent 产生两个独立消费入口。

## 4. Paimon snapshot source 接入

每张受管表配置独立 source 和 durable offset。首次普通接入使用 `LATEST`，只有明确的数据恢复才使用 `EARLIEST`：

```yaml
lakehouse-flow:
  snapshot-sources:
    scanner:
      enabled: true
      fixed-delay-ms: 10000
    reconciliation:
      enabled: true
      fixed-delay-ms: 60000
    paimon:
      enabled: true
      zone-id: Asia/Shanghai
      catalogs:
        - name: paimon-prod
          options:
            warehouse: s3://warehouse/paimon
            metastore: hive
          tables:
            - database: ods
              table: orders
              source-name: paimon-prod.ods.orders
              batch-size: 100
              startup-mode: LATEST
```

启用后按以下顺序验收：

1. 查询 `/api/v1/operations/snapshot-sources?sourceType=PAIMON&sourceName=paimon-prod.ods.orders`。
2. 确认 source 最终为 `HEALTHY`，且 earliest/latest、durable offset 和投影证据一致。
3. 进行一次真实业务数据提交，确认 `asset_state.latestDataSnapshotId` 推进。
4. 进行一次 compaction，确认只更新物理观察轨，不确认数据处理 intent。
5. 对一个测试 intent 提交带完整归因属性的 snapshot，确认结果为 `SNAPSHOT_CONFIRMED`。

禁止直接修改 `event_consumer_offset`、跳到 latest、伪造 `lakehouse_event` 或手工推进 `asset_state`。这些操作会破坏 source 对账和结果可信度。

## 5. Writer 注册、启动与重启

### 5.1 注册单 writer

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/writer-jobs' \
  -H 'Content-Type: application/json' \
  -d '{
    "writerJobKey": "writer.ods.orders",
    "tableAssetKey": "paimon-prod.ods.orders",
    "allowedProcessingModes": ["STREAMING"]
  }'
```

相同 `writerJobKey` 的重复请求必须与原绑定完全一致。同一 `tableAssetKey` 绑定第二个 writer 会被拒绝。

### 5.2 START_JOB

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/writer-jobs/writer.ods.orders/start' \
  -H 'Content-Type: application/json' \
  -d '{
    "requestKey": "start-writer-ods-orders-20260914",
    "requestedBy": "platform-operator",
    "reason": "initial production start"
  }'
```

执行面被动接收 `JobControlIntent`，根据 `writerJobKey` 解析真实 Flink 作业定义。delivery ACK 只表示收到 START；只有当前 epoch 的可归因业务 snapshot 才能形成 `SNAPSHOT_CONFIRMED`。

### 5.3 RESTART_JOB

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/writer-jobs/writer.ods.orders/restart' \
  -H 'Content-Type: application/json' \
  -d '{
    "requestKey": "restart-writer-ods-orders-20260914-01",
    "requestedBy": "platform-operator",
    "reason": "restore from external checkpoint"
  }'
```

RESTART 会分配更大的 `writerEpoch`。执行面必须先让旧 epoch 失去提交资格，再从外部 checkpoint/savepoint 恢复。`JdbcWriterEpochFence` 在 Paimon commit 期间锁定并校验 `writer_job_binding`；旧 epoch 提交被拒绝后，不能通过修改 binding 或降低 epoch 绕过。

使用 `GET /api/v1/job-control-intents/{intentKey}` 分别检查 delivery、source 和 snapshot 三类证据，不查询 Flink 状态来替代 snapshot 结论。

## 6. 日常巡检

```bash
curl --fail 'https://lakehouse-flow.example/actuator/health'
curl --fail 'https://lakehouse-flow.example/api/v1/operations/blockers?limit=100'
curl --fail 'https://lakehouse-flow.example/api/v1/operations/snapshot-sources?limit=100'
curl --fail 'https://lakehouse-flow.example/api/v1/scheduling-intent-deliveries/dead-letters?limit=100'
curl --fail 'https://lakehouse-flow.example/api/v1/job-control-intent-deliveries/dead-letters?limit=100'
```

巡检结论必须同时记录：对象标识、delivery 状态、snapshot 结果、source 健康、证据时间和最近一次变更。不得把任一维度折叠成通用“任务失败”。

统一 blockers 分类与处置入口：

| blockerType | 含义 | 首要检查 |
|---|---|---|
| `INPUT_SNAPSHOT` | 输入资产条件不满足 | Flow 依赖声明和上游资产事实 |
| `DAG_DEPENDENCY` | 同实例直接父节点未确认 | 父 task 的目标 snapshot 证据 |
| `DATE_CONCURRENCY` | 同一目标资产和日期已有发布槽 | 冲突 intent、确认结果和准入租约 |
| `INTENT_PUBLICATION` | intent 尚未形成 | outbox scanner、writer binding、版本并发策略 |
| `TARGET_SNAPSHOT` | 已发布并等待目标 snapshot | baseline、归因属性和确认窗口 |
| `SOURCE_BLOCKED` | source 证据链不可信 | source 对账详情和 catalog 保留范围 |
| `DELIVERY_EXHAUSTED` | 传输尝试已耗尽 | destination、网络、2xx ACK 和幂等实现 |

## 7. 故障处置

### 7.1 SOURCE_BLOCKED

1. 使用 snapshot source API 按 source 或目标资产定位 `sourceHealthDetail` 和 `checkedAt`。
2. 检查 Paimon catalog 可用性、snapshot earliest/latest、对象存储权限和 metastore 连通性。
3. 对 `LAGGING` 或可修复投影问题，保持 scanner 和 reconciliation 开启，让系统从 durable offset 的严格后继连续补扫。
4. 对 `RETENTION_GAP`、`OFFSET_AHEAD` 或缺失 durable event，确认系统已让依赖该 source 的 snapshot 结果保持待定并保留证据，不能手改 offset。
5. 若历史 snapshot 已不可恢复，从受控备份恢复 catalog 或重新建立新 source 身份并走变更评审。不得在原 source 上伪造连续性。
6. source 回到 `HEALTHY` 后，先确认 offset、event 和 AssetState 对账一致，再执行 snapshot recheck 或新的重跑/补数 action。

`SOURCE_BLOCKED` 期间 snapshot 结果保持待定。它不证明下游没有完成写入。

### 7.2 DELIVERY_EXHAUSTED

1. 从对应 dead-letter API 保存 `intentKey`、channel、destination、attemptCount、lastError 和 `deadLetteredAt`。
2. 修复 DNS、TLS、认证、endpoint、数据库消费者或 2xx ACK 行为，并确认下游仍按 `intentKey` 幂等。
3. 不直接把 `EXHAUSTED` 改回 `PENDING`，否则会绕过审计和 fencing。
4. 数据处理 intent 使用新的幂等 action key 发起 rerun 或 backfill，生成新的 intent；原死信永久保留为证据。
5. JobControlIntent 使用新的 request key 发起 RESTART，分配新 epoch；不得重放已过期的旧 START/RESTART。
6. 如果目标 snapshot 已经由原 intent 推进，先核对归因证据，不要仅因为 delivery 死信重复生产数据。

### 7.3 SNAPSHOT_NOT_ADVANCED

该结果只能在确认窗口结束且 source 为 `HEALTHY` 时产生。先核对 intent 的 baseline、目标资产、业务日期和 required snapshot properties，再决定使用 recheck、rerun 或 backfill。不能把它解释为 Flink/Spark 任务失败，也不能通过手工推进 AssetState 关闭事件。

### 7.4 Scheduler 进程中断或节点丢失

1. 保留至少一个连接同一 PostgreSQL 的 scheduler 节点。
2. 不清理 `PUBLISHING` claim；等待 `claimExpiresAt` 后由其他节点重占。
3. 迟到 ACK 会因 fencing token 不匹配被拒绝。
4. source ingestion 事务回滚后会从 durable offset 重放；不得手工越过失败 snapshot。
5. 恢复后检查 delivery backlog、source health、target admission 和 snapshot confirmation 扫描是否继续推进。

## 8. 补数暂停、恢复与取消

创建补数后先保存 `actionKey` 和返回的 `backfillBatchId`。按 action 查询批次详情：

```bash
curl 'https://lakehouse-flow.example/api/v1/scheduling-actions/{actionKey}'
curl 'https://lakehouse-flow.example/api/v1/backfills/{backfillBatchId}/items'
```

暂停只阻止尚未形成 intent 的后续 item：

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/scheduling-actions/pause-backfill' \
  -H 'Content-Type: application/json' \
  -d '{"backfillBatchId":81,"actionKey":"pause-81-01","requestedBy":"operator-a","reason":"source maintenance"}'
```

恢复使用新的 action key：

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/scheduling-actions/resume-backfill' \
  -H 'Content-Type: application/json' \
  -d '{"backfillBatchId":81,"actionKey":"resume-81-01","requestedBy":"operator-a","reason":"source healthy"}'
```

取消同样不会撤回已持久化 intent：

```bash
curl -X POST 'https://lakehouse-flow.example/api/v1/scheduling-actions/cancel-backfill' \
  -H 'Content-Type: application/json' \
  -d '{"backfillBatchId":81,"actionKey":"cancel-81-01","requestedBy":"operator-a","reason":"operator stop"}'
```

snapshot 未推进的批次恢复必须创建替代批次。默认使用 `FULL_SCOPE`；只有唯一失败节点且其下游范围依赖闭合时才使用 `FAILED_NODE_CASCADE`。恢复批次继续遵守父节点 snapshot、日期槽和单 writer 约束。

## 9. 禁止操作

- 不执行 `flyway clean`，不修改已应用 migration 的文件或 checksum。
- 不直接更新 intent、delivery、task、workflow、offset、AssetState、writer binding 或 epoch 来“恢复状态”。
- 不根据 HTTP 2xx、数据库消费游标或 Flink Job 状态写入 snapshot 成功。
- 不复用已经代表不同请求内容的 `actionKey`、`requestKey` 或 `intentKey`。
- 不把 Paimon compaction、analyze 等维护 snapshot 当作业务推进。
- 不在同一物理表上并发启动第二个 writer。
- 不使用 source `EARLIEST` 替代正式补数。

## 10. 投产完成标准

投产只有同时满足以下条件才算完成：

1. 所有应用实例健康，Flyway 到达发布清单指定版本。
2. 两类 intent 的选定通道已完成一次真实接收和幂等重投验证。
3. 所有受管 source 为 `HEALTHY`，没有 retention gap 或 projection inconsistency。
4. START/RESTART 的旧 epoch fencing 已在执行面验证。
5. 一条真实业务 snapshot 能确认对应 intent，compaction 不会误确认。
6. blockers、source 对账、死信和 Prometheus 指标可从受信运维网络访问。
7. 回滚制品、数据库备份、恢复命令和当班责任人已记录。
