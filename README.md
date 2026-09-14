# Lakehouse Flow

[![Maven Build and Test](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/maven-build.yml/badge.svg?branch=master)](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/maven-build.yml)
[![Code Quality Checks](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/code-quality.yml/badge.svg?branch=master)](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/code-quality.yml)

Lakehouse Flow 是一个面向 CDC 湖仓的 **snapshot 推进式调度原型**。它的核心目标是把调度判断从固定 cron 时间推进到“数据资产版本已经到达且状态满足条件”。

当前仓库还不是生产就绪系统。它已经具备领域模型、PostgreSQL/Flyway 表结构、格式无关的 snapshot source SPI、Paimon Catalog API 适配器、原子事件投影、资产状态单调推进、FlowPlan 组合依赖评估、DAG snapshot 门禁、snapshot 进展确认、触发审计、action/snapshot 证据联查、最小 REST API，以及由 Lakehouse Flow 主动发布的数据库、HTTP 或 MQ 调度意图；外部投递已具备 claim 租约、fencing、退避重试和死信审计。`LF-1.0` 将面向单团队受信环境，以 Flink CDC 持续流式写入 ODS、DWD/DWS/ADS 流批一体 writer-side adapter、真实 Paimon 闭环、`DATABASE_TABLE + HTTP` 投递和整体 Testcontainers E2E 作为发布门槛。任务执行、资源队列、执行器适配和下游结果回调明确不属于 Lakehouse Flow 的职责。完整 1.0 范围与验收状态以 [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) 为准。

## 一句话边界

Lakehouse Flow 当前应该回答：

```text
某个湖仓资产的 snapshot 是否已经推进到目标版本？
资产状态是否满足 snapshot/watermark/quality/schema 条件？
这些判断能否被审计和幂等记录？
```

它当前不应该声称已经负责：

```text
真正执行任务、跟踪任务运行状态、资源调度、执行器回调、生产级队列消费。
```

## 当前实现状态

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| Maven 多模块骨架 | 已实现 | Java 17，Spring Boot 3.2，多模块工程 |
| PostgreSQL schema | 已实现 | Flyway SQL 使用 PostgreSQL JSONB；当前不支持 MySQL |
| LakehouseEvent | 已实现 | 原始湖仓事件，按 `event_id` 去重 |
| AssetState | 已实现 | 物理观察与业务数据 snapshot 双轨单调推进；维护提交只更新表级观察轨，数据提交才投影 `changedPartitions` 分区状态 |
| Lakehouse snapshot source SPI | 已实现基础版 | source/provider/identity/offset ordering 均与湖格式解耦；同一摄取循环可挂载 Paimon、Iceberg、Hudi 等适配器 |
| Paimon snapshot source | 实现待 E2E | 使用 Paimon 1.3 Catalog API 顺序读取真实 snapshot properties，并从 delta manifests 推导 `changedPartitions`；真实 catalog 环境尚待整体 E2E 验证 |
| EventIngestionService | 基础版 | 动态扫描全部已配置 source；事件、表/分区状态、来源路由、FlowPlan 决策和格式独立 offset 在同一事务提交 |
| 条件与触发评估 | 基础版 | `FlowPlanConditionService` 支持 AND/OR 分组；`FlowPlanEvaluationService` 从已发布版本生成完整 DAG 调度意图 |
| SnapshotProgressService | 基础版 | 捕获目标资产 publication baseline；资产级 latest 推进不再直接确认共享写入结果 |
| SnapshotEvidenceService | 基础版 | 在 baseline 后的事件序列中按 intent 属性、格式适配器给出的 `dataChange` 和目标分区匹配 snapshot |
| SnapshotTriggerRoutingService | 基础版 | 补数/恢复/重跑 snapshot 只推进所属实例，不进入全局自然触发；非法和维护 snapshot 失败关闭 |
| SnapshotConfirmationScanner | 基础版 | 周期扫描 `SCHEDULED` 任务，只用可归属的目标 snapshot 证据确认结果 |
| TriggerHistory | 基础版 | 支持结构化 JSONB 审计 payload |
| FlowPlan/Node | 基础版 | 支持版本化 DAG、发布前图校验、输入依赖和日期模板化目标资产 |
| SchedulingAction | 基础版 | 支持 workflow 重跑、task instance 重跑、published FlowPlan node 重跑、统一批次化的完整 Flow/Node 子图补数、全范围或单失败节点级联恢复、安全整日跳过、补数批次暂停/恢复交付/取消、取消、跳过和 snapshot 重检 |
| REST API | 基础版 | 暴露 FlowPlan/Node、action command/query、instance evidence、已发布 scheduling intent 审计和 backfill query/control API；不存在供下游抢任务的 ready/claim/deliver API |
| SchedulingIntent Delivery | 基础版 | Lakehouse Flow 内部扫描 READY 决策，冻结 baseline，并按单一选定路由写入版本化完整指令；数据库立即可见，HTTP 由 Java 17 client 主动调用，MQ 通过 broker-neutral gateway 发布，外部投递支持租约、fencing、退避和死信审计 |
| Target-Date Admission | 基础版 | 正常、补数、恢复和重跑共享 `targetAssetKey + bizDate` 持久化发布槽位；冲突任务保持 READY，snapshot 确认或发布准入租约过期后释放；它尚不提供单表 writer 唯一绑定 |
| 补偿/运维视图 | 基础版 | 已有 snapshot 确认扫描、补数批次查询，以及按 Flow/版本/节点筛选和按 action key 展开的 snapshot 证据联查；UI 仍未实现 |
| 执行器 | 不属于职责 | 本项目只做调度，不提交、运行或跟踪外部任务 |

## 核心流程

```text
LakehouseSnapshotScanner
  -> LakehouseSnapshotSourceProvider[*]
     -> PaimonSnapshotSource / future IcebergSnapshotSource / HudiSnapshotSource
  -> EventIngestionService
  -> SnapshotIngestionTransactionService
  -> lakehouse_event + table/partition asset_state
  -> SnapshotTriggerRoutingService
     -> action-owned: owning intent only
     -> natural: FlowPlanEvaluationService
  -> event_consumer_offset
  -> WorkflowInstance + TaskInstance + TriggerHistory
  -> internal SchedulingIntentOutboxScanner
  -> scheduling_target_admission(targetAssetKey + bizDate)
  -> writer_job_binding(tableAssetKey -> writerJobKey, LF-1.0 target)
  -> scheduling_intent + scheduling_intent_delivery
  -> SchedulingIntentDeliveryScanner (HTTP/MQ claim + push + retry)
  -> downstream polls table / receives HTTP call / consumes MQ
  -> downstream logical-completion snapshot carries required intent properties
  -> SnapshotConfirmationScanner / SnapshotEvidenceService
  -> DagProgressionService
  -> next READY_TO_SCHEDULE intent

platform START_JOB / RESTART_JOB (LF-1.0 G20 target)
  -> writer_job_binding + next writerEpoch
  -> independent job_control_intent + delivery
  -> platform execution plane operates configured engine (Flink in LF-1.0)
  -> target data snapshot carries writer-generation properties
```

关键约束：

- 事件是证据，`asset_state` 才是调度判断的事实来源。
- snapshot ID 字段以字符串存储，但比较时数字 ID 使用数字顺序，避免 `"99" > "100"` 这类字典序误判。
- 摄入 offset 只推进到连续成功处理的 snapshot；中间 snapshot 失败时停止推进，下一轮重试。
- 调度结果只由受管目标资产中可归属于本次 intent 的 snapshot 推进确认，不读取、不接收也不依赖下游任务结果回调。
- `SchedulingIntent` 是 Lakehouse Flow 主动发布的不可变指令；baseline 在首次发布前冻结，完整 `instructionPayload` 对数据库、HTTP 和 MQ 保持一致。
- 正常、补数、恢复和重跑首次发布前共享目标日期准入槽；互斥只控制 Lakehouse Flow 的意图发布，不声称锁住或停止下游执行。
- 一个 FlowPlan DAG 可以混合流式和批式节点，但同一规范化物理表只能绑定一个 `writerJobKey`。平台启动/重启该作业时创建独立 `JobControlIntent` 并推进 `writerEpoch`，执行侧负责 fencing 旧 epoch；它不伪造 task/workflow/bizDate，当前代码尚未实现这条 G20 约束。
- Flow/Node 只知道 `processingMode=STREAMING|BATCH`，不保存 Flink、Spark 或其他执行引擎类型。流式父节点通过持续产出的资产 snapshot/watermark 为下游提供依赖证据，批式父节点通过同一调度实例的已确认输出提供证据；汇聚下游必须等待全部父边和自身依赖。
- 发布版本策略会决定节点确认窗口、目标日期租约和版本活跃 workflow 上限；达到并发上限的任务保持 READY，不产生 intent，也不提前冻结 baseline。
- 下游必须把 `requiredSnapshotProperties` 原样写入本次逻辑输入边界完成时的数据 snapshot；湖格式适配器把原生 operation 映射为 typed `dataChange`。维护 snapshot 只推进 source offset 和表级 `latestSnapshotId`，不推进 `latestDataSnapshotId`、分区状态、自然触发或 task 确认。
- 补数、恢复和重跑 snapshot 会更新真实资产事实并确认所属 intent，但不会额外创建正常 `SNAPSHOT_DRIVEN` workflow；正常 intent 的逻辑完成 snapshot 和外部数据提交仍可驱动自然调度。
- `changedPartitions` 会形成独立分区状态，历史分区补数不会推进当前日期的分区资产键。
- 传输 ACK 只作为投递证据，不能放行 DAG 或确认任务结果。完整协议见 [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md)。
- `PENDING/PUBLISHING/RETRY_WAIT/PUBLISHED/EXHAUSTED` 只属于 delivery；其中 `EXHAUSTED` 对外表达为 `DELIVERY_EXHAUSTED`，是传输死信，不会写成 task 执行失败，也不会替代 snapshot 结果。
- source 对账 `BLOCKED` 对外表达为 `SOURCE_BLOCKED`。source 证据未追平或存在缺口时不得把确认超时写成 `SNAPSHOT_NOT_ADVANCED`，snapshot 结果必须保持待定。
- 补数只允许显式 `startNode` 绕过其 DAG 上游；级联下游必须等同一业务日期内所有直接父节点的目标 snapshot 已确认推进。
- 级联范围遇到缺失父节点的汇聚节点时直接拒绝，不能把缺依赖的下游意图提前交付。
- 补数暂停/取消只阻断尚未交付的调度意图；已交付意图不会被伪装成已撤回。
- 补数失败恢复保留原批次证据，从最早 snapshot 未推进日期创建单一替代批次；默认 `FULL_SCOPE` 重建原范围，显式 `FAILED_NODE_CASCADE` 只允许从唯一失败节点开始并选择依赖闭合的可达下游。多失败节点或缺父 join 会拒绝并要求全范围恢复。
- Node 补数默认不会跳过历史日期；显式启用安全跳过后，也只有全部选中节点都存在同版本、同日期、同节点和同目标资产的 snapshot 确认证据时才省略整个日期。历史证据不参与新实例 DAG 放行。
- 完整 Flow 与 Node 子图补数共享 `BackfillBatch` 生命周期；完整 Flow 以全部根节点作为日期入口，Node 子图仅以用户选中节点作为入口，批次会冻结入口节点和选中节点集合供查询与恢复。
- 触发审计 payload 必须是合法 JSONB，而不是 Java 对象的 `toString()`。

## 本地运行

### 前置要求

- Java 17+
- `./mvnw`（会下载 Maven 3.9.9，并读取 `.mavenrc` 使用 JDK 17）
- Docker / Docker Compose
- PostgreSQL 15 或兼容版本

### 启动数据库

```bash
docker compose up -d postgres
```

连接信息：

```text
url      jdbc:postgresql://localhost:5432/lakehouse_flow
user     postgres
password postgres
```

### 构建与测试

```bash
./mvnw clean verify
```

默认分支 `master` 的 GitHub Actions 使用 JDK 17 和 Maven Wrapper 执行同一条命令。Service 模块在 `verify` 阶段强制要求 line coverage >= 90%、branch coverage >= 65%；校验成功后才触发 Boot JAR 构建产物 workflow。当前 service 及跨层协作行为以 Mockito 单元测试为主，纯模型行为使用 JUnit，并要求 service 每个 public 方法至少有直接测试入口。Testcontainers 用于 `LF-1.0` Lakehouse Flow 系统级 E2E，从 API/事件入口贯穿 PostgreSQL/Flyway、DB/HTTP 投递、Flink/Paimon 写读、snapshot 确认、DAG 与补数推进；它不归属于某个单独模块，也不替代当前单元测试。

Lakehouse Flow 内部扫描 READY 决策并写入 `scheduling_intent`。每条 intent 选择一个路由：`DATABASE_TABLE` 供下游轮询专用表，`HTTP` 由内部 publisher 主动 POST，`MQ` 由内部 publisher 调用部署提供的 `SchedulingIntentMessageGateway`。下游始终被动接收，不调用 Lakehouse Flow 抢占任务。REST 接口仅供审计：

下游消费 `instruction_payload_json`，并按 [Outbound Intent 下游契约](./SCHEDULING_INTENT_CONTRACT.md) 将其中的 `requiredSnapshotProperties` 写入逻辑完成目标 snapshot。Flow 只传递流/批模式与冻结输入向量；具体 checkpoint、job-end 或其他提交钩子由执行适配器解释，两者使用统一调度和确认模型。

```bash
curl 'http://localhost:8080/api/v1/scheduling-intents/tasks/42'
```

启用 Paimon source 时配置原生 catalog options 和受管表；每张表拥有独立 offset。默认关闭，避免未配置 catalog 时启动扫描：

```yaml
lakehouse-flow:
  snapshot-sources:
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
              batch-size: 100
              startup-mode: LATEST
```

`options` 原样交给 Paimon Catalog；对象存储和 metastore 的认证材料应由运行环境提供，不写入仓库配置。

选择 HTTP 投递时配置 endpoint；选择 MQ 时配置 topic，并由部署模块提供具体 broker gateway：

```yaml
lakehouse-flow:
  scheduling-intent-delivery:
    channel: HTTP
    destination: https://scheduler-consumer.example/api/intents
    publisher:
      claim-lease: PT30S
      max-attempts: 8
      initial-backoff: PT1S
      maximum-backoff: PT5M
```

HTTP 只把 2xx 视为传输 ACK；MQ gateway 正常返回只表示 broker 接受。publisher 采用至少一次投递，`intentKey` 是下游消费幂等键。超过 `publicationAdmission.leaseExpiresAt` 的意图不再向外发布并进入传输死信，但 task 的结果判断仍只取决于是否出现可归属的目标 snapshot。

调度决策、投递与 snapshot source 可观测性通过 Actuator 暴露：

```bash
curl http://localhost:8080/actuator/prometheus
curl 'http://localhost:8080/api/v1/scheduling-intent-deliveries/dead-letters?channel=MQ&limit=100'
```

核心指标包括 `lakehouse_flow_scheduling_decision_plan_inspections_total`、`lakehouse_flow_scheduling_decision_trigger_evaluations_total`、`lakehouse_flow_scheduling_decision_trigger_evaluation_duration_seconds`、`lakehouse_flow_scheduling_backlog_task_instances`、`lakehouse_flow_scheduling_backlog_backfill_blocked_items`、`lakehouse_flow_scheduling_intent_delivery_publisher_attempts_total`、`lakehouse_flow_scheduling_intent_delivery_records`、`lakehouse_flow_snapshot_source_scans_total`、`lakehouse_flow_snapshot_source_offsets_pending`、`lakehouse_flow_snapshot_source_retention_gap` 和 `lakehouse_flow_snapshot_source_projection_inconsistent`。调度决策 metric 只使用检查/决策结果等低基数 tag，Flow、asset、snapshot 和 trigger 标识留在结构化日志与 `TriggerHistory`。task backlog 只统计 `CREATED`、`WAITING_SNAPSHOT`、`READY_TO_SCHEDULE`、`SCHEDULED` 四个非终态，其中 `SCHEDULED` 表示意图已发布并等待目标 snapshot；补数阻塞只使用 `DATE_CONCURRENCY` 和 `DAG_DEPENDENCY`。delivery 指标只表示传输状态；`EXHAUSTED` 仍不是 task 失败。

Prometheus 初始告警基线如下，投产后应按扫描周期、Flow 数量和确认窗口校准：

1. P1：5 分钟内 `decision="failed"` 增量大于 0，表示调度评估事务失败关闭。
2. P1：`retention_gap` 或 `projection_inconsistent` 连续 5 分钟大于 0，表示 source 事实或 AssetState 投影不可信。
3. P2：任一 `status="EXHAUSTED"` delivery 连续 15 分钟大于 0，表示调度意图已进入传输死信。
4. P2：`state="READY_TO_SCHEDULE"` backlog 连续 15 分钟大于 0，优先检查 publisher、版本并发上限和目标日期准入。
5. P2：`state="SCHEDULED"` backlog 连续超过确认超时的 75% 仍大于 0；默认 `PT1H` 下可先取 45 分钟，检查下游提交及 snapshot source。

`BLOCKED_CONDITION`、`DATE_CONCURRENCY` 和 `DAG_DEPENDENCY` 在正常调度中可以非零，默认只用于趋势和排障，不以单次出现报警。它们持续增长时，应结合部署容量与 Flow SLO 设置绝对值或增长率阈值。

source reconciliation 周期性核对湖表 earliest/latest、durable offset、最新物理事件/状态和最新业务数据事件/状态。`UNINITIALIZED` / `LAGGING` 通过正常连续摄入补偿；物理或数据投影缺失/漂移时重放对应 durable event；`RETENTION_GAP`、`OFFSET_AHEAD`、durable offset 缺少事件或无事件支撑的状态等情况进入 `BLOCKED` 并告警，不允许自动跨越历史或构造 snapshot。

`startup-mode` 默认 `LATEST`，首次接入只摄入最新 snapshot 并建立一次当前事实；`EARLIEST` 会显式回放仍被保留的历史 snapshot，可能触发历史业务日期，只用于明确的数据恢复场景。常规历史补数应使用 `BackfillBatch`，不要依赖 source 回放。

Paimon source 读取 snapshot properties，但普通 Paimon SQL/commit 链路不能假定会自动写入 Lakehouse Flow 的单次意图属性。`LF-1.0` 使用下游侧 Flink/Paimon 流批一体 writer-side adapter：批式在有界输入结束提交时注入 `requiredSnapshotProperties`，流式在 checkpoint 已覆盖 intent 冻结的 input snapshot/watermark vector 时注入。`lakehouse-flow.final=true` 表示逻辑 intent 完成，不表示流作业结束。上线前分别用流式和批式探针验证属性、分区和 `dataChange` 可回读。只配置 source 而没有写入扩展时，外部 snapshot 仍可驱动资产状态，Lakehouse Flow 下发的 intent 则不会被误判为成功。

Action 审计查询示例：

```bash
curl 'http://localhost:8080/api/v1/scheduling-actions?workflowCode=flow.orders&limit=50'
curl 'http://localhost:8080/api/v1/scheduling-actions/backfill-node-20260910'
```

列表接口只返回 action 摘要；按 key 的详情接口会联查相关 `BackfillBatch`、有序 `BackfillItem` 和 task target snapshot 证据。响应中的 `action.status` 只表示调度命令是否应用，`snapshotAdvanced` 才表示目标资产是否相对 publication baseline 推进。

失败节点级联恢复示例：

```bash
curl -X POST 'http://localhost:8080/api/v1/scheduling-actions/recover-backfill' \
  -H 'Content-Type: application/json' \
  -d '{
    "backfillBatchId": 81,
    "recoveryStrategy": "FAILED_NODE_CASCADE",
    "actionKey": "recover-backfill-81-attempt-1",
    "requestedBy": "operator-a",
    "reason": "retry the single snapshot-failed branch"
  }'
```

省略 `recoveryStrategy` 时兼容使用 `FULL_SCOPE`。补数专题的功能与质量退出标准见 `PHASE2_PROGRESS.md`。

### 启动应用

```bash
cd lakehouse-flow-boot
../mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

或者从根目录打包后运行：

```bash
./mvnw clean package -DskipTests
source .mavenrc
"$JAVA_HOME/bin/java" -jar lakehouse-flow-boot/target/lakehouse-flow-boot-0.1.0-SNAPSHOT.jar
```

健康检查：

```bash
curl --noproxy '*' http://localhost:8080/actuator/health
```

## 模块说明

| 模块 | 职责 |
| --- | --- |
| `lakehouse-flow-common` | 共享工具，例如 snapshot ID 比较 |
| `lakehouse-flow-model` | JPA 实体和值对象 |
| `lakehouse-flow-dao` | Spring Data Repository 与 Flyway schema migrations |
| `lakehouse-flow-service` | 资产状态、事件、条件评估、snapshot 进展确认、实例、action 联查和触发审计服务 |
| `lakehouse-flow-integration` | 格式无关 snapshot source SPI、通用摄取、source 对账补偿、指标与 Paimon Catalog 适配器 |
| `lakehouse-flow-api` | FlowPlan/Node、action command/query、instance evidence、intent 和 delivery 死信只读审计 API |
| `lakehouse-flow-scheduler` | 内部 intent 创建、HTTP/MQ 可靠投递、delivery 指标和 snapshot 确认扫描循环 |
| `lakehouse-flow-test` | `LF-1.0` 系统级 E2E 的测试装配入口；覆盖完整应用链路，不限定为本模块局部测试 |
| `lakehouse-flow-boot` | Spring Boot 启动模块和应用配置 |

## 下一步建议

1. G16 source-aware snapshot 超时判定与 G19 流批混合 input snapshot/watermark vector 已完成代码和单测。
2. 下一步增加 `tableAssetKey -> writerJobKey` 唯一绑定、writer epoch，以及不绑定 task 的平台 `JobControlIntent(START_JOB|RESTART_JOB)` 与可靠投递。
3. 实现 Flink/Paimon 参考 writer-side adapter，让流式 checkpoint 与批式结束提交都能写入可归因的 intent 和 writer epoch 属性。
4. 打通业务库、Flink CDC 流式 ODS、平台作业启动/重启、DWD/DWS/ADS 混合流批 DAG、DB/HTTP、真实 Paimon、重跑和补数的整体 E2E。
5. 完成 PostgreSQL 多 scheduler 锁竞争、中断、claim 重占、fencing、事务回滚和 offset 恢复验收。
6. 补齐阻塞原因和 source 对账的最小运维 API，随后冻结 1.0 REST、intent 和 migration 契约。
7. 容量基线放到真实生产负载下采集；在此之前不承诺未经测量的 SLA。
