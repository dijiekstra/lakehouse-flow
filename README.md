# Lakehouse Flow

[![Maven Build and Test](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/maven-build.yml/badge.svg?branch=master)](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/maven-build.yml)
[![Code Quality Checks](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/code-quality.yml/badge.svg?branch=master)](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/code-quality.yml)

Lakehouse Flow 是面向 CDC 湖仓的 snapshot 推进式调度系统。它观察受管数据资产的 snapshot，在依赖满足时发布调度意图，并以目标业务 snapshot 是否推进作为结果证据。

> 调度意图（Intent）是发给外部平台的不可变指令，表达“需要推进哪个目标资产”。它不是任务，也不表示作业已经成功。术语解释见 [GLOSSARY.md](./GLOSSARY.md)。

## 系统边界

Lakehouse Flow 负责：

- 观察 Paimon 等湖表的 snapshot、watermark 和分区变化。
- 维护单调的资产状态并评估 Flow DAG 依赖。
- 发布普通调度、补数、重跑和 writer 启停意图。
- 用可归因的目标业务 snapshot 确认结果。
- 提供互斥、幂等、死信、source 对账和运维证据。

Lakehouse Flow 不负责：

- 执行 Flink、Spark 或其他计算作业。
- 管理资源队列和执行器生命周期。
- 通过下游 `RUNNING/SUCCESS/FAILED` 回调判断结果。
- 保证外部系统只执行一次；下游必须按 `intentKey` 幂等消费。

## 核心模型

```text
FlowPlan -> FlowPlanVersion -> ScheduleNode DAG
                                  |
snapshot source -> AssetState -> dependency evaluation
                                  |
                         WorkflowInstance
                                  |
                           TaskInstance
                                  |
                    SchedulingIntent / JobControlIntent
                                  |
                     DATABASE_TABLE / HTTP / MQ SPI
                                  |
                         external execution platform
                                  |
                    attributed target data snapshot
                                  |
                       SnapshotConfirmationService
```

一个 Flow 可以混合流式和批式 Node，但不保存 Flink、Spark 等引擎类型：

- `STREAMING` Node 的 writer 由平台启动或重启，持续产生 snapshot。
- `BATCH` Node 在依赖满足后收到一次调度意图，完成有界处理并产生目标 snapshot。
- 同一物理表只能绑定一个受管 writer；`writerEpoch` 隔离重启前后的 writer generation。
- 批式下游必须等待同一实例的全部直接父 Node 产生可归因目标 snapshot。

## 结果语义

| 结果 | 含义 |
| --- | --- |
| `SNAPSHOT_CONFIRMED` | 发现目标匹配、产生业务数据变化且能归因到本次意图的 snapshot |
| `SNAPSHOT_NOT_ADVANCED` | source 健康，但确认窗口内没有目标 snapshot 证据 |
| `DELIVERY_EXHAUSTED` | 意图传输重试耗尽，不代表下游执行失败 |
| `SOURCE_BLOCKED` | source 缺口、不可用或投影不可信，不能推断任务失败 |

Compaction 可以推进物理 snapshot，但不能确认任务或触发下游。外部作业写入同一表也不能确认 Lakehouse Flow 意图，除非它遵循 writer-side contract 并携带完整归因属性。

## LF-1.0 状态

LF-1.0 面向单团队受信环境：

- 真实 Paimon writer-side adapter 和 Flink CDC/批处理闭环已实现。
- PostgreSQL 多 scheduler 协调、事务恢复、租约重占和 migration 已纳入整体 E2E。
- 正式投递方式为 `DATABASE_TABLE` 和 `HTTP`；MQ 保留 broker-neutral SPI。
- `/api/v1`、两类 intent schema、下游幂等与 migration 策略已有冻结基线。
- 本地与 CI 发布候选门禁已通过；最终 `1.0.0` 仍需受信环境试运行和 runbook 演练。
- 容量和 SLA 基线留到真实生产负载中测量，不提前承诺。

当前进度、剩余门禁和最近验证快照以 [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) 为准。

## 快速开始

环境要求：

- JDK 17
- Docker / Docker Compose
- 项目自带 Maven Wrapper

启动 PostgreSQL：

```bash
docker compose up -d postgres
```

构建并运行默认验证：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw clean verify -DskipITs
```

启动应用：

```bash
cd lakehouse-flow-boot
JAVA_HOME=$(/usr/libexec/java_home -v 17) ../mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

健康检查：

```bash
curl --noproxy '*' http://localhost:8080/actuator/health
```

完整配置和平台接入步骤见 [QUICKSTART.md](./QUICKSTART.md) 与 [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md)。

## 验证

日常单元测试和质量门禁：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw clean verify -DskipITs
```

LF-1.0 整体 Testcontainers E2E：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw -pl lakehouse-flow-e2e -am verify
```

E2E 使用真实 PostgreSQL、MySQL、Flink JobManager/TaskManager 和 Paimon warehouse，订单链路为：

```text
MySQL orders -> Flink CDC ODS -> Flink batch DWD -> DWS -> ADS GMV
```

真实 Flink 链路由测试执行端被动接收 HTTP 意图后启动作业，并且不回传执行状态。数据库通道当前验证两类意图的原子可见、载荷和投递记录；数据库轮询消费者启动真实 Flink 作业列为后续补强。完整场景矩阵见 [E2E_TEST_CASES.md](./E2E_TEST_CASES.md)。

## 文档导航

| 主题 | 文档 |
| --- | --- |
| 文档真源与阅读顺序 | [DOCS_INDEX.md](./DOCS_INDEX.md) |
| 中文术语 | [GLOSSARY.md](./GLOSSARY.md) |
| 系统架构 | [ARCHITECTURE.md](./ARCHITECTURE.md) |
| Flow、Node、实例、补数与重跑模型 | [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) |
| 下游意图与 snapshot 归因契约 | [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) |
| API、schema 与 migration 兼容 | [LF1_COMPATIBILITY.md](./LF1_COMPATIBILITY.md) |
| 整体 E2E 用例 | [E2E_TEST_CASES.md](./E2E_TEST_CASES.md) |
| 参与开发 | [CONTRIBUTING.md](./CONTRIBUTING.md) |
| 开发指南 | [DEVELOPMENT.md](./DEVELOPMENT.md) |
| 运维与故障处置 | [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md) |
| 数据库运维 | [DATABASE_OPERATIONS.md](./DATABASE_OPERATIONS.md) |
| 指标与告警 | [OBSERVABILITY.md](./OBSERVABILITY.md) |
| 发布检查 | [RELEASE_CHECKLIST.md](./RELEASE_CHECKLIST.md) |

## 模块

| 模块 | 职责 |
| --- | --- |
| `lakehouse-flow-common` | 共享契约与 snapshot 比较工具 |
| `lakehouse-flow-model` | 领域实体和值对象 |
| `lakehouse-flow-dao` | Spring Data Repository 与 Flyway migration |
| `lakehouse-flow-service` | 资产、依赖、实例、意图、补数、确认和运维服务 |
| `lakehouse-flow-integration` | 格式无关 snapshot source SPI、摄取、对账和 Paimon source |
| `lakehouse-flow-flink-paimon` | 下游 Paimon writer adapter、归因和 epoch fencing |
| `lakehouse-flow-api` | 配置、Action、证据与运维 REST API |
| `lakehouse-flow-scheduler` | 扫描、准入、可靠投递和 snapshot 确认循环 |
| `lakehouse-flow-test` | 跨模块共享测试夹具 |
| `lakehouse-flow-boot` | Spring Boot 启动与运行配置 |
| `lakehouse-flow-e2e-jobs` | E2E 使用的真实 Flink/Paimon 作业 JAR |
| `lakehouse-flow-e2e` | 整体 Testcontainers 验收套件 |

## 许可证

本项目使用 [Apache License 2.0](./LICENSE)。
