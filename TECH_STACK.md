# Lakehouse Flow 技术基线

**最后核对**: 2026-09-14

本文只记录当前仓库实际采用的技术与明确缺口。依赖版本以根 `pom.xml` 为唯一事实来源，阶段目标以 `PHASE2_PROGRESS.md` 为准。

## 架构边界

Lakehouse Flow 是独立的 snapshot 推进式调度决策系统。技术选型服务于以下链路：

```text
湖仓 snapshot -> 持久化事件 -> AssetState -> FlowPlan/DAG 决策
               -> SchedulingIntent -> 被动式下游接收
               -> 可归因的目标业务 snapshot -> 结果确认

平台 START/RESTART -> JobControlIntent -> 被动式执行面接收
                    -> writer epoch 业务 snapshot -> 世代证据
```

仓库不包含 executor、Worker、资源队列、外部作业状态轮询或结果回调。Workflow/Task 状态只表达调度、投递和 snapshot 确认阶段。

## 运行与构建

| 类别 | 当前技术 | 当前约束 |
|---|---|---|
| 语言 | Java 17 | 编译 source/target 均为 17 |
| 构建 | Maven Wrapper 3.9.9 | 所有本地和 CI 命令使用 `./mvnw` |
| 应用框架 | Spring Boot 3.2.0 | Boot 模块生成可执行 JAR |
| Web API | Spring MVC + Bean Validation | API 面向管理与审计，不提供下游抢任务接口 |
| API 文档 | Springdoc OpenAPI 2.0.2 | 默认暴露 Swagger UI |
| JSON | Jackson 2.15.2 | 用于 API、策略、审计和 instruction payload |

代码已使用 Java 17 record、switch expression、text block 和 Stream `toList()`。不以未来可能采用的语言特性作为当前完成能力。

## 数据与事务

| 类别 | 当前技术 | 当前状态 |
|---|---|---|
| 主数据库 | PostgreSQL 15 本地基线 | JSONB、唯一约束、行锁和事务是正确性的一部分 |
| ORM | Spring Data JPA / Hibernate | Entity 与 Repository 持久化 |
| Schema 迁移 | Flyway 9.22.0 | 当前迁移范围为 V1.0 至 V23.0，只允许前向迁移 |
| 本地测试数据库 | H2 | 仅用于 Boot 上下文等测试，不替代 PostgreSQL 语义验收 |

事件落库、资产状态投影、FlowPlan 评估和 source offset 推进位于同一摄入事务。目标日期互斥使用 PostgreSQL 持久化槽位；事务回滚、offset 恢复、租约重占和真实多 scheduler 锁竞争已由 PostgreSQL 16 Testcontainers E2E 覆盖。生产支持基线仍以 PostgreSQL 15 及兼容版本为准。

当前没有 Redis 依赖，也没有把缓存放入正确性链路。

## 湖格式集成

调度核心只消费格式无关的 `LakehouseSnapshotSourceProvider`、`LakehouseSnapshotSource`、`LakehouseSourceIdentity` 和 `LakehouseSnapshot`。

| 格式 | 当前状态 |
|---|---|
| Apache Paimon 1.3.1 | 已实现 Catalog API source、properties、delta manifest 分区推导、retention gap 失败关闭和 writer-side 属性注入；真实整体 E2E 已覆盖 |
| Apache Flink 1.20.3 | E2E 作业模块使用同一套流批 API；ODS 通过 Flink CDC 3.4.0 持续写入，DWD/DWS/ADS 使用有界批作业 |
| Apache Iceberg | 未实现 adapter |
| Apache Hudi | 未实现 adapter |

原生 snapshot/instant 标识、operation 和分区差异只能在 integration adapter 内解释。公共调度代码使用适配器提供的单调坐标和 typed `dataChange`，不能写死 Paimon 语义。

`LF-1.0` 的 Flink 集成遵守单表单写入者：规范化 `tableAssetKey` 只能绑定一个稳定 `writerJobKey`，同一作业的启动、重启和流批模式切换使用单调 writer epoch。Lakehouse Flow 只保存 binding、epoch 和独立 `JobControlIntent`，真实 Flink 操作由平台执行面完成；Paimon writer adapter 在 commit 期间执行旧 epoch fencing。绑定 `TaskInstance` 的 `SchedulingIntent` 继续只表达数据处理范围。

Flink 是 `LF-1.0` 的参考执行与 Paimon 写入实现，不是 Flow 领域类型。`ScheduleNode`、`WriterJobBinding` 和通用 outbound intent 只出现 `STREAMING|BATCH`；实际引擎、作业包、入口和提交钩子由 `writerJobKey` 对应的平台执行适配器解析。后续接入 Spark 或其他引擎不得修改 DAG、action、snapshot 结果或通用 intent schema。

仓库已在根 POM 锁定 Paimon 1.3.1、Flink 1.20.3 和 Flink CDC 3.4.0。升级任何一项都必须重新运行 writer adapter、CDC checkpoint 恢复、批式提交、Paimon source 和整体 E2E，不能只依赖依赖解析成功。

## 调度意图交付

每条不可变 outbound intent 只选择一个 route。`SchedulingIntent` 与 `JobControlIntent` 保留独立领域记录和 delivery 外键，并复用同一 publisher SPI、claim/fencing/退避/死信算法：

| 通道 | 当前实现 |
|---|---|
| `DATABASE_TABLE` | intent 与 delivery 同事务持久化，下游轮询专用表 |
| `HTTP` | Java 17 `HttpClient` 主动 POST，2xx 只表示传输 ACK |
| `MQ` | broker-neutral `SchedulingIntentMessageGateway`，具体 Kafka/Pulsar/RabbitMQ 绑定未提供 |

外部投递具备 claim lease、fencing token、指数退避、最大尝试和死信审计。传输状态不表示下游执行状态，也不能确认 DAG。

## 可观测性

- Spring Boot Actuator 暴露 health、metrics 和 Prometheus。
- Micrometer 记录 snapshot source、source reconciliation、调度决策、intent delivery 和调度积压指标。
- SLF4J + Logback 输出调度证据和异常。
- 当前没有分布式 tracing 实现。
- 指标与日志用于排障，不替代持久化 snapshot 证据。

## 测试与质量门槛

| 类别 | 当前技术或门槛 |
|---|---|
| 单元测试 | JUnit 5 + Mockito |
| Service 测试约束 | 每个显式 public service 方法必须有直接测试入口 |
| 覆盖率 | JaCoCo service line >= 90%，branch >= 65% |
| 系统级 E2E | `lakehouse-flow-e2e` 使用 Testcontainers 验证 PostgreSQL/MySQL/Flink/Paimon、DB/HTTP 投递和多 scheduler 恢复 |
| CI | 日常 GitHub Actions 使用 JDK 17 执行 `./mvnw -B -ntp clean verify -DskipITs`；发布候选运行完整 E2E |
| 交付物 | 验证成功后构建并检查可执行 Spring Boot JAR |

当前验证快照和测试数量只记录在 `PHASE2_PROGRESS.md`，避免在多个文档中复制后失真。

## 模块职责

| 模块 | 职责 |
|---|---|
| `lakehouse-flow-common` | 公共常量、异常和工具 |
| `lakehouse-flow-model` | 调度、snapshot、action、backfill 和 delivery 持久化模型 |
| `lakehouse-flow-dao` | Spring Data Repository 与 Flyway migration |
| `lakehouse-flow-service` | 资产状态、FlowPlan、DAG、action、writer control、snapshot 证据与策略服务 |
| `lakehouse-flow-api` | FlowPlan/Node、action、实例、writer control 和运维审计 REST API |
| `lakehouse-flow-scheduler` | intent outbox、外部投递、snapshot 确认和指标扫描 |
| `lakehouse-flow-integration` | 湖格式 source SPI、Paimon adapter、摄取和 source 对账 |
| `lakehouse-flow-flink-paimon` | 下游复用的 Paimon writer adapter、归因属性和 writer epoch fencing |
| `lakehouse-flow-test` | 跨模块共享测试夹具，不承载整体 E2E |
| `lakehouse-flow-e2e-jobs` | 提交到真实 Flink 容器的 CDC、批处理和 Paimon writer 测试作业 |
| `lakehouse-flow-e2e` | 整体 Testcontainers E2E 装配和验收入口 |
| `lakehouse-flow-boot` | Spring Boot 启动、运行配置和可执行 JAR |

模块边界不包含执行器适配层。

## 安全现状

- 写入型 API 使用 Bean Validation 做基础请求校验。
- `FlowPlan.owner` 和 `flowSpaceCode` 目前是归属与隔离字段，不构成可信授权。
- Spring Security、可信身份、Flow 级 RBAC 和配额尚未实现。
- 仓库中的 PostgreSQL 用户名和密码仅用于本地开发；生产凭据必须由部署环境注入。
- 当前不能声称已实现敏感信息加密、TLS 终止或生产网络隔离。

## 当前非目标与未验证项

- 不实现 executor、任务运行状态回调或资源调度。
- 不承诺 Redis、Kafka、Pulsar、RabbitMQ、Kubernetes、GraalVM 或 tracing 集成。
- 不声明尚未测量的吞吐、延迟或容量 SLA。
- 不把 H2 测试或 Mockito 测试描述成真实 PostgreSQL/Paimon E2E。
- `LF-1.0` 的 Flink/Paimon、DB/HTTP 和多 scheduler PostgreSQL 整体 E2E 已实现；当前补强用例见 `E2E_TEST_CASES.md`。
- Iceberg/Hudi、具体 MQ 产品绑定、可信身份/RBAC 和运维 UI 后移至 1.1+。
- 容量基线只在真实生产负载下采集，当前不声称未经测量的 SLA。

## 常用命令

```bash
./mvnw -version
./mvnw clean verify
docker compose up -d postgres
cd lakehouse-flow-boot
../mvnw spring-boot:run
```

更多启动步骤见 `QUICKSTART.md`，开发约束见 `DEVELOPMENT.md`。
