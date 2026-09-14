# Lakehouse Flow E2E 测试用例

## 1. 文档目的

本文档是 Lakehouse Flow 1.0 整体端到端测试的验收基线。它描述测试拓扑、业务数据、关键断言、已有自动化覆盖与待补强场景。

E2E 测试只验证 Lakehouse Flow 的调度责任，不把下游作业状态当作调度结果：

- Lakehouse Flow 通过 API 配置 Flow、Node、Writer 和 Action。
- 下游执行端被动接收调度意图（Intent）或作业控制意图（Job Control Intent）。
- 测试执行端收到意图后启动真实 Flink 作业，但不向 Lakehouse Flow 回报作业成功或失败。
- Lakehouse Flow 只根据目标资产 snapshot 是否推进、是否为业务数据变更、是否能归因到本次意图来确认结果。
- compaction、外部写入和无法归因的 snapshot 不能误判为本次调度成功。

> “意图（Intent）”是 Lakehouse Flow 发给下游的声明式调度指令。它说明“需要推进哪个目标资产、基于哪些输入证据、携带哪些归因属性”，并不表示 Lakehouse Flow 已经执行了作业。完整术语见 [GLOSSARY.md](GLOSSARY.md)。

## 2. 测试范围与边界

### 2.1 纳入范围

- 从订单业务库到 GMV 指标的真实 Flink + Paimon 数据链路。
- ODS 流式 CDC 和 DWD、DWS、ADS 批作业组成的流批混编 DAG。
- HTTP 与数据库表两种正式投递方式。
- 正常调度、补数、重跑、流作业启停与恢复。
- 多 scheduler 并发、事务回滚、租约过期重占、offset 恢复和数据库 migration。
- snapshot 数据变更识别、目标匹配、意图归因和结果语义。

### 2.2 不纳入范围

- Lakehouse Flow 内置执行器或资源队列。
- 依赖 Flink、Spark 或其他引擎的作业状态回调确认成功。
- Iceberg、Hudi 的真实 writer-side adapter。
- 生产容量和 SLA 基线。
- Web UI。

## 3. 整体测试拓扑

```text
MySQL orders
    |
    | Flink CDC（流式，checkpoint 提交 snapshot）
    v
Paimon ODS orders
    |
    | Flink bounded batch
    v
Paimon DWD order_detail
    |
    | Flink bounded batch
    v
Paimon DWS daily_gmv
    |
    | Flink bounded batch
    v
Paimon ADS gmv_report

Lakehouse Flow --------HTTP intent--------> 被动测试执行端
       ^                                         |
       |                                         | 启动真实 Flink 作业
       +----------- Paimon snapshot 观测 --------+

Lakehouse Flow ----database-table intent----> PostgreSQL 可轮询投递记录
```

容器和进程职责：

| 组件 | 作用 |
| --- | --- |
| PostgreSQL | 保存 Flow、实例、任务、意图、投递、source offset 和运维证据 |
| MySQL | 模拟订单业务库并开启 binlog |
| Flink JobManager | 接收测试执行端提交的真实流式/批式作业 |
| Flink TaskManager | 执行 Flink 作业 |
| Paimon warehouse | 保存 ODS、DWD、DWS、ADS 表及 snapshot |
| Lakehouse Flow Spring 进程 | 运行真实 API、扫描器、调度和确认逻辑 |
| 被动 HTTP 测试执行端 | 接收 HTTP 意图并启动真实 Flink 作业，不回报结果 |
| 数据库投递断言 | 验证两类意图原子可见、载荷和状态；真实轮询执行端仍为 P1 |

## 4. 测试数据与 Flow 定义

### 4.1 节点模型

| Node | 模式 | 输入 | 目标资产 | 行为 |
| --- | --- | --- | --- | --- |
| ODS | `STREAMING` | MySQL `orders` binlog | `ods.orders` | Flink CDC 持续写入，checkpoint 完成时提交 Paimon snapshot |
| DWD | `BATCH` | `ods.orders` | `dwd.order_detail` | 读取目标区间内订单明细并写入 DWD |
| DWS | `BATCH` | `dwd.order_detail` | `dws.daily_gmv` | 按业务日期聚合 GMV |
| ADS | `BATCH` | `dws.daily_gmv` | `ads.gmv_report` | 生成最终 GMV 指标结果 |

### 4.2 核心不变量

1. 同一个目标表在一个 Flow 中只能由一个 Writer 写入。
2. 流式 Node 不按每个 snapshot 创建普通批任务；它的 snapshot 是下游调度输入。
3. 批式 Node 必须等待当前实例对应的全部上游 snapshot 证据满足后才能进入可投递状态。
4. 补数起始 Node 可以跳过区间开始前的上游依赖，但仍必须由自身目标 snapshot 推进确认结果。
5. 非起始 Node 继续遵守 DAG 依赖，不得因补数而越过上游。
6. 正常推进与补数按目标资产和业务区间互斥，不能用同一 snapshot 同时确认两个竞争任务。
7. snapshot 只有同时满足目标匹配、业务数据变更和意图归因，才能得到 `SNAPSHOT_CONFIRMED`。

## 5. 结果断言口径

| 结果 | E2E 断言 |
| --- | --- |
| `SNAPSHOT_CONFIRMED` | 发现可归因到本次意图、且推进目标资产数据的 snapshot |
| `SNAPSHOT_NOT_ADVANCED` | 确认窗口结束，未发现满足条件的目标 snapshot；不是下游作业状态 |
| `DELIVERY_EXHAUSTED` | 意图超过投递重试上限；只表示传输失败 |
| `SOURCE_BLOCKED` | snapshot source 不可用或存在缺口；不得推断任务失败 |

所有成功用例至少校验：

- 目标 snapshot 单调推进。
- snapshot summary 中的 `flowId`、`taskInstanceId`、`intentId`、`writerEpoch` 等归因属性与调度意图一致。
- 任务确认依赖 snapshot 证据，而不是 HTTP 202、Flink Job 状态或执行端回调。
- 重复扫描和重复投递不产生重复确认。

## 6. 自动化用例矩阵

状态说明：

- **已自动化**：当前整体 E2E 已覆盖，并有明确断言。
- **计划 P1**：重要但不阻塞当前 1.0 RC，可在 P0 稳定后补齐。

### 6.1 真实业务闭环

| 编号 | 场景 | 关键断言 | 当前入口 | 状态 |
| --- | --- | --- | --- | --- |
| E2E-FLOW-001 | 通过 API 配置并发布订单 GMV Flow | Flow、Node、Writer 均由 Lakehouse Flow 配置；发布版本可被调度 | `OrderToGmvE2EIT` | 已自动化 |
| E2E-FLOW-002 | MySQL 新订单经 CDC 写入 ODS | checkpoint 提交 ODS snapshot；流 Node 不创建普通批任务 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-FLOW-003 | ODS 推进触发 DWD -> DWS -> ADS | 每个批 Node 只在上游证据满足后投递；最终 GMV 正确 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-FLOW-004 | 第二批订单增量推进 | 新 snapshot 只消费新增业务区间，不重复确认旧任务 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-DAG-001 | 下游等待同一实例的上游 snapshot | 上游未推进时 DWS/ADS 不产生可投递意图；上游推进后依次释放 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-DAG-002 | 节点同时依赖流式与批式输入 | 输入证据向量全部满足后才调度，不能只比较单一最大 snapshot | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |

### 6.2 Snapshot 识别与归因

| 编号 | 场景 | 关键断言 | 当前入口 | 状态 |
| --- | --- | --- | --- | --- |
| E2E-ATTR-001 | Flink writer 写入意图属性 | Paimon snapshot summary 可读取完整归因属性并确认任务 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ATTR-002 | compaction 推进 snapshot | maintenance snapshot 不确认数据处理任务，也不触发下游 | `ignoresMaintenanceAndForeignSnapshotsUntilAttributedTargetAdvances` | 已自动化 |
| E2E-ATTR-003 | 外部作业并发写入同一资产 | 无匹配 intent/epoch 的 snapshot 不确认 Lakehouse Flow 任务 | `ignoresMaintenanceAndForeignSnapshotsUntilAttributedTargetAdvances` | 已自动化 |
| E2E-ATTR-004 | 外部 snapshot 后出现正确归因 snapshot | 完整事件序列不会被较新的外部 snapshot 遮蔽，正确 snapshot 仍可确认 | `ignoresMaintenanceAndForeignSnapshotsUntilAttributedTargetAdvances` | 已自动化 |
| E2E-ATTR-005 | 旧 writer epoch 迟到写入 | 旧 epoch snapshot 被拒绝，新 epoch snapshot 可确认 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ATTR-006 | source snapshot 缺口 | 返回 `SOURCE_BLOCKED`，恢复后从持久化 offset 继续扫描 | `rollsBackAndRecoversSourceProjectionAcrossSchedulerProcesses` | 已自动化，需补真实 Paimon 缺口 P1 |

### 6.3 Action：补数、重跑与流作业控制

| 编号 | 场景 | 关键断言 | 当前入口 | 状态 |
| --- | --- | --- | --- | --- |
| E2E-ACT-001 | Node 补数 | 起始 Node 直接进入待调度；其下游仍逐级等待 snapshot | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-002 | Flow 补数 | 选定区间生成统一补数批次并串行推进 DAG | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-003 | Workflow 重跑 | 新建可审计尝试，保持 trigger key 和 snapshot 确认幂等 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-004 | Task 重跑 | 只重投目标任务，不把投递成功当作任务成功 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-005 | Node 重跑并级联下游 | 目标 Node 与下游重新推进，上游已确认资产不重复执行 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-006 | 补数失败后恢复 | 未产生目标 snapshot 时超时；恢复后可重新发起并确认 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-ACT-007 | 两个及以上日期分区补数 | 当前日期确认前下一日期无 intent；确认后按序释放且不复用 intent | `advancesSerialBackfillDatesOnlyAfterCurrentDateSnapshotsConfirm` | 已自动化 |
| E2E-ACT-008 | 正常推进与补数竞争同一目标区间 | 只有一个调度实例获得准入，另一方等待或返回冲突 | `serializesTargetAndFlowAdmissionAcrossSchedulerProcesses` | 已自动化，需补真实链路 P1 |
| E2E-CTRL-001 | 启动 ODS 流作业 | 控制意图被动投递，writer epoch 生效，snapshot 可归因 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-CTRL-002 | 重启 ODS 流作业 | 从 checkpoint 恢复，epoch 递增，旧实例写入被隔离 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-CTRL-003 | 补数期间暂停、回放并恢复流作业 | 补数不污染正常流式推进，恢复后继续消费增量 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |

### 6.4 投递、高可用与恢复

| 编号 | 场景 | 关键断言 | 当前入口 | 状态 |
| --- | --- | --- | --- | --- |
| E2E-DEL-001 | HTTP 被动接收调度意图 | HTTP 202 只表示接收；真实 Flink 作业完成后仍由 snapshot 确认 | `streamsOrdersAndBuildsAttributedGmvSnapshots` | 已自动化 |
| E2E-DEL-002 | 数据库表发布调度和控制意图 | 两类 outbox 均可被下游轮询，状态与载荷可追踪 | `publishesDataAndControlIntentsThroughDatabaseTables` | 已自动化 |
| E2E-DEL-003 | 数据库消费者启动真实 Flink 作业 | 消费者 claim 行、幂等启动作业，Lakehouse Flow 仍只看 snapshot | 待新增数据库轮询执行端 | 计划 P1 |
| E2E-HA-001 | 两个 scheduler 竞争 HTTP 投递 | PostgreSQL 行锁保证单次 claim，租约过期可由另一节点重占 | `reclaimsInterruptedHttpDeliveriesAcrossSchedulerProcesses` | 已自动化 |
| E2E-HA-002 | 旧 owner 在重占后迟到提交 | fencing token 阻止旧 owner 覆盖新 owner 的投递结果 | `reclaimsInterruptedHttpDeliveriesAcrossSchedulerProcesses` | 已自动化 |
| E2E-HA-003 | 多 scheduler 竞争目标和 Flow 准入 | 目标/日期及 Flow 互斥约束在共享 PostgreSQL 上成立 | `serializesTargetAndFlowAdmissionAcrossSchedulerProcesses` | 已自动化 |
| E2E-HA-004 | source 投影事务回滚 | 事件、资产状态和 offset 同事务回滚，重试后只推进一次 | `rollsBackAndRecoversSourceProjectionAcrossSchedulerProcesses` | 已自动化 |
| E2E-HA-005 | 确认进程中断后恢复 | 新 scheduler 从持久化状态恢复，确认结果保持幂等 | `resumesSnapshotConfirmationIdempotentlyAcrossSchedulerProcesses` | 已自动化 |
| E2E-HA-006 | 独立 JVM scheduler 被终止并恢复 | Boot JAR 在 HTTP ACK 前被强制终止；租约过期后另一 scheduler 重占，旧进程不能落 ACK | `reclaimsDeliveryAfterIndependentSchedulerJvmIsKilled` | 已自动化 |

### 6.5 结果语义、运维与兼容性

| 编号 | 场景 | 关键断言 | 当前入口 | 状态 |
| --- | --- | --- | --- | --- |
| E2E-SEM-001 | HTTP 投递重试耗尽 | 结果为 `DELIVERY_EXHAUSTED`，不能写成下游执行失败 | `keepsDeliveryExhaustionSourceBlockingAndSnapshotTimeoutOrthogonal` | 已自动化 |
| E2E-SEM-002 | source 不可用或缺口 | 结果为 `SOURCE_BLOCKED`，与 snapshot 未推进相互独立 | `keepsDeliveryExhaustionSourceBlockingAndSnapshotTimeoutOrthogonal` | 已自动化 |
| E2E-SEM-003 | 确认窗口内无目标证据 | 结果为 `SNAPSHOT_NOT_ADVANCED`，保留基线和观测证据 | `keepsDeliveryExhaustionSourceBlockingAndSnapshotTimeoutOrthogonal` | 已自动化 |
| E2E-OPS-001 | 统一运维 API 巡检 | 从健康 source 证据和失败补数定位 `SNAPSHOT_NOT_ADVANCED`；从同一巡检链路定位 HTTP 死信、`DELIVERY_EXHAUSTED` blocker、仍待目标 snapshot 的任务及其补数批次，无需登录数据库 | `inspectsOperationalEvidenceWithoutDatabaseAccess` | 已自动化 |
| E2E-MIG-001 | 从 2.2 schema 升级到 2.3 | Flyway migration 成功，历史任务补齐证据字段且可读取 | `upgradesExistingTaskEvidenceFromV22ToV23` | 已自动化 |

## 7. 后续增强顺序

当前 P0 场景已经自动化。后续按以下顺序补充 P1：

1. 数据库轮询消费者接收 intent 后启动真实 Flink 作业。
2. 使用真实 Paimon metadata 构造 snapshot 保留缺口并验证 offset 恢复。
3. 按真实试运行反馈补充其他运维异常组合与分页边界。

## 8. 执行与验收

整体 E2E 使用 JDK 17、Maven Wrapper 和 Testcontainers：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw -pl lakehouse-flow-e2e -am verify
```

验收要求：

- Testcontainers 环境可用且所有容器通过健康检查。
- Failsafe 报告没有失败、错误或跳过的必测用例。
- 测试不得通过伪造执行状态回调推进调度。
- 测试执行端必须在接收意图后再启动作业，不得绕过投递契约直接修改 Lakehouse Flow 状态。
- 测试产生的 Paimon snapshot 必须包含与生产 writer-side contract 一致的归因属性。
- 新增场景必须同步更新本矩阵的入口与状态。
