# Lakehouse Flow 当前进度基准

**最后更新**: 2026-09-14
**基准用途**: 后续开发进度、差距检查和版本目标均以本文档为准。
**当前推进项**: `LF-1.0` 的真实 Paimon 闭环与 PostgreSQL 高可用恢复门槛已经闭合：订单到 GMV 整体 Testcontainers E2E 已验证 Flink CDC 流式 ODS、START/RESTART、checkpoint offset 恢复、旧 writer epoch 拒绝、两轮普通 DAG 和 DWD 起始的 Node 子图补数；独立双 scheduler 上下文已验证事务中断、claim 重占、HTTP 至少一次投递、下游幂等、确认恢复、并发准入和 source offset 串行化。下一步推进剩余 action、`DATABASE_TABLE` 正式投递、最小运维 API 和契约冻结。

## 目标边界

Lakehouse Flow 是基于 snapshot 推进模式的新一代调度系统。

它只负责调度侧决策、调度意图记录、调度审计和 snapshot 推进确认，不负责真实任务执行，不内置 executor，不接管下游资源队列。

系统判断成功或失败时，以目标数据资产的 snapshot 是否按预期推进为准。Workflow/Task 状态只用于调度决策、意图交付和审计视图，不能演变成执行状态机。

## 优先级基线

| 优先级 | 判定标准 | 当前状态 |
|--------|----------|----------|
| P0 | 会破坏 snapshot 归因、幂等、DAG 顺序、目标日期互斥、事务/offset 一致性、可恢复性、构建可复现性或“只调度不执行”边界 | 基础正确性、真实 Paimon 普通/补数闭环、单表单 writer、writer 重启和 PostgreSQL 多节点恢复已闭合；`LF-1.0` 仍有剩余 action 闭环、`DATABASE_TABLE` 和 HTTP 超时/死信端到端验证未闭合 |
| P1 | 不破坏现有正确性，但影响 `LF-1.0` 在受信生产环境中的排障和契约稳定性 | 补齐最小运维查询、API/intent/migration 兼容策略和下游幂等验收 |
| P2 | 不影响 `LF-1.0` 单团队 Paimon 闭环的扩展能力 | 可信身份/RBAC、Iceberg/Hudi、具体 MQ 绑定、Web UI、完整聚合查询和 G13 高级策略后移至 1.1+ |

已闭合的单元级 P0 不表示系统已经达到 `LF-1.0`。从本次基准起，整体 Testcontainers E2E、真实 Flink/Paimon 闭环和多 scheduler 节点 PostgreSQL 语义均为 1.0 发布门槛，不再作为普通后移项。

## 版本目标

| 版本 | 目标 | 验收口径 |
|------|------|----------|
| LF-0.1 | Snapshot 调度闭环原型 | 能基于事件、资产状态和依赖条件生成调度实例，并可确认目标 snapshot 是否推进。 |
| LF-0.2 | FlowPlan / Node 对象模型 | 有稳定的 Flow 定义、版本、节点、节点依赖和目标资产绑定模型，后续调度实例可追溯到发布版本。 |
| LF-0.3 | 最小 API 与调度意图交付 | 可通过 API 管理 FlowPlan、触发 action、查询实例和 snapshot 证据；Lakehouse Flow 主动发布不可变调度意图，下游通过库表、被调用接口或 MQ 被动接收。 |
| LF-0.4 | Action 能力增强 | 支持 workflow/node 级重跑、补数、跳过、取消、snapshot 重检；补数严格遵守 DAG snapshot 依赖，只允许选中起点绕过前置节点。 |
| LF-0.5 | 生产化调度控制面 | 支持补偿扫描、并发保护、Flow 级隔离、审计查询、可观测性和运维排障视图。 |
| LF-1.0 | 单团队受信环境的可投产 Paimon 闭环 | 通过 Flink CDC 流式 ODS、Flink 流批一体 writer-side adapter 和真实 Paimon source 验证普通 DAG、action 与补数；数据库和 HTTP 投递可用；PostgreSQL 多节点、中断、重占、回滚与 offset 恢复通过整体 E2E；结果、契约和运维语义稳定。 |
| LF-1.1+ | 多团队与生态扩展 | 按真实需求增加可信身份/RBAC、Iceberg/Hudi、具体 MQ、Web UI 和高级并发策略。 |

## LF-1.0 目标与发布门槛

### 部署边界

1. `LF-1.0` 面向单团队受信环境。`FlowPlan.owner` 和 `flowSpaceCode` 继续提供 Flow 级归属与逻辑隔离，但可信身份、RBAC 和租户配额不是 1.0 发布门槛。
2. 唯一强制湖格式是 Apache Paimon 1.3.x。Iceberg/Hudi 保持 SPI 扩展目标，不为了 1.0 虚构第二套实现。
3. 业务库增量入湖固定使用 Flink CDC 持续流式写入 Paimon ODS。该常驻数据入口由平台控制面发起启动/重启意图、平台执行面托管运行；Lakehouse Flow 连续观察 ODS 业务数据 snapshot，并以其 snapshot/watermark 推进作为下游依赖事实，不为每条 CDC 变更重复创建执行意图。
4. DWD、DWS、ADS 的计算模式不按数仓层级写死。Flow/Node 只选择 `STREAMING` 或 `BATCH`，不保存 `FLINK`、`SPARK` 等引擎类型。`LF-1.0` 的真实执行参考使用统一 Flink Table/SQL 与 Paimon writer 适配边界，但 DAG、补数、重跑、互斥和 snapshot 确认保持引擎无关。
5. 批式处理以有界输入完成后的最终目标数据 snapshot 作为逻辑完成证据；流式回放以已经完整覆盖本次冻结输入 snapshot/watermark 边界的数据提交作为完成证据。具体 checkpoint、job-end 或其他 commit hook 由执行适配器解释。`lakehouse-flow.final=true` 表示本次 intent 的逻辑边界已完成，不表示常驻作业结束。
6. Lakehouse Flow 运行时不提交、托管、停止或读取任何执行引擎的 job 状态。它只发送 intent，并从 Paimon snapshot 读取可归因证据。
7. 作业启动和重启必须从平台控制面发起。Lakehouse Flow 将 `START_JOB` / `RESTART_JOB` 持久化为独立 `JobControlIntent` 并可靠投递，执行侧根据 `writerJobKey` 路由到实际引擎；该对象绑定 `WriterJobBinding`，不伪造 task、workflow、业务日期或引擎类型。传输 ACK 或外部 job 状态都不作为结果，仍由新 writer epoch 下的目标 snapshot 推进确认。
8. 一个 FlowPlan DAG 内允许同时存在流式和批式节点，但规范化到物理表后的 `tableAssetKey` 只能绑定一个稳定 `writerJobKey`。不同 Flow、不同节点、不同模式或不同业务日期都不能以不同作业并发写同一张表。
9. 1.0 正式投递通道是 `DATABASE_TABLE` 和 `HTTP`。MQ 保留稳定 SPI，不强制绑定 Kafka、Pulsar 或 RabbitMQ。

### 结果语义

结果必须保持三个正交维度，不合并为一个包含下游运行状态的大状态机：

| 维度 | 对外语义 | 判定依据 |
|------|----------|----------|
| Snapshot 结果 | `WAITING` / `SNAPSHOT_CONFIRMED` / `SNAPSHOT_NOT_ADVANCED` | 仅依据 baseline 之后可归因的目标业务数据 snapshot |
| Delivery 状态 | `PENDING` / `PUBLISHING` / `RETRY_WAIT` / `PUBLISHED` / `DELIVERY_EXHAUSTED` | 仅依据 DB/HTTP 传输证据；存储层 `EXHAUSTED` 对外表达为 `DELIVERY_EXHAUSTED` |
| Source 健康 | `HEALTHY` / `REPAIRABLE` / `SOURCE_BLOCKED` | 依据 source range、durable offset、事件账本与 AssetState 对账；存储层 `BLOCKED` 对外表达为 `SOURCE_BLOCKED` |

`SNAPSHOT_CONFIRMED` 只能由可归因的目标 snapshot 产生。`SNAPSHOT_NOT_ADVANCED` 只能在确认窗口结束且 source 证据链已追平、无 retention gap 或投影缺口时产生。source 不可用、落后尚未补齐或证据不完整时必须保持 snapshot 结果待定，并单独暴露 `SOURCE_BLOCKED`；不得因无法观测而推断下游失败。暂时中断后如能连续补扫并证明无缺口，可恢复确认判定。

`DELIVERY_EXHAUSTED` 只表示在投递窗口内未获得传输 ACK，不表示下游没有启动、执行失败或不会稍后提交 snapshot。即使 delivery 已耗尽，snapshot 确认仍依据事件证据独立进行。

### 发布门槛

| 编号 | 门槛 | 验收标准 | 当前状态 |
|------|------|----------|----------|
| R1 | 真实 Flink/Paimon 闭环 | 真实业务库变更经 Flink CDC 持续流式写入 Paimon ODS；DWD/DWS/ADS 按 Flow 配置选择流式或批式，验证 writer adapter 写入全部归因属性，Paimon source 读回并完成输入边界、分区证据、DAG 与补数推进 | 已完成：可复用 adapter、CDC ODS、三个批式加工节点、两轮普通 DAG，以及 DWD 起始的 DWD/DWS/ADS Node 子图补数均通过真实 Paimon E2E |
| R2 | Action 真实闭环 | 普通调度、重跑、完整 Flow 补数、Node 子图补数和失败恢复均在真实 Paimon 上遵守同一 DAG/snapshot 不变量 | 部分完成：Node 子图补数已通过真实 Paimon E2E；重跑、完整 Flow 补数和失败恢复仍待验收 |
| R3 | PostgreSQL 高可用与恢复 | 多 scheduler 并发、行锁、进程中断、claim 过期重占、新旧 fencing、事务回滚和 source offset 恢复经整体 E2E 通过 | 已完成：双 scheduler 上下文已覆盖故障切点 1-6；writer epoch 行锁、Flink checkpoint offset 恢复和旧 holder/epoch fencing 同时通过 |
| R4 | 正式投递 | 数据处理与作业控制意图都支持 `DATABASE_TABLE + HTTP`；数据库无丢 intent，HTTP 至少一次投递、重试、超时、死信和 `intentKey` 幂等在真实 PostgreSQL/网络上验证 | 部分完成：两类 intent 已通过真实 PostgreSQL 和 HTTP 正常投递；DATABASE_TABLE 路由及 HTTP 故障路径待 E2E |
| R5 | 稳定结果语义 | 确认、未推进、投递耗尽和 source 阻塞可独立查询；source 缺口不得产生 `SNAPSHOT_NOT_ADVANCED` | source 健康已持久化且超时门禁完成；最小运维查询和整体 E2E 尚未完成 |
| R6 | 稳定契约 | 冻结 1.0 REST API、`SchedulingIntent` 1.3、`JobControlIntent` 1.0、Flyway 只前进迁移策略和下游 `intentKey` 幂等要求，并有兼容性回归 | 数据处理 1.3 与作业控制 1.0 已实现；最终冻结和真实兼容性验收未完成 |
| R7 | 生产运维闭环 | 阻塞原因、delivery 死信、snapshot 证据、补数批次、source 对账和关键告警可通过 API/指标查询，无需登录数据库 | 部分完成 |
| R8 | 工程质量 | JDK 17；service line >= 90%、branch >= 65%；每个显式 public service 方法有直接单测入口；发布前整体 E2E 通过 | 已完成基线；开发期使用 `./mvnw clean verify -DskipITs` 持续守住单测和覆盖率，整体 E2E 按当前决策在功能收口后集中执行 |
| R9 | 作业生命周期与单表单写入者 | 平台可对 `WriterJobBinding` 发起独立 `JobControlIntent(START_JOB|RESTART_JOB)`，执行侧以 writer epoch fencing；控制意图不创建 task/workflow；发布两个不同 `writerJobKey` 指向同一物理表必须失败，混合流批 DAG 中不同表可独立推进 | 已完成：模型/约束/单测及真实 START、checkpoint RESTART、旧 epoch 提交拒绝、四表独立推进均已通过 |

### 整体 E2E 故障矩阵

`lakehouse-flow-e2e` 是整个系统的验收入口，`lakehouse-flow-test` 仅保留共享夹具；不能把单个生产模块的 Testcontainers 测试冒充整体 E2E。至少覆盖：

1. source snapshot 读取后、摄入事务提交前中断，验证事件、AssetState 和 offset 共同回滚并可重放。
2. intent 已提交、HTTP 调用前中断，验证新 scheduler 可重占并投递。
3. HTTP 已送达、ACK 落库前中断，验证至少一次重投与下游 `intentKey` 幂等。
4. 目标 snapshot 事件已持久化、task 确认前中断，验证重扫只产生一份确认证据并正确释放 DAG。
5. claim 租约过期时新旧 scheduler 竞争，验证旧 fencing token 无法覆盖新 holder。
6. 两个 scheduler 竞争同一 Flow 并发名额、目标日期槽位和 source offset，验证 PostgreSQL 行锁与唯一约束下无重复首发、无 offset 倒退。
7. 一个 DAG 同时包含流式和批式节点：流式父节点以输出资产 snapshot/watermark 作为边证据，批式父节点以同一调度实例的已确认输出作为边证据；验证汇聚下游必须等待全部父边和自身额外依赖，并冻结完整输入向量。Flow/intent 中不得出现 Flink/Spark 引擎字段。
8. 两个 Flow/节点尝试绑定同一物理表，验证不同 `writerJobKey` 在发布期被拒绝；平台重启同一 writer 时只产生 `JobControlIntent` 而不产生 task/workflow，旧 epoch 的迟到 snapshot 不能确认新控制或数据处理 intent。
9. 常驻流 writer 的目标表执行补数，验证回放由当前流 epoch 消费，或平台完成流 epoch -> 批 epoch -> 新流 epoch 的受控切换，整个过程不存在第二个并发 writer。

故障切点 1-6 已由 `SchedulerRecoveryE2EIT` 完成：两个独立 Spring scheduler 上下文连接同一真实 PostgreSQL，在事务和 delivery 持久化边界注入可重复的进程中断等价故障；验证回滚后重放、租约过期重占、旧 token 拒绝、HTTP 送达后 ACK 丢失的幂等重投、snapshot 确认重扫，以及 Flow/目标/source 三类行锁竞争。第 8 项的单表 writer 和重启 fencing 已由订单链路 E2E 完成；第 7、9 项仍分别属于流批汇聚扩展和常驻流表补数切换，不影响 R3 退出。

### 已完成实施顺序

1. `LF1-01` Source-aware confirmation（已完成代码与单测）：持久化表级 `SnapshotSourceHealth`，让分区/整表 intent 可确定绑定受管 source；在 snapshot 超时判定前校验截止时间之后的 source 证据链，并暴露正交的 snapshot/delivery/source 结果。
2. `LF1-02` 作业与单写入者模型（已完成代码与单测）：增加稳定 `writerJobKey`、规范化 `tableAssetKey`、发布期唯一绑定和 writer epoch；新增不绑定 task/workflow/bizDate 的 `JobControlIntent` / delivery，承载平台 `START_JOB` / `RESTART_JOB`，并复用已有可靠投递算法与 publisher SPI。
3. `LF1-03` 流批一体调度契约（已完成代码与单测）：Flow/Node 只冻结 `processingMode=STREAMING|BATCH`；运行时 intent 冻结混合父边的 input snapshot/watermark vector，流式父边来自资产推进，批式父边来自同实例确认输出。完成提交细节由执行适配器解释，两种模式输出统一的 intent-correlated snapshot 证据。
4. `LF1-04` Flink/Paimon writer-side adapter（已完成）：独立 `lakehouse-flow-flink-paimon` 模块统一流式 checkpoint/批式结束提交、完整属性校验和 `WriterEpochFence` SPI；JDBC 实现以 PostgreSQL binding 行锁覆盖整个 Paimon commit，真实旧 epoch 提交已失败关闭。
5. `LF1-05` 真实业务闭环 E2E（R1 已完成）：已贯穿业务库变更、Flink CDC 流式 ODS、平台 START/RESTART、外部 checkpoint offset 恢复、HTTP intent、DWD/DWS/ADS 批式 DAG、Paimon source、归因、两轮普通推进和 Node 子图补数。其余 action 属于 R2，数据库投递属于 R4。
6. `LF1-06` 高可用与恢复 E2E（R3 已完成）：真实 PostgreSQL 上的双 scheduler 锁竞争、事务中断、claim/确认恢复、旧 token fencing、至少一次 HTTP 幂等重投和 source offset 单调恢复均已通过。
7. `LF1-07` 最小运维 API（已完成代码与单测）：增加统一 blockers、持久化 source 对账和 job-control delivery dead-letter 查询；V23 为 task 持久化结构化 source 证据，查询不依赖 `waitingReason` 文本，也不读取下游运行状态。
8. `LF1-08` 契约冻结（开发候选已完成）：已固定 `/api/v1` surface、两类 intent JSON Schema、未知字段策略、V1-V23 migration checksum 和下游幂等要求；整体 E2E 通过后正式冻结。

### LF-1.0 未完成项四态清单

这里的“四态”按剩余工作的性质分类，不是新增运行时状态，也不会进入 workflow/task 状态机。开发期间先完成代码、运维与观测收口；整体 Testcontainers E2E 暂不逐项运行，统一留到稳定态末尾集中验收。

| 分类 | 编号 | 未完成项 | 当前基础 | 退出标准 |
|------|------|----------|----------|----------|
| 开发态 | DEV-1 | 最小运维只读聚合能力（已完成） | 已增加统一 blockers、持久化 source 对账和两类 delivery 死信 API；task source 结论已由 V23 结构化持久化 | 已达到：支持按 blocker、Flow、目标资产、source 和 source 结果过滤，只展示调度、传输和 snapshot/source 证据，不引入下游运行状态 |
| 运维态 | OPS-1 | 生产接入与故障处置手册（已完成） | `OPERATIONS_RUNBOOK.md` 已覆盖 `DATABASE_TABLE + HTTP`、Paimon source/writer adapter、START/RESTART、死信、`SOURCE_BLOCKED`、补数控制和 writer fencing | 已达到：配置检查、投产验收、日常巡检、结果语义和禁止操作均有可执行步骤，且不读取下游运行状态 |
| 运维态 | OPS-2 | PostgreSQL migration 与数据保留操作策略（已完成） | `DATABASE_OPERATIONS.md` 已固定 V1-V23 基线、V23 后 expand/contract 约束、备份恢复、单实例升级和 aggregate 保留边界 | 已达到：migration 只前进，生产关闭 baseline/clean，明确失败恢复和滚动兼容；容量基线前默认不自动删除，禁止破坏 source/intent/audit 证据链 |
| 观测态 | OBS-1 | Source 与阻塞原因可查询（已完成） | `/api/v1/operations/snapshot-sources` 与 `/blockers` 已支持 source、Flow、目标资产、结果和 blocker 分类过滤 | 已达到：可查看最新 `HEALTHY/REPAIRABLE/SOURCE_BLOCKED` 证据及阻塞对象，无需登录数据库 |
| 观测态 | OBS-2 | 两类 intent 的死信与关键告警闭环（已完成） | 已拆分数据处理与 job-control delivery 指标，预注册失败序列，并交付 `deploy/prometheus/lakehouse-flow-alerts.yml` 与 `OBSERVABILITY.md` | 已达到：decision failure、source gap、两类 delivery exhausted、READY/SCHEDULED backlog 均有可部署规则、稳定标签、证据 API 和 runbook 链接 |
| 稳定态 | STB-1 | R2 剩余 Action 真实闭环 | Node 子图补数已通过真实 Paimon；其余 action 的模型和 Mockito 单测已完成 | 集中 E2E 验证 workflow/task/node 重跑、完整 Flow 补数和失败恢复继续遵守归因、DAG、baseline 与互斥不变量 |
| 稳定态 | STB-2 | R4 正式投递验收 | 两类 intent 的 DB/HTTP 实现和正常 HTTP 路径已完成，HTTP ACK 丢失幂等重投已验证 | 集中 E2E 验证两类 intent 的 `DATABASE_TABLE` 无丢失，以及 HTTP 超时、退避、重试耗尽、死信和下游 `intentKey` 幂等 |
| 稳定态 | STB-3 | R5 结果语义回归 | `SNAPSHOT_CONFIRMED`、`SNAPSHOT_NOT_ADVANCED`、`DELIVERY_EXHAUSTED`、`SOURCE_BLOCKED` 已独立建模 | 集中 E2E 证明四种语义互不覆盖，尤其 delivery 耗尽不代表执行失败、source 缺口不产生未推进结论 |
| 稳定态 | STB-4 | 流批边界补充验收 | 普通流式 ODS、批式下游和单表单 writer 已验证 | 集中 E2E 补齐流/批父节点汇聚输入向量，以及常驻流目标表补数由当前 epoch 消费或受控 epoch 切换且无第二 writer |
| 稳定态 | STB-5 | 1.0 契约与兼容策略冻结（开发候选已完成） | 已交付 `/api/v1` surface、`SchedulingIntent` 1.3 与 `JobControlIntent` 1.0 JSON Schema、14 类 request 未知字段策略、V1-V23 checksum 基线和兼容回归 | 集中 E2E 验证正式 DB/HTTP 投递、action、流批和 V23 升级路径后正式冻结；后续 v1/同 major 只允许增量兼容 |

不计入 LF-1.0 未完成项：可信身份/RBAC、Iceberg/Hudi、具体 MQ 产品绑定、Web UI、Flow/Node 聚合稳定游标、`SERIAL_DISCARD` / `SERIAL_PRIORITY` / `dedupeWindow` 和补数审批上限，均保持在 1.1+。容量基线依照已确认决策放到真实生产负载采集，也不阻塞 1.0 功能发布。

### 当前验证策略

1. 从 2026-09-14 起，日常开发和 CI 使用 JDK 17 与 `./mvnw clean verify -DskipITs`，继续执行全部单元测试、Mockito public-service 入口检查和 JaCoCo 门槛，但不启动 Testcontainers 整体 E2E。
2. 可以使用 `./mvnw -pl <module> -am test` 做更快的定向反馈；不得用跳过测试的编译结果代替开发门槛。
3. 当 DEV、OPS、OBS 工作收口并进入 STB 最终验收时，再一次性运行完整 `./mvnw clean verify`，覆盖所有 action、正式投递、结果语义和流批边界。
4. 最近一次完整 E2E 通过记录继续作为有效基线；后续代码在集中验收前只能称为“单元验证通过”，不能宣称新增路径已经系统级验证。

DEV/OPS/OBS/STB 最近验证：JDK 17 下 `./mvnw clean verify -DskipITs` 通过，共 394 个单元/启动测试，失败 0、错误 0；新增兼容性回归覆盖 35 个 `/api/v1` 路由、14 类 request 未知字段策略、两类 intent JSON Schema 与 V1-V23 migration checksum。H2 application context 完成两类 delivery 分组查询的 Spring Data JPQL 启动解析，并锁定 Flyway 生产安全默认值，Service JaCoCo line 91.37%、branch 68.98%、method 91.42%。Prometheus 规则已通过 YAML 结构校验，但本机未安装 `promtool`，部署前仍需执行 PromQL 规则校验。整体 Testcontainers E2E 按当前策略未运行，因此 STB 目前是开发冻结候选，不能视为正式验收或最终冻结。

### 容量基线决策

容量基线不作为当前 `LF-1.0` 功能开发退出门槛，因为 Flow 数、节点数、受管资产数、snapshot 速率、intent 积压和恢复时间必须在真实生产负载下测量。未获得该证据前不编造吞吐、延迟或 SLA，也不声称“容量已验收”。上线后以受控流量逐步采集这些数据，再固化首份生产容量报告和告警阈值。

## 当前已完成能力

| 能力 | 状态 | 说明 |
|------|------|------|
| 资产状态模型 | 已完成 | `AssetState` 分离物理观察与业务数据 snapshot/watermark；所有 snapshot 更新表级观察轨，只有 `dataChange=true` 更新业务轨和 changed-partition 状态。 |
| 依赖条件模型 | 已完成 | `FlowPlanVersion.dependencySpecJson` / `ScheduleNode.inputDependencySpecJson` 与 `DependencyCondition` 支持 snapshot、水位、质量、schema 条件。 |
| 依赖评估服务 | 已完成基础版 | `FlowPlanConditionService` 解析 AND/OR 分组和 `${bizDate}`；自然触发统一由 `FlowPlanEvaluationService` 处理。 |
| 调度实例状态 | 已完成 | `WorkflowInstance` / `TaskInstance` 表示调度侧实例，不表示真实执行进程；已支持可选关联 `FlowPlanVersion` / `ScheduleNode`。 |
| Snapshot 推进确认 | 已完成基础版 | `SnapshotProgressService` 捕获 baseline；`SnapshotEvidenceService` 在完整事件序列中按 intent 属性、适配器统一 `dataChange` 分类和目标分区归因后确认结果。 |
| Snapshot 补偿扫描 | 已完成基础版 | `SnapshotConfirmationScanner` 周期性检查已调度但未确认的调度实例。 |
| Action 审计模型 | 已完成基础版 | `SchedulingAction` 记录重跑、补数、取消、跳过、重检等操作意图和结果。 |
| Action 联查视图 | 已完成基础版 | 支持按 action key 查询请求、统一补数批次和逐 task snapshot 证据，并按 Flow、发布版本、节点组合筛选 action 摘要；action 应用结果与 snapshot 推进结果分开表达。 |
| Action 服务 | 已完成基础版 | 完整 Flow 与 Node 子图补数统一记录 `BackfillBatch` / `BackfillItem`，共享日期串行/并行/限流、安全整日跳过、暂停/取消和两种失败恢复策略；显式入口 READY，其余节点等待父节点 snapshot，取消 workflow 会阻断其未交付子意图。 |
| FlowPlan 对象模型 | 已完成基础版 | `FlowPlan` / `FlowPlanVersion` / `ScheduleNode` 已支持定义、版本、节点依赖和目标资产绑定。 |
| FlowPlan 服务 | 已完成基础版 | 支持草稿、版本、节点、发布和查询；发布前拒绝缺失依赖、自依赖、重复节点和环。 |
| 最小 API 层 | 已完成基础版 | `lakehouse-flow-api` 已暴露 FlowPlan/Node 管理、action 触发、实例和 snapshot 证据查询。 |
| 调度意图交付 | 已完成基础版 | 内部 scanner 冻结 baseline，并按单一选定路由写入版本化完整 `instructionPayload`；数据库立即可见，HTTP/MQ 通过统一 publisher SPI 主动推送，外部投递支持 claim 租约、fencing、指数退避、最大尝试和死信审计。 |
| 目标日期准入互斥 | 已完成基础版 | 正常、补数、恢复和重跑共享 `targetAssetKey + bizDate` 持久化槽位；先准入再冻结 baseline，snapshot 确认或发布准入租约过期后释放，冲突任务保持 READY 等待后续扫描。 |
| 单表单 Writer 与作业控制 | 已完成基础版 | `WriterJobBinding` 固化物理表唯一 writer，Flow 发布和 intent 发布双重校验；平台 start/restart 使用独立 `JobControlIntent`、writer epoch、可靠投递和 source-aware snapshot 确认，不创建执行状态。 |
| Flink/Paimon writer adapter | 已完成 | 独立下游模块统一校验归因属性、准备流式 checkpoint/批式提交，并通过可替换 `WriterEpochFence` 在物理提交期间持有当前 writer 世代授权。 |
| 发布版本策略执行 | 已完成基础版 | 版本 `confirmationPolicyJson` 和节点覆盖驱动确认窗口；版本 `concurrencyPolicyJson` 驱动目标准入租约和活跃 workflow 上限，超限任务保持 READY 且不冻结 baseline。 |
| FlowPlan 自然触发 | 已完成基础版 | 只有外部数据提交和合法非 action 逻辑完成 snapshot 评估 `PUBLISHED` 版本；action-owned、中间、孤儿和维护 snapshot 不进入自然触发。 |
| DAG snapshot 推进 | 流批混合基础版完成 | 普通流式节点不生成逐 snapshot task；批式下游等待流父节点的资产 snapshot/watermark、批父节点的同实例确认输出和自身额外依赖，并在发布 intent 时冻结完整输入向量。显式补数/重跑的流式回放 task 仍按同实例确认推进。 |
| 摄入事务边界 | 已完成基础版 | 单个 source snapshot 的事件落库、AssetState 投影、DAG/FlowPlan 评估和 offset 推进在同一事务完成。 |
| 湖格式 source SPI | 已完成基础版 | provider、source identity、opaque offset、格式排序和统一 snapshot observation 已与 Paimon SDK 解耦。 |
| Paimon snapshot source | 基础闭环已验证 | Paimon 1.3 Catalog API 逐 snapshot 读取 properties，并从 delta manifests 推导规范化 changedPartitions；真实订单到 GMV 普通与 checkpoint 重启链路已验证，保留历史缺口仍失败关闭。 |
| Maven Wrapper / JDK17 | 已完成 | 本项目使用 `./mvnw` 构建，并以 Java 17 为编译目标。 |

## 差距清单

| 编号 | 差距 | 当前状态 | 目标版本 | 说明 |
|------|------|----------|----------|------|
| G1 | API 层缺失 | 基础完成 | LF-0.3 | 已新增 FlowPlan/Node、scheduling action、instance evidence、task scheduling intent 最小 REST API。 |
| G2 | FlowPlan / Node 模型缺失 | 已完成 | LF-0.2 | 模型、发布图校验、日期模板、自然触发和实例定义锚点均已完成；旧 `AssetDependency` 运行时路径已移除。 |
| G3 | 调度意图主动投递协议不完整 | 基础完成 | LF-0.3 / LF-1.0 | 不可变 intent、版本化 payload、单路由、DB/HTTP/MQ 边界、claim、fencing、退避和死信已实现。1.0 还需真实 PostgreSQL 下验证 `DATABASE_TABLE`，以及真实网络下验证 HTTP 至少一次投递与下游幂等。具体 MQ 产品绑定不属于 1.0。 |
| G4 | Node 级重跑和恢复能力不足 | 已完成 | LF-0.4 | 已支持 task/published node 局部重跑，以及 `FULL_SCOPE` 和单失败节点 `FAILED_NODE_CASCADE` 两种替代恢复；非闭合 DAG 或多失败节点请求会拒绝并要求全范围恢复。 |
| G5 | 补数缺少批次、并发和级联策略 | 已完成 | LF-0.4 | 完整 Flow 与 Node 子图补数均已纳入统一批次模型，支持不可变节点范围、日期准入、级联、安全整日跳过、控制和失败替代恢复；审批上限按当前决策暂缓且不阻塞专题退出。 |
| G6 | Target snapshot baseline 与节点绑定不足 | 基础完成 | LF-0.2 / LF-0.4 | intent 携带目标资产、定义锚点、baseline 和必须写入逻辑完成 snapshot 的归因属性；确认按历史事件而非任意 latest 推进，后续只由匹配 snapshot 驱动 DAG。 |
| G7 | 补偿扫描还不够生产化 | 基础完成 | LF-0.5 / LF-1.0 | delivery/source 指标、死信 API、双轨对账、连续补扫、投影重放及 source-aware 超时门禁已实现；真实 PostgreSQL 中断、确认重扫和 source offset 恢复已通过整体 E2E。 |
| G8 | 查询视图和审计视图不足 | 部分完成 | LF-0.5 / LF-1.0 | action、补数、snapshot 证据和 delivery 死信已可查。1.0 只补齐阻塞原因和 source 对账 API；Flow 聚合、稳定游标、通用 Web UI 后移。 |
| G9 | Flow 级隔离和并发策略未固化 | 部分建模 | LF-1.1+ | `FlowPlan.owner/flowSpaceCode` 已记录归属。`LF-1.0` 是单团队受信环境，不强制可信身份和 RBAC；后续仍按 Flow 增加轻量授权与配额，不引入重型租户层。 |
| G10 | 真实湖格式 snapshot 归因元数据采集不完整 | 已完成 Paimon 目标 | LF-1.0 | 通用 source SPI、Paimon properties/delta-manifest/offset、分区路径规范化及可复用 writer adapter 已完成；流式 checkpoint、批式提交、Node 子图补数和真实 epoch fencing 已验证。Iceberg/Hudi 后移至 1.1+。 |
| G11 | Action snapshot 与正常自然触发未隔离 | 已完成 | LF-0.4 / LF-0.5 | 已按持久化 `intentKey -> triggerType` 分类；补数、恢复、重跑只推进所属实例，非法/中间/维护 snapshot 失败关闭；action 查询也不再把未归属的 latest 推进显示为成功。日期隔离要求 FlowPlan 使用 `${bizDate}` 分区资产。 |
| G12 | 跨实例目标资产日期准入缺失 | 已完成 1.0 目标 | LF-0.5 / LF-1.0 | `targetAssetKey + bizDate` 唯一槽位、行锁、过期重占和 holder fencing 已实现，并已通过双 scheduler PostgreSQL 竞争验收；它只解决逻辑日期冲突，不替代 G20 的物理表 writer 唯一绑定。 |
| G13 | 发布版本控制策略未执行 | 已完成 1.0 目标 | LF-0.5 / LF-1.0 | `PARALLEL`、`SERIAL_WAIT` 和 `maxActiveInstances` 已通过真实 PostgreSQL 并发验收。`SERIAL_DISCARD`、`SERIAL_PRIORITY` 和 `dedupeWindow` 后移至 1.1+。 |
| G14 | 调度决策可观测性不足 | 已完成 | LF-0.5 | 已按低基数结果记录发布版本检查、自然触发决策和耗时，按稳定状态/类别聚合 task 与补数等待积压，并输出高维结构化证据和 Prometheus 初始告警基线。 |
| G15 | 默认分支 CI/CD 与 Agent 约束失真 | 已完成 | 工程基线 | workflow 已迁移到 `master`，统一 JDK 17 + `./mvnw`，修复废弃 action，强制 Service 覆盖率门槛并在 CI 成功后构建产物；Copilot agent 已移除执行器和下游状态模型。 |
| G16 | Snapshot 超时与 source 健康未联动 | 已完成 | LF-1.0 | 已持久化表级 `SnapshotSourceHealth`，分区目标按规范化表键绑定受管 source；确认窗口结束后只接受截止时间之后、offset 已追平且投影一致的 `HEALTHY` 证据。缺失、过期、`REPAIRABLE` 或 `BLOCKED` 证据保持 task `SCHEDULED`，独立暴露 source 状态，不产生 `SNAPSHOT_NOT_ADVANCED`。 |
| G17 | 整体高可用与恢复 E2E 缺失 | 高可用目标已完成，整体部分完成 | LF-1.0 | `lakehouse-flow-e2e` 已贯穿真实 PostgreSQL、HTTP、MySQL CDC、Flink/Paimon 普通/补数链路、checkpoint 重启、双 scheduler 中断恢复、claim 重占、source offset 串行化和新旧 fencing；剩余范围是 `DATABASE_TABLE` 与完整 action 集合。 |
| G18 | 1.0 契约与 migration 策略未冻结 | 开发候选完成 | LF-1.0 | `/api/v1` 路由 surface、两类 intent JSON Schema、未知字段、版本升级、下游幂等和 V1-V23 migration checksum 回归已完成；只剩整体 E2E 反馈与最终冻结。 |
| G19 | 流批混合节点与输入边界契约缺失 | 已完成 | LF-1.0 | `ScheduleNode` 已增加唯一的引擎无关 `processingMode=STREAMING|BATCH`；普通流式节点不创建逐 snapshot task，批式节点按流资产证据、同实例批父 task 证据和额外依赖统一门禁，`SchedulingIntent` 1.3 持久化并交付完整 input snapshot/watermark vector。补数或重跑显式选中的流节点仍生成一次回放 intent；Flow/intent 不含 Flink/Spark 字段。 |
| G20 | 平台作业生命周期与单表单写入者缺失 | 已完成 | LF-1.0 | V22 固化 table/writer 双唯一约束、独立 `JobControlIntent` / delivery 和单调 writer epoch；真实 START/RESTART、Flink checkpoint offset 恢复、epoch 2 数据推进及 epoch 1 Paimon 提交拒绝均已通过整体 E2E。 |

## 已完成推进项

### G2 / LF-0.2: FlowPlan / Node 对象模型

本阶段目标是在定义侧建立稳定对象模型，为后续重跑、补数、API、下游调度意图交付提供锚点。

已完成：

1. 新增 `FlowPlan`：Flow 级隔离、归属、状态和当前发布版本。
2. 新增 `FlowPlanVersion`：不可变或准不可变的发布版本，保存图结构、触发策略、依赖策略、并发策略和确认策略。
3. 新增 `ScheduleNode`：调度节点定义，记录节点依赖、输入依赖条件、输出目标资产和 snapshot 确认策略。
4. 新增持久化迁移和 Repository。
5. 新增最小 `FlowPlanService`：创建草稿 Flow、创建草稿版本、添加节点、发布版本、查询版本节点。
6. 增加单元测试，保证定义创建、版本发布和节点绑定语义稳定。
7. `WorkflowInstance` / `TaskInstance` 已支持可选 `flowPlanVersionId` / `scheduleNodeId`，node rerun 生成的实例可追溯到发布定义。
8. 移除旧 `AssetDependency`、Repository 和 `DependencyEvaluationService` 运行时路径，自然触发统一进入发布 FlowPlan；旧 schema 表仅保留数据库升级兼容，不再读取。

后续增强：

1. 增加按 FlowPlan、版本、节点维度的实例聚合查询视图。

暂不做：

1. 不接入真实执行器。
2. 不引入复杂租户模型。
3. 不把 workflow/task 状态改造成执行状态机。
4. 不一次性重构所有旧 workflow/task 字段，先通过新增模型向目标架构收敛。

### G3 / LF-0.3: 最小 API 与调度意图交付

本阶段目标是通过 API 暴露 FlowPlan / Node 管理和审计能力，并由 Lakehouse Flow 主动、可靠地发布调度意图。下游只通过轮询专用库表、等待接口调用或消费 MQ 被动接收，不从 Lakehouse Flow 查询和抢占 READY 任务。

已完成：

1. 新增 `FlowPlanController`：创建 FlowPlan、创建版本、添加节点、发布版本、查询版本节点、查询最新发布版本。
2. 新增 `SchedulingActionController`：暴露 workflow/node 重跑、workflow/node 补数、补数批次暂停/恢复/取消、workflow/task 取消、task 跳过、snapshot 重检。
3. 新增 `SchedulingInstanceController`：查询 workflow/task 调度实例和 snapshot 证据。
4. 新增不可变 `SchedulingIntent`：固定 task/workflow、发布版本、节点、业务日期、受管目标资产和首次发布 baseline，并以 task instance 建立唯一幂等关系。
5. 新增 `SchedulingIntentDelivery`：只记录 `DATABASE_TABLE` / `HTTP` / `MQ` 等传输通道的发布证据，不保存或推断下游执行结果。
6. 新增内部 `SchedulingIntentOutboxScanner`：批量扫描 READY 决策，按 batch、workflow、task 顺序加锁，并主动发布数据库 outbox。
7. 数据库 outbox 发布与 baseline 冻结、task `SCHEDULED`、workflow 审计状态和 backfill item 交付状态在同一事务内提交。
8. 发布使用稳定 `intentKey` 和 task 唯一约束实现幂等；重复扫描返回原 intent，不重采 baseline。
9. 取消 workflow、暂停/取消补数批次和未获日期准入的 backfill item 均无法进入 outbox。
10. 从 `TaskInstance` 移除下游 `claimOwner` / `claimKey` / `deliveryKey` 等传输字段；V16 migration 建立独立 outbox 表并清理旧字段。
11. `SchedulingIntentController` 只保留按 task 查询已发布 intent 的审计接口，不再提供 `/ready`、`/claim`、`/deliver`。
12. action 详情联查同时暴露不可变 intent id/key、投递通道、投递状态和发布时间，且与 snapshot 结果分开表达。
13. 新增 Mockito 单元测试，覆盖内部批量发布、发布幂等、baseline 冻结、补数/取消门禁、缺失目标资产拒绝和只读 API。
14. 新增版本化 `instructionPayload`，完整携带 identity、definition、schedule、target baseline、格式无关的 `requiredSnapshotChangeType`，以及下游必须写入的 `requiredSnapshotProperties`。
15. 新增 `SnapshotEvidenceService`，扫描 baseline 后的持久化事件序列；未打标外部写入、其他 intent 和 `COMPACT` 不再误确认当前 task。
16. 分区目标必须同时具备 `changedPartitions` 证据；匹配 snapshot 后出现更晚的外部 snapshot 也不会覆盖历史确认依据。
17. 下游契约固化在 `SCHEDULING_INTENT_CONTRACT.md`，数据库、HTTP 和 MQ 必须使用同一 payload。
18. 新增统一 `SchedulingIntentPublisher` SPI；每条 intent 只选择一个 outbound route，避免数据库、HTTP、MQ 被同一部署重复消费。
19. 新增 `SchedulingIntentDeliveryScanner` 和 V19 delivery 状态：短事务 claim、随机 fencing token、过期重占、指数退避、最大尝试和 `EXHAUSTED` 死信。
20. HTTP publisher 使用 Java 17 `HttpClient`，仅接受 2xx ACK 并忽略 response body；MQ publisher 通过 broker-neutral gateway 接入具体消息产品。
21. delivery API 审计新增 attempt、lastError、nextAttemptAt 和 deadLetteredAt；这些字段不进入 `TaskInstance`，也不参与 snapshot 结果判断。
22. publisher 不会在 `publicationAdmission.leaseExpiresAt` 之后下发旧 intent；传输采用至少一次语义，下游继续以 `intentKey` 做启动幂等。

当前 API 边界：

1. 所有写入型调度意图操作由 Lakehouse Flow 内部发起；REST scheduling-intent API 只读。
2. baseline 必须在首次发布前冻结，发布重试不得更换 baseline。
3. HTTP 响应、MQ broker ACK 或数据库 outbox 可见性只证明传输，不表示任务开始、运行、成功或失败。
4. 不接受 executor callback，不暴露 `RUNNING/SUCCESS/FAILED` 这类执行状态。
5. task 最终结果只根据其受管目标资产中可归属于该 intent 的 snapshot 是否相对 baseline 推进确认，DAG 也只消费该匹配证据。
6. `RECHECK_SNAPSHOT` 只刷新 snapshot 证据，不产生新的调度意图。
7. 补数暂停/取消只阻断尚未发布的 intent；已发布 intent 不声称撤回，继续由 snapshot 推进确认。

后续增强：

1. Flow/Node 级实例聚合、稳定游标和 Web 运维视图按当前决策后移。
2. `LF-1.0` 使用 `DATABASE_TABLE + HTTP` 完成整体 E2E；具体 MQ 产品绑定后移。
3. `LF-1.0` 通过 Flink writer-side adapter 将真实 Paimon source 纳入系统级 E2E；Iceberg/Hudi source 后移。

暂不做：

1. 不实现真实执行器。
2. 不引入任务运行状态回调。
3. 不把调度意图 API 设计成供下游拉取任务的队列系统或资源调度系统。

### G7 / LF-0.5: 投递可观测性与 Source 对账

已完成：

1. delivery publisher 按 channel/outcome 记录尝试次数和耗时，并通过一次分组查询刷新所有 channel/status 当前记录数。
2. 提供只读 `GET /api/v1/scheduling-intent-deliveries/dead-letters`，支持 channel 和 limit 筛选；返回值只表达传输死信。
3. source scan 记录成功/失败、耗时和新增事件数；reconciliation 记录 pending offset、健康状态、retention gap 和投影不一致。
4. `LakehouseSnapshotSource.inspectPosition` 将 earliest/latest、lag 和 retention gap 解释权固定在格式适配器；Paimon 已实现连续数字 snapshot id 语义。
5. 周期性对账核验 live source、durable offset、最新物理 event/state 和最新 typed data event/state。
6. `UNINITIALIZED` / `LAGGING` 复用正常原子摄入链路补偿；物理或业务数据 AssetState 缺失/漂移时重放对应 durable event。
7. `RETENTION_GAP`、`OFFSET_AHEAD`、offset 缺少 event、source latest 与 event 不一致均保持 `BLOCKED`，不得跳 offset 或构造 snapshot。
8. Mockito 测试覆盖所有 reconciliation service public 方法、三类运维结果、指标和死信筛选。
9. V20 将 `LakehouseEvent.dataChange` 提升为 typed 字段，并把 `AssetState` 拆成物理观察轨和业务数据轨；只从可确定分类的 durable data event 回建已有表/分区业务轨，compaction 只推进表级观察轨，不能满足依赖、创建分区状态或确认 intent。

退出规则：单元级功能已达到 G7 基础退出标准；真实 Paimon/PostgreSQL 的指标抓取、source-aware 超时、锁竞争和故障恢复已上升为 `LF-1.0` 整体发布门槛。

### G10 / LF-0.5: 多湖 Snapshot Source

本阶段目标是让调度核心只消费统一 snapshot 事实，湖格式差异全部留在 integration adapter；Paimon 是首个落地实现，不是公共模型的默认格式。

已完成：

1. 新增 `LakehouseSnapshotSourceProvider` / `LakehouseSnapshotSource` / `LakehouseSourceIdentity` / `LakehouseSnapshot`，source offset 保持 opaque，排序由格式适配器提供。
2. `EventIngestionService` 和 `LakehouseSnapshotScanner` 动态消费全部 provider；offset 的 `sourceType` 不再写死为 `PAIMON`，不同 source table 独立推进。
3. 移除生产环境 mock snapshot，接入 Apache Paimon 1.3 Catalog API，按 batch 顺序读取 earliest/latest 之间的每个 snapshot。
4. 真实读取 snapshot properties、commit 元数据和 delta manifest entries，并将受影响分区稳定映射到 `changedPartitions`。
5. 当已保存 offset 与 Paimon 最早保留 snapshot 之间存在缺口时失败关闭，防止静默漏过资产变化和调度决策。
6. 下游契约升级为 1.2：原生 `commitKind` 只用于审计；适配器输出格式无关 `dataChange`，确认 payload 使用 `requiredSnapshotChangeType=DATA`。历史 1.1 Paimon 事件保留 `APPEND/OVERWRITE` 兼容判断。
7. Paimon 支持多 catalog、多 table 配置，每张表拥有独立 `(sourceType, sourceName)` offset；认证材料由运行环境提供。
8. Mockito 单元测试覆盖通用摄取、格式排序、失败前缀、source identity、防重复配置、Paimon properties、delta-manifest 分区和 retention gap。
9. `snapshotProperties` 与 `changedPartitions` 已提升为统一 `LakehouseSnapshot` 强类型字段，由公共 mapper 投影到事件，避免新增格式复制隐式 payload 键约定。
10. 调度 `snapshotId` 明确定义为适配器归一化的单调坐标：Paimon 使用 snapshot id，Iceberg 预留 sequence number，Hudi 预留 timeline instant；原生非顺序 id 留在 payload。
11. source offset 在排序和事务推进中统一使用格式 adapter comparator；不同格式的 source 不得声明相同逻辑资产键。
12. 已确认 Paimon 1.3 普通 SQL/标准 commit 链路不能假定会自动注入 Lakehouse Flow 的单次 snapshot 归因属性；下游需 writer-side adapter 在满足本次逻辑输入边界的 commit 写入归因属性，且不得以 `commitUser` / `commitIdentifier` 弱替代。
13. source offset 更新会锁定既有 `(sourceType, sourceName)` 行，配合数据库唯一约束阻止多调度实例并发扫描造成位点倒退。
14. source 首次接入默认从 `LATEST` 建立当前事实，避免存量 snapshot 自动形成历史调度风暴；`EARLIEST` 仅允许显式配置，常规补数仍走统一批次模型。

G10 剩余验收项：

1. 在同一整体 E2E 中补齐完整 Flow/Node 子图补数，贯穿带属性 snapshot commit、事件入库、分区 AssetState、action 路由、intent 确认和 DAG 推进。

退出规则：Flink CDC 流式 ODS 和 Flink 批式 DWD/DWS/ADS 已使用统一 writer adapter 通过真实 Paimon 端到端验证，且 Flink/Paimon 依赖未进入 service/scheduler 核心；G10 的剩余 1.0 退出条件只有 action/补数真实闭环。Iceberg/Hudi 适配不是 G10 的 1.0 退出条件。

### G13 / LF-0.5: 发布版本策略执行

本阶段目标是让发布版本中冻结的 JSON 策略成为真实调度决策，而不是只用于展示。

已完成：

1. 新增统一 `FlowPlanPolicyService`，发布时校验策略，运行时按 task 的 `FlowPlanVersion` / `ScheduleNode` 锚点解析同一份不可变策略。
2. `confirmationPolicyJson.timeout` 使用 ISO-8601 Duration；节点值覆盖版本值，未配置时使用全局 `lakehouse-flow.snapshot-confirmation.timeout`。
3. `concurrencyPolicyJson.targetAdmissionLease` 驱动目标日期准入租约，并要求不短于有效确认窗口；未配置时与有效确认窗口一致。
4. `concurrencyPolicyJson.mode` 当前支持 `PARALLEL` / `SERIAL_WAIT`；`SERIAL_WAIT` 默认 `maxActiveInstances=1`，`PARALLEL` 可通过正整数 `maxActiveInstances` 限流。
5. 有限并发策略在首次 workflow intent 发布时锁定 `FlowPlanVersion`，再统计同版本 `SCHEDULED` workflow；达到上限时不发布 intent、不获取目标槽、不冻结 baseline，实例保持 READY 等待后续扫描。
6. 同一 workflow 的后续 root 或 DAG 节点不重复占用版本实例名额；workflow 由 snapshot 结果进入终态后自然释放名额。
7. intent payload 记录本次有效 `confirmationTimeout`、并发模式和上限，便于下游与审计侧解释 `leaseExpiresAt`，但这些字段不代表下游执行状态。
8. 暂不支持 `SERIAL_DISCARD`、`SERIAL_PRIORITY`、`priority` 和 `dedupeWindow`；发布包含这些未实现模式的版本会失败关闭。

G13 剩余增强：

1. `LF-1.0` 只验收 `PARALLEL` / `SERIAL_WAIT` / `maxActiveInstances` 的真实 PostgreSQL 并发语义。
2. 触发丢弃、优先级排队和去重窗口的审计语义暂不冻结，`SERIAL_DISCARD` / `SERIAL_PRIORITY` / `dedupeWindow` 后移至 1.1+。

### G14 / LF-0.5: 调度决策可观测性

已完成：

1. 新增 `FlowPlanDecisionMetrics`，按 `TRIGGER_POLICY_FILTERED` / `ASSET_UNMATCHED` / `MATCHED` 统计每个发布版本的 snapshot 检查结果。
2. 对匹配版本按 `EMITTED` / `BLOCKED_CONDITION` / `DEDUPLICATED` / `FAILED` 统计决策次数和耗时。
3. metric tag 只保留稳定枚举，不写 Flow code、asset key、snapshot id 或 trigger key；高维证据进入结构化日志和 `TriggerHistory`。
4. 评估异常记录 `FAILED` 后继续抛出，保持摄入事务和 source offset 失败关闭；指标不参与 snapshot 结果判断。
5. Mockito 和 `SimpleMeterRegistry` 测试覆盖两个 metrics public 方法、全部检查/决策结果以及异常传播。
6. 新增 `SchedulingBacklogMetrics`，通过两次分组查询刷新四类 task 非终态等待阶段和两类补数阻塞 gauge；不读取或标记自由文本原因。
7. README 已给出 decision、source、delivery 和 backlog 的 P1/P2 初始告警阈值，并明确正常阻塞指标只用于趋势与排障。

G14 退出标准：决策次数/耗时、异常失败关闭、非终态积压、补数稳定阻塞类别和初始告警口径均具备低基数指标与直接单元测试。已达到，后续只按真实运行反馈校准阈值。

### G15: CI/CD 与 Agent 工程基线

已完成：

1. 确认 Copilot workflow 只存在于过时的 `main` 分支，默认 `master` 推送不会触发；最近两次 Maven/Artifact workflow 又因 `actions/upload-artifact@v3` 在 job 初始化阶段失败。
2. 在默认分支新增 Maven Build、Repository Policy 和 Build Artifacts workflow，分别负责完整验证、仓库规则检查和验证通过后的 Boot JAR 产物。
3. GitHub Actions 统一 JDK 17、`./mvnw` 和当前 action 主版本，移除伪成功的可选 Checkstyle/依赖扫描步骤及不必要的 PostgreSQL service。
4. Service JaCoCo line 90%、branch 65% 门槛绑定 `verify` 阶段，低于门槛会直接使本地和 CI 构建失败。
5. 重写 Copilot custom agent，以当前进度文档为真源，明确禁止执行器、任务运行状态回调和旧 `AssetDependency` 路径。
6. `.mavenrc` 只在 macOS 探测 JDK 17，并在 Linux/CI 保留 `setup-java` 提供的 `JAVA_HOME`，消除 Wrapper 的平台耦合。
7. Boot 模块显式执行 Spring Boot `repackage`；Artifact workflow 在上传前校验 `JarLauncher`、应用 `Start-Class` 和 `BOOT-INF/lib`，拒绝只有几 KB 的不可执行薄 JAR。
8. 文档已按进度、架构、协议、操作和历史归档分层；当前技术栈与 Quickstart 只描述已实现能力。Compose 不再把 Flyway SQL 直接交给 PostgreSQL 按文件名字典序执行，空库已由应用按 1.0 至 20.0 顺序迁移并通过健康检查。

退出标准已达到（2026-09-13）：本地 `./mvnw clean verify` 通过覆盖率门槛；默认分支 `Code Quality Checks`、`Maven Build and Test`、`Build Artifacts` 三条 workflow 均真实运行成功；上传前的可执行 JAR 校验通过，远端 artifact 压缩包为 98,078,717 bytes。

## 最近完成推进项

### G16 / G19 / LF-1.0: Source-aware 结果门禁与流批混合 DAG

本阶段补齐 snapshot 结果可信性和引擎无关的流批混合调度契约，仍然只记录、发布和确认调度意图，不读取下游作业状态。

已完成：

1. 新增表级 `SnapshotSourceHealth` 与 V21 migration，持久化 source identity、规范化物理表键、offset/projection 状态、对账位置和实际检查时间。
2. snapshot 确认窗口结束后，只有在截止时间之后取得 `HEALTHY` 且 durable offset 已追平 source latest offset 的证据，才允许结束为 `SNAPSHOT_NOT_ADVANCED`。
3. source 未受管、证据过期、可修复、阻塞或 offset 未追平时，task 保持 `SCHEDULED`；确认结果单独返回 `REPAIRABLE` 或 `SOURCE_BLOCKED`，不释放目标准入，也不把不可观测性伪装成下游失败。
4. `ScheduleNode` 新增 `processingMode=STREAMING|BATCH`，旧定义通过 migration 和 Java 默认值保持 `BATCH`；API 可写入并读取该字段，Flow/intent 不保存执行引擎类型。
5. 普通 snapshot 推进只为 `BATCH` 节点创建 task；纯流式 Flow 不创建 workflow/task/data-processing intent，常驻流作业继续由平台生命周期控制。
6. 混合 DAG 中，普通 `STREAMING` 父节点从日期解析后的 `AssetState` 提供业务 snapshot/watermark；`BATCH` 父节点必须提供同 workflow 的 `SNAPSHOT_CONFIRMED` task 和 observed snapshot；显式 action 创建的流式回放父 task 也必须在同实例确认。
7. 节点额外 `inputDependencySpec` 的满足证据与直接父边合并为 `InputSnapshotEvidence`，发布前再次完整校验，并冻结进 `SchedulingIntent` 1.3 的 `processing.inputSnapshotVector`。
8. 补数/重跑入口的父依赖绕过已持久化到 task，确保 scanner 发布时不会重新阻塞；该入口仍需由自身目标 snapshot 推进确认。
9. 资产推进会重新评估同业务日期的等待节点；输入不完整时不会占用 `targetAssetKey + bizDate` 准入槽位、采集 baseline 或写入 outbox。
10. 新增 Mockito 单测覆盖 source 证据缺失/过期/追平、source 阻塞保持待定、混合父边汇聚、流式回放父节点、批父节点实例隔离、action 入口和 intent 输入向量。

代码与单测退出标准已达到；真实 Flink/Paimon writer、PostgreSQL 多节点竞争和 DB/HTTP 整体 E2E 继续由 G10/G17 验收，不在这里提前宣称完成。

### G20 / LF-1.0: 平台作业控制与单表单 Writer

本阶段把“平台启动/重启作业”和“数据处理调度意图”拆成两个独立领域，同时用同一个物理表 writer binding 和单调 epoch 阻止多个写入世代被误认为当前产出。Lakehouse Flow 仍不执行作业，也不读取引擎状态。

已完成：

1. V22 新增 `WriterJobBinding`，以全局唯一 `writerJobKey` 和规范化 `tableAssetKey` 双唯一约束固化一张物理表只有一个 writer 定义；binding 只记录调度授权，不保存运行中、成功或失败等引擎状态。
2. `ScheduleNode` 显式引用 `writerJobKey`；Flow 发布会校验目标物理表、节点 `STREAMING|BATCH` 模式和全局 binding 一致，不同 writer 指向同一物理表在定义或发布阶段失败关闭。
3. `SchedulingIntent` 1.3 增加 `writerJobKey` / `writerEpoch`，payload 同时交付这两个字段及必须写入目标 snapshot 的属性；snapshot 必须精确匹配 intent、writer 和 epoch 才能确认。
4. BATCH 数据处理 intent 在发布事务中锁定 binding、分配新的独占 epoch 和有期限 holder；STREAMING 回放 intent 复用平台已启动的当前流式 epoch。确认完成或健康 source 下确认未推进后，仅按 holder fencing 释放批式占用。
5. 新增独立 `JobControlIntent` 1.0 和 delivery 表。`START_JOB` / `RESTART_JOB` 只绑定 writer、物理表、前后 epoch 和控制审计，不创建 workflow、task、DAG 节点或业务日期。
6. start/restart 使用调用方 `requestKey` 幂等，并在 PostgreSQL 悲观锁下单调分配 epoch；`START_JOB` 只允许零世代 binding，后续切换必须使用 `RESTART_JOB`。
7. 控制 intent 与数据处理 intent 使用独立 outbox，但复用同一 claim、随机 fencing token、租约过期重占、指数退避、最大尝试和死信算法，以及同一个 DB/HTTP/MQ publisher SPI。
8. 新增控制投递 scanner 和控制 snapshot 确认 scanner。传输 ACK 只更新 delivery；控制结果只接受 baseline 之后精确匹配 `job-control-intent-key + writer-job-key + writer-epoch` 的业务数据 snapshot。
9. 控制确认窗口结束时复用 G16 source 健康门禁：只有 source 已追平且投影健康才写 `SNAPSHOT_NOT_ADVANCED`；source 缺口保持 `WAITING` 并暴露 `SOURCE_BLOCKED` 证据。
10. 当前有效控制 epoch 的业务 snapshot 可作为正常资产推进进入自然 DAG；旧 epoch、伪造 writer 或 compaction snapshot 不能确认新 intent，也不能触发自然调度。
11. 新增 writer binding 创建/查询、平台 start/restart 和控制 intent 审计 API；所有新增 service public 方法都有直接 Mockito 单测，控制投递和确认 scanner 也有编排测试。
12. 新增独立 `lakehouse-flow-flink-paimon` 下游模块，`WriterCommitContext` 对控制/数据处理属性失败关闭，流式 checkpoint 与批式结束提交统一经过 `FlinkPaimonWriterAdapter`。
13. `JdbcWriterEpochFence` 在 Paimon commit 前锁定 `writer_job_binding`，并在物理 commit 返回后才释放 PostgreSQL 行锁，关闭“校验旧 epoch 后、新 epoch 分配前”竞态。
14. 整体 E2E 已执行真实 `RESTART_JOB`：旧 Flink JobID 取消后从保留的外部 checkpoint 恢复 source offset，新作业使用 epoch 2 产出 ODS snapshot 2 并推动第二轮 DWD/DWS/ADS；使用 epoch 1 的真实 Paimon probe 在发布 snapshot 前被拒绝。

G20 代码、单测和系统级退出标准已经达到。后续完成的多 scheduler 竞争、claim 重占和事务中断归入 G17/R3；`DATABASE_TABLE` 与 HTTP 超时/死信仍属于 R4，不阻塞 G20 本身退出。

### G17 / LF-1.0: 真实 Paimon 闭环与高可用恢复

本阶段完成 LF-1.0 顶层目标 1、2。E2E 中的执行端只在被动收到 HTTP 意图后提交测试 Flink 作业，不进入生产模块，也不向 Lakehouse Flow 回传执行状态。

已完成：

1. 订单到 GMV 链路增加真实 Node 子图补数：选中 DWD 作为显式入口时允许绕过其父节点，但 DWD 自身必须提交匹配 intent/writer/epoch 的 Paimon snapshot；DWS、ADS 继续等待同一补数实例的直接父 snapshot 确认。
2. 补数链路通过 Flink 有界批作业依次写入 DWD、DWS、ADS，Paimon source 读回真实 properties、`dataChange` 和分区证据，三个 task 与批次最终均为 `SNAPSHOT_CONFIRMED` / `COMPLETED`。
3. source 摄入在任何事件投影前初始化并锁定 `(sourceType, sourceName)` offset 行，锁覆盖 event、AssetState、DAG/Flow 评估和 offset 提交；并发节点不得倒退 durable offset。
4. 数据 task 和 job-control snapshot 确认均通过 PostgreSQL `FOR UPDATE` 串行化；已确认对象支持幂等重扫，避免两个 scanner 重复释放准入或推进 DAG。
5. 新增 `SchedulerRecoveryE2EIT`，用两个独立 scheduler 应用上下文连接同一 PostgreSQL，验证事务提交前中断回滚、source offset 重放和并发单调推进。
6. 验证 intent 提交后 HTTP 调用前中断、HTTP 送达后 ACK 入库前中断、claim 过期重占、旧 fencing token 拒绝，以及下游按 `Idempotency-Key` 去重。
7. 验证两个 scheduler 竞争同一 Flow `maxActiveInstances` 和同一 `targetAssetKey + bizDate` 时只有一个首发；过期 holder 可重占，旧 holder 不能释放新租约。
8. 验证 snapshot event 已落库、task 确认前中断后，两个 scanner 竞争重扫只产生一份状态转换和证据；delivery 是否 ACK 不影响 snapshot 结果。

R1 与 R3 退出标准已达到。G17 作为整个 LF-1.0 E2E 总项仍保持部分完成，因为 R2 的其余 action 和 R4 的 `DATABASE_TABLE`、HTTP 超时/死信尚未验收。

### G4 / G5 / LF-0.4: Action 与补数控制能力增强

本阶段目标是把 DolphinScheduler 调度侧常见 action 能力收敛成 Lakehouse Flow 的 snapshot 推进语义：action 只改变调度记录或生成新的调度意图，不直接执行任务，也不以外部任务状态作为成功失败依据。

已完成：

1. 新增 `RERUN_TASK`：基于已有 task instance 生成新的 task scheduling intent。
2. `RERUN_TASK` 会创建 action-owned workflow wrapper 和新的 task instance，避免覆盖原 task 的 snapshot 证据。
3. 新 task intent 保持 `READY_TO_SCHEDULE`，等待 Lakehouse Flow 内部 outbox scanner 主动发布。
4. 新 task 会复制源 task 的 `targetAssetKey`；baseline 在首次 outbox 发布前捕获。
5. 新增 `/api/v1/scheduling-actions/rerun-task` API，并覆盖 service/API 单元测试。
6. 新增 `/api/v1/scheduling-actions/rerun-node` API，支持对 published `FlowPlanVersion` 中的 `ScheduleNode` 生成局部重跑意图。
7. `SchedulingAction` 已记录 `flowPlanVersionId` / `scheduleNodeId`，让 node 级 action 可追溯到发布定义。
8. node rerun 生成的 `WorkflowInstance` / `TaskInstance` 已写入发布定义锚点，下游 task intent 响应也会携带这些锚点。
9. 新增 `/api/v1/scheduling-actions/backfill-node` API，支持基于 published `FlowPlanVersion`、起始 `ScheduleNode`、日期区间和级联策略生成补数 task intents。
10. 新增 `BackfillBatch` / `BackfillItem`，记录 node 补数从 actionKey 到 date/node task intent 的展开证据。
11. 新增 `/api/v1/backfills/by-action/{actionKey}` 和 `/api/v1/backfills/{backfillBatchId}/items`，可查询补数批次和明细。
12. 新增 `PAUSE_BACKFILL` / `RESUME_BACKFILL` / `CANCEL_BACKFILL` action 和 API，所有控制请求均保留幂等审计记录。
13. `SchedulingIntentService` 已将补数批次状态纳入内部 outbox 发布判断，暂停和取消会真实阻断尚未发布的调度意图。
14. 补数 intent 成功写入 outbox 后，`BackfillItem` 更新为 `INTENT_DELIVERED`，可与 `INTENT_READY` / `CANCELLED` 区分。
15. 批次控制和 intent publication 对同一 `BackfillBatch` 加悲观写锁，避免暂停/取消与发布并发穿透。
16. 每个业务日期只将用户选中的 `startNode` 置为 `READY_TO_SCHEDULE`；所有级联下游先进入 `WAITING_SNAPSHOT`。
17. `DagProgressionService` 已统一使用 G19 混合输入证据：同实例批父 task 必须确认，普通流式父节点使用日期解析后的资产 snapshot/watermark；缺失证据继续阻塞。
18. 级联子图若在汇聚节点遗漏其他直接父节点，会在 action 展开前被拒绝；不会生成永远无法满足的等待项。
19. `${bizDate}` / `${biz_date}` 在每个补数日期分别解析，避免多日期任务共享错误目标资产。
20. `BackfillBatch` 新增 `progressionMode` / `maxActiveDates`，明确控制的是可交付业务日期数量，不是下游执行资源。
21. `SERIAL` 只释放最早一个日期，`PARALLEL_WITH_LIMIT` 只释放前 N 个日期，其余起点记录为 `WAITING_CONCURRENCY`。
22. 一个业务日期的选中子图全部 `SNAPSHOT_CONFIRMED` 后才释放下一个日期；单个节点 `SNAPSHOT_NOT_ADVANCED` 会将批次标记为 `FAILED` 并停止补位。
23. 外部资产事件和 DAG 推进都会识别并发等待状态，不能绕过日期配额；暂停期间不补位，恢复时重新计算槽位。
24. 新增 V9 schema migration、批次查询字段和 Mockito 单元测试，覆盖串行、限流、暂停、失败与完成路径。
25. 新增 `RECOVER_BACKFILL`：只接受包含 `SNAPSHOT_NOT_ADVANCED` 证据的 `FAILED` 批次，从最早失败业务日期到原结束日期创建新的替代批次。
26. 恢复不会修改原批次、原 item 或原 task；替代批次记录 `sourceBackfillBatchId` / `recoveryAttempt`，每个失败批次最多存在一个直接替代批次。
27. 替代批次重新生成原选中子图，继续只允许起点绕过其上游；级联下游必须等待新 workflow 实例内父节点 snapshot 确认，不能复用旧实例确认。
28. 恢复 task 创建时不采集 baseline；由内部 outbox 首次发布时重新采样当前目标资产 snapshot，之后仍只以 snapshot 是否推进确认结果。
29. 新增 `/api/v1/scheduling-actions/recover-backfill` 和 V10 lineage migration，批次查询已暴露恢复来源与次数。
30. Node 补数新增 `skipPolicy`；默认 `NONE` 保持强制重发，显式 `SKIP_FULLY_CONFIRMED_DATES` 才会评估历史确认凭据。
31. 跳过粒度固定为完整业务日期：同一 published `FlowPlanVersion`、业务日期、`ScheduleNode` 和日期解析后目标资产必须对全部选中节点存在带 observed snapshot 的 `SNAPSHOT_CONFIRMED` task 证据；任一节点缺失就完整展开该日期。
32. 历史确认只用于省略整个日期，绝不作为新 workflow 中的父节点确认来释放下游；因此不会破坏“仅补数起点可绕过上游”的 DAG 原则。
33. `BackfillBatch` 通过 V11 字段持久化跳过策略、跳过日期数和逐日证据；跳过日期不占用串行/限流日期槽位，全部日期都跳过时批次直接进入 `COMPLETED` 且不生成空 workflow。
34. Backfill query API 已暴露跳过策略、计数和证据，Mockito 测试覆盖完整跳过、部分证据不跳过、跳过日期不占槽位和非法策略拒绝。
35. 新增统一补数 scope：`FULL_FLOW` 表示完整发布 DAG，`NODE_SUBGRAPH` 表示从指定节点开始的闭合子图；两个 API 入口都创建相同的 `BackfillBatch` / `BackfillItem`。
36. `BackfillBatch` 持久化 `entryNodeCodes` 和 `selectedNodeCodes`：完整 Flow 的全部根节点是日期入口，Node 子图只有用户选中的起点可以绕过其图外上游。
37. 完整 Flow 补数已支持 `PARALLEL` / `SERIAL` / `PARALLEL_WITH_LIMIT`、安全整日跳过、暂停/恢复/取消和失败替代批次，不再存在绕开批次模型的直接日期循环。
38. 日期槽位按业务日期计数而不是按入口节点计数；完整 Flow 即使有多个根节点，也会在日期准入时一起释放，并在整日选中节点全部确认后归还一个槽位。
39. `FULL_SCOPE` 恢复严格继承源批次的 scope、入口节点和选中节点集合；V12 migration 为旧 Node 批次回填 scope 和节点范围，查询 API 同步暴露这些审计字段。
40. 新增 `SchedulingActionQueryService`，按 action key 返回 action 请求、调度侧应用结果、相关补数批次和逐 task snapshot 证据。
41. action detail 将 `action.status` 与 `snapshotAdvanced` 分开表达：前者只表示调度命令处理结果，后者只采用严格 snapshot-intent 归因已经确认的调度终态。
42. 补数创建、恢复 action 通过 action key 关联生成批次；暂停、恢复、取消 action 通过结构化 `backfillBatchId` payload 关联被控制批次，均可查看同一批次证据。
43. 新增 `GET /api/v1/scheduling-actions/{actionKey}` 和 `GET /api/v1/scheduling-actions`；列表支持按 `workflowCode`、`flowPlanVersionId`、`scheduleNodeId` 组合筛选，默认最多 50 条、硬上限 200 条。
44. action 列表不展开 task 证据，避免批量 N+1；详情按补数 item 顺序批量加载 task，关联 task 缺失时保留 item 锚点作为不完整审计证据。
45. workflow/task instance action 在应用时回写源实例的 Flow、发布版本、节点、业务日期和目标资产锚点，避免非补数 action 在 Flow/版本/节点筛选中静默缺失。
46. `RECOVER_BACKFILL` 新增显式 `recoveryStrategy`：缺省和兼容入口使用 `FULL_SCOPE`，也可选择 `FAILED_NODE_CASCADE`。
47. `FAILED_NODE_CASCADE` 只接受一个不同的 snapshot 失败节点；该节点作为新批次唯一显式入口直接 READY，其可达下游仍必须等待新 workflow 内父节点 snapshot 推进。
48. 失败分支经过汇聚节点但缺少其他直接父节点时拒绝恢复；存在多个失败节点时同样拒绝，并要求调用方显式使用 `FULL_SCOPE`，不做有歧义的自动选择。
49. 替代批次持久化 `recoveryStrategy`，V14 migration 为历史替代批次回填 `FULL_SCOPE`，command、batch query 和 action evidence API 均暴露该字段。
50. 恢复不复用旧 workflow 的 snapshot 确认；新 intent 在首次 outbox 发布前采集新 baseline，最终仍只以自身目标 snapshot 是否推进确认结果。
51. 新增 `SnapshotTriggerRoutingService`：通过 snapshot 中的 `intentKey` 反查不可变 intent；`BACKFILL` / `BACKFILL_RECOVERY` / `RERUN` / `RERUN_TASK` snapshot 禁止进入正常自然触发。
52. Lakehouse Flow 标记的 snapshot 只有在物理表、目标资产、业务日期、目标分区、final 属性和格式适配器 `dataChange` 分类全部匹配时才允许路由；孤儿、伪造、中间和维护 snapshot 均失败关闭。
53. snapshot 摄入会把表状态和 `changedPartitions` 对应的分区状态一起单调投影；历史日期补数不再推进当前日期的分区资产键。
54. action snapshot 证据视图只把 `SNAPSHOT_CONFIRMED` 显示为成功、`SNAPSHOT_NOT_ADVANCED` 显示为失败；较新的未归属 snapshot 保持未知。
55. 新增 `SchedulingTargetAdmission` 与 V18 migration，以数据库唯一键和悲观行锁串行化同 `targetAssetKey + bizDate` 的首次发布，所有触发类型走同一准入路径。
56. 只有获得槽位后才捕获 baseline 和写入 outbox；准入、baseline、intent、delivery 和 task 状态在同一事务提交，失败时共同回滚。
57. 目标 snapshot 严格确认或发布准入租约过期后释放槽位；租约过期允许后续调度重占，但只表示可再次下发，不声称旧下游工作已停止，也不强制把 source 阻塞的旧 task 标记为未推进。
58. 释放操作校验当前 holder task，晚到的旧 snapshot 不能释放新 holder；批量 scanner 遇到冲突会继续尝试后续独立任务，冲突任务保留 READY 等待重试。

### 补数专题退出标准

| 门槛 | 验收标准 | 当前结果 |
|------|----------|----------|
| 统一模型 | 完整 Flow、Node 子图和失败恢复统一使用 `BackfillBatch` / `BackfillItem`，并冻结入口、选中节点、日期范围和发布版本 | 通过 |
| DAG 不变量 | 只有显式补数入口可绕过图外上游；其余节点必须由同业务日期、同新 workflow 内全部直接父节点 snapshot 推进后释放 | 通过 |
| 日期推进 | `PARALLEL`、`SERIAL`、`PARALLEL_WITH_LIMIT` 按业务日期控制准入，失败后停止补位 | 通过 |
| 批次控制 | pause/resume/cancel 只影响尚未交付的调度意图，不伪装撤回已交付的下游工作 | 通过 |
| 安全跳过 | 只允许基于完整、同版本、同日期的历史 snapshot 证据省略整日，不进行部分节点跳过 | 通过 |
| 失败恢复 | 支持确定性的 `FULL_SCOPE` 和单失败节点 `FAILED_NODE_CASCADE`；多失败节点、缺父 join 和范围不闭合均 fail closed | 通过 |
| 审计查询 | action、替代批次、逐节点 intent 和 baseline/observed snapshot 证据可关联查询，action 状态与 snapshot 结果分离 | 通过 |
| 正常推进隔离 | 补数、恢复和重跑 snapshot 不产生额外正常实例；历史分区不推进当前日期分区状态；未归属 snapshot 不显示为成功 | 通过 |
| 质量门槛 | `./mvnw test` 零 failure/error/skip；service line >= 90%、branch >= 65%；每个显式 public service 方法都有直接单元测试调用 | 通过：360 tests，line 91.2%，branch 68.4%；显式 public service 方法直接调用检查通过，新增 source health、mixed-DAG evidence、writer binding 与 job-control service 均有直接 Mockito 覆盖 |

退出规则：以上门槛必须同时通过。达到后，补数专题只接受缺陷修复和整体能力带来的必要适配，不再独立扩展功能；开发回到整体版本差距。补数审批上限按已确认决策暂缓，真实 PostgreSQL/Flyway 验证归入 Lakehouse Flow 整体 Testcontainers E2E，二者都不阻塞本专题退出。

后续增强：

1. Flow/Node 级实例聚合、稳定游标和 Web 运维 UI 按当前决策后移。
2. 由部署增加具体 MQ broker gateway。
3. 补数审批上限暂缓，待核心功能闭环稳定后再设计。
4. 补数的真实 Flink/Paimon 整体 Testcontainers E2E 纳入 `LF-1.0`；Iceberg/Hudi source adapter 后移至 1.1+。

## Action 语义基线

| Action | 调度侧影响 | 下游影响 | Snapshot 影响 |
|--------|------------|----------|---------------|
| `RERUN_WORKFLOW` | 基于已有 workflow instance 生成新的调度决策。 | 下游看到新的调度意图；是否真实执行由下游决定。 | 不直接推进 snapshot，后续仍由目标资产 snapshot 推进确认。 |
| `RERUN_TASK` | 基于已有 task instance，或基于 published FlowPlanVersion + ScheduleNode 生成新的局部调度意图，并保留原审计记录。 | Lakehouse Flow 内部 publisher 主动发布新的 intent，下游被动接收。 | 首次发布前重新捕获 baseline，后续由目标资产 snapshot 推进确认。 |
| `BACKFILL_WORKFLOW` | 以 `FULL_FLOW` scope 创建统一批次，每个业务日期选择完整发布 DAG；全部 root 作为该日期入口，并共享日期准入、暂停/取消、跳过和恢复策略。 | 内部 publisher 只发布日期槽位和 DAG 门禁共同释放的意图。 | 每个节点首次发布前独立采集 baseline；父节点 snapshot 全部确认后才释放子节点，整日确认后归还日期槽位。 |
| `BACKFILL_NODE` | 从 published 版本选取起点和下游范围；仅起点跳过其上游，依赖不闭合的汇聚范围直接拒绝；日期按 `PARALLEL` / `SERIAL` / `PARALLEL_WITH_LIMIT` 准入。显式跳过策略只按完整日期省略已有完整确认凭据的子图。 | 下游只看到当前日期槽位和 DAG 门禁共同允许的意图；被安全跳过的完整日期不产生新意图。 | 每个新 intent 独立采集 baseline；日期内全部节点确认后才释放新的日期槽位。历史确认仅用于整日省略，不参与新实例 DAG 放行。 |
| `PAUSE_BACKFILL` | 将 `EXPANDED` 批次置为 `PAUSED`，内部 publisher 不再发布其待处理意图。 | 下游不再获得新的该批次意图；已发布意图不受影响。 | 不改变 snapshot，也不停止已开始的外部工作。 |
| `RESUME_BACKFILL` | 将 `PAUSED` 批次恢复为 `EXPANDED`。 | 尚未发布的意图重新进入内部 outbox 发布候选。 | 不直接改变 snapshot；恢复发布后仍按各 intent baseline 确认。 |
| `CANCEL_BACKFILL` | 将批次置为 `CANCELLED`，并取消其中尚未交付的 task intent。 | 不再交付新意图；已经交付的意图明确保留，不声称撤回。 | 已交付意图继续保留 snapshot 确认证据，未交付意图不采集 baseline。 |
| `RECOVER_BACKFILL` | 保留失败批次，从最早 `SNAPSHOT_NOT_ADVANCED` 日期创建单一替代批次；默认 `FULL_SCOPE` 重建原范围，`FAILED_NODE_CASCADE` 只重建唯一失败节点及依赖闭合的可达下游。多失败节点或缺父 join 拒绝并要求 `FULL_SCOPE`。 | 内部 publisher 发布一组全新的调度意图；失败节点是显式入口，级联下游仍受新 workflow DAG 门禁；旧批次待发布意图保持不可见。 | 新 intent 在首次发布前采集当前 baseline；不复用旧实例确认，仍只以各目标资产 snapshot 推进确认。 |
| `CANCEL_WORKFLOW` | 将调度实例标记为取消。 | 下游如果已消费，需要自行做幂等忽略或停止策略。 | 不改变 snapshot；若 snapshot 后续推进，也只能作为审计证据。 |
| `CANCEL_TASK` | 将任务调度实例标记为取消。 | 下游如果已消费，需要自行处理停止或忽略。 | 不改变 snapshot。 |
| `SKIP_TASK` | 将未确认任务标记为跳过。 | 下游不应再把该任务作为新的待调度意图处理。 | 不确认 snapshot 推进，只是调度侧跳过。 |
| `RECHECK_SNAPSHOT` | 重新扫描该 task baseline 后的目标 snapshot 事件并刷新确认结果。 | 不产生新调度意图。 | 仍要求匹配当前 intent 的属性、适配器 `dataChange` 分类和目标分区，不以任意 latest `AssetState` 代替。 |

## 验证基线

当前测试策略：service/API/integration 逻辑先使用 Mockito 单元测试隔离依赖；service 层每个 public 方法必须有直接测试入口。`LF-1.0` 必须再由 `lakehouse-flow-e2e` 的整体 Testcontainers E2E 贯穿 API/事件入口、PostgreSQL/Flyway、事务与约束、DB/HTTP 投递、Flink/Paimon 写读、snapshot 确认、DAG 和补数推进。E2E 验证的是 Lakehouse Flow 整体闭环，不归属于某个单独生产模块，也不用来替代当前单元测试。

2026-09-14 验证快照：JDK 17.0.12 下 `./mvnw clean verify` 全部通过，共 373 个单元测试和 5 个整体 Testcontainers E2E 场景，failure/error/skip 均为 0；service JaCoCo 为 line 91.2%、branch 68.4%、method 91.0%，构建门槛实际通过。所有显式 public service 方法保留直接 Mockito 测试入口。真实环境覆盖 V13-V22 PostgreSQL migration、HTTP、MySQL CDC、Flink/Paimon writer/source、两轮普通订单 DAG、START/RESTART、checkpoint source offset 恢复、旧 epoch 拒绝、Node 子图补数，以及双 scheduler 锁竞争、中断、claim/确认重占和事务回滚。`DATABASE_TABLE`、其余 action、HTTP 超时/死信仍待 E2E。

每次推进后至少执行：

```bash
./mvnw verify
git diff --check
```

较小改动可以先执行局部验证：

```bash
./mvnw -pl lakehouse-flow-service -am test
```

如果测试无法执行，必须在进度汇报中说明未验证项和原因。
