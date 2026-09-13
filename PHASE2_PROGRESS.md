# Lakehouse Flow 当前进度基准

**最后更新**: 2026-09-13
**基准用途**: 后续开发进度、差距检查和版本目标均以本文档为准。
**当前推进项**: 补数功能模型已达到退出标准；G2 已移除旧 `AssetDependency` 运行时路径，统一到发布 FlowPlan；G7 已补齐 delivery/source 指标、双轨 AssetState 对账和失败关闭补偿；G11/G12 已完成 snapshot 路由隔离和跨实例目标日期准入。G13 已完成基础策略执行，并对未实现的 mode、priority 和 dedupe window 失败关闭。G10 的整体 Testcontainers E2E 与 Iceberg/Hudi adapter、G8 Flow/Node 聚合与稳定游标均按当前决策后移。

## 目标边界

Lakehouse Flow 是基于 snapshot 推进模式的新一代调度系统。

它只负责调度侧决策、调度意图记录、调度审计和 snapshot 推进确认，不负责真实任务执行，不内置 executor，不接管下游资源队列。

系统判断成功或失败时，以目标数据资产的 snapshot 是否按预期推进为准。Workflow/Task 状态只用于调度决策、意图交付和审计视图，不能演变成执行状态机。

## 版本目标

| 版本 | 目标 | 验收口径 |
|------|------|----------|
| LF-0.1 | Snapshot 调度闭环原型 | 能基于事件、资产状态和依赖条件生成调度实例，并可确认目标 snapshot 是否推进。 |
| LF-0.2 | FlowPlan / Node 对象模型 | 有稳定的 Flow 定义、版本、节点、节点依赖和目标资产绑定模型，后续调度实例可追溯到发布版本。 |
| LF-0.3 | 最小 API 与调度意图交付 | 可通过 API 管理 FlowPlan、触发 action、查询实例和 snapshot 证据；Lakehouse Flow 主动发布不可变调度意图，下游通过库表、被调用接口或 MQ 被动接收。 |
| LF-0.4 | Action 能力增强 | 支持 workflow/node 级重跑、补数、跳过、取消、snapshot 重检；补数严格遵守 DAG snapshot 依赖，只允许选中起点绕过前置节点。 |
| LF-0.5 | 生产化调度控制面 | 支持补偿扫描、并发保护、Flow 级隔离、审计查询、可观测性和运维排障视图。 |

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
| 目标日期准入互斥 | 已完成基础版 | 正常、补数、恢复和重跑共享 `targetAssetKey + bizDate` 持久化槽位；先准入再冻结 baseline，snapshot 确认或确认超时后释放，冲突任务保持 READY 等待后续扫描。 |
| 发布版本策略执行 | 已完成基础版 | 版本 `confirmationPolicyJson` 和节点覆盖驱动确认窗口；版本 `concurrencyPolicyJson` 驱动目标准入租约和活跃 workflow 上限，超限任务保持 READY 且不冻结 baseline。 |
| FlowPlan 自然触发 | 已完成基础版 | 只有外部数据提交和合法非 action 最终 snapshot 评估 `PUBLISHED` 版本；action-owned、中间、孤儿和维护 snapshot 不进入自然触发。 |
| DAG snapshot 推进 | 已完成基础版 | 下游节点只有在同业务日期、同 workflow 内所有直接父节点 `SNAPSHOT_CONFIRMED` 且自身外部门禁满足时才释放。 |
| 摄入事务边界 | 已完成基础版 | 单个 source snapshot 的事件落库、AssetState 投影、DAG/FlowPlan 评估和 offset 推进在同一事务完成。 |
| 湖格式 source SPI | 已完成基础版 | provider、source identity、opaque offset、格式排序和统一 snapshot observation 已与 Paimon SDK 解耦。 |
| Paimon snapshot source | 实现待 E2E | Paimon 1.3 Catalog API 逐 snapshot 读取 properties，并从 delta manifests 推导 changedPartitions；保留历史缺口失败关闭。 |
| Maven Wrapper / JDK17 | 已完成 | 本项目使用 `./mvnw` 构建，并以 Java 17 为编译目标。 |

## 差距清单

| 编号 | 差距 | 当前状态 | 目标版本 | 说明 |
|------|------|----------|----------|------|
| G1 | API 层缺失 | 基础完成 | LF-0.3 | 已新增 FlowPlan/Node、scheduling action、instance evidence、task scheduling intent 最小 REST API。 |
| G2 | FlowPlan / Node 模型缺失 | 已完成 | LF-0.2 | 模型、发布图校验、日期模板、自然触发和实例定义锚点均已完成；旧 `AssetDependency` 运行时路径已移除。 |
| G3 | 调度意图主动投递协议不完整 | 基础完成 | LF-0.3 | 已完成不可变 intent、版本化完整 payload、单一选定路由、统一 publisher SPI、数据库/HTTP/MQ 通道边界、内部 claim 租约与 fencing、退避重试、最大尝试和死信审计。HTTP 已有 Java 17 client；MQ 保持 broker-neutral gateway，具体产品绑定由部署适配器提供。真实事务/网络链路留待整体 E2E。 |
| G4 | Node 级重跑和恢复能力不足 | 已完成 | LF-0.4 | 已支持 task/published node 局部重跑，以及 `FULL_SCOPE` 和单失败节点 `FAILED_NODE_CASCADE` 两种替代恢复；非闭合 DAG 或多失败节点请求会拒绝并要求全范围恢复。 |
| G5 | 补数缺少批次、并发和级联策略 | 已完成 | LF-0.4 | 完整 Flow 与 Node 子图补数均已纳入统一批次模型，支持不可变节点范围、日期准入、级联、安全整日跳过、控制和失败替代恢复；审批上限按当前决策暂缓且不阻塞专题退出。 |
| G6 | Target snapshot baseline 与节点绑定不足 | 基础完成 | LF-0.2 / LF-0.4 | intent 携带目标资产、定义锚点、baseline 和必须写入最终 snapshot 的归因属性；确认按历史事件而非任意 latest 推进，后续只由匹配 snapshot 驱动 DAG。 |
| G7 | 补偿扫描还不够生产化 | 基础完成 | LF-0.5 | 已有 delivery publisher/scan 指标、channel/status 记录数、死信筛选 API，以及物理 snapshot 轨和业务数据轨对账。落后 offset 连续补扫、已有物理/数据事件投影可重放；retention gap、offset ahead、事件缺失和无事件支撑的状态失败关闭。真实湖与 PostgreSQL 运行验证归入暂缓的整体 E2E。 |
| G8 | 查询视图和审计视图不足 | 部分完成 | LF-0.5 | action 侧已支持按 Flow、版本、节点筛选摘要，并按 action key 联查补数批次和逐 task snapshot 证据；仍缺 Flow 聚合实例视图、分页游标、指标和运维 UI。 |
| G9 | Flow 级隔离和并发策略未固化 | 部分建模 | LF-0.5 | `FlowPlan.owner/flowSpaceCode` 已记录归属，但当前 API 仍是全局 ID 访问且 `requestedBy` 只是审计字段，不构成授权；后续接可信身份后按 Flow 做 RBAC/配额，不引入重型租户层。 |
| G10 | 真实湖格式 snapshot 归因元数据采集不完整 | 部分完成（Paimon 实现） | LF-0.5 | 已移除生产 mock，建立通用 source SPI，并实现 Paimon properties/delta-manifest 读取、分区推导、offset 批量扫描和 retention gap 失败关闭；待整体 E2E 与 Iceberg/Hudi 适配器。 |
| G11 | Action snapshot 与正常自然触发未隔离 | 已完成 | LF-0.4 / LF-0.5 | 已按持久化 `intentKey -> triggerType` 分类；补数、恢复、重跑只推进所属实例，非法/中间/维护 snapshot 失败关闭；action 查询也不再把未归属的 latest 推进显示为成功。日期隔离要求 FlowPlan 使用 `${bizDate}` 分区资产。 |
| G12 | 跨实例目标资产日期准入缺失 | 基础完成 | LF-0.5 | 已建立同 `targetAssetKey + bizDate` 的持久化唯一槽位和行锁，覆盖正常、补数、恢复、重跑；冲突项留在 READY，确认/超时释放，过期租约可被重占且旧 holder 不能释放新 holder。真实 PostgreSQL 竞争语义待整体 E2E 验证。 |
| G13 | 发布版本控制策略未执行 | 基础完成 | LF-0.5 | 已执行版本/节点确认超时、版本目标准入租约和 `maxActiveInstances`；支持 `PARALLEL` / `SERIAL_WAIT`，超限保持 READY。`SERIAL_DISCARD`、优先级队列和 dedupe window 尚未实现。 |

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

1. 增加 Flow/Node 级实例聚合和游标查询视图。
2. Flow/Node 级实例聚合、稳定游标和 Web 运维视图按当前决策后移。
3. 由部署按选定 MQ 产品实现 `SchedulingIntentMessageGateway`，并在整体 E2E 中验证真实事务和网络重试。
4. 将真实 Paimon source 纳入系统级 E2E，并基于统一 SPI 增加 Iceberg/Hudi source。

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

退出规则：单元级功能已达到基础退出标准；真实 Paimon/PostgreSQL 的指标抓取、锁竞争和故障恢复验证随整体 Testcontainers E2E 补齐，不再单独阻塞 G7 功能推进。

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
12. 已确认 Paimon 1.3 普通 SQL/标准 `BatchTableCommit` 不直接提供单次 snapshot 任意属性注入；下游需 writer-side adapter 在最终 commit 写入归因属性，且不得以 `commitUser` / `commitIdentifier` 弱替代。
13. source offset 更新会锁定既有 `(sourceType, sourceName)` 行，配合数据库唯一约束阻止多调度实例并发扫描造成位点倒退。
14. source 首次接入默认从 `LATEST` 建立当前事实，避免存量 snapshot 自动形成历史调度风暴；`EARLIEST` 仅允许显式配置，常规补数仍走统一批次模型。

G10 剩余验收项：

1. 在 Lakehouse Flow 整体 E2E 中接入真实 Paimon catalog/table 和 writer-side property adapter，贯穿带属性 snapshot commit、事件入库、分区 AssetState、路由、intent 确认、DAG 和补数推进。
2. 基于同一 SPI 增加 Iceberg source，明确 snapshot summary、operation、partition diff 和 offset 语义。
3. 基于同一 SPI 增加 Hudi source，明确 instant timeline、commit metadata、partition diff 和 offset 语义。

退出规则：至少一个真实 Paimon 端到端场景通过，且新增 Iceberg/Hudi adapter 不需要修改 service/scheduler 核心接口；在此之前 G10 保持“部分完成”。

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

1. 在明确触发丢弃和优先级排队的审计语义后，再实现 `SERIAL_DISCARD` / `SERIAL_PRIORITY`。
2. 真实多 scheduler 节点下的 PostgreSQL 行锁竞争归入整体 Testcontainers E2E，当前按决策暂缓。

## 最近完成推进项

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
17. `DagProgressionService` 只在全部直接父节点的 target snapshot 已确认推进后释放下游，缺失父节点或未推进父节点都继续阻塞。
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
57. 目标 snapshot 严格确认或确认窗口超时后释放槽位；租约过期允许后续调度重占，但只表示可再次下发，不声称旧下游工作已停止。
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
| 质量门槛 | `./mvnw test` 零 failure/error/skip；service line >= 90%、branch >= 65%；每个显式 public service 方法都有直接单元测试调用 | 通过：303 tests，line 90.9%，branch 69.5%；显式 public service 方法直接调用检查通过，delivery query service 1/1、source reconciliation service 3/3 |

退出规则：以上门槛必须同时通过。达到后，补数专题只接受缺陷修复和整体能力带来的必要适配，不再独立扩展功能；开发回到整体版本差距。补数审批上限按已确认决策暂缓，真实 PostgreSQL/Flyway 验证归入 Lakehouse Flow 整体 Testcontainers E2E，二者都不阻塞本专题退出。

后续增强：

1. Flow/Node 级实例聚合、稳定游标和 Web 运维 UI 按当前决策后移。
2. 由部署增加具体 MQ broker gateway。
3. 补数审批上限暂缓，待核心功能闭环稳定后再设计。
4. 整体 Testcontainers E2E 与 Iceberg/Hudi source adapter 按当前决策暂缓。

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

当前测试策略：service/API/integration 逻辑先使用 Mockito 单元测试隔离依赖；service 层每个 public 方法必须有直接测试入口。Testcontainers 留到后续系统级 E2E：从 API/事件入口贯穿 PostgreSQL/Flyway、事务与约束、FlowPlan 决策、内部 outbox 发布、snapshot 确认、DAG 和补数推进。E2E 验证的是 Lakehouse Flow 整体闭环，不归属于某个单独模块，也不用来替代当前单元测试。

2026-09-13 验证快照：JDK 17 下执行 `./mvnw clean test` 共 303 个测试，failure/error/skip 均为 0；service JaCoCo 为 line 90.9%、branch 69.5%、method 90.5%。所有显式 public service 方法均存在测试源码中的直接调用入口，其中 delivery query service 为 1/1、source reconciliation service 为 3/3。V13-V20 PostgreSQL migration、真实 HTTP/MQ 网络重试、真实 Paimon writer/source 对账链路和有限并发版本锁竞争仍留待后续整体 Testcontainers E2E 验证。

每次推进后至少执行：

```bash
./mvnw test
git diff --check
```

较小改动可以先执行局部验证：

```bash
./mvnw -pl lakehouse-flow-service -am test
```

如果测试无法执行，必须在进度汇报中说明未验证项和原因。
