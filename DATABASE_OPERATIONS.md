# Lakehouse Flow 数据库升级与保留策略

**适用版本**: LF-1.0，PostgreSQL schema V1-V23

本文档固定 Lakehouse Flow 的 PostgreSQL migration、备份恢复、兼容窗口和数据保留边界。系统运行处置见 [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md)。

## 1. 基本原则

1. Flyway 是唯一 schema owner，Hibernate 固定使用 `ddl-auto=validate`。
2. migration 只前进。已合入或已在任一共享环境应用的 `V*.sql` 不得修改、删除、重排或重用版本号。
3. 生产禁止 `flyway clean`，默认禁止 `baseline-on-migrate`。
4. schema 恢复依赖升级前备份，不提供反向 SQL migration。
5. Lakehouse Flow 的 offset、事件、AssetState、intent 和 snapshot 证据必须作为一致整体处理。
6. 不使用字符串 SQL 比较湖格式 snapshot id 来决定清理边界；顺序语义属于 source adapter。

当前 canonical migration 由 `lakehouse-flow-dao/src/main/resources/db/migration` 中的 V1、V3-V23，以及 `lakehouse-flow-boot/src/main/resources/db/migration/V2.0__phase2_tables.sql` 共同组成。V2 是已经进入历史 checksum 的有效 migration，不能移动或重写；后续版本统一新增到 dao 模块，并始终由 Flyway 通过完整应用 classpath 执行，不得手工挑选 SQL。

## 2. Migration 兼容策略

V1-V23 是 LF-1.0 冻结前形成的历史基线。从 V23 之后新增 migration 必须遵循以下规则：

- 结构扩展优先使用新增 nullable 列、新表和新索引，先让旧、新应用都能运行。
- 新列回填必须可重入、可分批，并与 schema DDL 分开评估锁和事务时间。
- 删除列、重命名列、收紧非空、改变字段语义或删除兼容数据至少跨一个发布版本完成。
- 滚动升级窗口内，新 schema 必须同时兼容当前发布和前一发布的应用制品。
- API/payload 与数据库字段演进分别遵守契约版本，不允许借 migration 静默改变 intent 语义。
- 大表索引若需要 `CREATE INDEX CONCURRENTLY`，必须作为显式非事务 migration 评审，不能混入普通事务 DDL。

兼容变更采用四步：

1. **Expand**：增加兼容结构，旧应用继续可运行。
2. **Deploy**：部署写入新旧兼容结构的新应用。
3. **Backfill/Observe**：回填并观察至少一个完整业务和故障恢复周期。
4. **Contract**：下一发布才移除旧结构，且必须另有升级前备份。

当前未完成 STB-5 契约冻结前，任何破坏性 migration 都不得进入 LF-1.0。

## 3. 升级前检查

### 3.1 记录数据库状态

```sql
SELECT current_database(), current_schema(), version();

SELECT installed_rank, version, description, type, script, checksum,
       installed_by, installed_on, execution_time, success
FROM flyway_schema_history
ORDER BY installed_rank;
```

升级必须停止并调查以下情况：

- `success = false`；
- 同一版本出现多个脚本；
- 已应用脚本 checksum 与制品不一致；
- 生产最高版本高于待发布制品；
- schema 中存在未通过 migration 管理的同名表、列或约束。

### 3.2 创建并验证备份

凭据通过 `.pgpass`、Secret 或平台身份提供，不放在命令行和日志中：

```bash
pg_dump --format=custom --no-owner \
  --file="lakehouse-flow-before-vNEXT-$(date +%Y%m%d%H%M%S).dump" \
  --dbname="$LAKEHOUSE_FLOW_DATABASE_URL"

pg_restore --list lakehouse-flow-before-vNEXT-*.dump >/dev/null
```

备份记录至少包含数据库实例、schema、Flyway 最高版本、制品 commit、文件校验和和保留位置。生产首发及每个 schema 变更前都要在隔离数据库做一次 restore 抽检；仅有 `pg_dump` 成功日志不算恢复验证。

## 4. 受控升级流程

1. 暂停发布入口或进入部署维护窗口，等待当前数据库事务结束。
2. 保持数据库可用，停止所有旧 scheduler 节点。
3. 完成升级前备份及恢复抽检。
4. 仅启动一个新制品实例，并临时关闭会改变调度事实的 scanner：

```bash
java -jar lakehouse-flow-boot.jar \
  --lakehouse-flow.snapshot-sources.scanner.enabled=false \
  --lakehouse-flow.snapshot-sources.reconciliation.enabled=false \
  --lakehouse-flow.scheduling-intent-outbox.enabled=false \
  --lakehouse-flow.scheduling-intent-delivery.publisher.enabled=false \
  --lakehouse-flow.job-control-intent-delivery.publisher.enabled=false \
  --lakehouse-flow.snapshot-confirmation.enabled=false \
  --lakehouse-flow.job-control-snapshot-confirmation.enabled=false
```

5. 等待 Flyway migrate、Hibernate validate 和 `/actuator/health` 全部成功。
6. 再次查询 `flyway_schema_history`，确认最高版本、checksum 和成功状态符合发布清单。
7. 停止临时实例，按正式配置逐步启动 scheduler 节点。
8. 观察 source 对账、delivery backlog、claim 过期重占和 snapshot confirmation 至少两个扫描周期。
9. 恢复发布入口，执行一条受控 intent 和一条 source snapshot 冒烟验证。

Flyway 自带 schema history 锁，但生产仍采用单实例迁移，避免多个应用在 DDL 期间同时完成业务初始化。

## 5. 失败恢复

### 5.1 应用启动失败但 migration 未执行

如果 `flyway_schema_history` 没有新增版本且 schema diff 为空，可修复制品或配置后重试。不要使用 `repair` 掩盖 checksum 或脚本问题。

### 5.2 Migration 失败

1. 停止全部新制品实例，保留失败日志和 `flyway_schema_history`。
2. 不修改原 migration、不手工标记 success、不直接删除 history 行。
3. PostgreSQL 事务 DDL 通常会整体回滚，但必须以实际 schema 和 history 为准，不能凭日志假设。
4. 若确认 schema 未变化，修复为一个新的 migration 版本后重新走评审和备份。
5. 若存在部分变化或数据回填，恢复升级前备份到新的数据库实例，校验后切换连接。

### 5.3 应用回滚

只有在旧制品经过验证能读取当前 schema 时，才允许只回滚应用。否则执行数据库恢复：

```bash
createdb "$LAKEHOUSE_FLOW_RESTORE_DATABASE"
pg_restore --no-owner --exit-on-error \
  --dbname="$LAKEHOUSE_FLOW_RESTORE_URL" \
  lakehouse-flow-before-vNEXT-YYYYmmddHHMMSS.dump
```

恢复后先以 scanner 全关闭的旧制品完成 Flyway/Hibernate 校验，再切换应用连接。禁止在原生产库上边回滚 DDL 边恢复流量。

## 6. 数据分类与保留边界

LF-1.0 在获得真实生产容量基线前采用“默认不自动删除”。保留期由部署方定义为 `audit_retention_cutoff`，但任何清理都必须满足下表的额外边界：

| 数据类别 | 表 | LF-1.0 保留与清理规则 |
|---|---|---|
| 当前事实 | `asset_state`、`event_consumer_offset`、`snapshot_source_health` | 不按时间清理；它们共同定义 snapshot/source 当前事实 |
| Writer 所有权 | `writer_job_binding` | 受管表存在期间不清理，不降低 `current_writer_epoch` |
| 当前协调 | `scheduler_lease`、`scheduling_target_admission` | 由服务租约逻辑回收；禁止定时 SQL 按创建时间删除 |
| Flow 定义 | `flow_plan`、`flow_plan_version`、`schedule_node` | 已被实例、action、backfill 或 intent 引用时不清理；发布版本永久保留到完整审计归档完成 |
| 原始 snapshot 事件 | `lakehouse_event` | 只有 source offset、source health、AssetState 和所有 intent 归因均已归档验证后才可清理；禁止用 snapshot id 字符串大小判断 |
| 调度实例 | `workflow_instance`、`task_instance`、`trigger_history` | 以 workflow aggregate 整体归档；存在非终态 task、等待 snapshot、活动日期槽或关联 intent 时不得清理 |
| 数据处理 intent | `scheduling_intent`、`scheduling_intent_delivery` | 不可改单条状态；必须与 task、snapshot 证据和下游幂等保留窗口一起归档，死信至少保留到事故关闭后 |
| 作业控制 intent | `job_control_intent`、`job_control_intent_delivery` | 当前及历史 writer epoch 审计整体保留；当前 binding 引用的 control intent 不清理 |
| Action 与补数 | `scheduling_action`、`backfill_batch`、`backfill_item` | action、原批次、替代恢复批次、workflow/task 和 intent lineage 一起归档，不能只删失败批次 |
| 旧定义模型 | `workflow_definition`、`task_definition`、`asset_dependency`、`backfill_spec` | 仍有旧实例或兼容 API 引用时保留；LF-1.0 不新增自动清理 |

### 6.1 可清理 aggregate 的必要条件

只有同时满足以下条件的历史 aggregate 才能进入归档候选：

1. `created_at` 早于部署方批准的 `audit_retention_cutoff`。
2. workflow/task/backfill 均已进入终态，没有 `CREATED`、`WAITING_SNAPSHOT`、`READY_TO_SCHEDULE` 或 `SCHEDULED`。
3. delivery 不处于 `PENDING`、`PUBLISHING` 或 `RETRY_WAIT`。
4. 不存在未过期的 `scheduling_target_admission`、claim 或 writer holder。
5. snapshot 结果、source 证据、intent payload、action lineage 和 dead-letter 证据已完整导出。
6. 下游 `intentKey` 幂等记录保留期不短于 Lakehouse Flow intent 审计期。
7. 归档文件已校验、加密并完成一次可读恢复抽检。

### 6.2 清理执行约束

LF-1.0 不提供通用在线 DELETE job。原因是当前表之间存在 RESTRICT、CASCADE 和多条 action/backfill lineage，直接按单表时间条件删除会损坏归因证据。

需要清理时必须先实现并评审面向 aggregate 的专用归档 migration 或离线工具，要求：

- 先 dry-run 输出候选主键、行数和关联证据统计；
- 按 workflow/backfill/job-control aggregate 删除，不按单表独立删除；
- 每批使用有限行数和独立事务，记录批次、水位和校验和；
- 清理前后核对非终态数量、source 对账、intent/snapshot lineage 和外键；
- 大批删除后按 PostgreSQL 实际膨胀情况安排 `VACUUM (ANALYZE)`，不在高峰期执行阻塞式 `VACUUM FULL`；
- 任何失败都停止后续批次，不通过关闭外键或触发器继续。

在专用工具和生产容量证据出现前，运维动作只能做备份、归档导出和容量扩展，不能直接清理生产审计数据。

## 7. 数据库巡检

每日检查：

```sql
SELECT version, description, installed_on, success
FROM flyway_schema_history
ORDER BY installed_rank DESC
LIMIT 5;

SELECT status, count(*)
FROM scheduling_intent_delivery
GROUP BY status;

SELECT status, count(*)
FROM job_control_intent_delivery
GROUP BY status;

SELECT source_type, outcome, count(*)
FROM snapshot_source_health
GROUP BY source_type, outcome;
```

数据库巡检用于确认持久化状态和容量，不得绕过 `/api/v1/operations/*` 的领域解释，也不得从 delivery 行数推断下游执行结果。

## 8. 变更评审清单

每个新 migration 合入前必须回答：

1. 是否只新增了未使用过的递增版本号？
2. 是否保持旧、新制品滚动兼容？
3. 是否修改 intent、snapshot、offset、writer epoch 或 action lineage 语义？
4. 是否可能长时间锁表、全表回填或放大 WAL？
5. 是否有升级前备份、隔离恢复和失败切换方案？
6. 是否更新 JPA 模型、Repository 查询、测试和本手册？
7. 是否需要推迟到 STB-5 契约冻结之后？

任一问题没有证据时，migration 不进入生产发布。
