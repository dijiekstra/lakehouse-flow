# DolphinScheduler 湖仓资产版本事件驱动调度设计文档

> 参考文档：本文讨论的是 DolphinScheduler 扩展方案，不是当前独立 Lakehouse Flow 的架构或实现路线。只可参考湖仓调度背景与 DolphinScheduler action 语义，不得引入本文的 Master/Worker、executor、任务运行状态或 callback 设计；当前边界以 `ARCHITECTURE.md` 和 `SCHEDULING_MODEL_DESIGN.md` 为准。
>
> 状态：Draft / RFC
> 作者：Dolphin Agent
> 关联模块：dolphinscheduler-dao, dolphinscheduler-service, dolphinscheduler-master, dolphinscheduler-api, dolphinscheduler-task-plugin

## 1. 背景与目标

### 1.1 背景

现代 CDC 湖仓架构（基于 Paimon / Iceberg / Hudi 等表格式）正在推动数仓建模与数据平台调度范式的演进：

- 数仓建模从单一"表"升级为"实体状态表（latest-state）+ 事件事实表（event fact）+ 变更历史表（changelog）"三层语义，依赖于表格式提供的 Snapshot、Manifest、Primary Key Table、Changelog、Time Travel/Tag、Schema Evolution 等能力。
- 调度范式从"时间 DAG"升级为"数据资产状态调度"：即调度的触发依据不再仅仅是 cron 时间点，而是"数据资产是否已经推进到某个可用版本"，典型场景包括：
  - snapshot 生成后触发下游加工
  - watermark 到达后触发窗口计算产出
  - quality check 通过后才允许发布指标
  - tag 固化后触发财务报表出账
  - schema 变更审批通过后触发下游模型升级
  - 补数（backfill）完成后触发重算与对账

### 1.2 目标

在不破坏 DolphinScheduler 现有调度语义（cron 定时、手动运行、补数、失败恢复、DEPENDENT 任务、正常 DAG 执行）的前提下，新增"数据资产版本推进事件驱动调度"能力，实现：

```text
湖仓表 snapshot/instant 推进
    -> 事件被感知并持久化为 AssetEvent
    -> 更新资产的当前状态 AssetState（latest snapshot / watermark / quality / schema）
    -> 按工作流/任务声明的资产依赖条件进行匹配评估
    -> 依赖满足后，通过 Command 机制触发/释放 DolphinScheduler 工作流或任务实例
    -> 执行、观测、失败重试与补偿
```

设计遵循以下不可退让的原则：

1. 不能"事件到了就触发"，必须"资产状态达到条件才触发"。
2. 同一版本/同一依赖组合只能触发一次（幂等）。
3. 事件可能重复、乱序、延迟或丢失，资产状态是调度依据，事件只是输入证据。
4. 优先复用现有 Command / CommandType / DEPENDENT 任务 / 任务实例状态机等基础设施，不新建独立调度内核。
5. 第一阶段只做小范围可验证闭环（Paimon snapshot -> AssetEvent -> AssetState -> 依赖匹配 -> Command 触发），后续再扩展 Iceberg/Hudi、多资产 AND/OR 组合等能力。

### 1.3 适用边界与问题定义

本设计面向**新一代数仓架构**（基于 Paimon/Iceberg/Hudi 等湖仓表格式），其核心特征是：

- **整个DAG链路都基于snapshot版本驱动**：从ODS（数据入湖）到DWD（维度加工）到DWS（主题聚合）到ADS（对外服务），每一层都是前一层数据资产版本推进的结果，消除了时间与数据可用性的错配。
- **不依赖cron和时间表达式**：DAG中任何任务的触发都基于其依赖资产(snapshot/watermark/quality/tag)，而非"某个固定时刻"。
- **多个上游资产同时就绪才触发下游**：例如DWS中某个任务可能依赖DWD中多张表的快照都已推进，这些依赖通过snapshot版本组合表达，而不是时间窗口。
- **保留跨流依赖等编排能力**：若DAG中某个任务声明了跨流依赖（依赖其他工作流/项目中的任务），这些依赖也应该升级为基于snapshot模型，确保整体调度的数据驱动特性。

**适用场景**：任何基于湖仓表承载ODS/DWD/DWS/ADS全链路的数据处理工作流。

**不适用场景**：仍在使用"固定时刻批处理"的传统数仓（即使已上云）。

## 2. 现状分析：DolphinScheduler 已有的事件/依赖触发基础设施

通过对代码库的实际检查（版本：当前 master 分支），现有可复用能力如下：

### 2.1 CommandType / Command（`dolphinscheduler-dao`, `dolphinscheduler-common`）

- `org.apache.dolphinscheduler.common.enums.CommandType`（文件：`dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/common/enums/CommandType.java`）已定义 `START_PROCESS`、`SCHEDULER`、`COMPLEMENT_DATA`、`RECOVER_TOLERANCE_FAULT_PROCESS` 等多种触发来源类型（注释中已标注 `// todo: rename to WorkflowTriggerType`，说明官方已经将 Command 视为"触发类型"的抽象，而不仅是"启动动作"）。
- `org.apache.dolphinscheduler.dao.entity.Command`（`dolphinscheduler-dao/.../entity/Command.java`）是落库实体，对应表 `t_ds_command`，核心字段：`commandType`、`workflowDefinitionCode`、`workflowDefinitionVersion`、`commandParam`（JSON 格式的启动参数）、`workflowInstancePriority`。
- `org.apache.dolphinscheduler.service.command.CommandService`（`dolphinscheduler-service/.../command/CommandService.java`）暴露 `int createCommand(Command command)`，是当前所有"发起一次工作流实例运行"的统一入口，Master 侧的 `CommandService`/scanner 会消费 `t_ds_command` 表生成 WorkflowInstance。

**结论**：我们不需要新建一条并行的"事件触发工作流"的执行通道。新增一个 `CommandType.ASSET_EVENT_TRIGGER`（或复用 `START_PROCESS` + `commandParam` 中标记触发来源），通过已有 `CommandService#createCommand` 写入 `t_ds_command`，即可完全复用 Master 侧现有的 Command 消费、WorkflowInstance 创建、DAG 执行链路。这是风险最小的集成点。

### 2.2 DEPENDENT 任务（任务级依赖等待）

- `DependentType`、`DependentRelation`（`dolphinscheduler-task-plugin/dolphinscheduler-task-api/.../enums/DependentType.java`、`DependentRelation.java`）：当前 DEPENDENT 只支持对"另一个工作流/任务在某个周期内是否成功"的依赖（`DependentItem` 包含 `projectCode`/`definitionCode`/`depTaskCode`/`cycle`/`dateValue`/`dependResult`），**面向的是"任务实例执行结果"，而不是"外部数据资产状态"**。
- `DependentParameters`（同目录 `parameters/DependentParameters.java`）支持 `DependentRelation`（AND/OR）组合多个 `DependentTaskModel`，这个"多依赖项 AND/OR 组合评估"的结构可以直接借鉴用于资产依赖的组合表达。
- `DependentLogicTask` / `DependentTaskTracker`（`dolphinscheduler-master/.../executor/plugin/dependent/`）：任务实例进入执行态后，由 `DependentTaskTracker` 轮询依赖是否达成，`getDependentTaskStatus()` 返回 `TaskExecutionStatus`，本质是"任务实例占用一个执行槽位，内部做轮询等待"，不是事件回调驱动。

**结论**：DEPENDENT 任务的"多依赖 AND/OR 组合表达"值得复用其设计（`DependentRelation`），但其"依赖对象"是任务执行结果而非外部数据资产版本，因此不能直接复用其 `DependentItem`/`DependentTaskTracker` 实现，需要新增一种面向资产的依赖模型和评估器；执行形态可以参考它，作为新增 Task 类型 `ASSET_SENSOR`（策略 A，见下文）的实现基础，复用其"占用任务实例槽位 + 轮询判定 + 成功后进入下游"的运行模式与状态机（`onTaskRunning`/`onTaskPaused`/`onTaskKilled` 等在 `AbstractLogicTask` 中已有的生命周期钩子）。

### 2.3 没有的能力（需要新增）

- 没有 Airflow Dataset 风格的"资产（Asset）"一等公民模型，没有 `t_ds_asset` 一类的表。
- 没有对外部事件（消息/回调/轮询）的统一接入层；`dolphinscheduler-extract` 模块目前主要是 Master/Worker/API 之间的内部 RPC 契约（`extract-master`、`extract-worker`、`extract-alert` 等），没有面向外部湖仓系统的事件接入接口。
- 没有"事件去重 + 幂等触发历史"表，当前 `t_ds_command` 没有唯一约束防止对同一(工作流, 触发条件)重复插入 Command（业务层通过 `scheduler`/手动触发点控制，未覆盖"外部事件驱动"场景）。
- 没有 watermark / snapshot / quality / schema 等湖仓语义的字段和状态机。

综上，**总体策略**：新增 `event-source`、`asset-state`、`dependency-resolver` 三个逻辑模块和配套的 DAO 表，在触发侧对接现有 `CommandService.createCommand`（Strategy C，直接创建工作流实例）以及新增 `ASSET_SENSOR` 任务类型（Strategy A，任务级等待,复用 DAG 内嵌等待语义），两种策略分阶段实现,不对 Master 调度内核和任务状态机做侵入式修改。

## 3. 总体架构设计

### 3.1 总体链路

```text
┌───────────────┐   ┌────────────────┐   ┌───────────────┐   ┌─────────────────────┐   ┌───────────────┐
│ Lakehouse      │   │ Event Ingestion│   │ Asset State   │   │ Dependency Resolver  │   │ Scheduler      │
│ (Paimon/Iceberg│──▶│ (Poll Scanner /│──▶│ Store         │──▶│ (Match AssetDependency│──▶│ Adapter        │
│  /Hudi commit) │   │  Report API)   │   │ (t_ds_asset_  │   │  -> READY/BLOCKED)   │   │ (CommandService│
└───────────────┘   └────────────────┘   │  state)       │   └─────────────────────┘   │  / ASSET_SENSOR│
                                          └───────────────┘                              │  Task)         │
                                                                                          └───────┬────────┘
                                                                                                  ▼
                                                                                     Master 正常 Command/DAG 执行链路
```

### 3.2 资产（Asset）与资产事件（AssetEvent）领域模型

```java
// 资产唯一标识：catalog.database.table[/partition]
public class AssetIdentifier {
    private String assetKey;      // 规范化后的唯一键，例如 paimon://catalog/db/table/dt=2024-05-20
    private String storageFormat; // PAIMON / ICEBERG / HUDI
    private String catalogName;
    private String databaseName;
    private String tableName;
    private String partitionExpr; // 可为空，支持分区级资产
}

// 原始事件，来源可为轮询扫描或外部主动上报，仅作为“输入证据”，不可直接触发调度
public class AssetEvent {
    private String eventId;         // 幂等键之一，来源系统内可重复计算得到（见 4.2）
    private String assetKey;
    private String eventType;       // SNAPSHOT_COMMITTED / WATERMARK_ADVANCED / TAG_CREATED /
                                     // QUALITY_CHECK_PASSED / QUALITY_CHECK_FAILED /
                                     // SCHEMA_CHANGE_APPROVED / BACKFILL_COMPLETED / CDC_LAG_RECOVERED
    private Long snapshotId;        // Paimon snapshot-id / Iceberg snapshot-id / Hudi instant time 映射值
    private Long schemaId;
    private Long watermark;         // epoch millis
    private String commitKind;      // APPEND / COMPACT / OVERWRITE / ...
    private Long commitTime;
    private Long deltaRecordCount;
    private Boolean schemaChanged;
    private String payload;         // 原始 JSON，便于排障，不参与判定逻辑
    private Long receiveTime;
}

// 资产当前状态：调度判定的唯一事实来源（Source of Truth）
public class AssetState {
    private String assetKey;
    private Long latestSnapshotId;
    private Long latestSchemaId;
    private Long latestWatermark;
    private String latestTag;
    private String qualityStatus;   // UNKNOWN / PASSED / FAILED
    private String schemaStatus;    // UNKNOWN / COMPATIBLE / PENDING_APPROVAL / REJECTED
    private String backfillStatus;  // NONE / RUNNING / COMPLETED
    private Long updateTime;
    private Long version;           // 乐观锁，防止并发覆盖（见 4.4）
}
```

### 3.3 事件获取方式设计（Event Ingestion）

两种方式并行、互为补偿，MVP 阶段优先落地方式一：

**方式一：增量轮询扫描（Polling Scanner，优先落地）**

- 独立部署一个轻量 `AssetEventScanner`（可作为 DolphinScheduler 内的一个后台任务，或独立进程/API 定时任务），定期对已注册的 Paimon 表执行：
  ```sql
  SELECT snapshot_id, schema_id, commit_user, commit_identifier, commit_kind, commit_time,
         watermark, total_record_count, delta_record_count, changelog_record_count
  FROM catalog_name.database_name.`table_name$snapshots`
  WHERE snapshot_id > ?
  ORDER BY snapshot_id;
  ```
  其中 `?` 来自 `t_ds_asset_event_consumer_offset` 中记录的 `last_scanned_snapshot_id`。
- 优点：不依赖写入方改造，可对已存量的 Paimon/Iceberg 表直接接入；天然具备"补偿"能力（本身就是全量可重放的 pull 模型）。
- 缺点：存在扫描间隔带来的延迟（典型 10s~1min 级）。

**方式二：写入方主动上报（Push，用于低延迟场景，后续阶段接入）**

- Flink/Spark 写入作业在 commit 成功后，调用新增 REST 接口 `POST /dolphinscheduler/asset-events` 主动上报事件（见 4.6 API 设计）。
- 优点：低延迟（秒级）。
- 缺点：需要写入方业务代码改造；且必须与轮询扫描共存做兜底，否则漏报无法被发现。

**结论（取舍）**：MVP 阶段只实现方式一（Polling Scanner），因为它对现有 Flink/Paimon 生产链路零侵入、可独立验证、具备天然的补偿属性。方式二作为二期增强，两者共用同一条"事件去重 -> 状态更新 -> 依赖评估"处理链路，仅事件来源（`sourceType=POLL` / `sourceType=PUSH`）不同,通过 `t_ds_asset_event` 的唯一约束天然去重，双路径不会重复触发。

### 3.4 数据库表设计（DDL 草案）

新增 5 张表，均采用 `t_ds_` 前缀以贴合现有命名规范，放在 `dolphinscheduler-dao` 的新增/升级 SQL 目录中（参考现有 `dolphinscheduler-dao/src/main/resources/sql/upgrade/<version>_schema/{mysql,postgresql}/dolphinscheduler_ddl.sql` 的升级脚本组织方式）。

```sql
-- 资产注册表：描述一个可被依赖的湖仓数据资产
CREATE TABLE t_ds_asset (
    id              BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_key       VARCHAR(512) NOT NULL,   -- catalog.db.table[/partition_expr] 规范化后的唯一键
    storage_format  VARCHAR(32)  NOT NULL,   -- PAIMON / ICEBERG / HUDI
    catalog_name    VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    partition_expr  VARCHAR(256),
    description     VARCHAR(512),
    create_time     DATETIME NOT NULL,
    update_time     DATETIME NOT NULL,
    UNIQUE KEY uk_asset_key (asset_key)
);

-- 原始事件表：仅作为证据留存与排障，不作为调度判定依据
CREATE TABLE t_ds_asset_event (
    id                     BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id               VARCHAR(128) NOT NULL,
    source_type             VARCHAR(16)  NOT NULL,  -- POLL / PUSH
    asset_key               VARCHAR(512) NOT NULL,
    event_type              VARCHAR(32)  NOT NULL,  -- SNAPSHOT_COMMITTED / WATERMARK_ADVANCED / ...
    snapshot_id              BIGINT,
    schema_id                BIGINT,
    watermark                BIGINT,
    commit_kind              VARCHAR(32),
    commit_time              BIGINT,
    delta_record_count       BIGINT,
    schema_changed           TINYINT,
    payload                  TEXT,
    receive_time             BIGINT NOT NULL,
    create_time              DATETIME NOT NULL,
    UNIQUE KEY uk_dedup (source_type, asset_key, event_type, snapshot_id),
    KEY idx_asset_snapshot (asset_key, snapshot_id),
    KEY idx_event_type_time (event_type, receive_time)
);

-- 资产状态表：调度判定的唯一事实来源
CREATE TABLE t_ds_asset_state (
    id                 BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_key          VARCHAR(512) NOT NULL,
    latest_snapshot_id BIGINT,
    latest_schema_id   BIGINT,
    latest_watermark   BIGINT,
    latest_tag         VARCHAR(128),
    quality_status     VARCHAR(16) DEFAULT 'UNKNOWN',
    schema_status      VARCHAR(32) DEFAULT 'UNKNOWN',
    backfill_status    VARCHAR(16) DEFAULT 'NONE',
    version            BIGINT NOT NULL DEFAULT 0,   -- 乐观锁
    update_time        DATETIME NOT NULL,
    UNIQUE KEY uk_asset_key (asset_key)
);

-- 工作流/任务对资产的依赖声明
CREATE TABLE t_ds_asset_dependency (
    id                       BIGINT PRIMARY KEY AUTO_INCREMENT,
    workflow_definition_code BIGINT NOT NULL,
    task_definition_code     BIGINT,             -- 为空表示对整个工作流生效（Strategy C）
    asset_key                VARCHAR(512) NOT NULL,
    dependency_group         VARCHAR(64) NOT NULL DEFAULT 'default', -- 支持多资产 AND 分组
    condition_json           TEXT NOT NULL,       -- 见 3.6 依赖表达式
    enabled                  TINYINT NOT NULL DEFAULT 1,
    create_time              DATETIME NOT NULL,
    update_time              DATETIME NOT NULL,
    KEY idx_asset_key (asset_key),
    KEY idx_workflow (workflow_definition_code)
);

-- 幂等触发历史 + 审计
CREATE TABLE t_ds_asset_trigger_history (
    id                        BIGINT PRIMARY KEY AUTO_INCREMENT,
    trigger_key               VARCHAR(256) NOT NULL,  -- 幂等键，见 4.1
    event_id                   VARCHAR(128),
    asset_key                  VARCHAR(512) NOT NULL,
    workflow_definition_code   BIGINT NOT NULL,
    workflow_instance_id       BIGINT,
    task_definition_code       BIGINT,
    task_instance_id           BIGINT,
    trigger_status             VARCHAR(16) NOT NULL, -- TRIGGERING / SUCCESS / FAILED / SKIPPED
    reason                     VARCHAR(512),
    create_time                DATETIME NOT NULL,
    update_time                DATETIME NOT NULL,
    UNIQUE KEY uk_trigger_key (trigger_key),
    KEY idx_status_time (trigger_status, create_time)
);

-- 事件消费位点（用于 Polling Scanner 的断点续扫）
CREATE TABLE t_ds_asset_event_consumer_offset (
    id                    BIGINT PRIMARY KEY AUTO_INCREMENT,
    source_type            VARCHAR(16) NOT NULL,
    asset_key              VARCHAR(512) NOT NULL,
    consumer_group         VARCHAR(64) NOT NULL DEFAULT 'default',
    last_snapshot_id       BIGINT,
    last_scan_time         DATETIME,
    UNIQUE KEY uk_offset (source_type, asset_key, consumer_group)
);
```

### 3.5 调度触发链路（与现有 Command 机制对接）

```text
1. AssetEventScanner 定时扫描 Paimon $snapshots -> 产出候选 AssetEvent
2. 写入 t_ds_asset_event（利用唯一约束 uk_dedup 做插入去重，冲突则说明该事件已处理过，直接跳过）
3. 事件写入成功后，进入 AssetState 更新：
   - 读取当前 t_ds_asset_state（带 version）
   - 若 event.snapshotId <= state.latestSnapshotId，丢弃（乱序保护，见 4.3）
   - 否则以乐观锁 UPDATE ... WHERE asset_key=? AND version=? 更新状态，version+1
4. 状态更新成功后，触发 DependencyResolver：
   - 查询该 asset_key 关联的 t_ds_asset_dependency
   - 对每条依赖，按 condition_json 求值（快照存在/watermark>=X/quality=PASSED/schema=COMPATIBLE）
   - 对同一 dependency_group 下的多条依赖做 AND 聚合（同一工作流等待多个上游资产都就绪）
5. 若判定为 READY：
   a. 计算 trigger_key（见 4.1），向 t_ds_asset_trigger_history 做唯一插入
      - 插入成功 => 本次获得触发权，继续步骤 b
      - 插入失败（唯一冲突）=> 说明已被其他并发的 Master/Scanner 实例触发过，直接返回，不重复触发
   b. 根据依赖类型选择触发方式：
      - Strategy C（工作流级）：调用现有 CommandService#createCommand，
        使用新增 CommandType（如 ASSET_EVENT_TRIGGER，或复用 START_PROCESS 并在 commandParam 中标记
        triggerSource=ASSET_EVENT、assetKey、snapshotId 等审计信息），
        由 Master 侧既有 Command 消费链路完成 WorkflowInstance 创建与 DAG 执行，不改动 Master 内核。
      - Strategy A（任务级）：若工作流已在运行且包含 ASSET_SENSOR 任务，
        则更新对应任务实例的依赖满足标记，由 ASSET_SENSOR 任务（运行时轮询/订阅 AssetState 变化）
        自行判定成功，走入现有任务状态机（复用 DependentLogicTask 同款生命周期钩子）。
   c. 在真正触发前执行"原生依赖门禁校验"：
      - 若用户配置了必须先满足的原生依赖（上游任务成功、DEPENDENT 条件、运行窗口限制），则先校验通过再触发；
      - 仅在显式配置为 snapshot 直驱模式时，可由资产版本推进直接作为主触发条件。
   d. 触发结果（成功/失败）回写 t_ds_asset_trigger_history.trigger_status，
      失败的记录由 Compensation Scanner 定期扫描重试。
6. Master 按照现有正常链路执行 WorkflowInstance / TaskInstance，无需感知事件来源。
```

### 3.6 依赖表达式（结构化配置，非通用 DSL）

```json
{
  "assetKey": "paimon://catalog/db/ods_order",
  "snapshotRequired": true,
  "qualityStatus": "PASSED",
  "schemaStatus": "COMPATIBLE"
}
```

`dependency_group` 用于表达多资产 AND 语义：同一 group 下所有依赖都判定 READY，才认为该 group 就绪；工作流可配置多个 group，group 间为 OR（任一 group 就绪即可触发），语义与 `DependentParameters.Dependence.relation`（AND/OR）保持一致，便于用户理解迁移成本。首期只支持 AND-within-group / OR-across-group 两层结构，不做通用嵌套布尔表达式。

### 3.7 幂等性设计

`trigger_key` 组成（见 4.1）+ `t_ds_asset_trigger_history` 唯一约束是幂等的核心保障：

- 同一 workflowDefinitionCode + dependency_group + 所有相关 asset 的 snapshot 组合 只能成功触发一次。
- 多个 Master/Scanner 并发扫描同一批事件时，通过数据库唯一约束天然实现"只有一个赢家"，不需要分布式锁。
- 若 `createCommand` 调用失败（网络/DB 抖动），trigger_status 保持 `TRIGGERING`，由 Compensation Scanner 识别超时未终态的记录，安全重试（重试前需确认对应 Command/WorkflowInstance 是否已实际生成，避免重复创建，可通过在 `commandParam` 中回写 `trigger_key` 并在重试前查询是否已存在关联 Command/WorkflowInstance）。

### 3.8 多资产对齐（AND/OR）

- 依赖以 `dependency_group` 分组存储在 `t_ds_asset_dependency`，一次评估触发时需要读取整组依赖，逐一评估 `condition_json`，全部满足才进入触发流程。
- 为避免"资产 A 就绪 -> 触发评估 -> 资产 B 未就绪 -> 放弃"之后资产 B 就绪时不会重新触发的问题：**任一资产事件到来都会重新评估其所关联的全部 dependency_group**（而不仅是该资产自身），即 DependencyResolver 以 `asset_key -> dependency_group` 反向索引驱动重新求值。

### 3.9 乱序事件处理

- 事件层面：`t_ds_asset_event` 唯一约束按 `(source_type, asset_key, event_type, snapshot_id)`，重复上报直接被数据库拒绝，视为去重成功。
- 状态层面：更新 `AssetState` 时严格比较版本号（`snapshotId`/`watermark` 单调性），采用"只允许推进、不允许回退"策略：
  ```sql
  UPDATE t_ds_asset_state
  SET latest_snapshot_id = ?, latest_watermark = GREATEST(latest_watermark, ?),
      version = version + 1, update_time = ?
  WHERE asset_key = ? AND version = ? AND ? > latest_snapshot_id;
  ```
  若 `UPDATE` 影响行数为 0，说明并发冲突或事件过期（旧快照延迟到达），该事件被标记 `IGNORED` 并记录原因，不影响调度判定。

### 3.10 失败重试与补偿

- **Compensation Scanner**（复用 3.3 的 Polling Scanner 基础设施，独立调度周期，例如每 5~15 分钟一次）职责：
  1. 对比 Paimon 实际最新 snapshot 与 `t_ds_asset_state.latest_snapshot_id`，若落后则说明存在漏采集的事件（Push 失败或 Scanner 短暂宕机），重新拉取缺失快照并补写 `t_ds_asset_event`。
  2. 扫描 `t_ds_asset_trigger_history` 中长期处于 `TRIGGERING`/`FAILED` 状态的记录，重新执行触发（先检查是否已有对应 Command/WorkflowInstance，避免重复触发）。
  3. 对于 `BLOCKED_BY_*` 状态的依赖，可提供查询接口说明"为什么还没触发"（见观测性）。
- 该补偿链路与 Push 事件共用同一套去重与状态更新代码路径，保证两条路径不会双重触发。

## 4. 与现有模块的集成点

### 4.1 工作流/任务定义如何声明资产依赖

推荐两种落地形态，建议按阶段递进实现：

**（1）Strategy A：新增任务类型 `ASSET_SENSOR`（推荐 MVP 首选，风险最小）**

- 参照现有 `DEPENDENT` 任务的插件化机制（`DependentLogicTaskChannel` / `DependentLogicTaskChannelFactory` / `DependentLogicTask`），在 `dolphinscheduler-task-plugin` 下新增 `dolphinscheduler-task-asset-sensor` 模块：
  - `AssetSensorParameters extends AbstractParameters`：字段结构参考 3.6 依赖表达式 JSON，外加 `checkIntervalSeconds`、`timeoutMinutes`。
  - `AssetSensorLogicTask extends AbstractLogicTask<AssetSensorParameters>`：复用 `AbstractLogicTask` 的 `onTaskRunning/onTaskPaused/onTaskKilled` 生命周期，内部通过 `AssetStateDao` 轮询判定条件是否满足（也可注册回调，由 DependencyResolver 主动推送 READY 状态，降低轮询延迟）。
  - 用户在工作流 DAG 中新增一个 `ASSET_SENSOR` 节点作为"入口哨兵任务"，下游任务按普通 DAG 依赖串联，任务成功后 DAG 正常向后推进。
- 默认推荐把 `ASSET_SENSOR` 放在原有业务任务前作为数据就绪门禁，而不是替换原有 DAG 依赖；这样可同时满足"数据版本就绪"与"用户配置的原生依赖"。
- 优点：完全复用现有 WorkflowInstance/TaskInstance 状态机、重试、超时、告警、UI 展示（任务类型注册机制），改动面最小，等价于"给 DAG 加一个新的任务插件"。

**（2）Strategy C：工作流级事件直接创建工作流实例（用于纯事件驱动场景，二期实现）**

- 在工作流定义扩展属性中增加 `assetTriggerEnabled` 与关联 `dependency_group`（可以复用 `t_ds_asset_dependency` 中 `task_definition_code` 为空的记录表示工作流级依赖）。
- DependencyResolver 判定 READY 后，调用 `CommandService#createCommand`，`commandParam` 中携带触发相关的审计字段如 `assetKey`、`snapshotId` 等。
- 风险提示：需要明确工作流实例与快照版本的对应关系（见第11节风险）。

### 4.2 API 层新增接口

在 `dolphinscheduler-api` 新增 Controller（参考现有 `ExecutorController` 的分层方式：Controller -> Service -> DAO）：

```text
POST   /projects/{projectCode}/assets                       注册/更新资产
GET    /projects/{projectCode}/assets                        查询资产列表
GET    /projects/{projectCode}/assets/{assetKey}/state       查询资产当前状态
POST   /projects/{projectCode}/asset-events                  外部系统主动上报事件（Push 方式）
GET    /projects/{projectCode}/asset-events                  查询事件历史（排障）
POST   /projects/{projectCode}/asset-dependencies             为工作流/任务声明资产依赖
GET    /projects/{projectCode}/asset-dependencies/{workflowDefinitionCode}  查询依赖及其当前判定状态（为什么没触发）
GET    /projects/{projectCode}/asset-trigger-history          查询触发历史（幂等审计）
```

### 4.3 UI 层建议（非本次重点，简述）

- 工作流定义画布中，`ASSET_SENSOR` 任务节点展示当前等待的资产、条件表达式、当前 AssetState 快照对比（差几个 snapshot / watermark 差多久）。
- 新增"资产大盘"页面：资产列表、最近事件、当前状态、关联的下游工作流、触发历史时间线，类似 Airflow 的 Dataset 视图。

## 4.4 DAG链路中的资产依赖模式

在新一代数仓的DAG中，所有任务的触发都应该遵循**纯snapshot模型**，无需时间表达式或原生依赖门禁：

| DAG层级 | 依赖来源 | 示例 | 说明 |
|---|---|---|---|
| **ODS（数据入湖，第一层）** | 外部数据源的snapshot推进 | Flink CDC -> Paimon ods_order_latest snapshot | 最初的数据资产版本来源，通常由外部数据引擎（Flink/Spark）生产snapshot事件 |
| **DWD（维度加工，中间层）** | 依赖前一层ODS的snapshot | 等待 ods_order_latest、ods_payment_latest 快照都推进 | 多个上游资产通过AND条件组合 |
| **DWS（主题聚合，中间层）** | 依赖DWD的多张snapshot | 等待 dwd_trade_event、dwd_order_dim 快照推进 | 同样是多资产AND依赖，与DAG中的DAG前后置依赖保持一致 |
| **ADS（对外服务，终端层）** | 依赖DWS的snapshot | 等待 dws_gmv_daily、dws_order_stat 快照推进 | 最终的数据产品层 |

**关键原则**：
- 整个DAG中任何任务的触发**完全由其依赖资产的snapshot版本驱动**，不涉及时间表达式或cron。
- DAG中的"前后置依赖"自动转化为"snapshot依赖"：即后一个任务等待前一个任务产出的快照。
- 跨流依赖（依赖其他DAG中的任务）也升级为snapshot模型：通过关联外部DAG产出的资产快照来实现。
- **无需混合原生依赖和资产依赖**：新一代数仓架构下，资产snapshot就是唯一的触发依据。

## 4.5 对现有Master调度架构的影响与改造

### 4.5.1 现有Master调度流程概述

DolphinScheduler Master 的核心调度流程如下：

```text
1. CommandService 定时扫描 t_ds_command 表（按 id 递增）
   ↓
2. 对每条 Command 记录：
   a) 检查 commandType（START_PROCESS / SCHEDULER / COMPLEMENT_DATA / ...）
   b) 读取关联的 workflow_definition（工作流定义）
   c) 创建 WorkflowInstance（工作流实例，初始状态=SUBMITTED）
   d) 生成该工作流的 DAG 实例（TaskInstance + TaskDependency）
   ↓
3. MasterScheduler 定时扫描 WorkflowInstance / TaskInstance（按状态分组）
   ↓
4. 工作流实例状态转移：
   SUBMITTED -> RUNNING（任何非跳过的任务已提交）
             -> SUCCESS（所有任务成功）
             -> FAILURE（任何关键路径任务失败）
   ↓
5. 任务实例状态转移（核心）：
   SUBMITTED -> READY/WAITING_DEPENDENCY（依赖不满足）
             -> RUNNING（所有依赖满足，发送给 Worker）
             -> SUCCESS/FAILED/...
   ↓
6. 依赖判定逻辑（当前）：
   - DEPENDENT 任务：轮询查询上游任务实例状态（是否 SUCCESS）
   - DAG 前后置依赖：检查前置任务是否 SUCCESS
   - 其他：判定条件立即满足，直接 READY
   ↓
7. Worker 执行 TaskInstance，汇报结果
   ↓
8. 重复步骤 3-7，直到工作流完成
```

### 4.5.2 资产事件调度的集成点

引入资产事件驱动调度后，Master 需要处理**两种触发来源**和**三层依赖判定**：

**触发来源（Command 创建）**：

```text
Original:  START_PROCESS / SCHEDULER / COMPLEMENT_DATA / ...
           ↓
Enhanced:  新增 ASSET_EVENT_TRIGGER / 或复用 START_PROCESS + 特殊 commandParam
           ↓
           由 DependencyResolver 模块判定资产就绪后调用 CommandService#createCommand
           写入 t_ds_command（与现有流程完全相同）
```

**三层依赖判定（从外到内）**：

```text
第一层：工作流级依赖（Strategy C，二期实现）
  ├─ 资产依赖判定：DependencyResolver 评估 t_ds_asset_dependency（task_definition_code=null 的记录）
  └─ 不满足 => 不创建 Command / 工作流实例

第二层：任务级依赖（Strategy A，MVP）
  ├─ 原有 DAG 前后置依赖：Master 现有逻辑，检查前置任务是否 SUCCESS
  ├─ ASSET_SENSOR 任务依赖：轮询 t_ds_asset_state，判定条件（snapshot / watermark / quality）
  └─ DEPENDENT 任务依赖：轮询上游任务实例状态

第三层：工作流内部 DAG 依赖
  └─ 任务间的串联 / 并联，正常的 DAG 拓扑执行
```

### 4.5.3 Master 侧的具体改造

#### 改造 1：DependencyResolver 集成到 Master

**概念模型**：

```java
public interface DependencyResolver {
    /**
     * 评估一个 dependency_group 是否 READY
     * @param workflowCode 工作流定义编码（用于工作流级依赖评估）
     * @param taskCode 任务定义编码（如果为 null，表示工作流级依赖）
     * @param dependencyGroup 依赖分组名
     * @return 依赖是否 READY（true 可以触发 / 释放，false 继续等待）
     */
    DependencyStatus resolveDependency(Long workflowCode, Long taskCode, String dependencyGroup);
    
    /**
     * 注册资产状态变化的监听器
     * DependencyResolver 内部可基于事件回调而非轮询，降低延迟
     */
    void registerAssetStateChangeListener(AssetStateChangeListener listener);
}
```

**工作流级触发（Strategy C）的时机**：

```text
在 DependencyResolver 判定工作流级依赖 READY 后，立即调用：
  commandService.createCommand(
    Command.builder()
      .commandType(CommandType.ASSET_EVENT_TRIGGER)
      .workflowDefinitionCode(workflowCode)
      .commandParam(JSON 序列化的 {assetKey, snapshotId, triggerKey, ...})
      .build()
  );

然后在 t_ds_asset_trigger_history 中 INSERT 占位记录（trigger_key 唯一约束）。

Master 的现有 CommandService scanner 会照常消费这条 Command，创建 WorkflowInstance，执行 DAG。
无需在 Master 侧做特殊识别——完全复用现有流程。
```

#### 改造 2：ASSET_SENSOR 任务类型的执行生命周期

**在 Master 执行阶段的行为**：

```text
1. TaskInstance 状态 = SUBMITTED，判定是否可进入 READY
   ↓
2. MasterScheduler 调用 task plugin 的 tracker（AssetSensorTracker extends TaskTracker）
   ↓
3. AssetSensorTracker.checkTaskStatus() 周期性调用 DependencyResolver#resolveDependency()
   返回值：
   - DependencyStatus.READY => 任务转为 READY，发送给 Worker 执行
   - DependencyStatus.BLOCKED => 保持 WAITING_DEPENDENCY，继续轮询
   - DependencyStatus.TIMEOUT => 任务转为 ERROR（超时失败）
   ↓
4. Worker 侧照常执行该任务（可能是 Shell / SQL，或空操作代表"依赖已满足"）
   ↓
5. 下游任务通过 DAG 前后置依赖关系正常推进
```

**代码集成点**（`dolphinscheduler-master` 模块）：

```text
dolphinscheduler-master/src/main/java/org/apache/dolphinscheduler/server/master/
├── engine/
│   └── DAGExecutionEngine.java  (现有，DAG 状态机)
│       ├── 改造点1：在 commitTask / dispatchTask 前检查 ASSET_SENSOR 任务
│       │   if (isAssetSensorTask) {
│       │       // 调用 DependencyResolver 检查依赖
│       │       if (!dependencyResolver.resolveDependency(...).isReady()) {
│       │           return; // 不提交给 Worker，保持 WAITING_DEPENDENCY
│       │       }
│       │   }
│       └── 改造点2：在工作流 SUBMITTED -> RUNNING 转移前
│           if (isAssetEventTrigger && hasWorkflowLevelAssetDependency) {
│               // 可选：在 Master 侧提前检查，尽早发现不满足的依赖
│               // （否则直接创建实例，在工作流执行时逐个任务检查）
│           }
├── processor/
│   ├── WorkflowProcessor.java (工作流实例处理)
│   │   └── 改造点3：处理 CommandType.ASSET_EVENT_TRIGGER
│   │       解析 commandParam 提取 assetKey / snapshotId 等信息，
│   │       存入 WorkflowInstance 的 commandParam 或扩展字段
│   │
│   └── TaskProcessor.java (任务实例处理)
│       └── 改造点4：任务状态转移时，对 ASSET_SENSOR 任务特殊处理
│           调用 AssetSensorTracker.checkTaskStatus()
│
└── service/
    ├── DependencyResolverService.java (新增)
    │   ├── 注入 AssetStateService (DAO 层)
    │   ├── 注入 AssetDependencyService (DAO 层)
    │   └── 实现 resolveDependency 逻辑
    │       a) 查询 t_ds_asset_dependency(task_definition_code=?, dependency_group=?)
    │       b) 逐一查询 t_ds_asset_state 对应的 asset_key
    │       c) 对每个资产条件求值（snapshot exists, watermark gte, quality status, ...）
    │       d) 组合结果（同 group 所有条件 AND，不同 group 间 OR）
    │
    └── TrackerManager.java (现有，但需扩展)
        └── 改造点5：注册 ASSET_SENSOR 对应的 tracker
            trackerFactory.get(TaskType.ASSET_SENSOR) 
            => new AssetSensorTracker(dependencyResolverService)
```

#### 改造 3：工作流实例启动与 SUBMITTED 状态的处理

**当前逻辑**（简化）：

```java
// MasterScheduler#submitWorkflow
Command command = commandService.findOne(commandId);
WorkflowInstance instance = new WorkflowInstance();
instance.setWorkflowDefinitionCode(command.getWorkflowDefinitionCode());
instance.setState(WorkflowExecutionStatus.SUBMITTED);
// ... 生成 TaskInstance，立即进入 DAG 执行
```

**新增逻辑（如果需要工作流级资产依赖检查）**：

```java
// 在 WorkflowInstance 创建后、生成 DAG 任务前
if (command.getCommandType() == CommandType.ASSET_EVENT_TRIGGER) {
    // 可选：提前验证工作流级资产依赖是否已满足
    // （这是一个检查点，如果仍未满足则可能是 Compensation 重试时的竞态）
    boolean workflowLevelAssetReady = dependencyResolverService
        .resolveDependency(workflowCode, null, "default")
        .isReady();
    
    if (!workflowLevelAssetReady) {
        // 记录告警，可选地延迟创建 TaskInstance
        log.warn("Workflow {} triggered by asset event but asset dependency no longer ready. " +
                 "This may indicate compensation retry or state backtrack.", workflowCode);
    }
}

// 正常流程继续，生成 DAG 任务
for (TaskDefinition taskDef : dag.getTasks()) {
    TaskInstance taskInstance = new TaskInstance();
    taskInstance.setTaskDefinitionCode(taskDef.getCode());
    taskInstance.setState(TaskExecutionStatus.SUBMITTED);
    if (isAssetSensorTask(taskDef)) {
        // ASSET_SENSOR 任务状态初始化为 WAITING_DEPENDENCY
        taskInstance.setState(TaskExecutionStatus.WAITING_DEPENDENCY);
    }
    // ... 保存 taskInstance
}
```

#### 改造 4：Master 的循环扫描逻辑

**原有扫描周期**：

```text
CommandService.scanner()  // 扫描 t_ds_command，周期=配置（默认 5s）
  ↓
WorkflowProcessor.processWorkflow()  // 处理工作流状态转移
  ↓
TaskProcessor.processTask()  // 处理任务状态转移，检查依赖
  ↓
AbstractTaskTracker.checkTaskStatus()  // 调用具体 task plugin 的 tracker（如 DependentTaskTracker）
```

**新增扫描逻辑**（与现有不冲突）：

```text
新增 AssetEventCompensationScanner（可选，作为 Master 内部的后台线程）
  周期：5~15 分钟（独立于 CommandService 扫描）
  ↓
  扫描 t_ds_asset_event 和 t_ds_asset_state，检查：
  1. 是否有漏采集的事件（e.g. Paimon 实际最新 snapshot_id > t_ds_asset_state.latest_snapshot_id）
  2. 是否有卡顿的 trigger_history（TRIGGERING 或 FAILED 状态超过 N 分钟）
  ↓
  对于漏采集：补写 t_ds_asset_event（去重自动处理）-> 触发 DependencyResolver 重新评估
  对于卡顿：可选地重试 createCommand（检查唯一约束冲突，确保不重复）
  ↓
  该扫描是"兜底补偿"，不影响主调度流程
```

### 4.5.4 对现有架构的最小化影响

**不需要改动**：

- ✅ WorkflowInstance 和 TaskInstance 的核心状态机（SUBMITTED / RUNNING / SUCCESS / FAILED / ...）
- ✅ DAG 前后置依赖的表达和执行（保持原样）
- ✅ DEPENDENT 任务的现有逻辑（完全独立，不干扰）
- ✅ Worker 任务执行的生命周期（无感知）
- ✅ Alert / Monitor / Backfill 等周边功能（兼容）

**需要改动**（但都是插件化、非侵入式）：

- 新增 `ASSET_SENSOR` 任务类型（如 `DependentLogicTask` 一样作为 task plugin）
- 新增 `AssetSensorTracker` 作为该任务的 tracker（实现 TaskTracker 接口）
- 新增 `DependencyResolverService` 负责依赖评估逻辑（Service 层，不改 Master 内核）
- 新增 `CommandType.ASSET_EVENT_TRIGGER` 类型（可选，或复用 START_PROCESS）
- 在 `TaskProcessor#submitTask()` 前增加一行检查：
  ```java
  if (task is ASSET_SENSOR) {
      if (!dependencyResolver.resolveDependency(...).isReady()) {
          return; // 不提交
      }
  }
  ```

**改动的代码行数**：

预估 **< 500 行**（包括新增的 DependencyResolverService、AssetSensorTracker 和必要的集成点检查），相对于 DolphinScheduler 整体代码量（数万行）可以说是非常小的改动。

### 4.5.5 并发与一致性保证

**多 Master 并发场景**：

```text
场景：5 个 Master 实例同时运行，都在扫描 t_ds_command 和执行工作流

冲突点1：CommandService.createCommand() 写入同一条 Command
  => 依赖 t_ds_command 的 PK 或 sequence，一般由应用侧写入，不会重复

冲突点2：多个 Master 同时评估 ASSET_SENSOR 任务的依赖，决定是否提交给 Worker
  => 每个 Master 各自维护 TaskTracker，评估结果本地缓存
  => 当依赖首次从 NOT_READY 转为 READY 时，最快的 Master 抢到 Worker 资源
  => TaskProcessor#submitTask() 会检查 TaskInstance 是否已有 Worker 分配，重复提交会被幂等处理
  => 无需分布式锁，因为 TaskInstance 表会做状态检查

冲突点3：多个 Master 同时插入 t_ds_asset_trigger_history
  => 唯一约束 uk_trigger_key 保证只有一个成功
  => 失败的 Master 捕获唯一冲突异常，记录日志后继续（不影响调度）
```

**幂等性保证**：

```text
trigger_key 形如：
  workflow_12345 + dependency_group_default + snapshot_1001 + snapshot_1002 + ...
  = 完整标识"该工作流依赖这一组快照组合"的唯一键

同一 trigger_key 最多被成功创建一次 t_ds_asset_trigger_history 记录。
对应的 Command 也最多被创建一次（或可在 commandParam 中埋入 trigger_key 做检查）。
因此，无论有多少个 Master 并发评估，最终只会创建一个 WorkflowInstance。
```

### 4.5.6 关键配置项

Master 侧需要的新配置（写入 Master 的 application.yaml）：

```yaml
# 资产事件驱动调度开关
dolphinscheduler:
  asset-event-scheduling:
    enabled: true                           # 全局开关
    
    # DependencyResolver 配置
    dependency-resolver:
      cache-expire-seconds: 60              # 资产状态本地缓存过期时间
      resolve-timeout-seconds: 5            # 单次依赖评估的超时时间
      
    # AssetEventCompensationScanner 配置
    compensation-scanner:
      enabled: true
      interval-seconds: 300                 # 补偿扫描周期（5分钟）
      batch-size: 100                       # 每次扫描的批量大小
      
    # 事件源配置
    event-source:
      paimon-polling:
        enabled: true
        interval-seconds: 30                # Paimon $snapshots 轮询周期
        batch-size: 500
      push-receiver:
        enabled: true                       # 是否启用 HTTP/Webhook 接收事件
```

这些配置都是**可选的、渐进式的**，MVP 时可以只启用 `enabled: true`，使用默认值。

---

## 5. 端到端示例：基于 Paimon 订单表的 GMV 场景

### 5.1 业务流程与数据资产

```text
MySQL 订单库（order / payment / refund 表）
  -> Flink CDC 持续消费 binlog，生产 Paimon snapshot
  -> Paimon 湖仓表：
     ods_order_latest（主键表，最新订单状态）→ snapshot推进
     ods_payment_latest（主键表，最新支付状态）→ snapshot推进
  -> DWD 层任务：dwd_trade_event_fact（识别支付/退款事件）
     依赖 ods_order_latest + ods_payment_latest 快照都推进
  -> DWS 层任务：dws_gmv_daily（每日GMV聚合）
     依赖 dwd_trade_event_fact 快照推进
  -> ADS 层任务：ads_gmv_dashboard（对外报表/API）
     依赖 dws_gmv_daily 快照推进
```

### 5.2 工作流定义（DAG 结构）——纯snapshot模型

**新一代数仓DAG结构**（无cron、无时间表达式）：

```text
Flink CDC 持续运行，生产 ods_order_latest、ods_payment_latest snapshot
  ↓
DAG工作流启动（由外部系统触发，例如Airflow或DolphinScheduler的API）
  ↓
Task 1: DWD_trade_event_fact
  依赖：ods_order_latest snapshot_id >= N1 AND ods_payment_latest snapshot_id >= N2
  执行：读两张表的最新快照，识别交易事件
  产出：dwd_trade_event_fact snapshot
  ↓
Task 2: DWS_gmv_daily
  依赖：dwd_trade_event_fact snapshot_id >= M1
  执行：聚合每日GMV
  产出：dws_gmv_daily snapshot
  ↓
Task 3: ADS_gmv_dashboard
  依赖：dws_gmv_daily snapshot_id >= P1
  执行：同步到报表库
  产出：ads_gmv_dashboard 就绪
```

**核心改进**：
- 完全无cron，无固定执行时刻。DAG启动后，每个任务的执行取决于其依赖快照的推进。
- 如果Flink CDC延迟，ODS快照推进慢，则DWD会等待；一旦快照到达，DWD立即执行。
- 多个上游资产（ods_order_latest + ods_payment_latest）的快照通过AND条件组合。

### 5.3 资产依赖声明示例

**DWD 层对两个 ODS 表的依赖**：

```json
[
  {
    "workflowDefinitionCode": 12345,
    "taskDefinitionCode": 100,    // DWD_trade_event_fact 任务
    "assetKey": "paimon://ods_db/ods_order_latest",
    "dependencyGroup": "trade_event_group",
    "conditionJson": {
      "assetKey": "paimon://ods_db/ods_order_latest",
      "snapshotRequired": true,
      "qualityStatus": "PASSED",
      "schemaStatus": "COMPATIBLE"
    },
    "enabledFlag": 1
  },
  {
    "workflowDefinitionCode": 12345,
    "taskDefinitionCode": 100,    // 同一任务的第二个依赖
    "assetKey": "paimon://ods_db/ods_payment_latest",
    "dependencyGroup": "trade_event_group",
    "conditionJson": {
      "assetKey": "paimon://ods_db/ods_payment_latest",
      "snapshotRequired": true,
      "qualityStatus": "PASSED",
      "schemaStatus": "COMPATIBLE"
    },
    "enabledFlag": 1
  }
]
```

**DWS 层对 DWD 层的依赖**：

```json
[
  {
    "workflowDefinitionCode": 12345,
    "taskDefinitionCode": 200,    // DWS_gmv_daily 任务
    "assetKey": "paimon://dw_db/dwd_trade_event_fact",
    "dependencyGroup": "gmv_group",
    "conditionJson": {
      "assetKey": "paimon://dw_db/dwd_trade_event_fact",
      "snapshotRequired": true,
      "qualityStatus": "PASSED"
    },
    "enabledFlag": 1
  }
]
```

**触发行为**：

1. Flink CDC 持续消费 MySQL binlog，每次产生新 snapshot 时会生产事件（例如 ods_order_latest snapshot_id=1001）。
2. AssetEventScanner 或 Push 接口捕获事件，写入 `t_ds_asset_event`。
3. AssetStateService 以乐观锁更新 `t_ds_asset_state`：`asset_key=paimon://ods_db/ods_order_latest, latest_snapshot_id=1001`。
4. DependencyResolver 反向索引扫描：该 asset_key 关联的所有 dependencyGroup 有哪些？（例如 trade_event_group）。
5. 对 trade_event_group 中的所有依赖进行评估：
   - ods_order_latest 快照存在? ✓  
   - ods_payment_latest 快照存在? ✓ (假设也已推进)  
   - 两者 qualityStatus 都=PASSED? ✓  
   → 依赖组 READY
6. 计算 `trigger_key=workflow_12345_taskdef_100_group_trade_event_group_snapshot_1001_1002`（包含所有关键快照ID），向 `t_ds_asset_trigger_history` 唯一插入。
7. 插入成功 → DWD_trade_event_fact 任务实例被通知依赖满足，立即执行。
8. DWD 产出新快照（例如 dwd_trade_event_fact snapshot_id=2001），重复步骤2-7，触发 DWS。
9. ADS 类似地等待 DWS 快照推进。

## 6. 观测性设计

### 6.1 关键指标（Metrics）

系统应暴露以下 Prometheus 指标（Micrometer），方便 Grafana 大盘接入：

```text
# 事件接入层
lakehouse_asset_event_received_total{source_type="paimon", asset_key="..."}
  - 意义：从指定资产收到的事件总数

lakehouse_asset_event_deduplicated_total{source_type="paimon", asset_key="..."}
  - 意义：被去重丢弃的重复事件数

lakehouse_asset_event_invalid_total{source_type="paimon", asset_key="...", reason="xxx"}
  - 意义：被拒绝的无效事件（格式错误、资产不存在等），按原因分类

# 资产状态层
lakehouse_asset_state_updated_total{asset_key="..."}
  - 意义：资产状态被推进的次数

lakehouse_asset_state_lag_seconds{asset_key="..."}
  - 意义：资产最新快照的年龄（当前时间 - snapshot commit_time）

lakehouse_asset_watermark_lag_seconds{asset_key="..."}
  - 意义：资产 watermark 相对于业务日期的延迟（例如 watermark=10-10 23:59:59，当前日期 10-11，则 lag=-10秒表示超前）

# 依赖与触发层
lakehouse_asset_dependency_ready_total{asset_key="...", workflow_code="...", dependency_group="..."}
  - 意义：依赖就绪的次数

lakehouse_asset_dependency_blocked_by{asset_key="...", workflow_code="...", blocked_by="snapshot|watermark|quality|schema"}
  - 意义：依赖被卡住的原因分布

lakehouse_asset_trigger_success_total{workflow_code="...", trigger_source="asset_event"}
  - 意义：由资产事件成功触发的工作流实例数

lakehouse_asset_trigger_failed_total{workflow_code="...", reason="xxx"}
  - 意义：触发失败的工作流数，按原因分类（command_create_failed、duplicate_trigger 等）

# 补偿层
lakehouse_asset_compensation_scan_total{asset_key="..."}
  - 意义：补偿扫描器运行次数

lakehouse_asset_compensation_missed_events{asset_key="..."}
  - 意义：补偿发现的漏采集事件数

lakehouse_asset_trigger_history_retry_total{trigger_status="..."}
  - 意义：触发历史被重试的次数
```

### 6.2 关键日志字段

所有日志输出应包含以下上下文字段，便于链路追踪和问题排查：

```json
{
  "timestamp": "2024-09-10T10:15:30.123Z",
  "level": "INFO",
  "logger": "org.apache.dolphinscheduler.service.asset.AssetEventScanner",
  "traceId": "abc123xyz",
  "assetKey": "paimon://ods_db/ods_order_latest",
  "partitionKey": "dt=20240910",
  "snapshotId": 1001,
  "schemaId": 5,
  "watermark": "2024-09-10T23:59:59Z",
  "eventType": "SNAPSHOT_COMMITTED",
  "eventId": "paimon_evt_20240910_1001_xxx",
  "workflowDefinitionCode": 12345,
  "taskDefinitionCode": 54321,
  "workflowInstanceId": 67890,
  "taskInstanceId": 11111,
  "triggerKey": "workflow_12345_group_gmv_default_snapshot_1001",
  "status": "READY | BLOCKED_BY_QUALITY | TRIGGERED | FAILED",
  "reason": "Asset quality check passed, dependency READY",
  "message": "Asset snapshot processed, dependency READY, triggering workflow instance..."
}
```

### 6.3 告警规则（Alerting Rules）

|  告警条件  |  阈值  |  优先级 | 处理建议 |
|---|---|---|---|
| `lakehouse_asset_state_lag_seconds > 3600` | 1 小时 | P2 | 检查 Paimon 表是否有新快照产生；检查 Scanner 健康状态 |
| `lakehouse_asset_watermark_lag_seconds > 600` | 10 分钟 | P2 | 检查数据源 CDC/Flink 是否延迟；检查 Paimon 表的 `_watermark` 字段 |
| `lakehouse_asset_trigger_failed_total increase > 3/5min` | 5 分钟内失败 3 次 | P1 | 检查 Command 创建是否异常；检查 Master 可用性 |
| `lakehouse_asset_dependency_blocked_by{blocked_by="quality"} > 0` 持续 > 30min | 30 分钟 | P2 | 质量检查失败，需要数据生产方或质量团队介入 |
| `lakehouse_asset_compensation_missed_events > 10/hour` | 1 小时 > 10 个 | P1 | 说明 Push 或 Poll 路径有系统问题，补偿机制在弥补但需要根本修复 |

## 7. 资产标识规范

### 7.1 AssetKey 命名规范

新一代数仓中，assetKey 标识一个湖仓表的全局唯一位置，无需包含分区变量：

```text
paimon://<catalog_name>/<database_name>/<table_name>
```

示例：

- `paimon://ods_db/ods_order_latest` — ODS层订单表
- `paimon://dw_db/dwd_trade_event_fact` — DWD层交易事件表
- `paimon://dws_db/dws_gmv_daily` — DWS层每日GMV表

**注意**：
- assetKey 指向表级别，不包含分区（partition_spec），因为新一代数仓中每个表的所有分区都在同一个Paimon/Iceberg/Hudi主键表中，版本推进是表级的。
- 若需要追踪不同分区的快照独立性，应在 `t_ds_asset_event` 中的 `partitionKey` 字段记录（例如 `dt=20240910`）。

### 7.2 AssetKey 注册与发现

- 资产需在 `t_ds_asset` 表中注册（API或批量脚本），包含 sourceType（paimon/iceberg/hudi）、catalog、database、table 等元信息。
- 工作流定义依赖时，通过 UI"资产大盘"或 API 查询并选择已有资产，类似 Airflow Dataset 选择器。
- 若依赖的 assetKey 不存在，DependencyResolver 会记录警告并返回 BLOCKED_BY_UNKNOWN_ASSET，防止工作流被永久卡住。

## 8. 分阶段实施路线图

### 阶段一（MVP，2~3 周量级）：Paimon snapshot -> ASSET_SENSOR 单资产触发闭环

- 新增表：`t_ds_asset`、`t_ds_asset_event`、`t_ds_asset_state`、`t_ds_asset_dependency`（仅 `task_definition_code` 非空场景）、`t_ds_asset_trigger_history`、`t_ds_asset_event_consumer_offset`。
- 实现 `AssetEventScanner`（轮询 Paimon `$snapshots`）。
- 实现 `AssetStateService`（乐观锁更新、乱序丢弃）。
- 实现 `DependencyResolver`（仅支持单资产、snapshot 存在性判定）。
- 实现 `ASSET_SENSOR` 任务插件（对接 Strategy A）。
- 明确 MVP 的问题边界：优先解决"任务需要按 snapshot 就绪触发而不是时间触发"的场景，不改变既有原生 DAG 依赖语义。
- 打通端到端：手工制造 Paimon 表 snapshot 推进 -> 观察 ASSET_SENSOR 任务在几十秒内由等待转为成功 -> 下游任务执行。
- 补齐幂等/乱序/基础可观测（日志字段，见 5.3）单元测试。

关键接口草图：

```java
public interface AssetEventSource {
    List<AssetEvent> pollNewEvents(Asset asset, Long sinceSnapshotId);
}

public interface AssetStateService {
    /** 乱序保护 + 乐观锁更新，返回 true 表示状态被推进 */
    boolean applyEvent(AssetEvent event);
    AssetState getState(String assetKey);
}

public interface AssetDependencyResolver {
    /** 返回 READY / BLOCKED_BY_WATERMARK / BLOCKED_BY_QUALITY / BLOCKED_BY_SCHEMA / WAITING */
    DependencyEvalResult evaluate(AssetDependency dependency);
}

public interface AssetTriggerService {
    /** 幂等触发，内部完成 trigger_key 唯一插入 + Command 创建或 ASSET_SENSOR 状态回写 */
    TriggerResult triggerIfReady(AssetDependencyGroup group);
}
```

### 阶段二：多资产 AND/OR、watermark/quality/schema 条件、Compensation Scanner

- 扩展 `condition_json` 支持 watermark/qualityStatus/schemaStatus 判定。
- 支持 `dependency_group` 多资产 AND 聚合、跨 group OR。
- 上线 Compensation Scanner，定期核对 Paimon 实际快照与 AssetState 差异、重试失败触发。
- 补充观测指标与 Grafana 面板。

### 阶段三：Push 上报接口、工作流级 Strategy C、Iceberg/Hudi 事件源接入

- 新增 `POST /asset-events` API，Flink/Spark 作业可主动上报，与 Polling 共存去重。
- 支持工作流级直接创建 WorkflowInstance（Strategy C），并补充 bizDate 推导与补数场景边界的规则文档。
- 扩展 `AssetEventSource` 接口的 Iceberg/Hudi 实现。

### 阶段四：治理与观测增强（非本次核心，预留）

- 资产血缘、资产全生命周期看板、跨项目资产依赖等。

## 9. 新一代数仓DAG的改造与兼容性

### 9.1 与现有 cron 调度的关系

新一代数仓DAG**完全基于snapshot版本推进**，而不是时间触发。这意味着：

- **不保留 cron**：在新架构下，DAG 不再有"定时执行时间"的概念。
- **不保留时间依赖**：DAG 中的任务也不再依赖原始的 DEPENDENT（它面向历史实例结果）或"业务日期"这类时间维度。
- **完全由数据资产版本驱动**：DAG 启动后，每个任务的执行 100% 取决于其上游资产快照的推进。

对于现有的使用者：
- 若要升级到新一代数仓架构，需要**全量重新定义 DAG**，将所有"cron + DEPENDENT"的组织方式改为"snapshot + assetKey"。
- 这是一次架构升级，不是渐进式改造，因此需要统一规划和实施。

### 9.2 ASSET_SENSOR 任务类型的注册

- ASSET_SENSOR 是新增的任务类型，需要在 `dolphinscheduler-task-plugin` 注册并打包发布。
- 现有集群升级到新版本后自动支持。
- 若工作流定义中引用了 ASSET_SENSOR 而集群未安装，Master 会记录错误并跳过该任务。

### 9.3 数据库升级路径

- `t_ds_asset` 等新表通过 `dolphinscheduler-dao` 的 SQL migration 脚本自动创建。
- 升级无需停机，migration 脚本幂等且对现有数据无侵入。
- 兼容 MySQL 5.7+、PostgreSQL 10+ 等常见数据库。

### 9.4 与既有 DependentTask 的区别

| 特性 | DEPENDENT 任务（旧） | ASSET_SENSOR 任务（新） |
|---|---|---|
| 依赖对象 | 上游任务/工作流实例执行结果 | 外部数据资产快照版本 |
| 适用 | 传统DAG内的任务编排 | 新一代数仓的数据版本驱动 |
| 触发条件 | 上游任务成功/失败 | 资产快照推进/质量通过 |

## 10. 常见问题解答（FAQ）

**Q1：DAG中如何处理多个上游资产（例如DWS依赖多个DWD表）的依赖？**

A1：通过 `dependencyGroup` 和AND条件组合。同一 dependencyGroup 下的所有资产依赖都必须READY，该组才READY。例如：
```json
[
  { "taskDefinitionCode": 200, "assetKey": "paimon://dw_db/dwd_trade_event", "dependencyGroup": "dws_gmv" },
  { "taskDefinitionCode": 200, "assetKey": "paimon://dw_db/dwd_order_dim", "dependencyGroup": "dws_gmv" }
]
```
两张DWD表的快照都推进时，DWS才会触发。

---

**Q2：如果资产事件丢失了（例如 Paimon snapshot 没有被 Scanner 捕获），下游会永久卡住吗？**

A2：不会。有两层防护：

1. 每个任务都设置了 `timeoutMinutes`（例如 60 分钟），超时后任务进入 ERROR 状态，触发告警。
2. Compensation Scanner 每 5~15 分钟运行一次，对比 Paimon 实际快照与 AssetState，若发现漏采集会补写事件并重新触发。

---

**Q3：多个工作流依赖同一个资产，它们会被重复触发吗？**

A3：不会。每个工作流对该资产的依赖是独立的 trigger_key，例如：
```text
workflow_A_group_default_snapshot_1001
workflow_B_group_default_snapshot_1001
```
虽然都依赖同一个 snapshot，但触发记录是分开的，各自触发一次。

---

**Q4：如果任务因网络原因多次重试，会重复触发下游吗？**

A4：不会。任务实例的重试机制在 Worker 侧处理，只要任务最终成功，就会释放下游任务。下游任务会通过 DAG 依赖被触发，同样遵循"任务实例唯一性"原则，不会重复。

---

**Q5：是否支持多个数据湖表格式（Paimon、Iceberg、Hudi）的混合依赖？**

A5：MVP（阶段一）只支持 Paimon。阶段二扩展 watermark/quality/schema 条件后可统一处理。阶段三会增加 Iceberg/Hudi 事件源，理论上支持混合依赖，但需要确保每种格式的 EventSource 实现质量和测试覆盖。

---

**Q6：如何在新架构下支持跨流依赖（DAG之间的任务依赖）？**

A6：跨流依赖升级为"依赖外部DAG产出的资产快照"。例如，DAG-B 的某个任务依赖 DAG-A 产出的某个表快照：

```json
{
  "assetKey": "paimon://dw_db/external_dag_a_output_table",
  "dependencyGroup": "cross_flow",
  "conditionJson": {
    "snapshotRequired": true,
    "qualityStatus": "PASSED"
  }
}
```

这样 DAG-B 会等待外部 DAG-A 的表快照推进，而不是等待任务实例。

---

**Q7：数据库如果 INSERT 唯一约束冲突时的异常处理，会影响性能吗？**

A7：数据库唯一约束冲突通常很快（< 1ms），是预期内的正常路径，被捕获后记录日志继续执行，不会有明显性能影响。但如果并发度极高（例如 10+ Master 实例同时竞争触发），建议监控数据库连接池和索引性能。

## 11. 风险与开放问题

1. **补数（backfill）与事件驱动的语义定义**：手动补数会重新产出历史快照，需要明确这些历史快照是否应触发下游任务。建议在设计中定义"backfill_mode"标记，告诉系统是否对历史快照触发。

2. **多 Master 并发触发的竞态窗口**：虽然唯一约束保证最终只有一次成功插入 `trigger_history`，但在 `createCommand` 调用与 `trigger_history` 落库之间仍存在短暂不一致窗口。需要明确"先插入 trigger_history 占位再调用 createCommand"的顺序约束，并对 `createCommand` 失败做补偿。

3. **任务轮询频率与数据库压力**：如果同时存在大量任务实例轮询同一批资产状态，需要考虑批量拉取/缓存策略，避免对 `t_ds_asset_state` 造成过大查询压力。建议引入 DependencyResolver 主动回调机制替代逐任务轮询。

4. **CommandType 语义扩展的兼容性**：新增 `ASSET_EVENT_TRIGGER` 类型需要评估对现有依赖 `CommandType` 做 switch-case 穷举的代码路径（如告警、日志、UI 展示）的影响。需全仓库搜索所有 `CommandType` 的使用点做兼容适配。

5. **跨 catalog/跨项目资产依赖的权限模型**：资产可能归属不同 DolphinScheduler 项目甚至不同租户，依赖声明与触发是否需要跨项目鉴权尚未设计。需要后续单独讨论。

6. **事件从 Push 与 Poll 双路径去重的时钟/顺序依赖**：两条路径若在极短时间内先后到达同一 snapshot 事件，唯一约束能防止重复落库，但需要验证在高并发下数据库唯一索引冲突处理的实现正确性与性能。

7. **exactly-once 声明范围**：本设计只能保证"同一 trigger_key 的触发记录只成功创建一次"，不能保证"下游 Command 消费与 WorkflowInstance 创建"在极端故障场景下的严格 exactly-once。需要在文档中明确边界。

8. **分区与无分区表的混合依赖**：当DAG中某个任务同时依赖有分区表和无分区表的快照时，如何统一表达和评估。需要在依赖表达式中定义clear的规则。

## 12. 总结

本设计将 DolphinScheduler 从"时间DAG调度系统"升级为"数据资产版本驱动的调度系统"。核心原理是：

- **整个工作流（DAG）代表ODS→DWD→DWS→ADS的完整数据加工链路**。
- **DAG中每一层任务的触发完全由其依赖资产的快照版本推进决定**，而非时间或手工触发。
- **通过新增 ASSET_SENSOR 任务类型**，复用现有任务实例状态机和DAG执行机制。
- **通过新增资产表和事件去重机制**，保证幂等触发和多Master并发安全。
- **不破坏现有 DolphinScheduler 内核**，以插件化任务和现有 Command 入口实现对接。

这是面向新一代数仓（Paimon/Iceberg/Hudi）的原生调度方案，解决了传统时间触发与数据可用性错配的根本问题。
