# 术语表与数据模型参考

## A

### Asset（数据资产）

数据湖中的一个表或分区级别的资源。

**字段**：
- `assetKey`：全局唯一标识，格式为 `paimon://catalog/db/table`
- `storageFormat`：存储格式（PAIMON、ICEBERG、HUDI）
- `catalogName`、`databaseName`、`tableName`：目录、数据库、表名
- `partitionExpr`：分区表达式（可选）
- `description`：资产描述
- `owner`：资产所有者

**示例**：
```json
{
  "assetKey": "paimon://ods_db/ods_order_latest",
  "storageFormat": "PAIMON",
  "catalogName": "ods_db",
  "databaseName": "ods_db",
  "tableName": "ods_order_latest",
  "description": "订单最新状态表（主键表）",
  "owner": "data_platform_team"
}
```

### AssetDependency（资产依赖）

工作流或任务对某个数据资产的依赖声明。

**字段**：
- `workflowCode`：工作流定义编码
- `taskCode`：任务定义编码（NULL 表示工作流级依赖）
- `assetKey`：依赖的资产
- `dependencyGroup`：依赖分组名（支持同组 AND、跨组 OR）
- `condition`：依赖条件（JSON 格式）
- `enabled`：是否启用（0 或 1）

**条件类型**：
```json
{
  "snapshotRequired": true,           // 资产必须有快照
  "minSnapshotId": 1000,               // 快照 ID >= 1000
  "qualityStatus": "PASSED",           // 质量检查必须通过
  "schemaStatus": "COMPATIBLE",        // Schema 必须兼容
  "minWatermark": "2024-09-11T23:59:59Z"  // 水位线不低于某时刻
}
```

### AssetState（资产状态）

资产当前的状态快照，是**调度判定的唯一事实来源**。

**字段**：
- `assetKey`：资产唯一标识
- `latestSnapshotId`：最新的 snapshot ID（单调递增）
- `latestSchemaId`：最新的 schema ID
- `latestWatermark`：最新的流式处理水位线
- `latestTag`：最新的标签（如果有）
- `qualityStatus`：质量状态（PASSED、FAILED、UNKNOWN）
- `schemaStatus`：Schema 状态（COMPATIBLE、INCOMPATIBLE、UNKNOWN）
- `backfillStatus`：补数状态（NONE、RUNNING、COMPLETED）
- `version`：版本号（用于乐观锁）
- `updateTime`：最后更新时间

**关键特性**：
- `latestSnapshotId` 和 `latestWatermark` 永不回退（单调递增）
- `version` 用于乐观锁，防止并发覆盖
- 所有状态变化都来自已处理的 `LakehouseEvent`

**示例**：
```json
{
  "assetKey": "paimon://ods_db/ods_order_latest",
  "latestSnapshotId": 1001,
  "latestWatermark": "2024-09-11T23:59:59Z",
  "qualityStatus": "PASSED",
  "schemaStatus": "COMPATIBLE",
  "version": 42,
  "updateTime": "2024-09-11T10:15:30Z"
}
```

## D

### DependencyGroup（依赖分组）

将多个 `AssetDependency` 按逻辑分组，同组内的依赖进行 AND 操作。

**示例**：
```
dependencyGroup = "gmv_ready"
条件1：ods_order_latest snapshot exists
条件2：ods_payment_latest snapshot exists
条件3：ods_order_latest quality = PASSED AND ods_payment_latest quality = PASSED

全部满足 → 依赖组 READY
否则 → 依赖组 BLOCKED
```

多个 `dependencyGroup` 之间是 OR 关系：任一组 READY 即可触发。

### DependencyResolver（依赖解析器）

根据当前 `AssetState` 和 `AssetDependency` 声明，判断依赖是否满足。

**工作流程**：
```
1. 输入：AssetState 变化事件
2. 查询所有关联的 AssetDependency
3. 分组评估：
   - 对每个 dependencyGroup
   - 逐一检查所有条件
   - 所有条件 AND 运算
4. 输出：
   - READY（可触发）
   - BLOCKED_BY_SNAPSHOT（缺少快照）
   - BLOCKED_BY_QUALITY（质量失败）
   - BLOCKED_BY_SCHEMA（Schema 不兼容）
   - BLOCKED_BY_WATERMARK（水位未到）
```

## E

### Event（见 LakehouseEvent）

### Executor（执行器）

负责实际执行任务的适配器。

**支持的执行器**：
- **ShellExecutor**：执行 shell 脚本
- **SqlExecutor**：执行 SQL 脚本
- **HttpExecutor**：调用 HTTP 端点
- **SparkExecutor**（Phase 2）：提交 Spark 任务
- **FlinkExecutor**（Phase 2）：提交 Flink 任务

**执行流程**：
```
TaskInstance (READY)
  → Executor.submit(taskDefinition, params)
  → 返回 external_job_id
  → 存储 external_job_id
  → 轮询 Executor.queryStatus(external_job_id)
  → 状态变化：RUNNING → SUCCESS/FAILED
```

## L

### LakehouseEvent（湖仓事件）

原始事件，来自湖仓表的 snapshot、watermark、quality 等变化。**仅作为证据，不直接触发调度**。

**字段**：
- `eventId`：事件唯一去重键
- `sourceType`：事件来源（POLL、PUSH）
- `assetKey`：关联的资产
- `eventType`：事件类型
- `snapshotId`：快照版本（Paimon/Iceberg/Hudi）
- `schemaId`：Schema 版本
- `watermark`：流式处理水位（epoch millis）
- `qualityStatus`：质量检查结果
- `commitKind`：提交类型（APPEND、COMPACT、OVERWRITE）
- `commitTime`：湖仓端提交时间
- `payload`：完整的原始事件 JSON（用于排障）
- `receiveTime`：Lakehouse Flow 接收时间

**示例**：
```json
{
  "eventId": "paimon_ods_order_latest_snapshot_1001_xxx",
  "sourceType": "POLL",
  "assetKey": "paimon://ods_db/ods_order_latest",
  "eventType": "SNAPSHOT_COMMITTED",
  "snapshotId": 1001,
  "watermark": 1725963600000,
  "qualityStatus": "PASSED",
  "commitTime": 1725962700000,
  "receiveTime": 1725962710000,
  "payload": { /* ... Paimon 原始事件 */ }
}
```

### LakehouseFlow

本项目的名称。一个事件驱动的数据资产调度系统。

## O

### OptimisticLocking（乐观锁）

并发控制机制，使用版本号防止冲突。

**实现**：
```sql
UPDATE t_ds_asset_state
SET latest_snapshot_id = ?,
    version = version + 1
WHERE asset_key = ? AND version = ?
```

**特点**：
- 读时不加锁，吞吐量高
- 冲突时重试（而不是阻塞）
- 适合冲突率低的场景

## P

### Paimon

开源的统一存储数据湖框架（Apache Paimon）。支持 snapshot、changelog、schema evolution 等能力。

**相关概念**：
- `snapshot_id`：Paimon 的快照版本号
- `$snapshots`：Paimon 系统表，记录所有快照的元数据
- `changelog`：Paimon 的变更日志（支持 CDC）

## S

### Snapshot（快照）

数据湖表在某个时刻的完整版本。

**特征**：
- 版本号递增（快照 1000 → 1001 → 1002）
- 不可修改（只读）
- 支持时间旅行查询（回看历史版本）
- 元数据记录在系统表中

**相关系统表**：
- Paimon：`table_name$snapshots`
- Iceberg：`iceberg_metadata.snapshots`
- Hudi：`.hoodie/` 目录

### SnapshotId

快照的版本号。在 Lakehouse Flow 中是比较状态的重要字段。

**特性**：
- 在单个资产内单调递增（1000, 1001, 1002, ...）
- 由湖仓引擎分配（Flink/Spark commit 时生成）
- 用于去重、乱序保护、版本对齐

## T

### TaskInstance（任务实例）

工作流 DAG 中某个任务的一次执行。

**字段**：
- `taskCode`：任务定义编码
- `workflowInstanceId`：所属工作流实例
- `state`：任务状态（CREATED、WAITING_DEPENDENCY、READY、RUNNING、SUCCESS、FAILED）
- `waitingReason`：当前等待的原因（如果是 WAITING_DEPENDENCY）
- `tryNumber`：当前重试次数
- `maxRetries`：最大重试次数
- `executorType`：执行器类型（SHELL、SQL、HTTP）
- `externalJobId`：外部执行系统的 job ID（如 Spark application ID）

**状态转移**：
```
CREATED
  ↓
WAITING_DEPENDENCY (如果有资产依赖或上游依赖)
  ↓
READY
  ↓
DISPATCHING (派发给执行器)
  ↓
RUNNING
  ↓ (success or failure)
SUCCESS / FAILED / TIMEOUT
```

### TriggerHistory（触发历史）

记录每次触发的决策和结果，用于幂等性和审计。

**字段**：
- `triggerId`：触发记录主键
- `triggerKey`：**幂等键**（唯一）
- `assetKey`：触发源资产
- `workflowCode`：被触发的工作流
- `taskCode`：被触发的任务（可能为 NULL）
- `workflowInstanceId`：创建的工作流实例 ID
- `taskInstanceId`：创建的任务实例 ID
- `triggerStatus`：触发状态（SUCCESS、FAILED、TRIGGERING、SKIPPED）
- `triggerReason`：触发原因说明
- `retryCount`：重试次数
- `createTime`：触发时间

**TriggerKey 组成**：
```
workflow_code + "_" + dependency_group + "_" + [snapshot_ids and other identifiers]

例如：
workflow_12345_group_gmv_default_snapshot_1001_snapshot_1002_quality_passed

同一个 trigger_key 在 t_ds_asset_trigger_history 中的唯一约束保证只能存在一次
```

### TriggerKey（触发键）

幂等触发的唯一标识符。

**组成**：
```
workflow_code + dependency_group + snapshot_id_1 + snapshot_id_2 + ... + condition_hash
```

**用途**：
- 在 `t_ds_asset_trigger_history` 中作为 PRIMARY KEY 或 UNIQUE KEY
- 多 Master 并发时，只有一个能成功 INSERT
- 幂等性保证：同一 trigger_key 最多成功创建一次

## W

### Watermark（水位线）

流式处理中的事件时间进度指标。

**含义**：
- "截至这个时刻的所有事件都已处理"
- 例如：watermark=2024-09-11T23:59:59Z 意味着到这个时刻的数据都已进入湖表

**应用**：
- 判断"数据是否及时"
- 触发窗口计算
- 与业务日期对齐

### WorkflowDefinition（工作流定义）

DAG 拓扑的不可变定义。

**字段**：
- `code`：工作流代码（唯一）
- `version`：定义版本（同一 code 可有多个版本）
- `name`：工作流名称
- `description`：描述
- `status`：状态（DRAFT、PUBLISHED、DEPRECATED）
- `definitionJson`：DAG 拓扑 JSON
- `owner`：所有者
- `createTime`、`updateTime`：时间戳

**DAG 拓扑示例**：
```json
{
  "tasks": [
    {
      "code": 100,
      "name": "wait_ods_order",
      "type": "ASSET_SENSOR",
      "assetKey": "paimon://ods_db/ods_order_latest",
      "downstreamTaskCodes": [200]
    },
    {
      "code": 200,
      "name": "process_order",
      "type": "SHELL",
      "command": "spark-submit ...",
      "downstreamTaskCodes": [300]
    },
    {
      "code": 300,
      "name": "publish_result",
      "type": "SQL",
      "sql": "INSERT INTO ... SELECT ...",
      "downstreamTaskCodes": []
    }
  ]
}
```

### WorkflowInstance（工作流实例）

工作流定义的一次执行。

**字段**：
- `code`：对应的 WorkflowDefinition 的 code
- `version`：使用的 WorkflowDefinition 版本
- `bizDate`：业务日期（可用于分区或版本管理）
- `triggerType`：触发类型（CRON、ASSET_EVENT、MANUAL）
- `triggerEventId`：关联的触发事件 ID
- `triggerReason`：触发原因（自由文本）
- `state`：工作流状态（CREATED、WAITING、RUNNING、SUCCESS、FAILED）
- `startTime`、`endTime`：执行时间
- `taskInstances`：关联的任务实例列表

**示例**：
```json
{
  "code": 12345,
  "version": 5,
  "bizDate": "2024-09-11",
  "triggerType": "ASSET_EVENT",
  "triggerEventId": "paimon_ods_order_latest_snapshot_1001_xxx",
  "triggerReason": "Asset snapshot ready, quality PASSED",
  "state": "RUNNING",
  "startTime": "2024-09-11T10:15:30Z",
  "taskInstances": [
    { "taskCode": 100, "state": "SUCCESS" },
    { "taskCode": 200, "state": "RUNNING" },
    { "taskCode": 300, "state": "WAITING_DEPENDENCY" }
  ]
}
```

## 区别与关系

### Event vs State

| 特性 | Event | State |
|------|-------|-------|
| 来源 | 湖仓系统（外部） | 由 Event 推进（内部） |
| 特性 | 可能重复/乱序/延迟 | 最终一致、单调递增 |
| 用途 | 证据和审计日志 | 调度决策的唯一事实 |
| 留存 | 长期保留 | 实时更新 |

### TaskInstance vs WorkflowInstance

| 特性 | TaskInstance | WorkflowInstance |
|------|-------------|------------------|
| 粒度 | 单个任务 | 整个 DAG |
| 依赖 | 可能等待资产或上游任务 | 等待所有任务完成 |
| 状态 | 细粒度（WAITING_DEPENDENCY、READY、RUNNING 等） | 粗粒度（CREATED、RUNNING、SUCCESS 等） |
| 重试 | 单个任务可以重试 | 整个工作流可以重新运行 |

### Dependency vs Trigger

| 特性 | Dependency | Trigger |
|------|-----------|---------|
| 类型 | 声明式（工作流定义时） | 命令式（运行时生成） |
| 时间 | 定义阶段 | 执行阶段 |
| 存储 | `t_ds_asset_dependency` | `t_ds_asset_trigger_history` |
| 用途 | 定义依赖条件 | 记录触发事件 |

## 常见缩写

- **ODS**：Operational Data Store（操作数据存储，通常对应原始业务数据）
- **DWD**：Data Warehouse Detail（数据仓库明细层）
- **DWS**：Data Warehouse Service/Summary（数据仓库服务层/汇总层）
- **ADS**：Application Data Service（应用数据服务层）
- **CDC**：Change Data Capture（变更数据捕获）
- **DAG**：Directed Acyclic Graph（有向无环图）
- **PK**：Primary Key（主键）

