# 系统架构与设计文档

## 概述

Lakehouse Flow 是一个围绕**数据资产状态**构建的事件驱动调度系统。与传统调度系统基于时间和任务依赖不同，它通过以下核心驱动力实现调度：

```
数据资产 → 事件摄入 → 状态更新 → 依赖评估 → 触发决策 → 工作流执行 → 结果记录
```

## 系统分层

### 1. 领域模型层（Domain Model Layer）

```
LakehouseEvent          原始事件（来自 Paimon snapshot/Iceberg manifest）
  ├─ eventId            事件去重键
  ├─ assetKey           资产唯一标识
  ├─ snapshotId         快照版本
  ├─ watermark          流式处理水位
  └─ qualityStatus      质量检查结果

AssetState              资产当前状态（调度决策的唯一事实来源）
  ├─ assetKey           资产唯一标识
  ├─ latestSnapshotId   最新快照版本（单调递增）
  ├─ latestWatermark    最新水位
  ├─ qualityStatus      当前质量状态
  └─ version            乐观锁版本号

AssetDependency         资产依赖声明（工作流/任务等待条件）
  ├─ workflowCode       工作流定义编码
  ├─ taskCode           任务定义编码（为空表示工作流级依赖）
  ├─ assetKey           依赖的资产
  ├─ dependencyGroup    依赖分组（支持 AND 组合）
  └─ condition          条件表达式 JSON

TriggerHistory          触发历史（审计 + 幂等性）
  ├─ triggerKey         幂等键（唯一约束）
  ├─ assetKey           触发的资产
  ├─ workflowCode       被触发的工作流
  ├─ status             触发状态（SUCCESS/FAILED/SKIPPED）
  └─ reason             触发原因说明
```

### 2. 服务层（Service Layer）

**关键职责**：

- **EventIngestionService**：事件来源接入、去重、验证、持久化
- **AssetStateService**：资产状态更新（乐观锁）、查询、版本管理
- **DependencyResolverService**：条件评估、依赖组合、就绪状态判定
- **TriggerService**：幂等触发决策、触发历史记录
- **CompensationService**：漏采集检测、失败重试、卡顿任务扫描

### 3. 数据访问层（Repository/DAO Layer）

使用 Spring Data JPA + 自定义 SQL，关键特性：

- 数据库原生唯一约束支持幂等性
- 乐观锁通过 `version` 字段实现
- 高性能批量操作

### 4. API 层（REST Controller）

暴露 RESTful 接口给外部系统集成。

### 5. 执行层（Executor Layer）

适配器模式，支持多种执行器：

- **Shell Executor**：执行 shell 命令
- **SQL Executor**：执行 SQL 脚本
- **HTTP Executor**：调用 HTTP 端点
- **Spark Executor**（Phase 2）：提交 Spark 任务
- **Flink Executor**（Phase 2）：提交 Flink 任务

## 关键循环

Lakehouse Flow 的核心是 6 个独立的调度循环，可并行执行：

### 循环 1：事件摄入循环 (Event Ingestion Loop)

**频率**：高频轮询（默认 30 秒）

**流程**：
```
FOR EACH registered asset:
  → query Paimon $snapshots WHERE snapshot_id > last_scanned_snapshot_id
  → create candidate LakehouseEvent(s)
  → INSERT INTO t_ds_asset_event
    (ON DUPLICATE KEY UPDATE dedup_count++)
  → advance t_ds_asset_event_consumer_offset.last_snapshot_id
  → emit event_ingested metric
```

**关键特性**：
- 幂等性通过 `(source_type, asset_key, event_type, snapshot_id)` 唯一约束实现
- 重复事件自动去重，不会导致重复摄入
- 消费位点防止重复扫描

**数据库表**：
- `t_ds_asset`：已注册的资产清单
- `t_ds_asset_event`：原始事件存储
- `t_ds_asset_event_consumer_offset`：消费位点管理

### 循环 2：资产状态循环 (Asset State Loop)

**频率**：中频处理（默认 10 秒）

**流程**：
```
SELECT * FROM t_ds_asset_event WHERE processed = 0 LIMIT 100
FOR EACH event:
  → load current AssetState(asset_key, version)
  → if event.snapshotId <= state.latestSnapshotId:
      → mark event as IGNORED (乱序保护)
      → continue
  → UPDATE t_ds_asset_state
      SET latest_snapshot_id = event.snapshotId,
          latest_watermark = GREATEST(latest_watermark, event.watermark),
          quality_status = event.qualityStatus,
          version = version + 1
      WHERE asset_key = ? AND version = ?
  → if UPDATE affected_rows > 0:
      → mark event as PROCESSED
      → emit DependencyEvaluationCommand(asset_key)
    else:
      → mark event as FAILED (乐观锁冲突，需重试)
      → schedule retry in compensation
```

**关键保护机制**：
- **乱序保护**：比对 snapshot_id，仅接受单调递增的版本
- **乐观锁**：通过 version 字段防止并发覆盖
- **单调性**：latestSnapshotId 和 latestWatermark 永不回退

**数据库表**：
- `t_ds_asset_state`：资产当前状态（核心表）

### 循环 3：依赖评估循环 (Dependency Evaluation Loop)

**频率**：事件驱动 + 中频（默认 15 秒）

**流程**：
```
FOR EACH AssetStateChangeEvent(asset_key):
  → SELECT * FROM t_ds_asset_dependency 
      WHERE asset_key IN (direct dependencies)
  → GROUP BY dependency_group, workflow_code, task_code
  → FOR EACH group:
      → evaluate all conditions in the group
      → if ALL conditions PASS:
          → dependency_group state = READY
        else:
          → dependency_group state = BLOCKED_BY_<reason>
  → FOR EACH READY group:
      → emit TriggerEvaluationCommand(workflow_code, task_code, group)
```

**条件评估**：
```
evaluate_condition(condition_json, asset_state):
  if condition.snapshotRequired:
    if asset_state.latestSnapshotId == NULL:
      return BLOCKED_BY_SNAPSHOT
  
  if condition.minSnapshotId:
    if asset_state.latestSnapshotId < condition.minSnapshotId:
      return BLOCKED_BY_SNAPSHOT
  
  if condition.qualityStatus:
    if asset_state.qualityStatus != condition.qualityStatus:
      return BLOCKED_BY_QUALITY
  
  if condition.schemaStatus:
    if asset_state.schemaStatus != condition.schemaStatus:
      return BLOCKED_BY_SCHEMA
  
  return READY
```

**数据库表**：
- `t_ds_asset_dependency`：依赖声明
- `t_ds_asset_state`：用于查询当前状态

### 循环 4：幂等触发循环 (Idempotent Trigger Loop)

**频率**：事件驱动（毫秒级）

**流程**：
```
trigger_key = compute_trigger_key(
  workflow_code, task_code, dependency_group,
  [asset.snapshot_id for asset in dependency_group]
)

INSERT INTO t_ds_asset_trigger_history
  (trigger_key, asset_key, workflow_code, task_code, status='TRIGGERING', ...)
ON DUPLICATE KEY UPDATE:
  → if existed:
      → log DUPLICATE, return
    else:
      → execute trigger_strategy()
```

**Trigger Strategy A：任务级触发**
```
如果是任务级依赖（task_code != NULL）：
  → 不创建工作流实例
  → 只更新关联 TaskInstance 的依赖状态
  → TaskInstance 从 WAITING_DEPENDENCY 转为 READY
  → 等待 TaskProcessor 后续派发
```

**Trigger Strategy C：工作流级触发**
```
如果是工作流级依赖（task_code == NULL）：
  → 调用 CommandService#createCommand()
  → commandType = ASSET_EVENT_TRIGGER 或 START_PROCESS
  → commandParam 包含 assetKey, snapshotId, trigger_key 等审计信息
  → 由现有 Master 侧 Command 消费链路完成 WorkflowInstance 创建
  → 无需改动 Master 内核
```

**关键特性**：
- `trigger_key` 唯一性由数据库唯一约束保证
- 同一快照组合只能触发一次
- 无需分布式锁，完全依赖数据库原生约束

**数据库表**：
- `t_ds_asset_trigger_history`：触发历史记录

### 循环 5：任务派发循环 (Task Dispatch Loop)

**频率**：高频扫描（默认 5 秒）

**流程**：
```
SELECT * FROM task_instance 
  WHERE state = 'READY' AND locked_until < NOW()
FOR EACH task_instance:
  → acquire task-level lease
  → SELECT executor_type FROM task_definition
  → call ExecutorService.submit(executor_type, task_params)
  → store external_job_id
  → update task_instance.state = DISPATCHING
  → update task_instance.submit_time = NOW()
  → release lease
  → emit task_dispatched metric
```

**多执行器支持**：
```
ExecutorService.submit():
  match executor_type:
    SHELL → ShellExecutor
    SQL → SqlExecutor
    HTTP → HttpExecutor
    SPARK → SparkExecutor
    FLINK → FlinkExecutor
```

**数据库表**：
- `task_instance`：任务实例表
- `executor_job`：存储 external_job_id

### 循环 6：状态协调循环 (Status Reconciliation Loop)

**频率**：中频轮询（默认 10 秒）

**流程**：
```
SELECT * FROM executor_job 
  WHERE status IN ('RUNNING', 'PENDING')
FOR EACH job:
  → call Executor.queryStatus(external_job_id)
  → switch job.status:
      RUNNING:
        → update job.status, continue
      SUCCESS:
        → update task_instance.state = SUCCESS
        → emit task_succeeded metric
        → trigger downstream task DAG evaluation
      FAILED:
        → update task_instance.state = FAILED
        → check task.retry_policy
        → if retry_allowed:
            → update task.retry_count++
            → mark for retry
          else:
            → mark task as FAILED, cascade to workflow
      TIMEOUT (execute_time > timeout_seconds):
        → update task_instance.state = TIMEOUT
        → check if timeout_behavior = RETRY
        → if RETRY:
            → schedule retry
          else:
            → cascade TIMEOUT to workflow
      UNKNOWN (> 1 hour no status):
        → emit LOST_CALLBACK alert
        → mark for manual review
```

**数据库表**：
- `executor_job`：外部执行状态跟踪
- `task_instance`：任务实例状态机

### 补充循环 7：补偿循环 (Compensation Loop)

**频率**：低频定期（默认 300 秒/ 5 分钟）

**职责**：
```
1. 检查漏采集事件：
   FOR EACH asset:
     actual_latest = query_paimon_latest_snapshot_id()
     state_latest = t_ds_asset_state.latest_snapshot_id
     if actual_latest > state_latest:
       → backfill missing snapshots
       → INSERT INTO t_ds_asset_event

2. 检查失败触发：
   SELECT * FROM t_ds_asset_trigger_history 
     WHERE status = 'FAILED' AND retry_count < max_retries
       AND created_time > NOW() - 1 HOUR
   FOR EACH:
     → check if corresponding Command/WorkflowInstance exists
     → if not, retry createCommand()
     → increment retry_count

3. 检查卡顿任务：
   SELECT * FROM task_instance 
     WHERE state = 'WAITING_DEPENDENCY' 
       AND waiting_time > timeout_threshold
   FOR EACH:
     → emit STUCK_TASK alert
     → optionally cancel or timeout

4. 检查丢失的回调：
   SELECT * FROM executor_job 
     WHERE status IN ('RUNNING', 'PENDING') 
       AND last_status_check_time > 2 HOURS
   FOR EACH:
     → force reconciliation
     → emit LOST_CALLBACK alert
```

## 数据库表设计

### 核心表

#### t_ds_asset（资产注册表）

```sql
CREATE TABLE t_ds_asset (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_key VARCHAR(512) NOT NULL UNIQUE,
    storage_format VARCHAR(32) NOT NULL,    -- PAIMON, ICEBERG, HUDI
    catalog_name VARCHAR(128) NOT NULL,
    database_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    partition_expr VARCHAR(256),
    description VARCHAR(512),
    enabled TINYINT DEFAULT 1,
    owner_user_id BIGINT,
    create_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    
    UNIQUE KEY uk_asset_key (asset_key),
    KEY idx_enabled (enabled),
    KEY idx_storage_format (storage_format)
);
```

#### t_ds_asset_event（原始事件表）

```sql
CREATE TABLE t_ds_asset_event (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(128) NOT NULL,
    source_type VARCHAR(16) NOT NULL,      -- POLL, PUSH
    asset_key VARCHAR(512) NOT NULL,
    event_type VARCHAR(32) NOT NULL,        -- SNAPSHOT_COMMITTED, WATERMARK_ADVANCED
    snapshot_id BIGINT,
    schema_id BIGINT,
    watermark BIGINT,
    commit_kind VARCHAR(32),
    commit_time BIGINT,
    delta_record_count BIGINT,
    schema_changed TINYINT,
    quality_status VARCHAR(16),
    payload LONGTEXT,
    processed TINYINT DEFAULT 0,
    receive_time BIGINT NOT NULL,
    create_time DATETIME NOT NULL,
    
    UNIQUE KEY uk_dedup (source_type, asset_key, event_type, snapshot_id),
    KEY idx_asset_snapshot (asset_key, snapshot_id),
    KEY idx_processed (processed),
    KEY idx_event_type_time (event_type, receive_time)
);
```

#### t_ds_asset_state（资产状态表）

```sql
CREATE TABLE t_ds_asset_state (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_key VARCHAR(512) NOT NULL UNIQUE,
    latest_snapshot_id BIGINT,
    latest_schema_id BIGINT,
    latest_watermark BIGINT,
    latest_tag VARCHAR(128),
    quality_status VARCHAR(16) DEFAULT 'UNKNOWN',   -- PASSED, FAILED, UNKNOWN
    schema_status VARCHAR(32) DEFAULT 'UNKNOWN',    -- COMPATIBLE, INCOMPATIBLE
    backfill_status VARCHAR(16) DEFAULT 'NONE',
    version BIGINT NOT NULL DEFAULT 0,              -- 乐观锁
    update_time DATETIME NOT NULL,
    
    UNIQUE KEY uk_asset_key (asset_key),
    KEY idx_quality_status (quality_status)
);
```

#### t_ds_asset_dependency（依赖声明表）

```sql
CREATE TABLE t_ds_asset_dependency (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    workflow_code BIGINT NOT NULL,
    task_code BIGINT,                       -- NULL 表示工作流级依赖
    asset_key VARCHAR(512) NOT NULL,
    dependency_group VARCHAR(64) NOT NULL DEFAULT 'default',
    condition_json TEXT NOT NULL,
    enabled TINYINT DEFAULT 1,
    create_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    
    KEY idx_asset_key (asset_key),
    KEY idx_workflow_task (workflow_code, task_code),
    KEY idx_workflow_group (workflow_code, dependency_group)
);
```

#### t_ds_asset_trigger_history（触发历史表）

```sql
CREATE TABLE t_ds_asset_trigger_history (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    trigger_key VARCHAR(256) NOT NULL UNIQUE,
    asset_key VARCHAR(512),
    workflow_code BIGINT NOT NULL,
    task_code BIGINT,
    workflow_instance_id BIGINT,
    task_instance_id BIGINT,
    trigger_status VARCHAR(16) NOT NULL,    -- SUCCESS, FAILED, TRIGGERING, SKIPPED
    reason VARCHAR(512),
    retry_count INT DEFAULT 0,
    create_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    
    UNIQUE KEY uk_trigger_key (trigger_key),
    KEY idx_status_time (trigger_status, create_time),
    KEY idx_workflow_code (workflow_code)
);
```

#### t_ds_asset_event_consumer_offset（消费位点表）

```sql
CREATE TABLE t_ds_asset_event_consumer_offset (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    source_type VARCHAR(16) NOT NULL,
    asset_key VARCHAR(512) NOT NULL,
    consumer_group VARCHAR(64) DEFAULT 'default',
    last_snapshot_id BIGINT,
    last_scan_time DATETIME,
    
    UNIQUE KEY uk_offset (source_type, asset_key, consumer_group)
);
```

## 观测性设计

### 关键指标（Prometheus）

```
# 事件摄入
lakehouse_asset_event_received_total{source_type="paimon", asset_key="..."}
lakehouse_asset_event_deduplicated_total{asset_key="..."}
lakehouse_asset_event_invalid_total{asset_key="...", reason="..."}

# 资产状态
lakehouse_asset_state_updated_total{asset_key="..."}
lakehouse_asset_state_lag_seconds{asset_key="..."}    # 资产年龄
lakehouse_asset_watermark_lag_seconds{asset_key="..."} # 水位延迟

# 依赖与触发
lakehouse_asset_dependency_ready_total{workflow_code="...", group="..."}
lakehouse_asset_dependency_blocked_by{blocked_by="snapshot|quality|schema|watermark"}
lakehouse_asset_trigger_success_total{workflow_code="..."}
lakehouse_asset_trigger_failed_total{workflow_code="...", reason="..."}

# 补偿
lakehouse_asset_compensation_scan_total{type="missed_events|failed_triggers"}
lakehouse_asset_compensation_missed_events{asset_key="..."}
lakehouse_asset_trigger_history_retry_total{status="..."}
```

### 关键日志字段

所有日志应包含链路追踪上下文：

```json
{
  "timestamp": "2024-09-11T10:15:30Z",
  "level": "INFO",
  "traceId": "abc123xyz",
  "assetKey": "paimon://ods_db/ods_order_latest",
  "snapshotId": 1001,
  "eventId": "paimon_evt_1001_xxx",
  "workflowCode": 12345,
  "taskCode": 100,
  "workflowInstanceId": 67890,
  "taskInstanceId": 11111,
  "triggerKey": "workflow_12345_group_default_snapshot_1001",
  "status": "READY|BLOCKED_BY_QUALITY|TRIGGERED|FAILED",
  "message": "Asset snapshot processed, dependency READY, triggering workflow..."
}
```

### 告警规则

| 告警 | 阈值 | 优先级 | 处理建议 |
|------|------|--------|---------|
| 资产状态延迟 | > 1 小时 | P2 | 检查 Scanner 健康，检查 Paimon 是否有新快照 |
| 触发失败 | > 3 次/5 分钟 | P1 | 检查 Command 创建是否异常 |
| 补偿扫描失败 | 连续 > 3 次 | P1 | 检查系统稳定性，可能需要手工介入 |
| 质量依赖卡顿 | 阻塞 > 30 分钟 | P2 | 质量检查失败，联系数据生产方 |
| 丢失的回调 | > 10 个/小时 | P1 | 执行器可能有问题，检查日志 |

## 设计模式与最佳实践

### 幂等性模式

```
幂等键 = workflow_code + dependency_group + [snapshot_id_1, snapshot_id_2, ...]

例如：
workflow_12345_group_gmv_default_snapshot_1001_snapshot_1002

唯一性由 t_ds_asset_trigger_history.trigger_key 的唯一约束保证
多 Master 并发时，只有一个能成功 INSERT
其他的捕获唯一约束冲突异常，继续执行
```

### 乐观锁模式

```java
do {
  state = SELECT * FROM t_ds_asset_state WHERE asset_key = ? FOR UPDATE
  newSnapshotId = event.snapshotId
  
  if (newSnapshotId <= state.latestSnapshotId) {
    throw OutOfOrderException()  // 乱序保护
  }
  
  updated_rows = UPDATE t_ds_asset_state
    SET latest_snapshot_id = newSnapshotId,
        version = version + 1
    WHERE asset_key = ? AND version = state.version
  
  if (updated_rows == 0) {
    // 并发冲突，重试
    continue
  } else {
    break  // 成功更新
  }
} while (retries < max_retries)
```

### 命令队列模式

```
事件驱动命令队列（可选 Kafka）：

LakehouseEvent 
  → EventReceivedCommand(eventId, assetKey, snapshotId)

AssetState changed 
  → DependencyEvaluationCommand(assetKey)

Dependency satisfied 
  → TriggerEvaluationCommand(workflowCode, taskCode, dependencyGroup)

Trigger succeeded 
  → WorkflowInstanceCreatedCommand(workflowCode, commandId)
```

## 扩展点

### 事件源插件

```java
public interface AssetEventSource {
    List<AssetEvent> pollNewEvents(Asset asset, Long sinceSnapshotId);
}

实现类：
- PaimonEventSource
- IcebergEventSource
- HudiEventSource
```

### 执行器插件

```java
public interface Executor {
    String submit(TaskDefinition task, Map<String, String> params);
    String queryStatus(String jobId);
}

实现类：
- ShellExecutor
- SqlExecutor
- HttpExecutor
- SparkExecutor
- FlinkExecutor
```

## 并发与一致性

### 多 Master 场景

```
Master A, Master B, Master C 同时运行

冲突点1：Command 创建
  → 由应用侧保证幂等，不会重复创建

冲突点2：触发历史唯一性
  → t_ds_asset_trigger_history 的 trigger_key 唯一约束
  → 最快的 Master 成功 INSERT
  → 其他的捕获 DuplicateKeyException，继续

冲突点3：资产状态乐观锁
  → 多个 Master 并发更新 AssetState
  → 乐观锁保证只有一个成功
  → 其他的捕获版本冲突，重试

无需分布式锁，完全依赖数据库原生机制
```

---

**完整的实现示例和代码见 [DEVELOPMENT.md](./DEVELOPMENT.md)**

