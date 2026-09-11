# Lakehouse Flow - Phase 2: Event Processing & Scheduling Loops - 设计文档

**日期**: 2026-09-11  
**版本**: 0.1.0-SNAPSHOT  
**阶段**: Phase 2 (Event Processing & Scheduling Loops)

## 一、架构定位澄清

### 1.1 本系统职责（调度决策层）

**Input**: Lakehouse metadata events (Paimon/Iceberg/Hudi snapshots)  
**Output**: Ready-to-dispatch task instances

```
事件入库 → 资产状态更新 → 依赖评估 → 实例创建 → [送出去]
```

本系统**只负责决策**：
- ✅ 某个资产是否就绪？
- ✅ 依赖是否满足？
- ✅ 应该创建哪个实例？
- ✅ 实例为什么在等待？

### 1.2 下游系统职责（执行编排层）

**Input**: Ready-to-dispatch task instances  
**Output**: Execution results, job status

下游系统**负责执行**：
- ✅ 资源组检查 (resource quota validation)
- ✅ 依赖编排 (DAG orchestration, if needed)
- ✅ 监控和回调 (callback mechanisms)
- ✅ 重试策略 (retry policies, if needed)
- ✅ 真实任务执行 (Shell, SQL, HTTP, Spark, etc.)

### 1.3 集成点

```
┌─────────────────────────────────────────┐
│  Lakehouse Flow - 调度决策系统           │
├─────────────────────────────────────────┤
│ Event → AssetState → Dependency → Task  │
│                    Instance (READY)     │
└──────────────┬────────────────────────┘
               │ [API 调用]
               ▼
┌─────────────────────────────────────────┐
│  下游执行系统 (e.g., Airflow/K8s/etc.)  │
├─────────────────────────────────────────┤
│ Resource Check → DAG Orchestration      │
│ → Execute → Monitor → Report            │
└─────────────────────────────────────────┘
```

**集成协议**:
- Lakehouse Flow 创建 TaskInstance 并设置 state=READY
- 下游系统通过 `/api/task-instances/ready` 查询 READY 实例
- 下游系统分配资源和编排依赖（不涉及本系统）
- 下游系统执行任务并通过 callback 报告状态
- Lakehouse Flow 接收 callback 并更新 TaskInstance.state

## 二、Phase 2 核心循环

### 2.1 Event Ingestion Loop

**职责**: 持续监听和摄入 lakehouse 事件

**流程**:
```
1. 扫描 Paimon $snapshots 表（或其他源）
2. 读取最新快照（按 snapshot_id DESC）
3. 去重（event_id 唯一性）
4. 存入 LakehouseEvent
5. 触发 AssetState 更新（不等待结果）
6. 推进 consumer offset
```

**关键代码位置**: `lakehouse-flow-integration`

**实现**:
```java
@Component
public class PaimonSnapshotScanner {
    void scanAndIngest() {
        // Query Paimon $snapshots
        List<PaimonSnapshot> snapshots = queryPaimon(...);
        
        for (PaimonSnapshot s : snapshots) {
            LakehouseEvent event = mapToLakehouseEvent(s);
            try {
                eventService.ingestEvent(event);  // Idempotent
            } catch (DataIntegrityViolationException e) {
                // event_id already exists, skip
            }
        }
        
        // Update consumer offset
        updateOffset(maxSnapshotId);
    }
}
```

**触发方式**: 
- 后台 @Scheduled 定时任务（e.g., 每 10 秒）
- 或被动等待下游 pull 时才扫描

### 2.2 Asset State Update Loop

**职责**: 从事件更新资产状态

**流程**:
```
1. 查询未处理的 LakehouseEvent（processedAt is null）
2. 按 assetKey + eventTime 分组
3. 对每个 asset，调用 assetStateService.updateAssetStateFromEvent()
4. 标记事件已处理（processedAt）
5. 触发 Dependency Evaluation Loop
```

**关键代码位置**: `lakehouse-flow-service` 中扩展

**实现**:
```java
@Component
public class AssetStateUpdateLoop {
    void processUnprocessedEvents() {
        List<LakehouseEvent> unprocessed = 
            eventRepository.findByProcessedAtIsNull(limit(1000));
        
        for (LakehouseEvent event : unprocessed) {
            AssetState state = assetStateService.updateAssetStateFromEvent(event);
            
            // 标记已处理
            event.setProcessedAt(LocalDateTime.now());
            eventRepository.save(event);
            
            // 触发依赖评估（异步或同步取决于设计）
            if (state.versionChanged()) {
                dependencyEvaluationService.evaluateForAsset(state.getAssetKey());
            }
        }
    }
}
```

### 2.3 Dependency Evaluation Loop

**职责**: 评估依赖是否满足，创建/更新实例

**流程**:
```
1. 查询受影响的 AssetDependency（assetKey 匹配）
2. 对每个 dependency，评估条件
3. 条件满足 → 创建或更新 WorkflowInstance/TaskInstance
4. 记录 TriggerHistory（审计日志）
5. 更新实例状态为 READY（如所有依赖已满足）
```

**关键代码位置**: 新增 `DependencyEvaluator` + `ConditionEvaluator`

**实现**:
```java
@Component
public class DependencyEvaluationService {
    void evaluateForAsset(String assetKey) {
        List<AssetDependency> deps = 
            dependencyRepository.findByAssetKey(assetKey);
        
        for (AssetDependency dep : deps) {
            EvaluationResult result = conditionEvaluator.evaluate(dep, assetKey);
            
            if (result.isSatisfied()) {
                // 创建或更新实例
                WorkflowInstance wf = workflowInstanceService.createOrGetInstance(
                    dep.getWorkflowCode(),
                    dep.getWorkflowVersion(),
                    dep.getBizDate(),
                    "SNAPSHOT_DRIVEN",
                    assetKey  // trigger asset
                );
                
                // 记录触发事件
                triggerHistoryService.recordTrigger(
                    dep.getId(),
                    result
                );
            } else {
                // 条件不满足，记录等待原因
                log.debug("Dependency not satisfied: {}, reason: {}", 
                    dep.getId(), result.getWaitingReason());
            }
        }
    }
}
```

### 2.4 Task Ready Check Loop

**职责**: 从 WorkflowInstance 创建 TaskInstance，标记 READY

**流程**:
```
1. 查询 state=WAITING 的 WorkflowInstance
2. 对每个实例，评估其内部依赖（上游任务）
3. 如果依赖满足，创建或解锁 TaskInstance，设置 state=READY
4. 本系统不负责执行，只负责标记 READY
```

**关键代码位置**: 新增 `TaskReadyCheckService`

**实现**:
```java
@Component
public class TaskReadyCheckService {
    void markReadyTasksForWorkflow(Long workflowInstanceId) {
        WorkflowInstance wf = workflowInstanceService.getById(workflowInstanceId);
        List<TaskDefinition> tasks = taskDefinitionRepository
            .findByWorkflowCode(wf.getWorkflowCode());
        
        for (TaskDefinition task : tasks) {
            if (areUpstreamTasksSuccessful(task, wf)) {
                // 创建或解锁 TaskInstance
                TaskInstance ti = taskInstanceService.createInstance(
                    workflowInstanceId,
                    task.getTaskCode(),
                    task.getTaskVersion(),
                    wf.getBizDate()
                );
                
                // 标记为 READY（可供下游系统分配）
                taskInstanceService.transitionState(ti.getId(), "READY", null);
                
                log.info("Task instance {} marked READY", ti.getInstanceKey());
            }
        }
    }
    
    private boolean areUpstreamTasksSuccessful(TaskDefinition task, WorkflowInstance wf) {
        // 简单逻辑：检查 upstreamTaskCodes 中的任务是否都 SUCCESS
        return true;  // 简化示意
    }
}
```

### 2.5 Status Reconciliation Loop (可选 Phase 2.5)

**职责**: 从下游系统拉取执行状态，更新实例

**暂不实现** - 等下游系统通过 callback 主动报告状态

## 三、新增组件设计

### 3.1 ConditionEvaluator

```java
public class ConditionEvaluator {
    /**
     * 评估依赖条件是否满足
     * 
     * @param dependency 依赖定义（含条件 JSON）
     * @param assetKey 触发的资产 key
     * @return 评估结果（是否满足 + 等待原因）
     */
    public EvaluationResult evaluate(AssetDependency dependency, String assetKey) {
        // 解析 dependency.conditionsJson
        DependencyCondition[] conditions = parseConditions(dependency.getConditionsJson());
        
        for (DependencyCondition cond : conditions) {
            EvaluationResult condResult = evaluateCondition(cond, assetKey);
            if (!condResult.isSatisfied()) {
                return condResult;  // 短路：任何条件不满足都不满足
            }
        }
        
        return EvaluationResult.satisfied();
    }
    
    private EvaluationResult evaluateCondition(DependencyCondition cond, String assetKey) {
        return switch (cond.getType()) {
            case SNAPSHOT_EXISTS -> checkSnapshotExists(cond.getAssetKey());
            case SNAPSHOT_ID_GTE -> checkSnapshotIdGte(cond.getAssetKey(), cond.getValue());
            case WATERMARK_GTE -> checkWatermarkGte(cond.getAssetKey(), cond.getValue());
            case QUALITY_PASSED -> checkQualityStatus(cond.getAssetKey());
            case SCHEMA_COMPATIBLE -> checkSchemaStatus(cond.getAssetKey());
            default -> EvaluationResult.notSupported(cond.getType());
        };
    }
}
```

### 3.2 TriggerHistoryService

```java
@Service
public class TriggerHistoryService {
    /**
     * 记录为什么某个工作流被触发
     * 审计日志，用于调试和追踪
     */
    public void recordTrigger(
            Long dependencyId,
            String triggerKey,
            EvaluationResult result,
            Long workflowInstanceId) {
        
        TriggerHistory history = TriggerHistory.builder()
            .triggerKey(triggerKey)
            .dependencyId(dependencyId)
            .assetKey(result.getAssetKey())
            .snapshotId(result.getSnapshotId())
            .watermark(result.getWatermark())
            .eventId(result.getEventId())
            .workflowInstanceId(workflowInstanceId)
            .decision("TRIGGERED")
            .decisionReason(result.getDescription())
            .createdAt(LocalDateTime.now())
            .build();
        
        triggerHistoryRepository.save(history);
    }
}
```

### 3.3 Paimon Source Adapter

```java
@Component
public class PaimonSnapshotSource {
    /**
     * 扫描 Paimon $snapshots 表
     * 返回自上次处理以来的新快照
     */
    public List<PaimonSnapshot> scanSnapshots(Long sinceSnapshotId) {
        String sql = """
            SELECT snapshot_id, schema_id, commit_user, commit_kind, commit_time,
                   watermark, delta_record_count, changelog_record_count
            FROM %s.`%s$snapshots`
            WHERE snapshot_id > ?
            ORDER BY snapshot_id ASC
            LIMIT 1000
            """.formatted(catalogName, fullTableName);
        
        return jdbcTemplate.query(sql, new Object[]{sinceSnapshotId}, 
            (rs, rowNum) -> mapToPaimonSnapshot(rs));
    }
    
    public LakehouseEvent mapToLakehouseEvent(PaimonSnapshot snapshot) {
        return LakehouseEvent.builder()
            .eventId(generateDeterministicEventId(snapshot))
            .eventType("SNAPSHOT_COMMITTED")
            .sourceType("PAIMON")
            .catalogName(catalogName)
            .databaseName(databaseName)
            .tableName(tableName)
            .snapshotId(snapshot.snapshotId)
            .watermark(snapshot.watermark)
            .payloadJson(objectMapper.writeValueAsString(snapshot))
            .build();
    }
    
    private String generateDeterministicEventId(PaimonSnapshot snapshot) {
        // event_id = source_type:catalog:db:table:snapshot_id
        return "PAIMON:%s:%s:%s:%s".formatted(
            catalogName, databaseName, tableName, snapshot.snapshotId);
    }
}
```

## 四、REST API 设计

### 4.1 查询 API（供下游系统调用）

```
GET /api/task-instances/ready
    - 查询所有 state=READY 的 TaskInstance
    - 返回可供下游系统分配的任务列表
    
Response:
{
    "items": [
        {
            "id": 123,
            "instanceKey": "workflow1:v1:2026-09-11:SNAPSHOT_DRIVEN:paimon.prod.ods.orders.dt=2026-09-10",
            "workflowCode": "workflow1",
            "taskCode": "task1",
            "bizDate": "2026-09-11",
            "state": "READY",
            "triggerReason": "Snapshot paimon.prod.ods.orders.dt=2026-09-10 ready"
        }
    ]
}
```

### 4.2 Callback API（下游系统报告状态）

```
POST /api/task-instances/{id}/status
    - 下游系统报告任务执行结果
    
Request:
{
    "state": "RUNNING",  // or SUCCESS, FAILED, TIMEOUT
    "externalJobId": "spark-job-123",
    "message": "Job started"
}

Response: 200 OK or error
```

### 4.3 Debug API（便于排障）

```
GET /api/asset-state/{assetKey}
    - 查询资产当前状态
    
GET /api/workflow-instances/{workflowCode}/waiting
    - 查询某工作流正在等待的实例
    
GET /api/workflow-instances/{id}/waiting-reason
    - 查询某实例为什么在等待
    - 返回依赖条件的详细评估结果
    
GET /api/triggers/{workflowInstanceId}
    - 查询为什么创建了某实例
    - 返回 TriggerHistory
```

## 五、Phase 2 实现清单

### 5.1 新增代码文件

| 文件 | 位置 | 描述 |
|------|------|------|
| `PaimonSnapshotScanner.java` | integration | 定时扫描 Paimon 快照 |
| `PaimonSnapshotSource.java` | integration | Paimon SQL 适配器 |
| `EventIngestionService.java` | service | 事件摄入的高层编排 |
| `ConditionEvaluator.java` | service | 依赖条件评估器 |
| `DependencyEvaluationService.java` | service | 依赖评估循环 |
| `TriggerHistoryService.java` | service | 触发审计日志 |
| `TaskReadyCheckService.java` | service | 任务 READY 标记 |
| `TaskInstanceController.java` | api | REST API 端点 |
| `WorkflowInstanceController.java` | api | REST API 端点 |
| `DebugController.java` | api | 调试 API 端点 |
| `TriggerHistoryRepository.java` | dao | 触发历史持久化 |
| 数据库迁移脚本 | db | 添加 trigger_history 表 |

### 5.2 新增 Entity

| Entity | 描述 |
|--------|------|
| `TriggerHistory` | 触发审计日志（谁触发了这个实例，为什么） |
| `EventConsumerOffset` | 事件消费进度（用于幂等重启） |

### 5.3 扩展已有 Service

| Service | 扩展内容 |
|---------|---------|
| `AssetStateService` | 添加 event 处理进度标记 |
| `WorkflowInstanceService` | 添加 trigger asset tracking |
| `TaskInstanceService` | 无需扩展（已足够） |

### 5.4 新增配置

| 配置项 | 说明 |
|--------|------|
| `lakehouse.paimon.catalogs[0].name` | Paimon catalog 名称 |
| `lakehouse.paimon.catalogs[0].jdbc-url` | Paimon JDBC 连接 |
| `lakehouse.scheduler.event-scan-interval-ms` | 事件扫描间隔 |
| `lakehouse.scheduler.dependency-eval-batch-size` | 依赖评估批大小 |

## 六、Phase 2 实现步骤

**Step 1**: 设计 EvaluationResult 和 ConditionEvaluator  
**Step 2**: 实现 PaimonSnapshotSource（Mock 或真实 JDBC）  
**Step 3**: 实现 EventIngestionService + PaimonSnapshotScanner  
**Step 4**: 实现 AssetStateUpdateLoop  
**Step 5**: 实现 DependencyEvaluationService + ConditionEvaluator  
**Step 6**: 实现 TaskReadyCheckService  
**Step 7**: 添加 TriggerHistory entity + repository + service  
**Step 8**: 实现 REST API（查询、callback、debug）  
**Step 9**: 单元测试和集成测试  
**Step 10**: 文档和例子  

## 七、Phase 2 完成标准

- ✅ 所有循环能够独立运行（互不阻塞）
- ✅ 事件可以从 Paimon 摄入到数据库
- ✅ 资产状态可以自动更新
- ✅ 依赖条件可以被评估
- ✅ 满足条件的 TaskInstance 可以标记为 READY
- ✅ 下游系统可以通过 API 查询和消费 READY 任务
- ✅ 可以通过 API 查询为什么某实例在等待
- ✅ 所有关键路径有单元测试覆盖
- ✅ 无内存泄漏（循环中正确关闭资源）

## 八、关键设计决策

### 8.1 为什么分开循环？

不同的循环有不同的延迟容忍度：
- Event Ingestion: 可以 10s 延迟
- Asset State Update: 可以 5s 延迟  
- Dependency Evaluation: 可以 2s 延迟
- Task Ready Check: 可以 1s 延迟

分开后可以独立优化和扩展。

### 8.2 为什么不用消息队列？

第一阶段保持简单：
- ✅ 数据库 SELECT + 轮询足够了
- ✅ 可以通过指数退避避免频繁查询
- ✅ 后续可以替换为 Kafka（只改 EventIngestionService）

### 8.3 为什么 TaskInstance 由 Lakehouse Flow 创建？

虽然下游系统负责执行，但：
- TaskInstance 是调度决策的**证据**
- 用于**审计**和**追踪**
- 用于**重试**和**重跑**的决策
- 下游系统只需要查询和更新状态

## 九、集成示例

### 9.1 下游系统集成

```python
# 伪代码：下游 Airflow 集成

import requests

class LakehouseFlowSync:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url
    
    def pull_ready_tasks(self):
        """从 Lakehouse Flow 拉取 READY 的任务"""
        resp = requests.get(f"{self.base_url}/api/task-instances/ready")
        return resp.json()["items"]
    
    def execute_task(self, task_instance_id, executor_config):
        """执行任务（e.g., 提交 Spark Job）"""
        job_id = submit_spark_job(executor_config)
        
        # 通知 Lakehouse Flow：我开始执行了
        requests.post(
            f"{self.base_url}/api/task-instances/{task_instance_id}/status",
            json={"state": "RUNNING", "externalJobId": job_id}
        )
        
        return job_id
    
    def report_result(self, task_instance_id, job_id, status):
        """任务执行完毕，报告结果"""
        requests.post(
            f"{self.base_url}/api/task-instances/{task_instance_id}/status",
            json={"state": status, "externalJobId": job_id}
        )
```

## 十、风险和注意事项

### 10.1 并发性

- 多个 scheduler 节点同时运行循环
- 使用数据库 row-level lock 或 SELECT FOR UPDATE
- 或使用 distributed lease（暂不实现）

### 10.2 性能

- Event ingestion 不应该在事务中等待 dependency eval
- 分开循环可以独立扩展
- 加索引: (assetKey, enabled) on asset_dependency

### 10.3 可观测性

- 每个循环要有 metrics（处理数、延迟、错误）
- 关键路径要有 log
- Callback 超时要有 alert

---

**下一步**: Phase 2 实现开始

**预计工作量**: 2-3 天（含测试）

**交付物**:
- Event ingestion → Asset state → Dependency eval 完整链路
- REST API for queries and callbacks
- 10+ 单元/集成测试
- 完整文档

