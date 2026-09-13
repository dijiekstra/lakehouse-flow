# Lakehouse Flow - 架构定位澄清

> 历史文档：保留用于追溯早期方案。文中的下游 READY 查询和 executor callback 均已废弃；当前由 Lakehouse Flow 内部发布 outbox，也不使用 `RUNNING/SUCCESS/FAILED` 判断结果。请以 `ARCHITECTURE.md` 和 `PHASE2_PROGRESS.md` 为准。

**日期**: 2026-09-11  
**版本**: 0.1.0-SNAPSHOT

## 核心定位

### 本系统 = 调度决策层（Scheduling Decision Layer）

```
输入: Lakehouse metadata events (Paimon/Iceberg/Hudi snapshots)
处理: Asset state → Dependency evaluation → Instance readiness
输出: Ready-to-dispatch task instances (via API)
```

**系统只负责回答这些问题**:
- ❓ 某个数据资产是否就绪？
- ❓ 某个工作流的依赖是否满足？
- ❓ 应该创建哪个 TaskInstance？
- ❓ 某个 TaskInstance 为什么在等待？

**绝不涉及**:
- ❌ 真实任务执行
- ❌ 资源组检查和分配
- ❌ 预检验证（resource validation）
- ❌ DAG 编排优化

### 下游系统 = 执行编排层（Execution Orchestration Layer）

```
输入: Ready-to-dispatch task instances
处理: Resource allocation → Dependency DAG → Monitor → Report
输出: Execution results, callback status
```

**下游系统负责**:
- ✅ 资源组检查 (resource quota, affinity, etc.)
- ✅ DAG 编排 (if multi-step, if needed)
- ✅ 真实执行 (Shell, SQL, HTTP, Spark, Flink, etc.)
- ✅ 监控和告警
- ✅ 重试策略和补偿

### 集成协议

```
Lakehouse Flow                          下游系统
     │
     ├─ [CREATE] TaskInstance.READY
     │
     ├─ [API] GET /api/task-instances/ready ←── [查询]
     │
     ├─ [RECEIVE] POST /api/task-instances/{id}/status (RUNNING)
     │
     ├─ [RECEIVE] POST /api/task-instances/{id}/status (SUCCESS/FAILED)
     │
     └─ [UPDATE] TaskInstance.SUCCESS/FAILED
     
本系统职责: 创建、查询、更新状态
下游系统职责: 执行、监控、报告
```

## Phase 1 验证：现有设计是否正确？

### ✅ 已实现的部分符合定位

**LakehouseEvent**:
- ✅ 存储原始事件（不包含执行逻辑）
- ✅ JSONB payload 只用于追踪和审计
- ✅ 目的是获取 snapshot_id, watermark, quality_status

**AssetState**:
- ✅ 只关注资产是否就绪
- ✅ 单调性检查（新 snapshot/watermark 不会被旧的覆盖）
- ✅ 不涉及执行

**TaskInstance**:
- ✅ 记录调度决策的证据
- ✅ state machine 用于追踪实例生命周期
- ✅ external_job_id 用于 callback 追踪
- ✅ ❌ **注意**: 重试逻辑暂时在这里，但不应该过度设计

**Service 层**:
- ✅ EventService: 去重和存储（正确）
- ✅ AssetStateService: 单调性更新（正确）
- ✅ WorkflowInstanceService: 创建和状态转换（正确）
- ✅ TaskInstanceService: 创建和 READY 标记（正确）

### ⚠️ 需要注意的地方

现有 TaskInstanceService 中的重试逻辑：
```java
public void handleFailedTask(Long taskId) {
    // FAILED → RETRY_WAITING → READY
}
```

这个逻辑**仍然正确**，因为：
- 调度系统需要记录为什么重试
- 重试是调度决策（何时重新运行），不是执行决策（如何执行）
- 但是，**最大重试次数、退避策略**等可以由下游系统决定

## Phase 2 设计原则

### 原则 1: 不要做下游系统的工作

```
❌ 不实现: 资源组查询、资源预留、优先级调度
✅ 实现: 依赖条件评估、实例创建、状态转换

❌ 不实现: 复杂的 DAG 编排逻辑
✅ 实现: 简单的上游任务成功检查
```

### 原则 2: 所有循环都是独立的

```
Event Ingestion Loop
    ↓
Asset State Update Loop
    ↓
Dependency Evaluation Loop
    ↓
Task Ready Check Loop
    
每个循环:
- 独立运行（可在不同线程或不同节点）
- 异步通知下一个循环（或轮询）
- 不阻塞上游
```

### 原则 3: API 是唯一的集成点

```
下游系统通过 REST API 与本系统交互:
1. GET /api/task-instances/ready       查询可执行的任务
2. POST /api/task-instances/{id}/status 报告执行结果
3. GET /api/workflow-instances/{id}/waiting-reason 查询等待原因
```

### 原则 4: 审计日志记录一切

```
TriggerHistory:
- 哪个事件触发了哪个实例
- 依赖条件的具体评估结果
- 为什么创建这个实例
```

## Phase 2 核心组件

### 1. ConditionEvaluator

评估这些条件类型：
```
SNAPSHOT_EXISTS        : 资产是否有快照
SNAPSHOT_ID_GTE        : 快照 ID ≥ 阈值
WATERMARK_GTE          : watermark ≥ 时间戳
QUALITY_PASSED         : 质量检查是否通过
SCHEMA_COMPATIBLE      : schema 是否兼容
BACKFILL_COMPLETED     : 补数是否完成
CUSTOM_SQL_TRUE        : 自定义 SQL 条件
```

返回：
```java
EvaluationResult {
    satisfied: boolean
    waitingReason: String  // 如果不满足，为什么？
    description: String    // 审计日志
}
```

### 2. EventIngestionService + PaimonSnapshotScanner

定时扫描 Paimon $snapshots：
```sql
SELECT snapshot_id, schema_id, commit_time, watermark
FROM paimon_catalog.database.`table$snapshots`
WHERE snapshot_id > ?
ORDER BY snapshot_id ASC
```

去重策略：
```
eventId = "PAIMON:" + catalogName + ":" + databaseName + ":" + 
          tableName + ":" + snapshotId
          
同一 eventId 重复摄入时，由 LakehouseEvent.eventId unique constraint 自动处理
```

### 3. AssetStateUpdateLoop

处理步骤：
```
1. SELECT * FROM lakehouse_event WHERE processed_at IS NULL
2. 按 asset_key 分组
3. 对每个 asset，调用 assetStateService.updateAssetStateFromEvent()
4. 标记 processed_at = now()
5. 触发下一个循环（依赖评估）
```

### 4. DependencyEvaluationService

处理步骤：
```
1. SELECT * FROM asset_dependency WHERE asset_key = ?
2. 对每个 dependency，评估条件
3. 条件满足 → 创建 WorkflowInstance（幂等）
4. 记录 TriggerHistory
5. 触发下一个循环（Task Ready Check）
```

### 5. TaskReadyCheckService

处理步骤：
```
1. SELECT * FROM workflow_instance WHERE state = 'WAITING'
2. 对每个 workflow，获取其 TaskDefinition
3. 检查每个 task 的上游任务是否 SUCCESS
4. 如果上游都 SUCCESS → 创建 TaskInstance（幂等）
5. 标记 TaskInstance.state = 'READY'
```

### 6. TriggerHistory

记录一切触发事件（用于审计和排障）：
```java
TriggerHistory {
    id: Long
    triggerKey: String  // unique: workflow_id:task_id:trigger_id
    triggerType: String // "SNAPSHOT_DRIVEN" / "SCHEDULED" / "MANUAL"
    assetKey: String    // 触发的资产
    snapshotId: String
    watermark: LocalDateTime
    eventId: String
    workflowInstanceId: Long
    taskInstanceId: Long
    decision: String    // "TRIGGERED" / "SKIPPED"
    decisionReason: String  // 为什么？
    createdAt: LocalDateTime
}
```

## Phase 2 实现时间表

```
Week 1 (2-3 days):
  Day 1: ConditionEvaluator + EvaluationResult
  Day 1: PaimonSnapshotSource (mock or real)
  Day 2: EventIngestionService + 单元测试
  Day 2: AssetStateUpdateLoop + 单元测试
  Day 3: DependencyEvaluationService + 单元测试
  Day 3: TriggerHistoryService + TriggerHistory entity

Week 2 (2-3 days):
  Day 1: TaskReadyCheckService + 单元测试
  Day 1-2: REST API endpoints
  Day 2-3: 集成测试 (event → state → dependency → task)
  Day 3: 文档和例子
```

## 检查清单

实现 Phase 2 时，确保：

- [ ] ConditionEvaluator 只评估条件，不执行任何操作
- [ ] EventIngestionService 不涉及 dependency evaluation
- [ ] AssetStateUpdateLoop 不等待 dependency evaluation 完成
- [ ] DependencyEvaluationService 不涉及真实执行
- [ ] TaskReadyCheckService 只标记 READY，不执行任何操作
- [ ] 所有 Service 都返回业务结果（不做副作用）
- [ ] 所有循环都是幂等的（可重复运行）
- [ ] API 中没有资源相关的查询
- [ ] API 中没有执行相关的操作
- [ ] 所有关键路径有 TriggerHistory 记录
- [ ] 所有异常都有清晰的日志记录

## 关键区别：本系统 vs 传统调度系统

### 传统 Airflow/DolphinScheduler

```
时间到了 → 查询上游状态 → 如果成功则创建任务 → 执行任务
                                              ↑
                                        本系统负责的部分
```

### Lakehouse Flow

```
数据就绪 → 更新资产状态 → 评估依赖 → 创建任务实例 → [交给下游系统]
↑        ↑             ↑          ↑
事件摄入  单调性检查    条件评估   调度决策
                                (本系统的全部职责)
```

## 总结

**Lakehouse Flow 的三条铁律**:

1. **只做调度决策**  
   - 依赖评估、实例创建、状态转换
   - 不做资源检查、不做真实执行

2. **一切可追踪**  
   - 每个触发都记录 TriggerHistory
   - 用户能看到为什么某实例在等待

3. **充分解耦**  
   - 通过 REST API 与下游系统交互
   - 下游系统可以是任何调度/执行平台

---

**下一步**: 开始实现 Phase 2

**预计投入**: 2-3 天

**交付**: 从 Paimon 快照到 READY 任务的完整链路
