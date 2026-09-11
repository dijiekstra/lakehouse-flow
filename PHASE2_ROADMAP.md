# Lakehouse Flow - Phase 2 实现路线图

**日期**: 2026-09-11  
**状态**: Phase 2 开始准备  
**架构**: 调度决策层 + 下游执行编排层  

## Phase 2 目标

实现从 **事件摄入** 到 **任务就绪** 的完整调度链路：

```
Paimon 快照 → 事件入库 → 资产状态更新 → 依赖评估 → TaskInstance.READY
                                                      ↓
                                          [下游系统通过 API 消费]
```

## Phase 2 核心循环

### 循环 1: Event Ingestion Loop
**职责**: 持续监听和摄入 lakehouse 事件  
**输入**: Paimon $snapshots 表  
**输出**: LakehouseEvent（去重后存储）  

```
定时任务(10s):
  1. 扫描 Paimon $snapshots（snapshot_id > lastOffset）
  2. 生成 deterministic eventId
  3. 存入 LakehouseEvent（幂等）
  4. 推进消费进度（EventConsumerOffset）
```

**关键组件**:
- `PaimonSnapshotSource`: SQL 适配器
- `EventIngestionService`: 摄入逻辑
- `PaimonSnapshotScanner`: 后台循环

### 循环 2: Asset State Update Loop
**职责**: 从事件更新资产状态  
**输入**: 未处理的 LakehouseEvent  
**输出**: 更新后的 AssetState  

```
定时任务(5s):
  1. 查询 processed_at IS NULL 的事件
  2. 按 assetKey 分组
  3. 调用 assetStateService.updateAssetStateFromEvent()
  4. 标记 processed_at（幂等）
```

**关键组件**:
- `AssetStateUpdateLoop`: 循环实现
- `AssetStateService`: 单调性更新

### 循环 3: Dependency Evaluation Loop
**职责**: 评估依赖，创建 WorkflowInstance  
**输入**: 更新的 AssetState  
**输出**: 创建或更新 WorkflowInstance（幂等）  

```
定时任务(2s):
  1. 查询受影响的 AssetDependency（assetKey 匹配）
  2. 对每个 dependency，调用 conditionEvaluator.evaluate()
  3. 条件满足 → 创建 WorkflowInstance（幂等）
  4. 记录 TriggerHistory（审计）
```

**关键组件**:
- `ConditionEvaluator`: 条件评估引擎
- `DependencyEvaluationService`: 循环实现
- `TriggerHistoryService`: 审计日志

### 循环 4: Task Ready Check Loop
**职责**: 标记 TaskInstance 为 READY  
**输入**: 状态为 WAITING 的 WorkflowInstance  
**输出**: state=READY 的 TaskInstance  

```
定时任务(1s):
  1. 查询 state=WAITING 的 WorkflowInstance
  2. 对每个 workflow，检查其 TaskDefinition
  3. 如果上游任务都 SUCCESS，创建下游 TaskInstance
  4. 标记 TaskInstance.state = 'READY'（幂等）
```

**关键组件**:
- `TaskReadyCheckService`: 循环实现
- `TaskInstanceService`: 创建和状态转换

## Phase 2 新增代码

### Service 层（lakehouse-flow-service）

```
ConditionEvaluator.java
  - evaluate(dependency, assetKey) → EvaluationResult
  - 评估 SNAPSHOT_EXISTS, WATERMARK_GTE, QUALITY_PASSED, 等
  
DependencyEvaluationService.java
  - evaluateForAsset(assetKey)
  - 查询和评估所有受影响的依赖
  
EventIngestionService.java
  - ingestFromPaimon()
  - 高层编排循环
  
AssetStateUpdateLoop.java
  - processUnprocessedEvents()
  - 处理未处理事件
  
TaskReadyCheckService.java
  - markReadyTasksForWorkflow(workflowInstanceId)
  - 标记 READY 的任务
  
TriggerHistoryService.java
  - recordTrigger(...)
  - 记录为什么创建了实例
```

### Integration 层（lakehouse-flow-integration）

```
PaimonSnapshotSource.java
  - scanSnapshots(sinceSnapshotId) → List<PaimonSnapshot>
  - mapToLakehouseEvent(snapshot) → LakehouseEvent
  
PaimonSnapshotScanner.java (@Component, @Scheduled)
  - scanAndIngest()
  - 后台定时扫描
```

### DAO 层（lakehouse-flow-dao）

```
TriggerHistoryRepository.java
  - findByWorkflowInstanceId(...)
  - findByTriggerKey(...)
```

### Model 层（lakehouse-flow-model）

```
TriggerHistory.java
  - 触发审计日志实体
  
EvaluationResult.java
  - 依赖评估结果（DTO）
```

### API 层（lakehouse-flow-api）

```
TaskInstanceController.java
  - GET /api/task-instances/ready
  - POST /api/task-instances/{id}/status
  
WorkflowInstanceController.java
  - GET /api/workflow-instances/{id}/waiting-reason
  - GET /api/workflow-instances/{code}/waiting
  
DebugController.java
  - GET /api/triggers/{workflowInstanceId}
  - GET /api/asset-state/{assetKey}
```

## Phase 2 数据库变更

### 新增表

```sql
-- 触发历史（审计）
CREATE TABLE trigger_history (
    id BIGSERIAL PRIMARY KEY,
    trigger_key VARCHAR(255) NOT NULL UNIQUE,
    trigger_type VARCHAR(32) NOT NULL,  -- SNAPSHOT_DRIVEN, SCHEDULED, MANUAL
    asset_key VARCHAR(255),
    snapshot_id VARCHAR(255),
    watermark TIMESTAMP,
    event_id VARCHAR(255),
    workflow_instance_id BIGINT REFERENCES workflow_instance(id),
    task_instance_id BIGINT REFERENCES task_instance(id),
    decision VARCHAR(32),  -- TRIGGERED, SKIPPED
    decision_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 事件消费进度
CREATE TABLE event_consumer_offset (
    id BIGSERIAL PRIMARY KEY,
    source_type VARCHAR(32) NOT NULL,  -- PAIMON, ICEBERG, HUDI
    source_name VARCHAR(255) NOT NULL,
    last_processed_snapshot_id VARCHAR(255),
    last_processed_watermark TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, source_name)
);
```

### 新增字段

```sql
-- 在 lakehouse_event 表中添加
ALTER TABLE lakehouse_event ADD COLUMN processed_at TIMESTAMP;
CREATE INDEX idx_lakehouse_event_processed_at ON lakehouse_event(processed_at);

-- 在 workflow_instance 表中添加
ALTER TABLE workflow_instance ADD COLUMN trigger_asset_key VARCHAR(255);
ALTER TABLE workflow_instance ADD COLUMN trigger_event_id VARCHAR(255);
```

## Phase 2 实现清单

### Step 1: 基础数据结构（1 天）
- [ ] EvaluationResult.java
- [ ] TriggerHistory.java
- [ ] EventConsumerOffset.java
- [ ] 数据库迁移脚本
- [ ] 相应的 Repository 接口

### Step 2: 条件评估引擎（1 天）
- [ ] ConditionEvaluator.java
- [ ] 支持 SNAPSHOT_EXISTS, WATERMARK_GTE, QUALITY_PASSED, 等
- [ ] 单元测试（各条件类型）
- [ ] 集成测试（条件组合）

### Step 3: 事件摄入循环（1 天）
- [ ] PaimonSnapshotSource.java（SQL 适配器）
- [ ] EventIngestionService.java
- [ ] PaimonSnapshotScanner.java（定时任务）
- [ ] 去重逻辑验证
- [ ] 单元测试

### Step 4: 资产状态更新循环（1 天）
- [ ] AssetStateUpdateLoop.java
- [ ] 事件标记处理
- [ ] 单调性验证
- [ ] 单元测试

### Step 5: 依赖评估循环（1 天）
- [ ] DependencyEvaluationService.java
- [ ] TriggerHistoryService.java
- [ ] WorkflowInstance 创建（幂等）
- [ ] 单元测试

### Step 6: Task Ready 循环（0.5 天）
- [ ] TaskReadyCheckService.java
- [ ] 上游任务检查逻辑
- [ ] TaskInstance.state = READY 标记
- [ ] 单元测试

### Step 7: REST API（1 天）
- [ ] TaskInstanceController（GET /ready, POST /status）
- [ ] WorkflowInstanceController（GET /waiting-reason）
- [ ] DebugController（GET /triggers, GET /asset-state）
- [ ] 集成测试

### Step 8: 端到端测试（0.5 天）
- [ ] Event → State → Dependency → Task 完整链路
- [ ] Mock Paimon 事件
- [ ] 验证幂等性和状态转换

### Step 9: 文档和例子（0.5 天）
- [ ] Phase 2 实现总结
- [ ] 下游系统集成示例（Airflow/Spark 脚本）
- [ ] 故障排查指南

**总计工作量**: 7-8 天（含测试）

## Phase 2 完成标准

### 功能完整性
- ✅ 能够从 Paimon 摄入快照事件
- ✅ 能够自动更新资产状态
- ✅ 能够评估依赖条件
- ✅ 能够创建 WorkflowInstance 和 TaskInstance
- ✅ 能够通过 API 查询 READY 的任务
- ✅ 能够通过 callback 更新任务状态

### 代码质量
- ✅ 所有循环都是幂等的
- ✅ 所有关键路径都有单元测试
- ✅ 所有异常都有清晰的日志
- ✅ 没有内存泄漏或资源泄露

### 可观测性
- ✅ 关键指标：事件摄入数、依赖满足数、任务创建数
- ✅ 关键日志：事件入库、状态更新、依赖评估结果
- ✅ Debug API：查询为什么任务在等待、查询触发历史

### 文档完整性
- ✅ Phase 2 实现总结
- ✅ 下游系统集成指南
- ✅ 故障排查文档
- ✅ API 文档

## Phase 2 后续

### 可选功能（Phase 2.5 及以后）
- Status Reconciliation Loop（从下游系统拉取最新状态）
- Timeout Detection Loop（检测超时的运行实例）
- Compensation Loop（处理失败和重试）
- Manual Trigger API（手工触发工作流）
- Backfill Support（支持补数）

### 优化方向
- 消息队列替换（Kafka 代替数据库轮询）
- 分布式协调（etcd/ZooKeeper 代替数据库 lease）
- 性能优化（批处理、缓存、索引）
- 可视化界面（查看 DAG、追踪流程）

## 关键设计原则（再次强调）

1. **只做调度决策**
   - 不涉及资源检查、不涉及真实执行
   - 所有执行相关的工作交给下游系统

2. **充分解耦**
   - 通过 REST API 与下游系统交互
   - 循环之间异步通知或独立轮询

3. **完全幂等**
   - 重复运行同一操作不会产生重复结果
   - 使用唯一 key 和去重逻辑保证

4. **全面可追踪**
   - 每个关键决策都记录 TriggerHistory
   - 用户能看到为什么创建了某实例

## 时间估计

| 模块 | 工作量 | 难度 |
|------|--------|------|
| 数据结构 | 0.5d | ⭐ |
| 条件评估 | 1d | ⭐⭐ |
| 事件摄入 | 1d | ⭐⭐ |
| 资产更新 | 0.5d | ⭐ |
| 依赖评估 | 1d | ⭐⭐ |
| Task Ready | 0.5d | ⭐ |
| REST API | 1d | ⭐⭐ |
| 测试 | 1.5d | ⭐⭐⭐ |
| 文档 | 0.5d | ⭐ |
| **总计** | **7d** | **平均 ⭐⭐** |

---

**准备就绪**: Phase 2 开始实现

**预期产出**:
- 从 Paimon 快照到 READY 任务的完整链路
- REST API 可供下游系统集成
- 10+ 循环和集成测试
- 完整文档

