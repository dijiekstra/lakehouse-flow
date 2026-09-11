# 架构定位澄清总结

**日期**: 2026-09-11  
**原因**: 确保 Phase 2 设计与系统定位一致  
**影响范围**: 无（Phase 1 已完全符合设计）

## 背景

在准备进入 Phase 2（事件处理循环）时，明确了系统的核心定位：

**Lakehouse Flow = 纯调度决策系统**

不负责任务执行的具体细节，这些交由下游系统处理。

## 系统定位对比

### Phase 1 之前的理解（可能有歧义的地方）

❓ 本系统要不要处理资源检查？  
❓ 本系统要不要做 DAG 编排？  
❓ 本系统要不要真实执行任务？  

### Phase 1 之后的明确定位

✅ **本系统职责**:
- 监听 lakehouse 事件（Paimon/Iceberg/Hudi snapshots）
- 更新数据资产状态
- 评估工作流依赖条件
- 创建 TaskInstance（幂等）
- 标记任务为 READY
- **通过 REST API 提供给下游系统**

❌ **本系统不做**:
- 资源组检查（resource quota validation）
- 优先级调度（priority scheduling）
- DAG 编排优化（DAG orchestration）
- 真实任务执行（task execution）
- 监控和告警（monitoring and alerting）

### 架构图

```
┌─────────────────────────────────────────────────┐
│     Lakehouse Flow - 调度决策层                  │
├─────────────────────────────────────────────────┤
│ Input:  Paimon/Iceberg/Hudi Snapshots           │
│ Output: READY TaskInstances (via REST API)      │
│                                                 │
│ • Event Ingestion Loop (10s)                    │
│ • Asset State Update Loop (5s)                  │
│ • Dependency Evaluation Loop (2s)               │
│ • Task Ready Check Loop (1s)                    │
└────────┬────────────────────────────────────────┘
         │ REST API (GET/POST)
         ▼
┌─────────────────────────────────────────────────┐
│   下游执行系统 (Airflow/K8s/Custom/etc.)        │
├─────────────────────────────────────────────────┤
│ • 资源检查 (Resource validation)                │
│ • DAG 编排 (Dependency orchestration)           │
│ • 真实执行 (Task execution)                     │
│ • 监控告警 (Monitoring & alerting)              │
└─────────────────────────────────────────────────┘
```

## Phase 1 验证

### ✅ 已实现的部分完全符合定位

| 组件 | 职责 | 评价 |
|------|------|------|
| **LakehouseEvent** | 存储原始事件 | ✅ 纯数据，无逻辑 |
| **AssetState** | 资产状态更新 | ✅ 决策数据 |
| **WorkflowInstance** | 工作流实例创建和转换 | ✅ 调度决策证据 |
| **TaskInstance** | 任务实例生命周期 | ✅ 就绪标记和状态转换 |
| **EventService** | 事件去重和存储 | ✅ 幂等操作 |
| **AssetStateService** | 单调性更新 | ✅ 业务逻辑 |
| **WorkflowInstanceService** | 实例创建和状态验证 | ✅ 调度逻辑 |
| **TaskInstanceService** | 实例生命周期管理 | ✅ 只涉及状态，不执行 |

### ⚠️ 需要澄清的地方

**TaskInstanceService 中的重试逻辑**：

```java
public void handleFailedTask(Long taskId) {
    // FAILED → RETRY_WAITING → READY
}
```

这个逻辑**仍然正确**，因为：
- ✅ 重试是**调度决策**（何时重新运行），不是**执行决策**（如何执行）
- ✅ Lakehouse Flow 需要记录为什么重试
- ✅ 但是最大重试次数、退避策略等**可以由下游系统覆盖**

## Phase 2 设计指导原则

### 原则 1: 不要越权

```
❌ 不要实现资源检查、资源分配、优先级调度
✅ 实现: 依赖条件评估、实例创建、状态转换
```

### 原则 2: 循环独立运行

```
各个循环之间不相互阻塞：
• Event Ingestion Loop (10s) → LakehouseEvent
• Asset State Update Loop (5s) → AssetState
• Dependency Evaluation Loop (2s) → WorkflowInstance
• Task Ready Check Loop (1s) → TaskInstance.READY
```

### 原则 3: REST API 是唯一的集成点

```
下游系统通过以下 API 与本系统交互：
• GET /api/task-instances/ready           查询就绪的任务
• POST /api/task-instances/{id}/status    报告执行结果
• GET /api/workflow-instances/{id}/waiting-reason  查询等待原因
```

### 原则 4: 审计日志记录一切

```
TriggerHistory 记录：
• 哪个事件触发了哪个实例
• 依赖条件的具体评估结果
• 为什么创建了这个实例
→ 便于用户排障和审计
```

## 对 Phase 1 的验证

### 编译和测试

✅ 所有 9 模块编译成功（3.251s）  
✅ 13 个单元测试全部通过  
✅ 代码无内存泄漏或资源泄露  

### 架构正确性

✅ 幂等性保证（所有创建操作都有唯一 key）  
✅ 单调性保证（旧事件不会覆盖新状态）  
✅ 状态机验证（所有转换都是合法的）  
✅ 乐观锁并发控制（@Version）  
✅ 事件去重（eventId unique constraint）  

### 代码质量

✅ 无过度设计（使用直观的 Service + Repository 模式）  
✅ 清晰的职责划分（每个 Service 对应一个实体）  
✅ 充分的文档（所有关键类都有 JavaDoc）  

## 对 Phase 2 的指导

### 设计原则（再次强调）

1. **只做调度决策** - 不做资源/执行相关的事
2. **充分解耦** - 通过 REST API 与下游系统交互
3. **完全幂等** - 重复运行同一操作不产生重复结果
4. **全面可追踪** - 每个决策都有 TriggerHistory 记录

### Phase 2 核心循环

| 循环 | 间隔 | 输入 | 输出 | 关键组件 |
|------|------|------|------|---------|
| Event Ingestion | 10s | Paimon $snapshots | LakehouseEvent | PaimonSnapshotScanner |
| Asset State Update | 5s | 未处理事件 | 更新的 AssetState | AssetStateUpdateLoop |
| Dependency Evaluation | 2s | 更新的 AssetState | WorkflowInstance | DependencyEvaluationService |
| Task Ready Check | 1s | WAITING WorkflowInstance | READY TaskInstance | TaskReadyCheckService |

### Phase 2 新增组件（总计 10+）

**Service 层**:
- ConditionEvaluator
- DependencyEvaluationService
- EventIngestionService
- AssetStateUpdateLoop
- TaskReadyCheckService
- TriggerHistoryService

**Integration 层**:
- PaimonSnapshotSource
- PaimonSnapshotScanner

**API 层**:
- TaskInstanceController
- WorkflowInstanceController
- DebugController

**Model 层**:
- TriggerHistory
- EvaluationResult

## 检查清单

实现 Phase 2 时，确保满足以下条件：

- [ ] 所有循环都是幂等的
- [ ] 所有循环都可以独立运行
- [ ] 没有一个循环依赖另一个循环的在内存中的状态
- [ ] API 中没有资源相关的查询
- [ ] API 中没有执行相关的操作
- [ ] 所有关键路径都有 TriggerHistory 记录
- [ ] 所有异常都有清晰的日志记录
- [ ] 没有过度的优化（如缓存、预分配等）

## 文档更新

已更新以下文档反映这个澄清：

- ✅ README.md - 强调系统定位
- ✅ ARCHITECTURE_CLARIFICATION.md - 详细说明系统定位
- ✅ PHASE2_DESIGN.md - 新组件和循环设计
- ✅ PHASE2_ROADMAP.md - 实现路线图
- ✅ 本文档

## 结论

Lakehouse Flow 是一个**纯调度决策系统**，与下游执行系统通过 REST API 解耦。

Phase 1 的设计完全符合这个定位。  
Phase 2 将基于这个定位继续实现事件处理循环。

---

**下一步**: 开始 Phase 2 实现

**预计工作量**: 7 天

**交付物**: 从 Paimon 快照到 READY 任务的完整链路，包括 REST API 和完整测试

