# Phase 2 设计更新 - 任务交付方式澄清

> 历史文档：文中的下游轮询 Lakehouse Flow READY API 方案已废弃。当前由 Lakehouse Flow 内部写入数据库 outbox，后续通过内部 HTTP/MQ publisher 主动投递；下游结果不会回传并参与判断。

**日期**: 2026-09-11 22:00  
**更新**: 任务调度后的实现方式

## 核心澄清

### 任务调度后的三种可能实现方式

本系统已**确定采用**：**方案 1 - 入表扫库**

```
┌─────────────────────────────────────────┐
│  Lakehouse Flow (调度决策)               │
│                                         │
│  TaskInstance.state = READY             │
│  INSERT INTO task_instance_queue        │
└────────────┬────────────────────────────┘
             │ 数据库表
             ▼
┌─────────────────────────────────────────┐
│  下游执行系统                           │
│                                         │
│  SELECT * FROM task_instance_queue      │
│  WHERE state = READY                    │
│  → 提取、执行、标记消费                │
└─────────────────────────────────────────┘
```

### 为什么选择方案 1？

| 特性 | 方案 1（入表） | 方案 2（API） | 方案 3（消息） |
|------|--------|--------|---------|
| 耦合度 | ✅ 最低 | ❌ 中等 | ⚠️ 需要 MQ |
| 实现难度 | ✅ 最简单 | ⚠️ 需要 HTTP 集成 | ⚠️ 需要 MQ 集成 |
| 扩展性 | ✅ 支持多消费者 | ⚠️ 单点 API | ✅ 支持多消费者 |
| 容错性 | ✅ 天然支持重试 | ✅ 可实现 | ✅ 天然支持 |
| 性能 | ✅ 无网络开销 | ❌ 网络延迟 | ✅ 异步高效 |

**结论**: 方案 1 最适合"纯 snapshot 驱动"的设计

## Phase 2 的简化设计

### 核心流程（简化版）

```
Event Ingestion Loop (10s)
  ├─ Scan Paimon $snapshots
  ├─ Deduplicate (eventId unique)
  └─ INSERT INTO lakehouse_event
     ↓
Asset State Update Loop (5s)
  ├─ SELECT * FROM lakehouse_event WHERE processed_at IS NULL
  ├─ UPDATE asset_state (单调性检查)
  └─ UPDATE lakehouse_event.processed_at
     ↓
Dependency Evaluation Loop (2s)
  ├─ SELECT * FROM asset_dependency WHERE asset_key = ?
  ├─ Evaluate conditions (JSON)
  ├─ CREATE OR SKIP WorkflowInstance
  └─ INSERT INTO trigger_history (审计)
     ↓
Task Ready Check Loop (1s)
  ├─ SELECT * FROM workflow_instance WHERE state = WAITING
  ├─ Check upstream task success
  ├─ CREATE TaskInstance (幂等)
  └─ Mark state = READY
     ↓
TaskInstance.READY → 入表等待消费
```

### 不需要的循环

❌ **Status Reconciliation Loop**: 
- 原因：系统不关心任务执行结果
- 解耦：下游系统自行处理执行和监控

❌ **Callback API**:
- 原因：不需要实时状态更新
- 简化：系统职责更清晰

### 需要的表结构

原设计完全适用，无需修改：
- ✅ lakehouse_event
- ✅ asset_state
- ✅ workflow_instance
- ✅ task_instance ← **入表后，等待消费**
- ✅ trigger_history ← **审计和追踪**

## Phase 2 简化后的完成标准

✅ **事件摄入**
- 能从 Paimon 摄入快照事件
- 事件去重（eventId unique）
- 消费进度记录

✅ **资产状态更新**
- 自动更新资产状态
- 单调性保证（新 snapshot/watermark 不被旧的覆盖）
- 版本跟踪

✅ **依赖评估**
- 评估依赖条件（多类型）
- 自动创建 WorkflowInstance（幂等）
- 审计日志（TriggerHistory）

✅ **任务就绪**
- 创建 TaskInstance（幂等）
- 标记为 READY
- 入表等待消费

❌ **不需要的东西**
- ❌ 回调 API（下游系统不回报结果）
- ❌ 状态拉取循环（无需同步最新状态）
- ❌ 执行监控（下游系统自己监控）
- ❌ 重试管理（下游系统自己决定）

## 执行系统集成示例

### 方案 1（推荐）：下游系统主动扫表

```python
# 伪代码：下游 Airflow/K8s 集成

def fetch_and_execute():
    """从 Lakehouse Flow 数据库获取 READY 任务"""
    
    # 1. 查询 READY 的任务
    tasks = db.query("""
        SELECT id, instance_key, workflow_code, task_code, biz_date
        FROM task_instance
        WHERE state = 'READY'
        LIMIT 100
    """)
    
    for task in tasks:
        try:
            # 2. 执行任务（下游系统的职责）
            job_id = submit_spark_job(task.workflow_code, task.biz_date)
            
            # 3. 更新状态（可选，下游系统决定是否回写）
            # db.update(f"UPDATE task_instance SET state='RUNNING' WHERE id={task.id}")
            
        except Exception as e:
            log.error(f"Failed to execute {task.instance_key}: {e}")
            # 下游系统决定是否标记失败、重试等
```

### 为什么这样设计最好？

1. **下游系统完全独立**
   - 不需要知道 Lakehouse Flow 的 API
   - 只需要访问同一个数据库
   - 或通过异步工具（如 Logstash）监听表变化

2. **天然的去重和幂等**
   - 任务 state = READY 时，还没被任何执行系统消费
   - 下游系统消费后可自行标记状态

3. **支持多种部署方式**
   - 同库部署：共用同一个 PostgreSQL
   - 异库部署：Lakehouse Flow 的表通过 CDC（Debezium）同步给下游

4. **易于调试和问题排查**
   - 所有中间态都在数据库里
   - 可以直接 SQL 查询

## Phase 2 实现步骤（不变）

Step 1: TriggerHistory + EvaluationResult 实体  
Step 2: ConditionEvaluator（条件评估引擎）  
Step 3: PaimonSnapshotSource + EventIngestionService  
Step 4: AssetStateUpdateLoop  
Step 5: DependencyEvaluationService  
Step 6: TaskReadyCheckService  
Step 7: ~~REST API (callback)~~ → **变更：查询 API 仍需要，但不需要 callback**  
Step 8-9: 测试和文档  

### API 设计更新

只需要提供**查询 API**（不需要 callback API）：

```
GET /api/task-instances/ready
    查询所有 state=READY 的任务
    下游系统自行扫表或通过这个 API 定期检查

GET /api/workflow-instances/{id}/waiting-reason
    查询为什么某个工作流在等待
    便于排障

GET /api/triggers/{workflowInstanceId}
    查询为什么创建了某个实例
    审计和追踪

DELETE /api/task-instances/{id}  (可选)
    下游系统消费后，可选地删除已处理的任务
```

## 关键洞察

这个设计的妙处在于：

1. **系统职责最纯粹**
   - Lakehouse Flow = 纯决策 + 入表
   - 下游系统 = 纯执行 + 扫表
   - 完全解耦

2. **没有复杂的集成**
   - 不需要 HTTP API 集成
   - 不需要消息队列集成
   - 只需要共用数据库或 CDC

3. **天然高可用**
   - 多个下游系统可以并行消费任务
   - 任务表是持久化的，系统宕机也不丢失
   - 自动支持重试和幂等

4. **完全可追踪**
   - 所有中间态都在表里
   - 可以随时查询决策过程
   - 便于审计和调试

## 下一步

开始 Phase 2 实现，从 Step 1 开始创建数据模型和表结构。

---

**结论**: 采用方案 1（入表扫库）是最简洁、最可靠、最易扩展的方案。
