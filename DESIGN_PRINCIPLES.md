# 设计原则与不可退让的约束

## 简介

Lakehouse Flow 不是通用调度系统的增强版，而是为 CDC 湖仓架构重新设计的调度系统。本文阐述为什么选择这些设计，以及在实现中**必须遵守**的七大原则。

## 七大不可退让的原则

### 1. 资产状态驱动，而非事件触发

**原则**：不能"事件到了就触发"，必须"资产状态达到条件才触发"。

**为什么**：

事件是易碎的。在分布式系统中，事件可能：
- **重复到达**：同一个 snapshot 被上报两次
- **乱序到达**：新快照的事件先到，旧快照的事件后到
- **延迟到达**：某个快照事件迟到 1 小时
- **丢失**：网络故障导致事件完全丢失

如果直接用事件触发，这些问题会导致：
- 重复触发同一个 snapshot（浪费资源，可能导致数据不一致）
- 旧快照覆盖新快照（数据回退）
- 永久卡住等待一个丢失的事件

**解决方案**：

事件只作为**证据**，被摄入后，**立即归并成资产的当前状态**。调度决策基于资产状态，而非原始事件。

```
事件          资产状态         触发决策
snapshot_1001 → latestSnapshotId=1001 → 依赖满足 → 触发
snapshot_1002 → latestSnapshotId=1002 → 依赖满足 → 触发
(重复) 1001   → latestSnapshotId=1002 → 已过期 → 忽略
```

### 2. 幂等触发，同一版本只能触发一次

**原则**：同一快照版本、同一依赖组合只能触发一次。

**为什么**：

在多 Master 并发或网络抖动的场景下，同一个触发条件可能被评估多次：

```
时间     Master A                    Master B
T1       发现 snapshot_1001 就绪
T2       计算 trigger_key
T3                                   发现 snapshot_1001 就绪
T4       创建 Command A
T5                                   计算 trigger_key
T6       工作流 A 开始执行
T7                                   创建 Command B
T8                                   工作流 B 开始执行（重复！）
```

这会导致同一个快照被处理两次，产生重复的数据和破坏一致性。

**解决方案**：

`trigger_key` 由以下组件组成：
```
workflow_code + dependency_group + snapshot_id_1 + snapshot_id_2 + ...
```

例如：
```
workflow_12345_group_gmv_default_snapshot_1001_snapshot_1002
```

这个 `trigger_key` 在 `t_ds_asset_trigger_history` 表中有**唯一约束**。

```
INSERT INTO t_ds_asset_trigger_history 
  (trigger_key, status, ...)
```

在并发下：
- Master A：INSERT 成功 → 获得触发权 → 创建 Command
- Master B：INSERT 失败（唯一约束冲突）→ 捕获异常 → 跳过（已被 Master A 触发）

**这消除了对分布式锁的需要**。

### 3. 事件视为证据，资产状态是事实来源

**原则**：事件可能重复/乱序/延迟/丢失，资产状态才是调度判定的唯一事实。

**为什么**：

传统调度在 cron 时间点生成实例。现代调度需要"数据就绪时"生成实例，但什么是"就绪"？

如果只看事件：
- "收到 snapshot_1001 事件" ≠ "快照 1001 真的存在"
- 事件可能被处理器吞掉、丢失或延迟

如果看资产状态：
- `t_ds_asset_state.latest_snapshot_id = 1001` → 这是"事实"
- 这个事实是通过事件推进的，但事实本身是独立的
- 即使新事件到达后，旧事实仍然有效

**解决方案**：

```
事件流                           资产状态流
snapshot_1001 事件
  → INSERT event (去重)          
  → event_id 已存在? NO          
  → 处理事件                      → UPDATE AssetState (乐观锁)
  → latestSnapshotId 推进为 1001 → latestSnapshotId = 1001
  ↓
(重复) snapshot_1001 事件
  → INSERT event (去重)
  → event_id 已存在? YES
  → 直接跳过
  ↓
旧的 snapshot_1000 事件（延迟到达）
  → INSERT event (去重)
  → event_id 已存在? NO
  → 处理事件
  → latestSnapshotId = 1000 < 1001? 是
  → 乱序保护，忽略，不更新 AssetState
```

### 4. 乐观锁防止并发覆盖，单调递增

**原则**：资产状态的版本号（latestSnapshotId、latestWatermark）必须单调递增，使用乐观锁。

**为什么**：

假设没有乐观锁保护，在高并发场景：

```
事件处理线程 A              事件处理线程 B
读取 AssetState
  latestSnapshotId=1000
  version=5
                           读取 AssetState
                             latestSnapshotId=1000
                             version=5
UPDATE SET latestSnapshotId=1001
处理快照 1001
UPDATE SET latestSnapshotId=1002
  (此时 latestSnapshotId=1002)
                           UPDATE SET latestSnapshotId=1001
                             (快照 1001 覆盖了 1002！)
```

结果：资产状态从 1002 回退到 1001，破坏了单调性。

**解决方案**：

```sql
UPDATE t_ds_asset_state
SET latest_snapshot_id = ?,
    latest_watermark = GREATEST(latest_watermark, ?),
    version = version + 1
WHERE asset_key = ? 
  AND version = ?
  AND ? > latest_snapshot_id
```

关键检查：
1. `WHERE version = ?`：乐观锁，版本必须匹配
2. `AND ? > latest_snapshot_id`：单调性检查，新 snapshot_id 必须大于当前值
3. 如果 UPDATE 影响行数为 0，说明并发冲突或乱序，需要重试或忽略

### 5. 唯一触发键通过数据库唯一约束实现幂等

**原则**：使用数据库原生的唯一约束，而不是分布式锁或内存状态，实现幂等性。

**为什么**：

分布式锁的问题：
- 需要额外的协调服务（Zookeeper、Redis、etcd）
- 实现复杂，容易有 bug（锁泄露、竞态）
- 性能受限于锁服务的吞吐

数据库唯一约束的优势：
- **数据库原生支持**，可靠性已经验证过
- **零额外依赖**，不需要第三方服务
- **性能高**，比分布式锁快 100+ 倍
- **符合 ACID**，与事务语义一致

**关键实现**：

```sql
CREATE TABLE t_ds_asset_trigger_history (
  ...
  trigger_key VARCHAR(256) NOT NULL UNIQUE,
  ...
)
```

```java
try {
  triggerHistoryDao.insert(new TriggerHistory(
    triggerKey: "workflow_12345_group_gmv_default_snapshot_1001_1002",
    status: "TRIGGERING",
    ...
  ))
  // INSERT 成功，获得触发权
  commandService.createCommand(...)
} catch (DuplicateKeyException e) {
  // 唯一约束冲突，已被其他 Master 触发
  log.info("Trigger already executed by other master, skipping")
}
```

### 6. 可解释的等待，用户随时可查询原因

**原则**：每个等待中的任务都必须能清楚地说明"为什么还在等待"。

**为什么**：

传统调度系统中，如果任务卡住，运维很难排查：
- "为什么任务还在 WAITING？"
- 上游任务是否完成了？
- 是否有 BUG 导致任务被永久遗忘？

在资产驱动的调度中，等待原因应该非常明确。

**解决方案**：

每个等待中的任务实例都记录当前**不满足的条件**：

```json
{
  "taskInstanceId": 11111,
  "state": "WAITING_DEPENDENCY",
  "dependencies": [
    {
      "assetKey": "paimon://ods_db/ods_order_latest",
      "condition": "snapshotRequired",
      "currentState": { "latestSnapshotId": null },
      "satisfied": false,
      "waitingReason": "Asset has no snapshot yet"
    },
    {
      "assetKey": "paimon://ods_db/ods_order_latest",
      "condition": "qualityStatus == PASSED",
      "currentState": { "qualityStatus": "FAILED" },
      "satisfied": false,
      "waitingReason": "Asset quality check failed: 10000 rows with NULL order_id"
    }
  ],
  "waitingSince": "2024-09-11T09:30:00Z",
  "expectedReadyTime": "当 ods_order_latest snapshot 推进 AND quality 通过"
}
```

API 端点：
```
GET /api/v1/tasks/{taskId}/waiting-reason
  → 返回详细的等待条件分析
```

### 7. 完整的补偿机制，定期兜底

**原则**：遗漏的事件、失败的触发都不会永久卡住，补偿扫描器定期检查和重试。

**为什么**：

即使有了前 6 个原则，仍然可能发生：
- 事件摄入扫描器宕机，漏采集了某些快照
- Command 创建失败（网络抖动、数据库暂时不可用）
- 触发历史记录丢失（极端场景）

如果没有补偿，系统会永久卡住某些任务。

**解决方案**：

补偿扫描器（Compensation Scanner）每 5~15 分钟运行一次：

```
1. 检查漏采集事件
   FOR EACH asset:
     actual_latest = query_paimon_latest_snapshot_id()
     state_latest = SELECT latest_snapshot_id FROM t_ds_asset_state
     IF actual_latest > state_latest:
       → 缺失了 snapshots，补写 t_ds_asset_event

2. 检查失败触发
   SELECT * FROM t_ds_asset_trigger_history 
     WHERE status = 'FAILED' AND retry_count < MAX_RETRIES
   FOR EACH:
     → 检查是否已创建对应 Command
     → 如果没有，重试 createCommand()

3. 检查卡顿任务
   SELECT * FROM task_instance 
     WHERE state = 'WAITING_DEPENDENCY' 
       AND waiting_time > 6 HOURS
   FOR EACH:
     → 发告警，可选地强制转为 ERROR

4. 检查丢失的回调
   SELECT * FROM executor_job 
     WHERE status = 'RUNNING' 
       AND last_status_check > 2 HOURS
   FOR EACH:
     → 重新查询执行器状态
     → 如果已完成但未回写，补写状态
```

补偿的幂等性：
- 补写的事件通过唯一约束自动去重
- 重试触发通过 trigger_key 的幂等性保证
- 补偿本身是可重复运行的

## 为什么这样设计

### 背景：CDC + 湖仓架构的新需求

传统调度系统（DolphinScheduler、Airflow）基于：
1. **时间表达**：cron 表达式定义何时运行
2. **任务依赖**：DAG 中前置任务成功后，后置任务才能运行
3. **离线/实时分离**：离线和实时有两套链路

这套模型在时间和数据可用性一致的场景下工作得很好。但在 CDC 湖仓场景下有根本性矛盾：

```
预定时间：01:00 AM
实际数据到达：23:50 PM（前一天）或 02:30 AM（延迟）

如果用时间触发，可能：
- 数据还没到就跑了（结果不完整）
- 等了太久才跑，浪费了可用时间
- 要手工调整 cron 表达式，容易出错
```

### Lakehouse Flow 的解决方案

**不问"现在几点了"，问"数据就绪了吗"**

```
Paimon snapshot 版本推进 → 被感知为 LakehouseEvent
  → 立即更新 AssetState（基于乐观锁和单调性）
  → 评估所有依赖该资产的工作流/任务
  → 条件满足就立即触发（秒级响应）
  → 无需等待预定时间
```

这样做的好处：

1. **零延迟**：数据一就绪就立即处理，不浪费任何时间
2. **自适应**：快速到达就快速处理，慢速到达就耐心等待，自动适应变化
3. **准确性**：不会因为时间到了就处理不完整的数据
4. **可审计**：每次触发都有明确的快照版本、质量检查、schema 版本作为证据

## 与通用调度系统的权衡

### Lakehouse Flow 放弃的特性

- **通用任务编排**：不支持"任意拓扑的 DAG"，假设用户的 DAG 是基于数据资产的
- **复杂的触发条件**：不支持"某个文件是否存在"、"API 是否可达"这类外部条件
- **跨组织协调**：假设在单个数据平台内部，不处理跨组织的调度
- **资源竞争**：不处理"在有限的 Spark 集群中分配资源"这类问题

### 为什么放弃这些

这些特性在通用调度系统中很重要，但在 CDC 湖仓场景下是**噪音**。

如果你的数据架构是：
- ODS（从业务库 CDC）→ DWD（加工）→ DWS（聚合）→ ADS（服务）

那么整个链路都是**数据资产驱动**的。你不需要通用的任意拓扑支持。

### 如何复用现有的调度系统

如果你已经有 DolphinScheduler 或 Airflow，Lakehouse Flow 可以和它们**共存**：

```
选项 1：完全替代
Lakehouse Flow 完全取代原有的时间驱动调度
（需要重新定义所有 DAG）

选项 2：混合使用
- 实时链路（CDC → ODS → DWD → DWS）用 Lakehouse Flow
- 其他批处理任务（数据清洗、报表生成）仍用 DolphinScheduler
- 通过 API 集成

选项 3：Lakehouse Flow 作为消费端
Lakehouse Flow 通过 HTTP/Webhook 触发 DolphinScheduler 的工作流
```

## 关键设计决策

### 为什么用数据库表作为事件队列？

**选择**：用 `t_ds_asset_event` 表作为事件存储，而不是 Kafka

**原因**：
- MVP 阶段不需要高吞吐（1000 events/sec 足够）
- 数据库是已有的基础设施
- 简化部署，无需额外的 Kafka 集群
- 事件持久化天然实现

**后续扩展**：
- 如果吞吐需求增长，可以引入 Kafka
- Kafka 消费者写入 `t_ds_asset_event`
- 应用侧无需改动

### 为什么用乐观锁而不是悲观锁？

**选择**：乐观锁（version 字段）而不是 SELECT FOR UPDATE

**原因**：
- 并发冲突本来就是稀有情况（一个资产同时被多个事件更新的概率很低）
- 乐观锁不需要持有行锁，吞吐量更高
- 乐观锁的冲突可以安全地重试

### 为什么不用分布式锁？

**选择**：用数据库唯一约束，不引入 Redis 或 Zookeeper

**原因**：
- 简化架构，减少外部依赖
- 数据库唯一约束的可靠性已经被验证
- 唯一约束冲突本身就是"已经被触发"的证据

## 总结

这七个原则不是任意选择，而是为 CDC 湖仓架构量身定制的。它们相互关联：

```
原则 1（资产状态驱动）
  ↓
原则 2（幂等触发）
  ↓
原则 5（唯一约束实现幂等）
  ↓
原则 3（事件视为证据）
  ↓
原则 4（乐观锁单调递增）
  ↓
原则 7（补偿兜底）
  ↓
原则 6（可解释的等待）
```

遵循这些原则的系统能够：
- ✅ 在分布式、高并发、不可靠的网络下保持一致性
- ✅ 不需要分布式锁
- ✅ 性能足以应对生产规模
- ✅ 问题可排查，等待原因清楚
- ✅ 自动补偿，没有永久的卡顿

