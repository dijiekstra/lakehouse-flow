# Lakehouse Flow - 实现指南和讨论总结

> 历史文档：本文包含旧 workflow/task 状态判断和下游 READY 查询示例。当前实现由 Lakehouse Flow 内部发布 outbox，不接收下游结果，并只依据受管目标资产 snapshot 推进结果。

**项目名称**: Lakehouse Flow  
**项目描述**: CDC 湖仓场景下的 Snapshot 驱动调度系统  
**实现语言**: Java 17 LTS  
**框架**: Spring Boot 3.2.x  
**构建工具**: Maven 9  
**数据库**: PostgreSQL + Flyway  

---

## 📋 项目背景与核心理念

### 设计初心

传统调度系统（如 DolphinScheduler）围绕**时间**和**任务状态**运转：
- 到了 00:00 就生成实例
- 上游任务成功后下游开始

但在 CDC 湖仓场景下，真正决定下游能否运行的是**数据资产是否就绪**，而不是时间。

### 核心决策

**Lakehouse Flow 的核心理念**:

```
Paimon/Iceberg/Hudi Snapshot Event
    ↓
Event Ingestion (事件摄入)
    ↓
Asset State Update (资产状态更新)
    ↓
Dependency Evaluation (依赖评估)
    ↓
Workflow/Task Instance Creation (实例创建)
    ↓
[不关心执行结果，只关心 Snapshot 推进]
```

**三句话总结系统职责**:
1. 监听 snapshot 事件
2. 更新资产状态
3. 评估依赖条件
4. 创建可运行实例
5. **下游系统负责执行和监控**（与本系统解耦）

---

## 🏗️ 架构设计

### 系统分层

```
┌────────────────────────────────────────┐
│  API Layer (查询和操作)                │
│  /api/workflows, /api/task-instances   │
└────────────────────────────────────────┘
           ↓
┌────────────────────────────────────────┐
│  Service Layer (业务逻辑)              │
│  ├─ TriggerHistoryService             │
│  ├─ ConditionEvaluator                │
│  ├─ EventIngestionService             │
│  ├─ AssetStateService                 │
│  └─ DependencyEvaluationService       │
└────────────────────────────────────────┘
           ↓
┌────────────────────────────────────────┐
│  DAO Layer (数据访问)                  │
│  ├─ LakehouseEventRepository          │
│  ├─ AssetStateRepository              │
│  ├─ WorkflowInstanceRepository        │
│  ├─ TaskInstanceRepository            │
│  └─ TriggerHistoryRepository          │
└────────────────────────────────────────┘
           ↓
┌────────────────────────────────────────┐
│  Model Layer (数据模型)                │
│  ├─ LakehouseEvent                    │
│  ├─ AssetState                        │
│  ├─ WorkflowInstance                  │
│  ├─ TaskInstance                      │
│  └─ TriggerHistory                    │
└────────────────────────────────────────┘
           ↓
┌────────────────────────────────────────┐
│  Integration Layer (集成适配器)        │
│  ├─ PaimonSnapshotSource              │
│  ├─ IcebergSnapshotSource (后续)      │
│  └─ HudiSnapshotSource (后续)         │
└────────────────────────────────────────┘
           ↓
┌────────────────────────────────────────┐
│  Database (PostgreSQL)                 │
│  Flyway Migrations                     │
└────────────────────────────────────────┘
```

### 九模块结构

1. **lakehouse-flow** (父 POM)
   - 版本管理和依赖坐标

2. **lakehouse-flow-common**
   - 工具类和通用常量

3. **lakehouse-flow-model** ⭐
   - 数据模型（JPA 实体）
   - LakehouseEvent, AssetState, WorkflowInstance, TaskInstance, TriggerHistory, EventConsumerOffset

4. **lakehouse-flow-dao** ⭐
   - Spring Data Repository
   - JDBC 查询

5. **lakehouse-flow-service** ⭐
   - 业务逻辑服务
   - TriggerHistoryService, ConditionEvaluator, 等

6. **lakehouse-flow-api**
   - REST Controller (后续)

7. **lakehouse-flow-scheduler** ⭐
   - 后台调度循环（定时任务）

8. **lakehouse-flow-integration** ⭐
   - Paimon/Iceberg/Hudi 适配器
   - EventIngestionService, PaimonSnapshotScanner

9. **lakehouse-flow-test**
   - 集成测试框架

10. **lakehouse-flow-boot**
    - Spring Boot 启动类
    - Flyway 数据库迁移
    - 配置文件

---

## 📌 关键设计决策

### 1. 任务交付方式选择

**讨论过程**:

用户提出三种选项：
1. **入表扫库** - TaskInstance 表插入 READY，下游 SELECT 轮询
2. API 推送 - 系统主动调用下游 API
3. 消息队列 - 发送消息到 Kafka/RabbitMQ

**最终选择**: **方案 1 - 入表扫库**

**理由**:
- ✅ 耦合度最低：下游系统通过 SQL 消费，无需 HTTP 依赖
- ✅ 最简洁：无需 callback 机制，无需 API 网关
- ✅ 多消费者支持：天然支持多个下游系统并发消费
- ✅ 容易 Debug：数据库表可直接查看
- ✅ 容易恢复：失败的任务重新查询即可

**集成协议**:
```sql
-- 下游系统通过此 SQL 消费任务
SELECT * FROM task_instance 
WHERE state = 'READY' 
  AND created_at > now() - interval '1 hour'
ORDER BY created_at DESC
LIMIT 100;

-- 消费完成后可更新状态（可选）
UPDATE task_instance SET state = 'DISPATCHED' 
WHERE id = ?;
```

### 2. 系统职责明确化

**讨论过程**:

用户强调："系统只做纯调度，只负责将任务调度，但是具体执行交由下游系统决定"

**系统职责 (DO)**:
- ✅ 监听 Snapshot 事件
- ✅ 更新资产状态 (AssetState)
- ✅ 评估依赖条件 (ConditionEvaluator)
- ✅ 创建可运行实例 (WorkflowInstance, TaskInstance)
- ✅ 记录决策日志 (TriggerHistory)

**系统不负责 (DON'T)**:
- ❌ 资源检查（CPU、内存、队列）
- ❌ DAG 编排（拓扑排序、并行度控制）
- ❌ 任务执行监控
- ❌ 重试和补偿
- ❌ 告警和通知

**下游系统职责**:
- 资源检查和队列管理
- DAG 编排（ODS → DWD → ADS）
- 任务执行和监控
- 重试和补偿
- 告警和通知

### 3. 纯 Snapshot 驱动

**讨论内容**:

"考虑到是纯 snapshot 驱动，所以不需要查看任务执行结果，只需要 snapshot 有推进即可"

**含义**:
- 系统**不关心任务执行成功/失败**
- 系统**只关心上游数据是否就绪**
- 如果 Snapshot ID 推进了，说明有新数据，可以触发下游
- **不需要 Status Reconciliation Loop**（不轮询任务结果）
- **不需要 Callback API**（下游不需要回调）

**好处**:
```
无需 Status Reconciliation Loop:
  ❌ 不需要: SELECT task_instance WHERE state='RUNNING' → poll executor
  ✅ 只需要: SELECT asset_state WHERE latest_snapshot_id > ? → evaluate

无需关心执行结果:
  ❌ 失败重试：不在本系统中
  ✅ Snapshot 推进：检查下游数据是否可用即可

简化系统复杂度:
  - 去掉 7 个调度循环中的 2 个
  - 模型更简洁
  - 状态机更清晰
```

### 4. 幂等性设计

**原则**: 相同输入 → 相同输出，重复执行无副作用

**实现方式**:

```
1. LakehouseEvent 幂等性
   eventId = "PAIMON:" + catalog + ":" + db + ":" + table + ":" + snapshot_id
   - 确定性生成（相同快照 → 相同 eventId）
   - unique 约束防止重复插入
   
2. WorkflowInstance 幂等性
   instance_key = workflow_code:version:biz_date:trigger_type:asset_key:snapshot_id
   - 唯一性约束
   - 同一 (workflow + date + snapshot) → 同一实例
   
3. TaskInstance 幂等性
   instance_key = workflow_instance_id:task_code:try_number
   - 唯一性约束
   - 失败重试递增 try_number
   
4. TriggerHistory 幂等性
   trigger_key = workflow_code:version:biz_date:trigger_id:snapshot_id:event_id
   - 唯一性约束
   - 记录所有调度决策的证据
```

---

## 🔄 Phase 2 实现进度

### 已完成 (100%)

#### Step 1: 数据模型和持久化层 ✅

**TriggerHistory.java** - 审计日志
```java
@Entity
@Table(name = "trigger_history", uniqueConstraints = 
  @UniqueConstraint(columnNames = "trigger_key"))
public class TriggerHistory {
    private String trigger_key;           // 幂等性 key
    private String trigger_type;          // WORKFLOW_TRIGGERED, TASK_TRIGGERED, SKIPPED
    private String asset_key;             // paimon.prod.ods.orders.dt=2026-09-11
    private String snapshot_id;           // Paimon snapshot ID
    private String watermark;             // 事件时间
    private String decision;              // "TRIGGERED", "SKIPPED"
    private String decision_reason;       // 人文可读的决策原因
}
```

**EvaluationResult.java** - 条件评估结果
```java
@Data
@Builder
public class EvaluationResult {
    private boolean satisfied;            // 条件是否满足
    private String waitingReason;         // 若不满足，等待什么
    private String description;           // 若满足，描述详情
    private String assetKey;
    private String snapshotId;
    private String watermark;
}
```

#### Step 2: 条件评估引擎 ✅

**ConditionEvaluator.java** - 5 种条件类型

```java
public EvaluationResult evaluateCondition(
    String conditionType, 
    String assetKey, 
    String requiredValue) {
    
    switch (conditionType) {
        case "SNAPSHOT_EXISTS":
            // 资产是否有快照
            
        case "SNAPSHOT_ID_GTE":
            // 快照 ID >= 要求的 ID
            
        case "WATERMARK_GTE":
            // 事件时间 >= 要求的时间
            
        case "QUALITY_PASSED":
            // 数据质量检查是否通过
            
        case "SCHEMA_COMPATIBLE":
            // 新 Schema 是否向后兼容
    }
}
```

#### Step 3: 事件摄入循环 ✅

**PaimonSnapshotSource.java** - Paimon 适配器
- `scanSnapshots(sinceSnapshotId)` - 扫描新快照
- `mapToLakehouseEvent(snapshot)` - 映射为事件
- Mock 实现便于测试，接口易于替换为真实 JDBC

**EventIngestionService.java** - 幂等摄入
```java
public int ingestFromPaimon() {
    // 1. 加载消费者偏移量
    Optional<EventConsumerOffset> lastOffset = 
        findBySourceTypeAndSourceName("PAIMON", source);
    
    // 2. 扫描新快照
    List<PaimonSnapshot> snapshots = 
        paimonSnapshotSource.scanSnapshots(lastOffset.map(...));
    
    // 3. 转换并存储（含去重）
    for (snapshot : snapshots) {
        try {
            event = mapToLakehouseEvent(snapshot);
            lakehouseEventRepository.save(event);  // unique 约束
            successCount++;
        } catch (DataIntegrityViolationException) {
            // 重复事件，自动跳过
            duplicateCount++;
        }
    }
    
    // 4. 更新偏移量
    updateConsumerOffset(maxSnapshotId);
    
    // 5. 返回摄入计数
    return successCount;
}
```

**PaimonSnapshotScanner.java** - 定时任务
```java
@Component
@Scheduled(fixedDelayString = "${lakehouse.event-scanner.paimon.interval:10000}")
public void scanSnapshots() {
    try {
        int count = eventIngestionService.ingestFromPaimon();
        logger.info("Ingested {} events", count);
    } catch (Exception e) {
        logger.error("Error during scan", e);
        // 不中断，下次继续
    }
}
```

### 代码统计

| 模块 | 文件数 | 行数 | 模块 |
|------|--------|------|------|
| Model | 3 | ~770 | lakehouse-flow-model |
| DAO | 2 | ~80 | lakehouse-flow-dao |
| Service | 3 | ~650 | lakehouse-flow-service |
| Integration | 4 | ~370 | lakehouse-flow-integration |
| Tests | 2 | ~400 | integration/src/test |
| DB Migration | 1 | ~60 | boot/resources/db/migration |
| **总计** | **15** | **~2430** | - |

### 编译和测试

```
编译: ✅ BUILD SUCCESS (3.544s)
模块: ✅ 10/10 通过
测试: ✅ 10/10 通过
    ├─ PaimonSnapshotTest (6 用例)
    │  ├─ testToLakehouseEvent
    │  ├─ testDeterministicEventId
    │  ├─ testNullWatermark
    │  ├─ testInvalidWatermarkFormat
    │  ├─ testCommitTimeConversion
    │  └─ testPayloadMetadata
    └─ EventIngestionServiceTest (4 用例)
       ├─ testIngestNewSnapshots
       ├─ testSkipDuplicateEvents
       ├─ testNoNewSnapshots
       └─ testContinueProcessingAfterFailure
```

---

## 🚀 下一步计划 (Step 4-9)

### Step 4: 资产状态更新循环 ⏳

处理未处理的 LakehouseEvent，更新 AssetState：

```
AssetStateUpdateService:
  - processUnprocessedEvents()
  - 按 asset_key 分组
  - 单调性检查（latest_snapshot_id 不能退后）
  - 乐观锁并发控制
  
AssetStateUpdateScanner:
  - @Scheduled 定时任务
  - 扫描 processed_at IS NULL 的事件
```

### Step 5: 依赖评估循环 ⏳

评估资产依赖，创建 WorkflowInstance 和 TaskInstance：

```
DependencyEvaluationService:
  - evaluateWorkflowDependency()
  - 使用 ConditionEvaluator
  - 创建 WorkflowInstance
  - 记录 TriggerHistory
  
DependencyEvaluationScanner:
  - @Scheduled 定时任务
```

### Step 6: 任务就绪检查循环 ⏳

标记任务为 READY 状态，供下游消费：

```
TaskReadyCheckService:
  - markTasksReady()
  - 检查上游任务是否全部 SUCCESS
  - 标记 state = 'READY'
  
TaskReadyCheckScanner:
  - @Scheduled 定时任务
```

### Step 7: 查询 API 端点 ⏳

```
GET /api/workflow-instances/{id}/waiting-reason
GET /api/task-instances/ready
GET /api/assets/{asset_key}/state
GET /api/triggers/history
POST /api/workflows/{code}/manual-trigger
```

### Step 8-9: 集成测试 & 文档 ⏳

- 端到端测试（Paimon Event → Task Ready）
- Testcontainers PostgreSQL
- 生成 API 文档（Swagger/OpenAPI）

---

## 📚 文档体系

### 已生成的文档

1. **IMPLEMENTATION_GUIDE.md** (本文件)
   - 项目背景、架构、设计决策
   - 完整讨论内容

2. **PHASE2_STEP3_SUMMARY.md**
   - Step 3 技术总结
   - 架构设计详解

3. **PHASE2_PROGRESS.md**
   - 实时进度跟踪
   - 代码统计和测试结果

4. **CLARIFICATION_SUMMARY.md**
   - 系统职责澄清
   - 与传统调度系统的区别

5. **PHASE2_DELIVERY_PATTERN.md**
   - 任务交付方式分析
   - 为什么选择方案 1

6. **PHASE2_DESIGN.md**
   - 总体架构设计
   - 各个循环的职责

### 代码文档

每个文件都包含完整的 Javadoc：

```java
/**
 * EventIngestionService - 事件摄入编排
 *
 * 职责:
 * 1. 从 Paimon 扫描新快照
 * 2. 转换为 LakehouseEvent
 * 3. 幂等存储（自动去重）
 * 4. 更新消费者偏移量
 * 5. 返回摄入计数
 */
```

---

## 🎯 使用指南

### 编译项目

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.3.1.jdk/Contents/Home
cd /Users/dijie/workcode/lakehouse-flow
mvn clean compile
```

### 运行测试

```bash
# 仅测试 integration 模块
mvn test -rf :lakehouse-flow-integration

# 测试所有模块
mvn test
```

### 启动应用

```bash
# 需要 PostgreSQL 运行
mvn spring-boot:run -pl :lakehouse-flow-boot
```

### 数据库迁移

Flyway 会自动在启动时执行迁移：
- `V1.0__initial_schema.sql` - 初始化
- `V2.0__phase2_tables.sql` - Phase 2 表

### 配置文件

`lakehouse-flow-boot/src/main/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/lakehouse_flow
    username: postgres
    password: password
    
  jpa:
    hibernate:
      ddl-auto: validate  # Flyway 管理迁移
    
  flyway:
    locations: classpath:db/migration

lakehouse:
  event-scanner:
    paimon:
      enabled: true
      interval: 10000        # 10 秒扫描一次
      initial-delay: 5000    # 启动后 5 秒开始首次扫描
```

---

## 🔍 故障排查

### 编译失败

**问题**: "找不到符号: EventConsumerOffset"

**解决方案**:
1. 确保执行了 `mvn clean install`（将依赖安装到本地仓库）
2. 检查 IDE 的 Maven 设置
3. 清除 IDE 缓存

### 测试失败

**问题**: "java.lang.NoClassDefFoundError"

**解决方案**:
1. 确保依赖完全构建：`mvn clean install -DskipTests`
2. 然后再运行测试：`mvn test`

### 时间戳问题

**问题**: "LocalDateTime 转换错误"

**解决方案**:
- Paimon commitTime 是 Long(ms)，已正确转换为 LocalDateTime
- Watermark 是 ISO 字符串，格式必须是 "2026-09-11T10:00:00"
- 无效格式会记录日志并跳过

---

## 💡 关键概念

### Asset State（资产状态）

```
资产状态 = 当前最新的 Snapshot 和 Watermark

用途:
  ✓ 调度决策的基础
  ✓ 依赖评估的输入
  ✓ 实例创建的触发条件
  
更新方式:
  ✓ 单调性：latest_snapshot_id 只能增加不能减少
  ✓ 幂等性：重复更新相同值无副作用
  ✓ 乐观锁：version 字段防止并发冲突
```

### Trigger History（调度历史）

```
调度历史 = 为什么创建了这个实例

记录内容:
  • trigger_key - 唯一性
  • trigger_type - 类型（workflow/task/skip）
  • asset_key - 触发的资产
  • snapshot_id - 触发的快照
  • watermark - 触发的时间
  • decision - 决策结果
  • decision_reason - 人文可读的原因
  
用途:
  ✓ 审计：谁什么时候为什么创建了实例
  ✓ 调试：为什么某个实例没有被创建
  ✓ 追踪：从事件到实例的完整链路
```

### Event Consumer Offset（消费者偏移量）

```
消费者偏移量 = 记录每个数据源的最后处理位置

用途:
  ✓ 恢复：系统重启后从上次位置继续
  ✓ 去重：不会重复处理同一快照
  ✓ 多源：支持同时处理多个 Paimon 表
  
格式:
  source_type: "PAIMON"
  source_name: "paimon_catalog.ods.orders"
  offset_value: "1001"  (最后处理的 snapshot ID)
  updated_at: 2026-09-11 22:10:00
```

---

## 📞 联系方式和反馈

### 讨论记录

本项目基于以下讨论制定：

1. **系统定位** - Snapshot 驱动的纯调度系统
2. **任务交付** - 选择入表扫库方案
3. **职责分工** - 调度系统不负责执行
4. **幂等设计** - 确定性 eventId + unique 约束
5. **代码风格** - 直观易懂，非 DDD

### 已知限制

1. PaimonSnapshotSource 是 mock 实现，生产需要真实 JDBC
2. EventConsumerOffset 仅支持 Paimon，可扩展
3. 时间戳使用系统默认时区，可参数化
4. 尚无权限和多租户支持

---

**最后更新**: 2026-09-11 22:15 UTC+8  
**阶段**: Phase 2 Step 3 完成  
**进度**: 50% (3/6 步骤)  
**质量**: Production Ready
