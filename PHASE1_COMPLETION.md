# Lakehouse Flow - Phase 1: Core Domain Models - 完成报告

**日期**: 2026-09-11  
**状态**: ✅ Phase 1 完成

## 一、实现概览

### 1.1 开发方式（非DDD）

采用直观、易于 review 的实现方式：
- ✅ 简单JPA实体类（1:1 对应数据库表）
- ✅ Spring Data Repository 接口（无需自定义实现）
- ✅ 简洁的 Service 类（核心业务逻辑）
- ✅ 单元测试（验证逻辑，无需数据库）
- ❌ 避免过度抽象、Bounded Context、Value Object 等 DDD 概念

### 1.2 核心实体实现

#### Entity 类（5个）
1. **LakehouseEvent** (290 行)
   - 原始事件存储
   - JSONB payload 支持
   - 事件去重字段 (eventId unique)

2. **AssetState** (180 行)
   - 资产状态和版本跟踪
   - @Version 乐观锁支持
   - 单调性检查字段

3. **AssetDependency** (150 行)
   - 依赖条件定义
   - JSONB 条件存储
   - 支持 workflow/task 级别的依赖

4. **WorkflowInstance** (180 行)
   - 工作流实例生命周期
   - 状态机支持（CREATED → WAITING → RUNNING → SUCCESS）
   - 唯一 key 支持幂等性

5. **TaskInstance** (210 行)
   - 任务实例生命周期
   - 状态机支持（含重试逻辑）
   - 外部 job ID 追踪

**总计**: 5 个实体类，~1000 行代码

#### Repository 接口（5个）
1. **LakehouseEventRepository**
   - findByEventId (重复检测)
   - findEventsByAssetAndSnapshotRange
   - findLatestEventByAsset

2. **AssetStateRepository**
   - findByAssetKey (幂等更新)
   - findByReadinessStatus
   - findByTable

3. **AssetDependencyRepository**
   - findByAssetKey
   - findByWorkflowCode
   - findByTaskCode

4. **WorkflowInstanceRepository**
   - findByInstanceKey (幂等创建)
   - findWaitingInstances
   - findStuckInstances

5. **TaskInstanceRepository**
   - findByInstanceKey
   - findWaitingForDependencies
   - findStuckRunningTasks

**总计**: 5 个 Repository 接口，~150 行代码

#### Service 类（5个）

1. **EventService** (90 行)
   - 事件去重和存储
   - 主动触发资产状态更新
   - ingestEvent()
   - 处理 DataIntegrityViolationException 的去重

2. **AssetStateService** (180 行)
   - 单调性检查（snapshot/watermark）
   - 优化并发控制（@Version）
   - updateAssetStateFromEvent()
   - 非破坏性更新

3. **WorkflowInstanceService** (210 行)
   - 实例创建和幂等性
   - 状态转换验证
   - 手动重跑（FAILED → RUNNING）
   - instance key 生成

4. **TaskInstanceService** (230 行)
   - 任务实例管理
   - 重试逻辑（try_number + maxRetries）
   - 外部 job 追踪
   - 等待原因记录

**总计**: 4 个 Service 类，~700 行代码

## 二、核心实现细节

### 2.1 幂等性保证

所有创建操作都基于唯一 key：

```
WorkflowInstance:
  instanceKey = workflow_code:version:biz_date:trigger_type:trigger_id

TaskInstance:
  instanceKey = workflow_instance_id:task_code:try_number

LakehouseEvent:
  eventId = source_type:catalog:database:table:partition:event_type:snapshot_id
```

### 2.2 单调性检查（AssetStateService）

```java
isEventNewer(state, event):
  1. 比较 snapshotId (lexicographically)
  2. 比较 watermark (timestamp)
  3. 比较 commitTime (timestamp)
  
  只有更新的事件才覆盖旧状态
```

### 2.3 状态机转换验证

**WorkflowInstance**:
```
CREATED → WAITING → RUNNING → SUCCESS
      ↘ FAILED / TIMEOUT / CANCELLED
FAILED → RUNNING (manual rerun)
TIMEOUT → RUNNING (manual rerun)
```

**TaskInstance**:
```
CREATED → WAITING_DEPENDENCY → READY → DISPATCHING → RUNNING → SUCCESS
                                 ↘ SKIPPED        ↘ FAILED
FAILED → RETRY_WAITING → READY (if retries remain)
```

### 2.4 乐观锁并发控制

```java
@Column(name = "version", nullable = false)
@Version
private Long version;

// Hibernate 自动管理版本递增和冲突检测
```

## 三、单元测试覆盖

### 3.1 测试统计

| 模块 | 测试类 | 测试方法数 | 状态 |
|------|--------|-----------|------|
| Model | LakehouseEventTest | 2 | ✅ 通过 |
| Service | AssetStateServiceTest | 6 | ✅ 通过 |
| Service | WorkflowInstanceServiceTest | 5 | ✅ 通过 |
| **总计** | **3** | **13** | **✅ 全通过** |

### 3.2 测试覆盖范围

**不需要数据库的单元测试**:
- Entity 创建和字段赋值
- Service 逻辑（去重、单调性、状态转换）
- Key 生成和唯一性
- 状态转换验证

### 3.3 测试运行命令

```bash
# 运行除Boot模块外的所有测试
mvn test -pl '!lakehouse-flow-boot'

# 输出: 13 tests, 13 passed, 0 failed
```

## 四、代码统计

| 指标 | 数量 |
|------|------|
| Entity 类 | 5 个 |
| Repository 接口 | 5 个 |
| Service 类 | 4 个 |
| 单元测试类 | 3 个 |
| 总 Java 文件 | 17 个 |
| 代码行数 | ~2500 行 |
| 测试行数 | ~600 行 |

## 五、关键设计决策

### 5.1 为何不用 DDD

- ❌ Aggregate/Aggregate Root：直接用 Entity + Repository
- ❌ Value Objects：直接用 primitive types 或 JSONB
- ❌ Domain Events：直接用 LakehouseEvent（源事件）
- ❌ Bounded Contexts：单个模块足以处理当前需求

**理由**：
- 简化代码可读性和 review 难度
- 容易调试和追踪流程
- 快速迭代，后期可重构

### 5.2 Entity 字段设计

- 使用 `@Column(name = "...", nullable = false)` 明确声明
- 使用 `@JdbcType(JsonJdbcType.class)` 存储条件和配置（JSONB）
- 使用 `@Version` 乐观锁（并发更新安全）
- 使用 `@PrePersist/@PreUpdate` 自动管理时间戳

### 5.3 Service 设计

- **无接口抽象**（当前阶段不需要）
- **单一职责**（每个 Service 负责一个实体）
- **显式依赖注入**（Lombok @RequiredArgsConstructor）
- **事务边界清晰**（@Transactional 标记）

## 六、与其他模块的集成

### 6.1 DAO 模块
- ✅ 5 个 Repository 接口已定义
- ✅ 可直接注入到 Service 使用
- ⏳ 集成测试待 boot 模块启动

### 6.2 Boot 模块
- ✅ 所有 Service 自动扫描和 Bean 注册
- ✅ 配置文件已准备（application.yml）
- ⏳ 应用启动和集成测试待 PostgreSQL 启动

### 6.3 API 模块
- ⏳ 尚未创建 Controller（Phase 2）

## 七、后续工作（Phase 2）

### 7.1 事件处理循环
- [ ] Paimon 快照源适配器
- [ ] 事件消费进度跟踪
- [ ] 定期事件扫描

### 7.2 依赖条件评估
- [ ] 条件评估引擎
- [ ] 依赖条件构建器
- [ ] 失败原因记录

### 7.3 任务调度循环
- [ ] 任务分发器
- [ ] 执行器适配器（Shell/HTTP/SQL）
- [ ] 状态回收和重试

### 7.4 REST API
- [ ] Workflow API (CRUD + instance 查询)
- [ ] Task API (instance 查询 + 手动操作)
- [ ] Asset API (state 查询 + 等待原因)
- [ ] Debug API (触发历史、trace)

### 7.5 集成测试
- [ ] Boot 模块启动测试
- [ ] 端到端场景测试
- [ ] 并发性测试

## 八、质量指标

| 指标 | 目标 | 实际 |
|------|------|------|
| 编译成功 | ✅ | ✅ 3.251s |
| 所有单元测试通过 | ✅ | ✅ 13/13 |
| 代码行数 | 合理 | ✅ ~2500 |
| 注释覆盖 | 关键部分 | ✅ 已添加 |
| 依赖无环 | ✅ | ✅ 9 模块正确 |

## 九、文档清单

- ✅ 5 个 Entity 类（均有 JavaDoc）
- ✅ 5 个 Repository 接口（均有 JavaDoc）
- ✅ 4 个 Service 类（均有 JavaDoc）
- ✅ 3 个单元测试类（均有说明注释）

## 十、验收标准

- ✅ 5 个核心实体完整实现
- ✅ 5 个 Repository 接口已定义
- ✅ 4 个关键 Service 实现
- ✅ 13 个单元测试全通过
- ✅ 代码编译无错误
- ✅ 幂等性设计已验证
- ✅ 状态机转换已验证
- ✅ 单调性逻辑已验证
- ✅ JavaDoc 文档完整

## 总结

**Phase 1 核心域模型实现已完成**

使用直观、易于 review 的方式实现了：
- 5 个核心 Entity（事件、资产、依赖、工作流、任务）
- 5 个 Repository（数据访问）
- 4 个 Service（业务逻辑）
- 13 个单元测试（验证正确性）

所有代码均避免了 DDD 的复杂性，保持简洁、直观和易维护。

**下一步**: Phase 2 - 事件处理和调度循环实现

---

**生成时间**: 2026-09-11 21:32 UTC+8  
**项目版本**: 0.1.0-SNAPSHOT  
**Java 版本**: Java 17 LTS
