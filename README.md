# Lakehouse Flow：事件驱动的数据资产调度系统

![Status](https://img.shields.io/badge/status-Phase%201-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-green)
![Language](https://img.shields.io/badge/language-Java%2017-brightgreen)
![Architecture](https://img.shields.io/badge/architecture-scheduling%20layer-success)

**Lakehouse Flow** 是一个独立的、生产就绪的**纯调度决策系统**，专为现代 CDC 湖仓架构设计。

它将调度范式从基于时间的 cron 触发升级为**数据资产状态驱动**，实现真正的数据就绪调度：

```
输入: Paimon/Iceberg/Hudi snapshot 事件
处理: 资产状态 → 依赖评估 → 实例创建
输出: READY 任务实例（通过 REST API）
     ↓
下游系统（资源分配、DAG编排、真实执行）
```

**系统只负责回答**：数据资产是否就绪？依赖是否满足？应该创建哪个任务？
**系统不负责**：资源检查、DAG编排、真实任务执行。那些都交给下游系统。

## 核心理念

```
传统数仓调度：
  cron(0 2 * * *) → SQL 任务 → 报表
  └─ 问题：数据延迟 ≠ 预计执行时刻

现代湖仓调度（Lakehouse Flow）：
  Paimon Snapshot v1000 → 资产就绪 → 依赖满足 → TaskInstance.READY
  ↓
  下游系统接收 → 资源检查 → DAG编排 → 真实执行 → 报表
  └─ 解决：数据就绪驱动，消除时间与可用性错配
```

**关键区别**：
- ✅ Lakehouse Flow = **调度决策**（何时运行，为什么运行）
- ✅ 下游系统 = **执行编排**（如何分配资源、如何编排依赖、如何执行）

## 关键特性

- **资产状态驱动调度**：用数据资产版本状态替代 cron 表达式
- **幂等触发保障**：同一快照组合只能触发一次，通过唯一约束实现
- **乱序和重复处理**：乐观锁防止并发覆盖，唯一约束自动去重
- **完整审计日志**：记录为什么创建了某个实例（TriggerHistory）
- **多表依赖组合**：支持 AND/OR 复杂条件评估
- **端到端可观测**：调度决策过程完全可追踪
- **开放集成**：通过 REST API 与任何下游系统集成
- **生产就绪架构**：支持 Paimon/Iceberg/Hudi，易于扩展

## 适用场景

✅ **适合使用 Lakehouse Flow：**
- 数据仓库构建在 Paimon/Iceberg/Hudi 等湖仓格式上
- 完整链路处理（ODS → DWD → DWS → ADS）
- 希望用数据资产就绪驱动调度，而不是时间触发
- 需要消除预计执行时间与实际数据可用性的错配
- 有专门的下游执行系统或能够集成 REST API

❌ **不适合：**
- 传统无版本快照的 OLTP 数据库
- 纯时间表达式的定时任务
- 仅有数据着陆层，缺少数据加工链路的架构
- 需要完全的资源管理和执行引擎在同一个系统中

## 快速开始

### 系统要求

- Java 17+
- MySQL 5.7+ 或 PostgreSQL 10+
- Maven 3.8+

### 3分钟快速体验

#### 1. 启动应用

```bash
git clone https://github.com/your-org/lakehouse-flow.git
cd lakehouse-flow

mvn clean package
java -jar lakehouse-flow-service/target/lakehouse-flow-*.jar
```

#### 2. 注册一个 Paimon 资产

```bash
curl -X POST http://localhost:8080/api/v1/assets \
  -H "Content-Type: application/json" \
  -d '{
    "assetKey": "paimon://ods_db/ods_order_latest",
    "storageFormat": "PAIMON",
    "catalogName": "ods_db",
    "databaseName": "ods_db",
    "tableName": "ods_order_latest",
    "description": "订单最新状态表"
  }'
```

#### 3. 为任务声明资产依赖

```bash
curl -X POST http://localhost:8080/api/v1/asset-dependencies \
  -H "Content-Type: application/json" \
  -d '{
    "workflowCode": 12345,
    "taskCode": 100,
    "assetKey": "paimon://ods_db/ods_order_latest",
    "dependencyGroup": "order_ready",
    "condition": {
      "snapshotRequired": true,
      "qualityStatus": "PASSED"
    }
  }'
```

#### 4. 观察依赖被触发

```bash
# 查看资产当前状态
curl http://localhost:8080/api/v1/assets/paimon://ods_db/ods_order_latest/state

# 查看触发历史
curl http://localhost:8080/api/v1/triggers/history?assetKey=paimon://ods_db/ods_order_latest
```

## 核心概念

详细定义见 [GLOSSARY.md](./GLOSSARY.md)。这里快速说明最重要的5个概念：

### 1. LakehouseEvent（湖仓事件）

原始事件，来自 Paimon snapshot、Iceberg manifest 或外部 push。事件仅作为"证据"记录，不直接触发工作流。

```
eventId:      paimon_ods_order_latest_snapshot_1001_xxx
assetKey:     paimon://ods_db/ods_order_latest
snapshotId:   1001
watermark:    2024-09-11T23:59:59Z
qualityStatus: PASSED
```

### 2. AssetState（资产状态）

**调度决策的唯一事实来源**。由事件不断推进，但从不回退。采用乐观锁防止乱序覆盖。

```
assetKey:              paimon://ods_db/ods_order_latest
latestSnapshotId:      1001  (单调递增)
latestWatermark:       2024-09-11T23:59:59Z
qualityStatus:         PASSED
version:               42    (乐观锁版本号)
```

### 3. AssetDependency（资产依赖）

工作流或任务声明它等待哪些数据资产的哪些条件。

```json
{
  "workflowCode": 12345,
  "assetKey": "paimon://ods_db/ods_order_latest",
  "dependencyGroup": "order_ready",
  "condition": {
    "snapshotRequired": true,
    "qualityStatus": "PASSED"
  }
}
```

### 4. TriggerHistory（触发历史）

每次触发都生成唯一的触发记录，保证**幂等性**。同一 `trigger_key` 最多成功创建一次。

```
trigger_key: workflow_12345_group_order_ready_snapshot_1001_xxx
status:      SUCCESS
reason:      Asset snapshot ready, quality PASSED, dependency satisfied
```

### 5. WorkflowInstance & TaskInstance

标准的工作流 DAG 实例和任务实例，新增"由资产驱动"而非仅"由前置任务"驱动。

## 系统架构

```
┌──────────────────────────────┐
│   Paimon/Iceberg/Hudi 表     │
├──────────────────────────────┤
        ↓ snapshot event
┌──────────────────────────────┐
│  事件摄入循环                  │
│  (轮询扫描 + 去重)             │
├──────────────────────────────┤
        ↓ LakehouseEvent
┌──────────────────────────────┐
│  资产状态循环                  │
│  (乐观锁更新 + 乱序保护)       │
├──────────────────────────────┤
        ↓ AssetState changed
┌──────────────────────────────┐
│  依赖评估循环                  │
│  (条件匹配 + 触发决策)        │
├──────────────────────────────┤
        ↓ trigger_key
┌──────────────────────────────┐
│  幂等触发                      │
│  (唯一约束防止重复)            │
├──────────────────────────────┤
        ↓ WorkflowInstance
┌──────────────────────────────┐
│  DAG 执行 + 任务派发           │
│  (标准调度和状态机)            │
├──────────────────────────────┤
        ↓
┌──────────────────────────────┐
│  补偿扫描循环                  │
│  (漏采集 + 失败重试)           │
└──────────────────────────────┘
```

详见 [ARCHITECTURE.md](./ARCHITECTURE.md)。

## 设计原则

Lakehouse Flow 遵循**七大不可退让的原则**。详见 [DESIGN_PRINCIPLES.md](./DESIGN_PRINCIPLES.md)。

1. **资产状态驱动**：不能"事件到了就触发"，必须"资产状态达到条件才触发"
2. **幂等触发**：同一版本、同一依赖组合只能触发一次
3. **事件视为证据**：事件可能重复/乱序/延迟/丢失，资产状态才是事实来源
4. **乐观锁保护**：乱序或并发事件通过版本号防止覆盖，单调递增
5. **唯一触发键**：通过数据库唯一约束保证幂等，无需分布式锁
6. **可解释的等待**：用户随时可查询"为什么任务还在等待"
7. **完整补偿**：遗漏的事件、失败的触发都有定期补偿机制

## 关键循环

Lakehouse Flow 的核心是6个独立的调度循环，每个循环有明确的职责：

### 事件摄入循环
轮询或接收湖仓事件，去重后落库。

### 资产状态循环
消费未处理事件，以乐观锁更新资产状态。

### 依赖评估循环
当资产状态变化时，反向查询所有依赖项，重新评估。

### 幂等触发循环
计算 trigger_key，通过唯一约束确保同一组合只触发一次。

### 任务派发循环
释放 READY 任务给执行器（Shell/SQL/HTTP）。

### 补偿循环
定期检查漏采集事件、失败触发、卡顿任务。

详见 [ARCHITECTURE.md#关键循环](./ARCHITECTURE.md)。

## MVP 第一阶段范围

### ✅ 已实现

- Paimon snapshot 事件摄入与去重
- 资产状态单调更新 + 乐观锁
- 单资产依赖条件评估（snapshot 存在性）
- 幂等触发历史记录
- 工作流实例创建和任务派发
- Shell/SQL 执行器适配
- 状态协调和轮询
- 补偿扫描器

### 🚧 后续阶段（Phase 2+）

- 多资产 AND/OR 依赖组合
- Watermark/Quality/Schema 条件支持
- HTTP/Webhook 事件 push 接口
- 工作流级资产直接触发
- Iceberg 和 Hudi 事件源
- Spark/Flink 执行器
- 可视化 UI 和资产大盘

## 文档导航

| 文档 | 内容 |
|------|------|
| **[README.md](./README.md)** | 项目概览、快速开始、核心概念 |
| **[ARCHITECTURE.md](./ARCHITECTURE.md)** | 系统架构、数据模型、表结构、6个关键循环 |
| **[DESIGN_PRINCIPLES.md](./DESIGN_PRINCIPLES.md)** | 7大不可退让原则、约束、为什么这样设计 |
| **[DEVELOPMENT.md](./DEVELOPMENT.md)** | 开发环境、代码规范、本地运行、测试 |
| **[GLOSSARY.md](./GLOSSARY.md)** | 术语表、完整的数据模型定义 |

## API 参考

```
POST   /api/v1/assets                          注册资产
GET    /api/v1/assets                          查询资产列表
GET    /api/v1/assets/{assetKey}/state         查询资产当前状态
GET    /api/v1/assets/{assetKey}/events        查询资产事件历史

POST   /api/v1/asset-events                    外部系统上报事件 (push)
GET    /api/v1/asset-events                    查询事件历史

POST   /api/v1/asset-dependencies              声明资产依赖
GET    /api/v1/workflows/{workflowCode}/dependencies/evaluate   评估依赖

GET    /api/v1/workflows/{workflowCode}/instances               查询工作流实例
GET    /api/v1/tasks/{taskId}/waiting-reason                   查询任务等待原因
GET    /api/v1/triggers/history                                查询触发历史
```

详见 [docs/API.md](./docs/API.md)。

## 监控与告警

### 关键指标

```
lakehouse_asset_event_received_total
  - 收到的事件总数

lakehouse_asset_state_updated_total
  - 资产状态被推进的次数

lakehouse_asset_state_lag_seconds
  - 资产最新快照的年龄

lakehouse_asset_dependency_ready_total
  - 依赖就绪的次数

lakehouse_asset_trigger_success_total
  - 触发成功的工作流数

lakehouse_asset_trigger_failed_total
  - 触发失败的工作流数
```

### 告警阈值

| 告警 | 阈值 | 优先级 |
|------|------|--------|
| 资产状态延迟 | > 1 小时 | P2 |
| 触发失败 | > 3 次/5 分钟 | P1 |
| 补偿扫描失败 | 连续 > 3 次 | P1 |
| 依赖卡顿 | quality 失败 > 30 分钟 | P2 |

详见 [ARCHITECTURE.md#观测性设计](./ARCHITECTURE.md)。

## 常见问题

### Q: Lakehouse Flow 和 DolphinScheduler / Airflow 什么关系？

**A**: 
- **DolphinScheduler/Airflow**：通用调度系统，适合任意任务编排，基于时间和任务依赖
- **Lakehouse Flow**：专用调度系统，针对 CDC 湖仓架构优化，基于数据资产状态

Lakehouse Flow 专注于"数据就绪了吗"这一问题，不处理一般的任务依赖和时间调度。

### Q: 如果资产事件丢失了会怎样？

**A**: 不会永久卡住。有两层防护：
1. 每个任务都有超时时间，超时后进入 ERROR 状态并告警
2. 补偿扫描器每 5~15 分钟运行，对比 Paimon 实际快照和 AssetState，发现漏采集会补写事件

### Q: 支持哪些湖仓表格式？

**A**: MVP 阶段支持 **Paimon**。后续计划扩展 Iceberg 和 Hudi。所有源适配器实现相同的 `AssetEventSource` 接口。

### Q: 多 Master 并发时会重复触发吗？

**A**: 不会。每个触发都有唯一的 `trigger_key`，通过数据库唯一约束天然实现"只有一个赢家"，无需分布式锁。

详见 [DESIGN_PRINCIPLES.md#幂等性](./DESIGN_PRINCIPLES.md)。

## 性能与扩展

- **事件摄入**：~1000 events/sec（取决于数据库 INSERT 吞吐）
- **依赖评估**：~100 evaluations/sec（取决于条件复杂度）
- **建议扩展**：通过 Kafka + 消费者组进行水平扩展

详见 [DEVELOPMENT.md#性能测试](./DEVELOPMENT.md)。

## 开发与贡献

- [本地开发指南](./DEVELOPMENT.md)
- [代码规范](./DEVELOPMENT.md)
- [测试指南](./DEVELOPMENT.md)

### 快速启动开发环境

```bash
# 创建开发数据库
docker run -d --name mysql-dev \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=lakehouse_flow \
  -p 3306:3306 \
  mysql:8.0

# 运行初始化脚本
mysql -h 127.0.0.1 -u root -ppassword lakehouse_flow < db/schema.sql

# 启动应用
mvn spring-boot:run
```

## 许可证

Apache License 2.0

## 联系方式

- 项目主页：https://github.com/your-org/lakehouse-flow
- Issue 跟踪：https://github.com/your-org/lakehouse-flow/issues
- 讨论区：https://github.com/your-org/lakehouse-flow/discussions

---

**最后更新**: 2026-09-11
