# Phase 1 MVP 实现计划

## 目标

在 6 周内交付一个**可验证的端到端闭环**：

```
Paimon Snapshot -> LakehouseEvent -> AssetState -> DependencyEvaluation -> WorkflowInstance
```

## 范围边界

### ✅ 包含（Phase 1）

1. **事件源**：Paimon 轮询扫描（polling 模式）
2. **事件模型**：LakehouseEvent、EventDeduplication
3. **资产状态**：AssetState（快照、watermark、schema、quality）的单调递增更新
4. **依赖条件**：简单条件（snapshot_id >= X, watermark >= Y, quality=PASSED）
5. **触发链路**：依赖满足 → 创建 WorkflowInstance
6. **执行适配器**：Shell、SQL、HTTP 基本实现
7. **重试机制**：指数退避、最大重试次数
8. **补偿**：扫描丢失 snapshot、修复 stuck 实例

### ❌ 不包含（Phase 2+）

- Iceberg/Hudi 支持（扩展点预留）
- 复杂依赖表达式（AND/OR/NOT 组合）
- Push 事件（Webhook）
- 任务级依赖
- 分布式锁与多实例调度
- Backfill 语义
- 完整的 Web UI
- Lineage 追踪
- 数据质量集成

## 工作包分解 (Work Breakdown Structure)

### WP1: 基础设施与配置 (Week 1)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| Maven 项目结构 | 1d | - | pom.xml 多模块 + 编译配置 |
| Spring Boot 启动类 + 配置 | 1d | WP1.1 | LakehouseFlowApplication + application.yml |
| PostgreSQL 连接与 JPA 配置 | 1d | WP1.1 | DataSource + HibernateConfig |
| Flyway 迁移框架 | 1d | WP1.3 | schema 版本管理 + 初始化脚本 |
| 日志和监控基础 | 1d | WP1.1 | Logback + Micrometer 配置 |

### WP2: 领域模型与数据库 (Week 1-2)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| Entity 设计（PO 类）| 2d | WP1.3 | LakehouseEventPO, AssetStatePO, ... |
| 数据库表创建（Flyway）| 1d | WP2.1 | V1.0__init_schema.sql |
| JPA Repository 接口 | 1d | WP2.1 | EventRepository, AssetStateRepository, ... |
| DTO/VO 类 | 1d | WP2.1 | EventDto, AssetStateVo, DependencyConditionDto |
| 状态枚举与常量 | 1d | - | TaskState, WorkflowState, EventType, ... |

### WP3: 资产状态服务 (Week 2)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| AssetStateService 接口 + 实现 | 2d | WP2.1 | 乐观锁更新、版本管理 |
| 单调性校验 | 1d | WP3.1 | 忽略旧 snapshot、旧 watermark |
| Unit 测试 | 1d | WP3.1 | > 90% 覆盖 |
| Integration 测试（with Testcontainers）| 1d | WP3.1 | PostgreSQL container 测试 |

### WP4: 事件处理与去重 (Week 2-3)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| EventProcessor 服务 | 2d | WP2.1, WP3.1 | 事件入库 → 去重 → 资产状态更新 |
| EventDeduplication 逻辑 | 1d | WP4.1 | 唯一约束 + 冲突处理 |
| Unit 测试 | 1d | WP4.1 | 重复事件不重复落库 |
| Integration 测试 | 1d | WP4.1 | 高并发去重验证 |

### WP5: Paimon 事件源 (Week 3-4)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| PaimonEventScanner 实现 | 3d | WP4.1 | 轮询 Paimon $snapshots，提取 snapshot_id/watermark/schema_id |
| 扫描偏移量管理 | 1d | WP5.1 | EventConsumerOffset 表 + service |
| Unit 测试（Mock Paimon）| 1d | WP5.1 | 事件提取逻辑 |
| Integration 测试（真实 Paimon）| 2d | WP5.1 | 可选，需要 Paimon 环境 |

### WP6: 依赖条件评估 (Week 4)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| DependencyEvaluator 接口 + 实现 | 2d | WP2.1, WP3.1 | 支持 SNAPSHOT_EXISTS, SNAPSHOT_ID_GTE, WATERMARK_GTE, QUALITY_PASSED, SCHEMA_COMPATIBLE |
| 等待原因生成（waiting_reason） | 1d | WP6.1 | 清晰的中英文诊断信息 |
| Unit 测试 | 1d | WP6.1 | 各条件的满足/不满足分支 |
| 复杂表达式评估（可选 Phase 1 末）| 2d | WP6.1 | AND/OR 组合（如果时间允许）|

### WP7: 工作流实例触发 (Week 4-5)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| WorkflowInstanceService 实现 | 2d | WP2.1, WP6.1 | 依赖满足 → 创建 WorkflowInstance |
| TriggerHistory 记录 | 1d | WP7.1 | 审计日志：哪个事件、哪个条件、触发了什么 |
| 幂等性验证（唯一约束）| 1d | WP7.1 | 同 trigger_key 不重复触发 |
| Unit 测试 | 1d | WP7.1 | - |

### WP8: 执行适配器 (Week 5)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| Executor 框架设计 | 1d | WP2.1 | 抽象类 + 适配器模式 |
| ShellExecutor | 1d | WP8.1 | 执行 shell 命令、捕获输出 |
| SqlExecutor | 1d | WP8.1 | 连接数据库、执行 SQL |
| HttpExecutor | 1d | WP8.1 | POST/GET、重试、超时 |
| Unit 测试 | 1d | WP8.1 | Mock 外部系统 |

### WP9: 后台循环与补偿 (Week 5-6)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| EventProcessingLoop（轮询处理事件）| 2d | WP4.1, WP5.1 | 定时任务扫描 + 处理 |
| CompensationScanner（检测遗漏）| 2d | WP7.1 | 重新扫描 Paimon、修复卡住的实例 |
| TaskDispatchLoop（发送任务到执行器）| 1d | WP7.1, WP8.1 | 找 READY 实例 → 提交执行 |
| 监控 & 告警基础 | 1d | - | 关键指标埋点 + Prometheus 导出 |

### WP10: API 层与文档 (Week 5-6)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| REST Controller 基础 | 2d | WP3.1, WP6.1, WP7.1 | /workflows, /tasks, /assets, /events, /triggers |
| API 文档（Swagger/OpenAPI）| 1d | WP10.1 | 自动生成的 API 规范 |
| 错误处理与异常映射 | 1d | - | @ControllerAdvice, GlobalExceptionHandler |
| 代码样本与集成指南 | 2d | - | README 示例 |

### WP11: 集成测试 & E2E (Week 6)

| 任务 | 工作量 | 依赖 | 交付物 |
|------|--------|------|--------|
| 端到端测试脚本 | 2d | 全部 WP | Paimon 事件 → WorkflowInstance 创建 → 执行完成 |
| 性能基准测试（benchmark）| 1d | 全部 WP | 延迟分布、吞吐量 |
| 文档完成 | 1d | 全部 WP | README + ARCHITECTURE + DEVELOPMENT + GLOSSARY |
| Code review & 代码清理 | 1d | 全部 WP | - |

## 阶段性里程碑

### Milestone 1 (Week 1): 项目骨架就绪
- [x] Maven 多模块项目编译通过
- [x] Spring Boot 应用启动成功
- [x] PostgreSQL 连接正常
- [x] 基础表结构创建（Flyway）

**验收**: `mvn clean install` 通过，`java -jar lakehouse-flow-boot-*.jar` 正常启动，日志无错误

### Milestone 2 (Week 2): 核心数据模型 + 资产状态管理
- [x] Entity、Repository、Service 完整
- [x] AssetState 乐观锁更新逻辑通过单测
- [x] 资产状态单调性保证（旧快照被忽略）

**验收**: 单测覆盖 > 80%，集成测试通过

### Milestone 3 (Week 3): 事件处理与 Paimon 集成
- [x] EventProcessor 完整链路
- [x] 事件去重工作正常
- [x] Paimon 轮询扫描工作正常
- [x] 事件偏移量管理正确

**验收**: 真实 Paimon 环境下事件被正确捕获和去重

### Milestone 4 (Week 4): 依赖评估与触发链路
- [x] DependencyEvaluator 支持基础条件
- [x] WorkflowInstance 创建幂等性保证
- [x] TriggerHistory 记录审计日志

**验收**: 端到端闭环验证（Paimon 事件 → AssetState → 依赖满足 → WorkflowInstance 创建）

### Milestone 5 (Week 5): 执行与补偿
- [x] 三种 Executor 实现完整
- [x] 后台循环正确运行
- [x] CompensationScanner 能检测和修复遗漏

**验收**: 任务被正确派发、执行、重试

### Milestone 6 (Week 6): API 与文档
- [x] REST API 工作正常
- [x] Swagger 文档自动生成
- [x] E2E 测试通过
- [x] 所有文档完成

**验收**: 新用户能按照 README 快速启动和运行示例

## 开发环境要求

### 本地开发

```bash
# 系统需求
Java 17 LTS
Maven 3.8+
Docker & Docker Compose

# 启动依赖服务
docker-compose up -d  # PostgreSQL, Redis (optional)

# 编译与运行
mvn clean install
java -jar lakehouse-flow-boot/target/lakehouse-flow-boot-*.jar

# 访问
curl http://localhost:8080/actuator/health  # 健康检查
open http://localhost:8080/swagger-ui.html  # API 文档
```

### 测试

```bash
# 单元测试
mvn test

# 集成测试
mvn verify

# 特定模块
mvn test -pl lakehouse-flow-service

# 覆盖率报告
mvn jacoco:report
```

## 代码质量标准

### 覆盖率目标
- 单元测试：>= 80%
- 集成测试：关键路径 100%

### 代码规范
- 遵循 [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- 最多 120 字符行宽
- 异常必须有日志记录
- 所有 public API 必须有 JavaDoc

### 检查工具
```bash
# 格式化
mvn spotless:apply

# 静态分析（未来可加）
mvn findbugs:check
mvn pmd:check
```

## 风险与缓解

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| Paimon API 变化 | 低 | 中 | 定义 PaimonEventSource 接口，隔离外部依赖 |
| 高并发去重失败 | 中 | 高 | 充分的并发测试、数据库约束验证 |
| 资产状态乱序更新 | 中 | 高 | 版本字段 + 乐观锁，充分 unit 测试 |
| 执行器超时/失败处理不当 | 中 | 中 | 清晰的状态机、重试策略、补偿机制 |
| 文档与代码不同步 | 低 | 低 | Code review 时检查文档更新 |

## 成功标志

### 功能验收
- ✅ Paimon snapshot 被正确捕获
- ✅ AssetState 被正确更新（无冲突、无丢失）
- ✅ 依赖条件被正确评估
- ✅ WorkflowInstance 被正确创建（无重复）
- ✅ 任务被正确派发和执行
- ✅ 遗漏事件能被补偿检测到

### 质量验收
- ✅ 单测覆盖 >= 80%
- ✅ 无高风险 bug
- ✅ P99 延迟 < 1s

### 文档验收
- ✅ README 清晰完整
- ✅ ARCHITECTURE 图文并茂
- ✅ DEVELOPMENT 指南可操作
- ✅ GLOSSARY 术语表完整
- ✅ API 文档自动生成

## 后续（Phase 2）路线

1. 复杂依赖表达式（AND/OR/NOT）
2. Push 事件接收（Webhook）
3. 任务级依赖
4. Backfill 语义
5. 多实例分布式调度
6. Iceberg/Hudi 支持
7. Web UI
