# Lakehouse Flow 文档索引

**最后更新**：2026-09-14

## 文档组织原则

根目录只保留当前有效的项目入口、概念与架构、协议参考、开发测试、运维和发布文档。已经失效的阶段总结不再留在工作树中，需要追溯时使用 Git 历史。

文档采用以下约定：

- 面向中文读者，特殊术语首次出现时写作“中文名称（English Term）”。
- 类名、字段名、枚举值、配置键和 JSON 属性保持英文原文，并使用反引号标识。
- 版本完成度只在 `PHASE2_PROGRESS.md` 维护，其他文档不复制易失真的测试数量和待办。
- 文档不得把 Lakehouse Flow 描述为执行器，也不得使用下游作业状态替代目标 snapshot 证据。
- 已实现、已测试、已通过整体 E2E 和已在生产验证是四种不同结论，必须明确区分。

组织方式参考了成熟开源项目的当前文档实践：使用 [Kubernetes 文档风格指南](https://kubernetes.io/docs/contribute/style/style-guide/) 约束表达与链接，使用 [Kubernetes 术语表](https://kubernetes.io/docs/reference/glossary/) 和 [Flink 术语表](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/glossary/) 的概念入口方式服务新读者，并借鉴 [Apache Airflow README](https://github.com/apache/airflow/blob/main/README.md) 的短入口加分层导航。[Apache DolphinScheduler](https://github.com/apache/dolphinscheduler) 只用于核对调度 Action 能力，不复制其执行器或工作流状态模型。

## 文档真源

发生冲突时按以下顺序处理：

1. 机器可读 API/JSON Schema、Flyway migration 和当前代码决定实际行为。
2. `ARCHITECTURE.md` 与 `SCHEDULING_MODEL_DESIGN.md` 定义架构和领域不变量。
3. `SCHEDULING_INTENT_CONTRACT.md` 与 `LF1_COMPATIBILITY.md` 定义对外协议和兼容边界。
4. `PHASE2_PROGRESS.md` 记录版本目标、完成度、剩余工作和验证快照。
5. 运维、开发和入口文档不得覆盖上述契约，只负责说明如何使用。

发现文档与实现不一致时，应先判断是实现缺陷还是文档过期，再修改拥有该规则的真源文档和对应测试。

## 文档目录

### 项目入口

| 文档 | 回答的问题 |
| --- | --- |
| [README.md](./README.md) | 系统是什么、边界是什么、如何开始、去哪里继续阅读 |
| [GLOSSARY.md](./GLOSSARY.md) | Intent、snapshot、归因、准入、补数等术语是什么意思 |
| [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) | LF-1.0 做到哪里、还缺什么、最近验证证据是什么 |

### 概念与架构

| 文档 | 回答的问题 |
| --- | --- |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 组件、事务、扫描循环、归因、恢复和互斥如何协作 |
| [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) | FlowPlan、Node、实例、Action、补数和流批混编 DAG 的语义 |
| [TECH_STACK.md](./TECH_STACK.md) | 当前依赖、模块边界和基础设施选择是什么 |

### 协议与兼容

| 文档 | 回答的问题 |
| --- | --- |
| [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) | 两类调度意图如何投递、下游如何幂等消费和写入 snapshot 归因 |
| [LF1_COMPATIBILITY.md](./LF1_COMPATIBILITY.md) | `/api/v1`、intent schema、未知字段和 migration 如何演进 |

### 开发与测试

| 文档 | 回答的问题 |
| --- | --- |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | 开发环境、代码约束、单元测试和质量门禁是什么 |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | 贡献代码时必须遵守哪些范围、注释、测试和文档规则 |
| [QUICKSTART.md](./QUICKSTART.md) | 如何配置 PostgreSQL、构建并启动应用 |
| [E2E_TEST_CASES.md](./E2E_TEST_CASES.md) | 整体 E2E 的拓扑、用例、断言、覆盖状态和补强顺序是什么 |

### 运维与发布

| 文档 | 回答的问题 |
| --- | --- |
| [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md) | 如何接入投递和 source、启停 writer、巡检并处置故障 |
| [DATABASE_OPERATIONS.md](./DATABASE_OPERATIONS.md) | PostgreSQL 如何升级、备份、恢复和保留数据 |
| [OBSERVABILITY.md](./OBSERVABILITY.md) | 指标、Prometheus 规则、告警和证据入口是什么 |
| [RELEASE_CHECKLIST.md](./RELEASE_CHECKLIST.md) | 如何生成、核验、试运行和发布 LF-1.0 |

## 推荐阅读顺序

产品与架构讨论：

1. `README.md`
2. `GLOSSARY.md`
3. `SCHEDULING_MODEL_DESIGN.md`
4. `ARCHITECTURE.md`
5. `PHASE2_PROGRESS.md`

下游平台接入：

1. `GLOSSARY.md` 的 Intent、Delivery、Attribution 和 Writer
2. `SCHEDULING_INTENT_CONTRACT.md`
3. `LF1_COMPATIBILITY.md`
4. `OPERATIONS_RUNBOOK.md`

开发与测试：

1. `DEVELOPMENT.md`
2. `ARCHITECTURE.md`
3. `E2E_TEST_CASES.md`
4. 对应领域的模型或契约文档
5. 当前代码和测试

生产运维：

1. `OPERATIONS_RUNBOOK.md`
2. `OBSERVABILITY.md`
3. `DATABASE_OPERATIONS.md`
4. `RELEASE_CHECKLIST.md`

## 更新规则

- 领域概念变化：更新 `SCHEDULING_MODEL_DESIGN.md`、`GLOSSARY.md` 和对应测试。
- 组件或事务边界变化：更新 `ARCHITECTURE.md`。
- payload、归因属性或下游幂等要求变化：更新 `SCHEDULING_INTENT_CONTRACT.md`、机器可读 schema 和兼容测试。
- REST、版本或 migration 基线变化：更新 `LF1_COMPATIBILITY.md`、机器可读契约和 migration 测试。
- 模块、依赖或构建方式变化：更新 `TECH_STACK.md`、`DEVELOPMENT.md` 或 `QUICKSTART.md`。
- E2E 场景变化：先更新 `E2E_TEST_CASES.md`，再修改测试；完成后回填入口和状态。
- 指标或告警变化：更新 `OBSERVABILITY.md` 与 Prometheus 规则。
- 版本目标和验证结论变化：只更新 `PHASE2_PROGRESS.md`，并附可复现命令或 CI 证据。
