# Lakehouse Flow 文档索引

**最后更新**: 2026-09-14

## 文档真源

文档按以下优先级解释：

1. `PHASE2_PROGRESS.md`：版本目标、优先级、当前完成度、差距和验证快照的唯一真源。
2. `ARCHITECTURE.md`、`SCHEDULING_MODEL_DESIGN.md`：当前架构与对象模型不变量。
3. `SCHEDULING_INTENT_CONTRACT.md`：Lakehouse Flow 与下游之间的协议真源。
4. `OPERATIONS_RUNBOOK.md`、`DATABASE_OPERATIONS.md`：生产接入、故障处置、数据库升级恢复和数据保留边界。
5. `README.md`、`DEVELOPMENT.md`、`QUICKSTART.md`、`TECH_STACK.md`：当前入口、开发、启动和技术说明。
6. 标记为“历史文档”或“参考文档”的文件：只用于追溯，不得驱动实现。

如果历史文档与当前文档冲突，必须服从上述当前真源。任何文档都不能把 Lakehouse Flow 描述为任务执行器，也不能使用下游 `RUNNING/SUCCESS/FAILED` 回调代替目标业务 snapshot 推进。

## 当前文档

| 文档 | 状态 | 回答的问题 |
|---|---|---|
| [README.md](./README.md) | 当前入口 | 系统是什么、能做什么、不能做什么 |
| [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) | 进度真源 | 当前做到哪里、下一步是什么、验收证据是什么 |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 架构真源 | 组件、事务、扫描循环、归因与互斥如何协作 |
| [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) | 模型真源 | FlowPlan、Node、实例、action、补数和 DAG 的语义 |
| [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) | 协议真源 | 数据处理与作业控制两类出站意图如何投递、幂等，以及目标 snapshot 必须写什么 |
| [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md) | 生产运维手册 | 如何配置投递与 source、启动/重启 writer，以及处置阻塞、死信和补数 |
| [DATABASE_OPERATIONS.md](./DATABASE_OPERATIONS.md) | 数据库运维策略 | 如何升级、备份、恢复 PostgreSQL，以及哪些数据可以归档清理 |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | 当前开发指南 | 本地环境、代码约束和验证方式 |
| [QUICKSTART.md](./QUICKSTART.md) | 当前启动指南 | 如何启动 PostgreSQL、构建、运行和排查 |
| [TECH_STACK.md](./TECH_STACK.md) | 当前技术基线 | 实际依赖、模块边界、已实现与未实现的基础设施 |

## 历史文档

以下文件保留早期决策过程和阶段记录，但不代表当前能力：

| 文档 | 历史内容 |
|---|---|
| [ARCHITECTURE_CLARIFICATION.md](./ARCHITECTURE_CLARIFICATION.md) | 早期 READY 查询和 callback 方案 |
| [CLARIFICATION_SUMMARY.md](./CLARIFICATION_SUMMARY.md) | 早期架构澄清过程 |
| [DESIGN_PRINCIPLES.md](./DESIGN_PRINCIPLES.md) | 含已失效示例的原则草案 |
| [GLOSSARY.md](./GLOSSARY.md) | 旧 executor 和运行状态术语 |
| [IMPLEMENTATION_GUIDE.md](./IMPLEMENTATION_GUIDE.md) | 旧状态判断和 READY API 实现讨论 |
| [PHASE1_IMPLEMENTATION.md](./PHASE1_IMPLEMENTATION.md) | 旧 Phase 1 计划 |
| [PHASE1_COMPLETION.md](./PHASE1_COMPLETION.md) | 旧 Phase 1 完成快照 |
| [PHASE2_DESIGN.md](./PHASE2_DESIGN.md) | 旧 Phase 2 callback/执行状态设计 |
| [PHASE2_ROADMAP.md](./PHASE2_ROADMAP.md) | 已被当前进度基准替代的路线图 |
| [PHASE2_DELIVERY_PATTERN.md](./PHASE2_DELIVERY_PATTERN.md) | 已废弃的 READY API 交付讨论 |
| [PHASE2_STEP3_SUMMARY.md](./PHASE2_STEP3_SUMMARY.md) | 早期 source 实现快照 |
| [CHECKLIST.md](./CHECKLIST.md) | 旧项目交付清单 |
| [PROJECT_STATUS.md](./PROJECT_STATUS.md) | 旧项目状态快照 |
| [DELIVERY_REPORT.md](./DELIVERY_REPORT.md) | Phase 0 文档交付报告 |
| [SKELETON_COMPLETION_REPORT.md](./SKELETON_COMPLETION_REPORT.md) | 项目骨架阶段报告 |

历史文档中的 `AssetDependency` 运行时路径、executor、外部 job、READY 拉取 API、状态回调和旧表清单均不应恢复。

## 参考资料

| 文档 | 使用边界 |
|---|---|
| [lakehouse-asset-event-scheduling.md](./lakehouse-asset-event-scheduling.md) | DolphinScheduler 扩展 RFC；只参考背景与 action，不参考执行架构 |
| [现代CDC湖仓架构下的数仓与数据平台演进.md](./现代CDC湖仓架构下的数仓与数据平台演进.md) | CDC 湖仓、数仓与数据平台背景资料 |

## 按角色阅读

### 产品与架构讨论

1. `README.md`
2. `SCHEDULING_MODEL_DESIGN.md`
3. `PHASE2_PROGRESS.md`

### 后端开发

1. `PHASE2_PROGRESS.md`
2. `ARCHITECTURE.md`
3. 对应领域的 `SCHEDULING_MODEL_DESIGN.md` 或 `SCHEDULING_INTENT_CONTRACT.md`
4. `DEVELOPMENT.md`
5. 相关代码和测试

### 下游集成

1. `SCHEDULING_INTENT_CONTRACT.md`
2. `ARCHITECTURE.md`
3. `QUICKSTART.md`

### 测试与投产验收

1. `PHASE2_PROGRESS.md` 的 `LF-1.0 目标与发布门槛`
2. `PHASE2_PROGRESS.md` 的当前验证快照
3. `SCHEDULING_MODEL_DESIGN.md` 的不变量
4. `ARCHITECTURE.md` 的事务、互斥和失败关闭路径
5. `SCHEDULING_INTENT_CONTRACT.md` 的归因、Flink/Paimon writer 和租约要求

### 生产运维

1. `OPERATIONS_RUNBOOK.md`
2. `DATABASE_OPERATIONS.md`
3. `SCHEDULING_INTENT_CONTRACT.md`
4. `PHASE2_PROGRESS.md`

## 更新规则

- 版本目标、优先级和完成度只更新 `PHASE2_PROGRESS.md`，其他文档通过链接引用。
- 架构或对象语义变化必须同步更新对应权威文档和测试。
- 下游 payload 或归因属性变化必须同步更新 `SCHEDULING_INTENT_CONTRACT.md`。
- 依赖、模块或构建方式变化必须同步更新 `TECH_STACK.md`、`DEVELOPMENT.md` 或 `QUICKSTART.md`。
- 投递、source、writer、故障处置或数据库运维规则变化必须同步更新两份生产运维手册。
- 阶段总结一旦过期，保留内容但在标题后明确标记为历史。
- 未经过测试或真实环境验证的能力必须写成“未验证”或“未实现”，不得写成“生产就绪”。
