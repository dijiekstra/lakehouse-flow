# 参与 Lakehouse Flow 开发

感谢参与 Lakehouse Flow。提交修改前请先理解本项目的核心边界：系统只做调度决策和 snapshot 结果确认，不执行外部作业，也不通过下游任务状态判断成功或失败。

## 开始之前

1. 阅读 [README.md](./README.md) 和 [GLOSSARY.md](./GLOSSARY.md)。
2. 根据修改范围阅读 [ARCHITECTURE.md](./ARCHITECTURE.md)、[SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) 或 [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md)。
3. 在 [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) 核对当前目标和未完成项。
4. 检查现有实现和测试，不从类名或旧提交推断当前行为。

## 开发环境

- 使用 JDK 17。
- 使用仓库自带的 `./mvnw`，不要依赖系统 Maven。
- 本地持久化语义以 PostgreSQL 为准。
- 不要提交凭据、私有 endpoint、生产数据或本地生成物。

开发期验证：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw clean verify -DskipITs
```

整体 E2E 只在修改跨模块调度契约、PostgreSQL 并发恢复、投递、snapshot source/writer、Action 或发布候选时集中运行：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./mvnw -pl lakehouse-flow-e2e -am verify
```

完整环境说明见 [DEVELOPMENT.md](./DEVELOPMENT.md)，E2E 场景见 [E2E_TEST_CASES.md](./E2E_TEST_CASES.md)。

## 代码要求

- 遵循现有模块边界和命名，不引入内置 executor、资源队列或下游执行状态模型。
- 所有新增或修改的类和方法必须有简洁注释，重点说明领域意图、参数、结果和非显然约束。
- 使用 Java 17 已支持的 record、switch expression、text block 等特性时，以可读性和现有风格为准。
- snapshot ID 使用 `SnapshotIds` 或 source 提供的排序规则，不能直接进行字符串字典序比较。
- source 事件、AssetState 投影和 offset 必须保持同一事务；发生缺口时失败关闭。
- 每个物理表只能有一个受管 `writerJobKey`，旧 `writerEpoch` 不能提交可接受的 snapshot。
- 不改写已经发布的 Flyway migration；V1-V23 已冻结，后续从 V24 开始追加。

## 测试要求

- 行为变更必须覆盖正常路径、失败路径和关键幂等或并发边界。
- `lakehouse-flow-service` 每个显式 public 方法必须有直接单元测试入口，优先使用 JUnit 5 与 Mockito。
- Service JaCoCo 门槛为 line coverage >= 90%、branch coverage >= 65%。
- Testcontainers E2E 必须位于 `lakehouse-flow-e2e`，不能用某个生产模块的容器测试冒充整体 E2E。
- E2E 执行端只能被动接收意图，不得调用内部方法绕过投递，也不得回报执行状态推进调度。
- 新增 E2E 前先更新 [E2E_TEST_CASES.md](./E2E_TEST_CASES.md)，完成验证后回填自动化入口和状态。

## 文档与契约

- 中文文档首次出现特殊术语时写作“中文名称（English Term）”，协议字面量保持英文。
- API、JSON payload 或归因属性变化必须同步更新机器可读 schema、兼容测试和 `SCHEDULING_INTENT_CONTRACT.md`。
- 架构或对象语义变化必须同步更新对应真源文档。
- 版本完成度和测试数量只更新 `PHASE2_PROGRESS.md`，避免多份快照漂移。
- 已实现、单元测试通过、整体 E2E 通过和生产验证必须分开表述。

## 提交范围

- 保持修改聚焦，不顺带重构无关模块。
- 不覆盖或回退工作区中他人的修改。
- 提交信息应说明拥有该行为的模块和变化，例如 `test(e2e): cover snapshot attribution isolation`。
- 在提交说明中列出实际运行的验证命令；无法运行的检查要明确说明原因。
