# LF-1.0 契约与兼容策略

**状态**: 开发冻结候选已完成，最终冻结等待整体 Testcontainers E2E

本文档定义 LF-1.0 的 REST、出站 intent 和 PostgreSQL migration 兼容边界。它不改变 Lakehouse Flow 的职责：系统只产生调度或作业控制意图，并通过可归因目标 snapshot 判断结果，不读取下游执行状态。

## 1. 冻结产物

| 边界 | 版本 | 机器可读产物 |
|---|---|---|
| REST API | `/api/v1` | [rest-api-v1.surface.json](./lakehouse-flow-boot/src/main/resources/contracts/lakehouse-flow/lf-1.0/rest-api-v1.surface.json) |
| 数据处理意图 | `SchedulingIntent 1.3` | [scheduling-intent-1.3.schema.json](./lakehouse-flow-boot/src/main/resources/contracts/lakehouse-flow/lf-1.0/scheduling-intent-1.3.schema.json) |
| 作业控制意图 | `JobControlIntent 1.0` | [job-control-intent-1.0.schema.json](./lakehouse-flow-boot/src/main/resources/contracts/lakehouse-flow/lf-1.0/job-control-intent-1.0.schema.json) |
| PostgreSQL schema | Flyway `V1.0`～`V23.0` | migration 文件及 `Lf10ContractCompatibilityTests` 校验和基线 |

这些文件随 Boot JAR 一起打包到 `contracts/lakehouse-flow/lf-1.0/`。`SCHEDULING_INTENT_CONTRACT.md` 继续是字段语义和下游行为真源；JSON Schema 用于约束机器可读结构，不能替代语义说明。

## 2. REST 兼容规则

`/api/v1` 使用增量兼容策略：

1. 已发布路径、HTTP 方法、已有请求字段和响应字段不得删除、重命名或改变含义。
2. 新增可选请求字段、响应字段、查询参数或新 endpoint 可以留在 v1，但必须同步更新 surface、DTO 测试和文档。
3. 所有 JSON request DTO 显式忽略未知字段，使旧服务可以接收新客户端附带的可选字段。缺失必填字段仍由 Bean Validation 拒绝。
4. 客户端必须忽略响应中的未知字段，并对未知枚举值保留显式兜底；不能把新增状态默认解释为成功或失败。
5. 需要删除字段、改变字段类型、收紧原有输入或改变状态语义时，必须发布 `/api/v2`，不能原地修改 v1。

surface 文件锁定 method/path，运行时精确字段仍由 `/v3/api-docs` 和对应 DTO 共同描述。兼容测试从 Spring 实际 `RequestMappingHandlerMapping` 反查路由，因此 controller 与清单漂移会使构建失败。

## 3. Intent 版本规则

Lakehouse Flow 在 LF-1.0 中只产生：

- `SchedulingIntent.contractVersion=1.3`；
- `JobControlIntent.contractVersion=1.0`。

版本采用 `major.minor`：

1. 同一 major 只能新增可选字段或扩展非结果型元数据；已有字段、归因属性、幂等键和截止时间语义不可改变。
2. 新 minor 的消费者必须继续接受该 major 的旧 payload；旧消费者必须忽略同 major 的未知字段。
3. 新 required 字段、字段删除/重命名、类型改变、幂等或 snapshot 归因语义改变都必须升级 major。
4. 下游遇到未知 major 必须在启动工作前失败关闭；不能猜测字段，也不能通过回调向 Lakehouse Flow 伪造执行结果。
5. HTTP、DATABASE_TABLE 和 MQ SPI 必须传输同一份不可变 `instructionPayload`。通道不得自行重写版本或字段。

JSON Schema 在每个 object 上保留 `additionalProperties=true`，用于实现同 major 的增量读取；当前版本的 required 字段集合由生成 payload 的服务单测锁定。

## 4. 下游幂等规则

### 4.1 SchedulingIntent

- `intentKey` 是跨数据库轮询、HTTP 重试和 MQ 重投的唯一消费幂等键。
- 相同 `intentKey` 重复到达时，只能复用已有处理，不得再次启动等价任务。
- `X-Lakehouse-Flow-Delivery-Id` 仅标识一次 delivery 记录，不能替代 `intentKey`。
- 下游只有在当前时间早于 `publicationAdmission.leaseExpiresAt` 时才能接受尚未启动的 intent。
- 目标 snapshot 必须携带本 intent 的完整 `requiredSnapshotProperties`；其他 intent、外部写入或维护 snapshot 不能确认它。

### 4.2 JobControlIntent

- API 调用方使用 `requestKey` 保证一次 START/RESTART 请求幂等。
- 执行面使用 `intentKey` 保证同一 writer epoch 不被重复启动。
- `writerJobKey + writerEpoch` 是提交 fencing 坐标；更低 epoch 即使迟到也必须拒绝。
- `control.deliverBefore` 到期后不得首次启动该控制意图。

delivery ACK 只记录传输成功。上述幂等规则都不能用下游 `SUCCESS/FAILED/RUNNING` 状态替代 snapshot 证据。

## 5. Migration 兼容规则

1. `V1.0`～`V23.0` 已成为 LF-1.0 不可变基线，兼容测试按字节校验 SHA-256。
2. 已应用 migration 不得改名、改内容或重排。任何 schema 变化从下一未使用版本开始新增文件。
3. 兼容升级遵守 expand/contract：先增加可空列、表或索引并部署兼容代码；历史读写方退出后，破坏性收缩进入后续版本。
4. Flyway 只前进，生产保持 `baseline-on-migrate=false`、`validate-on-migrate=true`、`clean-disabled=true`。
5. LF-1.0 发布后如需 contract 阶段的 schema 变化，最低使用 `V24.0`，并补充从 V23 升级的整体 E2E。

详细备份、恢复和保留规则见 `DATABASE_OPERATIONS.md`。

## 6. 变更检查

修改 REST、intent 或 migration 时必须同时完成：

1. 判断是 v1/同 major 增量变化，还是需要新 major/API 版本。
2. 更新机器可读 surface/schema 和语义文档。
3. 更新生成 payload、请求未知字段、实际 Spring 路由和 migration checksum 回归。
4. 使用 JDK 17 运行 `./mvnw clean verify -DskipITs`。
5. 在 LF-1.0 最终验收时统一运行整体 Testcontainers E2E，验证 DB/HTTP 投递、全部 action、结果语义、流批汇聚和升级路径。

开发测试通过只表示冻结候选内部一致；只有最后一轮整体 E2E 通过后，才能把 STB-5 标记为正式冻结。
