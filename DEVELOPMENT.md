# Development

本文档描述当前仓库真实可用的开发方式。更完整的目标架构见 [ARCHITECTURE.md](./ARCHITECTURE.md)。

## 环境要求

- Java 17+
- `./mvnw`（会下载 Maven 3.9.9，并读取 `.mavenrc` 使用 JDK 17）
- Docker / Docker Compose
- PostgreSQL 15 或兼容版本

当前 Flyway SQL 使用 PostgreSQL `BIGSERIAL` 和 `JSONB`，不要按 MySQL 项目启动。

## 本地数据库

```bash
docker compose up -d postgres
```

连接信息：

```text
jdbc:postgresql://localhost:5432/lakehouse_flow
user: postgres
password: postgres
```

`docker-compose.yml` 会把 `lakehouse-flow-dao/src/main/resources/db/migration` 挂载到 PostgreSQL 初始化目录。已经初始化过的 volume 不会自动重放修改后的 SQL；如果需要重建本地库：

```bash
docker compose down -v
docker compose up -d postgres
```

## 构建与测试

```bash
./mvnw clean test
```

如果只想做编译检查：

```bash
./mvnw clean compile
```

不要依赖系统 `mvn`。本仓库的 `mvnw` 会校验 JDK 17，并在 `.mvn/wrapper/dists/` 缓存 Maven 发行包。

## 启动应用

开发模式：

```bash
cd lakehouse-flow-boot
../mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

打包运行：

```bash
./mvnw clean package -DskipTests
java -jar lakehouse-flow-boot/target/lakehouse-flow-boot-0.1.0-SNAPSHOT.jar
```

健康检查：

```bash
curl http://localhost:8080/actuator/health
```

## 当前模块

```text
lakehouse-flow-common       shared helpers
lakehouse-flow-model        JPA entities and value objects
lakehouse-flow-dao          repositories and Flyway schema migrations
lakehouse-flow-service      domain services
lakehouse-flow-integration  mock Paimon ingestion
lakehouse-flow-api          FlowPlan/Node、action、instance、intent、backfill query REST API
lakehouse-flow-scheduler    snapshot 确认扫描循环
lakehouse-flow-test         shared test placeholder
lakehouse-flow-boot         Spring Boot application
```

## 开发约束

- `asset_state` 是调度判断事实来源，事件只是证据。
- `AssetDependency.dependencyConditions` 是当前依赖 DSL 的第一版入口，支持 `AND/OR` 和 `conditions[]`。
- Lakehouse Flow 只做调度和 snapshot 结果确认，不提交任务、不跟踪执行器状态、不保存外部 job，也不依赖下游结果反馈。
- 调度意图必须由 Lakehouse Flow 内部 publisher 主动发布；对外 API 只做审计，不提供 ready/claim/deliver 抢任务协议。
- snapshot ID 不要直接用字符串字典序比较；统一使用 `SnapshotIds`。
- 摄入 offset 代表连续成功处理的位置，不能因为后续 snapshot 成功就越过中间失败 snapshot。
- workflow/task instance 的状态应使用 `SchedulingStates`，不要引入 `RUNNING/SUCCESS/FAILED` 这类执行生命周期状态。
- 写入 `trigger_history.evaluation_payload_json` 时必须保持 JSONB 结构化数据。
- 如果新增 REST API，优先放在 `lakehouse-flow-api`，不要把 Controller 写进 boot 模块。
- 如果新增真实 Paimon source，先抽接口，再把当前 mock 实现限制在测试或本地 demo profile。

## 推荐下一步

1. 增强 scheduling intent 主动投递：HTTP/MQ publisher、内部投递 lease、退避重试和死信审计。
2. 将常规 snapshot 触发链路逐步收敛到 `FlowPlanVersion` 和 `ScheduleNode`。
3. 扩展 Flow 实例聚合查询和运维指标；发布版本的确认窗口、目标准入租约和基础活跃实例上限已进入运行时。
4. 明确 `trigger_key` 生成规则，并为重复触发写数据库约束测试。
5. 给 `EventIngestionService` 和 `AssetStateService` 增加更贴近数据库的集成测试。
