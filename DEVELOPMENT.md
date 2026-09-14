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

`docker-compose.yml` 只创建空数据库，应用启动时由 classpath 中的 Flyway migration 按版本顺序建表和升级。不要把 `V*.sql` 直接挂到 PostgreSQL 初始化目录，否则 PostgreSQL 会按文件名字典序执行，无法保证 Flyway 版本顺序。

如果需要重建本地开发库：

```bash
docker compose down -v
docker compose up -d postgres
```

## 构建与测试

```bash
./mvnw clean verify -DskipITs
```

当前开发阶段使用 `-DskipITs` 跳过 `lakehouse-flow-e2e` 的 Testcontainers 测试，但仍执行全模块单元测试，并强制检查 service 模块 line coverage >= 90%、branch coverage >= 65%。局部开发可以先运行 `./mvnw -pl <module> -am test`，完成一项变更前回到上述开发期全量验证。

整体 E2E 暂时集中到 LF-1.0 稳定态末尾执行，开发过程中不要逐项启动。进入集中验收后使用：

```bash
./mvnw clean verify
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
source .mavenrc
"$JAVA_HOME/bin/java" -jar lakehouse-flow-boot/target/lakehouse-flow-boot-0.1.0-SNAPSHOT.jar
```

健康检查：

```bash
curl --noproxy '*' http://localhost:8080/actuator/health
```

## 当前模块

```text
lakehouse-flow-common       shared helpers
lakehouse-flow-flink-paimon downstream Paimon writer adapter and epoch fencing
lakehouse-flow-model        JPA entities and value objects
lakehouse-flow-dao          repositories and Flyway schema migrations
lakehouse-flow-service      domain services
lakehouse-flow-integration  lakehouse snapshot source SPI and adapters
lakehouse-flow-api          FlowPlan/Node、action、instance、intent、backfill query REST API
lakehouse-flow-scheduler    intent outbox、外部投递、snapshot 确认和积压指标扫描
lakehouse-flow-test         cross-module shared test fixtures
lakehouse-flow-boot         Spring Boot application
lakehouse-flow-e2e-jobs     test-only Flink CDC and bounded batch jobs
lakehouse-flow-e2e          whole-system Testcontainers E2E assembly
```

## 开发约束

- `asset_state` 是调度判断事实来源，事件只是证据。
- 依赖 DSL 只从发布的 `FlowPlanVersion.dependencySpecJson` 或 `ScheduleNode.inputDependencySpecJson` 读取，支持 `AND/OR` 分组和 `conditions[]`。
- Lakehouse Flow 只做调度和 snapshot 结果确认，不提交任务、不跟踪执行器状态、不保存外部 job，也不依赖下游结果反馈。
- 调度意图必须由 Lakehouse Flow 内部 publisher 主动发布；对外 API 只做审计，不提供 ready/claim/deliver 抢任务协议。
- snapshot ID 不要直接用字符串字典序比较；统一使用 `SnapshotIds`。
- 摄入 offset 代表连续成功处理的位置，不能因为后续 snapshot 成功就越过中间失败 snapshot。
- workflow/task instance 的状态应使用 `SchedulingStates`，不要引入 `RUNNING/SUCCESS/FAILED` 这类执行生命周期状态。
- 写入 `trigger_history.evaluation_payload_json` 时必须保持 JSONB 结构化数据。
- 如果新增 REST API，优先放在 `lakehouse-flow-api`，不要把 Controller 写进 boot 模块。
- 新增湖格式时只实现 `LakehouseSnapshotSource` SPI；格式原生 operation、offset 和分区差异不得泄漏到调度核心。

## 当前推进依据

不要在本开发指南中复制阶段待办。下一推进项、优先级和退出标准只读取 [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md)，以免已经完成的工作继续出现在旧清单中。

当前不可退让的开发顺序是：先检查进度基准中的四态清单和权威架构，再阅读相关实现与测试，完成修改后使用 Maven Wrapper 执行开发期验证。日常 CI 可使用 `-DskipITs`，但发布候选和任何冻结契约变更必须集中运行完整 Testcontainers E2E。不得放宽 `SchedulingIntent` 的 task 非空约束来承载作业生命周期，也不得把 Flink/Spark 类型带入 Flow 或通用 intent。
