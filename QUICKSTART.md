# Lakehouse Flow 快速启动

**最后核对**: 2026-09-13

本指南用于启动当前 Lakehouse Flow 应用和验证本地构建。系统只生成调度意图并通过目标资产 snapshot 确认结果，不会在本地替你执行下游任务。

## 前置要求

- JDK 17
- Docker 与 Docker Compose
- 可访问 Maven Central 的网络

项目自带 Maven Wrapper，不依赖系统 `mvn`：

```bash
source .mavenrc
"$JAVA_HOME/bin/java" -version
./mvnw -version
docker --version
docker compose version
```

两条版本命令都应显示 Java 17。macOS 的 `.mavenrc` 会尝试选择本机 JDK 17，Linux 和 CI 使用环境已经提供的 `JAVA_HOME`。macOS 的 `/usr/bin/java` 可能仍指向系统默认 JDK，因此直接运行 JAR 时使用 `"$JAVA_HOME/bin/java"`。

## 1. 启动 PostgreSQL

```bash
docker compose up -d postgres
docker compose ps
```

本地默认连接：

```text
URL:      jdbc:postgresql://localhost:5432/lakehouse_flow
User:     postgres
Password: postgres
```

这些凭据只用于本地开发。Compose 只创建空数据库；应用启动时，Flyway 从 classpath 按版本顺序执行 `lakehouse-flow-dao/src/main/resources/db/migration` 下的迁移。

## 2. 完整验证

```bash
./mvnw clean verify
```

该命令会：

- 编译全部九个模块；
- 运行 JUnit/Mockito 测试；
- 检查 service line coverage >= 90%、branch coverage >= 65%；
- 生成可执行 Spring Boot JAR。

较小改动可以先运行局部测试，例如：

```bash
./mvnw -pl lakehouse-flow-service -am test
```

完成一项变更前仍应执行全量 `clean verify`。

## 3. 启动应用

```bash
cd lakehouse-flow-boot
../mvnw spring-boot:run
```

另一个终端检查：

```bash
curl --noproxy '*' http://localhost:8080/actuator/health
curl --noproxy '*' http://localhost:8080/actuator/prometheus
```

OpenAPI：

```text
http://localhost:8080/swagger-ui.html
http://localhost:8080/v3/api-docs
```

默认配置下 Paimon source 关闭，避免未配置 catalog 时扫描失败。调度意图默认使用 `DATABASE_TABLE` 通道。

## 4. 运行打包产物

```bash
./mvnw -pl lakehouse-flow-boot -am clean package
source .mavenrc
"$JAVA_HOME/bin/java" -jar lakehouse-flow-boot/target/lakehouse-flow-boot-0.1.0-SNAPSHOT.jar
```

该 JAR 必须包含 Spring Boot `JarLauncher` 和 `BOOT-INF/lib`。默认分支 GitHub Actions 会在上传 artifact 前再次校验这些结构。

## 5. 下游如何接收

Lakehouse Flow 内部 scanner 会把 READY 调度决策固化为不可变 `SchedulingIntent`：

- `DATABASE_TABLE`：下游轮询 `scheduling_intent` 专用表；
- `HTTP`：Lakehouse Flow 主动调用配置的下游端点；
- `MQ`：Lakehouse Flow 调用部署提供的 `SchedulingIntentMessageGateway`。

REST scheduling-intent API 仅供审计，不提供 `ready/claim/deliver` 抢任务协议。下游必须按 `intentKey` 保证消费幂等，并把 payload 中的 `requiredSnapshotProperties` 写入逻辑完成业务数据 snapshot。完整规则见 `SCHEDULING_INTENT_CONTRACT.md`。

## 6. 配置 Paimon Source

在运行环境配置受管 catalog 和 table，并启用：

```yaml
lakehouse-flow:
  snapshot-sources:
    paimon:
      enabled: true
      zone-id: Asia/Shanghai
      catalogs:
        - name: paimon-prod
          options:
            warehouse: s3://warehouse/paimon
            metastore: hive
          tables:
            - database: dwd
              table: orders
```

认证材料由部署环境提供，不应提交到仓库。当前 Paimon source、Flink CDC 流式 ODS、独立 `JobControlIntent(START_JOB|RESTART_JOB)`、单表单 `writerJobKey`、writer epoch fencing 和流批一体 writer-side snapshot 属性注入均已有真实闭环基线。批式路径在有界输入结束时提交完成证据；流式路径在 checkpoint 覆盖 intent 冻结输入向量时提交完成证据，`final` 不表示流作业结束。完整生产配置和故障处置见 `OPERATIONS_RUNBOOK.md`。

## 7. 常见排查

### Java 版本不正确

```bash
source .mavenrc
"$JAVA_HOME/bin/java" -version
./mvnw -version
echo "$JAVA_HOME"
```

不要绕过 Wrapper 改用系统 Maven。

### 数据库未就绪

```bash
docker compose ps
docker compose logs postgres
docker exec lakehouse-flow-postgres psql -U postgres -d lakehouse_flow -c "SELECT 1"
```

### 需要重建本地数据库

```bash
docker compose down -v
docker compose up -d postgres
```

该命令会删除本地开发 volume，不应用于任何共享或生产数据库。

### 应用启动但没有湖仓事件

默认 Paimon source 为关闭状态。先检查 `lakehouse-flow.snapshot-sources.paimon.enabled`、catalog/table 配置和 source reconciliation 指标。不得通过伪造 offset 或 snapshot 事件绕过缺口。

## 延伸阅读

- `README.md`：系统边界和当前能力
- `ARCHITECTURE.md`：当前架构与核心事务
- `SCHEDULING_MODEL_DESIGN.md`：对象模型、DAG、action 和补数语义
- `SCHEDULING_INTENT_CONTRACT.md`：下游消费与 snapshot 归因契约
- `OPERATIONS_RUNBOOK.md`：生产接入、巡检和故障处置
- `DATABASE_OPERATIONS.md`：PostgreSQL 升级、恢复与保留策略
- `DEVELOPMENT.md`：开发规则
- `PHASE2_PROGRESS.md`：当前进度和唯一待办基准
