# Lakehouse Flow - 快速启动指南

本指南介绍如何快速启动和验证 Lakehouse Flow 项目的骨架环境。

## 前置要求

- **Java 17 LTS**: 已在系统中安装
- **Maven 3.8.x+**: 用于构建项目
- **Docker & Docker Compose**: 用于运行 PostgreSQL 容器
- **Git**: 版本控制（可选）

检查环境：

```bash
java -version          # 应该显示 Java 17+
mvn -version           # 应该显示 Maven 3.8.x+
docker --version       # 应该显示 Docker 版本
docker-compose --version  # 应该显示 Docker Compose 版本
```

## 快速启动步骤

### 1. 启动 PostgreSQL 容器

```bash
# 进入项目根目录
cd /path/to/lakehouse-flow

# 启动 PostgreSQL 和 pgAdmin
docker-compose up -d

# 验证容器是否运行
docker ps

# 查看日志（可选）
docker-compose logs -f postgres
```

PostgreSQL 信息：
- **Host**: `localhost:5432`
- **Database**: `lakehouse_flow`
- **Username**: `postgres`
- **Password**: `postgres`

pgAdmin 信息：
- **URL**: `http://localhost:5050`
- **Email**: `admin@lakehouse-flow.io`
- **Password**: `admin`

### 2. 编译项目

```bash
# 清空之前的编译结果，编译所有模块
mvn clean compile -DskipTests

# 如果需要跳过测试并打包
mvn clean package -DskipTests
```

输出应该显示：
```
[INFO] BUILD SUCCESS
```

### 3. 运行单元测试

```bash
# 运行所有单元测试（不需要 Docker）
mvn test

# 运行整个验证流程（包括集成测试，需要 Docker）
mvn verify
```

### 4. 启动应用程序

#### 方法 A：使用 Maven 直接运行（开发环境）

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
cd lakehouse-flow-boot
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=dev"
```

#### 方法 B：启动 JAR 文件（生产环境）

```bash
# 首先构建 FAT JAR
mvn clean package -DskipTests

# 运行 JAR
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
java -jar lakehouse-flow-boot/target/lakehouse-flow-boot-0.1.0-SNAPSHOT.jar \
  --spring.profiles.active=dev \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/lakehouse_flow
```

### 5. 验证应用程序运行

应用程序启动后，应该看到日志输出类似：

```
INFO  [main] org.springframework.boot.StartupInfoLogger - Started LakehouseFlowApplication
INFO  [main] o.s.b.a.e.web.EndpointLinksSupplier - Exposing 3 endpoint(s) beneath base path '/actuator'
```

然后可以验证端点：

```bash
# 检查应用健康状态
curl http://localhost:8080/actuator/health

# 应该返回：
# {"status":"UP"}

# 获取 Swagger UI（API 文档）
curl http://localhost:8080/swagger-ui.html
```

## 项目结构

```
lakehouse-flow/
├── pom.xml                          # 根 POM（9 个模块定义、依赖管理）
├── docker-compose.yml               # PostgreSQL + pgAdmin 容器定义
├── .mavenrc                         # Maven 配置（Java 17 JAVA_HOME）
├── .gitignore                       # Git 忽略规则
│
├── lakehouse-flow-common/           # 共享工具模块
│   └── src/main/java/...
│
├── lakehouse-flow-model/            # 域模型和实体类
│   └── src/main/java/...
│
├── lakehouse-flow-dao/              # 数据库访问层（Repository、Flyway 迁移）
│   ├── src/main/java/...
│   ├── src/main/resources/db/migration/
│   │   └── V1.0__init_schema.sql   # 初始化 12 个核心表
│   └── src/test/java/...           # 集成测试基类（Testcontainers）
│
├── lakehouse-flow-service/          # 业务逻辑层
│   └── src/main/java/...
│
├── lakehouse-flow-api/              # REST API 层（@RestController）
│   └── src/main/java/...
│
├── lakehouse-flow-scheduler/        # 调度器和后台任务循环
│   └── src/main/java/...
│
├── lakehouse-flow-integration/      # 湖仓事件源适配器（Paimon/Iceberg/Hudi）
│   └── src/main/java/...
│
├── lakehouse-flow-test/             # 共享测试工具和固定装置
│   └── src/main/java/...
│
└── lakehouse-flow-boot/             # Spring Boot 启动模块（入口点）
    ├── src/main/java/
    │   └── io/github/lakehouseflow/LakehouseFlowApplication.java
    ├── src/main/resources/
    │   ├── application.yml          # 默认配置（dev profile）
    │   └── application-test.yml     # 测试配置
    └── src/test/java/...
```

## 核心数据库表

Flyway 初始迁移（V1.0__init_schema.sql）创建以下 12 个表：

1. **lakehouse_event** - 原始事件（Paimon/Iceberg/Hudi 快照）
2. **asset_state** - 数据资产的当前状态
3. **asset_dependency** - 资产依赖条件
4. **workflow_definition** - 工作流定义（版本化）
5. **task_definition** - 任务定义
6. **workflow_instance** - 工作流执行实例
7. **task_instance** - 任务执行实例
8. **trigger_history** - 触发审计日志
9. **executor_job** - 外部作业追踪
10. **scheduler_lease** - 分布式调度器租赁
11. **event_consumer_offset** - 事件消费进度
12. **backfill_spec** - 补数操作配置

## 常见问题排查

### Q: Maven 编译时报 "无效的目标发行版: 17"

**A**: 系统的默认 Java 版本不是 17。解决方法：

```bash
# 方法 1：设置 JAVA_HOME 并重新编译
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn clean compile -DskipTests

# 方法 2：检查 .mavenrc 是否存在
cat .mavenrc
```

### Q: Docker 容器无法启动

**A**: 检查 Docker 守护进程是否运行：

```bash
docker ps
# 如果显示错误，启动 Docker Desktop

# 查看容器日志
docker-compose logs postgres
```

### Q: 应用程序启动时报数据库连接错误

**A**: 确认 PostgreSQL 容器已启动且健康：

```bash
# 查看容器状态
docker-compose ps

# 测试数据库连接
docker exec lakehouse-flow-postgres \
  psql -U postgres -d lakehouse_flow -c "SELECT 1"
```

### Q: Flyway 迁移失败

**A**: 检查迁移 SQL 是否有语法错误：

```bash
# 查看 Docker 日志
docker-compose logs postgres

# 手动运行迁移检查
mvn flyway:info -Dflyway.url=jdbc:postgresql://localhost:5432/lakehouse_flow \
  -Dflyway.user=postgres \
  -Dflyway.password=postgres
```

## 后续步骤

现在 Maven 骨架已完成，接下来的开发阶段包括：

1. **Phase 1 Implementation** - 实现核心域模型（事件、资产状态、依赖）
2. **Event Ingestion** - 构建 Paimon 快照事件源适配器
3. **Asset State Management** - 实现资产状态机和单调性检查
4. **Dependency Evaluation** - 实现依赖条件评估引擎
5. **Task Dispatch** - 实现任务调度和执行器适配器
6. **API & UI** - REST API 和基础监控端点

参考项目文档：
- [ARCHITECTURE.md](./ARCHITECTURE.md) - 整体架构设计
- [DESIGN_PRINCIPLES.md](./DESIGN_PRINCIPLES.md) - 设计原则
- [PHASE1_IMPLEMENTATION.md](./PHASE1_IMPLEMENTATION.md) - 第一阶段实现计划

## 命令速查

```bash
# 编译
mvn clean compile -DskipTests

# 测试
mvn test                    # 单元测试
mvn verify                  # 集成测试

# 打包
mvn clean package -DskipTests

# 清理
mvn clean

# 查看依赖树
mvn dependency:tree

# 生成 JaCoCo 覆盖率报告
mvn clean verify jacoco:report
# 查看报告：target/site/jacoco/index.html

# 启动应用
cd lakehouse-flow-boot
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=dev"
```

## 许可证

MIT License
