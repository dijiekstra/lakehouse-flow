# Lakehouse Flow 技术栈

## 概览

**Lakehouse Flow** 是一个从零到一实现的独立调度系统，专为现代 CDC 湖仓架构（基于 Paimon/Iceberg/Hudi）设计。

本文档定义项目的技术决策和实现方向。

## 核心技术栈

### 编程语言与运行时
- **主语言**: Java 17 LTS
- **JVM**: OpenJDK 17+ 或 Eclipse Temurin 17+
- **目标**: 在未来支持 GraalVM Native Image 编译

### Web 框架
- **Spring Boot**: 3.2.x 或 3.3.x
  - 原因：Java 17 最小版本支持、虚拟线程友好、完整生态
  - 依赖：Spring Web、Spring Data JPA、Spring Cache

### 数据库
- **主数据库**: PostgreSQL 12+
  - JSONB 列类型支持（条件、配置存储）
  - 乐观锁支持（通过 version 列）
  - 分区表支持（大表分区）
  
- **可选缓存**: Redis 5.0+ (可选在 Phase 2+)

### 持久化
- **ORM**: Hibernate + Spring Data JPA
  - 原因：成熟、类型安全、支持多数据库
  
- **数据库迁移**: Flyway 9.x+
  - 原因：简单可靠、与 Spring Boot 集成好

### 消息与事件
- **Phase 1**: 仅支持轮询（polling）
- **Phase 2+**: Kafka（可选）
  - 事件去重和幂等性通过数据库唯一约束实现
  - 初期不必强依赖消息队列

### 序列化与数据格式
- **JSON**: Jackson（Spring Boot 内置）
  - 用于：API 请求/响应、条件表达式、配置存储
  
- **Protocol Buffers**: 可选在 Phase 2+（性能优化）

### 测试框架
- **单元测试**: JUnit 5 (Jupiter)
- **集成测试**: Testcontainers（PostgreSQL、Redis）
- **Mock 框架**: Mockito
- **测试覆盖**: 目标 ≥ 80%

### 监控与可观测性
- **指标**: Micrometer + Prometheus
- **日志**: SLF4J + Logback
- **分布式追踪**: 可选（Phase 2+）

### API 文档
- **OpenAPI 3.0**: Springdoc-OpenAPI 2.x
- **UI**: Swagger UI（自动包含）

### 构建与包管理
- **构建工具**: Maven 3.8+
- **仓库管理**: Maven Central
- **Java 版本**: 编译器 `source`/`target` = 17

## 模块化设计

```
lakehouse-flow/                          # 根项目 (pom.xml parent)
├── lakehouse-flow-common/               # 通用工具、常量、异常
│   ├── src/main/java/io/github/lakehouseflow/common/
│   │   ├── constant/                    # 常量（状态枚举、错误码）
│   │   ├── exception/                   # 自定义异常
│   │   ├── util/                        # 工具类
│   │   └── model/                       # 公共模型（请求/响应包装）
│   └── pom.xml
│
├── lakehouse-flow-model/                # 领域模型（Entity、DTO、VO）
│   ├── src/main/java/io/github/lakehouseflow/model/
│   │   ├── entity/                      # JPA Entity（数据库 PO）
│   │   ├── dto/                         # Data Transfer Object
│   │   ├── vo/                          # Value Object
│   │   └── enums/                       # 枚举（状态机、类型）
│   └── pom.xml
│
├── lakehouse-flow-dao/                  # 数据访问层 + 数据库迁移
│   ├── src/main/java/io/github/lakehouseflow/dao/
│   │   └── repository/                  # Spring Data JPA Repository
│   ├── src/main/resources/db/migration/ # Flyway 迁移脚本
│   └── pom.xml
│
├── lakehouse-flow-service/              # 业务服务层
│   ├── src/main/java/io/github/lakehouseflow/service/
│   │   ├── asset/                       # 资产状态服务
│   │   ├── event/                       # 事件处理服务
│   │   ├── dependency/                  # 依赖解析服务
│   │   ├── workflow/                    # 工作流服务
│   │   ├── task/                        # 任务服务
│   │   └── executor/                    # 执行适配器
│   └── pom.xml
│
├── lakehouse-flow-api/                  # REST API 控制器层
│   ├── src/main/java/io/github/lakehouseflow/api/
│   │   ├── controller/                  # @RestController
│   │   └── interceptor/                 # 请求拦截器
│   └── pom.xml
│
├── lakehouse-flow-scheduler/            # 调度循环与后台任务
│   ├── src/main/java/io/github/lakehouseflow/scheduler/
│   │   ├── loop/                        # 各个调度循环实现
│   │   ├── monitor/                     # 监控和补偿
│   │   └── lease/                       # 分布式锁（可选）
│   └── pom.xml
│
├── lakehouse-flow-integration/          # Paimon/Iceberg/Hudi 集成
│   ├── src/main/java/io/github/lakehouseflow/integration/
│   │   ├── paimon/                      # Paimon 事件源适配器
│   │   ├── iceberg/                     # Iceberg（Phase 3）
│   │   └── hudi/                        # Hudi（Phase 3）
│   └── pom.xml
│
├── lakehouse-flow-test/                 # 共享测试工具与 Fixture
│   ├── src/main/java/io/github/lakehouseflow/test/
│   │   ├── container/                   # Testcontainers 配置
│   │   ├── fixture/                     # 测试数据生成器
│   │   └── assertion/                   # 自定义断言
│   └── pom.xml
│
├── lakehouse-flow-boot/                 # Spring Boot 启动类与配置
│   ├── src/main/java/io/github/lakehouseflow/
│   │   ├── LakehouseFlowApplication.java # 主启动类
│   │   └── config/                      # Spring 配置
│   ├── src/main/resources/
│   │   ├── application.yml              # 主配置
│   │   ├── application-dev.yml          # 开发环境
│   │   └── application-prod.yml         # 生产环境
│   └── pom.xml
│
└── pom.xml                              # 根 POM（parent、dependencyManagement）
```

## 依赖版本管理

根 POM 定义所有版本，子模块继承：

```xml
<dependencyManagement>
    <dependencies>
        <!-- Spring Boot 3.2.x -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-dependencies</artifactId>
            <version>3.2.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        
        <!-- PostgreSQL Driver -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.0</version>
        </dependency>
        
        <!-- Flyway -->
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
            <version>9.22.0</version>
        </dependency>
        
        <!-- Test -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-bom</artifactId>
            <version>1.19.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## 编码规范

### Java 17 特性使用
- ✅ **Records** for immutable data transfer objects
  ```java
  public record AssetStateDto(String assetKey, long snapshotId, String watermark) {}
  ```
  
- ✅ **Sealed Classes** for state machines
  ```java
  public sealed interface TaskState permits Created, Running, Success, Failed {}
  ```
  
- ✅ **Text Blocks** for SQL/JSON
  ```java
  String sql = """
      SELECT * FROM asset_state 
      WHERE asset_key = ? AND snapshot_id > ?
      """;
  ```
  
- ✅ **Pattern Matching** in conditions (when stable)

### 包命名约定
- `io.github.lakehouseflow.*` 为根包
- 按功能域分层：`service`, `dao`, `controller`, `model`, `common`

### 类命名规范
- Service 类：`*Service`（接口 + Impl）
- Repository：继承 `JpaRepository<Entity, ID>`
- Controller：`*Controller`
- Entity：`*PO`（Persistent Object）
- DTO：`*Dto`
- 枚举：`*Status`, `*Type`, `*State`

## 阶段性技术重点

### Phase 1 (MVP, 6 周)
- ✅ 基础数据模型和 JPA 映射
- ✅ Paimon 轮询事件源
- ✅ 资产状态更新（乐观锁）
- ✅ 依赖条件评估
- ✅ 简单 REST API
- ✅ 基础日志和监控

### Phase 2 (多资产依赖, 6 周)
- Push 事件接收（Webhook）
- 复杂依赖表达式（AND/OR/NOT）
- 补偿和失败恢复
- 高级 API（手工触发、回溯等）
- Redis 缓存（可选）

### Phase 3 (生产就绪, 8+ 周)
- Iceberg/Hudi 支持
- 多实例分布式协调
- 高级监控和告警
- 完整的 UI
- GraalVM Native Image

## 性能目标 (Phase 1)

| 指标 | 目标 | 补注 |
|------|------|------|
| 事件入库延迟 | < 100ms | P99 |
| 资产状态更新 | < 50ms | 单次 |
| 依赖评估 | < 100ms | 平均 |
| 触发创建 E2E | < 1s | P95 |
| 轮询扫描周期 | 10s | 可配置 |

## 安全考虑

- ✅ 参数化查询（Hibernate + JPA）
- ✅ 敏感信息加密（密码、token）
- ✅ API 认证与授权（Spring Security，Phase 2+）
- ✅ 审计日志（所有状态变更记录）
- ✅ 输入验证（@Valid + @Validated）

## 部署模式

### 开发
```bash
mvn clean install
java -jar lakehouse-flow-boot/target/lakehouse-flow-boot-*.jar
```

### Docker
```dockerfile
FROM eclipse-temurin:17-jre
COPY lakehouse-flow-boot/target/lakehouse-flow-boot-*.jar /app/lakehouse-flow.jar
ENTRYPOINT ["java", "-jar", "/app/lakehouse-flow.jar"]
```

### Kubernetes
- 标准 Spring Boot + Actuator 健康检查
- 水平扩展（无状态 API，DB 作为中心）
- ConfigMap 配置注入

## 参考资源

- [Spring Boot 3.2 文档](https://spring.io/projects/spring-boot)
- [Spring Data JPA 文档](https://spring.io/projects/spring-data-jpa)
- [Testcontainers](https://www.testcontainers.org/)
- [Micrometer 文档](https://micrometer.io/)
- [Java 17 新特性](https://www.oracle.com/java/technologies/javase/17-relnotes.html)
