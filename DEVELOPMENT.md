# 开发与部署指南

## 环境设置

### 系统要求

- **Java**：17 或更新版本
  ```bash
  java -version
  # openjdk version "17.0.x"
  ```

- **Maven**：3.8 或更新版本
  ```bash
  mvn -version
  # Apache Maven 3.8.x
  ```

- **数据库**：MySQL 5.7+ 或 PostgreSQL 10+
  ```bash
  # MySQL
  mysql --version
  # ver 5.7.x, 8.0.x
  
  # PostgreSQL
  psql --version
  # psql (PostgreSQL) 10.x, 12.x, 14.x
  ```

- **Docker & Docker Compose**（可选，用于本地开发）
  ```bash
  docker --version
  docker-compose --version
  ```

### 快速启动（Docker）

```bash
# 启动 MySQL 和应用
docker-compose up

# 首次启动会自动初始化数据库
# 应用将在 http://localhost:8080 可访问
```

### 手动启动

#### 1. 创建数据库

```bash
# MySQL
mysql -u root -p
CREATE DATABASE lakehouse_flow DEFAULT CHARSET utf8mb4;
EXIT;

# PostgreSQL
psql -U postgres
CREATE DATABASE lakehouse_flow;
\quit
```

#### 2. 初始化表结构

```bash
# 自动初始化（推荐）
mvn clean package
# Spring Boot 启动时自动执行 migration

# 或手动执行
mysql -u root -p lakehouse_flow < db/migration/init_schema.sql
```

#### 3. 启动应用

```bash
# 方式 1：使用 Maven
mvn spring-boot:run

# 方式 2：直接运行 JAR
java -jar lakehouse-flow-service/target/lakehouse-flow-*.jar

# 方式 3：指定数据库配置
java -Dspring.datasource.url=jdbc:mysql://localhost:3306/lakehouse_flow \
     -Dspring.datasource.username=root \
     -Dspring.datasource.password=password \
     -jar lakehouse-flow-service/target/lakehouse-flow-*.jar
```

#### 4. 验证启动

```bash
# 查看健康状态
curl http://localhost:8080/actuator/health

# 期望响应
# {"status":"UP"}
```

## 项目结构

```
lakehouse-flow/
├── docs/                                  文档和设计资料
│   ├── API.md                            API 参考
│   ├── DEPLOYMENT.md                     部署指南
│   └── examples/                         示例场景
│
├── lakehouse-flow-core/                  核心领域模型和服务
│   ├── src/main/java/com/lakehouse/flow/core/
│   │   ├── asset/                        资产模型
│   │   ├── event/                        事件模型
│   │   ├── dependency/                   依赖解析
│   │   ├── trigger/                      触发机制
│   │   └── state/                        状态管理
│   └── src/test/java/                    单元测试
│
├── lakehouse-flow-service/               Spring Boot 服务
│   ├── src/main/java/com/lakehouse/flow/service/
│   │   ├── api/                          REST API
│   │   ├── scheduler/                    调度循环
│   │   ├── executor/                     执行器
│   │   └── config/                       配置
│   ├── src/main/resources/
│   │   ├── application.yml               应用配置
│   │   ├── application-dev.yml           开发配置
│   │   ├── application-prod.yml          生产配置
│   │   └── db/migration/                 数据库迁移脚本
│   └── src/test/java/                    集成测试
│
├── lakehouse-flow-executor/              执行器实现
│   ├── shell-executor/                   Shell 执行器
│   ├── sql-executor/                     SQL 执行器
│   └── http-executor/                    HTTP 执行器
│
├── db/                                   数据库脚本
│   ├── migration/
│   │   ├── init_schema.sql               初始表结构
│   │   └── v1_add_quality_status.sql     版本化迁移
│   └── examples/
│       └── sample_data.sql               示例数据
│
├── pom.xml                               Maven 根 POM
├── docker-compose.yml                    Docker 组合启动
└── README.md                             项目概览
```

## 代码规范

### Java 风格

遵循 Google Java Style Guide：

```java
// 类声明
public class AssetStateService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AssetStateService.class);
  
  private final AssetStateRepository repository;
  private final EventService eventService;
  
  // 构造函数使用 @Autowired（隐式）或显式依赖注入
  @Autowired
  public AssetStateService(AssetStateRepository repository, EventService eventService) {
    this.repository = repository;
    this.eventService = eventService;
  }
  
  // 方法要包含 JavaDoc
  /**
   * 更新资产状态（带乐观锁保护）
   *
   * @param assetKey 资产唯一标识
   * @param event    触发事件
   * @return true 如果状态成功推进，false 如果乱序或并发冲突
   * @throws IllegalArgumentException 如果参数无效
   */
  public boolean updateState(String assetKey, LakehouseEvent event) {
    // 实现
  }
}
```

### 日志规范

```java
LOGGER.info("Asset state updated", new LogContext()
    .assetKey(assetKey)
    .snapshotId(snapshotId)
    .version(version)
    .build());

LOGGER.warn("Asset event ignored due to out-of-order", new LogContext()
    .assetKey(assetKey)
    .eventSnapshotId(event.getSnapshotId())
    .currentSnapshotId(state.getLatestSnapshotId())
    .build());

LOGGER.error("Asset state update failed", exception, new LogContext()
    .assetKey(assetKey)
    .retryCount(retryCount)
    .build());
```

### 命名规范

```
类名：AssetStateService, EventIngestionLoop, DependencyResolver
方法名：updateAssetState, evaluateDependency, resolveDependencies
变量名：assetKey, latestSnapshotId, dependencyGroup
常量名：DEFAULT_BATCH_SIZE, MAX_RETRIES
```

### 数据库字段命名

```sql
-- 遵循 snake_case
asset_key
latest_snapshot_id
quality_status
trigger_history
dependency_group
```

## 测试指南

### 单元测试

使用 JUnit 5 + Mockito：

```java
@ExtendWith(MockitoExtension.class)
class AssetStateServiceTest {
  
  @Mock
  private AssetStateRepository repository;
  
  @Mock
  private EventService eventService;
  
  @InjectMocks
  private AssetStateService service;
  
  @Test
  void testUpdateStateSuccessful() {
    // Given
    AssetState state = new AssetState("paimon://db/table", 1000L);
    LakehouseEvent event = new LakehouseEvent("paimon://db/table", 1001L);
    
    when(repository.findByAssetKey("paimon://db/table"))
        .thenReturn(Optional.of(state));
    when(repository.updateWithOptimisticLock(any()))
        .thenReturn(1);  // 1 row affected
    
    // When
    boolean result = service.updateState("paimon://db/table", event);
    
    // Then
    assertTrue(result);
    verify(repository).updateWithOptimisticLock(any());
  }
  
  @Test
  void testUpdateStateOutOfOrder() {
    // Given
    AssetState state = new AssetState("paimon://db/table", 2000L);  // 当前状态更新
    LakehouseEvent event = new LakehouseEvent("paimon://db/table", 1001L);  // 旧事件
    
    when(repository.findByAssetKey("paimon://db/table"))
        .thenReturn(Optional.of(state));
    
    // When
    boolean result = service.updateState("paimon://db/table", event);
    
    // Then
    assertFalse(result);
    verify(repository, never()).updateWithOptimisticLock(any());
  }
}
```

### 集成测试

使用 Testcontainers + SpringBootTest：

```java
@SpringBootTest
@Testcontainers
class AssetEventIngestionIntegrationTest {
  
  @Container
  static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
      .withDatabaseName("lakehouse_flow_test")
      .withUsername("test")
      .withPassword("test");
  
  @Autowired
  private EventIngestionService eventService;
  
  @Autowired
  private AssetStateService stateService;
  
  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", mysql::getJdbcUrl);
    registry.add("spring.datasource.username", mysql::getUsername);
    registry.add("spring.datasource.password", mysql::getPassword);
  }
  
  @Test
  void testEndToEndEventProcessing() throws Exception {
    // Given
    Asset asset = new Asset("paimon://ods_db/order_latest", "PAIMON", "ods_db", "ods_db", "order_latest");
    assetRepository.save(asset);
    
    LakehouseEvent event = new LakehouseEvent(
        "paimon_order_latest_snapshot_1001",
        "paimon://ods_db/order_latest",
        1001L,
        System.currentTimeMillis(),
        "SNAPSHOT_COMMITTED"
    );
    
    // When
    eventService.ingestEvent(event);
    Thread.sleep(100);  // 等待处理
    
    // Then
    AssetState state = stateService.getState("paimon://ods_db/order_latest");
    assertEquals(1001L, state.getLatestSnapshotId());
  }
}
```

### 运行测试

```bash
# 运行所有单元测试
mvn test

# 运行所有集成测试（需要 Docker）
mvn verify

# 运行特定测试类
mvn test -Dtest=AssetStateServiceTest

# 运行特定测试方法
mvn test -Dtest=AssetStateServiceTest#testUpdateStateSuccessful

# 生成覆盖率报告
mvn jacoco:report
# 报告位置：target/site/jacoco/index.html
```

## 配置

### application.yml

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/lakehouse_flow?characterEncoding=utf8mb4&useSSL=false
    username: root
    password: password
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      
  jpa:
    hibernate:
      ddl-auto: validate  # 生产环境不要 create-drop
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.MySQL8Dialect

  jackson:
    default-property-inclusion: non_null
    date-format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    time-zone: UTC

logging:
  level:
    root: INFO
    com.lakehouse: DEBUG
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
  file:
    name: logs/lakehouse-flow.log
    max-size: 10MB
    max-history: 30

lakehouse-flow:
  # 事件摄入配置
  event-source:
    paimon-polling:
      enabled: true
      interval-seconds: 30
      batch-size: 500
      max-retries: 3
      
    push-receiver:
      enabled: true
      api-path: /api/v1/asset-events
      max-payload-size: 1MB
  
  # 资产状态配置
  asset-state:
    enable-optimistic-locking: true
    max-update-retries: 3
    cache-expire-seconds: 60
  
  # 依赖解析配置
  dependency-resolver:
    cache-expire-seconds: 60
    resolve-timeout-seconds: 5
    max-concurrent-evaluations: 100
  
  # 幂等触发配置
  trigger:
    enable-duplicate-check: true
    max-pending-triggers: 10000
  
  # 补偿扫描配置
  compensation-scanner:
    enabled: true
    interval-seconds: 300           # 每 5 分钟运行一次
    batch-size: 100
    missed-events-check-enabled: true
    failed-triggers-check-enabled: true
    stuck-tasks-check-enabled: true
    stuck-task-threshold-minutes: 360  # 6 小时
    lost-callback-check-enabled: true
    lost-callback-threshold-minutes: 120  # 2 小时
  
  # 执行器配置
  executor:
    shell:
      enabled: true
      timeout-seconds: 3600
      max-concurrent: 10
    sql:
      enabled: true
      timeout-seconds: 1800
      max-concurrent: 5
    http:
      enabled: true
      timeout-seconds: 300
      max-concurrent: 20
      max-retries: 3
```

### application-prod.yml

```yaml
spring:
  datasource:
    url: jdbc:mysql://mysql-prod:3306/lakehouse_flow
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    hikari:
      maximum-pool-size: 50
      
  jpa:
    show-sql: false
    
logging:
  level:
    root: WARN
    com.lakehouse: INFO

lakehouse-flow:
  event-source:
    paimon-polling:
      interval-seconds: 30
      batch-size: 1000
      max-retries: 5
  
  compensation-scanner:
    enabled: true
    interval-seconds: 600
```

## 本地调试

### IDE 配置

**IntelliJ IDEA**：

1. 打开项目
2. File → Open → 选择 `pom.xml`
3. Maven 会自动下载依赖
4. 右键 `lakehouse-flow-service` → Run 'Application'

**VS Code**：

1. 安装 Extension Pack for Java
2. Command Palette → Java: Create Java Project
3. 选择 Maven 模板

### 断点调试

```java
// 在 AssetStateService.updateState() 中打断点
public boolean updateState(String assetKey, LakehouseEvent event) {
  LOGGER.debug("Updating asset state", assetKey, event.getSnapshotId());
  
  // 此处打断点
  AssetState state = repository.findByAssetKey(assetKey)
      .orElseThrow();
  
  if (event.getSnapshotId() <= state.getLatestSnapshotId()) {
    return false;  // 乱序
  }
  
  return repository.updateWithOptimisticLock(/* ... */) > 0;
}
```

运行调试：
```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--debug"
# 在 IDE 中 Debug As → Java Application
```

### 日志查看

```bash
# 查看实时日志
tail -f logs/lakehouse-flow.log

# 过滤特定模块的日志
grep "com.lakehouse.flow.service" logs/lakehouse-flow.log

# 查看错误堆栈
grep -A 50 "ERROR" logs/lakehouse-flow.log
```

## 性能测试

### 基准测试

```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 2, jvmArgs = {"-Xms1G", "-Xmx1G"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class AssetStateBenchmark {
  
  @Benchmark
  public boolean updateAssetState(BenchmarkState state) {
    return state.assetStateService.updateState(
        state.assetKey,
        state.generateEvent()
    );
  }
  
  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public AssetStateService assetStateService;
    public String assetKey;
    
    @Setup
    public void setup() {
      // 初始化
    }
  }
}

// 运行
mvn clean install
java -jar target/benchmarks.jar
```

### 负载测试

```bash
# 使用 Apache JMeter
jmeter -n -t test_plan.jmx -l results.jtl -j jmeter.log

# 或使用 Gatling
gatling.sh -s LoadTestSimulation

# 或使用 wrk
wrk -t12 -c400 -d30s http://localhost:8080/api/v1/assets
```

### 性能指标

| 操作 | 吞吐量 | 延迟 (P50) | 延迟 (P99) |
|------|-------|-----------|-----------|
| 事件摄入 | 1000 events/sec | 1ms | 10ms |
| 资产状态更新 | 500 updates/sec | 2ms | 20ms |
| 依赖评估 | 100 evaluations/sec | 10ms | 50ms |
| 触发历史插入 | 200 inserts/sec | 5ms | 30ms |

## 常见问题

### 启动报错："Connection refused"

**原因**：数据库未启动或连接信息错误

**解决**：
```bash
# 检查数据库是否运行
mysql -u root -ppassword -h 127.0.0.1

# 检查连接字符串
grep spring.datasource.url application.yml

# 或使用 Docker
docker run -d -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=lakehouse_flow \
  -p 3306:3306 mysql:8.0
```

### 表不存在

**原因**：数据库初始化脚本未执行

**解决**：
```bash
# 手动初始化
mysql -u root -ppassword lakehouse_flow < db/migration/init_schema.sql

# 或启用自动初始化
spring.jpa.hibernate.ddl-auto: create
```

### 内存溢出

**原因**：Heap 内存不足

**解决**：
```bash
java -Xms2G -Xmx4G -jar lakehouse-flow-*.jar
```

## 打包与发布

### 构建 JAR

```bash
# 构建所有模块
mvn clean package

# 跳过测试
mvn clean package -DskipTests

# 输出文件
# lakehouse-flow-service/target/lakehouse-flow-service-1.0.0.jar
```

### 构建 Docker 镜像

```bash
# 使用 Maven 插件
mvn clean package docker:build

# 或手动
docker build -f Dockerfile -t lakehouse-flow:1.0.0 .

# 推送到仓库
docker tag lakehouse-flow:1.0.0 registry.company.com/lakehouse-flow:1.0.0
docker push registry.company.com/lakehouse-flow:1.0.0
```

### 部署到 Kubernetes

```bash
# 创建 ConfigMap
kubectl create configmap lakehouse-flow-config \
  --from-file=application.yml

# 部署
kubectl apply -f k8s/deployment.yaml

# 验证
kubectl get pods -l app=lakehouse-flow
kubectl logs -f deployment/lakehouse-flow
```

