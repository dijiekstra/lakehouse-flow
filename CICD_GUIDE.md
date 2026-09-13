# GitHub Actions CI/CD 配置指南

## 概述

本项目已配置 GitHub Actions 自动化构建、测试和部署流程。

---

## 配置的工作流

### 1. Maven Build and Test (`maven-build.yml`)

**触发条件**:
- 在 `main`, `master`, `develop` 分支上执行 push
- 提交 PR 到 `main`, `master`, `develop` 分支

**执行步骤**:
1. 检出代码
2. 设置 JDK 17
3. 使用 Maven 编译代码
4. 启动 PostgreSQL 服务容器
5. 执行单元测试
6. 执行集成测试 (`mvn verify`)
7. 上传测试报告
8. 发布测试结果

**环境变量**:
- `MAVEN_OPTS=-Xmx2g` (增加 Maven 堆内存)
- `SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/lakehouse_flow`
- `SPRING_DATASOURCE_USERNAME=postgres`
- `SPRING_DATASOURCE_PASSWORD=postgres`

**PostgreSQL 服务**:
- 版本: 14
- 数据库: `lakehouse_flow`
- 用户名: `postgres`
- 密码: `postgres`
- 端口: 5432

---

### 2. Code Quality Checks (`code-quality.yml`)

**触发条件**:
- 在 `main`, `master`, `develop` 分支上执行 push
- 提交 PR 到 `main`, `master`, `develop` 分支

**执行步骤**:
1. 检出代码
2. 设置 JDK 17
3. 代码风格检查 (Maven 编译)
4. 代码质量检查 (可选)
5. 安全扫描 (可选)
6. 生成质量报告

**备注**:
- Checkstyle 和 Dependency-check 为可选配置
- 如需启用，请在 `pom.xml` 中添加相应 Maven 插件

---

### 3. Build Artifacts (`build-artifacts.yml`)

**触发条件**:
- 推送到 `main` 分支
- 创建标签 (例如: `v1.0.0`)
- 手动触发 (workflow_dispatch)

**执行步骤**:
1. 检出代码
2. 设置 JDK 17
3. 提取版本信息
4. 构建所有模块 (跳过测试)
5. 生成构建信息
6. 上传构建产物

**产物**:
- JAR 文件: `lakehouse-flow-boot-*.jar`
- 构建信息: `BUILD_INFO.txt`

---

## 工作流状态和日志

### 查看工作流状态

1. 访问 GitHub 仓库
2. 点击 **Actions** 标签
3. 查看各工作流的运行状态

### 查看构建日志

1. 点击某个工作流运行
2. 查看具体步骤的日志
3. 下载构建产物（如有）

---

## 配置说明

### 环境变量

所有工作流共享以下环境配置：

| 变量 | 值 | 说明 |
|------|-----|------|
| `JAVA_VERSION` | 17 | JDK 版本 |
| `MAVEN_OPTS` | `-Xmx2g` | Maven 堆内存 |
| `DB_NAME` | `lakehouse_flow` | 数据库名 |
| `DB_USER` | `postgres` | 数据库用户 |
| `DB_PASSWORD` | `postgres` | 数据库密码 |

### 工作流权限

所有工作流拥有以下权限：

- `contents: read` - 读取仓库内容
- `packages: write` - 写入 GitHub Packages (用于发布)

---

## 自定义和扩展

### 添加新的工作流步骤

编辑 `.github/workflows/` 下的 YAML 文件：

```yaml
- name: Custom Step Name
  run: |
    # 执行自定义命令
    echo "Hello CI/CD"
```

### 添加代码覆盖率报告

```yaml
- name: Generate Coverage Report
  run: mvn clean test jacoco:report

- name: Upload Coverage to Codecov
  uses: codecov/codecov-action@v3
```

### 添加代码分析 (SonarQube)

```yaml
- name: SonarQube Analysis
  run: mvn clean verify sonar:sonar -Dsonar.projectKey=lakehouse-flow
```

---

## 构建状态徽章

在 README.md 中添加构建状态徽章：

```markdown
![Build Status](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/maven-build.yml/badge.svg)
![Code Quality](https://github.com/dijiekstra/lakehouse-flow/actions/workflows/code-quality.yml/badge.svg)
```

---

## 故障排查

### 问题: Build 超时

**原因**: 编译或测试耗时过长

**解决方案**:
- 检查日志中的耗时步骤
- 优化代码或分解测试
- 增加超时时间 (在 workflow YAML 中添加 `timeout-minutes`)

### 问题: PostgreSQL 连接失败

**原因**: 服务启动延迟或网络问题

**解决方案**:
```yaml
# 检查服务状态
- name: Check PostgreSQL
  run: |
    until pg_isready -h localhost -p 5432; do
      echo "等待 PostgreSQL 启动..."
      sleep 2
    done
```

### 问题: Maven 内存不足

**原因**: 堆内存设置过小

**解决方案**:
```yaml
env:
  MAVEN_OPTS: -Xmx4g  # 增加到 4GB
```

---

## 监控和告警

### GitHub 通知设置

1. 进入 **Settings → Notifications**
2. 启用 "Workflows" 通知
3. 选择通知方式 (邮件 / Web)

### Slack 集成

添加 Slack 通知步骤：

```yaml
- name: Slack Notification
  if: failure()
  uses: slackapi/slack-github-action@v1
  with:
    payload: |
      {
        "text": "Build failed: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
      }
```

---

## 性能优化

### 1. 使用 Maven 缓存

已在工作流中配置：
```yaml
cache: maven
```

这会缓存 Maven 依赖，加快后续构建。

### 2. 并行执行模块

在 `mvn` 命令中添加：
```bash
mvn -T 1C clean verify  # 每个 CPU 核心一个线程
```

### 3. 跳过不必要的步骤

```yaml
- name: Skip Tests in Build Phase
  run: mvn clean package -DskipTests
```

---

## 版本发布流程

### 使用标签发布

```bash
# 创建版本标签
git tag -a v1.0.0 -m "Release version 1.0.0"

# 推送标签
git push origin v1.0.0
```

当推送标签时，`build-artifacts.yml` 工作流会自动触发，生成发布产物。

---

## 参考资源

- [GitHub Actions 官方文档](https://docs.github.com/en/actions)
- [Maven GitHub Actions](https://github.com/actions/setup-java)
- [PostgreSQL Docker](https://hub.docker.com/_/postgres)
- [EnricoMi/publish-unit-test-result-action](https://github.com/EnricoMi/publish-unit-test-result-action)

---

## 下一步

1. **监控首次构建**: 推送代码到 main 分支，观察 GitHub Actions 运行
2. **优化工作流**: 根据实际需求调整超时时间、内存分配等
3. **添加覆盖率报告**: 集成 Codecov 或 JaCoCo
4. **配置通知**: 设置构建失败时的告警
5. **发布流程**: 准备版本发布脚本

---

**最后更新**: 2026-09-13  
**配置者**: Copilot CI/CD Assistant
