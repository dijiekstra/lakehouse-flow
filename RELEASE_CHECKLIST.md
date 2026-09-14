# LF-1.0 发布清单

**最后更新**: 2026-09-14
**当前版本线**: `1.0.0-SNAPSHOT`
**当前状态**: 功能与稳定性门槛已闭合，等待发布候选门禁和受信环境试运行；尚未创建最终 `v1.0.0` 标签。

## 不可变边界

1. Lakehouse Flow 只做调度决策、调度意图投递、审计和 snapshot 确认，不提交、托管或查询下游任务运行状态。
2. 数据处理结果只由 baseline 之后可归因的目标业务 snapshot 判定。传输 ACK、Flink 状态和执行平台回调不能替代 snapshot 证据。
3. `SNAPSHOT_CONFIRMED`、`SNAPSHOT_NOT_ADVANCED`、`DELIVERY_EXHAUSTED` 和 `SOURCE_BLOCKED` 保持正交，不能折叠为执行状态机。
4. 正式通道必须支持 `DATABASE_TABLE` 和 HTTP。一个 intent 只能选择一个出站路由，下游必须以 `intentKey` 幂等消费。
5. `/api/v1`、`SchedulingIntent` 1.3、`JobControlIntent` 1.0 和 Flyway V1-V23 是 LF-1.0 冻结基线。

## 发布候选门禁

1. 候选提交必须来自干净工作区，Maven reactor 版本必须位于 `1.0.0-SNAPSHOT` 或 `1.0.0-rcN` 版本线。
2. 手动运行 GitHub Actions 的 `LF-1.0 Release Candidate` workflow。候选标签使用 `v1.0.0-rcN` 时，标签与 Maven 版本必须完全一致。
3. workflow 必须在 JDK 17 下执行不带 `-DskipITs` 的 `./mvnw -B -ntp clean verify`，任何单测、覆盖率门槛或整体 Testcontainers E2E 失败都阻断发布。
4. 当前验收基线是 396 个单元/启动测试、8 个整体 E2E、Service line 91.37%、branch 68.98%。测试数量可以随实现增加，但不能通过删除测试或放宽门槛制造通过结果。
5. 候选制品必须是可执行 Spring Boot JAR，并包含 REST surface、SchedulingIntent schema 和 JobControlIntent schema 三份冻结契约。
6. 候选包必须包含 build identity、关键接入文档和 `SHA256SUMS`；下载后重新计算校验和再部署。
7. Flyway 必须从空库完整迁移到 V23，并至少验证一次受支持的 V22 到 V23 升级路径。已发布 migration 禁止改写。

## 受信环境试运行

1. 备份 PostgreSQL 并执行 Flyway validate；关闭 baseline-on-migrate、clean 和任何自动降级脚本。
2. 先启动单 scheduler 验证 source、冻结契约和只读运维 API，再扩到多 scheduler，确认共享数据库下 claim、offset 和目标准入无重复推进。
3. 接入一个真实 Paimon Catalog、Flink CDC ODS writer 和 DWD/DWS/ADS 批式 writer。作业启动或重启只能由平台通过 `JobControlIntent` 执行。
4. 分别验证 `DATABASE_TABLE` 与 HTTP 能被平台被动消费。同一测试 intent 不得同时启用两个路由。
5. 下游写入必须原样携带 `requiredSnapshotProperties`，并在 Paimon commit 期间执行 `writerJobKey + writerEpoch` fencing。
6. 演练普通 DAG、workflow/task/node 重跑、完整 Flow/Node 补数、失败恢复和流式 writer 受控补数切换。
7. 演练 snapshot 确认、健康 source 下未推进、投递耗尽和 source 阻塞，确认四类结果可通过 API、指标和证据查询，且没有任务状态回调依赖。
8. 按 `OPERATIONS_RUNBOOK.md` 演练 delivery 死信、source gap、scheduler 中断、租约重占和 PostgreSQL 恢复。

## 最终发布

1. 试运行结论、候选 workflow run、候选提交和制品 SHA-256 必须形成同一份发布记录。
2. 将 Maven reactor 版本统一改为 `1.0.0`，重新运行完整发布候选 workflow；不得复用 SNAPSHOT 或 RC 版本的构建产物。
3. 完整门禁通过后，在同一提交创建 annotated tag `v1.0.0`。标签创建前不得对外宣称最终版已发布。
4. 发布后 V1-V23 migration 保持不可变；后续数据库变化从 V24 开始，REST v1 与 intent 同 major 只允许向后兼容扩展。
5. 应用回滚使用上一份已验证制品。数据库不得执行 Flyway clean 或破坏证据链的向下迁移。

## 发布后工作

容量基线在真实生产负载下采集，包括 Flow 数、节点数、受管资产数、snapshot 速率、intent 积压、扫描延迟和恢复时间。在数据形成前不承诺未经测量的 SLA，也不以实验室 E2E 结果替代生产容量结论。
