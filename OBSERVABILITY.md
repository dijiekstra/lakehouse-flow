# Lakehouse Flow 可观测性与告警

**适用版本**: LF-1.0

本文档定义 LF-1.0 的指标、Prometheus 告警和排障证据入口。规则文件位于 [deploy/prometheus/lakehouse-flow-alerts.yml](./deploy/prometheus/lakehouse-flow-alerts.yml)，生产故障处置见 [OPERATIONS_RUNBOOK.md](./OPERATIONS_RUNBOOK.md)。

## 1. 观测边界

Lakehouse Flow 只观测调度决策、intent 传输和 snapshot/source 证据。告警不采集、不推断下游 Flink/Spark 运行状态：

- scheduling decision failure 表示调度事务失败关闭；
- delivery exhausted 表示传输进入死信；
- source gap 表示 snapshot 证据链不可信；
- target snapshot backlog 表示已发布 intent 尚无可归因 snapshot。

这四类信号不能折叠成一个通用“任务失败”。

## 2. Prometheus 接入

应用通过 `/actuator/prometheus` 暴露指标。Prometheus 需要抓取每个 scheduler 实例，并加载规则文件：

```yaml
rule_files:
  - /etc/prometheus/rules/lakehouse-flow-alerts.yml

scrape_configs:
  - job_name: lakehouse-flow
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - lakehouse-flow-0:8080
          - lakehouse-flow-1:8080
```

多实例 counter 使用 `sum(increase(...))` 聚合，持久化 backlog gauge 使用 `sum(...)` 聚合。所有规则只使用枚举型低基数标签；Flow、asset、snapshot 和 intent key 留在 API、数据库审计和结构化日志中。

规则加载前使用部署环境中的 Prometheus 版本执行：

```bash
promtool check rules deploy/prometheus/lakehouse-flow-alerts.yml
```

本仓库不固定 Prometheus 容器版本。生产应使用与实际 Prometheus 服务一致的 `promtool`，避免本地语法版本与运行环境漂移。

## 3. 指标契约

| Prometheus metric | 标签 | 含义 |
|---|---|---|
| `lakehouse_flow_scheduling_decision_trigger_evaluations_total` | `decision` | FlowPlan 触发决策计数，`failed` 表示事务失败关闭 |
| `lakehouse_flow_snapshot_source_retention_gap` | `source_type`,`source_name` | durable offset 之后的 source 历史已缺失 |
| `lakehouse_flow_snapshot_source_projection_inconsistent` | `source_type`,`source_name` | event 或 AssetState 投影与 source 不一致 |
| `lakehouse_flow_scheduling_intent_delivery_publisher_attempts_total` | `channel`,`outcome` | 数据处理 intent 传输尝试 |
| `lakehouse_flow_job_control_intent_delivery_publisher_attempts_total` | `channel`,`outcome` | START/RESTART intent 传输尝试 |
| `lakehouse_flow_scheduling_intent_delivery_records` | `channel`,`status` | 数据处理 delivery 持久化库存 |
| `lakehouse_flow_job_control_intent_delivery_records` | `channel`,`status` | 作业控制 delivery 持久化库存 |
| `lakehouse_flow_scheduling_backlog_task_instances` | `state`,`wait_phase` | scheduler 持有的非终态 task 数量 |
| `lakehouse_flow_scheduling_backlog_backfill_blocked_items` | `status`,`category` | 补数日期并发或 DAG 依赖阻塞数量 |

decision 和两类 delivery outcome 在应用启动时预注册为零值，使第一次失败能够被 `increase()` 捕获。source gauge 在配置的 source 完成首次 reconciliation 后出现。

## 4. 告警与证据

| 告警 | 级别 | 初始条件 | 首要证据 |
|---|---|---|---|
| `LakehouseFlowDecisionFailure` | critical | 5 分钟内出现失败决策 | Actuator metric、结构化 scheduler 日志、关联 Flow/action 审计 |
| `LakehouseFlowSnapshotRetentionGap` | critical | source gap 持续 5 分钟 | `/api/v1/operations/snapshot-sources` |
| `LakehouseFlowSnapshotProjectionInconsistent` | critical | 投影不一致持续 5 分钟 | source API 的 offset/projection/detail 证据 |
| `LakehouseFlowDataIntentDeliveryExhausted` | warning | 5 分钟内新增数据 intent 死信 | 数据 delivery dead-letter API |
| `LakehouseFlowJobControlIntentDeliveryExhausted` | warning | 5 分钟内新增控制 intent 死信 | job-control delivery dead-letter API |
| `LakehouseFlowReadyIntentBacklog` | warning | READY 聚合积压持续 15 分钟 | blockers `INTENT_PUBLICATION` |
| `LakehouseFlowTargetSnapshotBacklog` | warning | SCHEDULED 聚合积压持续 45 分钟 | blockers `TARGET_SNAPSHOT` 与 source API |

规则 annotation 中的 `evidence_api` 是相对 Lakehouse Flow 服务地址的查询路径，`runbook` 是仓库内操作手册锚点。部署平台可以在通知模板中拼接服务地址和文档站点地址。

## 5. 阈值性质

当前 `15m` 和 `45m` 是 LF-1.0 的功能性初始门槛，不是容量 SLA：

- READY 规则用于发现 publisher、Flow 并发、writer admission 或日期槽长时间不能形成 intent；
- SCHEDULED 规则按默认 `PT1H` confirmation timeout 的 75% 提前预警；
- 如果 Flow 使用不同 confirmation policy，应在生产容量与延迟证据形成后拆分 recording rule 或调整阈值；
- 不根据尚未测量的 Flow 数、snapshot 速率或 intent 积压设置绝对容量阈值。

任何阈值调整都必须保留 decision、delivery、snapshot 和 source 四个正交维度。

## 6. 告警关闭条件

- decision failure：最近 5 分钟没有新的失败，并已定位对应异常日志；
- source gap：对应 source gauge 回到 0，source API 显示 `HEALTHY` 且 offset/event/AssetState 对账一致；
- delivery exhausted：增量告警自然关闭后，死信记录仍保留，事故需在 dead-letter API 证据上完成处置；
- READY backlog：intent 已形成或阻塞原因已被接受并建立维护窗口；
- SCHEDULED backlog：出现可归因目标 snapshot，或在 source 健康前提下形成 `SNAPSHOT_NOT_ADVANCED`。

Alertmanager 的静默只控制通知，不改变 Lakehouse Flow 的调度、delivery 或 snapshot 状态。

## 7. 验证清单

1. `/actuator/prometheus` 可见本文件列出的全部静态 decision、delivery 和 backlog 指标。
2. 每个配置 source 完成 reconciliation 后出现 source gauge。
3. Prometheus rule 页面显示 `lakehouse-flow-*` groups 加载成功。
4. 在非生产环境注入一次 decision failure 或传输耗尽，确认通知包含 `evidence_api` 和 `runbook`。
5. 验证告警恢复不会修改 dead-letter、task、intent、offset 或 AssetState。
6. 确认通知内容没有把 delivery 或 source 问题描述为下游执行失败。
