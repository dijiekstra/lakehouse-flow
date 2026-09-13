# Lakehouse Flow 文档索引

**最后更新**: 2026-09-13
**阶段**: Snapshot 推进式调度系统持续实现

> 后续开发进度、版本目标和差距检查以 [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) 为准。
>
> 当前架构语义以 [README.md](./README.md)、[ARCHITECTURE.md](./ARCHITECTURE.md) 和 [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) 为准。标记为“历史”的文档只用于追溯早期方案，其中的 executor callback、`RUNNING/SUCCESS/FAILED` 不代表当前系统设计。

## 📚 快速导航

### 🎯 系统定位和设计原则

| 文档 | 用途 | 长度 |
|------|------|------|
| [README.md](./README.md) | **必读** - 当前系统边界、能力和运行方式 | 5分钟 |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | **权威** - 当前 snapshot 调度架构和核心循环 | 15分钟 |
| [CLARIFICATION_SUMMARY.md](./CLARIFICATION_SUMMARY.md) | 历史 - 早期定位澄清 | 5分钟 |
| [ARCHITECTURE_CLARIFICATION.md](./ARCHITECTURE_CLARIFICATION.md) | 历史 - 旧集成协议，不作为当前实现依据 | 10分钟 |

### 🚀 实现指南

| 文档 | 用途 | 长度 |
|------|------|------|
| [PHASE1_COMPLETION.md](./PHASE1_COMPLETION.md) | 历史 - Phase 1 完成报告 | 10分钟 |
| [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) | **进度基准** - 版本目标、差距清单、当前推进项和验证口径 | 10分钟 |
| [PHASE2_DESIGN.md](./PHASE2_DESIGN.md) | 历史 - 旧 Phase 2 设计，不作为当前状态语义 | 20分钟 |
| [PHASE2_ROADMAP.md](./PHASE2_ROADMAP.md) | 历史 - 旧路线图 | 15分钟 |

### 💡 原理和设计

| 文档 | 用途 | 长度 |
|------|------|------|
| [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) | **权威** - snapshot 对象模型、Flow 隔离、action 和补数 DAG 原则 | 20分钟 |
| [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) | **权威** - 下游指令字段、消费幂等和 snapshot 归因约定 | 10分钟 |
| [DESIGN_PRINCIPLES.md](./DESIGN_PRINCIPLES.md) | 历史 - 原则草案，冲突时服从权威文档 | 10分钟 |
| [GLOSSARY.md](./GLOSSARY.md) | 历史 - 旧术语和执行状态机 | 10分钟 |

### 🔧 开发和部署

| 文档 | 用途 | 长度 |
|------|------|------|
| [README.md](./README.md) | 项目概览、快速开始、特性列表 | 5分钟 |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | 本地开发环境、代码规范、编译运行 | 10分钟 |
| [TECH_STACK.md](./TECH_STACK.md) | 技术栈详情、依赖版本、配置 | 5分钟 |

### 🎓 学习资料

| 文档 | 用途 | 长度 |
|------|------|------|
| [lakehouse-asset-event-scheduling.md](./lakehouse-asset-event-scheduling.md) | 参考文档：snapshot 驱动调度的原理 | 30分钟 |
| [现代CDC湖仓架构下的数仓与数据平台演进.md](./现代CDC湖仓架构下的数仓与数据平台演进.md) | 参考文档：湖仓架构背景 | 30分钟 |

---

## 🎯 不同角色的阅读指南

### 👨‍💼 项目经理 / 产品经理

**目标**: 了解系统能做什么

**推荐阅读顺序**:
1. [README.md](./README.md) - 项目概览和特性
2. [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - 对象模型和 action 能力
3. [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 当前完成度和剩余差距

**时间投入**: 20 分钟

---

### 👨‍💻 后端开发 / 架构师

**目标**: 理解系统设计和实现细节

**推荐阅读顺序**:
1. [README.md](./README.md) - 当前边界
2. [ARCHITECTURE.md](./ARCHITECTURE.md) - 当前架构
3. [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - 对象模型、Flow 隔离和 action 语义
4. [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 实现进度和剩余差距

**时间投入**: 1 小时

---

### 🔨 Phase 2 实现工程师

**目标**: 按照设计实现 Phase 2 功能

**推荐阅读顺序**:
1. [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 当前版本目标和差距基准
2. [ARCHITECTURE.md](./ARCHITECTURE.md) - 当前组件与循环
3. [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - 目标语义和不变量
4. [DEVELOPMENT.md](./DEVELOPMENT.md) - 本地开发环境

**时间投入**: 1.5 小时

---

### 🧪 测试工程师

**目标**: 设计测试用例和测试策略

**推荐阅读顺序**:
1. [README.md](./README.md) - 功能特性和测试入口
2. [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - DAG、snapshot 和 action 不变量
3. [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 当前验收基线和测试策略

**时间投入**: 1 小时

---

### 📊 下游系统集成工程师

**目标**: 集成 Lakehouse Flow 到自己的系统

**推荐阅读顺序**:
1. [README.md](./README.md) - 理解系统定位
2. [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) - 完整指令和 snapshot 归因要求
3. [ARCHITECTURE.md](./ARCHITECTURE.md) - 主动投递与 snapshot 确认流程
4. [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - 下游边界和 action 影响

**时间投入**: 30 分钟

---

## 📋 按主题查找

### 依赖关系和条件评估

- [ARCHITECTURE.md](./ARCHITECTURE.md) - FlowPlan 条件评估、自然触发与 snapshot 证据链
- [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - DependencySpec、DAG 门禁和调度实例语义
- [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 当前实现范围与尚未完成项

### Flow 隔离、重跑和补数

- [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - FlowPlan 隔离 / 权限 / Rerun / Backfill

### 并发和幂等性

- [ARCHITECTURE.md](./ARCHITECTURE.md) - snapshot 事件事务、意图 claim 和版本控制
- [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 已完成的原子性能力和后续并发差距

### 监控和可观测性

- [ARCHITECTURE.md](./ARCHITECTURE.md) - action、intent、snapshot confirmation 审计链
- [PHASE2_PROGRESS.md](./PHASE2_PROGRESS.md) - 生产可观测性待办

### 下游系统集成

- [SCHEDULING_INTENT_CONTRACT.md](./SCHEDULING_INTENT_CONTRACT.md) - 完整 instruction payload 与 snapshot 标记规则
- [README.md](./README.md) - 当前最小 API 与数据库 outbox 示例
- [ARCHITECTURE.md](./ARCHITECTURE.md) - 调度侧与下游执行侧的职责边界
- [SCHEDULING_MODEL_DESIGN.md](./SCHEDULING_MODEL_DESIGN.md) - action 对下游和 snapshot 的影响

### 扩展和定制

- [PHASE2_ROADMAP.md](./PHASE2_ROADMAP.md) - 后续优化方向
- [DEVELOPMENT.md](./DEVELOPMENT.md) - 代码规范和扩展指南

---

## 🗂️ 文件组织

```
lakehouse-flow/
├── 📚 文档（按阅读优先级）
│   ├── README.md                          项目概览 ⭐⭐⭐
│   ├── ARCHITECTURE.md                    当前完整架构 ⭐⭐⭐
│   ├── PHASE2_PROGRESS.md                 当前进度基准 ⭐⭐⭐
│   ├── SCHEDULING_MODEL_DESIGN.md         当前对象模型 ⭐⭐⭐
│   ├── DEVELOPMENT.md                     开发指南 ⭐
│   ├── TECH_STACK.md                      技术栈 ⭐
│   ├── 其余阶段性文档                     历史方案，仅供追溯
│   └── 参考资料
│       ├── lakehouse-asset-event-scheduling.md
│       └── 现代CDC湖仓架构下的数仓与数据平台演进.md
│
├── 📦 代码模块
│   ├── lakehouse-flow-model/               Entity 层
│   ├── lakehouse-flow-dao/                 Repository 层
│   ├── lakehouse-flow-service/             Service 层
│   ├── lakehouse-flow-integration/         适配器层
│   ├── lakehouse-flow-api/                 API 层
│   ├── lakehouse-flow-scheduler/           后台任务层
│   ├── lakehouse-flow-boot/                启动层
│   └── lakehouse-flow-common/              公共工具
│
└── 🔧 配置和脚本
    ├── pom.xml                             Maven 配置
    ├── docker-compose.yml                  本地开发环境
    └── db/                                 数据库脚本
```

---

## 📈 学习路径

### 快速了解（15 分钟）
1. README.md
2. ARCHITECTURE.md（边界与核心链路）

### 深入学习（1 小时）
1. ARCHITECTURE.md
2. SCHEDULING_MODEL_DESIGN.md
3. PHASE2_PROGRESS.md

### 准备开发（2 小时）
1. 以上全部
2. DEVELOPMENT.md
3. README.md（构建与 API 示例）

---

## ✅ 常见问题快速查答

| 问题 | 文档 | 位置 |
|------|------|------|
| Lakehouse Flow 是什么？| README.md | 核心理念 |
| 系统能做什么？| ARCHITECTURE.md | 系统职责 |
| 系统不能做什么？| ARCHITECTURE.md | 边界与非目标 |
| 怎样与下游系统集成？| SCHEDULING_INTENT_CONTRACT.md | 指令结构与下游规则 |
| 当前完成了什么？| PHASE2_PROGRESS.md | 进度基准 |
| 下一步要做什么？| PHASE2_PROGRESS.md | 版本目标与差距 |
| 目标对象模型是什么？| SCHEDULING_MODEL_DESIGN.md | 推荐对象模型 |
| 多用户隔离和共享怎么设计？| SCHEDULING_MODEL_DESIGN.md | Flow 隔离与共享 |
| 重跑和补数怎么设计？| SCHEDULING_MODEL_DESIGN.md | 能力设计 |
| 怎样本地开发？| DEVELOPMENT.md | 快速开始 |
| 数据模型是什么？| SCHEDULING_MODEL_DESIGN.md | 核心对象 |

---

## 🔄 文档更新历史

| 日期 | 事件 | 文档变更 |
|------|------|---------|
| 2026-09-11 | 架构定位澄清 | +CLARIFICATION_SUMMARY.md, 更新 README.md |
| 2026-09-11 | Phase 1 完成 | +PHASE1_COMPLETION.md |
| 2026-09-11 | Phase 2 设计 | +PHASE2_DESIGN.md, +PHASE2_ROADMAP.md |
| 2026-09-10 | 初始架构设计 | +ARCHITECTURE.md, +DESIGN_PRINCIPLES.md, +GLOSSARY.md |
| 2026-09-12 | 对象模型重新设计 | +SCHEDULING_MODEL_DESIGN.md |

---

## 💬 如何使用这个索引

1. **找不到想要的信息？** 
   - 用 Ctrl+F 在本文档中搜索关键词
   - 查看"按主题查找"部分

2. **不知道从哪里开始？**
   - 找到你的角色，按推荐顺序阅读

3. **想快速答疑？**
   - 查看"常见问题快速查答"

4. **需要找到代码对应的文档？**
   - 查看"文件组织"了解代码结构
   - 找到对应模块的文档

---

**最后更新**: 2026-09-11  
**维护者**: Copilot  
**许可证**: Apache 2.0
