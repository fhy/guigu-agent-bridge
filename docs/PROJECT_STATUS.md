# Project Status

## 摘要

- 项目：guigu-agent-bridge（通用 Agent 通信与任务路由基础设施）
- 当前阶段：阶段一「模型和配置」；T001 已建立 lib/bin 结构、tracing、顶层错误边界和 Ctrl-C 关闭生命周期
- 架构：已确定，见 [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- 决策：ADR-001（ACP Transport Compatibility）已接受，见 [docs/decisions/ADR-001-acp-transport-compatibility.md](decisions/ADR-001-acp-transport-compatibility.md)
- 当前工作：T001 已完成；用户已确认按 T002 → Review → T003 串行推进，下一项只指派 T002。

## 路线图与任务分解

| 阶段 | 内容 | 任务 |
|------|------|------|
| 一 模型和配置 | 领域模型、配置加载/校验、结构化日志、错误类型 | T001–T003 |
| 二 内存 Agent Bus | 任务提交、Worker、状态流转、事件广播、超时/取消、Mock Agent | T004–T007 |
| 三 SQLite 持久化 | 存储 schema、Repository、重启恢复、幂等 | T008–T009 |
| 四 Matrix 用户入口 | 登录、sync、room/thread 解析、消息路由、权限、去重 | T010–T011 |
| 五 Matrix Observer | 监控房间、任务摘要、告警、调用链、管理命令 | T012–T013 |
| 六 ACP Adapter | 子进程、JSON-RPC、session、流式事件、取消、进程恢复 | T014–T015 |
| 七 生产化 | 并发控制、健康检查、指标、部署配置、热加载、集成与路由可靠性验收 | T016–T018 |

关键依赖路径：

```text
T001 ─┬─ T002 ─ T004 ─ T005 ─ T006
      │          └─ T008 ─ T009
      └─ T003 ─ T004 / T010 ─ T011 ─ T012 ─ T013
T004 / T009 ─ T014 ─ T015
阶段二~六完成 ─ T016 ─ T017
T005 / T009 / T011 / T014 / T016 ─ T018
```

## 待定决策

以下为设计选择，需 Architect-Developer 在对应任务的实现前分析中提出方案与理由。公共或跨模块接口必须由 Coordinator 明确确认后才能进入实现；涉及兼容性、安全或范围变化的再升级用户。

- 依赖选型：SQLite 驱动（sqlx vs rusqlite）、Matrix 客户端（matrix-sdk vs ruma）、tokio 特性集——分别在 T008 / T010 / T001 前敲定。
- 是否引入 `src/lib.rs`（库 + 二进制结构）以支撑集成测试（T001 内决定）。
- id 生成策略、deadline 表示（时间戳 vs 时长）、priority 取值域（T002 内决定）。

## 风险

- 阶段间共享接口（领域模型、错误类型、事件）一旦稳定后变更成本高，阶段一需预留扩展点。
- Matrix 与 ACP 依赖外部服务/后端，无网络或凭据时无法真实联调，须以 Mock 与集成测试覆盖（对应 ADR-001）。
- 并行任务不得同时修改同一公共接口/文件；当前阶段一按 T001 → T002/T003 串行推进。
- 当前 Matrix 协作依赖显式通知与接收确认，不具备结构化 AgentTask 的可靠投递语义。
- `opencode-chat-bridge` 共享房间曾将同一事件交给多个 backend，导致重复 session、越权执行和状态竞争；本项目必须以结构化 recipient、排他路由、认领确认和乐观并发控制从设计上阻断，见 [INC-001](incidents/INC-001-shared-room-routing.md) 与 T018。

## 运维观察

`bridge-observer` 负责只读诊断 Agent、ACP、Matrix、服务、日志和资源异常，不参与正常任务审批。只有服务已异常退出、failed 或有明确 OOM kill 证据时，Observer 才可无需确认重启受影响的单个服务；其他状态变更必须先征得用户确认。

## 下一步

1. Coordinator 只指派 T002，并等待 Architect-Developer 以 Task ID 确认。
2. Architect-Developer 先提交 T002 的公共模型设计分析；Coordinator 确认后才允许实现。
3. T002 完成 handoff 和 Reviewer PASS 后，Coordinator 再指派 T003。
4. 不并行修改领域模型与配置公共接口。
