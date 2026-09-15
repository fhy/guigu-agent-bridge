# Project Status

## 摘要

- 项目：guigu-agent-bridge（通用 Agent 通信与任务路由基础设施）
- 当前阶段：阶段一「模型和配置」；T001 已建立 lib/bin 结构、tracing、顶层错误边界和 Ctrl-C 关闭生命周期
- 架构：已确定，见 [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- 决策：ADR-001（ACP Transport Compatibility）已接受，见 [docs/decisions/ADR-001-acp-transport-compatibility.md](decisions/ADR-001-acp-transport-compatibility.md)
- 当前工作：T001 已完成（Reviewer PASS @ f20c3d1，脚手架与基础设施就绪）；T002/T003 待用户确认后再指派。

## 路线图与任务分解

| 阶段 | 内容 | 任务 |
|------|------|------|
| 一 模型和配置 | 领域模型、配置加载/校验、结构化日志、错误类型 | T001–T003 |
| 二 内存 Agent Bus | 任务提交、Worker、状态流转、事件广播、超时/取消、Mock Agent | T004–T007 |
| 三 SQLite 持久化 | 存储 schema、Repository、重启恢复、幂等 | T008–T009 |
| 四 Matrix 用户入口 | 登录、sync、room/thread 解析、消息路由、权限、去重 | T010–T011 |
| 五 Matrix Observer | 监控房间、任务摘要、告警、调用链、管理命令 | T012–T013 |
| 六 ACP Adapter | 子进程、JSON-RPC、session、流式事件、取消、进程恢复 | T014–T015 |
| 七 生产化 | 并发控制、健康检查、指标、部署配置、热加载、集成测试 | T016–T017 |

关键依赖路径：

```text
T001 ─┬─ T002 ─ T004 ─ T005 ─ T006
      │          └─ T008 ─ T009
      └─ T003 ─ T004 / T010 ─ T011 ─ T012 ─ T013
T004 / T009 ─ T014 ─ T015
阶段二~六完成 ─ T016 ─ T017
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

## 运维观察

`bridge-observer` 负责只读诊断 Agent、ACP、Matrix、服务、日志和资源异常，不参与正常任务审批。只有服务已异常退出、failed 或有明确 OOM kill 证据时，Observer 才可无需确认重启受影响的单个服务；其他状态变更必须先征得用户确认。

## 下一步

1. 审查并提交当前规划文档。
2. Coordinator 明确指派 T001 并等待 Architect-Developer 确认 Task ID。
3. Architect-Developer 提交实现前分析；Coordinator 确认设计后才进入实现。
4. Developer 生成 handoff 并报告 `review_ready`；Coordinator 携带精确 commit 显式调度 Reviewer。
5. Coordinator 根据 Review 结论更新状态、安排返工或标记完成。
