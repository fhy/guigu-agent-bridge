# Task Board

> 极简重启索引。任务规格见 `docs/tasks/`，审查见 `docs/reviews/`，交接见 `docs/handoffs/`，异常见 `docs/incidents/`。
> 重启后先读本文件，再读 Current 任务的规格与最近一次审查/交接。不要用 `git log` 推断当前任务状态。

## Current

| ID | 任务 | 阶段 | Owner | 状态 | 分支/工作区 | Base commit | 规格 |
|----|------|------|-------|------|-------------|-------------|------|
| T001 | 项目脚手架与基础设施 | 一：模型和配置 | bridge-developer | in_review | main | be00814 | [spec](tasks/T001-project-scaffolding.md) |

## Queue

| ID | 任务 | 阶段 | Owner | 依赖 | 状态 | 规格 |
|----|------|------|-------|------|------|------|
| T002 | 核心领域模型 | 一：模型和配置 | 未指派 | T001 | queued | [spec](tasks/T002-core-domain-models.md) |
| T003 | 配置加载与校验 | 一：模型和配置 | 未指派 | T001 | queued | [spec](tasks/T003-config-loading-validation.md) |
| T004 | Agent Bus 抽象 + 内存队列 + 任务提交 | 二：内存 Agent Bus | 未指派 | T002, T003 | queued | 待编写 |
| T005 | Bus Worker 状态机 + 事件广播 | 二：内存 Agent Bus | 未指派 | T004 | queued | 待编写 |
| T006 | 超时/取消/重试 + 循环检测 | 二：内存 Agent Bus | 未指派 | T005 | queued | 待编写 |
| T007 | Mock Agent | 二：内存 Agent Bus | 未指派 | T004 | queued | 待编写 |
| T008 | 存储 schema 与迁移 | 三：SQLite 持久化 | 未指派 | T002 | queued | 待编写 |
| T009 | Repository + 重启恢复 + 幂等 | 三：SQLite 持久化 | 未指派 | T008, T005 | queued | 待编写 |
| T010 | Matrix 登录 / sync / room / thread 解析 | 四：Matrix 用户入口 | 未指派 | T003, T009 | queued | 待编写 |
| T011 | 用户消息路由 + 回复 + 权限 + 去重 | 四：Matrix 用户入口 | 未指派 | T010, T004 | queued | 待编写 |
| T012 | 监控房间 + 任务摘要 + 告警 + 调用链 | 五：Matrix Observer | 未指派 | T011, T005, T009 | queued | 待编写 |
| T013 | 管理命令 (/status /trace /cancel /retry) | 五：Matrix Observer | 未指派 | T012 | queued | 待编写 |
| T014 | ACP 子进程 + JSON-RPC + session | 六：ACP Adapter | 未指派 | T004, T009 | queued | 待编写 |
| T015 | 流式事件 + 取消 + 进程恢复 | 六：ACP Adapter | 未指派 | T014 | queued | 待编写 |
| T016 | 并发控制 + 健康检查 + 指标 | 七：生产化 | 未指派 | 阶段二~六完成 | queued | 待编写 |
| T017 | 部署配置 + 热加载 + 集成测试 | 七：生产化 | 未指派 | T016 | queued | 待编写 |

## Blocked

（无）

## Recent

（无）
