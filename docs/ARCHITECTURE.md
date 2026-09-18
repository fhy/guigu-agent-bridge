# guigu-agent-bridge 架构方案

## 1. 项目定位

`guigu-agent-bridge` 是通用的 Agent 通信与任务路由基础设施。

项目负责接收用户消息、连接外部 Agent、路由内部任务、保存状态，并提供超时、重试、取消、循环检测和可观察性能力。

项目不负责实现具体 Agent，不固定 planner、worker、reviewer 等角色，也不决定 Agent 使用的模型、工具或业务流程。

## 2. 总体原则

```text
Agent Bus     内部任务执行通道
Database      任务和状态的事实来源
Matrix        用户交互、可观察性和人工控制界面
Agent         外部能力提供者
```

Matrix 不作为唯一的内部通信通道。Agent 之间的机器调用优先通过内部 Agent Bus；任务状态变化通过事件投影到 Matrix，供用户和管理员查看。

## 3. 总体架构

```text
用户
  |
  v
User Transport (Matrix / Web)
  |
  v
Bridge Core (消息、路由、权限、会话)
  |                 |
  v                 v
Agent Bus       Event Store (SQLite)
  |
  v
Agent Adapters (ACP / A2A / Matrix / HTTP)
  |
  v
外部 Agent

Event Store -> Observer -> Matrix 监控房间
```

## 4. 模块边界

- **Transport**：处理外部协议和连接生命周期，不包含业务路由。
- **Bridge Core**：统一消息模型、路由、权限、幂等、循环检测和会话绑定。
- **Agent Bus**：排队、投递、取消、超时、重试并发布任务事件。
- **Agent Adapter**：把统一任务转换为 ACP、A2A、Matrix 或 HTTP 等协议。外部协议类型不得侵入内部 Agent Bus 的领域契约。
- **Storage**：保存 Agent、会话、消息、任务、投递和事件状态。
- **Observer**：订阅事件并向 Matrix/Web 等界面投影状态。

## 5. 核心模型

### AgentEndpoint

表示一个可接收任务的外部 Agent，包括稳定 ID、传输类型、地址、启用状态和能力列表。模型不包含固定业务角色。

### Conversation

表示用户或 Agent 之间的持续上下文。它与 Matrix room/thread、Agent session、内部 task 分开建模，但可以互相关联。

### Message

用户消息、Agent 消息和系统消息使用统一消息模型，包含消息 ID、conversation、发送者、接收者、正文、回复关联和元数据。

`recipient` 必须是 Bridge 解析后的稳定 endpoint ID，而不是仅保留自然语言中的 `@mention`。共享房间中的一条输入只能选择一个目标 Agent；无法唯一解析时应拒绝或交给显式配置的默认入口，不得广播给所有可访问 Agent。

### AgentTask

内部 Agent 协作的主要协议，至少包含：

- `task_id`、`root_task_id`、`parent_task_id`
- `from_agent`、`to_agent`
- `conversation_id`、`reply_to`
- `text`、`priority`
- `depth`、`hops`、`deadline`

内部协作优先使用结构化任务；Matrix 中的 @mention 仅作为用户可见表达或兼容入口。

A2A 用于 Bridge 与外部 Agent 系统之间的互操作，不替代内部 `AgentTask`。A2A task、message、artifact、状态和外部身份必须在 Adapter 边界显式映射并持久化关联；A2A SDK/wire 类型不得进入 Bus、Storage 或核心模型的公共接口。ACP 继续负责 Bridge 与具体 Agent 运行时进程之间的会话通信，两者职责不重叠。

## 6. Agent Bus

第一版使用 `tokio::sync::mpsc` 实现内存队列，但通过抽象接口隔离具体实现。正式状态仍由 SQLite 保存。

Bus Worker 负责：

1. 取出任务并验证目标 Agent。
2. 执行权限检查和并发限制。
3. 调用 Agent Adapter。
4. 更新任务状态并发布事件。
5. 处理超时、取消和重试。

任务投递与任务认领分开记录。发送成功不等于 Agent 已接受任务；只有带 `task_id` 和 delivery attempt 的明确确认才能把任务推进到运行态。状态更新必须校验当前状态和版本，防止多个执行者覆盖彼此结果。

会话隔离不等于执行隔离。Matrix room/thread、conversation 和 ACP session 只描述上下文；它们不能作为共享仓库、worktree 或其他可变资源的并发锁。启动可执行 Agent 前，调度器必须按稳定的执行资源键（第一版至少为 `agent_id + repository/workspace_id`）原子获取执行租约。租约记录 `task_id`、session/process identity、owner token、version、acquired/heartbeat/deadline 时间。竞争请求必须按配置排队或明确拒绝，并返回当前占用者；在获得租约前不得创建 ACP 进程或写入工作区。

租约释放必须校验 owner token/version，避免过期 session 释放新租约。正常完成、取消、超时、ACP 退出和 Bridge 重启都必须进入显式清理/恢复路径；重启时对持久化租约与实际进程进行 reconciliation。第一版默认同一执行资源串行运行，未来只有在不同 worktree/隔离 workspace 且策略明确允许时才并行。

未来可将队列替换为 SQLite queue、Redis 或 NATS，而不修改 Bridge Core。

## 7. 状态、可靠性和安全

任务状态包括 `queued`、`dispatched`、`running`、`completed`、`failed`、`timed_out` 和 `cancelled`。每次状态变化产生不可变事件。

Watchdog 通过 `last_activity_at`、`deadline`、进程健康状态和连接状态识别挂起、崩溃和超时。

循环检测使用最大深度、最大跳数、已访问 Agent、最大子任务数和 deadline。检测到循环时停止投递，记录调用链，并通知监控界面。

权限分为两层：Bridge 控制“谁可以访问和调用哪个 Agent”，Agent backend 控制文件、shell、网络和 MCP 等工具能力。敏感凭据只放在环境或专用 secret 管理中，不复制到 workspace、profile 或任务记录。

## 8. Matrix 的职责

Matrix 同时承担用户交互和系统观测：

- 用户房间：显示最终结果和简要进度。
- 监控房间：显示任务创建、执行、完成、失败、超时、重试和调用链。
- 管理命令：支持 `/status`、`/trace`、`/cancel`、`/retry`。

Matrix 是状态的投影和控制面，不是任务事实来源。Matrix 发送失败不应导致任务状态回滚。

共享房间路由必须记录 `event_id`、解析后的目标、处理实例和忽略原因。自然语言名称可作为兼容入口，但进入 Bridge Core 前必须解析成结构化 recipient；非目标实例不得创建 session 或启动 Agent。

## 9. 实施阶段

### 阶段一：模型和配置

完成核心对象、配置加载、配置校验和结构化日志。

### 阶段二：内存 Agent Bus

完成任务提交、Worker、状态流转、事件广播、超时、取消和 Mock Agent。

### 阶段三：SQLite 持久化

保存 Agent、conversation、message、task、task event、delivery 和 session，并支持重启恢复和幂等处理。

### 阶段四：Matrix 用户入口

完成登录、sync、room/thread 解析、用户消息路由、回复发送、权限和去重。

### 阶段五：Matrix Observer

完成监控房间、任务摘要、异常告警、调用链展示和管理命令。

### 阶段六：ACP Adapter

完成 ACP 子进程、JSON-RPC、session、流式事件、取消和进程恢复。最新版 `@agentclientprotocol/codex-acp` 的 ACP stdio 外层使用 JSONL，应作为必须通过的兼容目标。Transport 的 JSON-RPC 消息处理与字节 framing 仍需分层，避免未来 backend 使用不同 framing 时侵入 session 逻辑。ACP 的 `end_turn` 仅表示一次模型 turn 结束，不等于 `AgentTask` 完成；Adapter 必须输出结构化 `continue/completed/blocked/failed` 结果并保持 task/session 关联，禁止从自然语言尾句推断终态。

### 阶段七：生产化

增加并发控制、健康检查、指标、部署配置、配置热加载和完整集成测试。调度器对非终态 turn 进行有界自动续跑，保持执行租约，并以最大 turn 数、总 deadline、无进展检测和资源预算阻断死循环；状态与续跑计数必须支持持久化恢复和 Observer 告警。

### 阶段八：A2A 外部互操作

在内部任务语义、持久化、执行租约和端到端可靠性稳定后，T019 提供面向可信局域网的最小 A2A Server + Client。跨局域网优先由 T020 通过版本化 A2A envelope 复用第三方协议：Matrix 为必做 Transport，Redis Streams 为可选 Transport；这属于项目私有承载 profile，不冒充标准 A2A transport。Listener 默认关闭且仅允许私网/loopback。需要通用跨网互联时，单独创建中心化 `a2a-edge-gateway` Relay/Hub：各 Bridge 主动建立出站持久连接，无需公网地址或入站端口；Relay 负责连接路由、有限离线投递和传输确认，Nginx 仅负责公网 TLS、认证入口和限流。具体边界见 [A2A Relay / Edge Gateway 集成契约](integrations/A2A_EDGE_GATEWAY.md)。任务事实、授权、幂等、重放防护和状态机仍由 Bridge 掌握。A2A 和外部传输都不替换内部 Agent Bus；`guigu` 等执行后端继续通过 ACP 接入。

## 10. 第一版范围和验收

第一版仅实现：Matrix 用户入口、内存 Agent Bus、SQLite、Mock Agent、一个 ACP Adapter 和 Matrix 监控房间。A2A 属于第一版可靠性验收完成后的外部互操作阶段。

验收闭环：

```text
用户 -> Matrix -> Bridge -> Agent A
Agent A -> Agent Bus -> Agent B
Agent B -> Agent A -> Matrix -> 用户
```

同时必须满足：任务状态可查询、Agent 挂起或崩溃可告警、循环调用可阻断、服务重启后未完成任务可恢复。

端到端验收还必须证明：同一共享房间事件最多启动一个目标 Agent；未点名消息只进入配置的默认入口；投递未确认不会被记为运行；重复事件、并发状态更新和进程重启不会产生重复执行或状态倒退；不同 room/thread 请求同一 Agent 操作同一 execution resource 时最多只有一个 ACP 执行者，竞争请求不会在获得租约前启动进程。

## 11. 参考项目

`opencode-chat-bridge` 主要参考其 ACP 封装、Connector/Core 分离、线程会话隔离、进程管理、流式事件、权限边界、限流去重、配置分层和测试结构。

其固定触发词、固定 Agent 角色、Matrix 作为唯一通信通道和 JSON 文件队列不直接照搬。

## 12. 当前开发协作与运行时能力的边界

当前项目的开发协作采用 Coordinator、Architect-Developer、Reviewer 三个逻辑角色，使用 Matrix、任务文档、Git branch/worktree 和 Review 报告完成协作。公开仓库仅保留 [AGENTS.md](../AGENTS.md) 启动入口；动态任务状态和项目专用治理规则由私有治理仓库维护。

结构化 `AgentTask`、Agent Bus、任务状态机和持久化协作是本项目完成后提供的运行时能力。现有 OpenCode、ACP 和 Matrix 工具尚不支持这些能力，因此实现前不能把 `AgentTask` 当作现有协作工具使用。
