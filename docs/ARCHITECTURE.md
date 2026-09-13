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
Agent Adapters (ACP / Matrix / HTTP)
  |
  v
外部 Agent

Event Store -> Observer -> Matrix 监控房间
```

## 4. 模块边界

- **Transport**：处理外部协议和连接生命周期，不包含业务路由。
- **Bridge Core**：统一消息模型、路由、权限、幂等、循环检测和会话绑定。
- **Agent Bus**：排队、投递、取消、超时、重试并发布任务事件。
- **Agent Adapter**：把统一任务转换为 ACP、Matrix 或 HTTP 等协议。
- **Storage**：保存 Agent、会话、消息、任务、投递和事件状态。
- **Observer**：订阅事件并向 Matrix/Web 等界面投影状态。

## 5. 核心模型

### AgentEndpoint

表示一个可接收任务的外部 Agent，包括稳定 ID、传输类型、地址、启用状态和能力列表。模型不包含固定业务角色。

### Conversation

表示用户或 Agent 之间的持续上下文。它与 Matrix room/thread、Agent session、内部 task 分开建模，但可以互相关联。

### Message

用户消息、Agent 消息和系统消息使用统一消息模型，包含消息 ID、conversation、发送者、接收者、正文、回复关联和元数据。

### AgentTask

内部 Agent 协作的主要协议，至少包含：

- `task_id`、`root_task_id`、`parent_task_id`
- `from_agent`、`to_agent`
- `conversation_id`、`reply_to`
- `text`、`priority`
- `depth`、`hops`、`deadline`

内部协作优先使用结构化任务；Matrix 中的 @mention 仅作为用户可见表达或兼容入口。

## 6. Agent Bus

第一版使用 `tokio::sync::mpsc` 实现内存队列，但通过抽象接口隔离具体实现。正式状态仍由 SQLite 保存。

Bus Worker 负责：

1. 取出任务并验证目标 Agent。
2. 执行权限检查和并发限制。
3. 调用 Agent Adapter。
4. 更新任务状态并发布事件。
5. 处理超时、取消和重试。

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

完成 ACP 子进程、JSON-RPC、session、流式事件、取消和进程恢复。

### 阶段七：生产化

增加并发控制、健康检查、指标、部署配置、配置热加载和完整集成测试。

## 10. 第一版范围和验收

第一版仅实现：Matrix 用户入口、内存 Agent Bus、SQLite、Mock Agent、一个 ACP Adapter 和 Matrix 监控房间。

验收闭环：

```text
用户 -> Matrix -> Bridge -> Agent A
Agent A -> Agent Bus -> Agent B
Agent B -> Agent A -> Matrix -> 用户
```

同时必须满足：任务状态可查询、Agent 挂起或崩溃可告警、循环调用可阻断、服务重启后未完成任务可恢复。

## 11. 参考项目

`/home/fhy/opencode-chat-bridge` 主要参考其 ACP 封装、Connector/Core 分离、线程会话隔离、进程管理、流式事件、权限边界、限流去重、配置分层和测试结构。

其固定触发词、固定 Agent 角色、Matrix 作为唯一通信通道和 JSON 文件队列不直接照搬。
