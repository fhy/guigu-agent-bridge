# T002 — 核心领域模型

## 元数据

- 状态：queued
- 阶段：一「模型和配置」
- Owner：未指派
- 依赖：T001
- 阻塞：T004、T008
- Base commit：`3a717d4`（main）
- 设计门禁：先提交 ID、时间、priority 和序列化契约方案；Coordinator 明确确认后才能进入 `implementing`

## 目标

实现统一领域模型：`AgentEndpoint`、`Conversation`、`Message`、`AgentTask` 以及任务状态与事件类型，作为 Bus、存储与适配器共用的公共数据结构。

## 范围

- 目标文件：`src/models/`（可按 `agent.rs`、`conversation.rs`、`message.rs`、`task.rs`、`event.rs` 拆分）。
- 模型字段（依据 ARCHITECTURE.md §5）：
  - `AgentEndpoint`：稳定 id、transport 类型、地址、`enabled`、能力列表 `capabilities`；不含固定业务角色。
  - `Conversation`：id、参与者、与 room/thread/session 的关联；与 task、message 分开建模。
  - `Message`：id、conversation、sender、recipient、body、`reply_to`、metadata。
  - `AgentTask`：`task_id`、`root_task_id`、`parent_task_id`、`from_agent`、`to_agent`、`conversation_id`、`reply_to`、`text`、`priority`、`depth`、`hops`、`deadline`。
  - `TaskStatus`：`queued` / `dispatched` / `running` / `completed` / `failed` / `timed_out` / `cancelled`。
  - `TaskEvent`：不可变事件（id、task_id、seq、状态、时间戳、payload）。
- 所有结构实现 serde `Serialize`/`Deserialize`（供 SQLite 持久化与 JSON-RPC 复用）。
- 设计分析必须明确：
  - 各类 ID 是强类型 UUID、新类型包装还是字符串，以及外部 ID 的边界。
  - `deadline` 使用 UTC 绝对时间的内存/序列化表示，及精度和时区规则。
  - `priority` 的有效范围、默认值和越界行为。
  - enum 的 serde 命名、未知值处理及向后兼容策略。
- 排除：不实现 Bus 调度、状态机执行、存储、传输逻辑。

## 共享接口

- 模型类型被 T004（Bus）、T008（Storage）、T010（Matrix）、T014（ACP）引用，字段命名与类型需保持稳定，并在 handoff 中声明对外契约。

## 风险

- 这些模型是跨 Bus、Storage、Matrix 和 ACP 的公共契约；未经 Coordinator 确认不得在实现中自行选择关键语义，后续变更必须重新评估全部消费者。

## 验收标准

- [ ] 四类模型 + 状态 + 事件类型齐全，可 serde 序列化/反序列化。
- [ ] 单元测试覆盖 serde 往返与状态判定的关键分支。
- [ ] 门禁（fmt / check / clippy / test）全部通过。
- [ ] 模型为纯数据结构，不依赖传输/存储/调度模块。
- [ ] 设计分析已经 Coordinator 确认，handoff 记录最终公共契约与精确 commit。

## 门禁

```text
cargo fmt --check
cargo check
cargo clippy -- -D warnings
cargo test
```
