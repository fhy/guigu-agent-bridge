# 开发协作方案

本文档描述 `guigu-agent-bridge` 在项目实现阶段采用的多 Agent 协作方式。它描述的是**如何开发本项目**，不是 Bridge 完成后提供的运行时 AgentTask 协议。治理经验和门禁基线见 [GOVERNANCE.md](GOVERNANCE.md)。

## 1. 当前阶段的工具边界

当前使用的 OpenCode、ACP 和 Matrix 工具只提供聊天消息、thread、@mention、ACP session 和 prompt 等能力，尚不支持结构化 `AgentTask`、任务 DAG 或持久化 Agent Bus。

因此，当前开发协作使用：

```text
Matrix + @mention + thread
任务规格文档
TASK_BOARD.md
Git branch/worktree
Review 报告
```

`AgentTask` 和 Agent Bus 是本项目完成后新增的运行时能力，不能作为当前开发协作的既有前提。

## 2. 当前角色

项目当前采用三个逻辑角色，由三个 Agent 或在资源有限时合并承载：

### Coordinator

- 理解用户目标，拆分任务并确定优先级。
- 识别依赖、文件冲突和并行机会。
- 创建任务规格，维护 `TASK_BOARD.md` 和项目状态。
- 是任务状态的唯一维护者；将任务明确交给 Architect-Developer 并等待认领确认。
- 核验 handoff 后明确调度 Reviewer，不依赖 Developer 的自然语言输出自动触发审查。
- 汇总 Reviewer 结论并协调修复或合并。
- 遇到业务决策、安全策略、公开 API 或重大兼容性变化时请求用户决定。

Coordinator 不直接修改业务代码，也不替代 Reviewer 做技术审查。

### Architect-Developer

- 接到任务后必须先阅读任务规格、架构文档、当前任务板和相关代码，提交实现前分析。
- 分析应覆盖目标文件、现有接口、依赖、并发/错误路径、测试方案和潜在风险。
- 发现事实不明确或规格有歧义时，必须先反馈 Coordinator，不得盲目编码或扩大范围。
- 阅读任务规格并细化实现设计。
- 修改代码和测试，处理普通 Bug。
- 运行格式、编译、lint 和测试门禁。
- 提交实现并根据 Review 报告修复。
- 达到 `review_ready` 时写 handoff 并通知 Coordinator，不直接假设 Reviewer 已收到任务。

当前项目规模较小时，Architect 和 Developer 合并为一个角色。

### Reviewer

- 独立审查代码、接口、测试、安全和错误处理。
- 执行必要的验证门禁。
- 分析复杂 Bug、跨模块问题和反复失败。
- 生成审查报告，结论为 `PASS`、`CHANGES_REQUESTED` 或 `BLOCKED`。

Reviewer 不直接修改业务代码。

Observer 不进入正常开发审批链路。它默认只读诊断；仅当服务已经异常退出、failed 或有明确 OOM kill 证据时可自行重启受影响的单个服务，其他状态变更均需用户确认。

## 3. 任务流程

```text
用户需求
  -> Coordinator 拆解和排序
  -> 创建任务规格
  -> Architect-Developer 实现
  -> Reviewer 审查
       |-- CHANGES_REQUESTED -> Architect-Developer 修复 -> 再审查
       |-- PASS -> Coordinator 判断合并条件
  -> 用户处理重大决策或授权
```

普通任务可以简化为 `Coordinator -> Architect-Developer -> Reviewer -> Coordinator`。

### 任务状态机

开发协作使用以下固定状态，禁止用自由文本创建同义状态：

```text
queued -> assigned -> analyzing -> implementing -> review_ready -> in_review -> done
                           |              ^             |
                           v              |             v
                 needs_clarification      +--- changes_requested
                           |
                         blocked
```

- `queued`：规格已存在，但尚未指派；只出现在 Queue。
- `assigned`：Coordinator 已发送明确指派，等待 Developer 确认。
- `analyzing`：Developer 已确认并进行实现前分析。
- `needs_clarification`：存在可解决的事实、规格或设计疑问。
- `implementing`：分析结论已确认，允许编码。
- `review_ready`：实现、提交、门禁和 handoff 已完成。
- `in_review`：Coordinator 已携带 Task ID、精确 commit 和 handoff 明确调度 Reviewer。
- `changes_requested`：Reviewer 要求修改；Coordinator 重新交给 Developer。
- `blocked`：缺少用户决定、依赖、环境、凭据或外部服务，当前无法继续。
- `done`：Reviewer 对精确 commit 给出 `PASS`，且 Coordinator 核验流程条件完成。

Coordinator 是唯一可以修改任务状态的人。每次状态变化同时更新任务规格和 `TASK_BOARD.md`；`PROJECT_STATUS.md` 只在阶段、主要风险或项目级决策发生变化时更新。

### 显式调度

Matrix `@mention` 是当前的通知通道，不是可靠队列。消息发送成功不表示对方已认领；接收方必须回复 Task ID 和接受状态。没有确认时，Coordinator 保持任务为 `assigned` 并向用户报告，而不是重复创建任务。

Developer 完成后：

```text
1. 写 docs/handoffs/<task-id>-handoff.md。
2. 提供 exact commit、changed files、tests、risks 和 next action。
3. 通知 Coordinator：任务已 review_ready，不直接宣称任务完成。
```

Coordinator 核对 handoff、commit、范围和工作区后，使用以下最小信息显式调度 Reviewer：

```text
Task: Txxx
Review commit: <full SHA>
Specification: docs/tasks/<task>.md
Handoff: docs/handoffs/<handoff>.md
Required verdict: PASS | CHANGES_REQUESTED | BLOCKED
```

Reviewer 将不可变报告写入 `docs/reviews/<task-id>-review-rN.md` 并通知 Coordinator。`PASS` 不自动合并或变更状态；Coordinator 核对精确 commit 和门禁后才标记 `done`。`CHANGES_REQUESTED` 由 Coordinator 转回 Developer，修复后必须生成新 handoff 或明确更新至新 commit，再进行新一轮审查。

### 实现前分析和疑问升级

Architect-Developer 的实现前分析至少确认：

```text
目标文件和排除文件
现有接口和调用链
依赖任务和共享接口
错误、并发、恢复和兼容性影响
测试和验收方式
```

实现前分析还必须主动检查设计风险，而不是只检查需求文字是否清楚，至少包括：

```text
锁的获取顺序、持锁 await、锁粒度和释放路径
异步任务、channel、stream、actor 的生命周期和关闭传播
并发状态机、取消/超时与完成事件之间的竞争
测试是否可能因真实锁、channel 或任务未退出而死锁
错误传播、资源回收、重启恢复和幂等语义
接口是否可注入 mock，行为是否可以被稳定验证
```

分析中发现事实不明确、规格矛盾、接口不完整、存在多个行为选择，或发现可能导致死锁、竞态、资源泄漏、不可测试或无法恢复的结构时，任务应标记为 `needs_clarification` 或 `blocked`，并先反馈 Coordinator。不得以“先实现再看测试结果”为理由绕过分析。

Coordinator 应依据架构文档、任务规格、历史决策和 Review 结论解决；若仍无法判断，或问题涉及业务目标、公开 API、安全策略、兼容性、并发语义或范围变化，必须升级给用户决定。决定记录后才能恢复实现。

推荐反馈格式：

```text
Task: Txxx
Question: 需要确认的具体问题
Facts: 已确认事实
Options: 可行选项及影响
Recommendation: 技术建议（如有）
Impact: 对文件、接口、测试和进度的影响
```

## 4. 合并规则

Reviewer 负责判断技术审查是否通过，Coordinator 负责确认流程条件。满足以下条件时可推进低风险合并：

- Reviewer 明确给出 `PASS`。
- `cargo fmt --check`、`cargo check`、`cargo clippy -- -D warnings` 和 `cargo test` 全部通过。
- 没有未解决的阻塞问题或越权修改。
- 没有跨任务接口冲突。

以下情况必须交给用户决定：公开 API、数据库兼容性、安全策略、外部依赖、功能删除、Agent 权限或其他重大行为变化。

## 5. 文档协议

```text
TASK_BOARD.md              当前任务极简索引
docs/tasks/<id>-*.md       单个任务规格和验收标准
docs/reviews/<id>-*.md     审查结论和问题
docs/handoffs/<id>-*.md    跨 Agent/分支/worktree 交接
docs/incidents/<id>-*.md   Bug、挂起、崩溃和异常调查
docs/decisions/ADR-*.md    需要长期保留的架构决策
docs/PROJECT_STATUS.md     项目整体状态和风险
```

`TASK_BOARD.md` 必须保持短小，优先展示 `Current`、`Queue`、`Blocked` 和 `Recent`。Agent 重启后先读取该文件，再读取 Current 任务对应的规格和最近的审查/交接文档。

`Current` 只包含 `assigned` 到 `in_review`、`changes_requested` 或 `needs_clarification` 的活跃任务。未指派的 `queued` 任务只放在 Queue；完成项只在 Recent 保留简短索引。

## 6. 任务注册信息

跨 Agent 或跨 worktree 任务需要记录：

```text
Task ID、Feature、Owner、Status
Branch、Worktree、Base commit
目标文件、排除文件、共享接口
依赖、风险、验收标准
```

开工前检查 `git status`、当前分支、worktree 和目标文件冲突。合并操作在 `main` 上串行进行。

## 7. Bug 和异常

- 普通代码 Bug：Architect-Developer 定位并修复。
- 复杂或跨模块 Bug：Reviewer 独立分析根因，再创建修复任务。
- Agent/ACP/路由异常：Reviewer 负责诊断，Coordinator 负责升级和用户沟通。
- 修复完成后必须由 Reviewer 回归验证。

未来项目扩大时，可以将当前角色拆分为 Governance、Design、Execution、Review、Verification/Diagnostic 五类 Agent；当前不强制拆分。

## 8. 未来运行时协作

项目完成后，Bridge 才提供结构化 `AgentTask` 和 Agent Bus。届时机器协作通过任务协议完成，Matrix 主要作为用户交互、状态观测和人工控制界面。当前文档中的开发协作流程不应与未来运行时协议混用。
