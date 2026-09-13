# 开发协作方案

本文档描述 `guigu-agent-bridge` 在项目实现阶段采用的多 Agent 协作方式。它描述的是**如何开发本项目**，不是 Bridge 完成后提供的运行时 AgentTask 协议。

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
- 将任务交给 Architect-Developer。
- 汇总 Reviewer 结论并协调修复或合并。
- 遇到业务决策、安全策略、公开 API 或重大兼容性变化时请求用户决定。

Coordinator 不直接修改业务代码，也不替代 Reviewer 做技术审查。

### Architect-Developer

- 阅读任务规格并细化实现设计。
- 修改代码和测试，处理普通 Bug。
- 运行格式、编译、lint 和测试门禁。
- 提交实现并根据 Review 报告修复。

当前项目规模较小时，Architect 和 Developer 合并为一个角色。

### Reviewer

- 独立审查代码、接口、测试、安全和错误处理。
- 执行必要的验证门禁。
- 分析复杂 Bug、跨模块问题和反复失败。
- 生成审查报告，结论为 `PASS`、`CHANGES_REQUESTED` 或 `BLOCKED`。

Reviewer 不直接修改业务代码。

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
