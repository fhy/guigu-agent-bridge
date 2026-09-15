# T001 — 项目脚手架与基础设施

## 元数据

- 状态：queued（待 Architect-Developer 认领）
- 阶段：一「模型和配置」
- Owner：未指派
- 依赖：无
- 阻塞：T002、T003
- Base commit：`3a717d4`（main）
- 工作区：单独推进时在 main 串行；若与其他任务并行需独立 branch/worktree 并在交接中登记
- 设计门禁：Developer 先提交实现前分析，Coordinator 确认后才能进入 `implementing`

## 目标

建立可编译、可测试的最小 Rust 项目地基：确定 crate 结构、运行时基础、结构化日志、顶层错误边界和关闭生命周期，为后续任务提供稳定入口。

## 范围

- 目标：
  - `Cargo.toml`：只加入本任务需要的运行时、日志和错误依赖；版本与 feature 由实现分析说明。
  - `src/main.rs`：最小异步入口，初始化 tracing 并支持 Ctrl-C 关闭。
  - `src/lib.rs`（新增）：暴露可测试的应用入口和最小模块边界；具体布局在实现分析中说明。
  - `src/error.rs`：提供当前入口所需的顶层错误边界，避免提前枚举尚不存在的子系统错误。
  - 只声明 T001 实际使用的模块，不创建 Bus、Storage、Matrix、ACP 或 Observer 空目录。
- 排除：配置加载及占位、领域模型、Bus、存储、传输和 Adapter；不引入 serde、Matrix、ACP 或 SQLite 专用依赖，除非本任务实际代码需要并经 Coordinator 确认。

## 共享接口

- 顶层错误边界：后续模块可扩展，但 T001 不预先固化全部错误变体。
- 日志约定：业务日志统一走 `tracing` 结构化字段，不使用 `println!` 承载。

## 风险

- Tokio feature、tracing 初始化和 lib/bin 边界会影响后续任务，需在实现分析中记录理由。
- Rust edition 2024 与依赖 MSRV 需兼容，`cargo check` 必须通过。

## 验收标准

- [ ] `cargo fmt --check`、`cargo check`、`cargo clippy -- -D warnings`、`cargo test` 全部通过。
- [ ] `cargo run` 输出结构化日志，且可 Ctrl-C 优雅退出。
- [ ] 存在最小顶层错误边界，`main`/`lib` 正确传播错误。
- [ ] 没有配置占位或未使用的后端模块骨架。
- [ ] 依赖选型与理由记录在实现分析或 handoff 中。
- [ ] Developer 写入 handoff，包含精确 commit、变更文件、门禁结果、风险和下一步，并通知 Coordinator 进入 `review_ready`。

## 门禁

```text
cargo fmt --check
cargo check
cargo clippy -- -D warnings
cargo test
```
