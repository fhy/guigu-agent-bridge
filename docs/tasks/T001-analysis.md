# T001 实现前分析

> Owner: bridge-developer · 阶段: analyzing · Base commit: be00814（main）
> 本文档是 T001 的实现前分析，供 Coordinator 确认设计后再进入 implementing。

## 0. 结论摘要

| 项 | 结论 |
|----|------|
| crate 结构 | 引入 `src/lib.rs`，main 薄、逻辑在 lib，支撑集成测试 |
| 依赖 | tokio（rt-multi-thread + macros + signal）、tracing、tracing-subscriber（env-filter）、thiserror |
| 日志 | tracing `fmt` + `EnvFilter`，业务日志用结构化字段，不用 `println!` |
| 错误边界 | `src/error.rs` 定义最小可扩展 `Error` 枚举（thiserror derive） |
| 关闭生命周期 | `run()` 初始化 tracing → await Ctrl-C → 优雅退出；`run_with_shutdown` 可注入供测试 |

---

## 1. crate 结构：引入 src/lib.rs（决策点 D1）

**结论：引入 `src/lib.rs`，采用 lib + thin bin 结构。**

布局：

```text
src/
  main.rs     #[tokio::main] async fn main() -> Result<(), Error>，仅调用 lib 入口
  lib.rs      pub mod error; init_tracing(); run(); run_with_shutdown()
  error.rs    pub enum Error（顶层错误边界）
```

理由：
- `PROJECT_STATUS.md` 已把「是否引入 src/lib.rs（库+二进制结构）以支撑集成测试」列为 T001 内待定决策，此处给出结论：**引入**。
- 后续任务（T002~T017）的领域模型、Bus、Storage、Adapter 都以库模块形式存在，只有 lib/bin 分离才能让 `tests/` 集成测试和未来的 `examples/`、benches 通过公共 API 走真实调用路径，而不是只能测 `src/` 内部的单元测试。
- `main.rs` 保持极薄：不承载任何业务逻辑，只负责把 `run()` 的 `Result` 传播给运行时（错误由 tokio 宏按 Debug 打印并以非零码退出）。这样「main/lib 正确传播错误」可被验证。

排除：不创建 `src/bus/`、`src/storage/`、`src/matrix/`、`src/acp/`、`src/observer/` 等目录或空模块；T001 只声明 `mod error`。

---

## 2. Cargo.toml 依赖与 Tokio feature 集（决策点 D2 / D3）

目标 `Cargo.toml`：

```toml
[package]
name = "guigu-agent-bridge"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
```

解析版本（分析时实测）：tokio 1.53.1（MSRV 1.71）、tracing 0.1.44（MSRV 1.65）、tracing-subscriber 0.3.23（MSRV 1.65）、thiserror 2.0.20（MSRV 1.71）。工具链 1.98.0。

### 2.1 Tokio feature 集（决策点 D2）

只启用 T001 实际需要的三个 feature：

- `rt-multi-thread`：`#[tokio::main]` 默认 flavor 是 multi_thread，需要此 feature。选择多线程运行时而非 `current_thread`，理由：本项目是长驻 daemon，阶段二~七会承载并发 Bus Worker、Adapter、Observer 等；现在就用多线程运行时，避免后续迁移运行时 flavor 的 churn。代价可忽略。
- `macros`：`#[tokio::main]` 与测试里的 `#[tokio::test]` 需要。
- `signal`：`tokio::signal::ctrl_c()` 需要。

**不**启用 `full`，也**不**启用 `sync`/`time`/`fs`/`io-util` 等：它们在本任务没有调用点，留到对应任务（T004 用 `sync` 队列、T006 用 `time` 超时）再按需添加。这是「只加入本任务需要的依赖/feature」的落地。

### 2.2 日志依赖

- `tracing = "0.1"`：结构化日志核心 facade。
- `tracing-subscriber = { version = "0.3", features = ["env-filter"] }`：`fmt`（人类可读 formatter，默认 feature 已含）+ `env-filter`（`RUST_LOG` 级别控制）。不启用 `json`（T001 无此需求，JSON 输出留待生产化阶段再议）。

### 2.3 错误依赖（决策点 D3：thiserror vs 手写）

**推荐 `thiserror = "2"`**，理由：
- 规格允许「运行时、日志和**错误**依赖」，thiserror 属于错误依赖范畴，在范围内。
- 后续 T002~T017 会不断新增子系统错误（config / storage / matrix / acp / bus），`#[derive(Error)]` + `#[from]`/`#[source]` 让扩展变体零样板，且不易漏写 `source()` 或 `Display`。
- 纯编译期 proc-macro，无运行时开销；MSRV 1.71 ≤ edition 2024 的 1.85 下限。

备选（不推荐）：手写 `enum Error { Shutdown(std::io::Error) }` 并手动实现 `Display` + `std::error::Error::source`。T001 只有一个变体时可行，但一旦进入多子系统阶段，手写样板会随变体数线性膨胀，且易出错。若 Coordinator 希望 T001 严格零 proc-macro 依赖，可改用手写，成本约 20 行，后续再引入 thiserror。

### 2.4 MSRV / edition

- `edition = "2024"` 需要 Rust 1.85+；所有依赖 MSRV ≤ 1.71，无冲突。
- 新增 `rust-version = "1.85"` 显式声明最低版本（等于 edition 2024 下限），使边界可被 `cargo` 校验。此为可选但推荐的小改动。

---

## 3. tracing 初始化与结构化日志约定（决策点 D4）

```rust
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
```

约定：
- subscriber：`tracing_subscriber::fmt()`（人类可读，`field=value` 形式，已满足「结构化」要求）。
- 级别：`RUST_LOG` 环境变量控制，缺省 `info`；`try_from_default_env` 对非法值回退而非 panic。
- **幂等**：用 `try_init()` 而非 `init()`，重复初始化（如测试多次调用、或未来热加载）不会 panic。
- **字段规范**：业务日志统一 `tracing::info!/warn!/error!` 并带结构化字段（如 `tracing::info!(task_id = %id, "task created")`），**不使用 `println!`/`eprintln!` 承载业务日志**。
- **决策点 D4**：T001 用默认人类可读格式，不启用 `json` feature。JSON 结构化输出属生产化（T016 指标/观测）范畴，届时再决定，避免本任务提前引入无用依赖。

---

## 4. 顶层错误边界 src/error.rs

```rust
use thiserror::Error;

/// 顶层应用错误边界。
///
/// T001 只保留当前入口真实能产生的错误；后续任务（config/storage/transport/adapter）
/// 通过新增变体扩展，不预固化尚不存在的子系统错误。
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to await shutdown signal")]
    Shutdown(#[source] std::io::Error),
}
```

- 为什么只有 `Shutdown` 一个变体：T001 运行时唯一的可失败点是 `tokio::signal::ctrl_c()`（返回 `io::Result<()>`）。不预建 `Config`/`Storage`/`Matrix`/`Acp` 等空变体（违反「避免提前枚举」）。
- 扩展方式：后续模块新增变体，如 `#[error("config: {0}")] Config(#[from] config::Error)`，现有代码零改动。
- `main`/`lib` 传播：`run() -> Result<(), Error>`，`main` 以 `Result<(), Error>` 返回，错误由运行时打印并退出非零。

---

## 5. Ctrl-C 关闭生命周期（决策点 D5）

```rust
// lib.rs
pub async fn run() -> Result<(), Error> {
    init_tracing();
    tracing::info!(target: "guigu_agent_bridge", "starting");
    run_with_shutdown(tokio::signal::ctrl_c()).await
}

/// 等待给定 shutdown future 完成后优雅退出。
/// 从 `run` 拆出，便于集成测试注入立即/可控信号，而非真实 OS 信号。
pub async fn run_with_shutdown<F>(shutdown: F) -> Result<(), Error>
where
    F: Future<Output = std::io::Result<()>>,
{
    shutdown.await.map_err(Error::Shutdown)?;
    tracing::info!(target: "guigu_agent_bridge", "shutdown signal received, exiting gracefully");
    Ok(())
}
```

```rust
// main.rs
#[tokio::main]
async fn main() -> Result<(), guigu_agent_bridge::Error> {
    guigu_agent_bridge::run().await
}
```

- 关闭语义：收到 Ctrl-C（SIGINT）→ `ctrl_c()` 完成 → 记录「exiting gracefully」→ `run()` 返回 `Ok(())` → main 正常结束。
- 错误路径：`ctrl_c()` 自身失败（罕见，如信号处理安装失败）→ `Error::Shutdown` 传播到 main → 非零退出。
- **决策点 D5**：`run_with_shutdown` 设为 `pub`，作为测试注入口（公共 API 的最小新增）。备选是只在 `src/lib.rs` 的 `#[cfg(test)]` 内测私有函数，但那样 `tests/` 集成测试无法走公共入口，违背「真实调用路径」要求。因此推荐 `pub`。

---

## 6. 测试方案

目标：验证「入口、关闭、错误传播」走真实调用路径，而非仅字符串断言。

| 测试 | 位置 | 验证点 |
|------|------|--------|
| `entry_returns_ok_on_shutdown` | `tests/shutdown.rs` | 调用公共 `run_with_shutdown(future::ready(Ok(())))`，断言返回 `Ok(())`，走真实关闭路径 |
| `entry_propagates_shutdown_error` | `tests/shutdown.rs` | 注入 `future::ready(Err(io::Error::...))`，断言返回 `Err(Error::Shutdown)`，验证错误边界传播 |
| `error_shutdown_displays_and_sources` | `src/error.rs` 内 `#[cfg(test)]` | 验证 `Display` 输出与 `source()` 返回底层 io 错误（错误边界本身的正确性） |
| `init_tracing_is_idempotent` | `src/lib.rs` 内 `#[cfg(test)]` | 连续两次 `init_tracing()` 不 panic（`try_init` 幂等） |

- 集成测试用 `#[tokio::test]`（依赖已含 `macros` + `rt-multi-thread`，含 `rt`）。
- 死锁/泄漏风险：`run_with_shutdown` 只 `await` 注入的 future，不 spawn 任何后台任务、不建 channel，无任务退出/死锁问题；`future::ready` 立即完成，单线程 `#[tokio::test]` 运行时安全。
- 不测试「真实 Ctrl-C 信号」，因跨平台、不确定且难以稳定自动化；信号源通过注入抽象掉，生产 `run()` 传入真实 `ctrl_c()`。

---

## 7. 设计风险检查

- **锁 / 持锁 await**：T001 无共享可变状态、无锁。N/A。
- **异步任务 / channel 生命周期**：不 spawn 任务、不建 channel；关闭生命周期只有「等待信号→返回」单一路径。N/A。
- **取消/超时竞态**：无超时、无取消令牌；`ctrl_c()` 与主流程之间无竞争窗口。N/A。
- **测试死锁**：见 §6，注入 future 立即完成，无后台任务残留。
- **错误传播 / 资源回收**：唯一可失败点 `ctrl_c()` 的 `io::Error` 经 `Error::Shutdown` 上抛；无文件句柄/连接需要回收（T001 无 I/O 资源）。
- **可注入 mock**：`run_with_shutdown<F: Future<Output = io::Result<()>>>` 提供信号注入口，测试无需真实信号。
- **兼容性**：edition 2024 与依赖 MSRV 已核对无冲突（§2.4）。

---

## 8. 需要 Coordinator 确认的决策点

| 编号 | 决策 | 我的推荐 | 性质 |
|------|------|----------|------|
| D1 | 是否引入 `src/lib.rs`（lib+bin） | 引入 | 公共/跨模块接口 |
| D2 | Tokio feature 集 | `rt-multi-thread + macros + signal`（多线程运行时） | 跨模块影响 |
| D3 | 错误依赖：thiserror vs 手写 | thiserror 2 | 公共接口 |
| D4 | 日志输出格式：默认文本 vs json | 默认文本（不启用 json） | 影响后续观测 |
| D5 | `run_with_shutdown` 设为 `pub` 作为测试注入口 | 设 pub | 公共 API |

以上均为规格/架构明确委托我在 T001 内给出方案、由 Coordinator 确认的设计选择，非「事实不明/规格矛盾」类疑问，故不标记为 `needs_clarification`。若 Coordinator 对任一推荐有异议或想改选备选方案，请指明，我在确认后按其调整并进入实现。

---

## 9. Coordinator 确认记录

Coordinator（bridge-coordinator）已审核本分析，D1–D5 全部按推荐采纳：

- D1 引入 src/lib.rs（lib + thin bin）：确认
- D2 Tokio feature = rt-multi-thread + macros + signal：确认
- D3 thiserror 2：确认
- D4 默认人类可读日志格式（不启用 json）：确认
- D5 run_with_shutdown 设为 pub 作为测试注入口：确认

状态已由 analyzing 推进至 implementing。
