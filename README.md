# guigu-agent-bridge

`guigu-agent-bridge` 是一个面向用户和多个外部 Agent 的通信与任务路由基础设施。

项目的内部协作使用 Agent Bus，SQLite 保存任务和事件状态，Matrix 作为用户交互、可观察性和人工控制界面。外部 Agent 可以通过 ACP、Matrix、HTTP 等 Adapter 接入。

当前仓库处于 Rust 项目初始化和架构设计阶段，尚未提供可用的生产连接器。正式架构和分阶段实施计划见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 设计边界

Bridge 负责消息接入、Agent 路由、任务投递、会话管理、权限、超时、重试、取消、循环检测和状态观测；具体 Agent 负责模型、提示词、工具和业务逻辑。项目不固定 planner、worker、reviewer 等角色。

```text
Agent Bus   内部任务执行通道
SQLite      任务和状态的事实来源
Matrix      用户交互、观测和人工控制界面
```

## 开发准备

需要 Rust stable toolchain。初始化项目后可运行：

```bash
cargo check
cargo test
cargo fmt --check
```

推荐先阅读架构文档，再按其中的阶段顺序实现模型、内存 Bus、SQLite、Matrix Observer 和 ACP Adapter。

## 目录

```text
src/main.rs              程序入口（当前为初始化模板）
docs/ARCHITECTURE.md     已确定的架构和实施方案
Cargo.toml               Rust 项目清单
```

## 许可证

本项目采用 MIT License，见 [LICENSE](LICENSE)。
