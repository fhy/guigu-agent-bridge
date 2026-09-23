# guigu-agent-bridge

`guigu-agent-bridge` 是一个面向用户和多个外部 Agent 的通信与任务路由基础设施。

项目的内部协作使用 Agent Bus，SQLite 保存任务和事件状态，Matrix 作为用户交互、可观察性和人工控制界面。外部 Agent 可以通过 ACP、Matrix、HTTP 等 Adapter 接入。

当前仓库已实现 Agent Bus、SQLite 持久化、Matrix 接入与观察、ACP Adapter、运行时恢复和可靠性验收。正式架构见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 快速开始

```bash
cargo install guigu-agent-bridge --version 0.2.0
install -m 600 config.example.toml "$HOME/.config/guigu-agent-bridge.toml"
export MATRIX_USER_ID='@bridge:example.org'
export MATRIX_ACCESS_TOKEN='replace-with-secret-manager-value'
guigu-agent-bridge "$HOME/.config/guigu-agent-bridge.toml"
```

示例配置默认关闭 Matrix、Gateway、A2A 和 Agent。首次运行前按
[配置参考](docs/CONFIGURATION.md) 启用所需端点并填写严格 allowlist。进程启动会在
配置的 SQLite 文件上运行 embedded migrations；生产环境必须先按
[发布清单](docs/RELEASE.md) 备份。

## 文档

- [产品与边界](docs/PRODUCT.md)
- [用户指南](docs/USER_GUIDE.md)
- [配置参考](docs/CONFIGURATION.md)
- [架构](docs/ARCHITECTURE.md)
- [0.2.0 发布说明](docs/RELEASE-0.2.0.md)
- [发布、监控与回滚清单](docs/RELEASE.md)

动态任务状态、内部审查、交接和事件记录不属于公开代码仓库。

## 设计边界

Bridge 负责消息接入、Agent 路由、任务投递、会话管理、权限、超时、重试、取消、循环检测和状态观测；具体 Agent 负责模型、提示词、工具和业务逻辑。项目不固定 planner、worker、reviewer 等角色。

```text
Agent Bus   内部任务执行通道
SQLite      任务和状态的事实来源
Matrix      用户交互、观测和人工控制界面
```

## 开发准备

需要 Rust 1.94 或更新的 stable toolchain。初始化项目后可运行：

```bash
cargo check
cargo test
cargo fmt --check
```

推荐先阅读架构文档，再按其中的阶段顺序实现模型、内存 Bus、SQLite、Matrix Observer 和 ACP Adapter。

## 发布与替换

v0.2.0 的 E2EE、crypto store、兼容性和升级注意事项见
[版本发布说明](docs/RELEASE-0.2.0.md)。配置核验、SQLite 迁移、
`opencode-chat-bridge` 替换、健康检查、监控和回滚步骤见
[发布清单](docs/RELEASE.md)。迁移由进程启动时自动执行；
发布前必须备份数据库，并且不得让新旧 Bridge 同时消费同一 Matrix 账户或房间。

## 目录

```text
src/main.rs              程序入口
docs/ARCHITECTURE.md     已确定的架构和实施方案
docs/RELEASE.md          部署、监控、迁移和回滚清单
docs/decisions/          架构决策记录
docs/integrations/       外部集成边界
Cargo.toml               Rust 项目清单
```

## 许可证

本项目采用 MIT License，见 [LICENSE](LICENSE)。
