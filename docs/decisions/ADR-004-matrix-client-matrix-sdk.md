# ADR-004: Matrix 客户端选型 — matrix-sdk

## Status

Accepted.

## Context

T010（Matrix 登录 / sync / room / thread 解析）需要一个 Matrix 客户端库。候选为：

- **matrix-sdk**：官方高层 Rust SDK，内置登录、sync 循环、room/thread 状态解析与事件解码。
- **ruma**：更低层的类型与协议库，控制力更强但需自行实现更多传输/状态逻辑。

选型只影响 `src/matrix/` adapter 层；桥接自身的路由、任务状态、循环检测、权限、去重与执行租约不随选型变化。

## Decision

采用 **matrix-sdk** 作为 Matrix 传输/状态客户端，并冻结以下边界：

- matrix-sdk **只负责**：登录/鉴权、sync 循环、room/thread 解析、事件解码与发送。
- 桥接自身**负责且不委托给 matrix-sdk**：结构化 recipient 与排他路由（INC-001）、任务状态与循环检测、权限与去重（T011）、执行租约（INC-002）、turn 生命周期（INC-003）。
- **类型不泄漏**：matrix-sdk 类型只出现在 `src/matrix/` adapter 层；`src/models/**`、`src/bus/**`、`src/storage/**` 只使用桥接自有模型与 `ExternalRef`。
- **v1 不引入 E2EE/设备管理**：未加密房间与明文事件是 v1 目标；加密房间/会话恢复/跨设备同步延后（作为显式后续决策）。
- **版本钉住**：matrix-sdk 版本写入 `Cargo.toml` 钉住；升级视为兼容性相关的公共契约变更（走 ADR/Coordinator 确认）。

## Consequences

- 引入较重的 matrix-sdk 依赖树，`Cargo.lock` 会显著增长；需串行协调 global config 变更（GOVERNANCE §25）。
- 登录凭据/访问令牌只存在于运行期配置与 `src/matrix/`，不得进入事件、日志、存储或 handoff。
- matrix-sdk 的事件模型与桥接模型之间须有显式映射边界，adapter 是唯一转换点。
