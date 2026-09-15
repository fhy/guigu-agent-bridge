# T003 — 配置加载与校验

## 元数据

- 状态：queued
- 阶段：一「模型和配置」
- Owner：未指派
- 依赖：T001
- 阻塞：T004、T010
- Base commit：`3a717d4`（main）
- 设计门禁：先提交替换顺序、路径展开、未知字段和校验契约；Coordinator 明确确认后才能进入 `implementing`

## 目标

实现与 `config.example.toml` 对应的配置结构，支持文件加载、环境变量替换与启动期校验。

## 范围

- 目标文件：`src/config.rs`（结构体、加载、校验、默认值）。
- 覆盖字段：
  - `[bridge]`：`database`、`session_root`、`max_task_depth`、`max_task_hops`、`default_timeout_seconds`。
  - `[transports.matrix]`：`enabled`、`homeserver`、`user_id`、`access_token`、`monitor_room`。
  - `[agents.*]`：`transport`、`command`、`args`、`enabled`。
- 支持 `{env:VAR}` 占位符替换；凭据只从环境/secret 读取，不写进仓库或日志。
- 校验规则：超时 > 0、depth/hops 设合理上限、路径可展开（`~`）、缺省值兜底。
- 设计分析必须明确：
  - `{env:VAR}` 在 TOML 解析前还是解析后替换，支持哪些值类型和转义规则。
  - 缺失或空环境变量的差异及错误定位方式。
  - 只有哪些路径字段允许 `~`，`~user` 和相对路径如何处理。
  - 未知字段、重复 Agent ID、空 command/args、禁用 transport 和无效 URL 的行为。
  - secret 字段的 `Debug`、日志和错误信息脱敏策略。
- 排除：不实现 Matrix 连接、Agent 启动、数据库初始化。

## 共享接口

- `Config` / `BridgeConfig` / `TransportConfig` / `AgentEndpointConfig` 由 `main` 与后续模块消费，命名与字段在 handoff 中声明。

## 风险

- `{env:VAR}` 解析与 TOML 类型约束需明确：变量缺失或未替换应显式报错，不得静默使用空值。
- 配置热加载（阶段七）应预留结构，但本任务不实现。
- `Config` 是 main、Transport 和 Adapter 共用接口；未经 Coordinator 确认不得自行改变字段语义。

## 验收标准

- [ ] `config.example.toml` 可完整解析为 `Config`。
- [ ] 环境变量替换、缺失变量报错、默认值与校验规则均有单元测试。
- [ ] 日志不输出 `access_token` 等凭据。
- [ ] 门禁（fmt / check / clippy / test）全部通过。
- [ ] 设计分析已经 Coordinator 确认，handoff 记录最终配置契约与精确 commit。

## 门禁

```text
cargo fmt --check
cargo check
cargo clippy -- -D warnings
cargo test
```
