# T3 Code 项目进度快照

> 更新时间：2026-08-27 01:35 | 会话：default
> 本文件是跨会话的权威进度记录，更新进度时直接改写此文件。

## 项目路线图（自用中文多模型控制台）

| #   | 阶段                                                    | 状态                                                                                                                                      |
| --- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | 汉化 lingui（设置页/侧栏/审批弹窗等主体）               | ✅ 完成                                                                                                                                   |
| 2   | Antigravity 驱动（agy CLI stream-json）                 | ✅ **完成，已推送 + 01:35 热修复 `already exists`**                                                                                       |
| 3   | GeminiDriver（ACP 协议）                                | ⏭️ **跳过** — `agy` 已覆盖 Gemini 3.7/3.6/3.1 全系列 + Claude/GPT-OSS，无需单独接 `gemini` CLI（按需再补）                                |
| 4   | DshDriver（HTTP POST /api + 双 WS 下行，loopback 免配） | ✅ **完成 — feat/dsh-driver 3157行，POST /api + 双WS，思考/工具常驻，审批/中断/恢复，同线程切模型，typecheck 0，已推 fork，待 3080 联调** |
| 5   | 国产模型直连                                            | ⬜ 未开始                                                                                                                                 |
| 6   | 插件管理页签                                            | ⬜ 未开始                                                                                                                                 |

## Git 状态（关键！）

- 本地仓库：`~/Documents/t3code`（全量 clone）
- **`origin` = pingdotgg/t3code（上游，禁止 push！）**
- **push 目标 remote 名为 `fork`** = Hu9956/t3code
- 分支 `feat/i18n-zh` 已推送并跟踪 `fork/feat/i18n-zh`
- HEAD = `f87b24b8`（fix 探测挂起），前一提交 `74d82d34`（Antigravity 驱动主体），i18n 收尾在 `e30aaf82`
- **工作区未提交**：`AntigravityAdapter.ts:216` 热修复 `startSession already exists` → 先 kill 旧会话再起（对标 Cursor/Grok 逻辑），待 `typecheck` 后提交
- 注意：分支混了 i18n 与驱动两条线，若拆 PR 以 `e30aaf82` 为界

## Antigravity 驱动已交付内容

新文件：

- `apps/server/src/provider/Drivers/AntigravityDriver.ts`
- `apps/server/src/provider/Layers/AntigravityAdapter.ts`（NDJSON 会话运行时）
- `apps/server/src/provider/Layers/AntigravityProvider.ts`（--version + models 探测）
- `apps/server/src/provider/Services/AntigravityAdapter.ts`（shape 接口）
- `apps/server/src/textGeneration/AntigravityTextGeneration.ts`

改动注册点：`builtInDrivers.ts`、`serverSettings.ts`、contracts `settings.ts`/`model.ts`、web 端 `session-logic.ts`/`providerIconUtils.ts`/`providerDriverMeta.ts`。

v1 范围与限制（留 v2）：

- 文本回合闭环；权限走 `--mode accept-edits`，无交互审批 UI
- 回复是一次性整体下发（result 帧到齐才发一个 content.delta），step_update 尚未翻译成增量流（思考/工具不可见符合预期）
- 无会话内模型切换（capabilities: unsupported）
- 默认关闭，引擎设置页手动启用

## 关键技术事实（踩坑实录）

1. **agy 一次性子命令（models/--version 等）在 stdin 管道保持打开时永远挂起等 EOF** → Effect ChildProcess 必须 `stdin: "ignore"`。stream-json 会话进程相反，要保持 stdin。
2. `agy models` 是真实网络调用，代理链路实测 ~7s；`MODELS_PROBE_TIMEOUT_MS` 已放宽至 30s。
3. stream-json 协议实测（agy 1.1.21）：stdin 发一帧 `{event:"user",message:{content:[{type:"text",text:"…"}]}}`，stdout 依序 `init`（带 conversation_id）→ `step_update`\* → `result`（status/response/usage）。会话=单进程多回合。
4. dev server（`node src/bin.ts --watch`）：**watch 重启会丢 T3CODE_HOME**，导致 server 切到 `~/.t3/dev` 而 token 失效 → 需 `T3CODE_HOME=$PWD/.t3-dev` 重启 `pnpm dev`，并修正 `server-runtime.json` 的 pid。`pnpm dev` 自带 `--watch`，改后端后会自动重载但需确认 env 仍在。
5. **AntigravityAdapter `already exists` 修复（2026-08-27）**：`ProviderCommandReactor` 在 `cwdChanged` 时会 `restarting provider session` 并对同一 `threadId` 再调 `startSession`；Cursor/Grok 先 `stopSessionInternal` 再起新进程，Antigravity v1 直接抛错 → 改为 `existing && killContext(existing)` 再 `sessions.delete`。
6. 无头 RPC 冒烟方法：`bin.ts auth session issue --label x --ttl 10m --json` 拿 Bearer token → POST `/api/auth/websocket-ticket` 拿 ticket → 连 `ws://127.0.0.1:13773/ws?wsTicket=…` → 发 `{_tag:"Request",id,tag,payload,headers:[]}`（tag 如 `server.getSettings` / `server.updateSettings` / `server.refreshProviders`）→ 响应取 `exit.value`。脚本模板在 `/tmp/t3-smoke*.mjs`（临时）。

## 测试遗留与环境

- dev 在 `13773/5733` 跑着（PID 75229，`T3CODE_HOME=.t3-dev`），配对 token `FA5SUHYTC3PJ` 已验证 `authenticated:true`，预览窗已进 `minitodo / New thread`。
- `providers.antigravity.enabled=true`，`status:ready/authenticated`，模型 14 个。
- 已知长尾：外观页主题卡片的英文 aria-label（视觉无影响，低优先级）。
- TokenTracker、minitodo 等其他项目与本线无关。

## 下一步（DshDriver 要点）

- 协议：`POST /api`（Typert RPC 单一路由）+ `/api/events.mux` & `/api/events.host` 双 WS 仅下行，loopback 免配，`pluginInventory/list` 插件清单
- 参考：`packages/client/connection` + `examples/jsonrpc-agent`（备选 stdio JSON-RPC）
- 需做：能力探测 + 接口变更可读报错（rc 期无兼容承诺）
