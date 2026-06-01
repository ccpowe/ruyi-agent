# LangGraph checkpoint 存储膨胀调查

调查日期：2026-05-25
复核日期：2026-05-29

## 背景

当前运行时使用 LangGraph SQLite checkpointer 持久化 agent 会话状态：

- checkpointer 创建位置：`src/ruyi_agent/runtime/bootstrap.py`
- agent 创建位置：`src/ruyi_agent/runtime/agent_factory.py`
- 默认 checkpoint 文件：`data/checkpoints.sqlite`

本次调查的问题是：长会话下 LangGraph checkpoint 数据增长明显，是否有官方优化建议，以及升级到 `langgraph>=1.2` 是否会影响当前功能。

## 结论摘要

现有 checkpoint 变大是 LangGraph 默认机制导致的正常现象：每个 super-step 会新增一条 checkpoint，且 checkpoint 中保存当时的完整 state。对话历史、任务状态、todos 等 append-heavy 字段会在后续 checkpoint 中被重复包含。

2026-05-29 复核结论：

- 官方已经把 checkpoint 存储优化写进正式文档，推荐方向仍是 `DeltaChannel`：对 append-heavy channel 只保存增量。
- `DeltaChannel` 已随 `langgraph>=1.2` 进入正式发布链路；当前最新解析目标是 `langchain==1.3.2` / `langgraph==1.2.2`。
- 但官方仍明确标注 `DeltaChannel` 是 beta API，后续可能有破坏性调整。
- 对本项目来说，依赖升级本身已经基本可做；直接启用 `DeltaChannel` 还不应和依赖升级绑在同一个变更里。
- 关键原因：本项目当前通过 `langchain.agents.create_agent` 创建 agent，没有自己声明主 agent 的 `StateGraph` state schema。只升级依赖不会自动让 `messages` 改用 `DeltaChannel`，checkpoint 体积也不会自动下降。

例如 `messages` 的增长模式近似为：

```text
checkpoint 1: [m1]
checkpoint 2: [m1, m2]
checkpoint 3: [m1, m2, m3]
checkpoint 4: [m1, m2, m3, m4]
```

旧 checkpoint 不会被原地改写，但后续 checkpoint 会重复存储已有历史。因此总存储量会接近 `1 + 2 + 3 + ... + n` 的累积量，而不是只保存 `n` 条消息。

官方主要优化方向：

1. 使用 `DeltaChannel`，对 append-heavy channel 只存增量。
2. 使用 TTL 或 `delete_thread` 删除过期 checkpoint。
3. 对短期记忆做 trim、delete、summarize，避免最新 state 本身无限变大。
4. 避免把大对象或可推导对象放入 LangGraph state。

## 本项目当前状态

当前锁文件中的关键版本：

```text
langchain==1.2.15
langgraph==1.1.6
langgraph-checkpoint==4.0.1
langgraph-checkpoint-sqlite==3.0.3
langgraph-prebuilt==1.0.9
langgraph-sdk==0.3.12
```

当前 `langchain==1.2.15` 要求 `langgraph>=1.1.5,<1.2.0`，所以不能只单独升级 `langgraph` 到 `1.2`。要使用 `DeltaChannel`，需要一起升级到 LangChain 1.3 系列。

2026-05-29 复核时，PyPI 最新版本关系为：

```text
langchain==1.3.2                requires langgraph>=1.2.2,<1.3.0
langgraph==1.2.2                requires langgraph-checkpoint>=4.1.0,<5.0.0
langgraph-checkpoint==4.1.1
langgraph-checkpoint-sqlite==3.1.0
```

本地 checkpoint 数据粗略检查结果：

```text
data/checkpoints.sqlite      312K
data/checkpoints.sqlite-wal  3.7M
data/checkpoints.sqlite-shm  32K
```

SQLite 表：

```text
checkpoints: 126 rows
writes:      174 rows
```

payload 统计：

```text
checkpoints checkpoint+metadata payload: 约 1.52 MB
writes value payload:                  约 0.23 MB
```

这说明当前体积主要集中在完整 checkpoint blob，而不是单独 writes 表。

## DeltaChannel 是什么

`DeltaChannel` 是 LangGraph `1.2` 引入的 beta channel 类型，用于减少长线程 checkpoint 的重复存储。官方文档说明：它不会在每一步重新序列化完整累积值，而是只保存该步写入的增量。

普通 channel：

```text
step 1: store [m1]
step 2: store [m1, m2]
step 3: store [m1, m2, m3]
```

`DeltaChannel`：

```text
step 1: store +[m1]
step 2: store +[m2]
step 3: store +[m3]
```

读取 state 时，LangGraph 使用 reducer replay 增量，重建完整值。

简化示例：

```python
from typing import Annotated, Any, Sequence
from typing_extensions import TypedDict
from langgraph.channels import DeltaChannel


def list_reducer(state: list[Any], writes: Sequence[list[Any]]) -> list[Any]:
    result = list(state)
    for write in writes:
        result.extend(write)
    return result


class State(TypedDict):
    messages: Annotated[list[Any], DeltaChannel(list_reducer)]
```

关键约束：

- `DeltaChannel` 要求 `langgraph>=1.2`。
- 当前 API 是 beta，后续可能调整。
- reducer 是 bulk reducer，签名是 `reducer(state, writes)`，其中 `writes` 是当前 step 的所有写入序列。
- reducer 必须是纯函数，且应满足 batching-invariant：

```text
reducer(reducer(state, [xs]), [ys]) == reducer(state, [xs, ys])
```

- 读取时需要 replay 增量，可能增加恢复延迟。可以通过 `snapshot_frequency` 周期性写完整快照，平衡存储量与读取速度。

## DeltaChannel 与 TTL 的区别

这两个方案解决的问题不同。

`DeltaChannel` 是改变 checkpoint 的存储格式：

- 仍可保留多个历史 checkpoint。
- 每个 checkpoint 只保存增量，减少重复数据。
- 更适合仍然需要 time travel、历史调试、fork、回滚能力的场景。

TTL 或 `delete_thread` 是删除历史 checkpoint：

- 可以只保留最新 checkpoint，或者删除整个 thread。
- 对“继续最新会话”和“服务重启后从最新状态恢复”通常足够。
- 会影响 time travel、回滚到旧步骤、按旧 checkpoint 分叉、完整历史调试。

如果只需要继续最新会话，每个 thread 保留最新 checkpoint 基本够用。如果需要 LangGraph 的历史 replay 和 debug 能力，就不应该只保留最新。

## 升级到 LangGraph 1.2 的影响

2026-05-29 依赖解析 dry-run 显示，基于当前锁文件做最小相关升级会变更以下 6 个包：

```text
langchain 1.2.15 -> 1.3.2
langgraph 1.1.6 -> 1.2.2
langgraph-checkpoint 4.0.1 -> 4.1.1
langgraph-checkpoint-sqlite 3.0.3 -> 3.1.0
langgraph-prebuilt 1.0.9 -> 1.1.0
langgraph-sdk 0.3.12 -> 0.3.15
```

不需要升级到 `deepagents>=0.6`。当前 `deepagents>=0.5,<0.6` 约束仍可解析。

升级后的关键 API 检查结果：

- `langchain.agents.create_agent` 仍存在。
- `langchain.agents.middleware.types.AgentMiddleware` 等 middleware 类型仍存在。
- `langgraph.types.Command` 和 `interrupt` 仍存在。
- `langgraph.config.get_config` 仍存在。
- `langgraph.prebuilt.ToolRuntime` 仍存在。
- `langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver` 仍存在。
- `langgraph.channels.DeltaChannel` 可导入。

本项目当前显式使用 `version="v2"` 调用 `ainvoke` / `astream`。LangGraph 1.2 新增 v3 streaming，但这是 opt-in，不会自动改变当前 v2 流式行为。

## 验证结果

2026-05-25 初次验证：使用隔离环境安装当时解析到的关键包，并运行测试：

```text
关键 LangGraph 相关测试：60 passed
完整单测：341 passed in 26.21s
```

还复制了一份当前 checkpoint SQLite 数据库到 `/tmp`，用新版 `AsyncSqliteSaver` 读取：

```text
checkpoint rows readable 126
```

这说明在当时测试覆盖范围内，升级到 `langchain==1.3.1` / `langgraph==1.2.1` 没有发现破坏当前功能的问题，旧 checkpoint 数据也至少可以被新版 saver 读取。

2026-05-29 使用 `langchain==1.3.2` / `langgraph==1.2.2` / `langgraph-checkpoint-sqlite==3.1.0` 复测：

```text
关键导入检查：passed
LangGraph 相关单测：56 passed in 9.48s
完整单测：350 passed in 22.68s
```

关键导入包括：

- `langchain.agents.create_agent`
- `langchain.agents.middleware.types.AgentMiddleware`
- `langchain.agents.middleware.types.AgentState`
- `langgraph.types.Command`
- `langgraph.types.interrupt`
- `langgraph.config.get_config`
- `langgraph.prebuilt.ToolRuntime`
- `langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver`
- `langgraph.channels.DeltaChannel`

## 建议

短期建议：

1. 先做 thread 级 checkpoint 清理能力。
   - 对已结束 worker task 或过期 thread 调用 `checkpointer.delete_thread(thread_id)`。
   - 对只需要最新状态的会话，可以设计“保留最新 checkpoint，删除旧 checkpoint”的策略。

2. 对长会话做短期记忆治理。
   - 定期 summarize 旧消息。
   - 用 `RemoveMessage` 删除已总结的旧消息。
   - 只 trim 模型上下文不一定能减少 checkpoint，需要真正改变 state 中的 `messages`。

3. 避免把大对象放进 state。
   - 大文件、长结果、artifact 内容应放外部存储或 backend 文件系统。
   - state 中只保存引用、摘要、路径或 ID。

中期建议：

1. 定向升级这 6 个包，而不是执行全量 `uv lock -U`。
2. 升级前备份 `data/checkpoints.sqlite*`。
3. 升级后运行完整单测和至少一次真实 agent 会话烟测。
4. 把“启用 `DeltaChannel`”拆成单独实验变更：
   - 先确认 `create_agent` 是否支持替换/扩展 agent state schema。
   - 如果不支持，需要评估是否值得从 `create_agent` 迁到显式 `StateGraph`，或等待 LangChain agent 暴露更稳定的 state/channel 配置能力。
   - 实验时优先只覆盖 `messages` 这类 append-heavy channel，并设置合适的 `snapshot_frequency`，避免长 thread 读取时 replay 过深。

注意：只升级到 `langgraph>=1.2` 不会自动减少 checkpoint。`DeltaChannel` 需要后续显式配置 state schema 才会生效。

当前建议的落地顺序：

1. 可以先做依赖升级 PR：`langchain==1.3.2` / `langgraph==1.2.2` 这组在本地测试里已通过。
2. 不在同一个 PR 中启用 `DeltaChannel`，避免把 beta API 行为变化和基础依赖升级混在一起。
3. 依赖升级合入后，再开一个实验分支验证 `messages` channel 的 `DeltaChannel` 接入方式、checkpoint 体积收益和恢复延迟。

## 参考资料

- LangGraph persistence 文档：https://docs.langchain.com/oss/python/langgraph/persistence
- LangGraph runtime / DeltaChannel 文档：https://docs.langchain.com/oss/python/langgraph/pregel
- LangGraph checkpointer integrations 文档：https://docs.langchain.com/oss/python/integrations/checkpointers/index
- LangGraph memory 管理文档：https://docs.langchain.com/oss/python/langgraph/add-memory
- LangSmith / LangGraph Platform TTL 文档：https://docs.langchain.com/langsmith/configure-ttl
- LangSmith checkpointer backend 文档：https://docs.langchain.com/langsmith/configure-checkpointer
- LangChain / LangGraph release policy：https://docs.langchain.com/oss/python/release-policy
- `langchain==1.2.15` PyPI metadata：https://pypi.org/pypi/langchain/1.2.15/json
- `langchain==1.3.2` PyPI metadata：https://pypi.org/pypi/langchain/1.3.2/json
- `langgraph==1.2.2` PyPI metadata：https://pypi.org/pypi/langgraph/1.2.2/json
- `langgraph-checkpoint==4.1.1` PyPI metadata：https://pypi.org/pypi/langgraph-checkpoint/4.1.1/json
- `langgraph-checkpoint-sqlite==3.1.0` PyPI metadata：https://pypi.org/pypi/langgraph-checkpoint-sqlite/3.1.0/json
- LangGraph releases：https://github.com/langchain-ai/langgraph/releases
