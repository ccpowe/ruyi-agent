# Ruyi Agent Context

Ruyi Agent is an agent runtime for multi-channel task execution, delegation, review, and recovery. This context records the project language used to discuss runtime behavior and channel architecture.

## Language

**Channel Turn**:
One inbound user interaction from a channel, interpreted against the current channel session and routed to a gateway task or review decision.
_Avoid_: message handler, adapter flow, chat loop

**Channel Adapter**:
A platform-specific adapter that receives channel messages and sends channel responses without owning cross-platform turn policy.
_Avoid_: bot service, platform service

**Channel Session**:
The persisted relationship between a channel identity and the active agent or current gateway task.
_Avoid_: chat state, conversation cache

**Gateway Task**:
A task created through the Gateway control plane and later continued, reviewed, cancelled, or queried by channel turns.
_Avoid_: job, request

**Review Command**:
A channel command that resolves a pending human review for a gateway task.
_Avoid_: approval message, HITL reply

**Task Watch**:
The shared policy for observing a gateway task after a channel turn until it reaches a pending review, a newer run, or a terminal state.
_Avoid_: polling loop, watcher task

**Active Agent**:
The agent selected for future channel turns within a channel identity.
_Avoid_: default bot, current worker

## Relationships

- A **Channel Adapter** produces **Channel Turns**.
- A **Channel Turn** reads and updates one **Channel Session**.
- A **Channel Session** may point to one current **Gateway Task**.
- A **Review Command** resolves a pending review on one **Gateway Task**.
- A **Task Watch** observes one run of one **Gateway Task**.
- An **Active Agent** determines which agent receives a new **Gateway Task** when a **Channel Turn** starts a fresh task.

## Example Dialogue

> **Dev:** "Should Telegram own `/resume` differently from Feishu?"
> **Domain expert:** "No. `/resume` is Channel Turn policy; each Channel Adapter should only expose it through its platform message format."

## Flagged Ambiguities

- "message handler" has been used for both platform receive/send code and cross-platform turn policy. Resolved: use **Channel Adapter** for platform code and **Channel Turn** for shared policy.
