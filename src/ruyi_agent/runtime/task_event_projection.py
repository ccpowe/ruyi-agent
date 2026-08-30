from langchain_core.messages import AIMessageChunk

import ruyi_agent.gateway_protocol.projection as _projection


def __getattr__(name: str) -> object:
    if name not in _projection.__dict__:
        raise AttributeError(name)
    return _projection.__dict__[name]


_PUBLIC_PROVENANCE = {
    "ns": (),
    "langgraph_node": "model",
    "langgraph_path": ("__pregel_pull", "model"),
}


def assistant_delta_from_stream_part(part: object) -> str | None:
    if not isinstance(part, dict) or any(
        (part.get("type") != "messages", part.get("ns") != ())
    ):
        return None
    data = part.get("data")
    if not isinstance(data, (tuple, list)) or len(data) != 2:
        return None
    message, metadata = data
    if not isinstance(message, AIMessageChunk) or not isinstance(metadata, dict):
        return None
    if (metadata.get("langgraph_node"), metadata.get("langgraph_path")) != (
        "model",
        ("__pregel_pull", "model"),
    ):
        return None
    content = message.content
    text = (
        content
        if isinstance(content, str)
        else "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict)
            and block.get("type") in {"text", "output_text"}
            and isinstance(block.get("text"), str)
        )
        if isinstance(content, list)
        else None
    )
    if not text:
        return None
    delta = _projection.assistant_delta_from_stream_part(
        _projection.AssistantDelta(text, _PUBLIC_PROVENANCE)
    )
    return delta.content if delta is not None else None


__all__ = tuple(
    "artifact_event_data AssistantDelta assistant_delta_from_stream_part "
    "build_reconciled_anchor end_reason_for_record end_reason_from_data "
    "ensure_durable_event_data_fits lifecycle_event_data lifecycle_event_type "
    "normalize_task_event_text public_task_event_fingerprint "
    "stored_event_to_stream_event stream_end_event".split()
)
