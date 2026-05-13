"""Canonical response selection for deterministic agent tools."""

from __future__ import annotations

import logging
import os
from typing import Any

from langchain_core.messages import BaseMessage

logger = logging.getLogger(__name__)

CANONICAL_TOOL_ALLOWLIST = {
    "run_risk_analysis",
    "plan_itn_distribution",
}


def canonical_responses_enabled() -> bool:
    """Return whether deterministic tool messages should override LLM paraphrases."""
    value = os.getenv("CHATMRPT_USE_CANONICAL_TOOL_RESPONSES", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def select_canonical_response(state: dict[str, Any]) -> dict[str, Any] | None:
    """Select the canonical tool response that should be shown to the user."""
    if not canonical_responses_enabled():
        return None

    candidates = []
    for item in state.get("canonical_responses") or []:
        if not isinstance(item, dict):
            continue
        if item.get("tool_name") not in CANONICAL_TOOL_ALLOWLIST:
            continue
        message = (item.get("message") or "").strip()
        if not message:
            continue
        candidates.append(item)

    if not candidates:
        return None

    def sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
        requires_input = 1 if item.get("requires_user_input") else 0
        priority = int(item.get("priority") or 0)
        sequence = int(item.get("sequence") or 0)
        return (requires_input, priority, sequence)

    selected = max(candidates, key=sort_key)
    logger.info(
        "[CANONICAL] selected tool=%s success=%s requires_input=%s priority=%s sequence=%s",
        selected.get("tool_name"),
        selected.get("success"),
        selected.get("requires_user_input"),
        selected.get("priority"),
        selected.get("sequence"),
    )
    return selected


def select_final_message(state: dict[str, Any], final_message: BaseMessage | None) -> tuple[str, dict[str, Any]]:
    """Return the user-facing message and metadata about where it came from."""
    selected = select_canonical_response(state)
    if selected:
        return selected["message"].strip(), {
            "source": "canonical",
            "tool_name": selected.get("tool_name"),
            "success": selected.get("success"),
            "requires_user_input": selected.get("requires_user_input", False),
            "priority": selected.get("priority"),
        }

    content = final_message.content if final_message else "Analysis complete."
    return str(content or "Analysis complete.").strip(), {"source": "ai_final"}
