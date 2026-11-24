"""Check that answers about the data were based on a tool call in the same turn.

The model sometimes answers a data question from the conversation history alone, inventing ward
names and scores. When a reply names wards from the data or quotes several figures and no tool ran
since the user's message, the agent asks the model again with a tool call required.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, List, Set

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

WARD_COLUMNS = ('WardName', 'ward_name', 'Ward', 'ward')
DECIMAL = re.compile(r'(?<![\w.])\d+\.\d+(?!\d)')


def ward_names(input_data: Iterable[dict]) -> Set[str]:
    """Lower-case ward names in the loaded datasets (names of 4+ characters only)."""
    names: Set[str] = set()
    for item in input_data or []:
        frame = item.get('data') if isinstance(item, dict) else None
        if frame is None or not hasattr(frame, 'columns'):
            continue
        column = next((c for c in WARD_COLUMNS if c in frame.columns), None)
        if column is None:
            continue
        names |= {str(v).strip().lower() for v in frame[column].dropna().unique() if len(str(v).strip()) >= 4}
    return names


def tool_used_this_turn(messages: List[BaseMessage]) -> bool:
    """True if a tool was called after the user's latest message."""
    for message in reversed(messages):
        if isinstance(message, ToolMessage):
            return True
        if isinstance(message, AIMessage) and getattr(message, 'tool_calls', None):
            return True
        if isinstance(message, HumanMessage) and '[WORKFLOW CONTEXT]' not in str(message.content):
            return False
    return False


def quotes_data(reply: Any, names: Set[str]) -> bool:
    """True if a reply names at least two wards from the data or quotes three or more decimal figures."""
    text = str(reply or '').lower()
    if not text:
        return False
    mentioned = sum(1 for name in names if re.search(r'(?<!\w)' + re.escape(name) + r'(?!\w)', text))
    return mentioned >= 2 or len(DECIMAL.findall(text)) >= 3
