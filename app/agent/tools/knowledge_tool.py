"""Agent tool for reviewed explanations from the ChatMRPT knowledge library."""

from langchain_core.tools import tool

from app.knowledge import find, get


@tool
def lookup_knowledge(topic: str) -> str:
    """Look up ChatMRPT's reviewed explanation of a topic.

    Use this before explaining how ChatMRPT works or any of its methods:
    workflow, data requirements, malaria burden, TPR, composite scoring, PCA,
    risk categories, risk variables, urban extent, settlement classification,
    ITN planning, Arena, glossary terms, maps and exports.

    Args:
        topic: The topic or the user's question, in plain words.
    """
    entry = get(topic.strip().lower())
    entries = [entry] if entry and entry.in_prompts else find(topic, limit=2)
    if not entries:
        return (
            f"No reviewed explanation found for '{topic}'. If this is about how ChatMRPT "
            "works, say you are not certain rather than guessing."
        )
    return "\n\n".join(e.full_text() for e in entries)
