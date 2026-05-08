"""WhatsApp adapters for Arena mode."""

from __future__ import annotations

from textwrap import shorten

from app.whatsapp.responses import arena_unavailable_response


def normalize_vote(vote: str) -> str:
    mapping = {"a": "left", "b": "right", "tie": "tie"}
    return mapping.get((vote or "").lower().strip(), "tie")


def format_battle(result: dict) -> str:
    if not result or "error" in result:
        return arena_unavailable_response((result or {}).get("error"))

    response_a = shorten((result.get("response_a") or "").replace("\n", " "), width=700, placeholder="...")
    response_b = shorten((result.get("response_b") or "").replace("\n", " "), width=700, placeholder="...")
    round_no = result.get("current_round", 1)

    return (
        f"Arena comparison - round {round_no}\n\n"
        f"A:\n{response_a}\n\n"
        f"B:\n{response_b}\n\n"
        "Reply A, B, or tie. Type cancel arena to stop."
    )


def format_vote_result(result: dict) -> str:
    if not result or "error" in result:
        return arena_unavailable_response((result or {}).get("error"))
    if result.get("continue"):
        return format_battle(result)

    winner = result.get("winner") or "No winner recorded"
    ranking = result.get("final_ranking") or []
    ranking_text = ", ".join(ranking[:4]) if ranking else "Not available"
    return f"Arena complete.\n\nWinner: {winner}\nRanking: {ranking_text}"


def start_battle(prompt: str, session_id: str | None = None) -> dict:
    from app.api.analysis.arena_helpers import start_arena_battle

    try:
        return start_arena_battle(prompt, session_id=session_id)
    except Exception as exc:
        return {"error": str(exc)}


def submit_vote(battle_id: str, vote: str) -> dict:
    from app.api.arena_routes import arena_manager

    if arena_manager is None:
        return {"error": "Arena system is not initialized"}
    try:
        return arena_manager.submit_vote(battle_id, normalize_vote(vote))
    except Exception as exc:
        return {"error": str(exc)}
