"""Concise WhatsApp-specific direct replies."""

from __future__ import annotations

from app.knowledge import get
from app.whatsapp.formatter import format_welcome


def _summary(entry_id: str) -> str:
    """Summary text of a knowledge library entry."""
    entry = get(entry_id)
    if entry is None:
        raise KeyError(f"Knowledge entry '{entry_id}' not found")
    return entry.summary


def identity_response() -> str:
    return _summary("about-chatmrpt")


def capabilities_response(has_upload: bool = False) -> str:
    if has_upload:
        return (
            "Your data is ready. You can ask me to summarize it, start the TPR workflow, "
            "map malaria burden, run malaria risk analysis, or use Arena with `arena: your question`."
        )
    return (
        "You can ask general malaria questions here, or send a CSV/Excel file to analyze your own data. "
        "After upload I can run TPR, burden maps, risk ranking, and ITN planning. Use `arena: your question` "
        "for an optional multi-model expert comparison."
    )


def upload_guidance_response() -> str:
    return (
        "Send a CSV or Excel file directly in WhatsApp. For TPR/burden analysis, include facility/location fields, "
        "reporting period, tested counts, and positive counts. I will process the file and tell you when it is ready."
    )


def tpr_definition_response() -> str:
    return (
        _summary("tpr")
        + "\n\nTo calculate it for your facilities, upload a CSV or Excel file with tested and positive counts."
    )


def burden_definition_response() -> str:
    return _summary("malaria-burden")


def malaria_general_response() -> str:
    return (
        "Malaria is a mosquito-borne disease caused by Plasmodium parasites. Surveillance teams usually track "
        "confirmed cases, testing volume, positivity rate, facility attendance, geography, seasonality, and population "
        "risk so programs can target prevention, diagnosis, and treatment."
    )


def risk_mapping_response() -> str:
    return _summary("risk-variables") + "\n\n" + _summary("risk-categories")


def itn_planning_response() -> str:
    return (
        _summary("itn-planning")
        + "\n\nUpload your data first if you want ChatMRPT to run the planning workflow."
    )


def upload_required_response(task: str | None = None) -> str:
    if task:
        return (
            f"I can help with {task}, but I need your dataset first. Send a CSV or Excel file here, "
            "wait for the upload confirmation, then ask the same question again."
        )
    return (
        "I can do that after you upload a CSV or Excel file. Send the file here, wait for the upload confirmation, "
        "then ask the same question again."
    )


def upload_processing_response() -> str:
    return "Your file is still processing. I'll message you when it's ready."


def unsupported_response(has_upload: bool = False) -> str:
    if has_upload:
        return (
            "I can help with your uploaded data, TPR workflow, burden maps, risk analysis, or ITN planning. "
            "Try asking: `summarize my data`, `start TPR workflow`, or `map malaria burden`."
        )
    return (
        "I can answer malaria surveillance questions or analyze your data after upload. "
        "Try `what is TPR?`, `what data do I need?`, or send a CSV/Excel file."
    )


def workflow_side_help_response(text: str) -> str:
    lowered = (text or "").lower()
    if "primary" in lowered:
        answer = "Primary facilities are frontline health facilities such as PHCs and clinics."
    elif "secondary" in lowered:
        answer = "Secondary facilities are referral-level facilities, often general hospitals."
    elif "tertiary" in lowered:
        answer = "Tertiary facilities are higher-level specialist or teaching hospitals."
    elif "u5" in lowered or "under" in lowered:
        answer = "U5 means children under 5 years old."
    elif "o5" in lowered or "over" in lowered:
        answer = "O5 means people over 5 years old."
    elif "pw" in lowered or "pregnant" in lowered:
        answer = "PW means pregnant women."
    else:
        answer = "I can answer a quick workflow question without changing your current selection."
    return answer + "\n\nWhen you're ready, reply with one of the options shown in the workflow prompt."


def arena_help_response() -> str:
    return (
        "Arena compares answers from multiple models. Start one with `arena: your question` or "
        "`compare models: your question`. After I send the comparison, reply `A`, `B`, or `tie`."
    )


def arena_unavailable_response(error: str | None = None) -> str:
    base = "Arena is not available right now, but normal ChatMRPT analysis still works."
    return f"{base} Details: {error}" if error else base


def welcome_response() -> str:
    return format_welcome()
