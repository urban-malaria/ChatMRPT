"""Deterministic WhatsApp text routing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from app.whatsapp import responses


class WhatsAppRouteType(str, Enum):
    RESET = "reset"
    WELCOME = "welcome"
    SIDE_HELP = "side_help"
    ARENA_CANCEL = "arena_cancel"
    ARENA_COMMAND = "arena_command"
    ARENA_VOTE = "arena_vote"
    TPR_ACTIVE = "tpr_active"
    TPR_START = "tpr_start"
    DATA_QUESTION = "data_question"
    NO_DATA_EDUCATION = "no_data_education"
    UPLOAD_NEEDED = "upload_needed"
    UPLOAD_PROCESSING = "upload_processing"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class WhatsAppRouteDecision:
    route_type: WhatsAppRouteType
    reply: str | None = None
    analysis_message: str | None = None
    arena_prompt: str | None = None
    arena_vote: str | None = None
    reason: str = ""


_RESET_COMMANDS = {"reset", "restart", "new chat", "start over"}
_HELP_COMMANDS = {"help", "start", "hi", "hello", "heloo", "hey", "hiya", "good morning", "good afternoon", "good evening"}
_ARENA_PREFIXES = ("arena:", "compare models:", "expert view:")
_ARENA_CANCEL = {"cancel arena", "stop arena", "end arena"}
_ARENA_VOTES = {"a": "a", "b": "b", "tie": "tie"}
_WORKFLOW_SELECTIONS = {"yes", "y", "continue", "proceed", "start", "begin", "ok", "okay", "primary", "secondary", "tertiary", "u5", "o5", "pw", "all", "back", "exit", "status"}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _contains_any(text: str, terms: tuple[str, ...] | set[str]) -> bool:
    return any(term in text for term in terms)


def _arena_prompt(text: str) -> str | None:
    lowered = _norm(text)
    for prefix in _ARENA_PREFIXES:
        if lowered.startswith(prefix):
            return text.split(":", 1)[1].strip() if ":" in text else ""
    return None


def classify_whatsapp_message(
    text: str,
    *,
    has_ready_upload: bool,
    upload_processing: bool = False,
    workflow_active: bool = False,
    arena_active: bool = False,
) -> WhatsAppRouteDecision:
    """Return a deterministic route decision for WhatsApp text."""
    lowered = _norm(text)
    if not lowered:
        return WhatsAppRouteDecision(WhatsAppRouteType.UNSUPPORTED, responses.unsupported_response(has_ready_upload), reason="empty")

    if lowered in _RESET_COMMANDS:
        return WhatsAppRouteDecision(WhatsAppRouteType.RESET, reason="reset_command")

    if lowered in _ARENA_CANCEL:
        return WhatsAppRouteDecision(WhatsAppRouteType.ARENA_CANCEL, "Arena cancelled. " + responses.capabilities_response(has_ready_upload), reason="arena_cancel")

    if arena_active and lowered in _ARENA_VOTES:
        return WhatsAppRouteDecision(WhatsAppRouteType.ARENA_VOTE, arena_vote=_ARENA_VOTES[lowered], reason="arena_vote")

    arena_prompt = _arena_prompt(text)
    if arena_prompt is not None:
        if not arena_prompt:
            return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.arena_help_response(), reason="arena_help")
        return WhatsAppRouteDecision(WhatsAppRouteType.ARENA_COMMAND, arena_prompt=arena_prompt, reason="arena_prefix")

    if upload_processing:
        return WhatsAppRouteDecision(WhatsAppRouteType.UPLOAD_PROCESSING, responses.upload_processing_response(), reason="upload_processing")

    if lowered in _HELP_COMMANDS:
        return WhatsAppRouteDecision(WhatsAppRouteType.WELCOME, responses.welcome_response(), reason="help_or_greeting")

    if workflow_active:
        if lowered in _WORKFLOW_SELECTIONS:
            return WhatsAppRouteDecision(WhatsAppRouteType.TPR_ACTIVE, analysis_message=text, reason="workflow_selection")
        if _contains_any(lowered, ("what is primary", "what does primary", "what is secondary", "what is tertiary", "what is u5", "what does u5", "what is o5", "what does o5", "what is pw", "what does pw", "pregnant women")):
            return WhatsAppRouteDecision(WhatsAppRouteType.SIDE_HELP, responses.workflow_side_help_response(text), reason="workflow_side_help")
        return WhatsAppRouteDecision(WhatsAppRouteType.TPR_ACTIVE, analysis_message=text, reason="workflow_delegate")

    if _contains_any(lowered, ("who are you", "what are you", "what is chatmrpt")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.identity_response(), reason="identity")

    if _contains_any(lowered, ("what can you do", "capabilities", "how can you help", "what do you do")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.capabilities_response(has_ready_upload), reason="capabilities")

    if _contains_any(lowered, ("what data", "which data", "file format", "columns", "how do i upload", "how to upload", "upload")) and not _contains_any(lowered, ("uploaded", "my data")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.upload_guidance_response(), reason="upload_guidance")

    if _contains_any(lowered, ("what is tpr", "what does tpr", "test positivity", "positivity rate", "explain tpr")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.tpr_definition_response(), reason="tpr_definition")

    if _contains_any(lowered, ("what is malaria burden", "explain malaria burden", "burden mean")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.burden_definition_response(), reason="burden_definition")

    if _contains_any(lowered, ("what is malaria", "explain malaria", "malaria transmission", "malaria surveillance")) and not _contains_any(lowered, ("my data", "my dataset", "map", "risk", "burden")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.malaria_general_response(), reason="malaria_general")

    if _contains_any(lowered, ("what is risk mapping", "explain risk mapping", "what is malaria risk mapping")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.risk_mapping_response(), reason="risk_mapping_definition")

    if _contains_any(lowered, ("what is itn", "explain itn", "itn planning")) and not _contains_any(lowered, ("run", "plan my", "generate")):
        return WhatsAppRouteDecision(WhatsAppRouteType.NO_DATA_EDUCATION, responses.itn_planning_response(), reason="itn_definition")

    tpr_start = _contains_any(lowered, ("start tpr", "start the tpr", "tpr workflow", "run tpr", "calculate tpr"))
    if tpr_start:
        if has_ready_upload:
            return WhatsAppRouteDecision(WhatsAppRouteType.TPR_START, analysis_message=text, reason="tpr_start")
        return WhatsAppRouteDecision(WhatsAppRouteType.UPLOAD_NEEDED, responses.upload_required_response("TPR analysis"), reason="tpr_needs_upload")

    dataset_action = _contains_any(lowered, (
        "map", "risk analysis", "summarize my data", "summary of my data", "rank", "ranking",
        "analyze my data", "analyse my data", "burden distribution", "malaria burden distribution",
        "itn plan", "plan itn", "vulnerability", "show my data", "my dataset",
    ))
    if dataset_action:
        if has_ready_upload:
            return WhatsAppRouteDecision(WhatsAppRouteType.DATA_QUESTION, analysis_message=text, reason="data_action")
        task = "that analysis"
        if "map" in lowered:
            task = "mapping"
        elif "risk" in lowered:
            task = "risk analysis"
        elif "burden" in lowered:
            task = "malaria burden analysis"
        return WhatsAppRouteDecision(WhatsAppRouteType.UPLOAD_NEEDED, responses.upload_required_response(task), reason="data_action_needs_upload")

    if has_ready_upload:
        return WhatsAppRouteDecision(WhatsAppRouteType.DATA_QUESTION, analysis_message=text, reason="uploaded_unknown_to_analysis")

    return WhatsAppRouteDecision(WhatsAppRouteType.UNSUPPORTED, responses.unsupported_response(False), reason="no_upload_unknown")
