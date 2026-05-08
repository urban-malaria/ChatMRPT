from __future__ import annotations

from app.whatsapp.routing import WhatsAppRouteType, classify_whatsapp_message


def test_pre_upload_tpr_question_is_direct_education():
    decision = classify_whatsapp_message(
        "what is TPR in malaria surveillance?",
        has_ready_upload=False,
    )

    assert decision.route_type == WhatsAppRouteType.NO_DATA_EDUCATION
    assert "Test Positivity Rate" in decision.reply


def test_pre_upload_map_request_requires_upload():
    decision = classify_whatsapp_message(
        "map malaria burden distribution",
        has_ready_upload=False,
    )

    assert decision.route_type == WhatsAppRouteType.UPLOAD_NEEDED
    assert "upload" in decision.reply.lower()


def test_post_upload_map_request_routes_to_data_analysis():
    decision = classify_whatsapp_message(
        "map malaria burden distribution",
        has_ready_upload=True,
    )

    assert decision.route_type == WhatsAppRouteType.DATA_QUESTION


def test_typo_greeting_is_welcome():
    decision = classify_whatsapp_message("heloo", has_ready_upload=False)

    assert decision.route_type == WhatsAppRouteType.WELCOME


def test_general_malaria_question_before_upload_returns_education():
    decision = classify_whatsapp_message("what is malaria surveillance?", has_ready_upload=False)

    assert decision.route_type == WhatsAppRouteType.NO_DATA_EDUCATION
    assert "surveillance" in decision.reply.lower()


def test_active_workflow_selection_delegates_to_tpr():
    decision = classify_whatsapp_message(
        "primary",
        has_ready_upload=True,
        workflow_active=True,
    )

    assert decision.route_type == WhatsAppRouteType.TPR_ACTIVE


def test_active_workflow_side_help_is_direct_reply():
    decision = classify_whatsapp_message(
        "what is primary?",
        has_ready_upload=True,
        workflow_active=True,
    )

    assert decision.route_type == WhatsAppRouteType.SIDE_HELP
    assert "Primary facilities" in decision.reply


def test_arena_command_is_explicit_only():
    plain = classify_whatsapp_message(
        "explain limitations of facility TPR",
        has_ready_upload=False,
    )
    arena = classify_whatsapp_message(
        "arena: explain limitations of facility TPR",
        has_ready_upload=False,
    )

    assert plain.route_type != WhatsAppRouteType.ARENA_COMMAND
    assert arena.route_type == WhatsAppRouteType.ARENA_COMMAND
    assert arena.arena_prompt == "explain limitations of facility TPR"


def test_upload_processing_takes_precedence():
    decision = classify_whatsapp_message(
        "map malaria burden",
        has_ready_upload=False,
        upload_processing=True,
    )

    assert decision.route_type == WhatsAppRouteType.UPLOAD_PROCESSING


def test_arena_cancel_has_dedicated_route():
    decision = classify_whatsapp_message(
        "cancel arena",
        has_ready_upload=True,
        arena_active=True,
    )

    assert decision.route_type == WhatsAppRouteType.ARENA_CANCEL
