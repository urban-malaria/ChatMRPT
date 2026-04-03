"""Blueprint routes delegating to chat handlers."""

from __future__ import annotations

from app.auth.decorators import require_auth
from app.utils.decorators import handle_errors, log_execution_time, validate_session

from . import analysis_bp
from .chat_sync import handle_send_message
from .chat_stream import handle_send_message_streaming


@analysis_bp.route('/send_message', methods=['POST'])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def send_message():
    """Entrypoint for synchronous chat messages."""
    return handle_send_message()


@analysis_bp.route('/send_message_streaming', methods=['POST'])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def send_message_streaming():
    """Entrypoint for streaming chat responses."""
    return handle_send_message_streaming()
