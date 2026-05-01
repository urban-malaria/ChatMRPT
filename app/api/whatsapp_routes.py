"""
WhatsApp webhook — Phase 1: text Q&A.

Receives Twilio POST requests, validates the signature, routes the
message through ChatMRPT's LLM, and returns a TwiML reply.

Phase 2 (file uploads + maps as images) will extend this file.
"""

import logging
import os
import threading

from flask import Blueprint, Response, current_app, request
from twilio.request_validator import RequestValidator
from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse

from app.whatsapp.formatter import chunk_text, format_error, format_welcome
from app.whatsapp.session import WhatsAppSessionManager

logger = logging.getLogger(__name__)

whatsapp_bp = Blueprint('whatsapp', __name__)

_RESET_COMMANDS = {'reset', 'restart', 'new chat', 'start over'}
_HELP_COMMANDS = {'help', 'hi', 'hello', 'start'}


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

def _validate_twilio(req) -> bool:
    auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')
    if not auth_token:
        logger.warning('TWILIO_AUTH_TOKEN not set — skipping signature validation')
        return True
    validator = RequestValidator(auth_token)
    return validator.validate(
        req.url,
        req.form,
        req.headers.get('X-Twilio-Signature', ''),
    )


def _twiml_empty() -> Response:
    """Return an empty 200 TwiML response — used when reply is sent async."""
    return Response(str(MessagingResponse()), mimetype='text/xml')


def _send_messages(to: str, messages: list[str], app) -> None:
    """Send one or more messages via Twilio REST API (runs in background thread)."""
    account_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')
    from_number = os.getenv('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')

    if not account_sid or not auth_token:
        logger.error('Twilio credentials not configured — cannot send reply')
        return

    client = TwilioClient(account_sid, auth_token)
    with app.app_context():
        for msg in messages:
            try:
                client.messages.create(body=msg, from_=from_number, to=to)
            except Exception:
                logger.exception(f'Failed to send WhatsApp message to {to}')


class _InMemoryRedis:
    """Minimal Redis-compatible in-memory store for local dev (no Redis needed)."""
    def __init__(self):
        self._store: dict = {}
        self._ttls: dict = {}

    def get(self, key):
        return self._store.get(key)

    def setex(self, key, ttl, value):
        self._store[key] = value
        self._ttls[key] = ttl

    def expire(self, key, ttl):
        self._ttls[key] = ttl

    def delete(self, *keys):
        for k in keys:
            self._store.pop(k, None)


_dev_memory_store = _InMemoryRedis()


def _get_session_manager() -> WhatsAppSessionManager:
    redis_client = current_app.config.get('SESSION_REDIS')
    if not redis_client:
        logger.warning('Redis not available — using in-memory session store (dev only)')
        return WhatsAppSessionManager(_dev_memory_store)
    return WhatsAppSessionManager(redis_client)


# --------------------------------------------------------------------------- #
#  Webhook
# --------------------------------------------------------------------------- #

@whatsapp_bp.route('/api/whatsapp/webhook', methods=['POST'])
def whatsapp_webhook():
    if not _validate_twilio(request):
        logger.warning('Invalid Twilio signature — request rejected')
        return Response('Forbidden', status=403)

    sender = request.form.get('From', '')   # e.g. whatsapp:+2348012345678
    body = request.form.get('Body', '').strip()
    num_media = int(request.form.get('NumMedia', 0))

    if not sender:
        return _twiml_empty()

    logger.info(f'WhatsApp inbound from {sender}: media={num_media} text="{body[:60]}"')

    try:
        mgr = _get_session_manager()
    except RuntimeError as exc:
        logger.error(str(exc))
        # Respond synchronously — no Redis means we can't do async either
        resp = MessagingResponse()
        resp.message(format_error())
        return Response(str(resp), mimetype='text/xml')

    # --- Reset command ---
    if body.lower() in _RESET_COMMANDS:
        mgr.clear_session(sender)
        resp = MessagingResponse()
        resp.message("✅ Session reset. " + format_welcome())
        return Response(str(resp), mimetype='text/xml')

    # --- Welcome / help ---
    if body.lower() in _HELP_COMMANDS and not mgr.get_session_id(sender):
        mgr.get_or_create_session(sender)
        resp = MessagingResponse()
        resp.message(format_welcome())
        return Response(str(resp), mimetype='text/xml')

    # --- File upload (Phase 2 stub) ---
    if num_media > 0:
        resp = MessagingResponse()
        resp.message(
            "📂 File uploads are coming soon. For now, please use the "
            "ChatMRPT web app to upload your data files."
        )
        return Response(str(resp), mimetype='text/xml')

    # --- Text message → LLM (async so we return 200 immediately) ---
    session_id = mgr.get_or_create_session(sender)
    history = mgr.get_history(sender)

    app = current_app._get_current_object()

    def process_and_reply():
        try:
            with app.app_context():
                llm_manager = app.services.llm_manager
                reply_text = llm_manager.generate_response(
                    prompt=body,
                    context={'conversation_history': history} if history else None,
                    session_id=session_id,
                    max_tokens=1500,
                )
                reply_text = (reply_text or '').strip()

                mgr.append_history(sender, 'user', body)
                mgr.append_history(sender, 'assistant', reply_text)

                chunks = chunk_text(reply_text) if reply_text else [format_error()]
                _send_messages(sender, chunks, app)

        except Exception:
            logger.exception(f'Error processing WhatsApp message from {sender}')
            _send_messages(sender, [format_error()], app)

    thread = threading.Thread(target=process_and_reply, daemon=True)
    thread.start()

    # Return empty 200 immediately so Twilio doesn't retry
    return _twiml_empty()
