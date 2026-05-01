"""
WhatsApp webhook — Phase 1 + Phase 2.

Receives Twilio POST requests, validates the signature, routes the
message through ChatMRPT's LLM, and returns a TwiML reply.

Phase 2 adds: file uploads (CSV/Excel), idempotency guard, async processing.
"""

import logging
import os
import threading
import uuid

from flask import Blueprint, Response, current_app, request
from twilio.request_validator import RequestValidator
from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse

from app.whatsapp.formatter import chunk_text, format_error, format_upload_ack, format_welcome
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
_idem_store = _InMemoryRedis()  # fallback idempotency store (dev only)

_IDEM_TTL = 120  # seconds — covers Twilio retry window


def _get_idem_store():
    redis_client = current_app.config.get('SESSION_REDIS')
    return redis_client if redis_client else _idem_store


def _get_session_manager() -> WhatsAppSessionManager:
    redis_client = current_app.config.get('SESSION_REDIS')
    if not redis_client:
        logger.warning('Redis not available — using in-memory session store (dev only)')
        return WhatsAppSessionManager(_dev_memory_store)
    return WhatsAppSessionManager(redis_client)


# --------------------------------------------------------------------------- #
#  Media handler
# --------------------------------------------------------------------------- #

def _handle_media_message(
    sender: str,
    filename: str,
    media_url: str,
    content_type: str,
    message_sid: str,
    mgr: WhatsAppSessionManager,
    app,
) -> None:
    """
    Send acknowledgment immediately, then process the upload in a background thread.
    Idempotency-guarded so Twilio retries don't double-process.
    """
    # Idempotency check
    idem = _get_idem_store()
    idem_key = f'wa_idem:{message_sid}'
    if idem.get(idem_key):
        logger.info(f'Duplicate media message {message_sid} — skipping')
        return
    idem.setex(idem_key, _IDEM_TTL, '1')

    # New upload → fresh session + clear history (prevents data bleed)
    new_session_id = str(uuid.uuid4())
    session_key = f'wa_session:{sender}'
    redis = mgr.redis
    redis.setex(session_key, 86400, new_session_id)
    redis.delete(f'wa_history:{sender}')
    redis.delete(f'wa_upload:{sender}')
    logger.info(f'New session for upload: {sender} → {new_session_id}')

    # Acknowledgment (synchronous REST call — fast, before thread)
    _send_messages(sender, [format_upload_ack(filename)], app)

    def _process():
        try:
            with app.app_context():
                from app.whatsapp.media import download_twilio_media
                from app.whatsapp.uploader import build_summary_messages, process_whatsapp_upload

                # Download
                file_bytes, detected_ct = download_twilio_media(media_url)
                effective_ct = detected_ct or content_type

                # Process
                result = process_whatsapp_upload(
                    file_bytes=file_bytes,
                    filename=filename,
                    content_type=effective_ct,
                    session_id=new_session_id,
                    app=app,
                )

                if result.get('error'):
                    _send_messages(sender, [f"⚠️ {result['error']}"], app)
                    return

                # Store upload metadata in session
                mgr.set_upload_metadata(sender, {
                    'filename': filename,
                    'rows': result['rows'],
                    'cols': result['cols'],
                    'detected_type': result['detected_type'],
                    'session_id': new_session_id,
                })

                # Append upload context to conversation history so LLM knows
                key_cols = ', '.join(result.get('key_columns', [])[:5])
                mgr.append_history(sender, 'system', (
                    f"User has uploaded {filename} ({result['rows']:,} rows, "
                    f"{result['cols']} columns). "
                    f"Dataset type: {result['detected_type']}. "
                    f"Key columns: {key_cols}. "
                    f"Session ID: {new_session_id}. "
                    f"Data is ready for analysis."
                ))

                # Send summary + next steps
                msgs = build_summary_messages(filename, result)
                _send_messages(sender, msgs, app)

        except Exception:
            logger.exception(f'Error processing WhatsApp upload from {sender}')
            _send_messages(sender, [format_error()], app)

    threading.Thread(target=_process, daemon=True).start()


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

    # --- File upload ---
    if num_media > 0:
        message_sid = request.form.get('MessageSid', '')
        media_url = request.form.get('MediaUrl0', '')
        media_ct = request.form.get('MediaContentType0', '')
        raw_filename = request.form.get('MediaFilename0', '').strip()

        # Fallback filename when Twilio doesn't provide one
        if not raw_filename:
            ext = {
                'text/csv': '.csv',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
                'application/vnd.ms-excel': '.xls',
            }.get(media_ct, '')
            raw_filename = f'upload{ext}' if ext else 'upload.bin'

        app = current_app._get_current_object()
        _handle_media_message(sender, raw_filename, media_url, media_ct, message_sid, mgr, app)
        return _twiml_empty()

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
