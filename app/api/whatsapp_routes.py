"""
WhatsApp webhook.

Receives Twilio POST requests, validates the signature, routes the
message through ChatMRPT, and returns a TwiML reply quickly.
"""

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

from flask import Blueprint, Response, current_app, request
from twilio.request_validator import RequestValidator
from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse
from werkzeug.utils import secure_filename

from app.whatsapp.formatter import chunk_text, format_error, format_upload_ack, format_welcome
from app.whatsapp.session import WhatsAppSessionManager

logger = logging.getLogger(__name__)

whatsapp_bp = Blueprint('whatsapp', __name__)

_RESET_COMMANDS = {'reset', 'restart', 'new chat', 'start over'}
_HELP_COMMANDS = {'help', 'hi', 'hello', 'start'}
_JOB_TTL = 24 * 60 * 60
_ALLOWED_MEDIA_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
_CONTENT_TYPE_EXTENSIONS = {
    'text/csv': '.csv',
    'application/csv': '.csv',
    'application/vnd.ms-excel': '.xls',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
}


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


def _send_messages(to: str, messages: list[str], app) -> bool:
    """Send one or more messages via Twilio REST API (runs in background thread)."""
    account_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')
    from_number = os.getenv('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886')

    if not account_sid or not auth_token:
        logger.error('Twilio credentials not configured — cannot send reply')
        return False

    client = TwilioClient(account_sid, auth_token)
    success = True
    with app.app_context():
        for msg in messages:
            try:
                client.messages.create(body=msg, from_=from_number, to=to)
            except Exception:
                logger.exception(f'Failed to send WhatsApp message to {to}')
                success = False
    return success


class _InMemoryRedis:
    """Minimal Redis-compatible in-memory store for local dev (no Redis needed)."""
    def __init__(self):
        self._store: dict = {}
        self._ttls: dict = {}

    def _purge_if_expired(self, key):
        expires_at = self._ttls.get(key)
        if expires_at is not None and expires_at <= time.time():
            self._store.pop(key, None)
            self._ttls.pop(key, None)

    def get(self, key):
        self._purge_if_expired(key)
        return self._store.get(key)

    def setex(self, key, ttl, value):
        self._store[key] = value
        self._ttls[key] = time.time() + ttl

    def set(self, key, value, ex=None, nx=False):
        self._purge_if_expired(key)
        if nx and key in self._store:
            return False
        self._store[key] = value
        if ex is not None:
            self._ttls[key] = time.time() + ex
        else:
            self._ttls.pop(key, None)
        return True

    def expire(self, key, ttl):
        self._ttls[key] = time.time() + ttl

    def delete(self, *keys):
        for k in keys:
            self._store.pop(k, None)
            self._ttls.pop(k, None)


_dev_memory_store = _InMemoryRedis()


def _get_session_manager() -> WhatsAppSessionManager:
    redis_client = current_app.config.get('SESSION_REDIS')
    if not redis_client:
        logger.warning('Redis not available — using in-memory session store (dev only)')
        return WhatsAppSessionManager(_dev_memory_store)
    return WhatsAppSessionManager(redis_client)


def _safe_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning("Malformed Twilio NumMedia value: %r", value)
        return default


def _decode_redis_value(raw):
    if isinstance(raw, bytes):
        return raw.decode('utf-8')
    return raw


def _job_key(message_sid: str) -> str:
    return f'wa_job:{message_sid}'


def _get_job(redis_client, message_sid: str) -> dict | None:
    raw = redis_client.get(_job_key(message_sid))
    if not raw:
        return None
    try:
        return json.loads(_decode_redis_value(raw))
    except Exception:
        return None


def _set_job(redis_client, message_sid: str, payload: dict, *, nx=False) -> bool:
    value = json.dumps(payload, default=str)
    if hasattr(redis_client, 'set'):
        return bool(redis_client.set(_job_key(message_sid), value, ex=_JOB_TTL, nx=nx))
    if nx and redis_client.get(_job_key(message_sid)):
        return False
    redis_client.setex(_job_key(message_sid), _JOB_TTL, value)
    return True


def _claim_job(redis_client, message_sid: str, sender: str, job_type: str) -> bool:
    if not message_sid:
        return False

    existing = _get_job(redis_client, message_sid)
    if existing and existing.get('status') in {'processing', 'succeeded'}:
        logger.info(
            "Ignoring duplicate WhatsApp webhook %s with status=%s",
            message_sid,
            existing.get('status'),
        )
        return False

    retry_count = int((existing or {}).get('retry_count') or 0)
    if existing and existing.get('status') == 'failed':
        retry_count += 1

    payload = {
        'status': 'processing',
        'type': job_type,
        'sender': sender,
        'message_sid': message_sid,
        'retry_count': retry_count,
        'started_at': time.time(),
        'finished_at': None,
        'session_id': existing.get('session_id') if existing else None,
        'error': None,
    }
    return _set_job(redis_client, message_sid, payload, nx=not existing)


def _finish_job(redis_client, message_sid: str, status: str, *, session_id=None, error=None) -> None:
    payload = _get_job(redis_client, message_sid) or {'message_sid': message_sid}
    payload.update({
        'status': status,
        'finished_at': time.time(),
        'session_id': session_id or payload.get('session_id'),
        'error': str(error) if error else None,
    })
    _set_job(redis_client, message_sid, payload, nx=False)


def _extension_from_content_type(content_type: str) -> str:
    return _CONTENT_TYPE_EXTENSIONS.get((content_type or '').split(';', 1)[0].strip().lower(), '')


def _filename_from_media(form, message_sid: str) -> tuple[str | None, str | None]:
    media_url = (form.get('MediaUrl0') or '').strip()
    content_type = (form.get('MediaContentType0') or '').split(';', 1)[0].strip().lower()
    raw_filename = (
        form.get('MediaFilename0')
        or form.get('MediaFilename')
        or Path(unquote(urlparse(media_url).path)).name
        or ''
    )

    safe_name = secure_filename(raw_filename)
    ext = Path(safe_name).suffix.lower() if safe_name else ''
    content_ext = _extension_from_content_type(content_type)

    if ext in _ALLOWED_MEDIA_EXTENSIONS:
        return safe_name, None
    if content_ext in _ALLOWED_MEDIA_EXTENSIONS:
        stem = Path(safe_name).stem if safe_name else f'whatsapp_upload_{message_sid}'
        return secure_filename(f'{stem}{content_ext}'), None

    return None, 'Please send a CSV or Excel file (.csv, .xlsx, or .xls).'


def _build_upload_success_messages(upload_result) -> list[str]:
    lines = [
        f'File "{upload_result.original_filename}" uploaded successfully.',
        f'Dataset: {upload_result.rows} rows x {upload_result.cols} columns.',
        f'Type detected: {upload_result.detected_type}.',
    ]
    if upload_result.key_columns:
        lines.append('Columns: ' + ', '.join(upload_result.key_columns[:5]))

    next_steps = (
        'Your data is ready. You can now ask summary questions, start the TPR workflow, '
        'or request maps and analysis.'
    )
    return ['\n'.join(lines), next_steps]


def _process_upload_job(
    *,
    mgr: WhatsAppSessionManager,
    sender: str,
    message_sid: str,
    media_url: str,
    filename: str,
    app,
) -> None:
    session_id = None
    try:
        with app.app_context():
            from app.services.analysis_upload_service import process_analysis_upload
            from app.services.instance_sync import sync_session_after_upload
            from app.whatsapp.media import download_twilio_media

            _send_messages(sender, [format_upload_ack(filename)], app)

            file_bytes, downloaded_content_type = download_twilio_media(media_url)
            content_ext = _extension_from_content_type(downloaded_content_type)
            filename_ext = Path(filename).suffix.lower()
            if filename_ext not in _ALLOWED_MEDIA_EXTENSIONS and content_ext not in _ALLOWED_MEDIA_EXTENSIONS:
                raise ValueError('Downloaded media is not a supported CSV or Excel file')
            if filename_ext not in _ALLOWED_MEDIA_EXTENSIONS and content_ext:
                filename = secure_filename(f'{Path(filename).stem}{content_ext}')

            session_id = str(uuid.uuid4())
            upload_root = app.config.get('UPLOAD_FOLDER', 'instance/uploads')

            upload_result = process_analysis_upload(
                session_id=session_id,
                file_obj=file_bytes,
                original_filename=filename,
                upload_root=upload_root,
            )

            sync_session_after_upload(session_id)

            mgr.set_session_id(sender, session_id)
            mgr.clear_history(sender)
            mgr.clear_upload_metadata(sender)
            mgr.set_upload_metadata(sender, {
                'session_id': session_id,
                'filename': upload_result.original_filename,
                'rows': upload_result.rows,
                'cols': upload_result.cols,
                'detected_type': upload_result.detected_type,
                'uploaded_at': time.time(),
            })

            messages = _build_upload_success_messages(upload_result)
            if not _send_messages(sender, messages, app):
                raise RuntimeError('Failed to send WhatsApp upload summary')
            mgr.append_history(sender, 'user', f'Uploaded file: {upload_result.original_filename}')
            mgr.append_history(sender, 'assistant', '\n'.join(messages))
            _finish_job(mgr.redis, message_sid, 'succeeded', session_id=session_id)
            logger.info(
                'WhatsApp upload completed: sid=%s sender=%s session=%s filename=%s',
                message_sid,
                sender,
                session_id,
                upload_result.original_filename,
            )

    except Exception as exc:
        logger.exception(
            'WhatsApp upload job failed: sid=%s sender=%s session=%s',
            message_sid,
            sender,
            session_id,
        )
        try:
            _send_messages(sender, [format_error()], app)
        finally:
            _finish_job(mgr.redis, message_sid, 'failed', session_id=session_id, error=exc)


# --------------------------------------------------------------------------- #
#  Webhook
# --------------------------------------------------------------------------- #

@whatsapp_bp.route('/api/whatsapp/webhook', methods=['POST'])
def whatsapp_webhook():
    if not _validate_twilio(request):
        logger.warning('Invalid Twilio signature — request rejected')
        return Response('Forbidden', status=403)

    sender = request.form.get('From', '')   # e.g. whatsapp:+2348012345678
    message_sid = (request.form.get('MessageSid') or '').strip()
    body = request.form.get('Body', '').strip()
    num_media = _safe_int(request.form.get('NumMedia', 0), default=0)

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
        if not message_sid:
            logger.warning('WhatsApp media webhook missing MessageSid; ignoring')
            return _twiml_empty()

        if not _claim_job(mgr.redis, message_sid, sender, 'upload'):
            return _twiml_empty()

        media_url = (request.form.get('MediaUrl0') or '').strip()
        if not media_url:
            _finish_job(mgr.redis, message_sid, 'failed', error='Missing MediaUrl0')
            logger.warning('WhatsApp media webhook missing MediaUrl0: sid=%s sender=%s', message_sid, sender)
            return _twiml_empty()

        filename, validation_error = _filename_from_media(request.form, message_sid)
        if validation_error or not filename:
            _finish_job(mgr.redis, message_sid, 'failed', error=validation_error)
            resp = MessagingResponse()
            resp.message(validation_error or format_error())
            return Response(str(resp), mimetype='text/xml')

        app = current_app._get_current_object()
        thread = threading.Thread(
            target=_process_upload_job,
            kwargs={
                'mgr': mgr,
                'sender': sender,
                'message_sid': message_sid,
                'media_url': media_url,
                'filename': filename,
                'app': app,
            },
            daemon=True,
        )
        thread.start()
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
