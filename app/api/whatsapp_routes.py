"""
WhatsApp webhook.

Receives Twilio POST requests, validates the signature, routes the
message through ChatMRPT, and returns a TwiML reply quickly.
"""

import logging
import os
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from flask import Blueprint, Response, current_app, request
from twilio.request_validator import RequestValidator
from twilio.twiml.messaging_response import MessagingResponse
from werkzeug.utils import secure_filename

from app.whatsapp.formatter import format_error, format_welcome
from app.whatsapp.job_state import claim_job, finish_job, mark_job_queued
from app.whatsapp.observability import log_event
from app.whatsapp.queue import enqueue_whatsapp_job, make_whatsapp_job_id, whatsapp_requires_redis
from app.whatsapp.session import WhatsAppSessionManager

logger = logging.getLogger(__name__)

whatsapp_bp = Blueprint('whatsapp', __name__)

_RESET_COMMANDS = {'reset', 'restart', 'new chat', 'start over'}
_HELP_COMMANDS = {'help', 'hi', 'hello', 'start'}
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
        if whatsapp_requires_redis(current_app):
            raise RuntimeError('WhatsApp requires Redis in this environment; SESSION_REDIS is not configured')
        logger.warning('Redis not available — using in-memory session store (dev only)')
        return WhatsAppSessionManager(_dev_memory_store)
    redis_client.ping()
    return WhatsAppSessionManager(redis_client)


def _safe_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning("Malformed Twilio NumMedia value: %r", value)
        return default


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
    log_event("webhook_received", sender=sender, message_sid=message_sid, num_media=num_media)

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

        if not claim_job(mgr.redis, message_sid, sender, 'upload'):
            return _twiml_empty()
        log_event("upload_received", sender=sender, message_sid=message_sid)

        media_url = (request.form.get('MediaUrl0') or '').strip()
        if not media_url:
            finish_job(mgr.redis, message_sid, 'failed', error='Missing MediaUrl0')
            logger.warning('WhatsApp media webhook missing MediaUrl0: sid=%s sender=%s', message_sid, sender)
            return _twiml_empty()

        filename, validation_error = _filename_from_media(request.form, message_sid)
        if validation_error or not filename:
            finish_job(mgr.redis, message_sid, 'failed', error=validation_error)
            resp = MessagingResponse()
            resp.message(validation_error or format_error())
            return Response(str(resp), mimetype='text/xml')

        app = current_app._get_current_object()
        try:
            from app.whatsapp.jobs import process_whatsapp_upload_job

            rq_job_id = make_whatsapp_job_id(message_sid, 'upload')
            mark_job_queued(mgr.redis, message_sid, rq_job_id=rq_job_id)
            job = enqueue_whatsapp_job(
                process_whatsapp_upload_job,
                kwargs={
                    'sender': sender,
                    'message_sid': message_sid,
                    'media_url': media_url,
                    'filename': filename,
                },
                message_sid=message_sid,
                job_type='upload',
                job_id=rq_job_id,
                app=app,
            )
            log_event("upload_job_enqueued", sender=sender, message_sid=message_sid, rq_job_id=job.id)
        except Exception as exc:
            logger.exception('Failed to enqueue WhatsApp upload job: sid=%s sender=%s', message_sid, sender)
            log_event("upload_enqueue_failure", logging.ERROR, sender=sender, message_sid=message_sid, error=exc)
            finish_job(mgr.redis, message_sid, 'failed', error=exc)
            resp = MessagingResponse()
            resp.message(format_error())
            return Response(str(resp), mimetype='text/xml')

        return _twiml_empty()

    # --- Text message -> shared Data Analysis V3 service ---
    if not message_sid:
        logger.warning('WhatsApp text webhook missing MessageSid; ignoring')
        return _twiml_empty()

    session_id = mgr.get_or_create_session(sender)
    if not claim_job(mgr.redis, message_sid, sender, 'analysis'):
        return _twiml_empty()

    app = current_app._get_current_object()
    try:
        from app.whatsapp.jobs import process_whatsapp_analysis_job

        rq_job_id = make_whatsapp_job_id(message_sid, 'analysis')
        mark_job_queued(mgr.redis, message_sid, rq_job_id=rq_job_id, session_id=session_id)
        job = enqueue_whatsapp_job(
            process_whatsapp_analysis_job,
            kwargs={
                'sender': sender,
                'message_sid': message_sid,
                'body': body,
                'session_id': session_id,
            },
            message_sid=message_sid,
            job_type='analysis',
            job_id=rq_job_id,
            app=app,
        )
        log_event("analysis_job_enqueued", sender=sender, message_sid=message_sid, session_id=session_id, rq_job_id=job.id)
    except Exception as exc:
        logger.exception('Failed to enqueue WhatsApp analysis job: sid=%s sender=%s', message_sid, sender)
        log_event("analysis_enqueue_failure", logging.ERROR, sender=sender, message_sid=message_sid, session_id=session_id, error=exc)
        finish_job(mgr.redis, message_sid, 'failed', session_id=session_id, error=exc)
        resp = MessagingResponse()
        resp.message(format_error())
        return Response(str(resp), mimetype='text/xml')

    # Return empty 200 immediately so Twilio doesn't retry
    return _twiml_empty()
