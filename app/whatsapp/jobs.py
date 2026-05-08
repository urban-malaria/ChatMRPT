"""RQ job entrypoints for WhatsApp uploads and analysis."""

from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from twilio.rest import Client as TwilioClient
from werkzeug.utils import secure_filename

from app.whatsapp.formatter import format_error, format_upload_ack
from app.whatsapp.job_state import finish_job, mark_job_processing
from app.whatsapp.observability import log_event
from app.whatsapp.session import WhatsAppSessionManager

logger = logging.getLogger(__name__)

_ALLOWED_MEDIA_EXTENSIONS = {".csv", ".xlsx", ".xls"}
_CONTENT_TYPE_EXTENSIONS = {
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


def _create_app():
    from app import create_app

    config_name = os.getenv("FLASK_CONFIG") or os.getenv("APP_CONFIG") or None
    return create_app(config_name)


def _extension_from_content_type(content_type: str) -> str:
    return _CONTENT_TYPE_EXTENSIONS.get((content_type or "").split(";", 1)[0].strip().lower(), "")


def _get_session_manager(app) -> WhatsAppSessionManager:
    redis_client = app.config.get("SESSION_REDIS")
    if not redis_client:
        raise RuntimeError("WhatsApp jobs require Redis-backed Flask sessions")
    redis_client.ping()
    return WhatsAppSessionManager(redis_client)


@contextmanager
def _sender_lock(redis_client, sender: str):
    if not hasattr(redis_client, "lock"):
        yield
        return

    timeout = int(os.getenv("CHATMRPT_WHATSAPP_SENDER_LOCK_TTL", "3600"))
    blocking_timeout = int(os.getenv("CHATMRPT_WHATSAPP_SENDER_LOCK_WAIT", "1800"))
    lock = redis_client.lock(f"wa_sender_lock:{sender}", timeout=timeout, blocking_timeout=blocking_timeout)
    acquired = lock.acquire(blocking=True)
    if not acquired:
        raise RuntimeError(f"Timed out waiting for WhatsApp sender lock: {sender}")
    try:
        yield
    finally:
        try:
            lock.release()
        except Exception:
            logger.warning("Could not release WhatsApp sender lock for %s", sender, exc_info=True)


def _send_messages(to: str, messages: list[str], app) -> bool:
    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    from_number = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

    if not account_sid or not auth_token:
        logger.error("Twilio credentials not configured; cannot send WhatsApp reply")
        return False

    client = TwilioClient(account_sid, auth_token)
    success = True
    with app.app_context():
        for msg in messages:
            try:
                client.messages.create(body=msg, from_=from_number, to=to)
                log_event("twilio_send_success", sender=to)
            except Exception:
                logger.exception("Failed to send WhatsApp message to %s", to)
                log_event("twilio_send_failure", logging.ERROR, sender=to)
                success = False
    return success


def _build_upload_success_messages(upload_result) -> list[str]:
    lines = [
        f'File "{upload_result.original_filename}" uploaded successfully.',
        f"Dataset: {upload_result.rows} rows x {upload_result.cols} columns.",
        f"Type detected: {upload_result.detected_type}.",
    ]
    if upload_result.key_columns:
        lines.append("Columns: " + ", ".join(upload_result.key_columns[:5]))

    next_steps = (
        "Your data is ready. You can now ask summary questions, start the TPR workflow, "
        "or request maps and analysis."
    )
    return ["\n".join(lines), next_steps]


def process_whatsapp_upload_job(
    *,
    sender: str,
    message_sid: str,
    media_url: str,
    filename: str,
) -> None:
    app = _create_app()
    mgr = _get_session_manager(app)
    session_id = None
    mark_job_processing(mgr.redis, message_sid)
    log_event("upload_job_started", sender=sender, message_sid=message_sid, filename=filename)

    try:
        with app.app_context():
            from app.services.analysis_upload_service import process_analysis_upload
            from app.services.instance_sync import sync_session_after_upload
            from app.whatsapp.media import download_twilio_media

            with _sender_lock(mgr.redis, sender):
                _send_messages(sender, [format_upload_ack(filename)], app)

                try:
                    file_bytes, downloaded_content_type = download_twilio_media(media_url)
                    log_event("media_download_success", sender=sender, message_sid=message_sid, content_type=downloaded_content_type)
                except Exception as exc:
                    log_event("media_download_failure", logging.ERROR, sender=sender, message_sid=message_sid, error=exc)
                    raise

                content_ext = _extension_from_content_type(downloaded_content_type)
                filename_ext = Path(filename).suffix.lower()
                if filename_ext not in _ALLOWED_MEDIA_EXTENSIONS and content_ext not in _ALLOWED_MEDIA_EXTENSIONS:
                    raise ValueError("Downloaded media is not a supported CSV or Excel file")
                if filename_ext not in _ALLOWED_MEDIA_EXTENSIONS and content_ext:
                    filename = secure_filename(f"{Path(filename).stem}{content_ext}")

                session_id = str(uuid.uuid4())
                upload_root = app.config.get("UPLOAD_FOLDER", "instance/uploads")

                upload_result = process_analysis_upload(
                    session_id=session_id,
                    file_obj=file_bytes,
                    original_filename=filename,
                    upload_root=upload_root,
                )
                log_event("upload_process_success", sender=sender, message_sid=message_sid, session_id=session_id)

                try:
                    sync_session_after_upload(session_id)
                except Exception:
                    log_event("instance_sync_failure", logging.ERROR, sender=sender, message_sid=message_sid, session_id=session_id)
                    raise

                mgr.set_session_id(sender, session_id)
                mgr.clear_history(sender)
                mgr.clear_upload_metadata(sender)
                mgr.set_upload_metadata(sender, {
                    "session_id": session_id,
                    "filename": upload_result.original_filename,
                    "rows": upload_result.rows,
                    "cols": upload_result.cols,
                    "detected_type": upload_result.detected_type,
                    "uploaded_at": time.time(),
                })

                messages = _build_upload_success_messages(upload_result)
                if not _send_messages(sender, messages, app):
                    raise RuntimeError("Failed to send WhatsApp upload summary")
                mgr.append_history(sender, "user", f"Uploaded file: {upload_result.original_filename}")
                mgr.append_history(sender, "assistant", "\n".join(messages))
                finish_job(mgr.redis, message_sid, "succeeded", session_id=session_id)
                log_event("upload_job_succeeded", sender=sender, message_sid=message_sid, session_id=session_id)

    except Exception as exc:
        logger.exception(
            "WhatsApp upload job failed: sid=%s sender=%s session=%s",
            message_sid,
            sender,
            session_id,
        )
        log_event("upload_job_failed", logging.ERROR, sender=sender, message_sid=message_sid, session_id=session_id, error=exc)
        try:
            _send_messages(sender, [format_error()], app)
        finally:
            finish_job(mgr.redis, message_sid, "failed", session_id=session_id, error=exc)
        raise


def process_whatsapp_analysis_job(
    *,
    sender: str,
    message_sid: str,
    body: str,
    session_id: str,
) -> None:
    app = _create_app()
    mgr = _get_session_manager(app)
    mark_job_processing(mgr.redis, message_sid, session_id=session_id)
    log_event("analysis_job_started", sender=sender, message_sid=message_sid, session_id=session_id)

    try:
        with app.app_context():
            from app.whatsapp.responder import run_whatsapp_analysis_and_respond

            with _sender_lock(mgr.redis, sender):
                latest_session_id = mgr.get_session_id(sender)
                if latest_session_id and latest_session_id != session_id:
                    session_id = latest_session_id
                    mark_job_processing(mgr.redis, message_sid, session_id=session_id)

                _send_messages(sender, ["Running analysis..."], app)
                response = run_whatsapp_analysis_and_respond(
                    user_message=body,
                    sender=sender,
                    session_id=session_id,
                    send_fn=_send_messages,
                    app=app,
                )

                assistant_message = response.get("message") or "Analysis complete."
                mgr.append_history(sender, "user", body)
                mgr.append_history(sender, "assistant", assistant_message)
                finish_job(mgr.redis, message_sid, "succeeded", session_id=session_id)
                log_event("analysis_job_succeeded", sender=sender, message_sid=message_sid, session_id=session_id)

    except Exception as exc:
        logger.exception(
            "WhatsApp analysis job failed: sid=%s sender=%s session=%s",
            message_sid,
            sender,
            session_id,
        )
        log_event("analysis_job_failed", logging.ERROR, sender=sender, message_sid=message_sid, session_id=session_id, error=exc)
        try:
            _send_messages(sender, [format_error()], app)
        finally:
            finish_job(mgr.redis, message_sid, "failed", session_id=session_id, error=exc)
        raise
