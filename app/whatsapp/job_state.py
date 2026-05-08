"""Redis-backed WhatsApp job status helpers."""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)

JOB_TTL = 24 * 60 * 60


def _decode_redis_value(raw):
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return raw


def job_key(message_sid: str) -> str:
    return f"wa_job:{message_sid}"


def get_job(redis_client, message_sid: str) -> dict | None:
    raw = redis_client.get(job_key(message_sid))
    if not raw:
        return None
    try:
        return json.loads(_decode_redis_value(raw))
    except Exception:
        logger.warning("Could not decode WhatsApp job state: sid=%s", message_sid)
        return None


def set_job(redis_client, message_sid: str, payload: dict, *, nx: bool = False) -> bool:
    value = json.dumps(payload, default=str)
    if hasattr(redis_client, "set"):
        return bool(redis_client.set(job_key(message_sid), value, ex=JOB_TTL, nx=nx))
    if nx and redis_client.get(job_key(message_sid)):
        return False
    redis_client.setex(job_key(message_sid), JOB_TTL, value)
    return True


def claim_job(redis_client, message_sid: str, sender: str, job_type: str) -> bool:
    if not message_sid:
        return False

    existing = get_job(redis_client, message_sid)
    if existing and existing.get("status") in {"processing", "queued", "succeeded"}:
        logger.info(
            "Ignoring duplicate WhatsApp webhook %s with status=%s",
            message_sid,
            existing.get("status"),
        )
        return False

    retry_count = int((existing or {}).get("retry_count") or 0)
    if existing and existing.get("status") == "failed":
        retry_count += 1

    payload = {
        "status": "processing",
        "type": job_type,
        "sender": sender,
        "message_sid": message_sid,
        "retry_count": retry_count,
        "started_at": time.time(),
        "queued_at": None,
        "finished_at": None,
        "session_id": existing.get("session_id") if existing else None,
        "rq_job_id": None,
        "error": None,
    }
    return set_job(redis_client, message_sid, payload, nx=not existing)


def mark_job_queued(redis_client, message_sid: str, *, rq_job_id: str, session_id: str | None = None) -> None:
    payload = get_job(redis_client, message_sid) or {"message_sid": message_sid}
    payload.update({
        "status": "queued",
        "queued_at": time.time(),
        "session_id": session_id or payload.get("session_id"),
        "rq_job_id": rq_job_id,
        "error": None,
    })
    set_job(redis_client, message_sid, payload, nx=False)


def mark_job_processing(redis_client, message_sid: str, *, session_id: str | None = None) -> None:
    payload = get_job(redis_client, message_sid) or {"message_sid": message_sid}
    payload.update({
        "status": "processing",
        "started_at": payload.get("started_at") or time.time(),
        "session_id": session_id or payload.get("session_id"),
        "error": None,
    })
    set_job(redis_client, message_sid, payload, nx=False)


def finish_job(redis_client, message_sid: str, status: str, *, session_id=None, error=None) -> None:
    payload = get_job(redis_client, message_sid) or {"message_sid": message_sid}
    payload.update({
        "status": status,
        "finished_at": time.time(),
        "session_id": session_id or payload.get("session_id"),
        "error": str(error) if error else None,
    })
    set_job(redis_client, message_sid, payload, nx=False)
