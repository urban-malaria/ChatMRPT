"""RQ queue helpers for WhatsApp background work."""

from __future__ import annotations

import os
import time
import uuid
from typing import Any, Callable


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _is_production(app=None) -> bool:
    if os.getenv("FLASK_ENV", "").lower() == "production":
        return True
    if os.getenv("DEPLOYMENT_ENVIRONMENT", "").lower() == "production":
        return True
    return bool(app and not app.config.get("DEBUG") and not app.config.get("TESTING"))


def whatsapp_requires_redis(app=None) -> bool:
    return _env_flag("CHATMRPT_WHATSAPP_REQUIRE_REDIS", _is_production(app))


def whatsapp_requires_queue(app=None) -> bool:
    return _env_flag("CHATMRPT_WHATSAPP_REQUIRE_QUEUE", _is_production(app))


def get_redis_connection(app=None):
    redis_client = app.config.get("SESSION_REDIS") if app else None
    if redis_client:
        redis_client.ping()
        return redis_client

    import redis

    redis_client = redis.StrictRedis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        password=os.getenv("REDIS_PASSWORD"),
        db=int(os.getenv("REDIS_DB", "0")),
        socket_connect_timeout=int(os.getenv("REDIS_SOCKET_TIMEOUT", "5")),
        socket_timeout=int(os.getenv("REDIS_SOCKET_TIMEOUT", "5")),
        decode_responses=False,
    )
    redis_client.ping()
    return redis_client


def get_whatsapp_queue(app=None):
    from rq import Queue

    name = os.getenv("CHATMRPT_WHATSAPP_QUEUE_NAME", "whatsapp")
    return Queue(name=name, connection=get_redis_connection(app))


def make_whatsapp_job_id(message_sid: str, job_type: str) -> str:
    return f"wa_{job_type}_{message_sid}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def enqueue_whatsapp_job(
    func: Callable[..., Any],
    *,
    kwargs: dict[str, Any],
    message_sid: str,
    job_type: str,
    job_id: str | None = None,
    app=None,
):
    queue = get_whatsapp_queue(app)
    timeout = int(os.getenv("CHATMRPT_WHATSAPP_JOB_TIMEOUT", "3600"))
    result_ttl = int(os.getenv("CHATMRPT_WHATSAPP_RESULT_TTL", "86400"))
    failure_ttl = int(os.getenv("CHATMRPT_WHATSAPP_FAILURE_TTL", "86400"))
    job_id = job_id or make_whatsapp_job_id(message_sid, job_type)
    return queue.enqueue(
        func,
        kwargs=kwargs,
        job_id=job_id,
        job_timeout=timeout,
        result_ttl=result_ttl,
        failure_ttl=failure_ttl,
        meta={"message_sid": message_sid, "job_type": job_type},
    )
