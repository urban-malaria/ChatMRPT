"""TPR pre-computation background service using Redis queue."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None

from app.tpr.precompute import (
    precompute_all_tpr_combinations,
    get_precompute_db_path,
)
from app.agent.encoding_handler import EncodingHandler

logger = logging.getLogger(__name__)

QUEUE_KEY = "chatmrpt:tpr_precompute_queue"
JOB_KEY_PREFIX = "chatmrpt:tpr_precompute_job:"
STATUS_FILENAME = "tpr_precompute_status.json"


def _get_redis_client() -> Optional["redis.StrictRedis"]:
    if redis is None:
        return None
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    password = os.environ.get("REDIS_PASSWORD")
    db = int(os.environ.get("REDIS_DB", "0"))
    try:
        client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        client.ping()
        return client
    except Exception as exc:
        logger.warning(f"TPR precompute Redis unavailable: {exc}")
        return None


def _status_path(session_id: str) -> Path:
    session_folder = Path("instance/uploads") / session_id
    session_folder.mkdir(parents=True, exist_ok=True)
    return session_folder / STATUS_FILENAME


def _read_status(session_id: str) -> Dict[str, Any]:
    path = _status_path(session_id)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _write_status(session_id: str, data: Dict[str, Any]) -> None:
    path = _status_path(session_id)
    data["updated_at"] = time.time()
    path.write_text(json.dumps(data, indent=2))


def get_precompute_status(session_id: str) -> Dict[str, Any]:
    status = _read_status(session_id)
    if not status:
        status = {
            "state": "missing",
            "session_id": session_id,
        }
    db_exists = os.path.exists(get_precompute_db_path(session_id))
    status["db_ready"] = db_exists and status.get("state") == "completed"
    status.setdefault("job_id", None)
    return status


def schedule_precompute(
    session_id: str,
    state: str,
    data_path: str,
    exclude_combination: Dict[str, str],
) -> Dict[str, Any]:
    """Run precomputation in a background thread.

    Previously this queued to Redis, but the consumer worker was never
    started so jobs sat in "queued" forever.  A daemon thread is simpler
    and works in both development and production without extra ops.
    """
    import threading

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "session_id": session_id,
        "state": state,
        "data_path": data_path,
        "exclude": exclude_combination,
        "enqueued_at": time.time(),
    }
    status = {
        "job_id": job_id,
        "session_id": session_id,
        "state": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "progress": [],
        "error": None,
        "job": {
            "state": state,
            "data_path": data_path,
            "exclude": exclude_combination,
        },
    }
    _write_status(session_id, status)

    # Run in a daemon thread so the HTTP response returns immediately
    # while precomputation continues in the background.
    thread = threading.Thread(
        target=_execute_job,
        args=(job,),
        daemon=True,
        name=f"tpr-precompute-{session_id[:8]}",
    )
    thread.start()
    logger.info("Started TPR precompute thread for job %s (session %s)", job_id, session_id)
    return {"mode": "thread", "job_id": job_id}


def retry_failed(session_id: str) -> Optional[Dict[str, Any]]:
    status = _read_status(session_id)
    if not status or status.get("state") != "error":
        return None
    job_info = status.get("job")
    if not job_info:
        return None
    return schedule_precompute(
        session_id=session_id,
        state=job_info.get("state", ""),
        data_path=job_info.get("data_path", ""),
        exclude_combination=job_info.get("exclude", {}),
    )


def consume_jobs(poll_interval: float = 2.0) -> None:
    """Continuously consume queued jobs."""
    client = _get_redis_client()
    if client is None:
        logger.error("Cannot start TPR precompute worker - Redis unavailable")
        return

    logger.info("TPR precompute worker started")
    while True:
        try:
            item = client.brpop(QUEUE_KEY, timeout=int(poll_interval))
            if not item:
                continue
            _, payload = item
            job = json.loads(payload)
            _execute_job(job)
        except KeyboardInterrupt:  # pragma: no cover
            logger.info("TPR precompute worker stopped")
            break
        except Exception as exc:
            logger.exception(f"Unexpected worker error: {exc}")
            time.sleep(poll_interval)


def _execute_job(job: Dict[str, Any]) -> None:
    session_id = job["session_id"]
    status = _read_status(session_id)
    status.update(
        {
            "state": "running",
            "started_at": time.time(),
            "progress": status.get("progress", []),
            "error": None,
        }
    )
    _write_status(session_id, status)

    def _progress_callback(facility: str, age: str, state: str, meta: Dict[str, Any]):
        progress = status.get("progress", [])
        progress.append(
            {
                "facility": facility,
                "age_group": age,
                "state": state,
                "meta": meta,
                "timestamp": time.time(),
            }
        )
        status["progress"] = progress
        _write_status(session_id, status)

    try:
        data_path = job.get("data_path")
        if not data_path or not os.path.exists(data_path):
            raise FileNotFoundError(f"Data path not found: {data_path}")

        df = EncodingHandler.read_csv_with_encoding(data_path)
        result = precompute_all_tpr_combinations(
            session_id=session_id,
            data=df,
            state=job.get("state", ""),
            exclude_combination=job.get("exclude"),
            progress_callback=_progress_callback,
        )
        status.update(
            {
                "state": "completed",
                "finished_at": time.time(),
                "result": result,
            }
        )
        _write_status(session_id, status)
        logger.info("TPR precompute completed for %s", session_id)
    except Exception as exc:
        status.update(
            {
                "state": "error",
                "finished_at": time.time(),
                "error": str(exc),
            }
        )
        _write_status(session_id, status)
        logger.error("TPR precompute failed for %s: %s", session_id, exc)
