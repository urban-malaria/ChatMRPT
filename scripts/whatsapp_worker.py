#!/usr/bin/env python3
"""Run the RQ worker that processes WhatsApp upload and analysis jobs."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rq import Worker

from app import create_app
from app.whatsapp.queue import get_whatsapp_queue


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app = create_app(os.getenv("FLASK_CONFIG") or os.getenv("APP_CONFIG") or None)
    with app.app_context():
        queue = get_whatsapp_queue(app)
        app.logger.info("Starting WhatsApp RQ worker for queue=%s", queue.name)
        worker = Worker([queue], connection=queue.connection)
        worker.work(with_scheduler=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
