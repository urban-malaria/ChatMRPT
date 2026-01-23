#!/usr/bin/env python3
"""Command-line worker that consumes TPR precompute jobs."""

import logging
import os
import sys

# Ensure project root on path when executed directly
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from app.core.tpr_precompute_service import consume_jobs  # noqa: E402


if __name__ == "__main__":
    consume_jobs()
