"""Modular analysis routes blueprint."""

from __future__ import annotations

import logging
from flask import Blueprint


analysis_bp = Blueprint('analysis_chat', __name__)
logger = logging.getLogger(__name__)


# Import route modules so their handlers register with the blueprint.
from . import analysis_exec  # noqa: E402,F401
from . import analysis_chat  # noqa: E402,F401
from . import analysis_vote  # noqa: E402,F401


__all__ = [
    'analysis_bp',
    'logger',
]
