"""Analysis chat routes package."""
import logging
from flask import Blueprint

logger = logging.getLogger(__name__)

analysis_bp = Blueprint("analysis_chat", __name__)

from .analysis_chat import *
