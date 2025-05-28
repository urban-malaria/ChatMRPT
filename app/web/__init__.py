"""
Web interface blueprints for ChatMRPT application.

This module contains blueprints for user-facing web routes including
the main interface, admin panels, and authentication.
"""

from .main import main_bp
from .admin import admin_bp

__all__ = ['main_bp', 'admin_bp'] 