"""
Web interface blueprints for ChatMRPT application.

This module contains blueprints for user-facing web routes including
the main interface, admin panels, and all functional route modules.
"""

from .routes import main_bp
from .admin import admin_bp
from .routes import (
    core_bp, upload_bp, analysis_bp, viz_bp, 
    reports_bp, debug_bp, register_all_blueprints
)

__all__ = [
    'main_bp', 'admin_bp', 'core_bp', 'upload_bp', 
    'analysis_bp', 'viz_bp', 'reports_bp', 'debug_bp',
    'register_all_blueprints'
] 