"""Session utilities for authentication workflows."""
from __future__ import annotations

import logging
from typing import Optional

from flask import current_app, session
from flask_login import login_user


def _cleanup_previous_session(session_id: Optional[str]) -> None:
    """Remove cached artifacts associated with a previous anonymous session."""
    if not session_id:
        return

    logger = current_app.logger if current_app else logging.getLogger(__name__)

    # Clear data-service caches (uploads, analysis artifacts, etc.)
    try:
        services = getattr(current_app, 'services', None)
        data_service = getattr(services, 'data_service', None)
        if data_service:
            data_service.clear_session_data(session_id)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to clear data service cache for %s: %s", session_id, exc)


def establish_authenticated_session(user, token: str, method: str, *, remember: bool = False) -> None:
    """Reset Flask session state for a freshly authenticated user.

    Ensures any anonymous session artifacts are discarded and a new session will
    be initialised on the next page load (which creates an isolated session_id).
    """
    previous_session_id = session.get('session_id')

    session.clear()

    # Re-authenticate within the fresh session context
    login_user(user, remember=remember)

    session['user_id'] = user.id
    session['user_email'] = getattr(user, 'email', None)
    session['auth_token'] = token
    session['auth_method'] = method
    session.permanent = True
    session.modified = True

    _cleanup_previous_session(previous_session_id)
