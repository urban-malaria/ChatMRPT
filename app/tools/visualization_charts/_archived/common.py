"""Shared helpers for visualization chart tools."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd
import plotly.graph_objects as go
from flask import current_app

logger = logging.getLogger(__name__)


def save_plotly_chart(fig: go.Figure, session_id: str, chart_name: str) -> Dict[str, str]:
    """Save a Plotly figure as HTML and return filesystem/web paths."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{chart_name}_{timestamp}.html"

    session_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_dir, exist_ok=True)

    file_path = os.path.join(session_dir, filename)
    fig.write_html(file_path)

    web_path = f"/serve_viz_file/{session_id}/{filename}"
    return {
        'file_path': file_path,
        'web_path': web_path,
        'filename': filename,
    }


def validate_numeric_column(df: pd.DataFrame, column: str) -> bool:
    """Return True when the column exists and contains numeric data."""
    if column not in df.columns:
        return False
    return pd.api.types.is_numeric_dtype(df[column])


def get_color_scheme(scheme: str) -> str:
    """Resolve a Plotly color scale name from user input."""
    valid_schemes = {
        'viridis': 'viridis',
        'plasma': 'plasma',
        'inferno': 'inferno',
        'blues': 'Blues',
        'reds': 'Reds',
        'greens': 'Greens',
        'plotly': 'plotly',
        'set1': 'Set1',
        'set2': 'Set2',
        'set3': 'Set3',
    }
    return valid_schemes.get((scheme or '').lower(), 'viridis')


__all__ = [
    'save_plotly_chart',
    'validate_numeric_column',
    'get_color_scheme',
]
