"""
Visualization processing utilities for the Data Analysis Agent.

Extracted from agent.py — converts output plots (pickle files, HTML files)
into visualization objects that can be served to the frontend.
"""

import os
import logging
import pickle
import shutil
import uuid
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


# File-name prefixes of saved maps -> titles shown to users.
_TITLES = (
    ('vulnerability_map_composite', 'Malaria risk by ward (composite score)'),
    ('vulnerability_map_pca', 'Malaria risk by ward (PCA)'),
    ('pca_vulnerability_map', 'Malaria risk by ward (PCA)'),
    ('vulnerability_map', 'Malaria risk by ward'),
    ('itn_distribution_map', 'Bed net (ITN) distribution plan'),
    ('urban_extent', 'Urban and rural wards'),
    ('composite_score_maps', 'Risk score maps by model'),
    ('composite_map_page', 'Risk score maps by model'),
    ('box_plot', 'Risk score ranges by ward'),
    ('tpr_distribution', 'Malaria burden by ward'),
    ('burden_distribution', 'Malaria burden by ward'),
    ('settlement', 'Settlement classification'),
)


def readable_title(filename: str) -> str:
    """A title for a saved map file, without timestamps or IDs.

    'Burden_multi_year_distribution_20261124_100241.html' -> 'Burden by ward, by year'.
    """
    import re
    stem = re.sub(r'\.html$', '', filename, flags=re.IGNORECASE)
    lowered = stem.lower()
    for prefix, title in _TITLES:
        if lowered.startswith(prefix) or f'_{prefix}' in lowered:
            return title + (', by year' if 'multi_year' in lowered else '')
    match = re.match(r'(.+?)_(multi_year_)?distribution', stem, flags=re.IGNORECASE)
    if match:
        variable = match.group(1).replace('_', ' ')
        variable = 'Malaria burden' if variable.lower() == 'burden' else variable[:1].upper() + variable[1:]
        return f"{variable} by ward" + (', by year' if match.group(2) else '')
    cleaned = re.sub(r'[_-]?(\d{8}[_-]?\d{6}|[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}|[0-9a-f]{8,})', '', stem, flags=re.IGNORECASE)
    cleaned = cleaned.replace('_', ' ').strip()
    return cleaned[:1].upper() + cleaned[1:] if cleaned else 'Map'


def process_visualizations(session_id: str, output_plots: List) -> List[Dict[str, Any]]:
    """Process output plots (pickle files) into visualization objects."""
    visualizations = []

    # Save visualizations into the session's visualizations folder
    # so they are served via /serve_viz_file/<session_id>/visualizations/<file>
    session_viz_dir = os.path.join(f"instance/uploads/{session_id}", "visualizations")
    os.makedirs(session_viz_dir, exist_ok=True)

    for plot_path in output_plots:
        if isinstance(plot_path, str) and os.path.exists(plot_path):
            try:
                # HTML files (from map tools) — already rendered, just serve
                if plot_path.endswith('.html'):
                    html_filename = os.path.basename(plot_path)
                    is_settlement = 'settlement' in plot_path.lower() or 'settlement' in html_filename.lower()
                    if is_settlement and not html_filename.lower().startswith('settlement'):
                        parent_name = os.path.basename(os.path.dirname(plot_path))
                        html_filename = f"settlement_{parent_name}_{html_filename}"
                    # If not already in the viz dir, copy it there
                    target_path = os.path.join(session_viz_dir, html_filename)
                    if os.path.abspath(plot_path) != os.path.abspath(target_path):
                        shutil.copy2(plot_path, target_path)
                    web_url = f"/serve_viz_file/{session_id}/visualizations/{html_filename}"
                    visualizations.append({
                        'type': 'iframe',
                        'url': web_url,
                        'title': readable_title(html_filename),
                        'height': 820 if is_settlement else 600
                    })
                    logger.info(f"Served HTML visualization: {plot_path} → {web_url}")
                    continue

                # Pickle files (from analyze_data Plotly captures) — convert to HTML
                with open(plot_path, 'rb') as f:
                    fig = pickle.load(f)

                # Generate unique HTML filename
                viz_id = str(uuid.uuid4())
                html_filename = f"data_analysis_{viz_id}.html"
                html_path = os.path.join(session_viz_dir, html_filename)

                # Save as HTML
                viz_html = fig.to_html(include_plotlyjs=True)
                with open(html_path, 'w') as html_file:
                    html_file.write(viz_html)

                # Create web-accessible URL via serve_viz_file route
                web_url = f"/serve_viz_file/{session_id}/visualizations/{html_filename}"

                # Extract title from the Plotly figure layout
                fig_title = 'Data Analysis Visualization'
                try:
                    if hasattr(fig, 'layout') and fig.layout.title and fig.layout.title.text:
                        fig_title = fig.layout.title.text
                except Exception:
                    pass

                visualizations.append({
                    'type': 'iframe',
                    'url': web_url,
                    'title': fig_title,
                    'height': 600
                })
                logger.info(f"Converted visualization: {plot_path} → {web_url}")
            except Exception as e:
                logger.error(f"Error processing visualization {plot_path}: {e}")

    return visualizations
