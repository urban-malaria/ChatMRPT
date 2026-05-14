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
                        'title': html_filename.replace('_', ' ').replace('.html', '').title(),
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
