"""
Shared tab HTML builder for multi-year choropleth maps.

Used by variable_distribution.py (Burden maps) and maps_tools.py
(vulnerability maps) to produce a consistent tabbed HTML output with
lazy Plotly rendering.

Tab keys can be integers (years: 2020, 2021, ...) or the string 'agg'
for the aggregate / All-Years panel. JS quoting is handled automatically.
"""

import os
import logging
from datetime import datetime
from typing import List, Tuple, Union

import plotly.graph_objects as go

logger = logging.getLogger(__name__)

# Type alias: (key, label, fig)
TabEntry = Tuple[Union[int, str], str, go.Figure]


def _js_key(key: Union[int, str]) -> str:
    """Return a JS-safe literal for use inside showYear() and _allFigs[]."""
    return f"'{key}'" if isinstance(key, str) else str(key)


def build_tabbed_html(
    tabs: List[TabEntry],
    nav_label: str,
    filename: str,
    session_id: str,
) -> dict:
    """Bundle a list of Plotly figures into a single tabbed HTML file.

    Args:
        tabs:       [(key, label, fig), ...] — first tab is the default active one.
                    key is an int (year) or 'agg' (aggregate).
        nav_label:  Text shown before the tab buttons, e.g. "BURDEN by year:".
        filename:   Output filename (saved in session upload folder).
        session_id: Used to resolve the upload directory.

    Returns:
        {'file_path': str, 'web_path': str}
    """
    try:
        from flask import current_app
        upload_folder = current_app.config['UPLOAD_FOLDER']
    except RuntimeError:
        upload_folder = os.path.join('instance', 'uploads')

    session_dir = os.path.join(upload_folder, session_id)
    os.makedirs(session_dir, exist_ok=True)
    file_path = os.path.join(session_dir, filename)

    nav_buttons = ''
    panels = ''
    fig_data_js = 'var _allFigs = {};\n'
    default_key = _js_key(tabs[0][0]) if tabs else "'agg'"

    for key, label, fig in tabs:
        jk = _js_key(key)
        fig_data_js += f'_allFigs[{jk}] = {fig.to_json()};\n'
        nav_buttons += (
            f'<button class="year-btn" id="btn-{key}" '
            f'onclick="showTab({jk})">{label}</button>\n'
        )
        panels += (
            f'<div class="year-panel" id="panel-{key}">'
            f'<div id="plotly-{key}" style="height:600px;width:100%;"></div>'
            f'</div>\n'
        )

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ margin: 0; font-family: sans-serif; }}
    .year-nav {{
      display: flex; gap: 8px; padding: 12px 16px;
      background: #f3f4f6; border-bottom: 1px solid #e5e7eb;
      flex-wrap: wrap; align-items: center;
    }}
    .nav-label {{ font-weight: 600; color: #374151; margin-right: 4px; }}
    .year-btn {{
      padding: 5px 14px; border: 1px solid #d1d5db; border-radius: 6px;
      cursor: pointer; background: white; font-size: 14px; color: #374151;
      transition: all 0.15s;
    }}
    .year-btn:hover {{ background: #eff6ff; border-color: #93c5fd; }}
    .year-btn.active {{
      background: #2563eb; color: white; border-color: #2563eb;
    }}
    .year-panel {{ display: none; }}
    .year-panel.active {{ display: block; }}
  </style>
</head>
<body>
  <div class="year-nav">
    <span class="nav-label">{nav_label}</span>
    {nav_buttons}
  </div>
  {panels}
  <script>
    {fig_data_js}
    var _rendered = {{}};

    function showTab(key) {{
      document.querySelectorAll('.year-btn').forEach(function(b) {{
        b.classList.remove('active');
      }});
      document.querySelectorAll('.year-panel').forEach(function(p) {{
        p.classList.remove('active');
      }});
      var btn = document.getElementById('btn-' + key);
      var panel = document.getElementById('panel-' + key);
      if (btn) btn.classList.add('active');
      if (panel) {{
        panel.classList.add('active');
        var plotDiv = document.getElementById('plotly-' + key);
        if (plotDiv) {{
          if (!_rendered[key]) {{
            var fd = _allFigs[key];
            Plotly.newPlot(plotDiv, fd.data, fd.layout, {{responsive: true}});
            _rendered[key] = true;
          }} else {{
            Plotly.Plots.resize(plotDiv);
          }}
        }}
      }}
    }}
    showTab({default_key});
  </script>
</body>
</html>"""

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html)

    logger.info(f"Saved tabbed HTML: {file_path}")
    return {
        'file_path': file_path,
        'web_path': f"/serve_viz_file/{session_id}/{filename}",
    }
