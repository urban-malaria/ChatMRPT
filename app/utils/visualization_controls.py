"""HTML control injection for visualization files."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

CONTROL_START = "<!-- GEO CONTROLS START -->"
CONTROL_END = "<!-- GEO CONTROLS END -->"


def inject_geographic_controls(html_file_path: str, config: Dict) -> None:
    """Inject (or update) the geographic control overlay into an HTML file."""
    file_path = Path(html_file_path)
    if not file_path.exists():
        return
    html = file_path.read_text(encoding='utf-8')
    # Remove any previous block
    if CONTROL_START in html and CONTROL_END in html:
        start_idx = html.index(CONTROL_START)
        end_idx = html.index(CONTROL_END) + len(CONTROL_END)
        html = html[:start_idx] + html[end_idx:]
    controls_block = _build_controls_block(config)
    if '<body' not in html:
        html = controls_block + html
    else:
        html = html.replace('<body>', '<body>\n' + controls_block + '\n', 1)
    file_path.write_text(html, encoding='utf-8')


def _build_controls_block(config: Dict) -> str:
    config_json = json.dumps(config)
    return f"""
{CONTROL_START}
<style>
#geo-controls-container {{
  position: sticky;
  top: 0;
  z-index: 9999;
  background: rgba(255, 255, 255, 0.95);
  border-bottom: 1px solid #e5e7eb;
  padding: 12px 16px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}}
#geo-controls-container button {{
  cursor: pointer;
}}
#geo-controls-container .geo-level-group button {{
  border: 1px solid #d1d5db;
  background: #fff;
  padding: 4px 10px;
  font-size: 12px;
  border-radius: 4px;
  color: #374151;
}}
#geo-controls-container .geo-level-group button.active {{
  background: #2563eb;
  color: #fff;
  border-color: #2563eb;
}}
#geo-controls-container select {{
  min-width: 220px;
  padding: 4px;
}}
#geo-controls-container .geo-actions button {{
  padding: 6px 14px;
  font-size: 12px;
  border-radius: 4px;
  border: 1px solid #d1d5db;
  background: #fff;
  color: #111827;
}}
#geo-controls-container .geo-actions button.apply {{
  background: #2563eb;
  color: #fff;
  border-color: #2563eb;
}}
#geo-controls-loading {{
  display: none;
  font-size: 12px;
  color: #2563eb;
  margin-left: 12px;
}}
</style>
<div id="geo-controls-container">
  <div style="display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between;">
    <div style="display:flex; gap:10px; align-items:center;">
      <span style="font-size:13px; font-weight:600; color:#111827;">View Level:</span>
      <div class="geo-level-group">
        <button type="button" data-level="ward">Ward</button>
        <button type="button" data-level="lga">LGA</button>
      </div>
    </div>
    <div style="display:flex; gap:8px; align-items:center;">
      <label for="geo-lga-select" style="font-size:12px; color:#4b5563;">Focus LGA</label>
      <select id="geo-lga-select" multiple size="4"></select>
      <div class="geo-actions">
        <button type="button" class="apply">Apply</button>
        <button type="button" class="clear">Clear</button>
      </div>
      <span id="geo-controls-loading">Updating…</span>
    </div>
  </div>
</div>
<script>
(function() {{
  const config = {config_json};
  const container = document.getElementById('geo-controls-container');
  if (!container) return;
  const buttons = container.querySelectorAll('.geo-level-group button');
  const lgaSelect = container.querySelector('#geo-lga-select');
  const applyBtn = container.querySelector('.geo-actions .apply');
  const clearBtn = container.querySelector('.geo-actions .clear');
  const loadingEl = container.querySelector('#geo-controls-loading');
  const normalize = (value) => value == null ? '' : String(value).trim();

  function syncButtons(level) {{
    buttons.forEach(btn => {{
      if (btn.dataset.level === level) btn.classList.add('active');
      else btn.classList.remove('active');
    }});
    lgaSelect.disabled = level !== 'lga';
    lgaSelect.style.opacity = level === 'lga' ? '1' : '0.5';
  }}

  function populateOptions() {{
    lgaSelect.innerHTML = '';
    const selected = new Set((config.selected_lgas || []).map(normalize));
    (config.available_lgas || []).forEach(item => {{
      const option = document.createElement('option');
      option.value = normalize(item.code);
      option.textContent = item.label || option.value;
      if (selected.has(option.value)) option.selected = true;
      lgaSelect.appendChild(option);
    }});
  }}

  function getSelectedLGAs() {{
    return Array.from(lgaSelect.selectedOptions).map(opt => opt.value).filter(Boolean);
  }}

  function requestUpdate(level, lgas) {{
    loadingEl.style.display = 'inline';
    const payload = {{
      viz_type: config.viz_type,
      geographic_level: level,
      session_id: config.session_id,
      selected_lgas: lgas,
      viz_params: config.viz_params || {{}}
    }};
    fetch('/visualization/rerender', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      credentials: 'include',
      body: JSON.stringify(payload)
    }})
      .then(resp => resp.json())
      .then(data => {{
        if (data.status === 'success' && data.web_path) {{
          window.location.href = data.web_path;
        }} else {{
          alert(data.message || 'Unable to update visualization');
        }}
      }})
      .catch(err => alert(err.message || err))
      .finally(() => {{
        loadingEl.style.display = 'none';
      }});
  }}

  buttons.forEach(btn => {{
    btn.addEventListener('click', () => {{
      const level = btn.dataset.level;
      if (level === config.current_level) return;
      requestUpdate(level, level === 'lga' ? getSelectedLGAs() : []);
    }});
  }});

  applyBtn.addEventListener('click', () => {{
    if (config.current_level !== 'lga') requestUpdate('lga', getSelectedLGAs());
    else requestUpdate('lga', getSelectedLGAs());
  }});

  clearBtn.addEventListener('click', () => {{
    Array.from(lgaSelect.options).forEach(opt => (opt.selected = false));
    requestUpdate('lga', []);
  }});

  syncButtons(config.current_level || 'ward');
  populateOptions();
}})();
</script>
{CONTROL_END}
"""


HOVER_HIGHLIGHT_START = "<!-- LGA HOVER HIGHLIGHT START -->"
HOVER_HIGHLIGHT_END = "<!-- LGA HOVER HIGHLIGHT END -->"


def inject_lga_hover_highlight(html_file_path: str, lga_codes: list) -> None:
    """Inject JavaScript for LGA highlighting on hover.

    When user hovers over a ward, all wards in the same LGA highlight
    while other LGAs fade to lower opacity.
    """
    file_path = Path(html_file_path)
    if not file_path.exists():
        return

    html = file_path.read_text(encoding='utf-8')

    # Remove any previous hover highlight block
    if HOVER_HIGHLIGHT_START in html and HOVER_HIGHLIGHT_END in html:
        start_idx = html.index(HOVER_HIGHLIGHT_START)
        end_idx = html.index(HOVER_HIGHLIGHT_END) + len(HOVER_HIGHLIGHT_END)
        html = html[:start_idx] + html[end_idx:]

    # Build the hover highlight script
    lga_codes_json = json.dumps(lga_codes)
    hover_script = f'''
{HOVER_HIGHLIGHT_START}
<script>
(function() {{
  // LGA codes for each ward (indexed by location)
  const lgaCodes = {lga_codes_json};

  // Wait for Plotly to be ready
  function initHoverHighlight() {{
    const plotDiv = document.querySelector('.js-plotly-plot');
    if (!plotDiv || !plotDiv.data) {{
      setTimeout(initHoverHighlight, 100);
      return;
    }}

    // Store original opacities
    const originalOpacity = 0.75;
    const fadedOpacity = 0.25;
    const highlightOpacity = 0.9;

    // Find the main choropleth trace index
    let traceIndex = 0;
    for (let i = 0; i < plotDiv.data.length; i++) {{
      if (plotDiv.data[i].type === 'choroplethmapbox') {{
        traceIndex = i;
        break;
      }}
    }}

    plotDiv.on('plotly_hover', function(data) {{
      if (!data.points || !data.points[0]) return;

      const pointIndex = data.points[0].pointIndex;
      const hoveredLga = lgaCodes[pointIndex];
      if (!hoveredLga) return;

      // Build array of opacities - highlight same LGA, fade others
      const opacities = lgaCodes.map(code =>
        code === hoveredLga ? highlightOpacity : fadedOpacity
      );

      // Update trace with new opacities (using marker.opacity array doesn't work well,
      // so we update the overall opacity and rely on visual contrast)
      Plotly.restyle(plotDiv, {{
        'marker.line.width': lgaCodes.map(code => code === hoveredLga ? 2 : 0.5)
      }}, [traceIndex]);
    }});

    plotDiv.on('plotly_unhover', function() {{
      const plotDiv = document.querySelector('.js-plotly-plot');
      if (!plotDiv) return;

      // Reset to original line widths
      Plotly.restyle(plotDiv, {{
        'marker.line.width': 1
      }}, [traceIndex]);
    }});
  }}

  // Start initialization
  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', initHoverHighlight);
  }} else {{
    initHoverHighlight();
  }}
}})();
</script>
{HOVER_HIGHLIGHT_END}
'''

    # Inject before </body>
    if '</body>' in html:
        html = html.replace('</body>', hover_script + '\n</body>')
    else:
        html += hover_script

    file_path.write_text(html, encoding='utf-8')
