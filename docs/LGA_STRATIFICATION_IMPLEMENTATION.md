# LGA Stratification Implementation

## Overview

LGA (Local Government Area) stratification allows users to view visualizations at two geographic levels:
- **Ward level** (default): Shows data at the finest granularity
- **LGA level**: Aggregates ward-level data up to LGA boundaries

Users can also highlight/focus on specific LGAs within the visualization.

---

## How It Works

### 1. User Interface Controls

When a visualization is created, interactive controls are injected into the HTML:

**File**: `app/utils/visualization_controls.py`

```
┌─────────────────────────────────────────────────────────────┐
│  View Level: [Ward] [LGA]     Focus LGA: [dropdown] [Apply] │
└─────────────────────────────────────────────────────────────┘
```

- **Ward/LGA buttons**: Toggle geographic aggregation level
- **Focus LGA dropdown**: Multi-select to highlight specific LGAs
- **Apply button**: Re-renders the visualization with new settings

### 2. Data Flow

```
User clicks "LGA" button
        │
        ▼
Frontend calls POST /visualization/rerender
        │
        ▼
visualization_routes.py:rerender_visualization()
        │
        ▼
Tool.execute() with geographic_level='lga'
        │
        ▼
geospatial_levels.py:dissolve_to_lga()
        │
        ▼
New HTML file with aggregated data
        │
        ▼
Page redirects to new visualization
```

### 3. Key Files

| File | Purpose |
|------|---------|
| `app/utils/geospatial_levels.py` | Core aggregation logic |
| `app/utils/visualization_controls.py` | Injects HTML controls into visualizations |
| `app/utils/lga_boundaries.py` | Reference LGA boundary geometries |
| `app/tools/variable_distribution.py` | Example tool using LGA stratification |
| `app/web/routes/visualization_routes.py` | Handles `/visualization/rerender` endpoint |

---

## Core Implementation Details

### geospatial_levels.py

**Key functions:**

#### `dissolve_to_lga(gdf, value_columns, sum_columns, mean_columns)`
Aggregates ward-level GeoDataFrame to LGA polygons:

```python
def dissolve_to_lga(gdf, value_columns, sum_columns=None, mean_columns=None):
    # Find LGACode column
    code_col = _detect_column(gdf, LGA_CODE_CANDIDATES)

    # Build aggregation dictionary
    agg_dict = {}
    for col in value_columns:
        agg_dict[col] = 'mean'  # Default: average values
    for col in sum_columns or []:
        agg_dict[col] = 'sum'   # Sum for counts
    for col in mean_columns or []:
        agg_dict[col] = 'mean'  # Mean for rates

    # Dissolve geometries and aggregate values
    dissolved = gdf.dissolve(by=code_col, aggfunc=agg_dict)
    return dissolved
```

#### `apply_lga_highlight(gdf, selected_lgas, code_column)`
Adds `_is_selected_lga` boolean column for highlighting:

```python
def apply_lga_highlight(gdf, selected_lgas, code_column=None):
    codes = {normalize_lga_code(code) for code in selected_lgas}
    gdf['_is_selected_lga'] = gdf[code_column].apply(
        lambda value: normalize_lga_code(value) in codes
    )
    return gdf
```

#### `collect_lga_options(*frames)`
Collects unique LGAs for dropdown controls:

```python
def collect_lga_options(*frames):
    # Returns list of {code, label} for each unique LGA
    return [{"code": "NG001", "label": "Kano Municipal"}, ...]
```

---

### variable_distribution.py

**How it uses LGA stratification (lines 366-401):**

```python
plot_level = self.geographic_level  # 'ward' or 'lga'
plot_data = clean_data.copy()

if plot_level == 'lga':
    # Option 1: Use reference LGA geometries (preferred)
    aggregated = clean_data.groupby('LGACode').agg({
        variable: 'mean',
        'StateName': 'first',
        'LGAName': 'first',
    })
    reference_shapes = get_reference_lga_geometries(aggregated)
    plot_data = reference_shapes.merge(aggregated, on='LGACode')

    # Option 2: Fallback - dissolve ward geometries
    # If reference shapes unavailable
    plot_data = dissolve_to_lga(clean_data, value_columns=[variable])

# Apply highlighting for selected LGAs
plot_data = apply_lga_highlight(plot_data, selected_lgas, 'LGACode')
```

**Rendering with highlighting (lines 443-455):**

```python
if highlight_codes:
    # Render unselected LGAs with faded colors
    faded = plot_data[~plot_data['_is_selected_lga']]
    add_trace(faded, opacity=0.25, colorscale='gray')

    # Render selected LGAs with full colors
    highlighted = plot_data[plot_data['_is_selected_lga']]
    add_trace(highlighted, opacity=0.85, colorscale=color_scale)
else:
    # Render all areas normally
    add_trace(plot_data, opacity=0.75)
```

---

### visualization_controls.py

**Injects this HTML block into visualization files:**

```html
<div id="geo-controls-container">
  <div>
    <span>View Level:</span>
    <button data-level="ward">Ward</button>
    <button data-level="lga">LGA</button>
  </div>
  <div>
    <label>Focus LGA</label>
    <select id="geo-lga-select" multiple></select>
    <button class="apply">Apply</button>
    <button class="clear">Clear</button>
  </div>
</div>
```

**JavaScript handles re-rendering:**

```javascript
function requestUpdate(level, lgas) {
  fetch('/visualization/rerender', {
    method: 'POST',
    body: JSON.stringify({
      viz_type: config.viz_type,
      geographic_level: level,
      session_id: config.session_id,
      selected_lgas: lgas,
      viz_params: config.viz_params
    })
  })
  .then(resp => resp.json())
  .then(data => {
    if (data.web_path) {
      window.location.href = data.web_path;  // Navigate to new visualization
    }
  });
}
```

---

### visualization_routes.py

**`/visualization/rerender` endpoint (lines 349-380):**

```python
@viz_bp.route('/visualization/rerender', methods=['POST'])
def rerender_visualization():
    payload = request.get_json()
    viz_type = payload.get('viz_type')
    geographic_level = payload.get('geographic_level', 'ward')
    selected_lgas = payload.get('selected_lgas', [])
    session_id = session.get('session_id')

    if viz_type == 'variable_distribution':
        variable_name = payload['viz_params']['variable_name']
        tool = VariableDistribution(
            variable_name=variable_name,
            geographic_level=geographic_level,
            selected_lgas=selected_lgas,
        )
        result = tool.execute(session_id=session_id)
        return jsonify({'status': 'success', 'web_path': result.data['web_path']})
```

---

## Summary

| Step | What Happens |
|------|--------------|
| 1 | User creates a visualization (e.g., TPR distribution map) |
| 2 | Tool renders at ward level by default |
| 3 | Controls injected into HTML via `inject_geographic_controls()` |
| 4 | User clicks "LGA" button |
| 5 | JavaScript calls `/visualization/rerender` |
| 6 | Backend creates new tool instance with `geographic_level='lga'` |
| 7 | Tool uses `dissolve_to_lga()` to aggregate ward data |
| 8 | New HTML file generated with LGA-level map |
| 9 | Frontend redirects to new visualization |

The key insight is that **the same tool is re-executed** with different parameters - it's not a separate visualization, just a different aggregation of the same underlying data.
