# LGA Boundary Overlays & Enhanced Hover Fix Plan

**Branch:** `bernard`
**Date:** January 23, 2026

## Problem Summary

When hovering over wards on map visualizations:
1. **BUG:** Ward name shows LGA name instead (e.g., all wards in Mashegu LGA show "Mashegu")
2. **MISSING:** No clear display of which LGA the ward belongs to
3. **MISSING:** No LGA highlighting effect when hovering

## Root Cause

In `app/tools/variable_distribution.py` line 432:
```python
name = row.get('LGAName') or row.get('WardName') or row.get('ward_name') or str(idx)
```
This picks `LGAName` FIRST, so ward names are never displayed.

---

## Desired User Experience

### Hover Tooltip Should Show:
```
┌─────────────────────────────┐
│ Kutriko                     │  ← Ward name (bold)
│ LGA: Mashegu                │  ← Which LGA this ward belongs to
│                             │
│ Ward TPR: 75.2%             │  ← Ward's value
│ LGA Avg: 63.7% (+11.5%)     │  ← LGA average with comparison
│                             │
│ Tested: 1,234               │  ← Additional context (optional)
│ Positive: 928               │
└─────────────────────────────┘
```

### Visual Highlighting:
- When hovering on any ward, ALL wards in the same LGA become more prominent
- Other LGAs fade to ~40% opacity
- LGA boundary lines become thicker/highlighted for the active LGA

---

## Implementation Plan

### Phase 1: Fix the Bug (CRITICAL) ✅ DONE
**File:** `app/tools/variable_distribution.py`

- [x] Line 432: Change name extraction order
  ```python
  # FROM:
  name = row.get('LGAName') or row.get('WardName') or row.get('ward_name') or str(idx)

  # TO:
  name = row.get('WardName') or row.get('ward_name') or str(idx)
  ```

- [x] Update `build_hover_text` function to include LGA name as separate line:
  ```python
  def build_hover_text(df):
      texts = []
      for idx, row in df.iterrows():
          ward_name = row.get('WardName') or row.get('ward_name') or str(idx)
          lga_name = row.get('LGAName') or row.get('lga_name') or 'Unknown'
          val = row.get(variable)
          lga_code = row.get('LGACode')

          # Build hover text
          text = f"<b>{ward_name}</b><br>"
          text += f"LGA: {lga_name}<br><br>"

          if pd.notna(val):
              text += f"<b>{variable}: {val:.2f}</b>"
              if lga_code and lga_code in lga_averages:
                  lga_avg = lga_averages[lga_code]
                  diff = val - lga_avg
                  diff_sign = '+' if diff > 0 else ''
                  text += f"<br>LGA Avg: {lga_avg:.2f} ({diff_sign}{diff:.2f})"
          else:
              text += f"{variable}: N/A"

          texts.append(text)
      return texts
  ```

### Phase 2: Add LGA Highlighting (Enhancement) ✅ DONE
**File:** `app/tools/variable_distribution.py`

- [x] Store LGA code in customdata for JavaScript access
- [x] Add JavaScript callback for hover events that:
  - On hover: Increase border width for same-LGA wards
  - On unhover: Reset all border widths to default
- [x] Inject JavaScript into the HTML output

### Phase 3: Consistency Check ✅ DONE
Updated all visualization files with:
- Ward name (bold) + LGA name on all hovers
- LGA boundary overlays where missing
- LGA hover highlighting (border width on same-LGA wards)

- [x] `app/services/agents/visualizations/composite_visualizations.py` - Updated hover + highlighting
- [x] `app/services/agents/visualizations/tpr_visualization_service.py` - Already OK
- [x] `app/services/agents/visualizations/pca_visualizations.py` - Updated hover + boundary + highlighting
- [x] `app/data_analysis_v3/tools/tpr_analysis_tool.py` - Updated hover format
- [x] `app/core/itn_pipeline.py` - Updated hover with LGA + boundary + highlighting

### Phase 4: Testing (USER TO VERIFY)
- [ ] Test variable distribution map (TPR Risk Distribution)
- [ ] Test vulnerability maps
- [ ] Test PCA maps
- [ ] Test ITN distribution maps
- [ ] Verify hover shows correct ward names (not LGA names)
- [ ] Verify LGA name displays as separate line
- [ ] Verify LGA average comparison displays correctly (for TPR maps)
- [ ] Test LGA highlighting effect works (border width on hover)

### Phase 5: Cleanup ✅ DONE
- [x] Delete duplicate file: `app/data_analysis_v3/tpr_analysis_tool.py` (keep only `tools/` version)
- [x] Commit and deploy to AWS

---

## Files to Modify

| File | Change |
|------|--------|
| `app/tools/variable_distribution.py` | Fix name extraction, enhance hover text, add LGA highlighting |
| `app/data_analysis_v3/tools/tpr_analysis_tool.py` | Verify/update hover format consistency |

## Files to Delete
| File | Reason |
|------|--------|
| `app/data_analysis_v3/tpr_analysis_tool.py` | Duplicate - `tools/` version is used |

---

## LGA Highlighting JavaScript (Phase 2)

```javascript
// Inject into Plotly HTML output
document.addEventListener('DOMContentLoaded', function() {
    var plot = document.querySelector('.plotly-graph-div');
    if (!plot) return;

    plot.on('plotly_hover', function(data) {
        var lgaCode = data.points[0].customdata;
        // Reduce opacity of wards not in this LGA
        Plotly.restyle(plot, {'marker.opacity': 0.4}, [0]);
        // Keep hovered LGA at full opacity (requires trace filtering)
    });

    plot.on('plotly_unhover', function() {
        Plotly.restyle(plot, {'marker.opacity': 0.8}, [0]);
    });
});
```

---

## Verification Checklist

After implementation, verify:
- [ ] Hovering over ward shows WARD name (not LGA name)
- [ ] Hovering shows "LGA: [name]" as separate line
- [ ] LGA average displays with comparison to ward value (TPR maps only)
- [ ] All wards in same LGA highlight together (increased border width)
- [ ] Works on TPR, vulnerability, PCA, and ITN maps
