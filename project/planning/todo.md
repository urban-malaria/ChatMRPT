# Multi-Year Analysis Plan v2
*Written: 2026-04-14 — reviewed and corrected after independent code review*
*Updated: 2026-04-16 — Phase 7 (multi-year maps) moved in-scope*
*Review found 4 blockers + 3 gaps + 2 incorrect assumptions — all fixed in v2*
*Full plan: `project/planning/multi_year_analysis_plan.md`*

**Status: AWAITING APPROVAL**

---

## Summary
- 11+ files modified, 2 files created (Phase 7 adds 4-5 more map files)
- 27 implementation tasks across 7 phases
- 12 unit tests
- Backward compatible — single-year data (Adamawa) unchanged
- Key design: `year_tag=''` default everywhere — all existing callers unaffected
- Phase 7: multi-year map requests auto-generate all year maps + comparison grid; zero frontend changes (existing carousel handles pagination)

## Files Modified
1. `app/tpr/utils.py` — add `add_burden_to_timeseries()`
2. `app/services/data_handler.py` — add `load_raw_data(year_tag='')`
3. `app/services/dataset_builder.py` — `year_tag` across all 7 hardcoded paths
4. `app/analysis/pipeline.py` — `year_tag` param + 4 paths + line 395 call
5. `app/analysis/pca_pipeline.py` — `year_tag` to `__init__`, line 770, 4 paths
6. `app/analysis/engine.py` — `year_tag` param to both functions
7. `app/tpr/workflow_manager.py` — multi-year detection + completion message
8. `app/agent/agent.py` — `_build_multi_year_context()`
9. `app/agent/tools/map_tools.py` — year-specific ITN routing
10. `app/tpr/analysis_tool.py` — comment only (no functional change)

## Files Created
11. `app/tpr/trend_analyzer.py` — pre-computes trend_summary.csv (slope, direction, delta per ward)
    Trend analysis is NOT fixed — agent handles open-ended questions dynamically
    via analyze_data tool against tpr_time_series.csv
12. `app/tpr/multi_year_service.py`

## Implementation Phases
- Phase 1: Foundation + tests (trend_analyzer, add_burden_to_timeseries)
- Phase 2: year_tag support across analysis stack + full test suite
- Phase 3: Background service
- Phase 4: Workflow wiring + local Kwara test
- Phase 5: Agent context + ITN routing
- Phase 6: Full integration + Adamawa regression
- Phase 7: Multi-year map generation (all viz types, comparison grid, zero frontend changes)

---

# ARCHIVED — LGA Boundary Overlays & Enhanced Hover Fix Plan

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
