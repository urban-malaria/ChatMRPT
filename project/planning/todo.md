# Phase 7 — Multi-Year Visualization (All Tools)
*Written: 2026-04-16*

**Status: AWAITING APPROVAL**

## Problem
Every visualization and planning tool reads hardcoded `unified_dataset.csv` or `raw_data.csv`.
Background analysis produces `unified_dataset_2020.csv` … `unified_dataset_2025.csv` and
`raw_data_2020.csv` … `raw_data_2025.csv` but no tool can target a specific year.

## Full tool inventory

| Tool | File | Reads | Action |
|------|------|-------|--------|
| `CreateVulnerabilityMap` | maps_tools.py | unified_dataset.csv | Add year_tag |
| `CreatePCAMap` | maps_tools.py | unified_dataset.csv | Add year_tag |
| `CreateUrbanExtentMap` | maps_tools.py | unified_dataset.csv | Add year_tag |
| `CreateCompositeScoreMaps` | maps_tools.py | unified_dataset.csv | Add year_tag |
| `ExportITNResults` | export_tools.py | unified_dataset.csv | Add year_tag |
| `VariableDistribution` | variable_distribution.py | raw_data.csv | Add year_tag |
| `UniversalVisualizationExplainer` | explainer.py | unified_dataset.csv + raw_data.csv | Low priority — context only, skip for now |
| `PlanITNDistribution` | itn_tools.py + map_tools.py | unified_dataset.csv | ✅ Already done via _load_year_specific_unified_dataset |

## Shared foundation (fix once, all tools benefit)

```
load_unified_dataset (dataset_builder.py)     ← add year_tag=''
  ↑ called by
get_session_unified_dataset (tool_base.py)    ← add year_tag=''
  ↑ called by
CreateVulnerabilityMap / CreatePCAMap /        ← add year_tag=''
CreateUrbanExtentMap / CreateCompositeScoreMaps (maps_tools.py)
ExportITNResults (export_tools.py)

VariableDistribution (variable_distribution.py) ← independent: raw_data{year_tag}.csv
```

Agent tools (map_tools.py) are the top of the chain — they accept `year: Optional[int]`
and derive `year_tag = f'_{year}' if year else ''`.

## Files Modified (6, no new files)

### 1. `app/services/dataset_builder.py`
`load_unified_dataset(session_id, require_geometry=False, year_tag='')` — parameterise
`unified_dataset{year_tag}.csv` and `unified_dataset{year_tag}.geoparquet`

### 2. `app/utils/tool_base.py`
`get_session_unified_dataset(session_id, require_geometry=False, year_tag='')` — pass through to `load_unified_dataset`

### 3. `app/visualization/maps_tools.py`
All 4 classes (`CreateVulnerabilityMap`, `CreatePCAMap`, `CreateUrbanExtentMap`, `CreateCompositeScoreMaps`):
- Add `year_tag: str = ''` field
- Fix hardcoded `unified_dataset.csv` path at line 91
- Pass `year_tag=self.year_tag` into every `get_session_unified_dataset` call (4 call sites)

### 4. `app/planning/export_tools.py`
`ExportITNResults` — add `year_tag: str = ''`, pass to `load_unified_dataset` call at line 173

### 5. `app/visualization/variable_distribution.py`
`VariableDistribution` — add `year_tag: str = ''`, parameterise `raw_data{year_tag}.csv`
at lines 88 and 215

### 6. `app/agent/tools/map_tools.py`
All 5 agent tool wrappers — add `year: Optional[int] = None`, derive year_tag, check
`multi_year_status.json` if year specified, pass year_tag into class constructors:
- `create_vulnerability_map`
- `create_composite_score_maps`
- `create_urban_extent_map`
- `plan_itn_distribution` — already has year param, verify export path too
- `create_variable_map` — passes year_tag to VariableDistribution

## Implementation Tasks
- [ ] Task A: `load_unified_dataset` — add year_tag, parameterise both file paths
- [ ] Task B: `get_session_unified_dataset` — add year_tag, pass through
- [ ] Task C: `CreateVulnerabilityMap` — year_tag field, fix hardcoded path, pass to get_session_unified_dataset
- [ ] Task D: `CreatePCAMap` — same pattern
- [ ] Task E: `CreateUrbanExtentMap` — same pattern
- [ ] Task F: `CreateCompositeScoreMaps` — same pattern
- [ ] Task G: `ExportITNResults` — year_tag field, pass to load_unified_dataset
- [ ] Task H: `VariableDistribution` — year_tag field, parameterise raw_data path (2 sites)
- [ ] Task I: `create_vulnerability_map` agent tool — year param, year_tag derivation, status check
- [ ] Task J: `create_composite_score_maps` agent tool — same
- [ ] Task K: `create_urban_extent_map` agent tool — same
- [ ] Task L: `create_variable_map` agent tool — year param → VariableDistribution(year_tag)
- [ ] Task M: End-to-end test — upload Kwara, run risk analysis, ask "show 2022 vulnerability map", "map TPR for 2020", "plan ITN for 2023"

## Constraints
- No new files
- All year_tag/year params default to `''`/`None` → single-year (Adamawa) unchanged
- If year's dataset not ready, return graceful message not crash
- `UniversalVisualizationExplainer` deferred — context/explanation tool, not blocking

---

# TPR Precompute Acceleration Plan
*Written: 2026-04-16*

**Status: AWAITING APPROVAL**

## Problem
`precompute_all_tpr_combinations` runs `calculate_ward_tpr` 16 times sequentially.
Each call independently:
1. Reads the full Nigeria shapefile off disk (`gpd.read_file` — ~1-2s per call)
2. Runs `zonal_stats` on population rasters for 193 wards — the real bottleneck

**Measured from terminal.txt (Kwara session 2026-04-16):**
- Total precompute: `14:30:04` → `14:35:29` = **325 seconds (5m 25s)**
- `all/u5` zonal_stats alone: 21s — `all/o5`: 25s — `all/pw`: 18s — `all/all_ages`: 11s
- Average per combination: ~20s, nearly all of it `zonal_stats`
- 15 combinations × ~20s ≈ 300s — this is 92% of total time

## Mathematical Insight
Population per ward depends only on **age_group** (u5/o5/pw/all_ages), NOT on facility level.
So `zonal_stats` only needs to run **4 times** (once per age group), not 15.
Shapefile only needs to be loaded **once**.
The 4 zonal_stats calls can run **in parallel** via `ThreadPoolExecutor`.
The initial computation (user's selected combination) already runs zonal_stats for one age group —
that result is passed into the precompute as a free cache seed, saving one more call.

Result: 15 independent expensive calls → 1 shapefile load + 3 parallel raster extractions (1 already done) + 15 trivial aggregations.
Estimated time: **325s → ~25-30s for precompute** (wall clock with parallelism).

## UX Change: Hold completion message until precompute is done
Currently the completion message lies — it says "I've pre-computed all combinations" while 325s of work is still pending.

**New behaviour:** The completion message is assembled in two parts:
1. Stream immediately: map, year table, analysis text
2. Hold the switching-combinations paragraph until precompute finishes (~25s)
3. When precompute completes, append and flush the final paragraph

Message wording changes from:
> *"I've pre-computed burden for all facility/age combinations in the background. You can switch at any time"*

To:
> *"All 16 combinations are ready. You can switch at any time:"*

No lie. No hedge. Everything is genuinely ready when the message appears.

## Files Modified (3 only, no new files)

### 1. `app/tpr/utils.py`
Add two optional parameters to `calculate_ward_tpr`:
- `state_gdf=None` — if provided, skip `load_state_shapefile`
- `pop_cache=None` — dict mapping `age_group → pd.Series` of population values;
  if provided, skip `extract_ward_population` / `zonal_stats`

Both params default to `None` → all existing callers unchanged, zero regression risk.

### 2. `app/tpr/precompute.py`
Modify `precompute_all_tpr_combinations` to:
1. Accept optional `state_gdf` and `pop_cache` seed from initial computation
2. Load shapefile once (or reuse seed) → `state_gdf`
3. Extract population for remaining age groups in parallel via `ThreadPoolExecutor` → complete `pop_cache`
4. Pass `state_gdf` + `pop_cache` into every `calculate_ward_tpr` call in the loop
5. Return when all 15 combinations are stored (no longer a fire-and-forget daemon)

### 3. `app/tpr/workflow_manager.py`
- Pass initial computation's `state_gdf` + population Series into `schedule_precompute` as cache seed
- Run precompute synchronously (blocking) before assembling final completion message
- Change completion message wording: remove "in the background", say "All combinations are ready"

## Implementation Tasks
- [ ] Task A: Add `state_gdf` + `pop_cache` optional params to `calculate_ward_tpr` in `utils.py`
- [ ] Task B: In `precompute.py` — accept `state_gdf` + `pop_cache` seed params
- [ ] Task C: In `precompute.py` — load shapefile once (or reuse seed)
- [ ] Task D: In `precompute.py` — extract remaining age group populations in parallel (`ThreadPoolExecutor`)
- [ ] Task E: In `precompute.py` — pass `state_gdf` + `pop_cache` into each `calculate_ward_tpr` call
- [ ] Task F: In `workflow_manager.py` — extract `state_gdf` + population from initial computation, pass to precompute
- [ ] Task G: In `workflow_manager.py` — run precompute blocking (not daemon thread), hold message until done
- [ ] Task H: In `workflow_manager.py` — update completion message wording
- [ ] Task I: Verify existing single-call path (non-precompute callers) unchanged
- [ ] Task J: End-to-end test with Kwara — confirm all 15 combinations ready, message arrives once, wording correct

## Constraints
- No new files
- All existing call sites pass no new params → identical behaviour guaranteed
- SQLite storage in `tpr_precomputed.db` unchanged — only the computation path is faster
- `schedule_precompute` in `precompute_service.py` still exists for retry path — only the happy path in `workflow_manager` goes synchronous

---

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
