# Phase 3: Consolidation Plan

## Status: INVESTIGATION NEEDED BEFORE EXECUTION

## Key Question
Phase 3 is about consolidation (reorganizing), not deletion. Moving files
doesn't reduce the count — it makes the structure clearer. Before we
execute, we need to answer:

1. Which consolidations actually REDUCE complexity (not just move it)?
2. Are any of the "scattered" files actually dead?
3. Is the current separation actually intentional (different purposes)?

---

## 3.1 TPR Code — 9 files across 3 directories

### Current layout
```
app/core/
  tpr_precompute.py          (418 lines)
  tpr_precompute_service.py  (240 lines)
  tpr_utils.py               (598 lines)
  tpr_ward_cache.py          (102 lines)

app/data_analysis_v3/tpr/
  workflow_manager.py         (1,152 lines)
  data_analyzer.py            (557 lines)

app/data_analysis_v3/core/
  tpr_workflow_handler.py     (1,972 lines)
  tpr_language_interface.py   (560 lines)
  tpr_intent_classifier.py    (241 lines)
```

### Investigation needed
- [ ] Are tpr_precompute.py and tpr_precompute_service.py doing the same thing?
- [ ] Is workflow_manager.py the same as tpr_workflow_handler.py? (both ~1000+ lines)
- [ ] Who imports each file? How many files would need import updates if we move them?
- [ ] Can any of these 9 be merged (not just moved)?

### Risk: HIGH
- 9 files, potentially dozens of importers
- TPR is the core workflow — breaking it breaks the product

---

## 3.2 State Management — 5 files

### Current layout
```
app/core/unified_data_state.py       (313 lines) — which DataFrames are loaded
app/core/session_state.py            (338 lines) — Flask session helpers
app/services/session_memory.py       (277 lines) — conversation message persistence
app/data_analysis_v3/core/state_manager.py (482 lines) — TPR workflow stage tracking
app/core/workflow_state_manager.py   (379 lines) — workflow state tracking
```

### Investigation needed
- [ ] Are these actually doing DIFFERENT things? (they might be — session vs workflow vs data vs memory)
- [ ] Do any overlap? (unified_data_state vs workflow_state_manager?)
- [ ] Would merging them actually simplify things or create a monster file?

### Risk: MEDIUM
- If they're genuinely different, moving them together might make things worse
- They might just need better naming, not consolidation

---

## 3.3 Visualization Code — 7+ files across 3 directories

### Current layout
```
app/tools/
  visualization_maps_tools.py        (1,101) — tool classes (our wrappers call these)
  visualization_charts_tools.py      (2,643) — chart tools (WHO CALLS THIS?)

app/services/visualization/
  chart_service.py                   (780)   — chart generation service

app/services/agents/visualizations/
  composite_visualizations.py        (1,613) — map rendering engine
  pca_visualizations.py              (223)   — PCA map rendering
  core_utils.py                      (423)   — GeoJSON utilities
  tpr_visualization_service.py       (218)   — TPR visualization
```

### Investigation needed
- [ ] Is visualization_charts_tools.py (2,643 lines) actually called by ANYTHING?
      Our @tool wrappers don't use it. The agent doesn't use it. It might be dead.
- [ ] Is chart_service.py called by anything active?
- [ ] The tools/ → services/agents/visualizations/ separation is actually logical
      (tools = interface, services = rendering). Do we want to merge them?

### Risk: MEDIUM
- visualization_charts_tools.py might be entirely dead (2,643 lines to archive!)
- chart_service.py might also be dead

---

## Before executing Phase 3, we need answers to:

### Quick investigations (can do now)
1. Is visualization_charts_tools.py dead? → grep for imports
2. Is chart_service.py dead? → grep for imports
3. Are tpr_precompute and tpr_precompute_service redundant? → compare code
4. Is workflow_manager.py the same as tpr_workflow_handler.py? → compare
5. Is workflow_state_manager.py still needed? → grep for imports
6. Are the 5 state management files genuinely different? → read each

### If we find dead files, we archive them (further reduces count)
### If we find genuine duplicates, we merge them
### Only THEN do we reorganize the survivors

---

## Proposed execution order

1. **Investigate first** — answer the 6 questions above
2. **Archive any newly discovered dead files** — may cut 3-5 more files
3. **Merge genuine duplicates** (if any found in TPR)
4. **THEN decide if reorganization is worth the import-update risk**

## Rule
DO NOT move files unless the benefit clearly outweighs the risk of
breaking imports. Better to have well-named files in "wrong" directories
than broken imports from hasty reorganization.
