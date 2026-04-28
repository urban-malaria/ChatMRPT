# Codebase Cleanup Plan (Updated April 3, 2026)

## Current State
- 209 active backend Python files, 83,689 lines of code
- 45 frontend TypeScript files
- 56 archived dead files in _archived/ folders
- 6+ duplicate implementations of the same functionality
- 10 monolithic files over 1000 lines each
- 7 empty directories
- TPR scattered across 9 files in 3 directories
- State management in 5 different places
- Response formatting in 3 different places
- Visualization code scattered across 7+ files in 3 directories

## Target
A clean, understandable codebase where:
- Every file has one clear purpose
- No duplicates
- No empty directories
- No file over 800 lines
- Related code lives together
- A new developer can follow the flow

---

## Phase 1: Quick wins (zero risk)

### 1.1 Delete empty directories
```
app/agents/                      (empty)
app/runtime/tpr/                 (empty)
app/data_analysis_v3/utils/      (empty)
app/data_analysis_v3/interface/  (empty)
app/services/tools/              (only _archived)
app/reports/                     (just a 20-line __init__.py)
app/models/                      (just 1 file — move data_handler.py elsewhere)
```

### 1.2 Remove empty template/static folders
```
app/templates/                   (empty — React replaced Jinja)
app/templates/survey/            (empty)
app/templates/prepost/           (empty)
app/templates_archived_*/        (entire archive folder)
```

**Test after:** `python run.py` starts without errors

---

## Phase 2: Resolve duplicates (low risk, high impact)

Do ONE duplicate at a time. Test after each.

### 2.1 LLMAdapter duplicate
- `app/core/llm_adapter.py` (718 lines)
- `app/services/llm_adapter.py` (306 lines)
- **Investigate:** Which is imported by active code? Keep that one.

### 2.2 UnifiedDataState duplicate
- `app/core/unified_data_state.py` (313 lines)
- `app/services/unified_data_state.py` (294 lines)
- **Investigate:** Which is imported? Are they different?

### 2.3 ServiceContainer duplicate
- `app/config/container.py` (590 lines)
- `app/services/container.py` (696 lines)
- **Investigate:** Which does `app/__init__.py` actually use?

### 2.4 python_tool.py duplicate
- `app/data_analysis_v3/core/python_tool.py` (291 lines)
- `app/data_analysis_v3/tools/python_tool.py` (338 lines)
- **Investigate:** Which does agent.py import?

### 2.5 system_prompt.py duplicate
- `app/data_analysis_v3/core/system_prompt.py` (167 lines)
- `app/data_analysis_v3/prompts/system_prompt.py` (171 lines)
- **Investigate:** Which does agent.py import? Are contents different?

### 2.6 Response formatter triplication
- `app/services/response_formatter.py` (546 lines)
- `app/data_analysis_v3/core/formatters.py` (409 lines)
- `app/data_analysis_v3/formatters/response_formatter.py` (243 lines)
- **Investigate:** Who imports which? Can we keep just one?

### 2.7 Earth Engine client duplicate
- `app/services/earth_engine_client.py` (354 lines)
- `app/services/robust_earth_engine_client.py` (540 lines)
- **Investigate:** Which is imported? (likely robust_ is newer)

### 2.8 executor.py duplicate
- `app/data_analysis_v3/core/executor.py` (687 lines)
- `app/data_analysis_v3/core/executor_simple.py` (617 lines)
- **Investigate:** Which does python_tool.py use?

### 2.9 agent.py vs agent_fixed.py
- `app/data_analysis_v3/core/agent.py` (1,117 lines)
- `app/data_analysis_v3/core/agent_fixed.py` (300 lines)
- **Investigate:** Is agent_fixed.py a legacy version?

**Test after each:** Full workflow (upload → TPR → map → risk → vulnerability)

---

## Phase 3: Consolidate scattered code (medium risk)

### 3.1 Consolidate TPR code
Currently 9 files across 3 directories:
- `app/core/tpr_precompute.py` (418)
- `app/core/tpr_precompute_service.py` (240)
- `app/core/tpr_utils.py` (598)
- `app/core/tpr_ward_cache.py` (102)
- `app/data_analysis_v3/tpr/workflow_manager.py` (1,152)
- `app/data_analysis_v3/tpr/data_analyzer.py` (557)
- `app/data_analysis_v3/core/tpr_workflow_handler.py` (1,972)
- `app/data_analysis_v3/core/tpr_language_interface.py` (560)
- `app/data_analysis_v3/core/tpr_intent_classifier.py` (241)

**Target:** All TPR code in `app/data_analysis_v3/tpr/`

### 3.2 Consolidate state management
Currently 5 places:
- `app/core/unified_data_state.py`
- `app/core/session_state.py`
- `app/services/session_memory.py`
- `app/services/unified_data_state.py`
- `app/data_analysis_v3/core/state_manager.py`

**Investigate:** Which are actually different vs doing the same thing?

### 3.3 Consolidate visualization code
Currently 7+ files across 3 directories:
- `app/tools/visualization_maps_tools.py` (1,101)
- `app/tools/visualization_charts_tools.py` (2,643)
- `app/services/visualization/chart_service.py` (780)
- `app/services/agents/visualizations/composite_visualizations.py` (1,613)
- `app/services/agents/visualizations/pca_visualizations.py` (223)
- `app/services/agents/visualizations/core_utils.py` (423)
- `app/services/agents/visualizations/tpr_visualization_service.py` (218)

**Investigate:** What calls what? What does the agent actually use?

---

## Phase 4: Extract misplaced code (medium risk)

### 4.1 Extract DataHandler from data/__init__.py
- `app/data/__init__.py` has 1,250 lines (DataHandler class)
- Move to `app/data/handler.py`
- Keep backward-compatible import in __init__.py

### 4.2 Check query_result.py
- `app/services/query_result.py` (177 lines) — was used by dead SQL pipeline
- Verify if still needed

---

## Phase 5: Split monolithic files (lower priority, after phases 1-4)

Files over 1000 lines:

| File | Lines | Action |
|------|-------|--------|
| `tools/visualization_charts_tools.py` | 2,643 | Check if agent uses it — may be dead |
| `data/unified_dataset_builder.py` | 2,166 | Split by builder stage |
| `data_analysis_v3/core/tpr_workflow_handler.py` | 1,972 | Addressed in Phase 3.1 |
| `web/routes/analysis_routes.py` | 1,835 | Split by feature |
| `tools/complete_analysis_tools.py` | 1,805 | Our wrappers call this — keep but simplify |
| `analysis/itn_pipeline.py` | 1,765 | Split pipeline stages |
| `services/agents/visualizations/composite_visualizations.py` | 1,613 | Addressed in Phase 3.3 |
| `tools/export_tools.py` | 1,522 | Check what's actually exported |
| `data_analysis_v3/tpr/workflow_manager.py` | 1,152 | Addressed in Phase 3.1 |
| `tools/visualization_maps_tools.py` | 1,101 | Our wrappers call this — keep but review |

---

## Phase 6: Frontend cleanup (separate effort)

- Audit 45 frontend files — are all components used?
- Remove dead stores, hooks, components
- Simplify after standard mode removal

---

## Rules

1. **Investigate before acting.** Grep, trace imports, verify.
2. **One change at a time.** Commit after each.
3. **Test after each change.** Full workflow.
4. **Archive, don't delete.** Move to _archived/ for reference.
5. **Update this plan** as we go — check off completed items.

---

## Progress Tracker

### Phase 1: Quick wins
- [ ] 1.1 Delete empty directories
- [ ] 1.2 Remove empty template/static folders

### Phase 2: Resolve duplicates (9 items)
- [ ] 2.1 LLMAdapter
- [ ] 2.2 UnifiedDataState
- [ ] 2.3 ServiceContainer
- [ ] 2.4 python_tool.py
- [ ] 2.5 system_prompt.py
- [ ] 2.6 Response formatters (3 files)
- [ ] 2.7 Earth Engine client
- [ ] 2.8 executor.py
- [ ] 2.9 agent_fixed.py

### Phase 3: Consolidate scattered code
- [ ] 3.1 TPR code (9 files → 1 directory)
- [ ] 3.2 State management (5 places → fewer)
- [ ] 3.3 Visualization code (7+ files → cleaner)

### Phase 4: Extract misplaced code
- [ ] 4.1 DataHandler from __init__.py
- [ ] 4.2 query_result.py check

### Phase 5: Split monolithic files
- [ ] (individual files TBD after Phases 3-4)

### Phase 6: Frontend cleanup
- [ ] 6.1 Audit
- [ ] 6.2 Remove dead code
