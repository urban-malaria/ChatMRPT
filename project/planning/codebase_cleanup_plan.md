# ChatMRPT Codebase Cleanup Plan

**Date:** 2026-03-12
**Status:** Reviewed x2 — corrections applied, ready to execute
**Goal:** Remove dead code, eliminate duplicates, and establish a single clear path through the codebase — without breaking anything.

---

## Guiding Principle

Nothing is deleted. All archived files move to `_archive/` at the repo root (gitignored), preserving the original folder structure so anything can be recovered. Use `git mv` to move files so history is preserved.

---

## Phase 1 — Safe Archival (Confirmed Dead Files)

**Risk:** Zero — these files are not imported from outside their own package.
**Effort:** ~1 hour
**Approach:** Move only. Do not modify any other file.

### Files to archive

| Source | Destination in `_archive/` | Why |
|---|---|---|
| `app/data_analysis_module/` (entire dir) | `_archive/app/data_analysis_module/` | Superseded by data_analysis_v3; ~1,060 lines. Note: referenced by `tests/test_data_analysis_module.py` — adjust that test script after archiving. |
| `app/web/routes/analysis_routes_session_fix.py` | `_archive/app/web/routes/analysis_routes_session_fix.py` | 87-line pasted snippet, never wired up, not imported anywhere |
| `app/core/visualization_maps_tools.py` | `_archive/app/core/visualization_maps_tools.py` | Confirmed not imported anywhere — all runtime code uses `app/tools/visualization_maps_tools.py` |
| All `*.backup*` files under `app/` | `_archive/` (mirror original paths) | Editor artifacts across 9+ directories. Use: `find app/ -name "*.backup*"` |

> **⚠️ NOT in Phase 1 — runtime/tpr/ package:**
> `workflow.py` exports `reset_tpr_handler_cache` (live import in `chat_stream_service.py:14`).
> `detector.py` imports `is_tpr_data`/`validate_tpr_data` from `utils.py` — so `utils.py` is still referenced within the package.
> The entire `app/runtime/tpr/` package must be handled together in Phase 2, not piecemeal here.

### `.gitignore` entry to add
```
_archive/
```
Note: `.gitignore` already ignores `*_archive_*` and various `*.backup*` patterns but NOT a top-level `_archive/` — add it explicitly.

### Verification
```bash
grep -rn "from app.data_analysis_module\|import data_analysis_module" app/ --include="*.py"
grep -rn "analysis_routes_session_fix" app/ --include="*.py"
grep -rn "from app.core.visualization_maps_tools\|import visualization_maps_tools" app/ --include="*.py"
# All should return zero results
```

---

## Phase 2 — Unify TPR Duplicates + Kwara Column Detection Fixes

**Risk:** Medium — touches active code paths. Requires tests before and after.
**Effort:** ~4 hours
**Prerequisite:** Phase 1 complete.

### Problem
Two `TPRDataAnalyzer` classes and two `calculate_ward_tpr()` implementations, all live:

**TPRDataAnalyzer:**
| File | Active code paths that use it |
|---|---|
| `app/data_analysis_v3/tpr/data_analyzer.py` | `data_analysis_v3_routes.py:567` → `workflow_manager.py` (React "Data Analysis" tab) |
| `app/data_analysis_v3/core/tpr_data_analyzer.py` | `chat_stream_service.py:113`, `tpr_workflow_langgraph_tool.py:20`, `runtime/tpr/workflow.py`, `tests/tpr/test_tpr_data_analyzer.py:10` |

**app/runtime/tpr/ package:**
| File | Status |
|---|---|
| `workflow.py` | Live — exports `reset_tpr_handler_cache` to `chat_stream_service.py:14` |
| `utils.py` | Referenced only within the package via `__init__.py` `__all__` and `detector.py:15` |
| `detector.py` | Imports `is_tpr_data`/`validate_tpr_data` from `utils.py` |

### Plan

**Step 1 — Relocate `reset_tpr_handler_cache`**
- Move the function from `runtime/tpr/workflow.py` into `app/data_analysis_v3/tpr/workflow_manager.py`
- Update the single import in `chat_stream_service.py:14`
- Archive `runtime/tpr/workflow.py`, `utils.py`, `detector.py`, `__init__.py`

**Step 2 — Choose canonical `TPRDataAnalyzer`**
- Canonical version: `app/data_analysis_v3/tpr/data_analyzer.py`
- Before archiving `core/tpr_data_analyzer.py`, update all import sites:
  - `chat_stream_service.py:113`
  - `tpr_workflow_langgraph_tool.py:20`
  - `tests/tpr/test_tpr_data_analyzer.py:10`
- ⚠️ Verify constructor signature: `chat_stream_service.py:116` calls `TPRDataAnalyzer(session_id)` with an argument — confirm `tpr/data_analyzer.py` accepts an optional `session_id` before switching
- Archive `app/data_analysis_v3/core/tpr_data_analyzer.py`

**Step 3 — Apply column detection fixes**
Apply Kwara bug fixes to **both**:
- `app/data_analysis_v3/tpr/data_analyzer.py` (drives the workflow UI)
- `app/core/tpr_utils.py` (the shared toolchain implementation — the more important one)

Fixes to apply to both:
- **Bug 1 (Critical):** Skip `orgunitlevel*` columns when detecting facility level
- **Bug 2 (Moderate):** Add `organisationunit` to facility name detection patterns
- **Bug 3 (Minor):** Add `orgunitlevel2` as explicit state column candidate
- **Bug 4 (Safeguard):** Validate detected facility level column values resemble facility types

### Verification
- Run `tests/tpr/test_tpr_data_analyzer.py` after updating imports
- Run TPR workflow end-to-end with Kwara data file
- Confirm streaming chat TPR path still works

---

## Phase 3 — Tool System Consolidation

**Risk:** Medium — touches many files, but each change is a simple import swap.
**Effort:** ~half day
**Prerequisite:** Phase 2 complete.

### Known duplicate pairs (with confirmed ownership)

| Duplicate | Keep | Archive | Evidence |
|---|---|---|---|
| Visualization maps | `app/tools/visualization_maps_tools.py` | `app/core/visualization_maps_tools.py` | Already archived in Phase 1 — core version has no importers |
| Export tools | `app/tools/export_tools.py` | `app/core/export_tools.py` | `tiered_tool_loader.py:143` and `tool_registry.py:487` use `app.tools` version; `app/core` version used only by `reports/modern_generator.py:89` — update that one import |
| TPR query | `app/data_analysis_v3/tools/tpr_analysis_tool.py` | TBD | Trace imports before deciding |

### Additional cleanup in this phase
The tool registry (`app/core/tool_registry.py`) still references modules that no longer exist (e.g., `app.tools.risk_analysis_tools`). Prune these broken references or the tool discovery will silently fail.

### Plan
1. For each pair above: grep importers, confirm, swap the one import that needs updating, archive the unused copy
2. Prune broken module references from `tool_registry.py`
3. Document canonical tool locations in `docs/TOOL_REGISTRY.md`

### Verification
Run full test suite. Manually trigger each tool type through the chat interface.

---

## Phase 4 — Blueprint & Route Cleanup

**Risk:** Low-Medium
**Effort:** ~2 hours
**Prerequisite:** Phase 3 complete.

### Problems

**Two `debug_bp` blueprints — both registered in `app/web/routes/__init__.py`:**
- `app/web/routes/debug_routes.py` — canonical, keep
- `app/web/routes/debug_session.py` — both defined AND registered; Flask will raise a name conflict if both load. Archive `debug_session.py` AND remove its registration line from `__init__.py`.

**Three copies of `data_analysis_v3_routes.py`:**
- `app/web/routes/data_analysis_v3_routes.py` — canonical, wired into Flask, keep
- `app/data_analysis_v3/core/data_analysis_v3_routes.py` — ⚠️ verify whether this is truly a duplicate or serves a different purpose before archiving
- `app/data_analysis_v3_routes.py` (root-level) — not imported; archive

### Plan
1. Archive `app/web/routes/debug_session.py` + remove its registration from `__init__.py`
2. Verify `app/data_analysis_v3/core/data_analysis_v3_routes.py` imports and purpose, then archive if unused
3. Archive root-level `app/data_analysis_v3_routes.py`

---

## Phase 5 — Decommission Old Analysis Systems

**Risk:** High — touches the oldest code. Do last.
**Effort:** ~1–2 days
**Prerequisite:** Phases 1–4 complete and stable.

### `app/analysis/` — heavily imported, cannot archive as a unit
Confirmed live importers include:
- `app/core/request_interpreter.py`
- `app/runtime/standard/workflow.py`
- `app/web/routes/analysis/analysis_exec.py` (registered blueprint)
- `app/web/routes/itn_routes.py` — imports `itn_pipeline.py`
- `app/data/*`

**Plan:** Audit each submodule individually. Archive only confirmed-unused files. `itn_pipeline.py` and `engine.py` stay until their callers are migrated or the standard risk workflow is decommissioned — that is out of scope for this cleanup.

### Config variants
- `app/config/production_optimized.py` — verify if used; archive if not
- `app/config/production_transition.py` — likely dead; archive

---

## What This Does NOT Change

- No logic changes in Phases 1, 3, 4, 5 — only file moves and import updates
- `tpr_workflow_handler.py` (1,956 lines) is NOT decomposed — future refactor, out of scope
- `app/analysis/` core modules (`itn_pipeline.py`, `engine.py`) stay until callers are migrated
- No renaming of functions or classes beyond what is required for deduplication

---

## Estimated Impact

| Phase | Files affected | Lines removed | Risk |
|---|---|---|---|
| 1 — Dead file archival | ~35 files | ~2,500 lines | Zero |
| 2 — TPR unification + Kwara fixes | 5 files updated + 5 archived | ~1,035 lines | Medium |
| 3 — Tool consolidation | ~6 files | ~500 lines | Medium |
| 4 — Blueprint cleanup | ~3 files + 1 line | ~50 lines | Low-Medium |
| 5 — Old analysis systems (partial) | ~20 files | ~4,000 lines | High |
| **Total** | **~69 files** | **~8,085 lines** | |
