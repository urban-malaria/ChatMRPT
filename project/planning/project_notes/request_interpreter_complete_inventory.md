# RequestInterpreter Complete Inventory

**Date:** 2026-01-16
**Purpose:** Document everything linked to RequestInterpreter before migrating to unified LangGraph agent

---

## 1. RequestInterpreter Core File

**File:** `app/core/request_interpreter.py` (~1600+ lines)

### Tools Registered (via `_register_tools()`)

| Tool Name | Function | Already in LangGraph? | Migration Status |
|-----------|----------|----------------------|------------------|
| `run_malaria_risk_analysis` | `_run_malaria_risk_analysis` | ✅ Yes | Done |
| `create_vulnerability_map` | `_create_vulnerability_map` | ✅ Yes | Done |
| `create_pca_map` | `_create_pca_map` | ❌ No | Need to add |
| `create_variable_distribution` | `_create_variable_distribution` | ✅ Yes | Done |
| `create_urban_extent_map` | `_create_urban_extent_map` | ❌ No | Need to add |
| `create_decision_tree` | `_create_decision_tree` | ❌ No | Need to add |
| `create_composite_score_maps` | `_create_composite_score_maps` | ❌ No | Need to add |
| `create_settlement_map` | `_create_settlement_map` | ❌ No | Need to add |
| `show_settlement_statistics` | `_show_settlement_statistics` | ❌ No | Need to add |
| `execute_sql_query` | `_execute_sql_query` | ❌ No (use analyze_data) | Replace with analyze_data |
| `explain_analysis_methodology` | `_explain_analysis_methodology` | ❌ No | Need to add |
| `run_itn_planning` | `_run_itn_planning` | ❌ No | Need to add |
| `analyze_data_with_python` | `_analyze_data_with_python` | ✅ Yes (as analyze_data) | Done |
| `list_dataset_columns` | `_list_dataset_columns` | ❌ No (use analyze_data) | Replace with analyze_data |

### Key Methods

| Method | Purpose | Keep for Unified? |
|--------|---------|-------------------|
| `process_message()` | Main entry point | No - LangGraph has `analyze()` |
| `process_message_streaming()` | Streaming entry | No - LangGraph handles this |
| `_build_system_prompt_refactored()` | Build prompts | No - LangGraph has `system_prompt.py` |
| `_interpret_raw_output()` | Interpret tool output | Maybe - review |
| `_explain_visualization_universally()` | Auto-explain vizs | Maybe - could be useful |
| `_handle_special_workflows()` | ITN, etc. | Review - may need |
| `_get_session_context()` | Get context | Covered by LangGraph state |
| `_store_conversation()` | Memory | LangGraph has memory service |

---

## 2. Helper Files Directly Used by RequestInterpreter

### 2.1 Core Dependencies (MUST REVIEW)

| File | Purpose | Used By | Migration Notes |
|------|---------|---------|-----------------|
| `app/core/choice_interpreter.py` | LLM-based arg extraction | RequestInterpreter | May not need - LangGraph tool binding |
| `app/core/tool_intent_resolver.py` | NL → tool mapping | RequestInterpreter | May not need - LangGraph routing |
| `app/core/tool_runner.py` | Execute tools by name | RequestInterpreter | Keep - used by tool registry |
| `app/core/prompt_builder.py` | Build system prompts | RequestInterpreter | Not needed - use system_prompt.py |
| `app/core/llm_orchestrator.py` | LLM call orchestration | RequestInterpreter | Not needed - LangGraph handles |
| `app/core/session_context_service.py` | Session context | RequestInterpreter | Not needed - LangGraph state |
| `app/core/data_repository.py` | Data access | Multiple | Keep - shared |

### 2.2 Supporting Files

| File | Purpose | Keep? |
|------|---------|-------|
| `app/core/interpreter_migration.py` | Migration utilities | DELETE after migration |
| `app/core/simple_request_interpreter.py` | Simplified version | DELETE |
| `app/core/arena_integration_patch.py` | Arena patching | DELETE |
| `app/core/tool_schema_registry.py` | Tool schemas | Keep - used by tool_runner |
| `app/core/tool_capabilities.py` | Tool metadata | Keep - could be useful |

---

## 3. Files That Import/Reference RequestInterpreter

### 3.1 Routes (CRITICAL - Must Update)

| File | Usage | Action Needed |
|------|-------|---------------|
| `app/web/routes/analysis_routes.py:870-890` | Uses `request_interpreter.process_message()` | Route to DataAnalysisAgent |
| `app/web/routes/analysis_routes.py:1805-1860` | Uses `process_message_streaming()` | Route to DataAnalysisAgent |
| `app/web/routes/analysis/chat_sync_service.py:213-221` | Uses `request_interpreter.process_message()` | Route to DataAnalysisAgent |
| `app/web/routes/analysis/chat_stream_service.py:195-255` | Uses `_stream_request_interpreter()` | Route to DataAnalysisAgent |
| `app/core/analysis_routes.py:1123-1140` | Uses `request_interpreter.process_message()` | Route to DataAnalysisAgent |

### 3.2 Container/Service Registration

| File | Usage | Action Needed |
|------|-------|---------------|
| `app/services/container.py:105,564-620` | Registers `request_interpreter` service | Remove or deprecate |
| `app/core/container.py:104,532-586` | Registers `request_interpreter` service | Remove or deprecate |
| `app/config/container.py:103,458-514` | Registers `request_interpreter` service | Remove or deprecate |

### 3.3 Other References

| File | Usage | Action Needed |
|------|-------|---------------|
| `app/core/tool_arena_pipeline.py:110-112` | Imports RequestInterpreter | Update or remove |
| `app/data_analysis_v3/core/data_exploration_agent.py:25` | Doc reference only | Update docs |

---

## 4. Tools in RequestInterpreter NOT in LangGraph

### Must Add to `unified_tools.py`:

1. **`create_settlement_map`** - Settlement visualization
   - Source: `app/tools/settlement_tools.py`
   - Wraps: `CreateSettlementMap`

2. **`show_settlement_statistics`** - Settlement stats
   - Source: `app/tools/settlement_tools.py`
   - Wraps: `ShowSettlementStatistics`

3. **`run_itn_planning`** - ITN distribution planning
   - Source: `app/core/itn_pipeline.py`
   - Complex multi-step workflow

4. **`create_pca_map`** - PCA-specific map
   - Already in `create_vulnerability_map(method='pca')`
   - Consider if separate tool needed

5. **`create_urban_extent_map`** - Urban extent visualization
   - Source: `app/tools/visualization_maps_tools.py`
   - Wraps: `CreateUrbanExtentMap`

6. **`create_decision_tree`** - Decision tree viz
   - Source: `app/tools/visualization_maps_tools.py`
   - Wraps: `CreateDecisionTree`

7. **`explain_analysis_methodology`** - Explain methods
   - Static text explanation
   - Could be in system prompt or simple tool

---

## 5. Data Analysis V3 (LangGraph) Current State

### File: `app/data_analysis_v3/tools/unified_tools.py`

**Current Tools:**
1. `analyze_data` - Python/pandas execution ✅
2. `analyze_tpr_data` - TPR calculation ✅
3. `run_malaria_risk_analysis` - Risk ranking ✅
4. `create_variable_distribution` - Choropleth maps ✅
5. `create_vulnerability_map` - Risk classification ✅

**Missing (Need to Add):**
- Settlement tools (2)
- ITN planning (1)
- Urban extent map (1)
- Decision tree (1)
- Methodology explanation (1)

---

## 6. Migration Checklist

### Phase 1: Add Missing Tools to `unified_tools.py`

- [ ] `create_settlement_map`
- [ ] `show_settlement_statistics`
- [ ] `run_itn_planning` (complex - may need workflow)
- [ ] `create_urban_extent_map`
- [ ] `create_decision_tree`
- [ ] `explain_analysis_methodology`

### Phase 2: Update Routes

- [ ] `app/web/routes/analysis_routes.py` - Route to DataAnalysisAgent
- [ ] `app/web/routes/analysis/chat_sync_service.py` - Route to DataAnalysisAgent
- [ ] `app/web/routes/analysis/chat_stream_service.py` - Route to DataAnalysisAgent
- [ ] `app/core/analysis_routes.py` - Route to DataAnalysisAgent

### Phase 3: Update Service Containers

- [ ] Remove `request_interpreter` from `app/services/container.py`
- [ ] Remove from `app/core/container.py`
- [ ] Remove from `app/config/container.py`

### Phase 4: Cleanup

- [ ] Delete `app/core/request_interpreter.py`
- [ ] Delete `app/core/simple_request_interpreter.py`
- [ ] Delete `app/core/interpreter_migration.py`
- [ ] Delete `app/core/arena_integration_patch.py`
- [ ] Review/update `app/core/choice_interpreter.py`
- [ ] Review/update `app/core/tool_intent_resolver.py`
- [ ] Review/update `app/core/prompt_builder.py`
- [ ] Review/update `app/core/llm_orchestrator.py`

---

## 7. Session Flag Simplification

### Current Flags (Confusing!)

```python
# Flask session flags
session['use_data_analysis_v3']      # True = use LangGraph
session['data_analysis_active']      # Another flag for same thing
session['csv_loaded']                # Data uploaded
session['shapefile_loaded']          # Shapefile uploaded
session['analysis_complete']         # Risk analysis done
session['tpr_workflow_complete']     # TPR done
session['itn_planning_complete']     # ITN done

# File-based flags
instance/uploads/{session_id}/.data_analysis_mode
instance/uploads/{session_id}/.analysis_complete
```

### Simplified (After Unification)

```python
# All handled by DataAnalysisStateManager
# No more use_data_analysis_v3 flag - EVERYTHING uses LangGraph
session['session_id']                # Only need session ID
# State in DataAnalysisStateManager handles:
#   - workflow_stage
#   - tpr_completed
#   - risk_analysis_completed
#   - available_tools
```

---

## 8. Risk Assessment

### Low Risk Changes
- Adding tools to unified_tools.py
- Updating system_prompt.py

### Medium Risk Changes
- Updating routes to use DataAnalysisAgent
- Removing session flags

### High Risk Changes
- Deleting RequestInterpreter (ensure all routes updated first!)
- Modifying container registrations

---

## 9. Rollback Plan

1. Keep `request_interpreter.py` as `request_interpreter.py.bak`
2. Use feature flag initially: `USE_UNIFIED_AGENT=true`
3. Test thoroughly before removing backup
4. Keep commit history clean for easy revert

---

## 10. Files Summary

### Files to Modify
- `app/data_analysis_v3/tools/unified_tools.py` - Add 6 tools
- `app/data_analysis_v3/prompts/system_prompt.py` - Update capabilities
- `app/web/routes/analysis_routes.py` - Route all to DataAnalysisAgent
- `app/web/routes/analysis/chat_sync_service.py` - Same
- `app/web/routes/analysis/chat_stream_service.py` - Same
- `app/services/container.py` - Remove request_interpreter

### Files to Eventually Delete
- `app/core/request_interpreter.py` (1600+ lines)
- `app/core/simple_request_interpreter.py`
- `app/core/interpreter_migration.py`
- `app/core/arena_integration_patch.py`
- `app/core/choice_interpreter.py` (maybe)
- `app/core/tool_intent_resolver.py` (maybe)
- `app/core/prompt_builder.py` (maybe)
- `app/core/llm_orchestrator.py` (maybe)

### Files to Keep
- `app/core/tool_runner.py` - Used by tool registry
- `app/core/tool_registry.py` - Core tool infrastructure
- `app/core/tool_schema_registry.py` - Tool schemas
- `app/core/data_repository.py` - Shared data access
