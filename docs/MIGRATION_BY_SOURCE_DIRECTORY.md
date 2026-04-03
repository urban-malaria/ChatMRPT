# Migration Execution Guide: Grouped by SOURCE DIRECTORY

This guide shows how to execute moves, grouped by where files currently are.

---

## PHASE 1: NO MOVES NEEDED (35 files stay in place)

These directories/files need no changes:
- app/__init__.py (factory function)
- app/analysis/* (14 files, except itn_pipeline.py)
- app/auth/* (6 files, all OAuth)
- app/config/* (9 files, all configuration)
- app/prepost/models.py, questions.py, __init__.py (3 files)
- app/survey/models.py, questions.py, populate_questions.py, __init__.py (4 files)
- app/utils/geospatial_levels.py, lga_boundaries.py, map_overlays.py, security.py (4 files)

**No moves needed for: 54 files total**

---

## PHASE 2: SIMPLE MOVES (1 file)

### From app/analysis/ to app/planning/
```bash
mkdir -p app/planning
mv app/analysis/itn_pipeline.py app/planning/itn_pipeline.py
```
- 1 file moved

---

## PHASE 3: DISTRIBUTE app/core/ (28 files → 6 destinations)

### Create directories first:
```bash
mkdir -p app/agent
mkdir -p app/conversation
mkdir -p app/tpr
mkdir -p app/arena
mkdir -p app/utils
mkdir -p app/services
```

### app/core → app/services (infrastructure, 10 files)
```bash
mv app/core/llm_manager.py app/services/
mv app/core/llm_adapter.py app/services/
mv app/core/session_state.py app/services/
mv app/core/session_context_service.py app/services/
mv app/core/session_helper.py app/services/
mv app/core/data_repository.py app/services/
mv app/core/unified_data_state.py app/services/
mv app/core/instance_sync.py app/services/
mv app/core/responses.py app/services/
mv app/core/utils.py app/utils/core_utils.py  # RENAME
```

### app/core → app/agent (orchestration, 4 files)
```bash
mv app/core/request_interpreter.py app/agent/
mv app/core/prompt_builder.py app/agent/
mv app/core/tool_schema_registry.py app/agent/
mv app/core/tool_validator.py app/agent/
```

### app/core → app/conversation (state management, 3 files)
```bash
mv app/core/redis_state_manager.py app/conversation/
mv app/core/workflow_state_manager.py app/conversation/
mv app/core/analysis_state_handler.py app/conversation/
```

### app/core → app/arena (arena-specific, 2 files)
```bash
mv app/core/arena_manager.py app/arena/
mv app/core/arena_system_prompt.py app/arena/
```

### app/core → app/tpr (TPR workflow, 5 files)
```bash
mv app/core/tpr_precompute.py app/tpr/
mv app/core/tpr_precompute_service.py app/tpr/
mv app/core/tpr_utils.py app/tpr/
mv app/core/tpr_ward_cache.py app/tpr/
mv app/core/variable_matcher.py app/tpr/
```

### app/core → app/utils (shared utilities, 3 files)
```bash
mv app/core/exceptions.py app/utils/
mv app/core/decorators.py app/utils/
mv app/core/dependency_validator.py app/utils/
```

### Clean up app/core
```bash
rm -rf app/core/
```

---

## PHASE 4: DISTRIBUTE app/data/ (11 files → 3 destinations)

### app/data → app/services (5 files)
```bash
mv app/data/unified_dataset_builder.py app/services/
mv app/data/flexible_data_access.py app/services/
mv app/data/loaders.py app/services/data_loaders.py  # RENAME
mv app/data/settlement_loader.py app/services/
mv app/data/population_data/itn_population_loader.py app/services/itn_population_loader.py  # RENAME
```

### app/data → app/analysis (2 files, RENAME)
```bash
mv app/data/analysis.py app/analysis/data_analysis_utils.py  # RENAME
mv app/data/utils.py app/analysis/data_utils.py  # RENAME
```

### app/data → app/utils (4 files, RENAME)
```bash
mv app/data/__init__.py app/utils/data_init.py  # RENAME
mv app/data/validation.py app/utils/data_validation.py  # RENAME
mv app/data/processing.py app/utils/data_processing.py  # RENAME
mv app/data/reporting.py app/utils/data_reporting.py  # RENAME
```

### Clean up app/data
```bash
rm -rf app/data/
```

---

## PHASE 5: DISTRIBUTE app/data_analysis_v3/ (~30 files → 5 destinations)

### Create subdirectories
```bash
mkdir -p app/agent/tools
mkdir -p app/agent/prompts
mkdir -p app/tpr
mkdir -p app/visualization
```

### app/data_analysis_v3/core → app/agent (5 files)
```bash
mv app/data_analysis_v3/core/agent.py app/agent/data_analysis_agent.py  # RENAME
mv app/data_analysis_v3/core/executor.py app/agent/
mv app/data_analysis_v3/core/executor_simple.py app/agent/
mv app/data_analysis_v3/core/state_manager.py app/agent/
mv app/data_analysis_v3/core/state.py app/agent/
```

### app/data_analysis_v3/core → app/tpr (2 files, RENAME)
```bash
mv app/data_analysis_v3/core/tpr_intent_classifier.py app/tpr/intent_classifier.py  # RENAME
mv app/data_analysis_v3/core/tpr_language_interface.py app/tpr/language_interface.py  # RENAME
```

### app/data_analysis_v3/core → app/services (9 files, RENAME some)
```bash
mv app/data_analysis_v3/core/formatters.py app/services/analysis_formatters.py  # RENAME
mv app/data_analysis_v3/core/metadata_cache.py app/services/
mv app/data_analysis_v3/core/lazy_loader.py app/services/
mv app/data_analysis_v3/core/data_profiler.py app/services/
mv app/data_analysis_v3/core/data_validator.py app/services/
mv app/data_analysis_v3/core/column_validator.py app/services/
mv app/data_analysis_v3/core/encoding_handler.py app/services/
mv app/data_analysis_v3/core/data_exploration_agent.py app/agent/
```

### app/data_analysis_v3/core → app/utils (2 files)
```bash
mv app/data_analysis_v3/core/scope_utils.py app/utils/
mv app/data_analysis_v3/core/analytics_helpers.py app/utils/
```

### app/data_analysis_v3/tools → app/visualization (1 file)
```bash
mv app/data_analysis_v3/tools/map_tools.py app/visualization/
```

### app/data_analysis_v3/tools → app/agent (1 file)
```bash
mv app/data_analysis_v3/tools/python_tool.py app/agent/
```

### app/data_analysis_v3/tools → app/tpr (2 files, RENAME one)
```bash
mv app/data_analysis_v3/tools/tpr_analysis_tool.py app/tpr/analysis_tool.py  # RENAME
mv app/data_analysis_v3/tools/tpr_workflow_langgraph_tool.py app/tpr/
```

### app/data_analysis_v3/prompts → app/agent/prompts (2 files)
```bash
mv app/data_analysis_v3/prompts/__init__.py app/agent/prompts/
mv app/data_analysis_v3/prompts/system_prompt.py app/agent/prompts/
```

### app/data_analysis_v3/tools → app/agent/tools (1 file)
```bash
mv app/data_analysis_v3/tools/__init__.py app/agent/tools/
```

### app/data_analysis_v3/tpr → app/tpr (2 files)
```bash
mv app/data_analysis_v3/tpr/__init__.py app/tpr/__init__.py
mv app/data_analysis_v3/tpr/data_analyzer.py app/tpr/
mv app/data_analysis_v3/tpr/workflow_manager.py app/tpr/
```

### Clean up app/data_analysis_v3
```bash
rm -rf app/data_analysis_v3/
```

---

## PHASE 6: DISTRIBUTE app/helpers/ (5 files → 2 destinations)

### app/helpers → app/agent (2 files)
```bash
mv app/helpers/tool_discovery_helper.py app/agent/
mv app/helpers/workflow_progress_helper.py app/agent/
```

### app/helpers → app/utils (3 files)
```bash
mv app/helpers/welcome_helper.py app/utils/
mv app/helpers/data_requirements_helper.py app/utils/
mv app/helpers/error_recovery_helper.py app/utils/
```

### Clean up app/helpers
```bash
rm -rf app/helpers/
```

---

## PHASE 7: DISTRIBUTE app/interaction/ (5 files → 1 destination + DELETE)

### app/interaction → app/services (4 files, RENAME all)
```bash
mv app/interaction/core.py app/services/interaction_core.py  # RENAME
mv app/interaction/events.py app/services/interaction_events.py  # RENAME
mv app/interaction/storage.py app/services/interaction_storage.py  # RENAME
mv app/interaction/utils.py app/services/interaction_utils.py  # RENAME
```

### Clean up app/interaction
```bash
rm -rf app/interaction/
```

---

## PHASE 8: DISTRIBUTE app/routing/ (1 file → 1 destination)

### app/routing → app/agent (1 file)
```bash
mv app/routing/semantic_router.py app/agent/
```

### Clean up app/routing
```bash
rm -rf app/routing/
```

---

## PHASE 9: DISTRIBUTE app/runtime/ (3 files → 2 destinations)

### Create upload directory
```bash
mkdir -p app/upload
```

### app/runtime → app/upload (1 file)
```bash
mv app/runtime/upload_service.py app/upload/
```

### app/runtime → app/agent (1 file, RENAME)
```bash
mv app/runtime/standard/workflow.py app/agent/standard_workflow.py  # RENAME
```

### Clean up app/runtime
```bash
rm -rf app/runtime/
```

---

## PHASE 10: REDISTRIBUTE app/services/ (existing → reorganize + new destinations)

### app/services → app/conversation (1 file, RENAME)
```bash
mv app/services/conversation_history.py app/conversation/history.py  # RENAME
```

### app/services → app/visualization (5 files)
```bash
mkdir -p app/visualization
mv app/services/agents/visualizations/composite_visualizations.py app/visualization/
mv app/services/agents/visualizations/core_utils.py app/visualization/
mv app/services/agents/visualizations/pca_visualizations.py app/visualization/
mv app/services/agents/visualizations/tpr_visualization_service.py app/visualization/
mv app/services/universal_viz_explainer.py app/visualization/viz_explainer.py  # RENAME
```

### app/services → app/planning (1 file, RENAME)
```bash
mkdir -p app/planning
mv app/services/reports/modern_generator.py app/planning/report_generator.py  # RENAME
```

### Keep in app/services (stays)
```
- __init__.py
- container.py
- memory_service.py
- query_result.py
- response_formatter.py
- session_memory.py
- shapefile_fetcher.py
- variable_resolution_service.py
Plus new ones from phase 3, 4, 5, 7
```

### Clean up app/services subdirectories
```bash
rm -rf app/services/agents/
rm -rf app/services/reports/
rm -rf app/services/visualization/
```

---

## PHASE 11: DISTRIBUTE app/tools/ (15 files → 4 destinations)

### Create directories
```bash
mkdir -p app/planning
mkdir -p app/visualization
mkdir -p app/utils
mkdir -p app/agent/tools
```

### app/tools → app/visualization (5 files, RENAME some)
```bash
mv app/tools/visualization_maps_tools.py app/visualization/maps_tools.py  # RENAME
mv app/tools/settlement_visualization_tools.py app/visualization/settlement_tools.py  # RENAME
mv app/tools/settlement_intervention_tools.py app/visualization/
mv app/tools/variable_distribution.py app/visualization/variable_distribution_charts.py  # RENAME
```

### app/tools → app/planning (2 files)
```bash
mv app/tools/itn_planning_tools.py app/planning/
mv app/tools/export_tools.py app/planning/
```

### app/tools → app/agent/tools (2 files, RENAME one)
```bash
mv app/tools/base.py app/agent/tools/
mv app/tools/complete_analysis_tools.py app/agent/tools/
mv app/tools/tpr_query_tool.py app/agent/tools/tpr_query_tools.py  # RENAME
```

### app/tools → app/utils (5 files)
```bash
mv app/tools/custom_analysis_parser.py app/utils/
mv app/tools/chatmrpt_help_tool.py app/utils/
mv app/tools/methodology_explanation_tools.py app/utils/
mv app/tools/settlement_validation_tools.py app/utils/
mv app/tools/data_description_tools.py app/utils/
```

### Clean up app/tools
```bash
rm -rf app/tools/
```

---

## PHASE 12: DISTRIBUTE app/utils/ (already mostly stays, 1 file moves)

### app/utils → app/visualization (1 file)
```bash
mv app/utils/visualization_controls.py app/visualization/
```

### Keep in app/utils (stays)
```
- geospatial_levels.py
- lga_boundaries.py
- map_overlays.py
- security.py
Plus new ones from previous phases
```

---

## PHASE 13: REDISTRIBUTE ALL app/web/routes/ → app/api/ (24 files + subdirs)

### Create api directory structure
```bash
mkdir -p app/api
mkdir -p app/api/analysis
```

### Main routes → app/api (13 files, RENAME some)
```bash
mv app/web/routes/analysis_routes.py app/api/
mv app/web/routes/api_routes.py app/api/
mv app/web/routes/arena_routes.py app/api/
mv app/web/routes/compatibility.py app/api/compatibility_routes.py  # RENAME
mv app/web/routes/conversation_routes.py app/api/
mv app/web/routes/core_routes.py app/api/
mv app/web/routes/data_analysis_v3_routes.py app/api/
mv app/web/routes/debug_routes.py app/api/
mv app/web/routes/export_routes.py app/api/
mv app/web/routes/itn_routes.py app/api/
mv app/web/routes/reports_api_routes.py app/api/
mv app/web/routes/session_routes.py app/api/
mv app/web/routes/upload_routes.py app/api/
mv app/web/routes/visualization_routes.py app/api/
mv app/web/routes/__init__.py app/api/__init__.py
```

### Analysis sub-routes → app/api/analysis/ (9 files, RENAME some)
```bash
mv app/web/routes/analysis/__init__.py app/api/analysis/
mv app/web/routes/analysis/analysis_chat.py app/api/analysis/chat.py  # RENAME
mv app/web/routes/analysis/analysis_exec.py app/api/analysis/exec.py  # RENAME
mv app/web/routes/analysis/analysis_vote.py app/api/analysis/vote.py  # RENAME
mv app/web/routes/analysis/arena_helpers.py app/api/analysis/
mv app/web/routes/analysis/chat_routing.py app/api/analysis/
mv app/web/routes/analysis/chat_stream_service.py app/api/analysis/
mv app/web/routes/analysis/chat_sync_service.py app/api/analysis/
mv app/web/routes/analysis/utils.py app/api/analysis/
```

### Admin routes → app/api (1 file, RENAME)
```bash
mv app/web/admin.py app/api/admin_routes.py  # RENAME
```

### app/routes.py → app/api (1 file)
```bash
mv app/routes.py app/api/routes.py
```

### PrePost routes → app/api (1 file, RENAME)
```bash
mv app/prepost/routes.py app/api/prepost_routes.py  # RENAME
```

### Survey routes → app/api (1 file, RENAME)
```bash
mv app/survey/routes.py app/api/survey_routes.py  # RENAME
```

### Clean up app/web
```bash
rm -rf app/web/
```

---

## SUMMARY OF PHASES

| Phase | Action | Files |
|-------|--------|-------|
| 1 | No moves (stays in place) | 54 |
| 2 | Simple move app/analysis → app/planning | 1 |
| 3 | Distribute app/core → 6 destinations | 28 |
| 4 | Distribute app/data → 3 destinations | 11 |
| 5 | Distribute app/data_analysis_v3 → 5 destinations | ~30 |
| 6 | Distribute app/helpers → 2 destinations | 5 |
| 7 | Distribute app/interaction → app/services | 5 |
| 8 | Distribute app/routing → app/agent | 1 |
| 9 | Distribute app/runtime → 2 destinations | 3 |
| 10 | Reorganize app/services → new destinations | 7 |
| 11 | Distribute app/tools → 4 destinations | 15 |
| 12 | Move app/utils file → app/visualization | 1 |
| 13 | Distribute app/web/routes → app/api | 24 |
| **TOTAL** | | **185** |

---

## POST-MIGRATION CLEANUP

After all moves:

1. Delete empty directories
2. Create missing __init__.py files in new directories
3. Update all imports in moved files (use refactoring tool)
4. Run tests to verify no import breaks

