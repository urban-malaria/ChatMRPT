# Complete File Migration Mapping for ChatMRPT Restructuring

## Format
```
CURRENT_LOCATION → NEW_LOCATION [RENAME: newname.py if different]
```

---

## app/__init__.py
```
app/__init__.py → app/__init__.py
```
*Reason:* Factory function, stays at root

---

## app/analysis/ (15 files → analysis/)
```
app/analysis/__init__.py → app/analysis/__init__.py
app/analysis/engine.py → app/analysis/engine.py
app/analysis/imputation.py → app/analysis/imputation.py
app/analysis/itn_pipeline.py → app/planning/itn_pipeline.py [MOVE to planning]
app/analysis/metadata.py → app/analysis/metadata.py
app/analysis/normalization.py → app/analysis/normalization.py
app/analysis/pca_pipeline.py → app/analysis/pca_pipeline.py
app/analysis/pca_statistical_tests.py → app/analysis/pca_statistical_tests.py
app/analysis/pipeline.py → app/analysis/pipeline.py
app/analysis/pipeline_stages/__init__.py → app/analysis/pipeline_stages/__init__.py
app/analysis/region_aware_selection.py → app/analysis/region_aware_selection.py
app/analysis/scoring.py → app/analysis/scoring.py
app/analysis/urban_analysis.py → app/analysis/urban_analysis.py
app/analysis/utils.py → app/analysis/utils.py
app/analysis/variable_comparison_validator.py → app/analysis/variable_comparison_validator.py
app/analysis/variable_selection_coordinator.py → app/analysis/variable_selection_coordinator.py
```
*Reason:* Core analysis pipelines stay; ITN planning moves to planning/

---

## app/auth/ (5 files → auth/)
```
app/auth/__init__.py → app/auth/__init__.py
app/auth/auth_complete.py → app/auth/auth_complete.py
app/auth/decorators.py → app/auth/decorators.py
app/auth/google_auth.py → app/auth/google_auth.py
app/auth/session_utils.py → app/auth/session_utils.py
app/auth/user_model.py → app/auth/user_model.py
```
*Reason:* OAuth & session management, stays in auth/

---

## app/config/ (9 files → config/)
```
app/config/__init__.py → app/config/__init__.py
app/config/arena.py → app/config/arena.py
app/config/base.py → app/config/base.py
app/config/data_paths.py → app/config/data_paths.py
app/config/development.py → app/config/development.py
app/config/production.py → app/config/production.py
app/config/redis_config.py → app/config/redis_config.py
app/config/settings.py → app/config/settings.py
app/config/testing.py → app/config/testing.py
```
*Reason:* Configuration stays in place

---

## app/core/ (28 files → SPLIT across services/, agent/, conversation/, tpr/, api/)

### Core → services/ (Infrastructure/LLM/Memory)
```
app/core/llm_manager.py → app/services/llm_manager.py
app/core/llm_adapter.py → app/services/llm_adapter.py
app/core/session_state.py → app/services/session_state.py
app/core/session_context_service.py → app/services/session_context_service.py
app/core/session_helper.py → app/services/session_helper.py
app/core/data_repository.py → app/services/data_repository.py
app/core/unified_data_state.py → app/services/unified_data_state.py
app/core/instance_sync.py → app/services/instance_sync.py
app/core/responses.py → app/services/responses.py
app/core/utils.py → app/utils/core_utils.py
```

### Core → agent/ (Orchestration/Execution)
```
app/core/request_interpreter.py → app/agent/request_interpreter.py
app/core/prompt_builder.py → app/agent/prompt_builder.py
app/core/tool_schema_registry.py → app/agent/tool_schema_registry.py
app/core/tool_validator.py → app/agent/tool_validator.py
```

### Core → conversation/ (History/Resume)
```
app/core/redis_state_manager.py → app/conversation/redis_state_manager.py
app/core/workflow_state_manager.py → app/conversation/workflow_state_manager.py
app/core/analysis_state_handler.py → app/conversation/analysis_state_handler.py
```

### Core → arena/ (Arena-specific state)
```
app/core/arena_manager.py → app/arena/arena_manager.py
app/core/arena_system_prompt.py → app/arena/arena_system_prompt.py
```

### Core → tpr/ (TPR-specific)
```
app/core/tpr_precompute.py → app/tpr/tpr_precompute.py
app/core/tpr_precompute_service.py → app/tpr/tpr_precompute_service.py
app/core/tpr_utils.py → app/tpr/tpr_utils.py
app/core/tpr_ward_cache.py → app/tpr/tpr_ward_cache.py
app/core/variable_matcher.py → app/tpr/variable_matcher.py
```

### Core → utils/ (Shared utilities)
```
app/core/exceptions.py → app/utils/exceptions.py
app/core/decorators.py → app/utils/decorators.py
app/core/dependency_validator.py → app/utils/dependency_validator.py
```

---

## app/data/ (11 files → services/ OR analysis/ OR utils/)

### data/ → services/ (Data state & loading)
```
app/data/unified_dataset_builder.py → app/services/unified_dataset_builder.py
app/data/loaders.py → app/services/data_loaders.py
app/data/flexible_data_access.py → app/services/flexible_data_access.py
app/data/population_data/itn_population_loader.py → app/services/itn_population_loader.py
app/data/settlement_loader.py → app/services/settlement_loader.py
```

### data/ → analysis/ (Analysis-specific data utilities)
```
app/data/analysis.py → app/analysis/data_analysis_utils.py
app/data/utils.py → app/analysis/data_utils.py
```

### data/ → utils/ (Shared validation/processing)
```
app/data/__init__.py → app/utils/data_init.py
app/data/validation.py → app/utils/data_validation.py
app/data/processing.py → app/utils/data_processing.py
app/data/reporting.py → app/utils/data_reporting.py
```

---

## app/data_analysis_v3/ (30+ files → SPLIT across agent/, tpr/, api/)

### data_analysis_v3/core/ → agent/ (Core orchestration)
```
app/data_analysis_v3/core/agent.py → app/agent/data_analysis_agent.py
app/data_analysis_v3/core/executor.py → app/agent/executor.py
app/data_analysis_v3/core/executor_simple.py → app/agent/executor_simple.py
app/data_analysis_v3/core/state_manager.py → app/agent/state_manager.py
app/data_analysis_v3/core/state.py → app/agent/state.py
```

### data_analysis_v3/core/ → tpr/ (TPR-specific LLM interface)
```
app/data_analysis_v3/core/tpr_intent_classifier.py → app/tpr/intent_classifier.py
app/data_analysis_v3/core/tpr_language_interface.py → app/tpr/language_interface.py
```

### data_analysis_v3/core/ → services/ (Utilities for analysis)
```
app/data_analysis_v3/core/formatters.py → app/services/analysis_formatters.py
app/data_analysis_v3/core/metadata_cache.py → app/services/metadata_cache.py
app/data_analysis_v3/core/lazy_loader.py → app/services/lazy_loader.py
app/data_analysis_v3/core/data_profiler.py → app/services/data_profiler.py
app/data_analysis_v3/core/data_validator.py → app/services/data_validator.py
app/data_analysis_v3/core/column_validator.py → app/services/column_validator.py
app/data_analysis_v3/core/encoding_handler.py → app/services/encoding_handler.py
app/data_analysis_v3/core/scope_utils.py → app/utils/scope_utils.py
app/data_analysis_v3/core/analytics_helpers.py → app/utils/analytics_helpers.py
```

### data_analysis_v3/tools/ → visualization/ (Map & chart tools)
```
app/data_analysis_v3/tools/map_tools.py → app/visualization/map_tools.py
```

### data_analysis_v3/tools/ → agent/ (Core analysis tools)
```
app/data_analysis_v3/tools/python_tool.py → app/agent/python_tool.py
app/data_analysis_v3/tools/tpr_analysis_tool.py → app/tpr/analysis_tool.py
app/data_analysis_v3/tools/tpr_workflow_langgraph_tool.py → app/tpr/workflow_langgraph_tool.py
```

### data_analysis_v3/tpr/ → tpr/ (TPR workflow)
```
app/data_analysis_v3/tpr/data_analyzer.py → app/tpr/data_analyzer.py
app/data_analysis_v3/tpr/workflow_manager.py → app/tpr/workflow_manager.py
```

### data_analysis_v3/prompts/ → agent/prompts/ (Agent prompts)
```
app/data_analysis_v3/prompts/__init__.py → app/agent/prompts/__init__.py
app/data_analysis_v3/prompts/system_prompt.py → app/agent/prompts/system_prompt.py
```

### data_analysis_v3/__init__.py & __init__.py
```
app/data_analysis_v3/__init__.py → DELETE (distributed across new modules)
app/data_analysis_v3/core/__init__.py → DELETE
app/data_analysis_v3/formatters/__init__.py → app/services/__init__.py (integrate)
app/data_analysis_v3/tools/__init__.py → app/agent/tools/__init__.py
app/data_analysis_v3/tpr/__init__.py → app/tpr/__init__.py
```

### data_analysis_v3/core/data_exploration_agent.py
```
app/data_analysis_v3/core/data_exploration_agent.py → app/agent/data_exploration_agent.py
```

---

## app/helpers/ (5 files → SPLIT across agent/ & utils/)

### helpers/ → agent/ (Agent-specific helpers)
```
app/helpers/tool_discovery_helper.py → app/agent/tool_discovery_helper.py
app/helpers/workflow_progress_helper.py → app/agent/workflow_progress_helper.py
```

### helpers/ → utils/ (General utilities)
```
app/helpers/__init__.py → DELETE
app/helpers/welcome_helper.py → app/utils/welcome_helper.py
app/helpers/data_requirements_helper.py → app/utils/data_requirements_helper.py
app/helpers/error_recovery_helper.py → app/utils/error_recovery_helper.py
```

---

## app/interaction/ (5 files → services/ & conversation/)

### interaction/ → services/ (Event handling)
```
app/interaction/__init__.py → app/services/__init__.py (integrate)
app/interaction/core.py → app/services/interaction_core.py
app/interaction/events.py → app/services/interaction_events.py
app/interaction/storage.py → app/services/interaction_storage.py
app/interaction/utils.py → app/services/interaction_utils.py
```

---

## app/prepost/ (3 files → prepost/ KEEP AS-IS)
```
app/prepost/__init__.py → app/prepost/__init__.py
app/prepost/models.py → app/prepost/models.py
app/prepost/questions.py → app/prepost/questions.py
app/prepost/routes.py → app/api/prepost_routes.py
```
*Reason:* Pre/post test system, stays except routes move to api/

---

## app/reports/ (1 file)
```
app/reports/__init__.py → DELETE (integrate into services/reports/)
```

---

## app/routing/ (2 files → agent/ OR services/)

```
app/routing/__init__.py → DELETE
app/routing/semantic_router.py → app/agent/semantic_router.py
```
*Reason:* Semantic routing is agent-level orchestration

---

## app/runtime/ (3 files → upload/ & services/)

```
app/runtime/__init__.py → DELETE
app/runtime/upload_service.py → app/upload/upload_service.py
app/runtime/standard/__init__.py → DELETE
app/runtime/standard/workflow.py → app/agent/standard_workflow.py
```
*Reason:* Upload logic goes to upload/, workflow orchestration to agent/

---

## app/services/ (10 files → STAY with reorganization)

### Stays in services/
```
app/services/__init__.py → app/services/__init__.py
app/services/container.py → app/services/container.py
app/services/conversation_history.py → app/conversation/history.py
app/services/memory_service.py → app/services/memory_service.py
app/services/query_result.py → app/services/query_result.py
app/services/response_formatter.py → app/services/response_formatter.py
app/services/session_memory.py → app/services/session_memory.py
app/services/shapefile_fetcher.py → app/services/shapefile_fetcher.py
app/services/universal_viz_explainer.py → app/visualization/viz_explainer.py
app/services/variable_resolution_service.py → app/services/variable_resolution_service.py
```

### services/agents/ (visualization stuff) → visualization/
```
app/services/agents/__init__.py → DELETE
app/services/agents/visualizations/__init__.py → app/visualization/__init__.py (integrate)
app/services/agents/visualizations/composite_visualizations.py → app/visualization/composite_visualizations.py
app/services/agents/visualizations/core_utils.py → app/visualization/core_utils.py
app/services/agents/visualizations/pca_visualizations.py → app/visualization/pca_visualizations.py
app/services/agents/visualizations/tpr_visualization_service.py → app/visualization/tpr_visualization_service.py
```

### services/reports/ → planning/ OR DELETE
```
app/services/reports/__init__.py → DELETE
app/services/reports/modern_generator.py → app/planning/report_generator.py
```

### services/visualization/ → visualization/
```
app/services/visualization/__init__.py → app/visualization/__init__.py (integrate)
```

---

## app/survey/ (4 files → survey/ KEEP AS-IS)
```
app/survey/__init__.py → app/survey/__init__.py
app/survey/models.py → app/survey/models.py
app/survey/populate_questions.py → app/survey/populate_questions.py
app/survey/questions.py → app/survey/questions.py
app/survey/routes.py → app/api/survey_routes.py
```
*Reason:* Survey system stays; routes move to api/

---

## app/tools/ (15 files → SPLIT across visualization/, planning/, agent/)

### tools/ → visualization/ (Map & chart creation)
```
app/tools/__init__.py → DELETE (integrate into agent/tools/)
app/tools/base.py → app/agent/tools/base.py
app/tools/visualization_maps_tools.py → app/visualization/maps_tools.py
app/tools/settlement_visualization_tools.py → app/visualization/settlement_tools.py
app/tools/settlement_intervention_tools.py → app/visualization/settlement_intervention_tools.py
app/tools/variable_distribution.py → app/visualization/variable_distribution_charts.py
app/tools/visualization_charts/__init__.py → DELETE
```

### tools/ → planning/ (ITN distribution)
```
app/tools/itn_planning_tools.py → app/planning/itn_planning_tools.py
app/tools/export_tools.py → app/planning/export_tools.py
```

### tools/ → agent/ (Core analysis tools)
```
app/tools/complete_analysis_tools.py → app/agent/tools/complete_analysis_tools.py
app/tools/tpr_query_tool.py → app/agent/tools/tpr_query_tools.py
```

### tools/ → utils/ (Helpers)
```
app/tools/custom_analysis_parser.py → app/utils/custom_analysis_parser.py
app/tools/chatmrpt_help_tool.py → app/utils/chatmrpt_help_tool.py
app/tools/methodology_explanation_tools.py → app/utils/methodology_explanation_tools.py
app/tools/settlement_validation_tools.py → app/utils/settlement_validation_tools.py
app/tools/data_description_tools.py → app/utils/data_description_tools.py
```

---

## app/utils/ (5 files → STAY & EXPANDED)
```
app/utils/geospatial_levels.py → app/utils/geospatial_levels.py
app/utils/lga_boundaries.py → app/utils/lga_boundaries.py
app/utils/map_overlays.py → app/utils/map_overlays.py
app/utils/security.py → app/utils/security.py
app/utils/visualization_controls.py → app/visualization/visualization_controls.py
```

---

## app/web/ (all routes → api/)

### Routes → api/
```
app/web/__init__.py → DELETE (integrate into api/__init__.py)
app/web/admin.py → app/api/admin_routes.py
app/web/routes/__init__.py → app/api/__init__.py

app/web/routes/analysis_routes.py → app/api/analysis_routes.py
app/web/routes/arena_routes.py → app/api/arena_routes.py
app/web/routes/compatibility.py → app/api/compatibility_routes.py
app/web/routes/conversation_routes.py → app/api/conversation_routes.py
app/web/routes/core_routes.py → app/api/core_routes.py
app/web/routes/data_analysis_v3_routes.py → app/api/data_analysis_v3_routes.py
app/web/routes/debug_routes.py → app/api/debug_routes.py
app/web/routes/export_routes.py → app/api/export_routes.py
app/web/routes/itn_routes.py → app/api/itn_routes.py
app/web/routes/reports_api_routes.py → app/api/reports_api_routes.py
app/web/routes/session_routes.py → app/api/session_routes.py
app/web/routes/upload_routes.py → app/api/upload_routes.py
app/web/routes/visualization_routes.py → app/api/visualization_routes.py
app/web/routes/api_routes.py → app/api/api_routes.py
```

### Analysis sub-routes → api/analysis/
```
app/web/routes/analysis/__init__.py → app/api/analysis/__init__.py
app/web/routes/analysis/analysis_chat.py → app/api/analysis/chat.py
app/web/routes/analysis/analysis_exec.py → app/api/analysis/exec.py
app/web/routes/analysis/analysis_vote.py → app/api/analysis/vote.py
app/web/routes/analysis/arena_helpers.py → app/api/analysis/arena_helpers.py
app/web/routes/analysis/chat_routing.py → app/api/analysis/chat_routing.py
app/web/routes/analysis/chat_stream_service.py → app/api/analysis/chat_stream_service.py
app/web/routes/analysis/chat_sync_service.py → app/api/analysis/chat_sync_service.py
app/web/routes/analysis/utils.py → app/api/analysis/utils.py
```

---

## app/routes.py
```
app/routes.py → app/api/routes.py
```
*Reason:* Additional route definitions go to api/

---

## Summary Statistics

Total files: 185

By destination:
- app/analysis/ — 14 files (EXPANDED to include core analysis)
- app/agent/ — 25 files (NEW, orchestration & tools)
- app/api/ — 35 files (NEW, all Flask routes)
- app/arena/ — 2 files (NEW, extracted from core)
- app/auth/ — 6 files (EXISTING, unchanged)
- app/config/ — 9 files (EXISTING, unchanged)
- app/conversation/ — 5 files (NEW, extracted from core)
- app/planning/ — 8 files (NEW, ITN + planning)
- app/prepost/ — 3 files (EXISTING, routes move to api)
- app/services/ — 25 files (EXPANDED, infrastructure & data)
- app/survey/ — 4 files (EXISTING, routes move to api)
- app/tpr/ — 12 files (NEW, TPR workflow)
- app/upload/ — 1 file (NEW, upload handling)
- app/utils/ — 15 files (EXPANDED, shared utilities)
- app/visualization/ — 12 files (NEW, all rendering)

Deleted/Distributed:
- app/core/ (28 files distributed)
- app/data/ (11 files distributed)
- app/data_analysis_v3/ (~30 files distributed)
- app/helpers/ (5 files distributed)
- app/interaction/ (5 files distributed)
- app/routing/ (1 file distributed)
- app/runtime/ (3 files distributed)
- app/tools/ (15 files distributed)
- app/web/ (all routes redistributed to api/)
