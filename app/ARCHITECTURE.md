# ChatMRPT App Architecture

## Directory Structure — What Each Directory Owns

```
app/
├── agent/          The ONE-BRAIN LangGraph agent
├── api/            ALL HTTP routes (Flask blueprints)
├── analysis/       Risk analysis pipeline (composite + PCA)
├── arena/          Arena mode (multi-model comparison)
├── auth/           Authentication (Google OAuth)
├── config/         Configuration (dev, prod, test, redis)
├── conversation/   Session state, workflow state, history
├── planning/       ITN distribution + exports
├── prepost/        Pre/post knowledge tests
├── services/       Shared infrastructure (LLM, data, memory)
├── survey/         Survey system
├── tpr/            TPR workflow (all of it)
├── upload/         File upload handling
├── utils/          Shared utilities (decorators, geo, tools)
├── visualization/  Map rendering + chart creation
```

## What Goes Where

### agent/ — The Brain
The LangGraph agent that handles all user queries after data upload.
- `agent.py` — Main agent class, LangGraph graph
- `exploration_agent.py` — Data exploration variant
- `executor.py`, `executor_simple.py` — Python code execution
- `interpreter.py` — Request interpreter (pre-upload routing)
- `prompt_builder.py` — System prompt construction
- `semantic_router.py` — Message classification
- `state_manager.py` — Agent workflow state
- `tools/` — @tool wrappers that call existing tool implementations

### api/ — HTTP Routes
Every Flask blueprint lives here. Nothing else.
- `core_routes.py` — Index, session management
- `data_analysis_routes.py` — V3 chat endpoint (the main one)
- `analysis_routes.py` — Legacy analysis routes (being phased out)
- `visualization_routes.py` — Viz serving
- `upload_routes.py` — File upload
- `analysis/` — Chat streaming, routing, arena helpers

### analysis/ — Risk Analysis Pipeline
The composite scoring + PCA analysis pipeline.
- `complete_tools.py` — Main analysis tool (entry point)
- `engine.py` — Analysis orchestrator
- `pipeline.py` — Composite pipeline stages
- `pca_pipeline.py` — PCA analysis
- `scoring.py`, `normalization.py`, `imputation.py` — Pipeline stages
- `itn_pipeline.py` — ITN allocation algorithm
- `pipeline_stages/` — Stage function implementations

### tpr/ — TPR Workflow
Everything related to the Test Positivity Rate workflow.
- `workflow_manager.py` — Main workflow handler
- `workflow_tool.py` — LangGraph tool integration
- `data_analyzer.py` — TPR data analysis
- `analysis_tool.py` — TPR analysis implementation
- `language.py`, `intent.py` — Natural language understanding
- `precompute.py`, `cache.py` — Background computation

### visualization/ — Maps & Charts
All map rendering and visual output.
- `composite.py` — Composite vulnerability map rendering
- `pca.py` — PCA map rendering
- `maps_tools.py` — Map tool implementations
- `variable_distribution.py` — Variable distribution maps
- `explainer.py` — LLM-powered visualization explanations
- `geo_utils.py` — GeoJSON utilities

### services/ — Shared Infrastructure
Services used across multiple modules.
- `container.py` — Dependency injection
- `llm_manager.py`, `llm_adapter.py` — LLM interface
- `session_memory.py`, `memory_service.py` — Conversation memory
- `data_handler.py` — DataHandler class (data loading)
- `dataset_builder.py` — Unified dataset creation
- `interaction_*.py` — Interaction logging (5 files)
- `variable_resolver.py` — Fuzzy variable matching
- `shapefile_fetcher.py` — Shapefile retrieval

### planning/ — Intervention Planning
ITN distribution and export functionality.
- `itn_tools.py` — ITN planning tool
- `export_tools.py` — File export
- `population_loader.py` — Ward population data
- `settlement_intervention.py` — Settlement-based targeting

### conversation/ — Session & State
Session state tracking and workflow management.
- `session_state.py` — Flask session helpers
- `workflow_state.py` — Workflow state manager
- `analysis_state.py` — Analysis state handler
- `session_helper.py` — Session utilities

### utils/ — Shared Utilities
Small, reusable functions and classes.
- `decorators.py` — Flask route decorators
- `exceptions.py` — Custom exceptions
- `tool_base.py` — Base classes for all tools
- `geospatial_levels.py`, `lga_boundaries.py`, `map_overlays.py` — Geo utilities
- `security.py` — Security helpers

## Rules for Adding New Code

1. **New tool?** → Add @tool wrapper in `agent/tools/`, implementation in the relevant domain directory
2. **New route?** → Add to `api/`
3. **New analysis method?** → Add to `analysis/`
4. **New visualization?** → Add to `visualization/`
5. **Shared utility?** → Add to `utils/` (if small) or `services/` (if stateful)
6. **Never** put business logic in `services/` — that's for infrastructure only

## Known Technical Debt

- 15 files over 1000 lines need splitting (tracked in codebase_cleanup_plan.md)
- services/ has 32 files — data-related files could be in their own sub-package
- analysis_routes.py (1835 lines) overlaps with data_analysis_routes.py (1023 lines)
- survey/ and prepost/ may have duplicate functionality
