# One-Brain Migration: Standard Mode → V3 Agent

## Status: PROTOTYPE VALIDATED — Approach A (@tool) proven, ready to implement

---

## Investigation Summary (Tasks 1-10)

### Task 1: create_variable_distribution (COMPLETE)
- **Library**: Plotly Choroplethmapbox (NOT Folium)
- **Data**: Reads raw_data.csv or unified_dataset.csv + raw_shapefile.zip
- **Dependencies**: 10 utility services (variable_resolver, geospatial_levels, lga_boundaries, etc.)
- **Key**: Fuzzy variable matching, ward/LGA level, auto-explanation via UniversalVizExplainer
- **Migration**: Plotly-based = compatible with agent's viz pipeline

### Task 2: run_malaria_risk_analysis (COMPLETE)
- **Core**: Dual-method (Composite + PCA) with region-aware variable selection
- **Creates**: unified_dataset.csv (49 columns) — master file for all downstream tools
- **Dependencies**: AnalysisEngine, pca_pipeline, UnifiedDatasetBuilder, DataHandler, spatial imputation
- **Marker**: Creates .analysis_complete flag unlocking visualization tools
- **Downstream**: 5+ tools require its output
- **Migration**: Heavy dependency tree — must be registered as standalone LangGraph tool

### Task 3: Map tools — all 6 (COMPLETE)
- **CRITICAL FINDING: ALL 6 ARE 100% PLOTLY — ZERO FOLIUM**
- CreateVulnerabilityMap: Choroplethmapbox, needs composite_score
- CreatePCAMap: Choroplethmapbox, needs pca_score
- CreateCompositeScoreMaps: Subplot multi-map, needs model scores
- CreateCompositeVulnerabilityMap: Same as vulnerability but explicit method
- CreateUrbanExtentMap: Urban/rural classification, only needs data_loaded
- CreateDecisionTree: Risk factor decision paths
- **Migration**: All compatible with agent's Plotly pipeline

### Task 4: run_itn_planning (COMPLETE)
- **Algorithm**: Two-tier (rural priority, urban surplus) net allocation
- **Creates**: Map HTML + CSV export + dashboard HTML + JSON results
- **Data**: Reads unified_dataset (rankings) + population data (wards_with_pop.csv)
- **Library**: Plotly for map
- **Export**: Download links via /export/download/ endpoint
- **Migration**: Self-contained, can be wrapped as LangGraph tool

### Task 5: query_data SQL layer (COMPLETE)
- **Engine**: DuckDB (not SQLite) for NL→SQL
- **Returns**: Text only, never visualizations
- **Replacement**: Agent's pandas can handle all computations, but loses NL→SQL convenience
- **Decision needed**: Keep as convenience tool or let agent handle via Python?

### Task 6: Settlement + TPR query tools (COMPLETE)
- **Settlement**: May be deprecated ("removed during streamlining" comment in code)
- **TPR query tools**: Optimization for pre-computed cache — unnecessary if staying in V3 mode
- **Migration**: Low priority, likely not needed

### Task 7: V3 Agent LangGraph architecture (COMPLETE)
- **Tool registration**: `self.tools = [analyze_data]` → `bind_tools(self.tools)`
- **Adding tools**: Define with @tool decorator, add to self.tools list, update viz handler
- **Viz pipeline**: Pickled Plotly figures → HTML → iframe
- **Folium support**: Can work alongside Plotly — detect .html files (Folium) vs .pkl (Plotly)
- **Key constraint**: Tools use InjectedState for graph_state access + session_id

### Task 8: TPR exit flow (COMPLETE)
- **Exit trigger**: Backend returns exit_data_analysis_mode: true when TPR completes
- **Frontend**: Sets dataAnalysisMode=false, switches to /send_message_streaming
- **State preserved**: TPR selections, column schema, chat history in .agent_state.json
- **If we DON'T exit**: All messages stay on /api/v1/data-analysis/chat — works for data queries but never triggers risk analysis transition
- **Fix needed**: Instead of exiting, make the V3 endpoint handle post-TPR tools

### Task 9: Data flow pipeline (COMPLETE)
- Upload → uploaded_data.csv
- TPR workflow → tpr_results.csv, tpr_time_series.csv, raw_data.csv, raw_shapefile.zip
- Risk analysis → unified_dataset.csv, .analysis_complete
- Each stage adds files, no files are deleted
- Agent loads files by priority (unified > raw > tpr > uploaded)

### Task 10: Frontend routing (COMPLETE)
- dataAnalysisMode=true → /api/v1/data-analysis/chat (JSON responses)
- dataAnalysisMode=false → /send_message_streaming (SSE streaming)
- Set true on data analysis upload, set false on exit_data_analysis_mode
- If never set false: all messages go to V3 endpoint — mostly works

---

## Key Findings for Migration

### Good news
1. **ALL visualization tools use Plotly** — zero Folium. Agent already supports Plotly.
2. **LangGraph supports multiple tools** — just add to self.tools list
3. **Data pipeline is file-based** — tools read from instance/uploads/ regardless of which system calls them
4. **Agent already has conversation memory** — loads from memory service
5. **Settlement tools likely deprecated** — one less thing to migrate

### Challenges
1. **run_malaria_risk_analysis** has heavy dependencies (AnalysisEngine, pca_pipeline, imputation, etc.)
2. **ITN planning** needs population data merge + export/download link generation
3. **Map tools** depend on ~10 utility services for geospatial operations
4. **NL→SQL convenience** (query_data) would be lost unless kept as tool
5. **Exit flow removal** requires backend V3 endpoint to handle ALL post-TPR operations
6. **Frontend streaming** — V3 endpoint returns JSON, standard returns SSE. Need to align.

### Architecture decisions needed
1. Register existing tools directly as LangGraph tools? Or rewrite?
2. Keep query_data for NL→SQL? Or let agent use pandas?
3. How to handle the export/download link generation for ITN?
4. How to handle the UniversalVizExplainer integration?
5. JSON vs SSE response format — standardize on which?
6. Phased migration or all-at-once?

---

## Implementation Plan (Updated After Prototype Validation)

### Phase 1: Register all standard mode tools as @tool wrappers ✅ STARTED
**Branch**: `one-brain-migration`

Wrap each tool following the `create_variable_map` pattern in `map_tools.py`:
- [x] `create_variable_map` (create_variable_distribution) — DONE, TESTED
- [ ] `run_risk_analysis` (run_malaria_risk_analysis) — heavy deps
- [ ] `create_vulnerability_map` — needs unified_dataset.csv
- [ ] `create_pca_map` — needs unified_dataset.csv
- [ ] `create_composite_score_maps` — needs unified_dataset.csv
- [ ] `create_urban_extent_map` — needs data_loaded
- [ ] `create_decision_tree` — needs unified_dataset.csv
- [ ] `plan_itn_distribution` (run_itn_planning) — needs analysis_complete
- [ ] Update system prompt to describe all available tools

### Phase 2: Disable TPR exit ✅ DONE (testing)
- [x] Disabled `exit_data_analysis_mode` in data_analysis_v3_routes.py
- [x] Persisted `dataAnalysisMode` in sessionStorage
- [ ] Handle `workflow_transitioned` flag properly (don't return early)
- [ ] Test full workflow: upload → TPR → map → risk analysis → ITN → follow-ups

### Phase 3: End-to-end testing
- [ ] All 19 standard mode tools callable from agent
- [ ] Follow-up questions work for all tool outputs
- [ ] Conversation history includes all tool results
- [ ] Visualizations display correctly for all map types
- [ ] Export/download links work from agent responses
- [ ] Compare agent quality vs standard mode for same questions

### Phase 4: Retire standard mode code
- [ ] Remove ToolIntentResolver from critical path
- [ ] Remove two-brain orchestrator
- [ ] Simplify request_interpreter.py
- [ ] Remove /send_message_streaming endpoint (or alias to V3)
- [ ] Clean up dead code (_interpret_raw_output, etc.)

### Phase 5: Align frontend
- [ ] Single endpoint throughout (always /api/v1/data-analysis/chat)
- [ ] Remove dataAnalysisMode toggle (always on)
- [ ] Simplify UploadModal (no mode switching needed)
- [ ] Update UI for one-brain experience

---

## Round 2 Investigation (Tasks 12-19)

### Task 12: Visualization rendering functions (COMPLETE)
- ALL rendering in `app/services/agents/visualizations/composite_visualizations.py` and `pca_visualizations.py`
- 5 functions: vulnerability_map, pca_map, composite_score_maps, urban_extent_map, decision_tree
- All use Plotly Choroplethmapbox (except decision_tree which is custom HTML)
- GeoJSON conversion via `create_geojson_from_gdf()` utility
- Color scales: Plasma (vulnerability), YlOrRd (composite scores), custom (PCA)
- Rank inversion for colorbar (rank 1 = highest risk at top)
- LGA-level rollup for hover info

### Task 13: Analysis engine + composite/PCA math (COMPLETE)
- **Composite scoring**: Mean of min-max normalized variables across all combinations
  - Variable relationships: direct (risk factor) vs inverse (protective)
  - Normalization: (x - min) / (max - min) for direct; 1/(x+eps) then normalize for inverse
  - All 2+ variable combinations generated → median across all models → rank
  - Categories: top 1/3 = High Risk, bottom 1/3 = Low Risk, rest = Medium
- **PCA**: StandardScaler → KMO test (≥0.5) + Bartlett's (p<0.05) → PCA extraction
  - Components: min(n_vars, max(2, 0.8 * n_vars))
  - Score: weighted sum of first 2-3 components by explained variance ratio
  - Ranking: descending by PCA score

### Task 14: Spatial imputation + unified dataset builder (COMPLETE)
- **Imputation**: Queen contiguity → spatial neighbor mean → global mean → mode → placeholder
  - Uses libpysal.weights.Queen for neighbor detection
  - Parallel processing via ThreadPoolExecutor
- **Unified dataset**: 49 columns merging raw data + composite + PCA + spatial metrics + comparison
  - Left join CSV to shapefile (preserves CSV ward count)
  - Duplicate ward names → "WardName (WardCode)" format
  - Saves as GeoParquet + CSV backup

### Task 15: Variable resolution + geospatial utilities (COMPLETE)
- **Variable resolver**: difflib SequenceMatcher + substring + token Jaccard + abbreviation
  - Threshold: 0.7 default, LRU cached with dataset fingerprinting
  - Batch processing for 3+ variables
- **Geospatial utils**: apply_lga_highlight, collect_lga_options, dissolve_to_lga
- **LGA boundaries**: Reference GeoPackage at app/reference_data/nga_lga_boundaries.gpkg
  - annotate_with_lga_names via spatial join (sjoin_nearest)
- **Map overlays**: LGA boundary lines, LGA averages, hover enhancement

### Task 16: ConversationalDataAccess + NL→SQL (COMPLETE)
- Full pipeline: NL → LLM generates SQL (temp=0.1) → DuckDB execution → intent analysis → formatted output
- Schema: comprehensive column analysis with malaria relevance scoring (0-10)
- Intent analyzer: COUNT, LIST, EXPLAIN, COMPARE, AGGREGATE, FILTER
- UnifiedFormatter: context-aware formatting with risk factor extraction + recommendations
- Code execution sandbox: safe globals, blocked imports, matplotlib capture
- **Migration note**: Agent's pandas handles computation, but NL→SQL convenience + formatted output are valuable

### Task 17: Export routes + download serving (COMPLETE)
- Two endpoints: /export/list/{session_id} (discovery) + /export/download/{session_id}/{filename} (serving)
- Scans instance/exports/ and instance/uploads/ for available files
- Path traversal prevention via basename + resolve + containment check
- MIME types: csv, html, zip, octet-stream
- ITN results stored in timestamped subdirectories
- Agent can generate download links via URL construction

### Task 18: Semantic router + chat routing (COMPLETE)
- 7 routes → 3 outcomes: needs_tools, can_answer, needs_clarification
- Embedding-based (OpenAI text-embedding-3-small) + context biasing + LLM fallback
- Confidence threshold: 0.55, margin threshold: 0.08
- **NOT affected by removing standard mode** — routing is session-context based
- Separate from ToolIntentResolver (coarse intent vs fine tool selection)
- SemanticRouter is a singleton in app/routing/semantic_router.py

### Task 19: _interpret_raw_output callback (COMPLETE)
- **REFERENCED BUT NOT IMPLEMENTED** — method doesn't exist on RequestInterpreter
- Would fail with AttributeError if execution path reaches it
- Intended: generate epidemiological interpretation of tool output
- Currently dead code — fallback path (_stream_with_tools) bypasses it
- **Migration**: Can be properly implemented in agent (agent already has LLM access)

---

## Complete Standard Mode Component Map

### Tools (19 total)
| # | Tool | File | Library | Data Required |
|---|------|------|---------|---------------|
| 1 | run_malaria_risk_analysis | complete_analysis_tools.py | Pandas/NumPy/sklearn | raw_data.csv + shapefile |
| 2 | create_vulnerability_map | visualization_maps_tools.py | Plotly | unified_dataset.csv |
| 3 | create_pca_map | visualization_maps_tools.py | Plotly | unified_dataset.csv |
| 4 | create_composite_score_maps | visualization_maps_tools.py | Plotly | unified_dataset.csv |
| 5 | create_composite_vulnerability_map | visualization_maps_tools.py | Plotly | unified_dataset.csv |
| 6 | create_urban_extent_map | visualization_maps_tools.py | Plotly | any loaded data |
| 7 | create_decision_tree | visualization_maps_tools.py | Custom HTML | unified_dataset.csv |
| 8 | create_variable_distribution | variable_distribution.py | Plotly | raw/unified + shapefile |
| 9 | run_itn_planning | itn_planning_tools.py | Plotly | unified_dataset + population |
| 10 | query_data | request_interpreter.py | DuckDB | any loaded data |
| 11 | analyze_data | request_interpreter.py | Python exec | any loaded data |
| 12 | explain_analysis_methodology | methodology_explanation_tools.py | None (text) | none |
| 13 | chatmrpt_help | chatmrpt_help_tool.py | None (text) | none |
| 14 | create_settlement_map | settlement_visualization_tools.py | Folium? | building footprints |
| 15 | show_settlement_statistics | settlement_visualization_tools.py | GeoPandas | building footprints |
| 16 | query_tpr_data | tpr_query_tool.py | Pandas | pre-computed cache |
| 17 | switch_tpr_combination | tpr_query_tool.py | Pandas | ward cache |
| 18 | compare_tpr_combinations | tpr_query_tool.py | Pandas | pre-computed cache |
| 19 | create_intervention_targeting_map | settlement_intervention_tools.py | Plotly | analysis data |

### Services (shared dependencies)
| Service | File | Used By |
|---------|------|---------|
| VariableResolver | variable_resolution_service.py | Tools 1-8, ToolIntentResolver |
| UniversalVizExplainer | universal_viz_explainer.py | Tools 2-8 (via request_interpreter) |
| ConversationalDataAccess | conversational_data_access.py | Tool 10 (query_data) |
| UnifiedFormatter | unified_formatter.py | Tool 10 (via ConversationalDataAccess) |
| AnalysisEngine | analysis/engine.py | Tool 1 |
| UnifiedDatasetBuilder | data/unified_dataset_builder.py | Tool 1 |
| ITNPopulationLoader | data/population_data/itn_population_loader.py | Tool 9 |
| SemanticRouter | routing/semantic_router.py | Chat routing (pre-tool selection) |
| ToolIntentResolver | core/tool_intent_resolver.py | Tool selection (post-routing) |
| LLMOrchestrator | core/llm_orchestrator.py | Function calling + tool execution |

### Utilities (shared)
| Utility | File | Used By |
|---------|------|---------|
| geospatial_levels | utils/geospatial_levels.py | Maps, variable distribution |
| lga_boundaries | utils/lga_boundaries.py | Maps, variable distribution |
| map_overlays | utils/map_overlays.py | Maps, variable distribution |
| visualization controls | utils/visualization_controls.py | Maps |
| imputation | analysis/imputation.py | Risk analysis |
| normalization | analysis/normalization.py | Risk analysis |
| scoring | analysis/scoring.py | Risk analysis |

### Broken/Dead Code
| Component | File | Status |
|-----------|------|--------|
| _interpret_raw_output | request_interpreter.py | Referenced but not implemented |
| Settlement tools | settlement_visualization_tools.py | "Removed during streamlining" comment |

---

---

## Prototype Results (April 2, 2026)

### What we tested
- Disabled `exit_data_analysis_mode` so user stays in V3 after TPR
- Registered `create_variable_map` as a LangGraph @tool in the agent
- Added `create_map()` helper to Python execution environment
- Persisted `dataAnalysisMode` in sessionStorage to survive page reloads
- Fixed visualization pipeline to handle HTML files alongside Plotly pickles

### Test: "map malaria burden distribution" after TPR
1. Request went to `/api/v1/data-analysis/chat` (V3 mode) ✅
2. Agent called `create_variable_map` @tool (Approach A) ✅
3. Map created and displayed with 1 visualization ✅
4. Map HTML served correctly ✅

### Test: "what does this map show?" (follow-up)
1. Request stayed on `/api/v1/data-analysis/chat` (V3 mode persisted) ✅
2. Agent EXPLAINED the map from conversation history ✅
3. Did NOT re-create the map (0 visualizations, text only) ✅
4. Response referenced the specific data shown ✅

### Decisions RESOLVED

| Decision | Answer | Evidence |
|----------|--------|----------|
| @tool vs Python helper? | **@tool wins** | Agent naturally chose it, correct params, proper viz |
| Stay in V3 after TPR? | **Yes, it works** | All requests stayed on V3 endpoint |
| Follow-ups work in V3? | **Yes** | Agent explains instead of re-creating |
| Viz pipeline handles HTML? | **Yes** | HTML maps served alongside Plotly pickles |
| Mode survives page reload? | **Yes** | sessionStorage persistence works |

### What the prototype proved
- The bridge layer approach works: wrap existing tools as @tool, existing code untouched
- No need to rewrite any tool logic
- The agent decides WHEN to call tools (no ToolIntentResolver needed)
- Follow-up questions work naturally via conversation history
- The two-brain orchestrator is not needed

---

## Key Decisions — Updated After Prototype

| # | Decision | Status | Answer |
|---|----------|--------|--------|
| 1 | Register existing tools as @tool wrappers? | **DECIDED** | Yes — bridge layer in `app/data_analysis_v3/tools/map_tools.py` |
| 2 | Keep semantic router? | Open | Probably yes — it's independent, handles greeting/knowledge routing |
| 3 | Keep query_data NL→SQL? | Open | Test if agent's pandas handles the same queries well enough |
| 4 | _interpret_raw_output? | **DECIDED** | Dead code, agent handles interpretation via conversation history |
| 5 | JSON vs SSE? | Open | V3 uses JSON, works fine. SSE only needed if we want streaming |
| 6 | Settlement tools? | **DECIDED** | Deprecated — skip |
| 7 | TPR query tools? | Open | May not need if staying in V3, but useful for pre-computed cache |
| 8 | Migration order? | **DECIDED** | See implementation plan below |

---

## Files that would be modified (estimated)

### Backend
- app/data_analysis_v3/core/agent.py — register new tools, update viz handler
- app/data_analysis_v3/tools/ — new tool wrapper files
- app/web/routes/data_analysis_v3_routes.py — remove exit logic, handle all requests
- app/web/routes/analysis/chat_stream_service.py — potentially simplified

### Frontend
- frontend/src/hooks/useMessageStreaming.ts — single endpoint
- frontend/src/stores/analysisStore.ts — simplify or remove
- frontend/src/components/Toolbar/Toolbar.tsx — simplify New Chat
- frontend/src/components/Modal/UploadModal.tsx — no mode switching

### Potentially retired
- app/core/llm_orchestrator.py — replaced by agent
- app/core/tool_intent_resolver.py — replaced by agent's tool selection
- app/core/request_interpreter.py — significantly simplified
- app/tools/tpr_query_tool.py — no longer needed
