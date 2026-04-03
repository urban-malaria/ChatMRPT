# ChatMRPT Target Directory Structure

## Design Principle
The directory structure should tell you what the app does by looking at it.
A new developer should understand the system in 5 minutes.

---

## Current (messy)
```
app/
  core/              28 files — LLM, arena, TPR, sessions, state, all mixed
  data_analysis_v3/  30 files — agent, TPR handler, tools, prompts, formatters
  services/          10 files — container, memory, viz explainer, mixed
  tools/             15 files — standard mode tools
  analysis/          15 files — risk analysis pipeline
  data/              10 files — data loading
  web/routes/        24 files — routes + services mixed
  config/            8 files
  auth/              6 files
  routing/           2 files
  interaction/       5 files
  runtime/           3 files
  helpers/           6 files
  utils/             5 files
  survey/            5 files
  prepost/           4 files
  15+ directories, names don't tell you what the app does
```

## Target (clean)
```
app/
  agent/             — The one-brain LangGraph agent
    tools/           — @tool wrappers (map_tools.py, etc.)
    prompts/         — System prompts
    executor/        — Code execution (executor_simple.py)

  tpr/               — TPR workflow (everything TPR in one place)
    workflow.py      — Workflow handler
    analyzer.py      — Data analysis
    precompute.py    — Background precomputation
    utils.py         — TPR utilities
    cache.py         — Ward cache
    language.py      — Natural language interface
    intent.py        — Intent classification

  analysis/          — Risk analysis pipeline (composite + PCA)
    engine.py        — Main orchestrator
    pipeline.py      — Composite scoring pipeline
    pca.py           — PCA analysis
    imputation.py    — Spatial imputation
    normalization.py — Variable normalization
    scoring.py       — Risk scoring
    itn.py           — ITN distribution planning
    variables.py     — Variable selection

  visualization/     — All map/chart rendering
    maps.py          — Choropleth map tools
    composite.py     — Composite visualization rendering
    pca.py           — PCA visualization rendering
    utils.py         — GeoJSON, overlays, boundaries
    explainer.py     — Universal viz explainer

  data/              — Data loading and dataset building
    handler.py       — DataHandler
    loader.py        — File loading
    builder.py       — Unified dataset builder
    validation.py    — Data validation
    processing.py    — Data processing

  api/               — All Flask routes
    routes.py        — Blueprint registration
    upload.py        — File upload endpoints
    chat.py          — V3 chat endpoint
    streaming.py     — Streaming chat (pre-upload)
    conversation.py  — Conversation history
    export.py        — File downloads
    visualization.py — Viz serving
    auth.py          — Auth routes
    arena.py         — Arena routes
    session.py       — Session management

  services/          — Shared services
    container.py     — Dependency injection
    memory.py        — Session memory
    history.py       — Conversation history
    llm.py           — LLM manager + adapter
    state.py         — Unified data state
    variables.py     — Variable resolution

  arena/             — Arena mode (model comparison)
    manager.py       — Battle management
    prompts.py       — Arena system prompts

  auth/              — Authentication (keep as-is, it's clean)

  survey/            — Survey system (keep as-is)
  prepost/           — Pre/post tests (keep as-is)

  config/            — Configuration (keep as-is, minus archived)

  utils/             — Shared utilities
    geospatial.py    — LGA boundaries, overlays, levels
    security.py      — Security utilities
```

## What this achieves
- **12 top-level directories** (down from 15+)
- **Each directory has a clear purpose** visible from the name
- **Related code lives together** (all TPR in tpr/, all viz in visualization/)
- **No more `data_analysis_v3`** — it's just `agent/`
- **No more `core/`** — that was a dumping ground, contents distributed properly
- **api/ replaces web/routes/** — clearer naming
- **A new developer reads the structure and understands the app**

## Migration risk
HIGH — this touches almost every import in the codebase.

## Migration approach
1. Create the new directories
2. Move files ONE DIRECTORY AT A TIME
3. After each directory, run `grep -r` to find all broken imports
4. Fix all imports
5. Verify with py_compile
6. Test locally
7. Commit
8. Next directory

## Order of migration (safest first)
1. Rename data_analysis_v3/ → agent/ (biggest clarity win)
2. Move TPR files from core/ to tpr/
3. Consolidate visualization code
4. Flatten web/routes/ to api/
5. Distribute core/ contents to proper homes
6. Clean up services/
