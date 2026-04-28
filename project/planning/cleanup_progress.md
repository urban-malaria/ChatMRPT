# Codebase Cleanup Progress

## Date: April 7, 2026

## What we accomplished today

### One-brain migration
- All 8 tools registered in V3 agent
- TPR exit removed — agent stays in V3 mode throughout
- Prototype validated, all tools tested locally
- dataAnalysisMode persists across page reloads

### Codebase restructuring
- Reorganized from 15+ confusing directories to 15 clean user-journey directories
- Eliminated: core/, data/, data_analysis_v3/, helpers/, interaction/, routing/,
  runtime/, web/, tools/, reports/, models/
- Created: agent/, api/, arena/, conversation/, planning/, tpr/, upload/, visualization/
- Fixed ~50 broken imports across the codebase
- Restored pipeline_stages/ (was incorrectly archived)

### Dead code removal
- Started: 231 files
- Current: 183 files
- Lines removed: ~6,600+ today, ~10,000+ total
- Key deletions: analysis_routes.py (1,835), settlement files (1,903),
  workflow_tool.py (1,137), help_tool.py (709), dead functions (1,048)

## What's left to do

### Files still over 1000 lines (need splitting)
1. services/dataset_builder.py (2,123) — largest file
2. analysis/complete_tools.py (1,401)
3. analysis/itn_pipeline.py (1,765)
4. visualization/composite.py (1,614)
5. planning/export_tools.py (1,495)
6. tpr/analysis_tool.py (1,491)
7. services/data_handler.py (1,251)
8. tpr/workflow_manager.py (1,152)
9. agent/agent.py (1,117)

### AWS deployment
- Task #38: Test ITN planning on AWS (needs www/wards_with_pop.csv)
- Task #39: Full regression test before merging to main

### Merge to main
- one-brain-migration branch has ALL changes
- Needs AWS regression test before merging
