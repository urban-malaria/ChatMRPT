# ChatMRPT Codebase Restructuring: Complete Mapping

## Overview

This folder contains complete mapping documentation for restructuring ChatMRPT from its current modular organization to a user-journey-driven architecture.

**Status:** READ-ONLY MAPPING (No files have been moved yet)  
**Total Files:** 185 Python modules  
**Files to Move:** 131  
**Files to Keep:** 54  

---

## Documents in This Folder

### 1. **MIGRATION_QUICK_START.txt** ⭐ START HERE
Quick reference for executives and project managers.
- 30-second summary
- File allocation breakdown
- Key decisions explained
- Success criteria checklist
- Time estimate (4-6 hours)

**Read this first to understand scope and plan.**

---

### 2. **MIGRATION_BY_SOURCE_DIRECTORY.md** ⭐ FOR EXECUTION
Step-by-step migration guide organized by source directory.

Contains 13 phases:
- Phase 1: No moves (54 files stay)
- Phase 2: analysis/ → planning/ (1 file)
- Phase 3: core/ → 6 destinations (28 files)
- Phase 4: data/ → 3 destinations (11 files)
- Phase 5: data_analysis_v3/ → 5 destinations (~30 files)
- Phase 6: helpers/ → 2 destinations (5 files)
- Phase 7: interaction/ → services/ (5 files)
- Phase 8: routing/ → agent/ (1 file)
- Phase 9: runtime/ → 2 destinations (3 files)
- Phase 10: services/ → redistribute (7 files)
- Phase 11: tools/ → 4 destinations (15 files)
- Phase 12: utils/ → visualization/ (1 file)
- Phase 13: web/routes/ → api/ (24 files)

**Each phase includes exact bash commands to execute.**

---

### 3. **COMPLETE_FILE_MAPPING.md** ⭐ FOR REFERENCE
Comprehensive mapping with detailed reasoning.

Organized by:
- Current directory (app/__init__.py, app/analysis/, app/auth/, etc.)
- New location for each file
- Rename if applicable
- Reason for placement

Includes summary statistics by destination directory.

---

### 4. **FILE_MIGRATION_CSV.csv** ⭐ FOR TOOLS
Machine-readable CSV format for IDE refactoring tools.

Columns:
- `CURRENT_PATH` — Where file is now
- `NEW_PATH` — Where file should go
- `RENAME` — If filename changes
- `NOTES` — Why it moves

Can be imported into:
- VS Code (multi-file refactoring plugins)
- PyCharm (refactoring tools)
- Shell scripts
- Custom tooling

---

### 5. **MAPPING_SUMMARY.txt** (This folder)
High-level summary with file counts by directory.

---

## New Directory Structure (After Migration)

```
app/
├── __init__.py                    (1 file)
├── analysis/       (14 files)    Core risk analysis pipelines
├── agent/          (25 files)    ONE-BRAIN LangGraph orchestration
├── api/            (35 files)    All Flask blueprint routes
├── api/analysis/   (9 files)     Analysis-specific sub-routes
├── arena/          (2 files)     Arena mode (model comparison)
├── auth/           (6 files)     Google OAuth, authentication
├── config/         (9 files)     Configuration management
├── conversation/   (5 files)     Conversation history, state, resume
├── planning/       (8 files)     ITN distribution planning
├── prepost/        (3 files)     Pre/post test models & questions
├── services/       (25 files)    Shared infrastructure (LLM, memory, data)
├── survey/         (4 files)     Survey system models & questions
├── tpr/            (12 files)    TPR workflow (intent, language interface)
├── upload/         (1 file)      File upload handling
├── utils/          (19 files)    Shared utilities & helpers
└── visualization/  (12 files)    Maps, charts, visual explanations

DELETED:
✗ app/core/           (28 files distributed)
✗ app/data/           (11 files distributed)
✗ app/data_analysis_v3/  (~30 files distributed)
✗ app/helpers/        (5 files distributed)
✗ app/interaction/    (5 files distributed)
✗ app/routing/        (1 file distributed)
✗ app/runtime/        (3 files distributed)
✗ app/web/            (24+ files distributed)
```

---

## Key Design Decisions

### 1. User Journey Driven
Structure mirrors the user's path:
1. **auth/** — Sign in (Google OAuth)
2. **upload/** — Upload data
3. **tpr/** — TPR workflow & analysis
4. **agent/** — The ONE-BRAIN orchestrator
5. **analysis/** — Risk analysis pipelines
6. **visualization/** — Maps & charts
7. **planning/** — ITN distribution
8. **conversation/** — History & resume

### 2. ONE-BRAIN Agent (app/agent/)
Single LangGraph agent orchestrates all analysis:
- `request_interpreter.py` — Intent classification & tool selection
- `executor.py` — Secure Python code execution
- `state_manager.py` — Agent state tracking
- `tools/` — All analysis tools (base, complete_analysis, etc.)

### 3. TPR Isolation (app/tpr/)
TPR workflow is separated for clarity:
- Intent classification (`intent_classifier.py`)
- Language interface (`language_interface.py`)
- Workflow management (`workflow_manager.py`)
- Data analysis (`data_analyzer.py`)

### 4. Shared Services (app/services/)
All infrastructure in one place:
- LLM: `llm_manager.py`, `llm_adapter.py`
- Memory: `memory_service.py`, `session_memory.py`
- State: `session_state.py`, `unified_data_state.py`
- Data: `data_loaders.py`, `settlement_loader.py`, `unified_dataset_builder.py`
- Analysis: `analysis_formatters.py`, `data_profiler.py`, `metadata_cache.py`

### 5. API Routes Consolidated (app/api/)
All Flask routes in one location:
- Main routes in `app/api/`
- Analysis routes in `app/api/analysis/`
- All files suffixed with `_routes.py`

---

## How to Use This Mapping

### For Planners:
1. Read **MIGRATION_QUICK_START.txt**
2. Review success criteria
3. Estimate timeline (4-6 hours + testing)

### For Developers:
1. Read **MIGRATION_QUICK_START.txt**
2. Study **MIGRATION_BY_SOURCE_DIRECTORY.md**
3. Create feature branch: `git checkout -b refactor/restructure-directories`
4. Execute phases 2-13 from the guide
5. Update imports (largest task)
6. Run tests

### For DevOps/Review:
1. Use **FILE_MIGRATION_CSV.csv** for tracking
2. Verify all files accounted for
3. Check test coverage
4. Deploy to staging first

### For Refactoring Tools:
1. Import **FILE_MIGRATION_CSV.csv** into IDE
2. Use bulk refactoring features (VS Code, PyCharm)
3. Review and commit changes
4. Run test suite

---

## Execution Timeline

| Phase | Description | Files | Est. Time |
|-------|-------------|-------|-----------|
| Setup | Read docs, create feature branch | - | 30 min |
| Execution | Run migration phases 2-13 | 131 | 30 min |
| Imports | Update all import statements | 185 | 2-3 hrs |
| Tests | Unit + integration + manual | - | 1-2 hrs |
| Docs | Update guides & readmes | - | 30 min |
| **TOTAL** | | | **4-6 hrs** |

---

## Success Criteria

**File Migration:**
- [ ] All 185 files in new locations
- [ ] No duplicate files
- [ ] All old directories deleted
- [ ] All __init__.py files present

**Code Quality:**
- [ ] All imports resolve (no import errors)
- [ ] pytest passes: `python -m pytest tests/ -v`
- [ ] All Flask routes accessible
- [ ] No circular dependencies

**Functionality:**
- [ ] Agent orchestration works
- [ ] TPR workflow functional
- [ ] Arena mode operational
- [ ] File uploads work
- [ ] Visualization rendering works
- [ ] Survey system works
- [ ] Pre/post tests work

**Deployment:**
- [ ] Staging deployment successful
- [ ] Production deployment successful (both instances)
- [ ] Health checks pass
- [ ] End-to-end workflows tested

---

## Important Notes

### Import Updates
This is the largest effort. Every moved file needs imports updated.

Example migrations:
```python
# OLD → NEW
from app.core.llm_manager import ... → from app.services.llm_manager import ...
from app.tools.base import ... → from app.agent.tools.base import ...
from app.web.routes.core_routes import ... → from app.api.core_routes import ...
from app.data_analysis_v3.core.agent import ... → from app.agent.data_analysis_agent import ...
```

### File Renames
~40 files get renamed. Examples:
- `app/core/utils.py` → `app/utils/core_utils.py`
- `app/data_analysis_v3/core/agent.py` → `app/agent/data_analysis_agent.py`
- `app/tools/tpr_query_tool.py` → `app/agent/tools/tpr_query_tools.py`

### Git Strategy
1. Create feature branch first
2. Execute migration on feature branch
3. Test thoroughly before merging
4. Deploy to staging
5. Deploy to production (both instances)

---

## Rollback

If issues arise:
```bash
git reset --hard origin/main    # Discard all changes
git revert <commit-hash>        # Revert if committed
```

---

## Questions?

Refer to these documents in order:
1. **MIGRATION_QUICK_START.txt** — Overview & decisions
2. **MIGRATION_BY_SOURCE_DIRECTORY.md** — Execution phases
3. **COMPLETE_FILE_MAPPING.md** — Full reference
4. **FILE_MIGRATION_CSV.csv** — Machine-readable format

---

## Version Info

- **Created:** 2026-04-02
- **Total Files Mapped:** 185
- **Phases:** 13
- **New Directories:** 9
- **Deleted Directories:** 8
- **Status:** Ready for execution

---
