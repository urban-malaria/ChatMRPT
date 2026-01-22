# ChatMRPT Codebase Dependency Map

## Overview

This document maps the file dependencies and request flow for the unified agent refactor.

---

## Two-Brain Architecture (Current State)

```
┌─────────────────────────────────────────────────────────────────────┐
│                           FRONTEND                                   │
│                                                                     │
│   Data Analysis Tab                        Standard Upload Tab      │
│         │                                        │                  │
│         ▼                                        ▼                  │
│   /api/v1/data-analysis/chat              /send_message             │
└─────────────┬─────────────────────────────────────┬─────────────────┘
              │                                     │
              ▼                                     ▼
┌─────────────────────────────┐     ┌─────────────────────────────────┐
│        BRAIN 1              │     │           BRAIN 2               │
│   DataAnalysisAgent         │     │      RequestInterpreter         │
│                             │     │                                 │
│   Tools:                    │     │   Tools:                        │
│   - analyze_data (Python)   │     │   - run_malaria_risk_analysis   │
│                             │     │   - create_vulnerability_map    │
│   Missing:                  │     │   - create_variable_distribution│
│   - analyze_tpr_data        │     │   - create_pca_map              │
│     (exists but NOT         │     │   - create_urban_extent_map     │
│      registered!)           │     │   - run_itn_planning            │
│                             │     │   - analyze_data_with_python    │
│                             │     │   - 6 more tools...             │
└─────────────┬───────────────┘     └─────────────────────────────────┘
              │
              │ ONE-WAY TRANSITION
              │ (exit_data_analysis_mode: true)
              │
              └──────────────────────────────────────┐
                                                     ▼
                                              Can't go back!
```

---

## File Dependency Map

### Entry Points (Routes)

| File | Endpoint | Uses |
|------|----------|------|
| `app/web/routes/data_analysis_v3_routes.py` | `/api/v1/data-analysis/chat` | Brain 1 |
| `app/web/routes/analysis_routes.py` | `/send_message` | Brain 2 |

### Brain 1 Files (Data Analysis V3)

```
app/data_analysis_v3/
├── core/
│   ├── agent.py                    # Main LangGraph agent
│   │   ├── imports: python_tool.analyze_data
│   │   ├── imports: prompts/system_prompt.py
│   │   ├── imports: state_manager.py
│   │   └── imports: encoding_handler.py
│   │
│   ├── state_manager.py            # Workflow state (file-based)
│   │   └── Key flags: workflow_transitioned, tpr_workflow_active
│   │
│   ├── tpr_workflow_handler.py     # TPR workflow logic
│   │   ├── imports: state_manager.py
│   │   ├── imports: encoding_handler.py
│   │   └── Contains: trigger_risk_analysis() ← TRANSITION POINT
│   │
│   └── encoding_handler.py         # CSV/Excel reading
│
├── tools/
│   ├── python_tool.py              # analyze_data tool (registered)
│   └── tpr_analysis_tool.py        # analyze_tpr_data (NOT registered!)
│
├── prompts/
│   └── system_prompt.py            # MAIN_SYSTEM_PROMPT, TPR_WORKFLOW_GUIDANCE
│
└── tpr/
    ├── workflow_manager.py         # TPRWorkflowHandler class
    └── data_analyzer.py            # TPRDataAnalyzer class
```

### Brain 2 Files (Request Interpreter)

```
app/core/
├── request_interpreter.py          # Main class with 13+ tools
│   ├── imports: tools/complete_analysis_tools.py
│   ├── imports: tools/visualization_maps_tools.py
│   ├── imports: tools/variable_distribution.py
│   └── imports: many more...
│
└── workflow_state_manager.py       # WorkflowStateManager

app/tools/
├── complete_analysis_tools.py      # RunMalariaRiskAnalysis (Pydantic)
├── visualization_maps_tools.py     # CreateVulnerabilityMap, etc.
├── variable_distribution.py        # VariableDistribution
└── itn_planning.py                 # ITN planning tool
```

---

## Request Flow: Data Analysis V3 Chat

```
1. POST /api/v1/data-analysis/chat
   └── data_analysis_v3_routes.py:471

2. Check: workflow_transitioned?
   └── If TRUE → return exit_data_analysis_mode: true (line 541-564)

3. Check: is_tpr_active?
   └── If TRUE → use TPRWorkflowHandler
   └── If FALSE → use DataAnalysisAgent

4. TPR Workflow Path:
   └── TPRWorkflowHandler.execute_command()
   └── On completion → trigger_risk_analysis()
       └── Sets workflow_transitioned: true
       └── Returns exit_data_analysis_mode: true

5. Agent Path:
   └── DataAnalysisAgent.analyze()
   └── Uses only analyze_data tool
```

---

## The Transition Mechanism (What We Want to Remove)

### Current Flow:
```
1. User completes TPR workflow
2. tpr_workflow_handler.py:1612 → trigger_risk_analysis()
3. Sets in state_manager:
   - workflow_transitioned: True
   - tpr_completed: True
4. Returns to frontend:
   - exit_data_analysis_mode: True
5. Frontend switches endpoint:
   - FROM: /api/v1/data-analysis/chat
   - TO: /send_message
6. User is now on Brain 2 (can't go back)
```

### Files Involved in Transition:
| File | Line | What Happens |
|------|------|--------------|
| `tpr_workflow_handler.py` | 1612-1734 | `trigger_risk_analysis()` method |
| `tpr_workflow_handler.py` | 1663 | Sets `workflow_transitioned: True` |
| `tpr_workflow_handler.py` | 1724 | Returns `exit_data_analysis_mode: True` |
| `data_analysis_v3_routes.py` | 541-564 | Checks `workflow_transitioned`, returns exit flag |
| `data_analysis_v3_routes.py` | 812-824 | Adds exit flag when stage is COMPLETE |

---

## Tools Comparison

### Brain 1 (DataAnalysisAgent) - Current:
| Tool | File | Status |
|------|------|--------|
| `analyze_data` | `python_tool.py` | ✅ Registered |
| `analyze_tpr_data` | `tpr_analysis_tool.py` | ❌ NOT registered |

### Brain 2 (RequestInterpreter) - Current:
| Tool | Purpose |
|------|---------|
| `run_malaria_risk_analysis` | Risk ranking |
| `create_vulnerability_map` | Risk classification maps |
| `create_pca_map` | PCA visualization |
| `create_variable_distribution` | Choropleth maps |
| `create_urban_extent_map` | Urban/rural patterns |
| `create_decision_tree` | Decision tree viz |
| `create_composite_score_maps` | Composite maps |
| `create_settlement_map` | Settlement viz |
| `show_settlement_statistics` | Settlement stats |
| `run_itn_planning` | ITN distribution |
| `analyze_data_with_python` | Flexible Python |
| `list_dataset_columns` | Column listing |
| `execute_sql_query` | SQL queries |
| `explain_analysis_methodology` | Explanations |

---

## Key Insight

**Brain 1's `analyze_data` tool** (from `python_tool.py`) and **Brain 2's tools** (from `app/tools/*.py`) use DIFFERENT patterns:

- Brain 1: LangGraph `@tool` decorator with `Annotated[dict, InjectedState]`
- Brain 2: Pydantic `BaseTool` class with `execute(session_id)` method

To unify, we need to either:
1. Wrap Brain 2 tools as LangGraph `@tool` functions
2. Or create a common interface

---

## What Needs to Change (Minimal)

### To add tools to Brain 1:

**File: `app/data_analysis_v3/core/agent.py`**
- Line 19: Change import
- Line 74: Change `self.tools = [analyze_data]` to include more tools

**But first we need:**
- Tool wrappers that convert Brain 2 Pydantic tools to LangGraph format

### To remove transition:

**File: `app/data_analysis_v3/core/tpr_workflow_handler.py`**
- Remove or modify `trigger_risk_analysis()` (line 1612-1734)
- Remove `exit_data_analysis_mode: True` returns

**File: `app/web/routes/data_analysis_v3_routes.py`**
- Remove transition check (line 541-564)
- Remove exit flag addition (line 812-824)

---

## Dependencies to Preserve

When modifying `agent.py`, we must preserve:
- Import: `from .state import DataAnalysisState`
- Import: `from .encoding_handler import EncodingHandler`
- Import: `from .formatters import ResponseFormatter`
- Memory service integration
- Graph building pattern
- Tool node pattern
- Session handling

When modifying routes, we must preserve:
- TPR workflow handling (the workflow itself is fine)
- State manager usage
- Visualization handling
- Error handling
- Interaction logging
