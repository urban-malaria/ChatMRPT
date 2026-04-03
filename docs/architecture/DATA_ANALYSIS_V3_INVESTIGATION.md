# ChatMRPT Data Analysis V3 Architecture Investigation

**Date**: January 2026
**Investigator**: Claude Code Analysis
**Status**: Complete

---

## Executive Summary

This document captures a comprehensive investigation into the ChatMRPT Data Analysis V3 architecture, specifically examining why users experience rigidity during data exploration and TPR (Test Positivity Rate) workflows.

### Key Findings

1. **The system has ONE flexible tool** (`analyze_data`) that can execute ANY Python code, but it's underutilized due to excessive middleware
2. **Specialized TPR tools exist but are NOT registered** with the agent - they're called directly by a rigid state machine
3. **Intent classification creates false routing** - exploratory questions are often misclassified as "selections"
4. **The solution is removal, not addition** - we need to remove barriers, not add more code

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Key Players](#2-key-players)
3. [Message Flow](#3-message-flow)
4. [Rigidity Points](#4-rigidity-points)
5. [Flexibility Points](#5-flexibility-points)
6. [Root Cause Analysis](#6-root-cause-analysis)
7. [Specialized Tools Analysis](#7-specialized-tools-analysis)
8. [Dependencies & Risks](#8-dependencies--risks)
9. [Proposed Solutions](#9-proposed-solutions)
10. [User Experience Comparison](#10-user-experience-comparison)

---

## 1. Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND                                 │
│                    (React Application)                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ROUTING LAYER                               │
│            data_analysis_v3_routes.py (Gatekeeper)              │
│                                                                  │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │ TPR Active? │───►│ Intent       │───►│ Route Decision   │   │
│  │   Check     │    │ Classifier   │    │                  │   │
│  └─────────────┘    └──────────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────────┐
│   TPR WORKFLOW HANDLER  │     │      DATA ANALYSIS AGENT        │
│                         │     │                                  │
│  - State Machine        │     │  - LangGraph + GPT-4o           │
│  - Stage Handlers       │     │  - Tool: analyze_data           │
│  - Direct Tool Calls    │     │  - Flexible Python Execution    │
└─────────────────────────┘     └─────────────────────────────────┘
              │                               │
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────────┐
│  SPECIALIZED TPR TOOLS  │     │      PYTHON EXECUTOR            │
│  (NOT Agent-Accessible) │     │      (SimpleExecutor)           │
│                         │     │                                  │
│  - analyze_tpr_data     │     │  - Runs ANY Python code         │
│  - tpr_workflow_step    │     │  - Has pandas, plotly, etc.     │
└─────────────────────────┘     └─────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| LLM | OpenAI GPT-4o | Natural language understanding, code generation |
| Agent Framework | LangGraph | Tool orchestration, state management |
| Tool Execution | SimpleExecutor | Python code execution in sandboxed environment |
| Web Framework | Flask | API endpoints, session management |
| State Persistence | File-based JSON | Session state, workflow progress |

---

## 2. Key Players

### 2.1 Entry Point: Data Analysis Routes

**File**: `app/web/routes/data_analysis_v3_routes.py`
**Endpoint**: `/api/v1/data-analysis/chat` (lines 471-914)

**Role**: Acts as the "gatekeeper" that decides how each message is processed.

**Key Logic**:
```python
# Line 586-588
is_tpr_active = state_manager.is_tpr_workflow_active()

if is_tpr_active:
    # Route to TPR workflow handler
    # Uses intent classification to decide selection vs question
else:
    # Check for TPR start triggers
    # Otherwise route to general agent
```

### 2.2 The Agent (Brain)

**File**: `app/data_analysis_v3/core/agent.py`
**Class**: `DataAnalysisAgent`

**Key Characteristics**:
- Uses GPT-4o with temperature=0.7
- **CRITICAL**: Only ONE tool registered (line 74):
  ```python
  self.tools = [analyze_data]
  ```
- LangGraph workflow with 2 nodes: `agent` and `tools`
- Can handle ANY data analysis request through Python execution

### 2.3 TPR Workflow Handler (State Machine)

**File**: `app/data_analysis_v3/core/tpr_workflow_handler.py`
**Class**: `TPRWorkflowHandler`

**Stage Progression**:
```
TPR_STATE_SELECTION → TPR_FACILITY_LEVEL → TPR_AGE_GROUP → COMPLETE
```

**Key Methods**:
| Method | Line | Purpose |
|--------|------|---------|
| `handle_workflow()` | 400 | Main router based on stage |
| `handle_state_selection()` | 654 | Process state choice |
| `handle_facility_selection()` | 719 | Process facility level choice |
| `handle_age_group_selection()` | 956 | Process age group choice |
| `calculate_tpr()` | ~1170 | Execute TPR calculation |

### 2.4 The Flexible Tool: analyze_data

**File**: `app/data_analysis_v3/tools/python_tool.py`
**Function**: `analyze_data` (lines 21-242)

**Capabilities**:
- Executes ANY Python code passed by GPT-4o
- Has access to uploaded DataFrame as `df`
- Can generate Plotly visualizations
- Has helper functions: `top_n`, `ensure_numeric`, `suggest_columns`, `capture_table`
- Returns formatted output with tables and charts

**This is the key to flexibility** - GPT-4o knows pandas and can write any analysis code.

### 2.5 Specialized TPR Tools (NOT Registered)

**File**: `app/data_analysis_v3/tools/tpr_analysis_tool.py`
**Function**: `analyze_tpr_data` (line 797)

**Actions Available**:
- `analyze`: Basic exploration and summary of TPR data
- `calculate_tpr`: Calculate ward-level TPR with user selections
- `prepare_for_risk`: Create raw_data.csv for risk analysis

**CRITICAL ISSUE**: This tool is NOT in the agent's tool list. It's called directly by `TPRWorkflowHandler.calculate_tpr()`.

---

## 3. Message Flow

### 3.1 Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           USER MESSAGE                                    │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    data_analysis_v3_routes.py                            │
│                         (Gatekeeper)                                      │
│                                                                           │
│   1. Get session_id                                                       │
│   2. Check workflow_transitioned flag                                     │
│   3. Check is_tpr_workflow_active()                                       │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
            TPR Active = YES                TPR Active = NO
                    │                               │
                    ▼                               ▼
┌───────────────────────────────┐   ┌──────────────────────────────────────┐
│     TPR WORKFLOW PATH         │   │        GENERAL AGENT PATH            │
│                               │   │                                       │
│  1. Load data                 │   │  1. Check TPR start triggers         │
│  2. Load state from manager   │   │     ("start tpr", "tpr workflow")    │
│  3. Check visualization       │   │                                       │
│     requests                  │   │  2. If trigger found:                │
│  4. Check confirmation        │   │     → Start TPR workflow             │
│     awaiting                  │   │                                       │
│  5. Run intent classifier     │   │  3. Otherwise:                       │
│                               │   │     → Build workflow context         │
│  Intent = "selection"         │   │     → Run agent.analyze()            │
│     → extract_command()       │   │                                       │
│     → execute_command()       │   └──────────────────────────────────────┘
│                               │
│  Intent = "question"          │
│     → Route to agent with     │
│       workflow context        │
└───────────────────────────────┘
```

### 3.2 Detailed Flow: TPR Workflow Active

```python
# Lines 586-826 in data_analysis_v3_routes.py

1. is_tpr_active = state_manager.is_tpr_workflow_active()  # Line 586

2. If active:
   a. Create TPRWorkflowHandler instance
   b. Load data from session folder
   c. Check for exact visualization phrases (lines 614-695)
   d. Check for confirmation awaiting (lines 701-712)
   e. Run intent classification (lines 740-744):

      intent_result = tpr_language.classify_intent(
          message=message,
          stage=current_stage.name,
          valid_options=valid_options
      )

   f. Route based on intent (lines 747-789):

      If intent == "selection" and confidence >= 0.7:
          → extract_command() → execute_command()
      Else:
          → Run agent with workflow context
```

### 3.3 Detailed Flow: General Agent Path

```python
# Lines 828-886 in data_analysis_v3_routes.py

1. Check TPR start triggers (line 829-830):
   start_triggers = ['start tpr', 'tpr workflow', 'test positivity', ...]

2. If trigger found:
   → Load data
   → Create TPRWorkflowHandler
   → Mark TPR workflow active
   → Return tpr_handler.start_workflow()

3. Otherwise (lines 857-865):
   → Build general workflow context
   → Run agent.analyze(message, workflow_context)
```

---

## 4. Rigidity Points

### 4.1 Intent Classification Gate

**Location**: `data_analysis_v3_routes.py:740-788`

**Problem**: The intent classifier decides if user input is a "selection" or "question". If classified as "selection" with confidence >= 0.7, the message bypasses the agent entirely.

```python
if intent_result['intent'] == 'selection' and intent_result['confidence'] >= 0.7:
    # BYPASSES AGENT - goes directly to stage handler
    command = tpr_language.extract_command(...)
    response = tpr_handler.execute_command(command, current_stage)
```

**Impact**: Exploratory questions during TPR workflow are often misclassified as selections, causing confusing responses.

### 4.2 Stage-Based Routing

**Location**: `tpr_workflow_handler.py:429-445`

**Problem**: The `handle_workflow()` method routes strictly based on current stage:

```python
if self.current_stage == ConversationStage.TPR_STATE_SELECTION:
    return self.handle_state_selection(user_query)
elif self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
    return self.handle_facility_selection(user_query)
elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
    return self.handle_age_group_selection(user_query)
```

**Impact**: Users MUST follow the predetermined sequence. Cannot skip steps or ask questions outside the current stage's scope.

### 4.3 Stage Handlers Accept Only Valid Options

**Location**: `tpr_workflow_handler.py` (various handlers)

**Example** - Facility Selection (line 719+):
```python
def handle_facility_selection(self, user_query: str) -> Dict[str, Any]:
    selected_level = self.extract_facility_level_from_query(user_query)

    if not selected_level:
        return {
            "message": "I didn't catch which facility level. Please specify..."
        }
```

**Impact**: If input doesn't match expected format, user gets a "please try again" response instead of intelligent handling.

### 4.4 Workflow Context Injection

**Location**: `agent.py:690-714`

**Problem**: When agent IS used during TPR workflow, rigid instructions are injected:

```python
if workflow_type == 'tpr':
    context_parts.append(
        f"User is in the TPR workflow at stage '{stage}'. "
        "Keep the guided flow moving while answering follow-up questions fully."
    )
```

**Impact**: Even when agent handles a question, it's biased toward pushing the user back to the workflow.

---

## 5. Flexibility Points

### 5.1 The analyze_data Tool

**Location**: `python_tool.py:21-242`

**Capability**: Can execute ANY Python code:
```python
@tool
def analyze_data(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    python_code: str  # <-- ANY PYTHON CODE
) -> Tuple[str, Dict[str, Any]]:
```

**What This Means**: If GPT-4o can reach this tool, it can:
- Run `df.describe()`, `df.groupby()`, any pandas operation
- Create any Plotly visualization
- Perform statistical analysis
- Calculate custom metrics

### 5.2 GPT-4o's Knowledge

**Location**: N/A (inherent to the model)

GPT-4o has extensive knowledge of:
- Pandas operations
- Data analysis techniques
- Statistical methods
- Visualization best practices
- Malaria/health domain (from training data)

**This knowledge is BLOCKED** by the rigid routing, not utilized.

### 5.3 Agent Fallback for Questions

**Location**: `data_analysis_v3_routes.py:773-789`

When intent is classified as "question", the agent IS used:
```python
else:
    # User is asking a question → Route to agent
    workflow_context = {
        'workflow': 'tpr',
        'stage': current_stage.name,
        ...
    }
    response = run_agent_sync(message, workflow_context=workflow_context)
```

**This works** but is underutilized due to aggressive "selection" classification.

### 5.4 System Prompt (Relatively Flexible)

**Location**: `prompts/system_prompt.py`

The system prompt is actually reasonable:
```python
BASE_SYSTEM_PROMPT = """
## Role
You are a data analysis assistant for malaria programmes...

## Tooling
- Use the `analyze_data` tool whenever a question requires inspecting data...
"""
```

**Note**: The prompt doesn't enforce rigid sequences - it's the routing code that does.

---

## 6. Root Cause Analysis

### Problem 1: Users Can't Do Flexible EDA During TPR Workflow

**Symptom**: User asks "How many facilities are in Kano?" and gets "Please choose from: primary, secondary..."

**Root Cause Chain**:
```
1. TPR workflow is active
2. Intent classifier runs on message
3. Classifier returns intent="selection" (false positive)
4. Message routed to extract_command()
5. Command extraction fails (it's a question, not a selection)
6. Stage handler returns "please choose from valid options"
```

**Fix Required**: Either improve intent classification OR remove the gate entirely.

### Problem 2: TPR Workflow Too Rigid

**Symptom**: User can't change parameters after making a selection without restarting.

**Root Cause Chain**:
```
1. Stage handlers only accept forward progression
2. "back" navigation exists but is limited
3. No way to say "actually use all facilities" after selecting "secondary"
4. State machine doesn't support parameter modification
```

**Fix Required**: Allow agent to handle all requests, even during TPR workflow.

### Problem 3: Specialized TPR Tool Not Accessible

**Symptom**: User can't directly request "calculate TPR for Kano, secondary, under-5"

**Root Cause**:
```python
# agent.py line 74
self.tools = [analyze_data]  # <-- analyze_tpr_data NOT included!
```

**Fix Required**: Register `analyze_tpr_data` with the agent.

---

## 7. Specialized Tools Analysis

### 7.1 analyze_tpr_data Tool

**Location**: `app/data_analysis_v3/tools/tpr_analysis_tool.py:797`

**Signature**:
```python
@tool
def analyze_tpr_data(
    thought: str,
    action: str = "analyze",  # "analyze", "calculate_tpr", "prepare_for_risk"
    options: str = "{}",
    graph_state: Annotated[dict, InjectedState] = None
) -> str:
```

**Actions**:
| Action | Purpose | Output |
|--------|---------|--------|
| `analyze` | Basic TPR data exploration | Summary statistics |
| `calculate_tpr` | Ward-level TPR calculation | Rankings, visualizations |
| `prepare_for_risk` | Create files for risk analysis | raw_data.csv, shapefile |

**Current Usage**: Called ONLY by `TPRWorkflowHandler.calculate_tpr()`:
```python
# tpr_workflow_handler.py ~line 1170
result = analyze_tpr_data.invoke({
    'thought': f"Calculating TPR for {self.tpr_selections['state']}",
    'action': "calculate_tpr",
    'options': json.dumps(options),
    'graph_state': graph_state
})
```

### 7.2 tpr_workflow_step Tool

**Location**: `app/data_analysis_v3/tools/tpr_workflow_langgraph_tool.py:971`

**Signature**:
```python
@tool
async def tpr_workflow_step(
    session_id: str,
    action: str,
    value: Optional[str] = None
) -> Dict[str, Any]:
```

**Purpose**: Designed for LangGraph-native workflow progression but NOT currently used.

### 7.3 Why Tools Are Not Registered

**Historical Reason**: The architecture evolved from a rigid wizard-style interface where:
1. User makes selection
2. System validates and stores
3. System presents next step

Tools were added to support this wizard, not to give the agent freedom.

---

## 8. Dependencies & Risks

### 8.1 What Depends on Current Architecture

| Component | Dependency | Risk if Changed |
|-----------|------------|-----------------|
| Frontend | Expects `workflow: "tpr"` and `stage` in responses | Need to maintain response format |
| State Manager | Stores stage progression, selections | May have stale state if bypassed |
| Visualizations | Built by stage handlers | Need alternative generation method |
| Risk Analysis Transition | Triggered by workflow completion | Must maintain trigger mechanism |
| Interaction Logging | Logs workflow stage | Update logging for new flow |

### 8.2 Files That Would Need Changes

| File | Change Type | Risk Level |
|------|-------------|------------|
| `agent.py` | Add tool registration | Low |
| `data_analysis_v3_routes.py` | Remove/modify intent gate | Medium |
| `system_prompt.py` | Update instructions | Low |
| `tpr_workflow_handler.py` | May need modification or bypass | Medium |
| `state_manager.py` | May need updates for new flow | Low |

### 8.3 Potential Breaking Changes

1. **Frontend Visualization Rendering**: Currently expects specific response structure
2. **Session State**: Workflow stage tracking may become inconsistent
3. **Risk Analysis Trigger**: Must ensure transition still works
4. **Multi-Instance Sync**: State changes must propagate correctly

---

## 9. Proposed Solutions

### Option A: Register analyze_tpr_data with Agent (Recommended)

**Changes Required**:
```python
# agent.py line 74
# FROM:
self.tools = [analyze_data]

# TO:
from ..tools.tpr_analysis_tool import analyze_tpr_data
self.tools = [analyze_data, analyze_tpr_data]
```

**Pros**:
- Minimal code change (1-2 lines)
- Preserves all specialized TPR logic
- GPT-4o decides when to use each tool
- No hardcoding

**Cons**:
- Still have intent classifier gate (may need additional changes)
- System prompt should be updated to explain both tools

### Option B: Remove Intent Classification Gate

**Changes Required**:
```python
# data_analysis_v3_routes.py lines 740-789
# Remove the intent classification logic
# Always route to agent with workflow context
```

**Pros**:
- All messages go to intelligent agent
- No false positive "selection" classifications
- Maximum flexibility

**Cons**:
- Loses the quick-path for simple selections
- Agent may be slower for obvious choices

### Option C: Hybrid Approach (Best)

Combine A + B with smart defaults:

1. Register both tools with agent
2. Keep intent classifier but lower confidence threshold
3. On classification failure, default to agent (not error message)
4. Update system prompt with clear tool guidance

**Implementation**:
```python
# Route to agent MORE often, not less
if intent_result['intent'] == 'selection' and intent_result['confidence'] >= 0.9:  # Higher threshold
    # Only very confident selections bypass agent
    ...
else:
    # Everything else goes to agent
    response = run_agent_sync(message, workflow_context=workflow_context)
```

---

## 10. User Experience Comparison

### 10.1 Current Experience (Rigid)

```
User: [Uploads malaria data]
System: "Your data is loaded. Type 'start TPR workflow' when ready."

User: "What columns do I have?"
System: [Agent responds with column list]

User: "start tpr"
System: "Welcome to TPR Analysis! Step 1: Select your state..."

User: "How many facilities are in Kano?"
System: "I understood you're making a selection, but couldn't determine
        which option. Please choose from: Kano, Lagos, Kaduna..."
        ❌ BLOCKED

User: "Kano"
System: "Great! Step 2: Select facility level..."

User: "Actually, show me TPR by ward first"
System: "Please choose from: primary, secondary, tertiary, all"
        ❌ BLOCKED

[...rigid sequence continues...]
```

### 10.2 Proposed Experience (Flexible)

```
User: [Uploads malaria data]
System: "Your data is loaded! I can see 45,000 rows across 3 states.
        What would you like to explore?"

User: "What columns do I have?"
System: "You have 23 columns including: state, lga, ward, facility_name..."

User: "Show me facility distribution by state"
System: [Runs analyze_data, shows chart]
        "Kano (18,234), Lagos (15,890), Kaduna (10,876)"

User: "Which wards have highest TPR in Kano?"
System: [Runs analyze_tpr_data with action='analyze']
        "Top 5 wards by TPR: Gwale (42.3%), Nassarawa (38.7%)..."

User: "Calculate full TPR for Kano, secondary facilities, under-5"
System: [Runs analyze_tpr_data with action='calculate_tpr']
        "Results: 156 wards analyzed, State Average: 28.4%..."
        [Shows map + rankings]

User: "What if I use all age groups instead?"
System: [Re-calculates with new parameters]
        "With all age groups: State Average: 31.2%..."
        ✅ CAN MODIFY FREELY

User: "Show me trend over time for top 3 wards"
System: [Runs analyze_data for time series]
        [Shows line chart]
        ✅ CAN DO ANY EDA
```

### 10.3 Key Improvements

| Aspect | Before | After |
|--------|--------|-------|
| Question handling | Blocked during workflow | Always allowed |
| Parameter changes | Must restart | Anytime |
| Data exploration | Only before/after TPR | Anytime |
| TPR calculation | 3-4 step wizard | Natural language |
| Visualizations | Pre-built only | Generated on demand |
| Navigation | Unreliable "back" | Just ask differently |

---

## Appendix A: File Reference

| File Path | Purpose | Key Lines |
|-----------|---------|-----------|
| `app/web/routes/data_analysis_v3_routes.py` | API endpoint, routing | 471-914 |
| `app/data_analysis_v3/core/agent.py` | LangGraph agent | 74, 617-750 |
| `app/data_analysis_v3/core/tpr_workflow_handler.py` | TPR state machine | 400-445, 654+ |
| `app/data_analysis_v3/tools/python_tool.py` | analyze_data tool | 21-242 |
| `app/data_analysis_v3/tools/tpr_analysis_tool.py` | analyze_tpr_data tool | 797+ |
| `app/data_analysis_v3/prompts/system_prompt.py` | System prompts | 1-109 |
| `app/data_analysis_v3/core/state_manager.py` | State persistence | Various |

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| TPR | Test Positivity Rate - ratio of positive malaria tests to total tests |
| EDA | Exploratory Data Analysis |
| LangGraph | Framework for building stateful LLM applications |
| Intent Classification | Determining if user message is a "selection" or "question" |
| Stage Handler | Function that processes input for a specific workflow stage |
| Workflow Context | Metadata passed to agent about current workflow state |

---

## Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | January 2026 | Initial investigation complete |
