# Dual-Brain Architecture Analysis

**Date:** 2026-01-16
**Issue:** Two separate LLM systems causing maintenance overhead and user flow problems

---

## Current Architecture (Two Brains)

### Brain 1: RequestInterpreter (Standard Brain)
**Location:** `app/core/request_interpreter.py` (~1000+ lines)

- **LLM:** OpenAI/Mistral via `llm_manager.generate_response()`
- **Pattern:** Direct function registration with manual tool dispatch
- **Tools:** 12 registered tools including:
  - `run_malaria_risk_analysis`
  - `create_vulnerability_map`
  - `create_settlement_map`
  - `execute_sql_query`
  - `analyze_data_with_python`
- **Endpoint:** `/api/send_message` (main chat)
- **Memory:** Custom memory service integration

### Brain 2: DataAnalysisAgent (LangGraph Brain)
**Location:** `app/data_analysis_v3/core/agent.py` (960 lines)

- **LLM:** OpenAI GPT-4o via LangGraph
- **Pattern:** State graph with proper node routing
- **Tools:** `unified_tools.py` with:
  - `analyze_data` (Python execution)
  - `create_visualization`
  - `get_data_info`
- **Endpoint:** `/api/v1/data-analysis/chat`
- **Memory:** LangChain message history + custom memory service
- **Special:** TPR workflow integration

---

## Routing Logic (The Mess)

```
User Request
    │
    ├── If use_data_analysis_v3=True ──► DataAnalysisAgent
    │       (Set when user uploads via Data Analysis tab)
    │
    └── Else ──► RequestInterpreter
            (Standard chat flow)
```

**Files involved in routing:**
- `app/web/routes/analysis_routes.py` - Main routing decisions
- `app/web/routes/data_analysis_v3_routes.py` - Data Analysis V3 endpoint
- `app/web/routes/analysis/chat_routing.py` - Additional routing logic
- `app/web/routes/analysis/chat_sync_service.py` - More routing

---

## Problems with Dual-Brain Architecture

### 1. Users Can't Go Back After TPR Workflow
**Root Cause:** After TPR workflow completes, `workflow_transitioned=True` flag was locking users out

```python
# In data_analysis_v3_routes.py (now removed, but symptom of the problem)
if current_state.get('workflow_transitioned'):
    # Lock user out of data analysis
```

### 2. Double Maintenance for LLM Fixes
Every improvement needs to be applied TWICE:
- Fix validation? → Update both brains
- Add logging? → Update both brains
- Improve prompts? → Update both brains

### 3. Inconsistent Capabilities
| Feature | RequestInterpreter | DataAnalysisAgent |
|---------|-------------------|-------------------|
| Malaria risk analysis | ✅ | ❌ (separate) |
| Settlement maps | ✅ | ❌ |
| TPR workflow | ❌ | ✅ |
| Python execution | ✅ (basic) | ✅ (better) |
| State management | ❌ (manual) | ✅ (LangGraph) |

### 4. Complex Routing Logic
Routing is spread across 4+ files with overlapping logic:
- Session flags: `use_data_analysis_v3`, `data_analysis_active`, `csv_loaded`
- File-based flags: `.data_analysis_mode`, `.analysis_complete`
- State managers: `WorkflowStateManager`, `DataAnalysisStateManager`

### 5. Session State Confusion
Both systems track state differently:
- RequestInterpreter: `conversation_history`, `session_data`
- DataAnalysisAgent: `DataAnalysisState`, `chat_history`

---

## The Decision: Which Brain to Keep?

### Option A: Keep RequestInterpreter, Remove LangGraph

**Pros:**
- Simpler architecture
- Already integrated with main chat
- Has malaria-specific tools

**Cons:**
- No structured workflow management
- Older tool-calling pattern (not as flexible)
- Manual state tracking
- Would need to rebuild TPR workflow

### Option B: Keep LangGraph DataAnalysisAgent, Remove RequestInterpreter ✅ RECOMMENDED

**Pros:**
- Modern LangGraph architecture with state machine
- Proper tool binding with OpenAI function calling
- Structured workflow support (TPR workflow ready)
- Can handle both structured AND flexible analysis
- GPT-4o with proper error handling
- Built-in message truncation and context management

**Cons:**
- Need to migrate existing malaria tools
- Need to update all routes

---

## Recommendation: Unified LangGraph Agent

**My recommendation: Keep LangGraph (Option B)**

### Why?

1. **LangGraph is the future** - It's the industry standard for agent architectures
2. **TPR workflow already works** - We'd lose this with Option A
3. **Better tool calling** - OpenAI function calling is more reliable
4. **Proper state management** - LangGraph handles state transitions cleanly
5. **Easier to extend** - Adding new workflows (e.g., ITN planning) is cleaner
6. **One prompt to maintain** - Only `system_prompt.py` needs updates

### Migration Plan

1. **Add missing tools to unified_tools.py**
   - `run_malaria_risk_analysis`
   - `create_settlement_map`
   - `show_settlement_statistics`
   - ITN planning tools

2. **Update routes to use single endpoint**
   - Make `/api/send_message` use DataAnalysisAgent
   - Keep `/api/v1/data-analysis/chat` as alias

3. **Remove RequestInterpreter**
   - Delete `app/core/request_interpreter.py`
   - Remove related routing code

4. **Simplify session management**
   - Remove redundant flags
   - Use DataAnalysisStateManager everywhere

---

## Alternative: Wrapper Approach (NOT Recommended)

We could wrap RequestInterpreter inside DataAnalysisAgent, but:
- Still have two systems to maintain
- Just adds complexity
- Kicks the can down the road

**The user is right** - we should pick ONE and commit to it.

---

## Next Steps

1. Document current tool inventory (both systems)
2. Plan tool migration to unified_tools.py
3. Update system prompt with all capabilities
4. Test unified agent with all workflows
5. Remove RequestInterpreter

---

## Files to Modify

### Core Changes
- `app/data_analysis_v3/tools/unified_tools.py` - Add missing tools
- `app/data_analysis_v3/prompts/system_prompt.py` - Update with all capabilities
- `app/web/routes/analysis_routes.py` - Route to unified agent

### Files to Delete (Eventually)
- `app/core/request_interpreter.py` (or deprecate)
- `app/core/choice_interpreter.py`
- `app/core/tool_intent_resolver.py`
- `app/core/llm_orchestrator.py`
- `app/core/prompt_builder.py`
- `app/core/tool_runner.py`

### Files to Update
- `app/web/routes/analysis_routes.py` - Single routing
- `app/services/container.py` - Remove RequestInterpreter
