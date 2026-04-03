# Standard Mode Archive — What It Was and How It Worked

## Purpose of This Document
The standard mode is being replaced by the one-brain V3 agent. This document
preserves the knowledge of how it worked so we can always reference it.

## What Standard Mode Was
After the TPR workflow completed, ChatMRPT exited data analysis mode and
switched to "standard mode" — a separate code path with its own:
- LLM orchestrator (two-brain: outer LLM generates tool calls, inner tools execute)
- ToolIntentResolver (keyword scoring to match user messages to tools)
- Separate system prompt (prompt_builder.py)
- Streaming SSE responses (vs V3's JSON responses)
- Separate conversation history system

## Why It Was Replaced
1. Two-brain orchestrator picked wrong DataFrames for trend analysis
2. ToolIntentResolver couldn't distinguish "create a map" from "explain the map"
3. No conversation history passed to LLM — every turn was a blank slate
4. Mode switch (exit_data_analysis_mode) caused session ID confusion
5. Follow-up questions recreated visualizations instead of explaining them
6. Complex code path with many edge cases

## Key Files (preserved in git history)

### Core Routing
- `app/core/request_interpreter.py` — The brain. Routes messages to tools.
  - `_attempt_direct_tool_resolution()` — Uses ToolIntentResolver
  - `_store_conversation()` — In-memory conversation cache (40 turns max)
  - `_ensure_memory_summary()` — LLM-generated memory summaries
  - `process_message()` / `process_message_streaming()` — Main entry points
  
### Tool Selection
- `app/core/tool_intent_resolver.py` — Keyword scoring system
  - Per-tool handlers with custom scoring logic
  - Confidence threshold: 1.8 score minimum
  - Fuzzy variable matching for distribution maps
  - Pronoun resolution ("show it again")
  
### LLM Orchestration
- `app/core/llm_orchestrator.py` — Function calling with tool execution
  - `run_with_tools()` — Sync path
  - `stream_with_tools()` — Streaming path
  - Sends messages + function schemas to OpenAI
  - Executes tool on function_call response

### System Prompt
- `app/core/prompt_builder.py` — Builds context-aware prompt
  - Schema injection (column names, types, ranges)
  - Two-layer data architecture rules (query_data vs analyze_data)
  - Memory summary integration
  - Tool guidance section

### Chat Routing
- `app/web/routes/analysis/chat_stream_service.py` — Streaming entry point
  - Semantic router (embedding-based classification)
  - Arena mode detection
  - Clarification handling
  - TPR workflow routing
  
- `app/routing/semantic_router.py` — 7 routes → 3 outcomes
  - Embedding: OpenAI text-embedding-3-small
  - Context biasing based on session state
  - LLM fallback for low confidence

### Visualization Explanation
- `app/services/universal_viz_explainer.py` — LLM-powered viz explanations
  - Reads underlying data, computes statistics
  - 9+ visualization type handlers
  - Called after tool execution in request_interpreter

## How Messages Flowed (Standard Mode)

```
User message
  → /send_message_streaming endpoint
  → Semantic router classifies (needs_tools / can_answer / needs_clarification)
  → If needs_tools:
    → ToolIntentResolver scores all tools
    → Best match above threshold → execute tool
    → No match → LLM orchestrator with function calling
  → If can_answer:
    → Arena mode check → direct LLM response
  → Response streamed via SSE
```

## How Messages Flow (One-Brain V3 Agent)

```
User message
  → /api/v1/data-analysis/chat endpoint
  → DataAnalysisAgent.analyze()
  → LangGraph: agent node → tool node → agent node (loop)
  → Agent decides which tool to call (or answers directly)
  → JSON response with message + visualizations
```

## What We Kept From Standard Mode
- All tool implementations (app/tools/*) — called via @tool wrappers
- Visualization rendering functions (app/services/agents/visualizations/*)
- Analysis engine (app/analysis/*)
- Data pipeline (upload → TPR → raw_data → risk analysis → unified_dataset)
- Export routes (app/web/routes/export_routes.py)
- Session memory system

## What We Retired
- ToolIntentResolver keyword scoring (agent decides tool selection)
- Two-brain LLM orchestrator (agent is one brain)
- exit_data_analysis_mode flag (never exit V3 mode)
- /send_message_streaming for post-TPR messages
- Separate prompt_builder.py system prompt
- _interpret_raw_output callback (was never implemented anyway)

## Lessons Learned
1. Let the LLM decide tool selection — keyword scoring is brittle
2. One brain with conversation history beats two brains without
3. Thin wrappers around existing tools work — no need to rewrite
4. Keep the agent in one mode throughout — mode switching causes bugs
5. Persist state (dataAnalysisMode) in sessionStorage to survive reloads
