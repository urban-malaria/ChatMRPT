# Agent Improvement Plan (Reviewed & Updated)

## Date: April 7, 2026
## Status: REVIEWED — Ready to implement
## Reviewer feedback: PASS with reservations — all issues addressed below

---

## Part 1: Config Changes (low risk, immediate impact)

### 1.1 max_tokens: 2,000 → 8,000
- File: `app/agent/agent.py:69`
- Why: Primary cause of cut-short responses. GPT-4o supports 16,384.
- Cost: ~4x more on output tokens per response. Worth it.

### 1.2 Temperature: 0.7 → 0.1
- File: `app/agent/agent.py:68`
- Why: Data analysis should be deterministic, not creative. 0.7 causes
  hallucinated column names → KeyError → error loops. Lower temperature
  produces consistent, reliable code.
- Risk: None for data analysis. Slightly less varied language.

### 1.3 Execution timeout: 20s → 60s
- File: `app/agent/executor_simple.py:478`
- Why: Complex analysis on real datasets times out. ChatGPT uses 60s.
- Note: daemon threads mean timed-out code keeps running in background.
  Acceptable for now, monitor under load.

### 1.4 LLM call timeout: 50s → 90s
- File: `app/agent/agent.py:70`
- Why: Reviewer caught that executor timeout (60s) > LLM timeout (50s).
  The LLM call can fail before code even runs. Must be higher than executor.

### 1.5 Message context: 10 → 20
- File: `app/agent/agent.py:393`
- Why: Agent forgets earlier context after 10 exchanges.
- Cost: More tokens per request. Data profile is prepended on EVERY loop
  iteration (reviewer caught this). Budget: ~40-60K tokens per multi-hop call.
  GPT-4o handles 128K, so still within limits.

### 1.6 Data profile: 12KB → 20KB
- File: `app/agent/agent.py:249`
- Why: Large datasets get profile truncated, agent can't see all columns.

---

## Part 2: Fix Broken Guardrails

### 2.1 Add missing state fields
- File: `app/agent/state.py`
- Add: `tool_call_count: int` and `consecutive_error_count: int`

### 2.2 Increment counters in _tools_node (NOT _route_from_tools)
- File: `app/agent/agent.py` — `_tools_node` method
- After tool execution: increment `tool_call_count`
- If tool result contains error: increment `consecutive_error_count`
- If tool result is success: reset `consecutive_error_count` to 0
- Reviewer caught: _route_from_tools is the CONSUMER, _tools_node is
  where increment must happen.

---

## Part 3: System Prompt Improvements

### Principles (from reviewer):
- Put most important instructions FIRST (GPT-4o follows top 80% reliably)
- No "I REPEAT" — doesn't help, just adds tokens
- Explain AFTER executing, not before (Julius pattern — faster UX)
- Keep additions concrete and actionable, not vague

### 3.1 Error Recovery (HIGHEST VALUE — add near top of prompt)
Insert after "Tooling" section:

```
## Error Recovery
- If code fails, read the error carefully, fix the issue, and retry.
  Try at least 3 different approaches before telling the user you cannot
  do something.
- KeyError? Check the data profile for the exact column name — names
  are case-sensitive.
- ValueError on numeric conversion? Use pd.to_numeric(col, errors='coerce')
  and report how many values could not be converted.
- Timeout? Simplify — sample the data with df.sample(1000) or reduce
  groupby categories.
- Visualization fails? Try a simpler chart type before giving up.
- NEVER respond with just "there was an error." Always explain what went
  wrong, what you tried, and what you'll try next.
- NEVER fabricate data or statistics. If you cannot find the answer in
  the data, say so and suggest what data would be needed.
```

### 3.2 Output Management (add after Error Recovery)

```
## Output Management
- For DataFrames with more than 20 rows, show the top 10 with a summary
  (total count, min, max, mean) rather than printing everything.
- When results are large, summarize key findings in 3-5 bullet points
  and offer to show full details if the user wants.
- Always use print() for every result. Code without print() produces
  no visible output.
- After getting results, sanity-check them: Do numbers make sense given
  the dataset? Are percentages between 0-100? If something looks off,
  investigate before presenting.
```

### 3.3 Confidence & Persistence (modify existing Response Style)
Add to Response Style section:

```
- You are a capable data analyst. When the user asks a question,
  proactively analyze the data and present findings. Do not ask
  unnecessary clarifying questions — make reasonable assumptions
  and state them.
- Keep going until the user's query is completely resolved. Do not
  give up after one attempt.
```

### 3.4 Improve tool docstring
- File: `app/agent/tools/python_tool.py:22-32`
- Add to docstring:
  - Available helpers: top_n(), suggest_columns(), capture_table(),
    run_trend_analysis()
  - Plotly figures: append to plotly_figures list (auto-captured)
  - Timeout: 60 seconds max
  - Must use print() — no output = no result
  - For large datasets: use .head(), .sample(), or groupby to manage size

---

## Part 4: Future Roadmap (NOT implementing now)

These are the "next level" improvements identified by the reviewer.
Tracked here for future sessions:

### 4.1 Streaming Responses (HIGHEST IMPACT)
- Currently: V3 endpoint returns JSON blob after full processing
- Target: Stream tokens as generated, like ChatGPT
- Impact: 10-second response that streams feels like 2 seconds
- Effort: Requires changes to route handler + frontend
- LangGraph supports streaming via astream_events

### 4.2 Proactive Insights
- Currently: Agent only responds to what's asked
- Target: After analysis, suggest interesting findings
  "I notice 3 wards have TPR above 80% — want to investigate?"
- Effort: Post-tool insight node in LangGraph

### 4.3 Smart Tool Output Truncation
- Currently: print(df.head(50)) stays in context through all future turns
- Target: Truncate tool outputs to 5 rows + summary in context
- Effort: Modify _smart_truncate_messages to detect DataFrame outputs

### 4.4 Multi-step Workflow Chaining
- Currently: _planner_node exists but is dead code (never wired into graph)
- Target: "Run risk analysis, show top 10 on a map, then plan ITN for 50K nets"
- Effort: Wire planner into graph, replace naive string splitter with LLM decomposer

### 4.5 Persistent Variable Display
- Currently: Users don't know what DataFrames are loaded
- Target: Show df, ts_df, unified_df status in sidebar/status message

---

## Implementation Order

1. Config changes (Part 1) — 6 one-liners, one commit
2. Fix guardrails (Part 2) — state.py + agent.py, one commit
3. System prompt + tool docstring (Part 3) — one commit
4. Test locally — full workflow
5. Future roadmap — separate sessions

---

## Success Criteria

After implementation, the agent should:
- [ ] Never cut a response short mid-sentence
- [ ] Retry at least once when code fails (not just say "error occurred")
- [ ] Validate results before presenting (no nonsensical numbers)
- [ ] Remember context from 20 exchanges ago
- [ ] Handle 6,948-row dataset analysis without timeout
- [ ] Produce consistent code (no hallucinated column names)
- [ ] Give clear error messages with what it's trying next

## What We're NOT Changing (this round)
- LLM model (staying with GPT-4o)
- LangGraph architecture
- Response format (JSON, not streaming — that's future roadmap)
- Frontend
- Tool registration pattern
