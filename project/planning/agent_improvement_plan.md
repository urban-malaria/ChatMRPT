# Agent Improvement Plan

## Date: April 7, 2026
## Status: PLANNED — Ready to implement

---

## Part 1: Config Changes (low risk, immediate impact)

### 1.1 Increase max_tokens: 2,000 → 8,000
- File: `app/agent/agent.py:69`
- Why: Responses get cut short. GPT-4o supports 16,384 output tokens.
  8,000 gives room for detailed analysis without excessive cost.
- Risk: Slightly higher API cost per response. Worth it.

### 1.2 Increase execution timeout: 20s → 60s
- File: `app/agent/executor_simple.py:478`
- Why: Complex analysis on 6,948 rows times out. ChatGPT uses 60s.
- Risk: Slower responses for complex analysis. Better than failing.

### 1.3 Increase message context: 10 → 20
- File: `app/agent/agent.py:393`
- Why: After 10 exchanges, agent forgets earlier context. GPT-4o has
  128K context window — we're only using a fraction.
- Risk: None. GPT-4o handles 20 messages easily.

### 1.4 Increase data profile: 12KB → 20KB
- File: `app/agent/agent.py:249`
- Why: Large datasets get profile truncated. Agent can't see all columns.
- Risk: Slightly more tokens used for context. Worth it.

---

## Part 2: Fix Broken Guardrails (medium risk)

### 2.1 Fix error counter state tracking
- File: `app/agent/state.py` — Add `tool_call_count` and 
  `consecutive_error_count` fields to DataAnalysisState
- File: `app/agent/agent.py` — Increment counters in `_route_from_tools`
  and reset `consecutive_error_count` on success
- Why: Currently dead code. Agent can loop forever on same error.
- Risk: Low — just wiring up existing logic.

---

## Part 3: System Prompt Improvements (the big one)

### What to add (based on research from ChatGPT, Julius, Open Interpreter, GPT-4.1):

### 3.1 Execution Philosophy (NEW SECTION)
Add between "Tooling" and "Column Interpretation":

```
## Execution Philosophy
- Work in small, incremental steps. Run code, examine the output, then
  decide the next step. Do NOT try to do everything in one massive code block.
- Before executing code, briefly state your plan: what you will compute,
  which columns you will use, and what output to expect.
- After getting results, sanity-check them: Do numbers make sense given
  the dataset size? Are percentages between 0-100? Are there unexpected nulls?
```

### 3.2 Error Recovery (NEW SECTION)
Add after Execution Philosophy:

```
## Error Recovery
- If code fails, read the error carefully, fix the issue, and retry.
  Try at least 3 different approaches before telling the user you cannot
  do something.
- KeyError? Check the data profile for the exact column name.
- ValueError? Use pd.to_numeric(col, errors='coerce').
- Timeout? Simplify the analysis — sample the data or reduce groupings.
- Visualization fails? Try a simpler chart type.
- NEVER say "there was an error" without explaining what went wrong and
  what you're trying next.
```

### 3.3 Output Management (NEW SECTION)
Add after Error Recovery:

```
## Output Management
- For DataFrames with more than 20 rows, show the top 10 with a summary
  (total count, min, max, mean) rather than printing everything.
- When results are large, summarize key findings in 3-5 bullet points.
- I REPEAT: Always use print() for every result. Code without print()
  produces no visible output.
```

### 3.4 Strengthen Confidence (MODIFY existing)
Add to Response Style:

```
- You are a capable data analyst. When the user asks a question, proactively
  analyze the data and present findings. Do not ask unnecessary clarifying
  questions — make reasonable assumptions and state them.
- Keep going until the user's query is completely resolved. Do not give up
  after one attempt.
```

### 3.5 Improve Tool Docstring
- File: `app/agent/tools/python_tool.py:22-32`
- Add to docstring: available helpers (top_n, suggest_columns, 
  capture_table, run_trend_analysis), plotly_figures capture, 
  60-second timeout, print() requirement

---

## Implementation Order

1. Config changes (Part 1) — all 4 in one commit, test
2. Fix guardrails (Part 2) — one commit, test
3. System prompt (Part 3) — one commit, test
4. Tool docstring (Part 3.5) — one commit, test

Each part is independently testable. If any breaks something, revert just that part.

---

## Success Criteria

After implementation, the agent should:
- [ ] Never cut a response short mid-sentence
- [ ] Retry at least once when code fails
- [ ] State its plan before running complex analysis
- [ ] Validate results before presenting (no nonsensical numbers)
- [ ] Remember context from 20 exchanges ago
- [ ] Handle 6,948-row dataset analysis without timeout
- [ ] Give clear error messages with next steps, not "there was an error"

---

## What We're NOT Changing

- LLM model (staying with GPT-4o)
- LangGraph architecture
- Tool registration pattern
- Data loading pipeline
- Frontend
