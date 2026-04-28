# Agent Improvement Plan — Final Version (Review #2)

## Date: April 7, 2026
## Status: UPDATED after second review — all 9 issues addressed
## Sources: OpenAI GPT-4.1 Guide, GPT-5.4 Guide, PandasAI, Julius AI,
##          Open Interpreter, Liu et al. 2024 ("Lost in the Middle")

---

## Part 1: Config Changes

| Setting | Current | New | File:Line | Why |
|---------|---------|-----|-----------|-----|
| temperature | 0.7 | 0.1 | agent.py:68 | Deterministic code, fewer hallucinated columns. Trade-off: slightly less conversational tone — monitor and adjust if needed. |
| max_tokens | 2,000 | 8,000 | agent.py:69 | Stop cutting responses short. Most responses will still be 500-1,500 tokens — the limit just stops truncation. |
| LLM timeout | 50s | 90s | agent.py:70 | Must exceed executor timeout (60s). Note: frontend/nginx may have shorter timeouts — verify infrastructure. |
| Executor timeout | 20s | 60s | executor_simple.py:478 | Complex analysis on real datasets needs more time. ChatGPT uses 60s. |
| Message context | 10 | 20 | agent.py:393 | Better conversation memory. Cost: ~15-25K extra tokens per call with tool history. Acceptable for GPT-4o's 128K window. |
| Data profile | 12KB | 20KB | agent.py:249 (max_chars param) | See more of large datasets. |
| Tool call limit | 5 | 10 | agent.py:468 | Current 5 conflicts with "try 3 approaches" instruction. Real multi-step analysis needs 8+ calls. |

---

## Part 2: Fix Broken Guardrails

### 2.1 Add state fields
File: `app/agent/state.py`

Add to DataAnalysisState TypedDict (NO operator.add — use replacement semantics):
```python
tool_call_count: int
consecutive_error_count: int
```

### 2.2 Rewrite _tools_node to RETURN updated counters

The current `_tools_node` delegates to `self.tool_node.invoke()` and returns
its result directly. LangGraph only sees state updates that are RETURNED
from node functions — mutations to the input dict are ignored.

File: `app/agent/agent.py` — `_tools_node` method

```python
def _tools_node(self, state: DataAnalysisState):
    state_with_session = {**state, "session_id": self.session_id}
    result = self.tool_node.invoke(state_with_session)

    # Count this tool call
    new_tool_count = state.get('tool_call_count', 0) + 1

    # Check if the tool result contains an execution error
    # Use the precise pattern already in _route_from_tools, not generic string matching
    new_error_count = state.get('consecutive_error_count', 0)
    messages = result.get('messages', [])
    if messages:
        last_msg = messages[-1]
        content = getattr(last_msg, 'content', str(last_msg))
        if '⚠️ **Execution Error:**' in content or 'Timeout:' in content:
            new_error_count += 1
        else:
            new_error_count = 0  # Reset on success

    # Return updated state — LangGraph merges this into graph state
    result['tool_call_count'] = new_tool_count
    result['consecutive_error_count'] = new_error_count
    return result
```

### Why NOT `'error' in str(result).lower()`
Reviewer caught: this false-positives on legitimate output like
"0 errors found", "standard error: 0.45", "error rate: 2.3%".
Use the precise `⚠️ **Execution Error:**` marker instead.

---

## Part 3: System Prompt Rewrite

### Structure (OpenAI recommended order — critical rules at TOP and BOTTOM):
```
1. Role and Identity                    [TOP — primacy effect]
2. Error Recovery                       [TOP — most critical behavior]
3. Output Management                    [TOP — quality control]
4. Guiding Principles                   [CORE — behavioral rules]
5. Tooling (expanded)                   [CORE — what's available]
6. Tool Selection Table                 [CORE — when to use what]
7. Malaria Domain Knowledge             [DOMAIN — compact reference]
8. Analysis Patterns (few-shot)         [EXAMPLES — 5 patterns]
9. Column Interpretation                [DETAIL — kept from current]
10. Analysis Approach                   [DETAIL — kept, deduped]
11. Response Style                      [STYLE — kept from current]
12. Trend Analysis                      [SPECIALTY — kept from current]
13. Data Pipeline Order                 [REFERENCE — kept from current]
14. Final Reminders                     [BOTTOM — recency effect]
```

### What to REMOVE from current prompt (to avoid duplication):
- Line 29: "Start with lightweight descriptive statistics..." — too passive,
  replaced by Analysis Patterns that show WHAT to do
- Line 33: "Always use print()..." — moved to Final Reminders (bottom)
- Lines 87-93: Tool selection as prose — replaced by table format

### EXACT NEW SECTIONS:

#### Error Recovery (TOP of prompt, after Role)
```
## Error Recovery
- If code fails, read the error carefully, fix the issue, and retry.
  Try at least 2 different approaches before telling the user you
  cannot do something.
- KeyError? Check the data profile for the exact column name — names
  are case-sensitive. Use suggest_columns() to find the closest match.
- ValueError on numeric conversion? Use pd.to_numeric(col, errors='coerce').
- Timeout? Simplify — use df.sample(1000) or reduce groupby categories.
- Visualization fails? Try a simpler chart type before giving up.
- NEVER respond with just "there was an error." Always explain what
  went wrong and what you're trying next.
- NEVER fabricate data or statistics. If you cannot find the answer
  in the data, say so explicitly.
```

#### Output Management (after Error Recovery)
```
## Output Management
- For DataFrames with more than 20 rows, show the top 10 with a
  summary (total count, min, max, mean).
- When results are large, summarize key findings in 3-5 bullet points
  and offer to show full details.
- After getting results, sanity-check: Do numbers make sense? Are
  percentages between 0-100? If something looks off, investigate
  before presenting.
```

#### Guiding Principles (REPLACE existing — adds persistence)
```
## Guiding Principles
- Work strictly with the uploaded data; be explicit when information
  is missing or uncertain.
- Never provide medical advice; focus on data interpretation.
- Keep responses conversational, transparent, and grounded in the
  numbers you compute.
- You are a capable data analyst. When the user asks a question,
  proactively analyze and present findings. Do not ask unnecessary
  clarifying questions — make reasonable assumptions and state them.
- Keep going until the user's query is completely resolved. Do not
  give up after one attempt.
- Let users know they can type **start the tpr workflow** whenever
  they want guided malaria TPR analysis.
```

#### Tooling (EXPAND existing)
```
## Tooling
- Use the `analyze_data` tool whenever a question requires inspecting
  data, computing statistics, ranking items, or creating visualisations.
- The primary DataFrame is available as `df`; additional DataFrames
  may include `ts_df` (time-series) and `uploaded_df` (original upload).
  Variables persist between tool executions.
- Plotly figures: store in `plotly_figures` list (never call fig.show()).
- Available helper functions:
  - top_n(df, by, n=10) — get top N rows by column
  - ensure_numeric(obj, cols) — convert columns to numeric safely
  - suggest_columns(name, df) — fuzzy-match column names
  - capture_table(df, name) — register DataFrame for download
  - run_trend_analysis(df, time_col, value_col, group_col) — trends
  - create_map(variable_name, geographic_level) — choropleth map
- Available libraries: pandas, numpy, scipy.stats, sklearn (KMeans,
  PCA, LinearRegression, StandardScaler), plotly.express,
  plotly.graph_objects, matplotlib, seaborn, geopandas
- Execution timeout: 60 seconds. For large datasets, sample first.
```

#### Tool Selection Table (REPLACE existing prose list)
```
### Tool Selection

| User Intent | Tool | When |
|---|---|---|
| Rankings, statistics, charts | analyze_data | Any data question |
| Map any variable | create_variable_map | "Map X", "show Y distribution" |
| Risk classification map | create_vulnerability_map | AFTER run_risk_analysis |
| Model breakdowns | create_composite_score_maps | AFTER run_risk_analysis |
| Urban vs rural | create_urban_extent_map | Any time with data |
| Full risk analysis | run_risk_analysis | Creates unified dataset |
| ITN allocation | plan_itn_distribution | AFTER run_risk_analysis |
| Trends over time | analyze_data + run_trend_analysis() | "trends", "improving" |
| Switch data subset | switch_tpr_combination | Change facility/age |

NEVER call a downstream tool before its prerequisites are complete.
```

#### Malaria Domain Knowledge (NEW)
```
## Malaria Domain Knowledge

### TPR Interpretation
- TPR > 50%: Very high transmission — urgent intervention needed
- TPR 25-50%: High transmission — priority for resource allocation
- TPR 10-25%: Moderate transmission — sustained control measures
- TPR < 10%: Low transmission — approaching pre-elimination

### Key Epidemiological Rules
- Increasing TPR = WORSENING. Decreasing = IMPROVING.
- TPR is more reliable than raw case counts (adjusts for testing volume)
- Ward-level analysis reveals hotspots masked by LGA aggregation
- Malaria transmission follows rainfall with 4-6 week lag

### Nigerian Admin Hierarchy
Country → State → LGA → Ward → Health Facility

### DHIS2 Column Conventions (treat as fact)
- orgunitlevel2 = State, orgunitlevel3 = LGA, orgunitlevel4 = Ward
- organisationunitname = facility name, period0me = time period

### Interventions
- ITN: insecticide-treated nets (>80% coverage target)
- IRS: indoor residual spraying (>85% structure coverage)
- Reprioritization: reallocating resources to highest-burden areas
```

#### Analysis Patterns (NEW — 5 few-shot examples)

NOTE: Column names in examples are ILLUSTRATIVE. Always use the
exact column names from the data profile.

```
## Analysis Patterns

In these examples, column names are illustrative. Always substitute
the actual column names from the data profile you receive.

### Pattern: Ranking
User: "Which wards have the highest malaria burden?"
Code:
  burden_col = suggest_columns('burden', df)
  ward_col = suggest_columns('ward', df)
  top = df.nlargest(10, burden_col)[[ward_col, burden_col]]
  print(top.to_string(index=False))
Interpret: Name the top wards, note the range, suggest which areas
need attention.

### Pattern: Comparison
User: "Compare TPR across facility levels"
Code:
  level_col = suggest_columns('facility level', df)
  tpr_col = suggest_columns('TPR', df)
  result = df.groupby(level_col)[tpr_col].agg(['mean','median','count'])
  print(result.sort_values('mean', ascending=False))
Interpret: Note which level has highest values, whether sample sizes
are comparable.

### Pattern: Correlation
User: "Is rainfall related to malaria burden?"
Code:
  from scipy.stats import pearsonr
  col_a = suggest_columns('rainfall', df)
  col_b = suggest_columns('burden', df)
  clean = df[[col_a, col_b]].dropna()
  r, p = pearsonr(clean[col_a], clean[col_b])
  print(f"Pearson r={r:.3f}, p={p:.4f}")
  print(f"{'Significant' if p < 0.05 else 'Not significant'} correlation")
Interpret: r > 0.5 strong, 0.3-0.5 moderate, < 0.3 weak.

### Pattern: Distribution
User: "Show me the distribution of burden"
Code:
  col = suggest_columns('burden', df)
  fig = px.histogram(df, x=col, nbins=30, title=f'Distribution of {col}')
  plotly_figures.append(fig)
  print(f"Mean: {df[col].mean():.1f}, Median: {df[col].median():.1f}")
  print(f"Range: {df[col].min():.1f} to {df[col].max():.1f}")
Interpret: Note skewness, compare mean vs median, flag outliers.

### Pattern: Statistical Test
User: "Is there a significant difference between LGAs?"
Code:
  from scipy.stats import kruskal
  lga_col = suggest_columns('LGA', df)
  val_col = suggest_columns('burden', df)
  groups = [g[val_col].dropna().values for _, g in df.groupby(lga_col)]
  stat, p = kruskal(*groups)
  print(f"Kruskal-Wallis H={stat:.2f}, p={p:.4f}")
  print(f"{'Significant' if p < 0.05 else 'No significant'} difference between LGAs")
Interpret: p < 0.05 means significant. Identify which groups differ.
```

#### Final Reminders (BOTTOM of prompt — recency effect)
```
## Final Reminders
- Always use print() for every result — code without print() produces
  no visible output.
- Always use tools to answer data questions — never guess from memory.
- Never fabricate numbers. Present real data, then interpret.
- If something fails, try again with a different approach.
- Use suggest_columns() when unsure about exact column names.
```

---

## Part 4: Tool Docstring Update

File: `app/agent/tools/python_tool.py:22-32`

```python
"""Execute Python code for data analysis on the loaded dataset.

Use this for any data question: rankings, statistics, comparisons,
correlations, regressions, clustering, visualizations.

Args:
    thought: Your reasoning about what to analyze, which columns to
             use, and what output to expect.
    python_code: Python code to execute.

Rules:
- MUST use print() for all outputs — code without print produces nothing
- Data available as: df (primary), ts_df (time series), uploaded_df (original)
- Plotly figures: append to plotly_figures list (auto-displayed)
- Helpers: top_n(), ensure_numeric(), suggest_columns(), capture_table(),
  run_trend_analysis(), create_map()
- Libraries: pandas, numpy, scipy.stats, sklearn, plotly, matplotlib, geopandas
- Timeout: 60 seconds — for large data use df.sample() or df.head()
"""
```

---

## Part 5: Future Roadmap (NOT this session)

| Item | Impact | Effort |
|------|--------|--------|
| Streaming responses | Highest UX impact | Route handler + frontend |
| Proactive insights | Makes agent assistive | Post-tool LLM node |
| Smart tool output truncation | Saves tokens | Modify _smart_truncate |
| Multi-step workflows | Complex queries | Wire planner node |
| Temperature per-phase | Better tone | Two LLM calls |

---

## Implementation Order

1. Config changes (Part 1) — 7 values, one commit
2. Fix guardrails (Part 2) — state.py + rewrite _tools_node, one commit
3. System prompt rewrite (Part 3) — one file, one commit
4. Tool docstring (Part 4) — one file, one commit
5. Test locally with Kwara data

---

## Test Plan

### Happy path (verify improvements):
1. "What's the correlation between rainfall and Burden?" — should use scipy
2. "Show me the distribution of TPR" — should create histogram
3. "Compare primary vs secondary facilities" — should groupby + compare
4. "Which wards are outliers?" — should compute z-scores
5. "Run a regression predicting Burden" — should use sklearn
6. "What does this data tell us about malaria risk?" — exploratory analysis
7. Long analysis request — response should NOT be cut short

### Error recovery (verify retries):
8. Ask about a column that doesn't exist — should retry with suggest_columns()
9. Request analysis that times out — should simplify and retry

### Guardrail verification:
10. Trigger multiple consecutive errors — should stop after limit, not loop
11. Check logs for tool_call_count incrementing correctly

---

## Issues Addressed From Review #2

| Issue | Severity | Resolution |
|-------|----------|------------|
| Guardrail mutation won't work with LangGraph | Critical | Rewrote _tools_node to RETURN updated state |
| Few-shot column names cause KeyErrors | High | Changed to suggest_columns() pattern + disclaimer |
| Error detection too broad | Medium | Use precise ⚠️ marker, not generic string match |
| Tool call limit (5) vs "try 3 approaches" | Medium | Raised limit to 10 |
| Duplicated instructions waste tokens | Medium | Remove from middle, keep in Final Reminders |
| Temperature 0.1 may be robotic | Medium | Noted as trade-off to monitor |
| 90s timeout vs infrastructure | Low | Noted dependency on nginx/frontend timeouts |
| Test plan missing negative cases | Low | Added error recovery + guardrail test cases |
| Token budget interaction | High | Acknowledged cost (~$0.10-0.25 per complex session) |
