# Agent Improvement Plan — Final Version

## Date: April 7, 2026
## Status: READY FOR REVIEW — Do not implement until approved
## Sources: OpenAI GPT-4.1 Guide, GPT-5.4 Guide, PandasAI, Julius AI, 
##          Open Interpreter, academic research (Liu et al. 2024)

---

## Research-Backed Design Decisions

### Prompt Section Order (from OpenAI + "Lost in the Middle" research)
Models pay strongest attention to the BEGINNING and END of prompts,
with 30% accuracy drop for middle content. Therefore:
- Critical rules at TOP (error recovery, tool use)
- Few-shot examples AFTER core instructions
- Final reminders at BOTTOM (repeat most-violated rules)

### Few-Shot Examples (from research)
- 2-5 examples is optimal (diminishing returns beyond 5)
- Last example gets most weight (recency bias)
- Format must be identical across all examples
- Show: user question → code → output → interpretation

### Temperature (from reviewer + research)
- 0.1 for data analysis (deterministic code generation)
- 0.7 causes hallucinated column names → KeyError loops

### Error Recovery (from OpenAI GPT-4.1 guide)
- "Without retry instructions, models often give up after a single failure"
- Must explicitly say: "retry at least once, try alternative approach"

---

## Part 1: Config Changes

| Setting | Current | New | File:Line | Why |
|---------|---------|-----|-----------|-----|
| temperature | 0.7 | 0.1 | agent.py:68 | Deterministic code, fewer hallucinated columns |
| max_tokens | 2,000 | 8,000 | agent.py:69 | Stop cutting responses short |
| LLM timeout | 50s | 90s | agent.py:70 | Must exceed executor timeout |
| Executor timeout | 20s | 60s | executor_simple.py:478 | Complex analysis needs more time |
| Message context | 10 | 20 | agent.py:393 | Better conversation memory |
| Data profile | 12KB | 20KB | agent.py:249 | See more of large datasets |

---

## Part 2: Fix Broken Guardrails

### 2.1 Add state fields
File: `app/agent/state.py`
```python
# Add to DataAnalysisState TypedDict:
tool_call_count: int           # Tracks total tool calls
consecutive_error_count: int   # Tracks consecutive failures
```

### 2.2 Increment in _tools_node (not _route_from_tools)
File: `app/agent/agent.py` — `_tools_node` method
```python
# After tool execution:
state['tool_call_count'] = state.get('tool_call_count', 0) + 1

# Check if result is an error:
if 'error' in str(tool_result).lower():
    state['consecutive_error_count'] = state.get('consecutive_error_count', 0) + 1
else:
    state['consecutive_error_count'] = 0  # Reset on success
```

---

## Part 3: System Prompt Rewrite

### Structure (following OpenAI recommended order):
```
1. Role and Identity                    [TOP — primacy effect]
2. Error Recovery                       [TOP — most critical behavior]
3. Output Management                    [TOP — quality control]
4. Guiding Principles                   [CORE — behavioral rules]
5. Tooling                              [CORE — what tools exist, how to use]
6. Tool Selection Table                 [CORE — when to use which tool]
7. Malaria Domain Knowledge             [DOMAIN — compact reference tables]
8. Analysis Patterns (few-shot)         [EXAMPLES — 5 concrete patterns]
9. Column Interpretation                [DETAIL — data-specific rules]
10. Response Style                      [STYLE — output formatting]
11. Trend Analysis                      [SPECIALTY — specific workflow]
12. Data Pipeline Order                 [REFERENCE — prerequisite chain]
13. Final Reminders                     [BOTTOM — recency effect]
```

### 3.1 EXACT PROMPT TEXT — New sections to add:

#### Section: Error Recovery (insert at TOP, after Role)
```
## Error Recovery
- If code fails, read the error carefully, fix the issue, and retry.
  Try at least 3 different approaches before telling the user you
  cannot do something.
- KeyError? Check the data profile for the exact column name — names
  are case-sensitive.
- ValueError on numeric conversion? Use pd.to_numeric(col, errors='coerce')
  and report how many values could not be converted.
- Timeout? Simplify — use df.sample(1000) or reduce groupby categories.
- Visualization fails? Try a simpler chart type before giving up.
- NEVER respond with just "there was an error." Always explain what
  went wrong and what you're trying next.
- NEVER fabricate data or statistics. If you cannot find the answer
  in the data, say so explicitly.
```

#### Section: Output Management (insert after Error Recovery)
```
## Output Management
- For DataFrames with more than 20 rows, show the top 10 with a
  summary (total count, min, max, mean).
- When results are large, summarize key findings in 3-5 bullet points
  and offer to show full details.
- Always use print() for every result. Code without print() produces
  no visible output.
- After getting results, sanity-check: Do numbers make sense? Are
  percentages between 0-100? If something looks off, investigate
  before presenting.
```

#### Section: Malaria Domain Knowledge (NEW — compact reference tables)
```
## Malaria Domain Knowledge

### TPR Interpretation
- TPR > 50%: Very high transmission — urgent intervention needed
- TPR 25-50%: High transmission — priority for resource allocation
- TPR 10-25%: Moderate transmission — sustained control measures
- TPR < 10%: Low transmission — approaching pre-elimination

### Key Epidemiological Rules
- Increasing TPR = WORSENING (more positive relative to tested)
- Decreasing TPR = IMPROVING
- TPR is more reliable than raw case counts (adjusts for testing volume)
- Ward-level analysis reveals hotspots masked by LGA aggregation
- Malaria transmission follows rainfall with 4-6 week lag

### Nigerian Admin Hierarchy
Country → State → LGA (Local Government Area) → Ward → Health Facility

### Interventions
- ITN: insecticide-treated nets (>80% coverage target)
- IRS: indoor residual spraying (>85% structure coverage)
- ACTs: artemisinin-based combination therapy
- Reprioritization: reallocating resources to highest-burden areas
```

#### Section: Analysis Patterns (NEW — 5 few-shot examples)
```
## Analysis Patterns

When users ask common questions, follow these patterns:

### Pattern: Ranking
User: "Which wards have the highest malaria burden?"
Code: top = df.nlargest(10, 'Burden')[['WardName', 'Burden', 'LGA']]
      print(top.to_string(index=False))
Interpret: Name the top wards, note the range, suggest which LGAs
need attention.

### Pattern: Comparison
User: "Compare TPR across facility levels"
Code: result = df.groupby('Facility level')['TPR'].agg(['mean','median','count'])
      print(result.sort_values('mean', ascending=False))
Interpret: Note which level has highest TPR, whether sample sizes
are comparable, what this means for resource allocation.

### Pattern: Correlation
User: "Is rainfall related to TPR?"
Code: from scipy.stats import pearsonr
      r, p = pearsonr(df['rainfall'].dropna(), df['TPR'].dropna())
      print(f"Pearson r={r:.3f}, p={p:.4f}")
Interpret: r > 0.5 strong, 0.3-0.5 moderate, < 0.3 weak.
Report significance (p < 0.05).

### Pattern: Distribution
User: "Show me the distribution of Burden"
Code: fig = px.histogram(df, x='Burden', nbins=30,
           title='Distribution of Malaria Burden per 1,000')
      plotly_figures.append(fig)
      print(f"Mean: {df['Burden'].mean():.1f}, Median: {df['Burden'].median():.1f}")
      print(f"Range: {df['Burden'].min():.1f} to {df['Burden'].max():.1f}")
Interpret: Note skewness, compare mean vs median, flag outliers.

### Pattern: Statistical Test
User: "Is there a significant difference between LGAs?"
Code: from scipy.stats import kruskal
      groups = [g['Burden'].values for _, g in df.groupby('LGA')]
      stat, p = kruskal(*groups)
      print(f"Kruskal-Wallis H={stat:.2f}, p={p:.4f}")
Interpret: p < 0.05 means significant difference between LGAs.
Identify which LGAs drive the difference.
```

#### Section: Confidence & Persistence (add to Guiding Principles)
```
- You are a capable data analyst. When the user asks a question,
  proactively analyze the data and present findings. Do not ask
  unnecessary clarifying questions — make reasonable assumptions
  and state them.
- Keep going until the user's query is completely resolved. Do not
  give up after one attempt.
```

#### Section: Tooling update (expand existing)
```
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

#### Section: Final Reminders (add at BOTTOM of prompt)
```
## Final Reminders
- Always use tools to answer data questions — never guess from memory.
- Always use print() — no output = no result.
- Never fabricate numbers. Present real data, then interpret.
- If something fails, try again with a different approach.
```

#### Section: Tool Selection Table (replace existing text list)
```
### Tool Selection

| User Intent | Tool | When |
|---|---|---|
| Rankings, statistics, charts | analyze_data | Any data question |
| Map any variable | create_variable_map | "Map X", "show Y distribution" |
| Risk classification map | create_vulnerability_map | AFTER run_risk_analysis |
| Full risk analysis | run_risk_analysis | Creates unified dataset |
| ITN allocation | plan_itn_distribution | AFTER run_risk_analysis |
| Trends over time | analyze_data + run_trend_analysis() | "trends", "improving" |
| Switch data subset | switch_tpr_combination | Change facility/age |

NEVER call a downstream tool before its prerequisites are complete.
```

---

## Part 4: Tool Docstring Update

File: `app/agent/tools/python_tool.py:22-32`

Current:
```python
"""Execute Python code for data analysis

Args:
    thought: Internal thought about what analysis to perform and why
    python_code: Python code to execute. Use print() to show outputs.
                 Data is available as 'df'.
"""
```

New:
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
- Helpers: top_n(), suggest_columns(), capture_table(), run_trend_analysis()
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
| Variable display | User awareness | Frontend sidebar |

---

## Implementation Order

1. Config changes (Part 1) — 6 values, one commit
2. Fix guardrails (Part 2) — state.py + agent.py, one commit
3. System prompt rewrite (Part 3) — one file, one commit
4. Tool docstring (Part 4) — one file, one commit
5. Test locally with Kwara data — full workflow + new analysis questions

---

## Test Plan

After implementation, test these questions to verify improvement:

1. "What's the correlation between rainfall and Burden?" — should use scipy
2. "Show me the distribution of TPR" — should create histogram
3. "Compare primary vs secondary facilities" — should groupby + compare
4. "Which wards are outliers?" — should compute z-scores
5. "Run a regression predicting Burden from environmental variables" — should use sklearn
6. "What does this data tell us about malaria risk?" — should do exploratory analysis
7. Ask a question that causes a KeyError — should retry, not give up
8. Ask for a very long analysis — response should NOT be cut short
