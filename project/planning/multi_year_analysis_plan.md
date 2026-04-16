# Multi-Year Analysis Plan v2
*Written: 2026-04-14 — reviewed and corrected after independent code review*
*Review found 4 blockers + 3 gaps + 2 incorrect assumptions — all fixed in this version*

---

## What We Are Building

When a user uploads multi-year DHIS2 data (e.g. Kwara 2020–2025), the TPR workflow
currently collapses all years into one aggregate burden number. Everything downstream
(risk analysis, vulnerability maps, ITN) sees only that aggregate.

After this feature:
- TPR workflow auto-detects multi-year data — no user action needed
- After age group selection, everything computes automatically:
  - Per-year burden (tpr_time_series.csv extended with Burden)
  - Per-year raw_data_YEAR.csv files (env variables static — just swap Burden column)
  - Trend summary (slope, direction, classification per ward)
  - Per-year composite + PCA risk analysis in background thread
  - Per-year unified_dataset_YEAR.csv (output of background thread)
- Completion message shows year-by-year burden table
- Agent answers year-specific questions from pre-computed files
- Single-year data (Adamawa): identical to today — zero changes to that path

---

## What Does NOT Change

- The three TPR workflow steps (state → facility → age): unchanged
- Single-year data flow: unchanged end-to-end
- PCA and composite being independent of each other: unchanged
- `.risk_ready` flag: unchanged
- ITN hard dependency on risk analysis: unchanged
- `raw_data.csv` (aggregate): still produced exactly as today
- `unified_dataset.csv` (aggregate): still produced exactly as today
- All existing file names (no renames — year-specific files are purely additive)

---

## Key Design Decisions

**year_tag pattern:** All new per-year files use a suffix: `_2020`, `_2021`, etc.
When `year_tag=''` (default everywhere), all existing code paths produce identical output.
This guarantees backward compatibility with zero regression risk.

**Synchronous vs background:**
- Synchronous (before completion message): TPR per year, per-year raw_data files, trend
  — pure pandas, fast, no LLM/raster calls
- Background daemon thread (same pattern as precompute_service.py): composite + PCA per year
  — slow, cannot block HTTP response

**Status tracking:** `multi_year_status.json` written by background thread.
Agent reads this to know what's ready before answering year-specific questions.

**ITN per year:** Agent tool loads `unified_dataset_{year}.csv` from disk and sets it on
a fresh DataHandler before calling the ITN pipeline. ITN pipeline itself unchanged.

**Map generation per year:** In scope — Phase 7 of this plan. On-demand: when user
asks "create vulnerability map", agent generates maps for ALL ready years + one
comparison grid HTML, returns all in the existing `visualizations` array. The
`visualization_handler.js` carousel already handles multiple files with prev/next
navigation — zero frontend changes needed. Comparison grid is a static HTML file with
a CSS grid of iframes pointing to each year's map file.

---

## Corrections from Code Review

The following issues were found in v1 and are fixed in this version:

**Blocker 1 — `DataHandler.load_cleaned_data()` does not exist.**
Fixed: Add a new public `load_raw_data(year_tag='')` method to DataHandler that
wraps the existing raw_data loading logic at line 666 with the filename parameterized.

**Blocker 2 — Ward name key mismatch in `add_burden_to_timeseries()`.**
`calculate_ward_tpr_timeseries()` stores normalized names (via `normalize_ward_name()`)
in the WardName column. `raw_data.csv` stores display names from the shapefile.
A direct merge on 'WardName' produces all-NaN Burden columns.
Fixed: Apply `normalize_ward_name()` to `raw_data['WardName']` before merging,
then drop the normalized key column after the merge.

**Blocker 3 — Schema scoping problem in workflow_manager.py multi-year block.**
`self.tpr_analyzer._schema` is not guaranteed to be in scope at the insertion point.
Fixed: Load `column_schema` from `DataAnalysisStateManager` (same as analysis_tool.py
does at line 971), not from `self.tpr_analyzer._schema`.

**Blocker 4 — `dataset_builder.py` has 7 hardcoded paths, not 2.**
v1 only modified lines 1473 and 1622. Full enumeration:
- Line 1463: `unified_dataset.geoparquet`
- Line 1473: `unified_dataset.csv`
- Line 1488: `unified_dataset.pkl`
- Line 1622: `unified_dataset.csv` (load function)
- Line 1623: `unified_dataset.geoparquet` (load function)
- Line 2109: `unified_dataset.csv` (_save_settlement_free_dataset)
- Line 2116: `unified_dataset.pkl` (_save_settlement_free_dataset)
Fixed: All 7 paths parameterized with year_tag.

**Gap 5 — `build_unified_dataset()` module-level function doesn't propagate year_tag.**
`pipeline.py` line 395 calls `build_unified_dataset(session_id)`. The plan's
year_tag never reached the actual save. Fixed: add `year_tag=''` to
`build_unified_dataset()` signature and the call at pipeline.py line 395.

**Gap 6 — PCA instantiation at line 770 not listed as a change.**
`PCAAnalysisPipeline(session_id)` at line 770 must become
`PCAAnalysisPipeline(session_id, year_tag=year_tag)`. Fixed: explicitly listed.

**Gap 7 — ITN does not read from file — reads `data_handler.unified_dataset` in memory.**
`itn_pipeline.py` reads `data_handler.unified_dataset` (set by pipeline.py line 403),
not from disk. Fixed: agent tool loads `unified_dataset_{year}.csv` from disk and
injects into a fresh DataHandler before calling ITN.

**Wrong location 8 — Year table goes at lines 788-792, not in `_format_tpr_results()`.**
The actual user-visible completion message is built by `MessageFormatter.format_tool_tpr_results()`
in `app/agent/formatters.py` line 192. The modification point is the block at
workflow_manager.py lines 788-792, not the fallback at `_format_tpr_results()` line 1065.

---

## Dependency Chain

```
uploaded_data.csv
    ↓ [TPR workflow — same 3 steps]
tpr_results.csv          (aggregate, unchanged)
tpr_time_series.csv      (extended with Burden per year)        ← synchronous
raw_data.csv             (aggregate, unchanged)
raw_data_2020.csv ... raw_data_2025.csv                        ← synchronous, fast
trend_summary.csv                                              ← synchronous, fast
multi_year_status.json   (created, status=running)             ← synchronous
    ↓ [background thread — per year then aggregate]
composite_scores_{year}.csv
vulnerability_rankings_{year}.csv
analysis_vulnerability_rankings_pca_{year}.csv  (if PCA passes)
unified_dataset_{year}.csv
multi_year_status.json   (updated as each year completes)
    ↓ [agent serves on demand from pre-computed files]
Year-specific vulnerability maps  (Phase 7 — on demand, all years at once)
Year-specific PCA maps            (Phase 7 — on demand, all years at once)
Year-specific ITN allocation
Year comparison grid HTML         (Phase 7 — appended as last visualization)
```

---

## Files Modified (11 files)

### 1. `app/tpr/utils.py`
Add one new function after `calculate_ward_tpr_timeseries()`:

```python
def add_burden_to_timeseries(ts_df: pd.DataFrame, ward_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ward-level population from calculate_ward_tpr() output into
    timeseries and compute per-year Burden.

    IMPORTANT: calculate_ward_tpr_timeseries() stores normalized WardName
    (via normalize_ward_name()). ward_df from calculate_ward_tpr() also uses
    normalized names. Both sides are consistent — merge is safe.

    Args:
        ts_df:   Output of calculate_ward_tpr_timeseries() —
                 WardName (normalized), LGA, Period, Total_Positive,
                 Total_Tested, TPR
        ward_df: Output of calculate_ward_tpr() or tpr_results.csv —
                 WardName (normalized), Population
    Returns:
        ts_df with Population and Burden columns added
    """
    pop = ward_df[['WardName', 'Population']].drop_duplicates('WardName')
    merged = ts_df.merge(pop, on='WardName', how='left')
    merged['Burden'] = (
        merged['Total_Positive'] / merged['Population'].replace(0, np.nan)
    ) * 1000
    return merged
```

Note: Both ts_df.WardName and ward_df.WardName use normalized names from
`normalize_ward_name()`. Merge is safe. No normalization step needed here.

The ward name mismatch risk is in the per-year raw_data_YEAR.csv construction
in workflow_manager.py — handled there (see item 8 below).

---

### 2. `app/services/data_handler.py`
Add new public method (fixes Blocker 1):

```python
def load_raw_data(self, year_tag: str = '') -> pd.DataFrame:
    """
    Load raw_data{year_tag}.csv from session folder into self.cleaned_data.
    year_tag='' loads aggregate raw_data.csv (existing behaviour).
    year_tag='_2024' loads raw_data_2024.csv.
    """
    filename = f'raw_data{year_tag}.csv'
    raw_path = os.path.join(self.session_folder, filename)
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f'{filename} not found in {self.session_folder}')
    self.cleaned_data = pd.read_csv(raw_path)
    logger.info(f'DataHandler loaded {filename}: {self.cleaned_data.shape}')
    return self.cleaned_data
```

Also update the existing raw_data.csv read at line 667 to be consistent with this
method (no change to signature — existing callers unaffected).

---

### 3. `app/services/dataset_builder.py`
Add `year_tag: str = ''` to `UnifiedDatasetBuilder.__init__()` and to the module-level
`build_unified_dataset()` function. Parameterize all 7 hardcoded paths:

| Line | Current | New |
|------|---------|-----|
| 1463 | `'unified_dataset.geoparquet'` | `f'unified_dataset{year_tag}.geoparquet'` |
| 1473 | `'unified_dataset.csv'` | `f'unified_dataset{year_tag}.csv'` |
| 1488 | `'unified_dataset.pkl'` | `f'unified_dataset{year_tag}.pkl'` |
| 1622 | `'unified_dataset.csv'` | `f'unified_dataset{year_tag}.csv'` |
| 1623 | `'unified_dataset.geoparquet'` | `f'unified_dataset{year_tag}.geoparquet'` |
| 2109 | `'unified_dataset.csv'` | `f'unified_dataset{year_tag}.csv'` |
| 2116 | `'unified_dataset.pkl'` | `f'unified_dataset{year_tag}.pkl'` |

Module-level function:
```python
# Before
def build_unified_dataset(session_id: str) -> ...:

# After
def build_unified_dataset(session_id: str, year_tag: str = '') -> ...:
```

---

### 4. `app/analysis/pipeline.py`
Two changes:

**a) Function signature** (line 28):
```python
def run_full_analysis_pipeline(..., session_id=None, year_tag='', ...):
```

**b) 4 output file paths** (fixes blocker 4 partial, and gap 5):
- Line 363: `f'composite_scores{year_tag}.csv'`
- Line 368: `f'model_formulas{year_tag}.csv'`
- Line 379: `f'vulnerability_rankings{year_tag}.csv'`
- Line 203: `f'unified_variable_metadata{year_tag}.json'`

**c) Call to build_unified_dataset** (line 395, fixes gap 5):
```python
# Before
build_unified_dataset(session_id)

# After
build_unified_dataset(session_id, year_tag=year_tag)
```

---

### 5. `app/analysis/pca_pipeline.py`
Three changes:

**a) `PCAAnalysisPipeline.__init__()` signature** (line 35):
```python
def __init__(self, session_id: str, year_tag: str = ''):
    self.year_tag = year_tag
    ...
```

**b) Instantiation** (line 770, fixes gap 6 — explicitly listed):
```python
# Before
pca_pipeline = PCAAnalysisPipeline(session_id)

# After
pca_pipeline = PCAAnalysisPipeline(session_id, year_tag=year_tag)
```

**c) 4 output file paths** (lines 674, 679, 683, 688):
- `f'analysis_vulnerability_rankings_pca{self.year_tag}.csv'`
- `f'analysis_pca_scores{self.year_tag}.csv'`
- `f'pca_variable_importance{self.year_tag}.json'`
- `f'pca_explained_variance{self.year_tag}.json'`

**d) `run_independent_pca_analysis()` signature** (line 749):
```python
def run_independent_pca_analysis(..., year_tag: str = ''):
```
Pass `year_tag=year_tag` to `PCAAnalysisPipeline(session_id, year_tag=year_tag)`.

---

### 6. `app/analysis/engine.py`
Add `year_tag=''` to both functions, pass through:

```python
def run_composite_analysis(self, session_id, variables=None, year_tag=''):
    run_full_analysis_pipeline(..., year_tag=year_tag)

def run_pca_analysis(self, session_id, variables=None, year_tag=''):
    run_independent_pca_analysis(..., year_tag=year_tag)
```

All existing callers pass no `year_tag` → default `''` → identical output.

---

### 7. `app/tpr/workflow_manager.py`

**a) Multi-year block** inserted after existing TPR tool call completes.

Schema loaded from state manager (fixes Blocker 3):
```python
# Load schema from state manager — same pattern as analysis_tool.py line 971
_sm = self.state_manager
column_schema = (_sm.load_state() or {}).get('column_schema') or {}
period_col = column_schema.get('period')

# Detect temporal mode
years = []
if period_col and period_col in df.columns:
    years = sorted(df[period_col].dropna().unique())

is_multi_year = len(years) > 1

if is_multi_year:
    session_folder = os.path.join('instance', 'uploads', self.session_id)

    # 1. Extend tpr_time_series.csv with Burden
    from app.tpr.utils import calculate_ward_tpr_timeseries, add_burden_to_timeseries
    ts_df = calculate_ward_tpr_timeseries(df, schema=column_schema)
    ward_df = pd.read_csv(os.path.join(session_folder, 'tpr_results.csv'))
    # Both use normalized WardName — merge is safe
    ts_df = add_burden_to_timeseries(ts_df, ward_df)
    ts_df.to_csv(os.path.join(session_folder, 'tpr_time_series.csv'), index=False)

    # 2. Create per-year raw_data_YEAR.csv
    # raw_data.csv WardName = shapefile display names (NOT normalized).
    # ts_df WardName = normalized. Must normalize raw_data before merge.
    from app.tpr.utils import normalize_ward_name
    raw_data = pd.read_csv(os.path.join(session_folder, 'raw_data.csv'))
    raw_data['_ward_key'] = raw_data['WardName'].apply(normalize_ward_name)

    for year in years:
        year_data = ts_df[ts_df['Period'] == year][
            ['WardName', 'Total_Positive', 'Burden']
        ].rename(columns={'WardName': '_ward_key'})
        year_raw = raw_data.drop(
            columns=['Burden', 'Total_Positive'], errors='ignore'
        ).merge(year_data, on='_ward_key', how='left').drop(
            columns=['_ward_key']
        )
        year_raw.to_csv(
            os.path.join(session_folder, f'raw_data_{year}.csv'), index=False
        )

    # 3. Compute trend
    from app.tpr.trend_analyzer import compute_trend
    trend_df = compute_trend(ts_df)
    trend_df.to_csv(os.path.join(session_folder, 'trend_summary.csv'), index=False)

    # 4. Schedule background risk analysis
    from app.tpr.multi_year_service import schedule_multi_year_risk_analysis
    schedule_multi_year_risk_analysis(
        session_id=self.session_id,
        years=years,
        session_folder=session_folder,
        state_manager=self.state_manager,
    )
```

**b) Completion message** injected at lines 788-792 (fixes wrong location 8):
The year table is inserted AFTER `format_tool_tpr_results()` returns, inside the
`if message and message.startswith("## Malaria Burden Analysis Complete"):` block.

```python
if is_multi_year and ts_df is not None and not ts_df.empty:
    # Build year table
    year_rows = []
    prev_burden = None
    for year in years:
        yr_data = ts_df[ts_df['Period'] == year]['Burden']
        burden = round(float(yr_data.mean()), 1) if not yr_data.empty else None
        if burden is None:
            continue
        if prev_burden is None:
            change = '—'
        else:
            delta = burden - prev_burden
            arrow = '↑' if delta > 0 else '↓'
            change = f'{arrow} {abs(delta):.1f}'
        year_rows.append(f'| {year} | {burden} | {change} |')
        prev_burden = burden

    table = (
        '\n\n| Year | Burden per 1,000 | Change |\n'
        '|------|-----------------|--------|\n'
        + '\n'.join(year_rows)
        + '\n\n*Full vulnerability rankings for each year are computing '
        'in the background and will be ready shortly.*\n'
    )
    # Insert table after the aggregate burden line
    message = message.replace(
        f'{state_name}: {avg_burden:.1f}',
        f'{state_name}: {avg_burden:.1f} (2020–{years[-1]} combined){table}'
    )
```

---

### 8. `app/agent/agent.py`
Add `_build_multi_year_context()`, call from `_create_data_summary()`:

```python
def _build_multi_year_context(self, session_folder: str) -> str:
    ts_path = os.path.join(session_folder, 'tpr_time_series.csv')
    if not os.path.exists(ts_path):
        return ''
    try:
        ts_df = pd.read_csv(ts_path)
        if 'Period' not in ts_df.columns:
            return ''
        years = sorted(ts_df['Period'].dropna().unique())
        if len(years) <= 1:
            return ''

        status_path = os.path.join(session_folder, 'multi_year_status.json')
        bg_status = 'unknown'
        year_detail = {}
        if os.path.exists(status_path):
            with open(status_path) as f:
                s = json.load(f)
            bg_status = s.get('status', 'unknown')
            year_detail = s.get('detail', {})

        ready_years = [
            y for y in years
            if os.path.exists(
                os.path.join(session_folder, f'unified_dataset_{y}.csv')
            )
        ]
        trend_ready = os.path.exists(
            os.path.join(session_folder, 'trend_summary.csv')
        )

        ctx = f'\nMULTI-YEAR DATA: {years[0]}–{years[-1]} ({len(years)} years)\n'
        ctx += f'Available years: {list(years)}\n'
        ctx += f'Risk analysis ready for years: {ready_years or "computing in background"}\n'
        ctx += f'Trend summary ready (pre-computed slope/direction per ward): {trend_ready}\n'
        ctx += (
            f'tpr_time_series.csv available for open-ended trend analysis '
            f'(WardName, LGA, Period, Burden, Total_Positive, Total_Tested, TPR)\n'
        )
        ctx += (
            'Use analyze_data tool with tpr_time_series.csv for any trend question: '
            'year comparisons, threshold crossings, volatility, LGA aggregates, '
            'statistical significance, custom time ranges.\n'
        )
        ctx += f'Background computation status: {bg_status}\n'
        if year_detail:
            ctx += f'Per-year status: {year_detail}\n'
        return ctx
    except Exception:
        return ''
```

---

### 9. `app/agent/tools/map_tools.py`
**ITN year-specific routing** (fixes gap 7):

When agent detects a year-specific ITN request (e.g. "allocate ITNs for 2024"),
before calling the ITN pipeline, load the year-specific unified dataset:

```python
def _load_year_specific_unified_dataset(session_folder: str, year: str,
                                         data_handler) -> bool:
    """
    Load unified_dataset_{year}.csv into data_handler.unified_dataset.
    Returns True if loaded, False if not yet available.
    """
    path = os.path.join(session_folder, f'unified_dataset_{year}.csv')
    if not os.path.exists(path):
        return False
    data_handler.unified_dataset = pd.read_csv(path)
    return True
```

If year-specific file not found → check `multi_year_status.json` → return
"Risk analysis for {year} is still computing. Try again in a moment."

Default (no year specified) → uses existing `data_handler.unified_dataset` (aggregate)
unchanged. Current behaviour preserved exactly.

---

### 10. `app/tpr/analysis_tool.py`
Minor change: the existing call to `calculate_ward_tpr_timeseries()` at lines 1009-1016
saves `tpr_time_series.csv` without Burden. The workflow_manager multi-year block will
overwrite this file with the Burden-enriched version.

To avoid confusion: wrap the existing analysis_tool.py timeseries save in a comment:
```python
# Note: If multi-year data, workflow_manager.py will overwrite this file
# with an extended version that includes Burden per year.
```
No functional change — just documentation.

---

## Files Created (2 files)

### 1. `app/tpr/trend_analyzer.py` (~140 lines)

**Trend analysis is NOT fixed to one method.** The pre-computed `trend_summary.csv`
answers the most common questions instantly. Everything else is handled dynamically
by the agent's `analyze_data` tool executing arbitrary pandas against `tpr_time_series.csv`.

#### What trend_summary.csv pre-computes (quick lookup for common questions):
- `Slope` — linear regression slope across all years (burden change per year)
- `Direction` — 'worsening' / 'improving' / 'stable' (based on slope)
- `Delta_Latest` — year-over-year change for the most recent year
- `Burden_First` — burden in the earliest year
- `Burden_Latest` — burden in the most recent year
- `Years_Count` — number of years with data

Direction thresholds are named constants (not magic numbers):
```python
WORSENING_SLOPE_THRESHOLD = 5.0   # burden increase per year
IMPROVING_SLOPE_THRESHOLD = -5.0
```

#### What the agent handles dynamically via analyze_data tool:
Any question beyond the pre-computed summary is answered by the agent writing
pandas code against `tpr_time_series.csv` (WardName, LGA, Period, Burden):

- "What happened between 2022 and 2023?" → filter two years, compute delta
- "Which wards had the biggest jump last year?" → sort by Delta between last 2 periods
- "Is the trend statistically significant?" → scipy.stats.linregress per ward
- "Which wards crossed above 500 per 1,000 in the last 3 years?" → threshold filter
- "Show me volatile wards" → compute std deviation of Burden across years
- "LGA-level trend" → groupby LGA, aggregate burden per year
- "State-level summary over time" → groupby Period, sum/mean
- "Compare 2020 to 2025 for all wards" → pivot, compute difference
- "Which wards were high risk in 2020 but improved by 2025?" → merge two year slices

The agent already does this for other analyses. The only requirement:
`tpr_time_series.csv` must be in the session folder and the agent must know it exists.

#### Functions:
- `compute_trend(ts_df) -> DataFrame` — produces trend_summary.csv
- `identify_emerging_hotspots(ts_df, threshold=HOTSPOT_THRESHOLD) -> DataFrame`
- `identify_resolving_hotspots(ts_df, threshold=HOTSPOT_THRESHOLD) -> DataFrame`

```python
WORSENING_SLOPE_THRESHOLD = 5.0
IMPROVING_SLOPE_THRESHOLD = -5.0
HOTSPOT_THRESHOLD = 200.0  # burden per 1,000 — high risk cutoff
```

No external dependencies beyond numpy and pandas.

---

### 2. `app/tpr/multi_year_service.py` (~140 lines)

Background daemon thread — same pattern as `precompute_service.py`.
No Flask app context needed (confirmed by reviewing precompute_service.py).

Key behaviour:
- Creates fresh `DataHandler` per year (avoids shared state between years)
- Calls `data_handler.load_raw_data(year_tag=year_tag)` (new method from item 2)
- Calls `engine.run_composite_analysis(session_id, year_tag=year_tag)`
- Calls `engine.run_pca_analysis(session_id, year_tag=year_tag)`
- Writes `multi_year_status.json` after each year (running/complete/failed/skipped)
- Processes years in chronological order, aggregate (year_tag='') last
- Per-year failure does not stop other years — try/except per year

Status file structure:
```json
{
  "status": "running|complete|failed",
  "years_total": 7,
  "years_complete": 3,
  "detail": {
    "_2020": "complete",
    "_2021": "complete",
    "_2022": "complete",
    "_2023": "running",
    "_2024": "pending",
    "_2025": "pending",
    "": "pending"
  }
}
```

---

## Runtime Files Produced

```
instance/uploads/{session_id}/
│
│   [synchronous — ready when completion message appears]
│
├── tpr_time_series.csv         ← EXTENDED: now includes Burden per year
├── raw_data_2020.csv           ← new: year-specific Burden, same env vars
├── raw_data_2021.csv
├── raw_data_2022.csv
├── raw_data_2023.csv
├── raw_data_2024.csv
├── raw_data_2025.csv
├── trend_summary.csv           ← new: Slope, Direction, Delta_Latest per ward
├── multi_year_status.json      ← new: background progress tracker
│
│   [background thread — appear as computation completes, years in order]
│
├── composite_scores_2020.csv
├── vulnerability_rankings_2020.csv
├── analysis_vulnerability_rankings_pca_2020.csv  (if PCA passes)
├── unified_dataset_2020.csv    ← available for maps and ITN when ready
├── ...  (repeated for 2021–2025)
│
│   [all existing files unchanged]
│
├── raw_data.csv                ← aggregate (unchanged)
├── tpr_results.csv             ← aggregate (unchanged)
├── unified_dataset.csv         ← aggregate (unchanged, produced last by background)
```

---

## Completion Message

**Single-year data:** Message identical to today. Zero changes.

**Multi-year data:** Year table inserted after aggregate burden line (at lines 788-792,
inside the format_tool_tpr_results block — NOT in _format_tpr_results() fallback):

```
## Malaria Burden Analysis Complete

Using primary facilities and focusing on children under 5 years, I calculated
malaria burden as cases per 1,000 under-5 population using WorldPop estimates.

Kwara: 572.5 cases per 1,000 population across 193 wards (2020–2025 combined)

| Year | Burden per 1,000 | Change |
|------|-----------------|--------|
| 2020 | 487.3           | —      |
| 2021 | 531.2           | ↑ 43.9 |
| 2022 | 548.7           | ↑ 17.5 |
| 2023 | 598.1           | ↑ 49.4 |
| 2024 | 612.4           | ↑ 14.3 |
| 2025 | 630.2           | ↑ 17.8 |

*Full vulnerability rankings for each year are computing in the background
and will be ready shortly.*

[rest of existing message unchanged]
```

---

---

## Phase 7 — Multi-Year Map Generation

### Design

**Trigger:** User asks for any visualization (vulnerability map, PCA map, ITN map, TPR map).
No special phrasing needed — the agent detects multi-year context from `_build_multi_year_context()`.

**On-demand generation (not pre-generated):**
Background thread handles risk analysis. Maps are generated when user requests them.
If a year's `unified_dataset_{year}.csv` is not yet ready, that year is skipped with a note.

**Output per map request:**
1. `{map_type}_{year}.html` for each ready year (e.g. `vulnerability_map_2020.html`)
2. `{map_type}_comparison.html` — CSS grid showing all year maps side by side

**Carousel handling:** `visualization_handler.js` already paginates multiple HTML files
(dot indicators + prev/next). The year maps appear as pages 1…N, comparison grid as
the final page. No frontend changes.

**Comparison grid HTML format:**
```html
<!DOCTYPE html><html><head><style>
  body { margin: 0; font-family: sans-serif; }
  .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px;
          height: 100vh; padding: 6px; box-sizing: border-box; }
  .cell h3 { text-align: center; margin: 2px 0; font-size: 13px; }
  iframe { width: 100%; height: calc(50vh - 28px); border: 1px solid #ddd; }
</style></head><body>
<div class="grid">
  <!-- repeated per year -->
  <div class="cell">
    <h3>2020</h3>
    <iframe src="/api/visualization/serve_viz_file/{session_id}/vulnerability_map_2020.html"
            sandbox="allow-scripts allow-same-origin"></iframe>
  </div>
  ...
</div>
</body></html>
```

**Map types covered** (all four trigger the same multi-year loop):
- Vulnerability map (composite risk) — reads `unified_dataset_{year}.csv`
- PCA map — reads `analysis_vulnerability_rankings_pca_{year}.csv`
- ITN allocation map — reads `unified_dataset_{year}.csv` (via DataHandler)
- TPR burden map — reads `tpr_time_series.csv`, filters by year

### Files to modify for Phase 7

**`app/agent/tools/map_tools.py`** (already listed for item 9 — extend same file):

Add `_generate_multi_year_maps(map_type, session_id, session_folder, years_ready)`:
```python
def _generate_multi_year_maps(map_type: str, session_id: str,
                               session_folder: str, years_ready: list) -> list[str]:
    """
    Generate {map_type}_{year}.html for each ready year + comparison grid.
    Returns list of viz file paths in order: [year_maps..., comparison_grid].
    """
    viz_paths = []
    for year in years_ready:
        path = _generate_single_year_map(map_type, session_id, session_folder, year)
        if path:
            viz_paths.append(path)
    if len(viz_paths) > 1:
        grid_path = _generate_comparison_grid(map_type, session_id, session_folder,
                                               years_ready, viz_paths)
        viz_paths.append(grid_path)
    return viz_paths
```

`_generate_single_year_map()` calls the existing map generation function for that
map type, passing `year_tag=f'_{year}'` so it reads the correct input file.

The existing map generation functions (`visual_tools.py`, `composite_visualizations.py`,
`pca_visualizations.py`, `itn_pipeline.py`) each need `year_tag=''` added to their
public interface — same pattern as the analysis stack above.

**Note:** Exact function signatures in these files must be confirmed during implementation
by reading each file. The year_tag pattern is consistent with Phase 2.

### Status check before generating

Before generating each year's map, check `unified_dataset_{year}.csv` exists:
```python
status_path = os.path.join(session_folder, 'multi_year_status.json')
if os.path.exists(status_path):
    with open(status_path) as f:
        status = json.load(f)
    years_ready = [y for y, s in status.get('detail', {}).items()
                   if s == 'complete' and y != '']
else:
    years_ready = []
```

If no years ready yet: return "Risk analysis is still computing. Try again in a moment."
If partial (some years ready): generate maps for ready years + note which are pending.

---

## Implementation Order (27 tasks)

### Phase 1 — Foundation (isolated, testable immediately)
1. Create `app/tpr/trend_analyzer.py`
2. Add `add_burden_to_timeseries()` to `app/tpr/utils.py`
3. Write unit tests `tests/tpr/test_multi_year.py` (12 tests — see Tests section)
4. Run tests — all must pass before proceeding

### Phase 2 — year_tag support (backward-compatible, default='')
5. `app/services/data_handler.py` — add `load_raw_data(year_tag='')` method
6. `app/services/dataset_builder.py` — add `year_tag` to all 7 paths + `build_unified_dataset()` signature
7. `app/analysis/pipeline.py` — add `year_tag` param, update 4 file paths + line 395 call
8. `app/analysis/pca_pipeline.py` — add `year_tag` to `__init__` + line 770 instantiation + 4 file paths + `run_independent_pca_analysis()`
9. `app/analysis/engine.py` — add `year_tag` param to both functions, pass through
10. **Run full test suite** (`python -m pytest tests/`) — must all pass before proceeding

### Phase 3 — Background service
11. Create `app/tpr/multi_year_service.py`
12. Unit test: mock engine, verify status file writes at each stage

### Phase 4 — Workflow wiring
13. `app/tpr/workflow_manager.py` — multi-year detection block (schema from state manager)
14. `app/tpr/workflow_manager.py` — completion message year table (at lines 788-792)
15. **Test locally with Kwara:** verify year table in completion message, all raw_data_YEAR.csv files exist, trend_summary.csv exists, multi_year_status.json shows 'running'

### Phase 5 — Agent context + ITN routing
16. `app/agent/agent.py` — add `_build_multi_year_context()`, call from `_create_data_summary()`
17. `app/agent/tools/map_tools.py` — year-specific ITN routing with `_load_year_specific_unified_dataset()`

### Phase 6 — Integration verification
18. **Full Kwara end-to-end:** upload → workflow → completion message → wait for background → verify all unified_dataset_YEAR.csv files → ask agent year-specific questions → ask for year-specific ITN
19. **Adamawa regression:** upload → workflow → verify completion message identical to today → verify no multi-year files created → verify all existing tests pass
20. **Run full test suite** — all must pass

### Phase 7 — Multi-Year Map Generation
21. **Confirm exact function signatures** in `visual_tools.py`, `composite_visualizations.py`, `pca_visualizations.py`, `itn_pipeline.py` that generate choropleth HTML — read each file to find the public entry point
22. **Add `year_tag=''`** to those 4 map-generating functions (same pattern as Phase 2)
23. **Extend `app/agent/tools/map_tools.py`** — add `_generate_multi_year_maps()`, `_generate_single_year_map()`, `_generate_comparison_grid()`
24. **Wire multi-year detection in map tool dispatch** — when multi-year context present, call `_generate_multi_year_maps()` instead of single-map path
25. **Test vulnerability maps locally with Kwara** — verify N year maps + comparison grid appear in carousel
26. **Test PCA maps locally with Kwara** — verify same pattern
27. **Test ITN maps locally with Kwara** — verify year-specific routing works end-to-end

---

## Tests (`tests/tpr/test_multi_year.py` — 12 tests)

1. `test_compute_trend_worsening` — steadily increasing burden → Direction='worsening', Slope > 0
2. `test_compute_trend_improving` — steadily decreasing → Direction='improving', Slope < 0
3. `test_compute_trend_stable` — flat burden → Direction='stable'
4. `test_compute_trend_single_year` — only 1 year per ward → returns empty DataFrame
5. `test_compute_trend_slope_threshold` — slope exactly at ±5 boundary → classified correctly
6. `test_add_burden_correct_calculation` — (positives/population) × 1000 = expected Burden
7. `test_add_burden_missing_ward` — ward in ts_df not in ward_df → NaN Burden, no crash
8. `test_add_burden_normalized_names_match` — both sides use normalized names → merge produces non-NaN results
9. `test_emerging_hotspots` — ward below 200 in year 1, above in year N → in output
10. `test_resolving_hotspots` — ward above 200 in year 1, below in year N → in output
11. `test_multi_year_detection_positive` — 6-year fixture → is_multi_year=True
12. `test_year_tag_backward_compat` — engine called with year_tag='' → output file names unchanged (composite_scores.csv not composite_scores_.csv)

---

## Risks and Mitigations

| Risk | Severity | Mitigation |
|------|---------|-----------|
| Ward name mismatch raw_data vs ts_df | HIGH | Normalize raw_data['WardName'] with `_ward_key` before merge — explicitly in plan |
| DataHandler missing method | HIGH | Add `load_raw_data(year_tag='')` — explicitly in plan |
| Schema not in scope in workflow_manager | HIGH | Load from state_manager, same pattern as analysis_tool.py line 971 |
| dataset_builder.py paths missed | HIGH | All 7 paths enumerated explicitly — read full file during Phase 2 step 6 |
| build_unified_dataset() chain broken | HIGH | Add year_tag to function + pipeline.py line 395 call |
| PCA instantiation at line 770 | MEDIUM | Explicitly listed in Phase 2 step 8 |
| ITN reads from memory not file | MEDIUM | Agent tool loads year-specific CSV before calling ITN |
| Background PCA fails for thin years | MEDIUM | Per-year try/except — composite-only fallback, year still usable |
| Aggregate (year_tag='') computed last | LOW | Background processes years first, aggregate last — aggregate always wins if conflict |
| test data uses normalized names | LOW | Test 8 explicitly tests normalized-name matching |

---

## Verification Checklist

After Kwara upload + TPR workflow:
- [ ] Completion message shows year table with 6 rows
- [ ] `tpr_time_series.csv` has `Burden` column (not just TPR ratio)
- [ ] `raw_data_2020.csv` ... `raw_data_2025.csv` all exist
- [ ] `raw_data_YEAR.csv` Burden values differ across years (not all same as aggregate)
- [ ] `trend_summary.csv` exists with Slope, Direction, Delta_Latest columns
- [ ] `multi_year_status.json` exists immediately after completion message
- [ ] `[MULTI_YEAR]` log entries visible in terminal logs

After background completes:
- [ ] `unified_dataset_2020.csv` ... `unified_dataset_2025.csv` all exist
- [ ] `multi_year_status.json` shows `status=complete`
- [ ] Agent correctly answers "which wards are getting worse" from trend_summary.csv
- [ ] Agent correctly routes year-specific ITN request and loads correct unified_dataset

After Phase 7 map generation (Kwara):
- [ ] "create vulnerability map" → N choropleth maps appear in carousel (one per year)
- [ ] Last carousel item is comparison grid (2×3 or N-grid of all years)
- [ ] "create PCA map" → same multi-year carousel pattern
- [ ] "create ITN map" → same pattern + ITN allocations correct per year
- [ ] If background still running → graceful message ("2022 still computing, showing 2020-2021")
- [ ] Single-year prompt like "create vulnerability map for 2023" → returns only 2023 map (no gallery)

Adamawa regression:
- [ ] Completion message identical to today
- [ ] No `raw_data_YEAR.csv` files created
- [ ] No `trend_summary.csv` created
- [ ] No `multi_year_status.json` created
- [ ] "create vulnerability map" → single map (no multi-year carousel)
- [ ] `python -m pytest tests/` all pass

---

## Memory update required after implementation
Update `memory/project_pipeline_architecture.md`:
- tpr_time_series.csv now includes Burden column
- Per-year raw_data_YEAR.csv files produced in multi-year mode
- trend_summary.csv produced in multi-year mode
- multi_year_status.json tracks background computation
- unified_dataset_YEAR.csv produced by background thread
- DataHandler has new `load_raw_data(year_tag='')` method
- All analysis functions accept `year_tag=''` parameter
