# TPR Column Detection — Problem Statement & LLM-Based Solution Proposal

**Author:** Bernard Boateng / Claude
**Date:** 2026-03-12
**Status:** Awaiting review
**Purpose:** Agent review of proposed solution before implementation

---

## 1. Background

ChatMRPT has a multi-step TPR (Test Positivity Rate) workflow that guides users through selecting a state, facility level, and age group before computing ward-level TPR rates. The workflow depends on correctly identifying which column in an uploaded dataset corresponds to which semantic concept (state, LGA, facility name, facility level, period, etc.).

The codebase currently identifies these columns using **keyword-in-name matching** — it scans column names for substrings like `level`, `state`, `facility`, etc. This approach was designed for one internal data format and breaks silently on any other.

---

## 2. The Two Data Formats

### Format A — Standard / NMEP-cleaned (what the system was designed for)

File in repo: `instance/uploads/0483f6d9-.../adamawa_tpr_cleaned.xlsx`

| Column | Semantic meaning | Sample value |
|--------|-----------------|--------------|
| `State` | Nigerian state | `Adamawa` |
| `LGA` | Local Government Area | `Yola South` |
| `WardName` | Ward | `Adarawo` |
| `HealthFacility` | Facility name | `Wuro-Hausa Primary Health Clinic` |
| `FacilityLevel` | Facility tier | `Primary` |
| `periodname` | Reporting period | `2025-01-24` |
| `periodcode` | Period code | `202401` |
| `Persons presenting with fever & tested by RDT <5yrs` | U5 RDT tests | `10` |
| `Persons tested positive for malaria by RDT <5yrs` | U5 RDT positives | `7` |
| ... (18 similar indicator columns) | | |

### Format B — DHIS2 raw export (what the Kwara team sent)

File in repo: `instance/uploads/c9f7cf96-.../data_analysis.xlsx` (uploaded 2026-03-10)

| Column | Semantic meaning | Sample value |
|--------|-----------------|--------------|
| `orgunitlevel2` | Nigerian state | `kw Kwara State` |
| `orgunitlevel3` | **LGA** (NOT facility level) | `kw Ilorin South Local Government Area` |
| `Ward` | Ward | `Unknown` |
| `organisationunit0me` | Facility name (unicode-corrupted `organisationunitname`) | `A Division Police Clinic` |
| `period0me` | Year (integer, corrupted `periodname`) | `2020` |
| `Facility level` | Facility tier | `primary` |
| `Persons presenting with fever & tested by RDT <5yrs` | U5 RDT tests | _(mostly NaN for year 2020)_ |
| ... (same 18 indicators, with some duplicated `.1` columns) | | |

**Notable DHIS2 format quirks:**
- State values are prefixed: `"kw Kwara State"` not `"Kwara"` — string matching against "Kwara" fails
- `orgunitlevel3` contains the word `level` in its column name → triggers false facility level detection
- Facility level values are **lowercase** (`primary`, `secondary`) vs. Format A's title case (`Primary`)
- Period is an integer year (`2020`) not a date string
- Unicode corruption: `organisationunit0me` (name), `period0me`, `Artesu0te` — `n` → `0me` pattern
- Some indicator columns appear twice with `.1` suffix (deduplication needed upstream)

---

## 3. The Bug — Root Cause

**File:** `app/data_analysis_v3/tpr/data_analyzer.py`

The `analyze_facility_levels()` method (line 142–150) identifies the facility level column like this:

```python
level_col = None
for col in df.columns:
    col_lower = col.lower()
    if any(keyword in col_lower for keyword in ['facility', 'level', 'type', 'tier', 'category']):
        unique_vals = df[col].dropna().nunique()
        if 2 <= unique_vals <= 20:
            level_col = col
            break  # first match wins — no value validation
```

On a DHIS2 export, columns appear in this order:
```
orgunitlevel2, orgunitlevel3, Ward, organisationunit0me, period0me, Facility level, ...
```

`orgunitlevel3` is encountered first. Its name contains `level`. It has 16 unique values (the 16 LGAs in Kwara). `2 <= 16 <= 20` passes. **It is selected as the facility level column.**

The UI then displays 16 LGA names ("kw Asa Local Government Area", etc.) as if they were facility types. The user selects one. `_filter_data()` runs the same broken detection, finds `orgunitlevel3`, looks for the user's "facility level" value among LGA names — no match — **returns all 6,948 rows unfiltered.** TPR is computed over the entire dataset regardless of the user's selection.

The same first-match-wins keyword loop is duplicated in three places:
- `analyze_facility_levels()` — lines 142–150
- `_filter_data()` — lines 492–501
- `analyze_states()` — lines 48–58 and 76–82

---

## 4. Why a Keyword Blacklist Is Not the Right Fix

The immediate symptom fix would be: skip columns whose names match `orgunitlevel\d+`. This fixes Kwara. But:

- WHO exports, HMIS exports, state-level custom Excel files all have their own naming conventions
- The next unexpected format would require another blacklist entry
- Column *names* are arbitrary — column *values* are what carry semantic meaning

The underlying design flaw is **trusting column names as ground truth** with no validation of what's actually in the column.

---

## 5. Proposed Solution — LLM-Based Schema Inference

### Concept

Instead of keyword matching, use the LLM (already integrated throughout ChatMRPT) to infer the column schema once at the start of the TPR workflow, then use that inferred schema for all subsequent detection.

```
Dataset uploaded
      ↓
_infer_column_schema(df)   ← ONE LLM call, ~300 tokens
      ↓
schema = {
    "state":          "orgunitlevel2",
    "lga":            "orgunitlevel3",
    "facility_name":  "organisationunit0me",
    "facility_level": "Facility level",
    "period":         "period0me",
    "ward":           "Ward"
}
      ↓
Cache schema in session (keyed by dataset hash)
      ↓
All detection methods read from schema dict
Keyword loop only runs as fallback if LLM returns null for a field
```

### Why the LLM is the right tool here

| Scenario | Keyword matching | LLM inference |
|----------|-----------------|---------------|
| Standard format (`State`, `FacilityLevel`) | ✅ Works | ✅ Works |
| DHIS2 format (`orgunitlevel2`, `orgunitlevel3`) | ❌ Picks wrong column | ✅ LLM knows DHIS2 hierarchy |
| Unicode corruption (`organisationunit0me`) | ❌ Not recognized | ✅ LLM infers from partial match + context |
| French/Portuguese column headers | ❌ Silent failure | ✅ LLM handles multilingual |
| State prefix (`"kw Kwara State"` vs `"Kwara"`) | ❌ String match fails | ✅ LLM returns column, downstream can strip prefix |
| Future format we've never seen | ❌ Requires code change | ✅ Zero-shot generalization |

### Prompt design (minimal)

```python
prompt = f"""
You are analyzing a Nigerian malaria surveillance dataset (could be a DHIS2 export,
HMIS data, NMEP-processed Excel, or custom format).

Column names and sample values (first 3 rows):
{sample_df.to_string()}

Identify the column that corresponds to each of the following semantic fields.
Return null if a field is not present. Return the exact column name as it appears.

Fields to identify:
- state: The Nigerian state name
- lga: Local Government Area
- ward: Ward name
- facility_name: Name of the health facility
- facility_level: Facility tier/type (Primary, Secondary, Tertiary, PHC, etc.)
- period: Reporting period (date, month, year, or period code)

Return JSON only, no explanation:
{{"state": "...", "lga": "...", "ward": "...", "facility_name": "...",
  "facility_level": "...", "period": "..."}}
"""
```

### Caching strategy

`TPRDataAnalyzer` is currently instantiated per-request, so instance-level caching doesn't persist across the multi-step workflow. The schema must be cached at the **session level**, keyed by a hash of the dataset columns (cheap — no need to hash the full file):

```python
cache_key = f"column_schema_{hash(tuple(df.columns))}"
schema = session.get(cache_key)
if not schema:
    schema = self._infer_column_schema(df)
    session[cache_key] = schema
```

This means the LLM is called **once per dataset**, not once per chat message.

### Fallback path

If the LLM call fails (API down, key missing, timeout), fall back to the current keyword loop. This means the feature degrades gracefully — Kwara breaks again, but nothing else regresses.

```python
def _get_column(self, df, field, schema=None):
    """Get column for a semantic field. LLM schema first, keyword fallback."""
    if schema and schema.get(field):
        col = schema[field]
        if col in df.columns:
            return col
    return self._keyword_fallback(df, field)  # existing logic
```

---

## 6. Files Affected

| File | Change needed |
|------|--------------|
| `app/data_analysis_v3/tpr/data_analyzer.py` | Add `_infer_column_schema()`, add `_get_column()` dispatcher, update `analyze_states()`, `analyze_facility_levels()`, `_filter_data()` to use it |
| `app/web/routes/analysis/chat_stream_service.py` | Pass session object to TPRDataAnalyzer, or manage schema cache at this level |
| `app/data_analysis_v3/tpr/workflow_manager.py` | Potentially trigger schema inference at workflow start (before first `analyze_states` call) |

Possibly relevant:
- `app/core/tpr_utils.py` — contains `calculate_ward_tpr()` which also does column detection; same fix applies
- `app/core/llm_manager.py` — existing LLM call pattern to follow for consistency

---

## 7. Open Questions for the Reviewing Agent

1. **Session access**: `TPRDataAnalyzer` currently has no access to the Flask session. The cleanest way to pass the schema cache without threading session into a data class — what's the right pattern in this codebase? Options: pass schema dict as a parameter, use a module-level cache keyed by session_id, or manage it in `workflow_manager.py` one level up.

2. **Schema inference timing**: Should schema inference happen at **upload time** (in the upload route, stored with the session's file metadata) or at **first TPR workflow call** (lazy, triggered by `analyze_states`)? Upload-time is cleaner but requires upload route changes. First-call is more contained.

3. **State value normalization**: DHIS2 state values have prefixes (`"kw Kwara State"`). Even with the correct column detected, downstream code that checks `if state in df[col].values` will fail unless values are normalized. Should normalization be part of schema inference (LLM returns prefix-stripped values), or a separate step?

4. **Scope**: Should this also fix `calculate_ward_tpr()` in `app/core/tpr_utils.py` (Phase 3 in the cleanup plan), or just `TPRDataAnalyzer` first and `tpr_utils.py` separately?

---

## 8. What Is NOT In Scope

- Rewriting the TPR workflow (the multi-step state → facility → age group flow)
- Changes to how data is stored in session after upload
- Any frontend changes
- Phases 3–6 of the codebase cleanup plan (`project/planning/codebase_cleanup_plan.md`)

---

## 9. Related Files for Context

| File | Purpose |
|------|---------|
| `app/data_analysis_v3/tpr/data_analyzer.py` | The file being changed — full column detection logic |
| `app/data_analysis_v3/tpr/workflow_manager.py` | TPR workflow orchestration |
| `app/web/routes/analysis/chat_stream_service.py` | Where TPRDataAnalyzer is instantiated (line ~113) |
| `app/core/tpr_utils.py` | `calculate_ward_tpr()` — also has column detection, same problem |
| `docs/KWARA_TPR_DATA_INVESTIGATION.md` | Full investigation of all 5 Kwara issues |
| `project/planning/codebase_cleanup_plan.md` | 6-phase cleanup plan (this work is Phase 2 Step 3) |
| `instance/uploads/c9f7cf96-.../data_analysis.xlsx` | The actual Kwara DHIS2 file that triggered this |
| `instance/uploads/0483f6d9-.../adamawa_tpr_cleaned.xlsx` | Standard format reference |
