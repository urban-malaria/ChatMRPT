# DHIS2 Data Cleaner — Implementation Plan

**Status:** Planning (v4 — schema-cleaner coordination fixed)
**Branch:** one-brain-migration
**Date:** 2026-04-10
**Author:** ChatMRPT Team

**v2 revisions (11 issues from first review):**
1. Integration point corrected (was wrong path)
2. Second data read path identified (TPR workflow)
3. Rule ordering fixed (identical → ratio, not ratio → identical)
4. Temporal threshold justified and simplified
5. Ratio column conflict handling improved
6. Check 5 logic fixed (was inverted)
7. DHIS2 detection tightened (require 2 malaria keywords)
8. Mojibake whitelist no-op removed
9. Test 7 NaN assertion fixed
10. Feature flag default corrected (off by default)
11. Temporal consistency threshold relaxed for malaria reality

**v3 revisions (4 issues from second review):**
12. THIRD data read path identified at `data_analysis_routes.py:912-943` (TPR workflow start handler — the most common entry point). v2 missed this entirely.
13. Feature flag helper extracted to prevent inconsistency across integration points (log_only mode was only honored in Path A)
14. Rule 4 overlap formula changed from "non-zero" to "non-null" to handle legitimate zero values in DHIS2 exports
15. Test count corrected (Section 16 said "10 tests", actual is 16)
16. Log-only mode advancement criteria defined (Phase 4 now has explicit exit conditions)
17. State-level aggregated exports documented as known limitation

**v4 revisions (schema-cleaner coordination bugs caught by user):**
18. **CRITICAL: Schema-cleaner desync fixed.** v3 had the cleaner running AFTER schema inference. Mojibake renames (e.g., `period0me` → `periodname`) would leave the schema pointing to non-existent columns, breaking the TPR calculation. v4 requires the cleaner to return a `column_rename_map` AND applies that map to the schema at all three integration points.
19. **Rule 5 reverted from `keep_both_renamed` back to `sum`.** v3's safer default (preserving both columns) was actually the MORE DANGEROUS default because renaming a base column referenced by the schema would break TPR. v4 uses sum as the safer default for count columns, accepts the theoretical double-counting risk in the rare ambiguous case, and emits a data_quality_warning when Rule 5 fires.
20. **v4 reverted Rule 4 to non-zero (not non-null).** Empirical test on real Kwara data showed non-null gives 0% non-overlap (both cols have values for same 5778 rows, just zeros for inactive years). Non-zero gives 84.5% and correctly identifies complementarity. v3's reviewer suggestion was theoretically sound but empirically wrong for Kwara's zero-vs-NaN encoding.
21. **Cleaning phase ordering fixed.** Mojibake fix runs BEFORE duplicate detection (not after). This ensures duplicate detection sees clean names and period column detection can find `periodname` rather than `period0me`.
22. **System prompt hardcoded references removed.** `system_prompt.py:86` had `period0me = time period` which is wrong after cleaning (and specific to Kwara even without cleaning). Replaced with generic guidance that points users to the actual data profile.
23. **New Section 10.5: Schema coordination contract** — explicit documentation of how cleaner and downstream consumers stay in sync.
24. **New Section 11.5: Integration test** — manual script to run the full Kwara pipeline (upload → clean → TPR → risk → maps) and verify it works end-to-end. Unit tests alone cannot catch integration bugs like the schema desync.
25. **Upgrade compatibility note added** — sessions created before the cleaner ships will have uncleaned `uploaded_data.csv`. Section 18 documents that users should re-upload for correct results (no auto-migration in v4).

**v4 final review corrections (3 blocking issues + 4 minor):**
26. **Path B/C file-selection excludes `uploaded_data.csv`** — The glob `*.csv` would pick up `uploaded_data.csv` (the cleaned output) as a "raw" candidate, causing double-cleaning or loops. Fixed by filtering out `uploaded_data.csv` from the raw-file candidate list.
27. **Path B fallback persists updated schema** — The fallback applied the cleaner and updated the local schema variable but did NOT persist it back to `state_manager`. Fixed so subsequent requests in the same session get the cleaned schema.
28. **Attribute name consistency** — The plan's pseudocode used `cleaning_report.applied` in one place but the dataclass defines `cleaning_applied`. Fixed to use `cleaning_applied` throughout.
29. **Path A fallback reasoning documented** — Added explicit sentence explaining why the fallback branch (schema inference failure) doesn't need schema update: Path C will re-infer schema from cleaned `uploaded_data.csv` and get clean names naturally.
30. **Test 7 comment clarified** — The comment misrepresented the NaN handling logic. Fixed.
31. **Phase 4 exit criterion reworded** — Rule 5 firing alone is not disqualifying; the criterion is now "any Rule 5 fires are manually reviewed and confirmed not to represent double-counting."
32. **Integration test import guard added** — Wrap the TPR module imports in try/except so a missing function fails with a clear check failure rather than a cryptic ImportError.

---

## 1. Executive Summary

We are building a data cleaning layer that runs after file read and before schema inference. It detects and fixes data quality issues specific to DHIS2 exports — particularly **duplicate column headers** and **mojibake in column names** — that currently cause the TPR workflow, risk analysis, and trend analysis to produce incorrect results.

**Problem validated:** Kwara TPR data produces a "Burden of 100.2 per 1,000" because the schema inference picks the wrong one of two duplicate columns, capturing only 1/6th of the temporal data. After cleaning, the same dataset yields a consistent 6-year time series with TPR trending 77%→61% (showing real malaria improvement in Kwara).

**Prototype verified.** Core algorithm runs correctly on:
- Kwara (raw DHIS2 export with duplicates): 32→28 columns, temporally consistent
- Adamawa (pre-cleaned): 22→22 columns, unchanged (pass-through)
- Non-DHIS2 generic CSVs: unchanged (detection skips)
- Legitimately-dotted column names (e.g., `version_1.0`): not flagged as duplicates

---

## 2. Problem Statement

### What's currently broken

The Kwara TPR workflow produces:
- Burden: 100.2 cases per 1,000 population
- `tpr_time_series.csv`: 181 rows for 2020 + 1 row for 2025 (missing 2021-2024)
- Risk analysis rankings based on 1/6th of actual data
- No meaningful trend analysis possible

### Root causes (three separate bugs)

**Bug 1: Source data has literal duplicate headers**
The Kwara XLS contains 4 pairs of byte-identical column names in row 1:
- `Persons presenting with fever & tested by RDT <5yrs` (appears in cols 7 and 8)
- Same pattern for Microscopy u5 tested, RDT u5 positive, Microscopy u5 positive

These are DHIS2 data element duplicates — verified by checking temporal coverage: one column was active only in 2020, the other took over in 2021-2025. Likely due to DHIS2 form migration.

**Bug 2: Schema inference cannot see duplicates**
At `app/tpr/data_analyzer.py:146-229`, the LLM receives column names as text. When two columns have identical names, the LLM has no way to refer to "the second one." When the LLM returns the name, the validator matches to `df.columns` and gets the first occurrence. Pandas auto-renames the second to `.1`, but this suffix is invisible to both the LLM and the validation logic.

**Bug 3: Character mojibake in column names**
Kwara columns contain `0me` where `name` should be:
- `period0me` → should be `periodname`
- `organisationunit0me` → should be `organisationunitname`
- `Artesu0te` → should be `Artesunate`

The pattern `na → 0` is consistent across the file, suggesting a character encoding issue in the DHIS2 export pipeline.

### Why Adamawa works but Kwara fails

Adamawa (`adamawa_tpr_cleaned.csv`) is already hand-cleaned: 22 unique columns, proper `periodname`/`periodcode`, no duplicates, no mojibake. Kwara is a raw DHIS2 pivot table export. **Our pipeline must handle raw exports** because we cannot assume users will clean their data first.

---

## 3. Design Principles

1. **Conservative** — Only apply cleaning when we detect DHIS2-like data. Non-DHIS2 files pass through unchanged.
2. **Transparent** — Every decision is logged and exposed to the user via a cleaning report.
3. **Auditable** — Clear decision tree, testable in isolation.
4. **Modular** — Single new module. Minimal integration with existing code.
5. **Reversible** — Original uploaded file is preserved. Feature flag for gradual rollout.
6. **Evidence-driven** — Merge strategies chosen based on actual data patterns, not assumptions.

---

## 4. Solution Architecture

### High-level flow

```
File upload
    ↓
EncodingHandler.read_excel_with_encoding()
    ↓
DataFrame (may have duplicates, mojibake)
    ↓
[NEW] dhis2_cleaner.clean(df) ← cleaning module
    ├── detect_dhis2_export()  → is this a DHIS2 file?
    ├── fix_mojibake()         → period0me → periodname
    ├── merge_duplicates()     → collapse pandas-renamed duplicates
    └── build_cleaning_report()→ log all decisions
    ↓
Cleaned DataFrame + CleaningReport
    ↓
Save to session as uploaded_data.csv
    ↓
DataAnalyzer._call_llm_schema() ← now sees clean data
    ↓
Downstream: TPR workflow, risk analysis, etc.
```

### New files

- `app/utils/dhis2_cleaner.py` — the cleaner module (~200 lines)
- `tests/test_dhis2_cleaner.py` — unit tests (~150 lines)

### Modified files

- `app/api/data_analysis_routes.py` — call cleaner after file read (~10 lines)
- `app/agent/data_loader.py` — no change (it reads from the cleaned `uploaded_data.csv`)
- `app/tpr/data_analyzer.py` — no change (it reads from the cleaned file)

### What we DO NOT change

- EncodingHandler — stays focused on encoding detection
- Schema inference logic — just sees cleaner input
- Agent/executor — zero changes
- All downstream code — unchanged

---

## 5. Component: DHIS2 Detector

**Function signature:**
```python
def is_dhis2_export(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    Detect if this DataFrame looks like a DHIS2/HMIS malaria surveillance export.

    Returns (is_dhis2, detected_signals).

    Three independent signals:
      1. period: has 'periodname', 'periodcode', 'period0me' (mojibake), or similar
      2. malaria_multi: has AT LEAST TWO distinct malaria data element keywords
         (e.g., 'rdt' AND 'microscopy', or 'tested' AND 'positive' AND 'malaria')
      3. facility_hierarchy: has 'orgunitlevel*', 'organisationunit*', or
         both a ward-like AND a facility-like column

    Requires ALL THREE signals to be true (tightened from 2-of-3 in v1).
    """
```

**Why all three (tightened from v1):** the reviewer correctly pointed out that a single malaria keyword is too permissive — a blood bank CSV could match on `malaria_diagnosis` alone. Requiring (a) period AND (b) two malaria keywords AND (c) facility hierarchy makes false positives very unlikely.

**Specifically for signal 2:** count distinct keywords found in column names:
```python
malaria_keywords = {'rdt', 'microscopy', 'tpr', 'malaria', 'tested', 'positive', 'fever', 'artesunate', 'artemether', 'chloroquine'}
col_text = ' '.join(df.columns).lower()
distinct_matches = sum(1 for kw in malaria_keywords if kw in col_text)
signal_2 = distinct_matches >= 2
```

**Tested on:**
- Kwara: ✅ (all 3 signals — period + rdt+microscopy+tested+positive+malaria+fever+artesunate + orgunitlevel)
- Adamawa: ✅ (all 3 — periodname + same keywords + WardName/HealthFacility)
- Fake facility CSV with `malaria_diagnosis`: ❌ (only signal 2 barely, no period, no orgunit)
- Blood bank CSV: ❌
- Legitimate dotted CSV: ❌

---

## 5.5 Main Entry Point Signature (v4)

```python
# app/utils/dhis2_cleaner.py

@dataclass
class CleaningReport:
    """Everything the cleaner did, for logging and user-facing display."""
    cleaning_applied: bool
    detected_as: Optional[str]           # "DHIS2 malaria export" or None
    detection_signals: List[str]          # ["period_column", "malaria_terminology", "facility_hierarchy"]
    original_shape: Tuple[int, int]
    cleaned_shape: Tuple[int, int]
    mojibake_fixed: List[Dict[str, str]]          # [{"from": "period0me", "to": "periodname"}, ...]
    duplicates_merged: List[Dict[str, Any]]       # details per group
    data_quality_warnings: List[Dict[str, Any]]   # source issues
    validation_checks: Dict[str, str]             # "temporal_consistency": "pass", etc.
    column_rename_map: Dict[str, str]             # {old_name: new_name} for schema sync
    fallback_reason: Optional[str]                # set if validation failed and we reverted

    def to_dict(self) -> dict: ...


def clean_dhis2_export(
    df: pd.DataFrame,
    mode: str = 'full',
) -> Tuple[pd.DataFrame, CleaningReport]:
    """
    Main entry point. Runs detection, cleaning, validation, reporting.

    Args:
        df: Input DataFrame (raw, possibly with duplicates and mojibake)
        mode: 'full' (apply changes), 'log_only' (compute but discard)

    Returns:
        (cleaned_df, report) — cleaned_df may be the original if:
        - detection returned False (not a DHIS2 file)
        - mode is 'log_only'
        - a BLOCKING validation check failed (fall-back)
        - an exception was caught (defensive)
    """
    report = CleaningReport(...)

    # STEP 1: Detection
    is_dhis2, signals = is_dhis2_export(df)
    if not is_dhis2:
        report.cleaning_applied = False
        return df, report

    # STEP 2: Mojibake fix FIRST (runs before duplicate detection)
    # This ensures period column detection works on clean names
    cleaned, mojibake_renames = fix_mojibake(df)
    report.mojibake_fixed = [{"from": k, "to": v} for k, v in mojibake_renames.items()]
    report.column_rename_map.update(mojibake_renames)

    # STEP 3: Detect period column on cleaned df
    period_col = detect_period_column(cleaned)

    # STEP 4: Duplicate group detection
    groups = detect_duplicate_groups(cleaned)

    # STEP 5: Merge each group
    for base, cols in groups.items():
        strategy, reason = analyze_group(cleaned, cols, period_col=period_col)
        cleaned = merge_group(cleaned, cols, strategy)
        report.duplicates_merged.append({...})

    # STEP 6: Validation checks
    try:
        validate_cleaning(df_original=df, df_cleaned=cleaned, report=report)
    except CleaningIntegrityError as exc:
        # Blocking check failed — fall back to original
        report.cleaning_applied = False
        report.fallback_reason = str(exc)
        return df, report

    # STEP 7: Log_only mode returns original (but keeps the report)
    if mode == 'log_only':
        return df, report  # Note: df, not cleaned — nothing is actually applied

    report.cleaning_applied = True
    return cleaned, report
```

**Critical ordering (v4):** Mojibake runs BEFORE duplicate detection. If we reversed the order, duplicate detection would not find `periodname` (it would see `period0me`) and Rule 4a could fail to detect the period column.

---

## 6. Component: Mojibake Fixer

**Approach:** whitelist of known mojibake patterns in `app/utils/dhis2_mojibake_patterns.py`. Do NOT attempt to reverse-engineer the encoding corruption.

**Separation of concerns from day one:** patterns live in a separate file so the algorithm and the data are independent. Anyone can add a new pattern without touching the cleaner logic.

**File: `app/utils/dhis2_mojibake_patterns.py`**
```python
"""
Known DHIS2 export mojibake patterns.
Pattern corruption is consistent: 'na' → '0' in DHIS2 template identifiers.
Add new patterns here as they are discovered in production.
"""

MOJIBAKE_PATTERNS = [
    # DHIS2 template variable corruption: na → 0
    (r'^period0me(\.\d+)?$',              r'periodname\1'),
    (r'^organisationunit0me(\.\d+)?$',    r'organisationunitname\1'),
    (r'^categoryoptioncomboid0me(\.\d+)?$', r'categoryoptioncomboname\1'),
    (r'^attributeoptioncomboid0me(\.\d+)?$', r'attributeoptioncomboname\1'),
    # Content mojibake in malaria-specific terms
    (r'Artesu0te',                         'Artesunate'),
    (r'Arte0te',                           'Artesunate'),
]
```

**Note:** removed the no-op `organisationunitcode` entry from v1 — it was changing nothing but would have appeared in cleaning reports as a fix. Added categoryoptioncomboname and attributeoptioncomboname from reviewer suggestion.

**Rules:**
- Only apply if `is_dhis2_export()` returns True
- Only apply at WORD level, using anchored regex (`^...$` for full column name, `\b...\b` for in-text)
- Skip any pattern that would be a no-op (from == to)
- Log every non-trivial fix in the cleaning report
- NEVER do blind `0 → na` replacement (too risky)

---

## 7. Component: Duplicate Merger (CORE LOGIC)

This is the most important component. Let me walk through the decision tree carefully.

### Step 7.1: Detect duplicate groups

```python
def detect_duplicate_groups(df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Find pandas-auto-renamed duplicate columns.

    Pattern: when source file has duplicate headers, pandas renames as
    'col', 'col.1', 'col.2'. The base name is still present.

    Returns: {base_name: [base, base.1, base.2, ...]}
    """
    groups = {}
    for col in df.columns:
        m = re.match(r'^(.+?)\.(\d+)$', col)
        if m:
            base = m.group(1)
            # Only a duplicate if base column also exists
            if base in df.columns:
                if base not in groups:
                    groups[base] = [base]
                groups[base].append(col)
    return groups
```

**False-positive protection:** if `version_1.1` exists but `version_1` doesn't, we do NOT treat it as a duplicate. Only flagged if base column is actually present.

### Step 7.2: Analyze each group

For each group of duplicate columns, decide the merge strategy based on the data patterns:

```python
def analyze_group(df, cols, period_col=None) -> tuple[str, str]:
    """
    Decide merge strategy for a group of duplicate columns.

    Returns: (strategy, reason)
      strategy ∈ {'sum', 'combine_first', 'drop_extra'}
      reason is a human-readable explanation
    """
```

**Decision tree (v2 — reordered and simplified):**

Rules are checked in order. First match wins.

```
Rule 1: Non-numeric columns
  If any column in the group is not numeric
  → strategy: 'drop_extra'  (keep first, drop rest)
  → reason: "non-numeric duplicates, cannot safely merge"
  Example: duplicate WardName columns

Rule 2: Identical values (CHECKED BEFORE RATIO CHECK)
  If all values in all duplicate columns are byte-identical (ignoring NaN)
  → strategy: 'drop_extra'
  → reason: "columns contain identical values (exact copies)"
  Rationale: this rule is cheap and definitive; running it first means
  identical ratio columns are correctly labeled "exact copies" rather than
  "ratio column" (reviewer issue #2).

Rule 3: Ratio/percentage columns
  If column name contains word-boundary match for 'rate', 'percent', '%',
  'ratio', 'tpr', 'proportion' (case insensitive)
  → Check for ROW-LEVEL CONFLICTS: if any row has distinct non-null values
    in two columns of the group → strategy: 'keep_both_renamed'
    (rename to col__v1, col__v2, emit data_quality_warning)
  → Otherwise (no conflict): strategy: 'combine_first'
  → reason: "ratio column, {resolution}"

  This addresses reviewer issue #4: combine_first silently suppressing
  conflicting values. Now conflicts are surfaced explicitly.

Rule 4a: Period-level coverage asymmetry (PRIMARY signal, v4 verified)
  Requires period_col (detected by name heuristic: 'period*', 'year',
  'month', or integer column with values in [2000, 2030]).

  For each column in the group:
    active_periods = number of distinct periods where the column has at
                     least one non-zero value across all rows
    coverage[col] = active_periods

  If max(coverage) >= 2 × max(min(coverage), 1)
  → strategy: 'sum'
  → reason: "period coverage asymmetry ({coverage}); likely DHIS2 form migration"

  Kwara verification (simulated on real data):
    col A: active in 2 periods (2020, marginal 2025)
    col B: active in 6 periods (2020-2025)
    6 >= 2 × 2 → sum ✓

Rule 4b: Row-level non-zero complementarity (FALLBACK when no period)
  Compute row-level non-zero overlap using NON-ZERO, NOT non-null:
    both_nonzero = rows where ALL cols have non-zero values
    any_nonzero  = rows where ANY col has non-zero values
    non_overlap  = 1 - (both_nonzero / max(any_nonzero, 1))

  If non_overlap >= 0.80
  → strategy: 'sum'
  → reason: "row-level complementarity ({non_overlap:.0%} non-overlapping)"

  CRITICAL DECISION: non-zero, not non-null. Empirical verification
  on real Kwara data:
    - Using NON-NULL: 5778/5778 = 100% overlap → 0% non-overlap → sum NOT triggered (WRONG)
    - Using NON-ZERO: 570/3680 = 15.5% overlap → 84.5% non-overlap → sum triggered (CORRECT)

  The DHIS2 export for Kwara stores zeros for inactive years, not NaN.
  The second review's "use non-null" suggestion was theoretically sound
  but empirically wrong for Kwara because DHIS2 treats "no cases" and
  "form not active" both as zero. Rule 4a (period-level check) is the
  PRIMARY signal for temporal duplicates — Rule 4b is only a fallback
  for datasets with no detectable period column.

Rule 5: Ambiguous — sum (default with warning)
  None of the above rules fired. Two numeric columns have similar
  period coverage AND row-level overlap. Rare, but possible. Could be:
    - Two different data clerks typing the same form (double-count risk)
    - Related-but-different measurements (OPD vs inpatient)
    - True duplicates we can't detect

  → strategy: 'sum'
  → reason: "ambiguous duplicate relationship; summing as safer count default"
  → emit a data_quality_warning so user sees it in the report

  IMPORTANT — v4 rationale (why not keep_both_renamed):
  v3 proposed 'keep_both_renamed' as a safer default (rename to
  col__v1, col__v2). But this BREAKS the schema contract: the TPR
  workflow uses schema.get("period") to find the period column. If
  we rename a schema-referenced column, downstream code can't find it.
  Sum is therefore the correct safer default. In the rare case where
  Rules 1-4 all fail and summing would double-count, the warning flag
  surfaces the issue to the user and the feature flag can be disabled
  for that session.
```

**Why 80% non-overlap threshold:**

For Kwara, col A has non-zero values only in 2020 (and a tiny bit of 2025),
col B has non-zero values in all 6 years. Computing cell-level non-overlap:
- active_cells (either col has data): ~6948 facility-years
- overlap_cells (both have data): ~1158 (only 2020 rows where both fired)
- non_overlap = 1 - 1158/6948 ≈ 0.83

So Kwara passes the 80% threshold comfortably. An 80% threshold also has a
clean interpretation: "columns are non-overlapping in at least 80% of the
cells where either has data." Contrast with the v1 "2x temporal dominance"
which was arbitrary and didn't handle the symmetric case (both columns have
equal period coverage but on disjoint rows).

**Why 'sum' is the default for count data:**

For TESTED/POSITIVE count columns, summing is conservative:
- If the two columns measure the same thing → summing double-counts (BAD)
- If the two columns measure complementary things → summing is correct (GOOD)
- If one column is discontinued → summing gives correct total because discontinued = 0

The evidence from Kwara shows **sum is correct**:
- 2020 sum: 102,632 tests (matches annual volume of 2021-2025)
- 2020 col_B alone: 42,080 (artificially low compared to other years)
- Summing produces TEMPORALLY CONSISTENT totals, which is strong validation

**When sum would be wrong:** if two columns are pure copies of the same data.
Rule 2 catches this case explicitly, BEFORE Rule 5 could trigger.

**Three-column-and-more groups:**

The decision tree handles N ≥ 2. For Rule 4, the non-overlap calculation
extends to "any col has data" vs "all cols have data" — an N=3 group where
col_A has 2020, col_B has 2021-2022, col_C has 2023-2025 would show nearly
zero overlap across all three and trigger sum correctly.

Unit test fixture must include an N=3 group to verify this path.

### Step 7.3: Apply the merge

```python
def merge_group(df, cols, strategy) -> pd.DataFrame:
    """Apply merge strategy, return DataFrame with extras dropped."""
    base = cols[0]
    extras = [c for c in cols if c != base]

    if strategy == 'sum':
        # Sum across columns, preserving NaN when ALL sources are NaN
        merged = df[cols].fillna(0).sum(axis=1)
        all_nan_mask = df[cols].isna().all(axis=1)
        merged = merged.where(~all_nan_mask, np.nan)
        df[base] = merged

    elif strategy == 'combine_first':
        merged = df[base]
        for c in extras:
            merged = merged.combine_first(df[c])
        df[base] = merged

    # drop_extra: just drop the duplicates, keep base as-is

    return df.drop(columns=extras)
```

**Important:** We preserve NaN correctly — if all source columns are NaN for a row, the merged value is also NaN (not 0). This prevents creating spurious zero values.

---

## 8. Component: Cleaning Report

Every cleaner run produces a structured report that goes to:
1. **Backend logs** (always)
2. **Metadata cache** (stored with the session)
3. **Upload response** (returned to user)
4. **Agent data context** (so agent knows what was cleaned, can explain to user)

**Report structure:**
```json
{
  "cleaning_applied": true,
  "detected_as": "DHIS2 malaria export",
  "detection_signals": ["period_column", "malaria_terminology", "facility_hierarchy"],
  "original_shape": [6948, 32],
  "cleaned_shape": [6948, 28],
  "duplicates_merged": [
    {
      "base_column": "Persons presenting with fever & tested by RDT <5yrs",
      "n_source_columns": 2,
      "strategy": "sum",
      "reason": "temporal coverage mismatch: col A has 2 periods, col B has 6 periods",
      "source_totals": {"col_A": 60625, "col_B": 547686},
      "merged_total": 608311
    }
    // ... more entries
  ],
  "mojibake_fixed": [
    {"from": "period0me", "to": "periodname"},
    {"from": "organisationunit0me", "to": "organisationunitname"}
  ],
  "data_quality_warnings": [
    {
      "type": "numerator_exceeds_denominator",
      "count": 18,
      "description": "18 rows have more positive cases than tested — source data quality issue"
    }
  ],
  "validation_checks": {
    "temporal_consistency": "pass",
    "tpr_range_reasonable": "pass",
    "numerator_le_denominator": "fail (18 rows)"
  }
}
```

### User-facing display

In the upload response, show a collapsible summary:
```
✅ File uploaded and cleaned

Applied DHIS2-specific cleanup:
• Merged 4 duplicate column pairs (DHIS2 form migration artifacts)
• Fixed 3 mojibake column names (period0me → periodname)
• Detected 18 rows with data quality issues (positive > tested) — kept for transparency

[Show details]
```

---

## 9. Component: Validation Checks (Post-Cleaning) — v2

After cleaning, run sanity checks and report any violations. Split into BLOCKING (indicates a bug in our cleaner) and WARNING (indicates a data quality issue in the source).

### BLOCKING checks (raise CleaningIntegrityError, fall back to uncleaned data)

**Check B1: Row count preserved**
```python
assert len(cleaned_df) == len(raw_df)
```
Cleaning should NEVER drop rows. If it does, there's a bug.

**Check B2: Column count is non-increasing**
```python
assert len(cleaned_df.columns) <= len(raw_df.columns)
```
Cleaning should only MERGE columns (reducing count). It should never add columns. This is simpler than the v1 `issubset` check which was broken by mojibake-renamed columns (reviewer issue #5).

**Check B3: Numeric column totals are conserved**
For each column in the cleaned DataFrame that was produced by `sum` or `drop_extra` strategy, verify:
```python
# Sum strategy: merged total == total across all source columns
cleaned_total = cleaned_df[base].fillna(0).sum()
source_total = sum(raw_df[c].fillna(0).sum() for c in source_cols)
assert abs(cleaned_total - source_total) < 1e-6 * max(source_total, 1)
```
If the merged total doesn't equal the sum of source totals, the merge is buggy.

### WARNING checks (report but don't block)

**Check W1: Temporal consistency (relaxed to IQR-based)**
For each merged numeric column, compute annual totals. Flag any year where:
- total < Q1 - 1.5 × IQR, OR
- total > Q3 + 1.5 × IQR

This is the standard outlier definition from boxplots. More defensible than the arbitrary "3x median" in v1 (reviewer issue #11). With only 6 years of data, this may yield false positives, so treat as informational only.

**Check W2: Numerator ≤ Denominator**
For (tested, positive) column pairs identified by schema, count rows where positive > tested. Report the count AND the row indices. Do NOT modify the data (this is a source data issue). The downstream TPR calculation has its own handling.

**Check W3: Ratio ranges**
For ratio columns (TPR), count rows outside [0, 100]. Report.

**Check W4: All-NaN columns after merge**
If any merged column is entirely NaN after cleaning (but sources had data), report it. This could indicate a merge bug.

### Summary of failure reactions

| Check | Type | Action on Fail |
|---|---|---|
| B1 (row count) | Blocking | Raise `CleaningIntegrityError`, fall back to uncleaned data, log stack trace |
| B2 (col count non-increasing) | Blocking | Raise, fall back, log |
| B3 (numeric conservation) | Blocking | Raise, fall back, log |
| W1 (temporal IQR) | Warning | Add to `cleaning_report.warnings`, continue |
| W2 (pos > tested) | Warning | Add to report with row indices, continue |
| W3 (ratio range) | Warning | Add to report, continue |
| W4 (all-NaN merge) | Warning | Add to report, continue |

**Fall-back behavior:** if any BLOCKING check fails, the cleaner returns the original DataFrame unchanged AND writes a cleaning report with `cleaning_applied: false, fallback_reason: "..."`. Upload still succeeds. No user-visible error.

---

## 10. Integration Points (CORRECTED v4)

The cleaner must hook into **THREE** separate data read paths, and each must coordinate with the schema. v4 adds schema rename-map application at every integration point — without this, the TPR calculation fails because `schema.get("period") = "period0me"` points to a column that no longer exists after mojibake fix.

### Shared helper: mode parsing

Before any integration, extract a single helper so all three paths agree on what `CHATMRPT_DHIS2_CLEANER` means:

```python
# app/utils/dhis2_cleaner.py

def get_cleaner_mode() -> str:
    """
    Parse CHATMRPT_DHIS2_CLEANER env var.
    Returns one of: 'off', 'log_only', 'full'
    """
    raw = os.getenv('CHATMRPT_DHIS2_CLEANER', 'off').strip().lower()
    if raw in ('true', '1', 'yes', 'full'):
        return 'full'
    if raw == 'log_only':
        return 'log_only'
    return 'off'
```

All three integration points call `get_cleaner_mode()` — no inline string parsing, no inconsistency.

### Shared helper: schema rename-map application (NEW in v4)

```python
# app/utils/dhis2_cleaner.py

def apply_rename_map_to_schema(
    schema: Dict[str, Any],
    rename_map: Dict[str, str],
) -> Dict[str, Any]:
    """
    Update a schema dict so that any column names referenced are remapped
    through the cleaner's rename_map. Non-string values (e.g., header_row int)
    are passed through unchanged.

    Example:
      schema = {'period': 'period0me', 'ward': 'Ward', 'header_row': 1}
      rename_map = {'period0me': 'periodname'}
      Returns: {'period': 'periodname', 'ward': 'Ward', 'header_row': 1}
    """
    updated = {}
    for field, value in schema.items():
        if isinstance(value, str) and value in rename_map:
            updated[field] = rename_map[value]
        else:
            updated[field] = value
    return updated
```

This helper is called at every integration point after the cleaner runs. Without it, schema entries pointing to renamed columns become stale.

### Shared helper: raw file selector

```python
# app/utils/dhis2_cleaner.py

# Known intermediate files produced by our TPR/risk analysis pipeline.
# These are NOT user uploads and should be excluded from raw-file fallbacks.
INTERMEDIATE_FILES = frozenset({
    'uploaded_data.csv',
    'raw_data.csv',
    'tpr_results.csv',
    'tpr_time_series.csv',
    'unified_dataset.csv',
    'analysis_cleaned_data.csv',
    'analysis_composite_scores.csv',
    'analysis_normalized_data.csv',
    'analysis_vulnerability_rankings.csv',
    'composite_scores.csv',
    'model_formulas.csv',
})


def _select_raw_upload_file(data_files: List[str]) -> str:
    """
    Select the original user-uploaded file from a list of candidates.

    Preference order:
      1. Most recently created XLS/XLSX file (always a user upload in our pipeline)
      2. Most recently created CSV that isn't in INTERMEDIATE_FILES
      3. Raises FileNotFoundError if neither is available

    This prevents the fallback path from accidentally picking up a derived
    intermediate file (e.g., unified_dataset.csv) and treating it as the
    original upload.
    """
    xls_candidates = [
        f for f in data_files
        if f.lower().endswith(('.xls', '.xlsx'))
    ]
    if xls_candidates:
        return max(xls_candidates, key=os.path.getctime)

    csv_candidates = [
        f for f in data_files
        if f.lower().endswith('.csv')
        and os.path.basename(f) not in INTERMEDIATE_FILES
    ]
    if csv_candidates:
        return max(csv_candidates, key=os.path.getctime)

    raise FileNotFoundError("No raw upload file found (all candidates are intermediates)")
```

Called from Path B and Path C fallback branches. Prevents the fallback from silently selecting `unified_dataset.csv`, `raw_data.csv`, or any other derived file as "the user's data."

### Path A: Upload happy path (`data_analysis_routes.py:327-335`)

**Current code:**
```python
from app.tpr.data_analyzer import TPRDataAnalyzer as _Analyzer
_analyzer = _Analyzer()
df_upload, schema_at_upload = _analyzer.infer_schema_from_file(filepath)
df_upload.to_csv(uploaded_csv_path, index=False)
```

**New code (v4 with schema rename-map):**
```python
from app.tpr.data_analyzer import TPRDataAnalyzer as _Analyzer
from app.utils.dhis2_cleaner import (
    clean_dhis2_export, get_cleaner_mode, apply_rename_map_to_schema
)

_analyzer = _Analyzer()
df_upload, schema_at_upload = _analyzer.infer_schema_from_file(filepath)

# Apply DHIS2 cleaner (conservative — only runs if detected as DHIS2 export)
_mode = get_cleaner_mode()
if _mode != 'off':
    try:
        df_upload, cleaning_report = clean_dhis2_export(df_upload, mode=_mode)

        # CRITICAL v4: update schema to reflect cleaner renames
        # Without this, schema.get("period") might still point to the
        # mojibake "period0me" which no longer exists in cleaned df.
        if cleaning_report.column_rename_map:
            schema_at_upload = apply_rename_map_to_schema(
                schema_at_upload, cleaning_report.column_rename_map
            )
            logger.info(
                "[DHIS2_CLEANER] Updated schema for %d renamed columns",
                len(cleaning_report.column_rename_map),
            )

        # Save cleaning report next to uploaded data
        report_path = os.path.join(upload_dir, 'cleaning_report.json')
        with open(report_path, 'w') as f:
            json.dump(cleaning_report.to_dict(), f, indent=2)

        if cleaning_report.cleaning_applied:
            logger.info(
                "[DHIS2_CLEANER] mode=%s detected=%s duplicates=%d mojibake=%d warnings=%d",
                _mode,
                cleaning_report.detected_as,
                len(cleaning_report.duplicates_merged),
                len(cleaning_report.mojibake_fixed),
                len(cleaning_report.data_quality_warnings),
            )
    except Exception as _clean_exc:
        # NEVER let cleaner failure break upload
        logger.exception("[DHIS2_CLEANER] Failed (falling back to uncleaned data): %s", _clean_exc)

df_upload.to_csv(uploaded_csv_path, index=False)
```

**And the saved schema must reflect the update.** Later in the route:
```python
# When saving schema to state_manager, use the UPDATED schema
state_manager.update_state({
    'column_schema': schema_at_upload,  # v4: this must be the post-cleaner version
})
```

**Fallback path (line 340-349):** the except branch fires when schema inference itself fails. It reads the file with EncodingHandler directly and doesn't call `infer_schema_from_file`, so there's no schema at this point to update. Still apply the cleaner to the DataFrame before saving.

**Why no schema update is needed in the fallback:** when Path C later runs, it will find `uploaded_data.csv` (already cleaned) and run `infer_schema_from_file` on it. Schema inference on the cleaned CSV produces clean column names directly (no mojibake, no duplicate base names to confuse the LLM). So the schema will reflect cleaned columns without any rename-map application. This is architecturally correct — the rename-map is ONLY needed when schema inference ran on the RAW file BEFORE cleaning.

### Path B: TPR workflow re-read (`data_analysis_routes.py:747-758`)

The TPR workflow independently re-reads the raw XLS when it starts. v1 missed this entirely. The reviewer correctly identified this as a second entry point.

**Current code:**
```python
if data_files:
    latest = max(data_files, key=os.path.getctime)
    if latest.endswith(('.xlsx', '.xls')):
        header_row = int(saved_schema.get('header_row', 0)) if saved_schema else 0
        df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
    else:
        df = EncodingHandler.read_csv_with_encoding(latest)
    tpr_handler.set_data(df)
```

**New code v4 (preferred: use cleaned uploaded_data.csv):**
```python
from app.utils.dhis2_cleaner import (
    clean_dhis2_export, get_cleaner_mode, apply_rename_map_to_schema
)

# Prefer cleaned uploaded_data.csv over the raw XLS — the cleaner already
# ran at upload time and produced a canonical clean DataFrame there.
# The saved schema in state_manager is also the post-cleaner version.
uploaded_csv = os.path.join(data_dir, 'uploaded_data.csv')
if os.path.exists(uploaded_csv):
    df = EncodingHandler.read_csv_with_encoding(uploaded_csv)
    logger.info("[TPR-WORKFLOW] Loaded cleaned uploaded_data.csv (%d rows, %d cols)", *df.shape)
    # No schema update needed — saved schema already reflects cleaner renames
elif data_files:
    # Fallback: re-read the ORIGINAL user upload using the shared helper.
    try:
        latest = _select_raw_upload_file(data_files)
    except FileNotFoundError:
        logger.warning("[TPR-WORKFLOW] No raw file candidates (all are intermediates)")
        raise

    if latest.endswith(('.xlsx', '.xls')):
        header_row = int(saved_schema.get('header_row', 0)) if saved_schema else 0
        df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
    else:
        df = EncodingHandler.read_csv_with_encoding(latest)

    # Apply cleaner here — if it renames columns, update the LOCAL schema
    # variable AND persist it back to state_manager
    _mode = get_cleaner_mode()
    if _mode != 'off':
        try:
            df, _report = clean_dhis2_export(df, mode=_mode)
            if _report.column_rename_map:
                saved_schema = apply_rename_map_to_schema(
                    saved_schema or {}, _report.column_rename_map
                )
                # CRITICAL v4 fix: persist updated schema so subsequent
                # requests in this session don't get stale column names
                state_manager.update_state({'column_schema': saved_schema})
            logger.info("[TPR-WORKFLOW] Applied DHIS2 cleaner on raw re-read (mode=%s)", _mode)
        except Exception as exc:
            logger.exception("[TPR-WORKFLOW] Cleaner failed: %s", exc)

tpr_handler.set_data(df)
```

**Why prefer `uploaded_data.csv`:** cleaner ran at upload, results are deterministic. Re-running on the raw XLS would waste compute and risk inconsistency if the cleaner ever becomes non-deterministic (e.g., if it gets randomness for tie-breaking).

### Path C: TPR workflow start handler (`data_analysis_routes.py:912-943`)

**This is the most common entry point** — it fires when a user types "start the TPR workflow". The second review identified this path as a critical miss in v2.

**Current code:**
```python
latest = max(data_files, key=os.path.getctime)
tpr_analyzer = TPRDataAnalyzer()

_saved_state = state_manager.load_state() or {}
_saved_schema = _saved_state.get('column_schema') or {}
_schema_complete = (
    _saved_schema.get('header_row') is not None
    and any(_saved_schema.get(c) for c in _tpr_cols)
)

if _schema_complete:
    logger.info(f"[TPR START] Reusing schema from upload ...")
    schema = _saved_schema
    tpr_analyzer._schema = schema
    header_row = int(schema.get('header_row', 0))
    try:
        from app.agent.encoding_handler import EncodingHandler as _EH
        if latest.lower().endswith(('.xlsx', '.xls')):
            df = _EH.read_excel_with_encoding(latest, header=header_row)
        else:
            df = _EH.read_csv_with_encoding(latest)
    except Exception as exc:
        ...

if not _schema_complete:
    df, schema = tpr_analyzer.infer_schema_from_file(latest)
    state_manager.update_state({'column_schema': schema})

tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)
tpr_handler.set_data(df)
```

**New code v4 (same pattern as Path B — prefer cleaned file + schema sync):**
```python
from app.utils.dhis2_cleaner import (
    clean_dhis2_export, get_cleaner_mode, apply_rename_map_to_schema
)

tpr_analyzer = TPRDataAnalyzer()

# PRIORITY: Prefer cleaned uploaded_data.csv if it exists.
# The saved schema in state_manager also reflects the cleaner (Path A
# applied rename_map before saving). Reading from uploaded_data.csv +
# saved_schema is the canonical path and avoids any risk of inconsistency.
uploaded_csv = os.path.join(
    os.path.dirname(data_files[0]), 'uploaded_data.csv'
) if data_files else None

if uploaded_csv and os.path.exists(uploaded_csv):
    from app.agent.encoding_handler import EncodingHandler as _EH
    df = _EH.read_csv_with_encoding(uploaded_csv)

    _saved_state = state_manager.load_state() or {}
    schema = _saved_state.get('column_schema') or {}
    if schema.get('header_row') is not None and any(schema.get(c) for c in _tpr_cols):
        # Schema was saved at upload time AND is complete — reuse it
        tpr_analyzer._schema = schema
    else:
        # Schema missing or incomplete — run inference on cleaned CSV.
        # Since uploaded_data.csv is ALREADY cleaned, the inferred schema
        # will naturally have clean column names without a rename map.
        df_tmp, schema = tpr_analyzer.infer_schema_from_file(uploaded_csv)
        state_manager.update_state({'column_schema': schema})
        df = df_tmp
    logger.info("[TPR START] Using cleaned uploaded_data.csv (%d rows, %d cols)", *df.shape)
else:
    # Fallback: re-read the ORIGINAL user upload (legacy sessions or upload
    # path failures). Use the shared helper that prefers XLS/XLSX and excludes
    # known intermediate files.
    try:
        latest = _select_raw_upload_file(data_files)
    except FileNotFoundError:
        logger.error("[TPR START] No raw file candidates found (all intermediates)")
        return _save_and_respond(
            {'success': False, 'message': 'No data file found', 'session_id': session_id},
            session_id
        )

    _saved_state = state_manager.load_state() or {}
    _saved_schema = _saved_state.get('column_schema') or {}
    _schema_complete = (
        _saved_schema.get('header_row') is not None
        and any(_saved_schema.get(c) for c in _tpr_cols)
    )

    if _schema_complete:
        schema = _saved_schema
        tpr_analyzer._schema = schema
        header_row = int(schema.get('header_row', 0))
        try:
            from app.agent.encoding_handler import EncodingHandler as _EH
            if latest.lower().endswith(('.xlsx', '.xls')):
                df = _EH.read_excel_with_encoding(latest, header=header_row)
            else:
                df = _EH.read_csv_with_encoding(latest)
        except Exception as exc:
            logger.warning(f"[TPR START] Re-read failed ({exc}), falling back to full inference")
            _schema_complete = False

    if not _schema_complete:
        df, schema = tpr_analyzer.infer_schema_from_file(latest)
        state_manager.update_state({'column_schema': schema})

    # Apply cleaner to raw-read data AND persist updated schema
    _mode = get_cleaner_mode()
    if _mode != 'off':
        try:
            df, _report = clean_dhis2_export(df, mode=_mode)
            if _report.column_rename_map:
                schema = apply_rename_map_to_schema(schema, _report.column_rename_map)
                tpr_analyzer._schema = schema
                state_manager.update_state({'column_schema': schema})
            logger.info("[TPR START] Applied cleaner to raw re-read (mode=%s)", _mode)
        except Exception as exc:
            logger.exception("[TPR START] Cleaner failed: %s", exc)

tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)
tpr_handler.set_data(df)
```

**Why this is critical:** Path C fires every time a user starts the TPR workflow. Without covering it, the cleaner would run at upload (producing a report) but the TPR workflow would still read the raw XLS and compute wrong burden numbers. The cleaner would appear to work while silently being bypassed.

---

## 10.5. Schema Coordination Contract (NEW in v4)

This section documents the contract between the cleaner and downstream consumers of the schema. It exists because v3's review missed a coupling bug: the TPR calculation uses `schema.get("period")`, `schema.get("u5_rdt_tested")`, etc. to access specific columns in the DataFrame. If the cleaner renames columns but the schema is not updated, downstream code breaks silently.

### The contract

**Cleaner guarantees:**
1. Returns a `column_rename_map: Dict[str, str]` listing every column name change. Empty dict if no renames.
2. Does NOT rename columns for duplicate merging (base name is preserved, only `.1`/`.2` variants are dropped).
3. Only renames columns for: (a) mojibake fixing, (b) `keep_both_renamed` strategy — but v4 drops this strategy, so in v4 only mojibake fixes produce renames.
4. `column_rename_map` contains ONLY entries where source != target (no no-op entries).

**Consumer responsibilities (data_analysis_routes.py):**
1. After calling `clean_dhis2_export()`, MUST call `apply_rename_map_to_schema(schema, report.column_rename_map)` before saving schema or using it for downstream operations.
2. MUST save the UPDATED schema to `state_manager` so the TPR workflow start handler (Path C) reads the right version.
3. MUST NOT pass the cleaned DataFrame to downstream code with the stale schema.

### How each cleaner operation affects the schema

| Cleaner operation | Effect on schema | Action needed |
|---|---|---|
| Mojibake fix (e.g., `period0me` → `periodname`) | Schema entry pointing to old name must be updated | `apply_rename_map_to_schema()` |
| Duplicate merge (Rule 4a, 4b, 5, default 'sum') | Base column name is preserved; data values change from wrong to correct | NONE — schema entry still valid |
| Exact copies drop (Rule 2 'drop_extra') | Base column preserved, `.1` variants dropped | NONE |
| Non-numeric drop_extra (Rule 1) | Base preserved, `.1` variants dropped | NONE |
| Ratio combine_first (Rule 3) | Base column preserved, receives combined non-null values | NONE |

**Key insight:** only mojibake fixing produces schema-affecting renames. Duplicate merging is transparent to the schema because pandas' auto-rename pattern preserves the base name, and the cleaner merges INTO that base name.

### Invariant testing

A unit test MUST verify the contract:
```python
def test_schema_coordination_invariant():
    """After cleaning, every schema column must exist in the cleaned df."""
    df, schema = load_kwara_fixture_with_schema()
    cleaned_df, report = clean_dhis2_export(df)
    updated_schema = apply_rename_map_to_schema(schema, report.column_rename_map)

    for field, col_name in updated_schema.items():
        if field == 'header_row' or col_name is None:
            continue
        assert col_name in cleaned_df.columns, (
            f"Schema[{field}] points to '{col_name}' which doesn't exist "
            f"in cleaned df. Columns: {list(cleaned_df.columns)[:10]}..."
        )
```

This invariant test catches the exact class of bug that slipped through two rounds of review.

---

## 10.6. System Prompt Updates (NEW in v4)

The system prompt contains hardcoded DHIS2 column name references that are either wrong after cleaning or specific to one dataset. These must be made generic.

### Changes to `app/agent/prompts/system_prompt.py`

**Line 86 (DHIS2 Column Conventions section):**

**Before:**
```
### DHIS2 Column Conventions (treat as fact)
- orgunitlevel2 = State, orgunitlevel3 = LGA, orgunitlevel4 = Ward
- organisationunitname = facility name, period0me = time period
```

**After:**
```
### DHIS2 Column Conventions
- `orgunitlevel2`/`orgunitlevel3`/`orgunitlevel4` are typically State/LGA/Ward.
  Check the data profile for exact mappings — not every DHIS2 export uses
  this naming.
- The time period column varies: `periodname`, `periodcode`, `period`, `Period`,
  or `Year`. Always check the actual columns in the data profile before querying.
- The facility name column is typically `organisationunitname` but may be
  truncated or renamed in older exports. Check the profile.
```

**Line 174 (Trend Analysis section):**

**Before:**
```
- `df` if it has a time column (period0me, Year).
```

**After:**
```
- `df` if it has a time column (check the profile — common names include
  `periodname`, `periodcode`, `Period`, `Year`).
```

### Why these changes matter

1. After the cleaner runs, `period0me` no longer exists (renamed to `periodname`). Telling the LLM "period0me = time period" is wrong.
2. Even without the cleaner, `period0me` is specific to the Kwara export. Adamawa uses `periodname`. A good prompt shouldn't commit to one dataset's quirks.
3. The data profile (in the workflow_context message) ALREADY contains the exact column names for the session. The generic guidance in the prompt should defer to that.

### Preserve the original file

Always keep the original uploaded XLS/CSV untouched. The cleaned version goes into `uploaded_data.csv`. This gives us fallback:
- `uploaded_data.csv` — cleaned (agent/analysis uses this)
- `Kwara_TPR_data.xls` — original raw file (never modified)
- `cleaning_report.json` — audit trail

### Feature flag defaults (CORRECTED)

**v1 bug:** defaulted to `'true'`, contradicting the rollout plan. v2 defaults to `'false'`:

```python
_cleaner_enabled = os.getenv('CHATMRPT_DHIS2_CLEANER', 'false').lower() in ('true', '1', 'yes')
```

Also handles `'True'`, `'TRUE'`, `'1'`, `'yes'` as true — fragile string comparison fixed per reviewer issue #10.

### Agent data context

When the agent loads data, include the cleaning report in the data summary message:

**app/agent/agent.py `_create_data_summary()`:**
```python
# Read cleaning report if present
cleaning_report_path = os.path.join(session_folder, 'cleaning_report.json')
if os.path.exists(cleaning_report_path):
    with open(cleaning_report_path) as f:
        report = json.load(f)
    if report.get('cleaning_applied'):
        summary += "\n\n**Data cleaning notes:**\n"
        for m in report.get('duplicates_merged', []):
            summary += f"- Column '{m['base_column']}' was merged from {m['n_source_columns']} duplicate source columns ({m['strategy']})\n"
        for fix in report.get('mojibake_fixed', []):
            summary += f"- Renamed '{fix['from']}' → '{fix['to']}'\n"
```

This way, if a user asks "why does this number differ from my Excel?", the agent can reference the cleaning.

---

## 11. Testing Strategy

### Unit tests (tests/test_dhis2_cleaner.py)

**Test 1: Pass-through for non-DHIS2 data**
```python
def test_non_dhis2_passes_through():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    cleaned, report = clean_dhis2_export(df)
    assert df.equals(cleaned)
    assert report.cleaning_applied == False
```

**Test 2: Legitimate dotted column names**
```python
def test_legitimate_dotted_names_not_flagged():
    df = pd.DataFrame({
        'version_1.0': [1, 2],
        'version_1.1': [3, 4],
        'periodname': ['2024-01', '2024-02'],
        'rdt_tested': [10, 20],
    })
    cleaned, _ = clean_dhis2_export(df)
    # version_1.0 and version_1.1 should NOT be merged
    assert 'version_1.0' in cleaned.columns
    assert 'version_1.1' in cleaned.columns
```

**Test 3: Identical duplicate columns → drop_extra**
```python
def test_identical_duplicates_dropped():
    df = pd.DataFrame({
        'periodname': ['2024-01', '2024-02'],
        'rdt_tested': [10, 20],
        'rdt_tested.1': [10, 20],  # exact copy
    })
    cleaned, report = clean_dhis2_export(df)
    assert list(cleaned.columns) == ['periodname', 'rdt_tested']
    assert cleaned['rdt_tested'].tolist() == [10, 20]
    assert report.duplicates_merged[0]['strategy'] == 'drop_extra'
```

**Test 4: Temporal dominance → sum**
```python
def test_temporal_dominance_triggers_sum():
    df = pd.DataFrame({
        'periodname': [2020, 2020, 2021, 2021, 2022, 2022],
        'rdt_tested': [10, 20, 0, 0, 0, 0],    # only 2020
        'rdt_tested.1': [5, 5, 30, 30, 40, 40], # all years
        'rdt_positive': [5, 10, 0, 0, 0, 0],
        'malaria_dummy': [1, 1, 1, 1, 1, 1],  # DHIS2 signal
    })
    cleaned, report = clean_dhis2_export(df)
    # col.1 active in 3 periods, col in 1 period → sum
    assert 'rdt_tested.1' not in cleaned.columns
    assert cleaned.loc[0, 'rdt_tested'] == 15  # 10 + 5
    assert cleaned.loc[2, 'rdt_tested'] == 30  # 0 + 30
    assert report.duplicates_merged[0]['strategy'] == 'sum'
```

**Test 5: Ratio columns → combine_first**
```python
def test_ratio_columns_use_combine_first():
    df = pd.DataFrame({
        'periodname': ['2024-01', '2024-02'],
        'rdt_tested': [100, 200],
        'tpr_rate': [50.0, None],
        'tpr_rate.1': [None, 75.0],
    })
    cleaned, report = clean_dhis2_export(df)
    assert cleaned['tpr_rate'].tolist() == [50.0, 75.0]
    # Not 50+None → None+75 sum
    assert report.duplicates_merged[0]['strategy'] == 'combine_first'
```

**Test 6: Mojibake fix**
```python
def test_mojibake_fix():
    df = pd.DataFrame({
        'period0me': [2024, 2024],
        'organisationunit0me': ['A', 'B'],
        'rdt_tested': [10, 20],
    })
    cleaned, report = clean_dhis2_export(df)
    assert 'periodname' in cleaned.columns
    assert 'organisationunitname' in cleaned.columns
    assert 'period0me' not in cleaned.columns
    assert len(report.mojibake_fixed) == 2
```

**Test 7: NaN preservation (corrected per reviewer issue #8)**
```python
def test_nan_preserved_when_all_sources_nan():
    df = pd.DataFrame({
        'periodname': [2024, 2024, 2024],
        'rdt_tested':   [10.0, None, None],
        'rdt_tested.1': [5.0,  None, 20.0],
        # DHIS2 signal columns
        'orgunitlevel2': ['A', 'A', 'A'],
        'malaria_positive': [3, 0, 5],
    })
    cleaned, _ = clean_dhis2_export(df)
    result = cleaned['rdt_tested'].tolist()
    assert result[0] == 15.0           # 10 + 5
    assert pd.isna(result[1])          # both sources NaN → NaN preserved
    assert result[2] == 20.0           # fillna(0).sum() treats None as 0; NaN only preserved when ALL sources NaN
```

**Test 8: Row count preserved**
```python
def test_row_count_preserved():
    df = make_kwara_like_fixture(n_rows=100)
    original_len = len(df)
    cleaned, _ = clean_dhis2_export(df)
    assert len(cleaned) == original_len
```

**Test 9: Three-column duplicate group (reviewer issue #9)**
```python
def test_three_column_duplicate_group():
    df = pd.DataFrame({
        'periodname': [2020]*5 + [2021]*5 + [2022]*5,
        'rdt_tested':   [10]*5 + [0]*5  + [0]*5,   # only 2020
        'rdt_tested.1': [0]*5  + [20]*5 + [0]*5,   # only 2021
        'rdt_tested.2': [0]*5  + [0]*5  + [30]*5,  # only 2022
        # DHIS2 signals
        'orgunitlevel2': ['X']*15,
        'malaria_positive': [5]*15,
    })
    cleaned, report = clean_dhis2_export(df)
    # All three should merge into one column via sum
    assert 'rdt_tested' in cleaned.columns
    assert 'rdt_tested.1' not in cleaned.columns
    assert 'rdt_tested.2' not in cleaned.columns
    # Annual totals should be preserved
    assert cleaned[cleaned['periodname']==2020]['rdt_tested'].sum() == 50
    assert cleaned[cleaned['periodname']==2021]['rdt_tested'].sum() == 100
    assert cleaned[cleaned['periodname']==2022]['rdt_tested'].sum() == 150
```

**Test 10: Ratio columns with non-overlapping values → combine_first works**
```python
def test_ratio_columns_non_overlapping_combines():
    df = pd.DataFrame({
        'periodname': [2020, 2020, 2021, 2021],
        'tpr':   [75.0, 80.0, None, None],   # 2020 only
        'tpr.1': [None, None, 70.0, 65.0],   # 2021 only
        # signals
        'orgunitlevel2': ['A']*4,
        'rdt_tested': [100]*4,
        'malaria_positive': [75, 80, 70, 65],
    })
    cleaned, report = clean_dhis2_export(df)
    assert cleaned['tpr'].tolist() == [75.0, 80.0, 70.0, 65.0]
    # No warning because no row-level conflict
    assert not any('conflict' in w.get('type', '') for w in report.data_quality_warnings)
```

**Test 11 (v4 rewrite): Ratio columns with conflicting values → combine_first + warning**
```python
def test_ratio_columns_with_conflict_warns_but_uses_combine_first():
    """v4: keep_both_renamed is dropped because it breaks schema. The cleaner
    uses combine_first (taking the first non-null) and emits a warning so the
    user knows the second column was suppressed in rows where both were set."""
    df = pd.DataFrame({
        'periodname': [2020, 2020, 2021],
        'tpr':   [75.0, 80.0, 70.0],
        'tpr.1': [76.0, 81.0, 71.0],  # different values in same rows
        # DHIS2 signals
        'orgunitlevel2': ['A']*3,
        'rdt_tested': [100]*3,
        'malaria_positive': [75, 80, 70],
    })
    cleaned, report = clean_dhis2_export(df)
    # combine_first keeps the first non-null, so tpr ends up with [75, 80, 70]
    assert 'tpr' in cleaned.columns
    assert 'tpr.1' not in cleaned.columns
    assert cleaned['tpr'].tolist() == [75.0, 80.0, 70.0]
    # Warning emitted about the suppression
    conflict_warnings = [w for w in report.data_quality_warnings
                          if w.get('type') == 'ratio_column_conflict']
    assert len(conflict_warnings) == 1
    # Warning mentions the 3 conflicting rows
    assert conflict_warnings[0]['conflicting_rows'] == 3
```

**Test 11a (NEW): Schema coordination invariant**
```python
def test_schema_coordination_invariant():
    """After cleaning, every schema column name must exist in the cleaned df.
    This test catches the v3 bug where mojibake renames left the schema stale."""
    # Fixture: Kwara-like data with mojibake and duplicates
    df = pd.DataFrame({
        'period0me': [2020, 2020, 2021, 2021],            # mojibake
        'organisationunit0me': ['A', 'B', 'A', 'B'],      # mojibake
        'Ward': ['W1', 'W2', 'W1', 'W2'],
        'orgunitlevel2': ['S']*4,
        'orgunitlevel3': ['L']*4,
        'Facility level': ['primary']*4,
        'Persons presenting with fever & tested by RDT <5yrs':   [10, 20, 0, 0],
        'Persons presenting with fever & tested by RDT <5yrs.1': [0, 0, 30, 40],
        'Persons tested positive for malaria by RDT <5yrs':   [5, 10, 0, 0],
        'Persons tested positive for malaria by RDT <5yrs.1': [0, 0, 15, 20],
    })
    # Pre-cleaner schema (what LLM would return, pointing to mojibake names)
    schema = {
        'header_row': 1,
        'period': 'period0me',
        'facility_name': 'organisationunit0me',
        'ward': 'Ward',
        'state': 'orgunitlevel2',
        'lga': 'orgunitlevel3',
        'facility_level': 'Facility level',
        'u5_rdt_tested': 'Persons presenting with fever & tested by RDT <5yrs',
        'u5_rdt_positive': 'Persons tested positive for malaria by RDT <5yrs',
    }

    cleaned, report = clean_dhis2_export(df)
    updated_schema = apply_rename_map_to_schema(schema, report.column_rename_map)

    # Invariant: every schema column must exist in cleaned df
    for field, col_name in updated_schema.items():
        if field == 'header_row' or col_name is None:
            continue
        assert col_name in cleaned.columns, (
            f"Schema[{field}] = '{col_name}' not found. "
            f"Columns: {list(cleaned.columns)}"
        )

    # Specifically verify the mojibake fields were updated
    assert updated_schema['period'] == 'periodname'
    assert updated_schema['facility_name'] == 'organisationunitname'
    # And the duplicate-merged field is unchanged (base name preserved)
    assert updated_schema['u5_rdt_tested'] == 'Persons presenting with fever & tested by RDT <5yrs'

    # Verify merge produced correct data
    assert cleaned.loc[cleaned['periodname']==2020, 'Persons presenting with fever & tested by RDT <5yrs'].sum() == 30
    assert cleaned.loc[cleaned['periodname']==2021, 'Persons presenting with fever & tested by RDT <5yrs'].sum() == 70
```

**Test 12: Detection-but-no-duplicates (Adamawa-style)**
```python
def test_dhis2_detected_no_duplicates():
    df = load_adamawa_snapshot()  # 22 cols, no duplicates
    cleaned, report = clean_dhis2_export(df)
    assert report.cleaning_applied == True  # detected as DHIS2
    assert len(report.duplicates_merged) == 0
    assert len(report.mojibake_fixed) == 0
    # Shape unchanged
    assert cleaned.shape == df.shape
    # Columns unchanged
    assert list(cleaned.columns) == list(df.columns)
```

**Test 13: End-to-end on Kwara snapshot**
Store a minimal Kwara-like fixture in `tests/fixtures/kwara_snapshot.csv` (first 100 rows preserving duplicates and mojibake) and verify:
- DHIS2 detection fires
- 4 duplicate groups detected
- All merge via `sum` (Rule 4 non-overlap)
- Final shape is 100 × 28
- Mojibake `period0me` → `periodname`
- Numeric conservation check passes

**Test 14: Large period column as integers (reviewer suggestion)**
```python
def test_integer_period_codes():
    df = pd.DataFrame({
        'periodcode': [20240101, 20240201, 20240301] * 3,  # int YYYYMMDD
        'rdt_tested':   [10, 0, 0] * 3,
        'rdt_tested.1': [0, 20, 30] * 3,
        # signals
        'orgunitlevel2': ['A']*9,
        'malaria_positive': [5]*9,
    })
    cleaned, _ = clean_dhis2_export(df)
    # Non-overlap check should work with integer periods
    assert 'rdt_tested.1' not in cleaned.columns
```

**Test 15: Non-DHIS2 passes through (reviewer hardening)**
```python
def test_non_dhis2_file_untouched():
    df = pd.DataFrame({
        'facility_id': [1, 2, 3],
        'num_beds': [10, 20, 30],
        'num_beds.1': [5, 5, 5],  # dup pattern but not DHIS2
        'region': ['north', 'south', 'east'],
    })
    cleaned, report = clean_dhis2_export(df)
    assert report.cleaning_applied == False
    assert cleaned.equals(df)  # zero changes
```

**Test 16: Cleaner exception → fall back gracefully**
```python
def test_cleaner_exception_falls_back_to_original():
    # Force an internal exception (e.g., monkeypatch merge_group to raise)
    with mock.patch('app.utils.dhis2_cleaner.merge_group', side_effect=RuntimeError('boom')):
        df = load_kwara_snapshot()
        cleaned, report = clean_dhis2_export(df)
        # Must return original unchanged
        assert cleaned.equals(df)
        assert report.cleaning_applied == False
        assert report.fallback_reason is not None
```

---

## 11.5. Integration Test Script (NEW in v4)

Unit tests verify the cleaner in isolation. The schema-cleaner desync bug slipped through BOTH v1 and v2 reviews because it only manifests when multiple components interact. v4 requires a manual integration test that exercises the full Kwara pipeline end-to-end.

### File: `scripts/integration_test_kwara.py`

```python
"""
Integration test: run the full Kwara pipeline with the cleaner enabled.

Prerequisites:
- Kwara XLS file at data/datasets/Kwara TPR data 2020 - 2025 ~ 2026-03-02 (1).xls
- CHATMRPT_DHIS2_CLEANER=full in environment
- Flask dev server not running (we use direct service calls)

Run: python scripts/integration_test_kwara.py

Expected output: all checks PASS. Exit code 0 on success, 1 on any failure.
"""

import os
import sys
import json
import shutil
import tempfile
import pandas as pd

os.environ['CHATMRPT_DHIS2_CLEANER'] = 'full'
os.environ['FLASK_ENV'] = 'development'

# ... import app + services ...

def run_integration_test():
    checks = []

    # Step 1: Create temp session
    session_id = "integration-test-kwara"
    session_dir = f"instance/uploads/{session_id}"
    os.makedirs(session_dir, exist_ok=True)

    try:
        src = "data/datasets/Kwara TPR data 2020 - 2025 ~ 2026-03-02 (1).xls"
        dst = os.path.join(session_dir, "kwara.xls")
        shutil.copy(src, dst)

        # Step 2: Schema inference + cleaning
        from app.tpr.data_analyzer import TPRDataAnalyzer
        from app.utils.dhis2_cleaner import (
            clean_dhis2_export, apply_rename_map_to_schema
        )

        analyzer = TPRDataAnalyzer()
        df_upload, schema_at_upload = analyzer.infer_schema_from_file(dst)
        df_upload, report = clean_dhis2_export(df_upload, mode='full')
        schema_at_upload = apply_rename_map_to_schema(
            schema_at_upload, report.column_rename_map
        )

        checks.append(("Cleaning applied", report.cleaning_applied == True))
        checks.append(("Duplicates detected", len(report.duplicates_merged) == 4))
        checks.append(("Mojibake fixed", len(report.mojibake_fixed) >= 2))
        checks.append(("Column count reduced", df_upload.shape[1] == 28))

        # Step 3: Schema invariant
        all_valid = all(
            col in df_upload.columns
            for field, col in schema_at_upload.items()
            if field != 'header_row' and isinstance(col, str)
        )
        checks.append(("Schema invariant holds", all_valid))

        # Step 4: Save cleaned CSV
        uploaded_csv = os.path.join(session_dir, "uploaded_data.csv")
        df_upload.to_csv(uploaded_csv, index=False)

        # Step 5: TPR calculation (guard imports so missing funcs fail check cleanly)
        try:
            from app.tpr.utils import calculate_ward_tpr, calculate_ward_tpr_timeseries
            _tpr_imports_ok = True
        except ImportError as exc:
            print(f"⚠️  TPR module imports failed: {exc}")
            checks.append(("TPR module imports", False))
            _tpr_imports_ok = False

        if _tpr_imports_ok:
            tpr_df = calculate_ward_tpr(
                df_upload, age_group='u5', test_method='rdt',
                facility_level='primary', schema=schema_at_upload
            )
            checks.append(("TPR calculation produces output", len(tpr_df) > 0))
            checks.append(("Burden column exists", 'Burden' in tpr_df.columns))

            # Step 6: Time-series generation
            ts_df = calculate_ward_tpr_timeseries(
                df_upload, age_group='u5', test_method='rdt',
                facility_level='primary', schema=schema_at_upload
            )
        # v4 expectation: ts_df should have data for ALL 6 years, not just 2020
        years_in_ts = sorted(ts_df['Period'].unique()) if len(ts_df) > 0 else []
        checks.append(
            ("Time series spans 2020-2025",
             len(years_in_ts) == 6 and min(years_in_ts) == 2020 and max(years_in_ts) == 2025)
        )
        checks.append(
            ("Time series has at least 500 rows",  # 193 wards * 6 years ~ 1100
             len(ts_df) >= 500)
        )

        # Step 7: Risk analysis (requires shapefile + env data)
        # Skipped if prerequisites not available
        if os.path.exists("data/geospatial/shapefiles"):
            # Run the risk analysis pipeline
            # Verify unified_dataset.csv is produced
            # Verify rankings are different from broken v1 run
            pass

        # Print results
        print("\n=== Integration Test Results ===")
        all_passed = True
        for name, result in checks:
            status = "✅" if result else "❌"
            print(f"  {status} {name}")
            if not result:
                all_passed = False

        return all_passed

    finally:
        # Clean up temp session
        shutil.rmtree(session_dir, ignore_errors=True)


if __name__ == '__main__':
    success = run_integration_test()
    sys.exit(0 if success else 1)
```

### When to run

- Before every implementation phase (as a smoke test)
- Before enabling log-only mode on staging (Phase 4 exit criterion)
- Before enabling full mode on production (Phase 6 exit criterion)
- After any refactor to the cleaner module or integration points

### Why manual, not in CI

- Requires a real XLS file (200KB, copyright-sensitive)
- Requires shapefile database for full pipeline
- Runs against a real OpenAI API (for schema inference)
- Not reproducible in automated CI without credentials

The script is idempotent and can be re-run safely. Cleans up after itself.

---

## 12. Rollout Plan (v2 — with log-only mode)

The reviewer pointed out that jumping straight from "deploy with flag off" to "enable for users" is too fast. The fix is a **log-only mode** — the cleaner runs, produces a report, but the output is discarded and the original data is used. This gives a free risk-free period of evidence collection.

### Feature flag states

```
CHATMRPT_DHIS2_CLEANER=off        # default — cleaner doesn't even run (production baseline)
CHATMRPT_DHIS2_CLEANER=log_only   # cleaner runs, writes report, but DISCARDS cleaned output
CHATMRPT_DHIS2_CLEANER=true       # cleaner runs and output is used
```

### Phase 1: Implementation + unit tests (day 1-2)
- Create `app/utils/dhis2_cleaner.py`
- Create `app/utils/dhis2_mojibake_patterns.py`
- Create `tests/test_dhis2_cleaner.py` with 16 test cases (from v2 test plan)
- Store fixtures in `tests/fixtures/kwara_snapshot.csv` and `tests/fixtures/adamawa_snapshot.csv`
- Run `pytest tests/test_dhis2_cleaner.py` until all pass

### Phase 2: Local end-to-end test with flag=true (day 3)
- Set `CHATMRPT_DHIS2_CLEANER=true` locally
- Upload Kwara → verify:
  - `uploaded_data.csv` has 28 columns (not 32)
  - `cleaning_report.json` created with correct entries
  - TPR workflow uses cleaned data (or cleaned `uploaded_data.csv`)
  - Burden number is different (higher than 100.2) and internally consistent
  - Agent can do trends over 6 years
  - Risk analysis produces different rankings than the broken v1 run
- Upload Adamawa → verify:
  - `uploaded_data.csv` unchanged (22 columns)
  - `cleaning_report.json` shows detected but no modifications
  - All functionality works
- Manual sanity checks:
  - Ask agent "How did TPR change from 2020 to 2025?" — should produce a declining trend story
  - Ask "Which LGAs are getting worse?" — should produce meaningful results

### Phase 3: Deploy to AWS, flag=off (day 4)
- Deploy both cleaner code + integration
- Flag defaults to `off` — **zero behavioral change in production**
- Verify prod is unaffected (existing sessions work)
- Smoke test: upload a file, workflow runs as before

### Phase 4: Enable log-only mode on AWS staging (day 5-7)
- Set `CHATMRPT_DHIS2_CLEANER=log_only` on staging only
- Cleaner runs on every upload, writes `cleaning_report.json`
- BUT the cleaned DataFrame is discarded — `uploaded_data.csv` is still the uncleaned version
- For 2-3 days, review cleaning reports from real uploads
- Look for:
  - False positives (non-DHIS2 files being detected)
  - Unexpected duplicate patterns
  - Exceptions in cleaner code
  - Validation check failures

**Exit criteria for advancing to Phase 5 (must meet ALL):**
- [ ] At least 10 distinct file uploads processed by log-only mode
- [ ] Zero `CleaningIntegrityError` exceptions in logs
- [ ] Zero unhandled exceptions in `app.utils.dhis2_cleaner` module logs
- [ ] Zero false-positive DHIS2 detections (manual review of cleaning reports where `detected_as='DHIS2 export'` but the file is clearly not DHIS2)
- [ ] Any Rule 5 (default 'sum' for ambiguous groups) fires are reviewed manually and confirmed not to represent double-counting of valid data
- [ ] Engineering owner reviews cleaning reports and signs off

**Owner:** whoever implements the cleaner (assign explicitly)
**Deadline:** no later than 7 days after log-only enabled. If criteria not met by day 7, either extend observation period with a reason, OR disable the flag and return to planning.

**Why this matters:** without explicit exit criteria, log-only mode risks sitting in that state indefinitely while users continue to see wrong Kwara TPR numbers. Define the success bar BEFORE starting observation.

### Phase 5: Enable full mode on staging (day 8)
- Flip to `CHATMRPT_DHIS2_CLEANER=true` on staging
- Monitor for 2-3 days
- Validate a few real uploads manually

### Phase 6: Enable on production with kill-switch (day 10)
- Flip production to `true`
- Keep flag as kill-switch for 1 month
- Monitor logs daily for first week

### Phase 7: Remove flag (month+2)
- Once confident, remove the feature flag code
- Cleaner becomes always-on, mandatory

### Implementing log-only mode

```python
def clean_dhis2_export(df, mode='full'):
    """
    mode='full'      → clean and return cleaned df
    mode='log_only'  → run detection and analysis, write report, but return ORIGINAL df unchanged
    """
    # ... detection, merge, report ...
    if mode == 'log_only':
        return df_original, report_with_note("LOG ONLY MODE — no changes applied")
    return df_cleaned, report
```

In the upload route:
```python
_mode = os.getenv('CHATMRPT_DHIS2_CLEANER', 'off').lower()
if _mode in ('true', '1', 'yes', 'full'):
    clean_mode = 'full'
elif _mode == 'log_only':
    clean_mode = 'log_only'
else:
    clean_mode = None  # don't run at all

if clean_mode:
    df_upload, cleaning_report = clean_dhis2_export(df_upload, mode=clean_mode)
    # write report always
```

This gives a risk-free observability period before the cleaner touches any user-visible data.

---

## 13. Validation Approach (since we lack external baseline)

The user correctly noted we don't have published NMEP Kwara numbers to validate against. So validation must be **internal consistency**:

### Internal checks (automated)

1. **Temporal consistency** — merged annual totals should be roughly similar year-over-year (flag outliers >3x median)
2. **Ratio sanity** — TPR computed from merged columns should stay in [0, 100]
3. **Sum reconciliation** — merged column total should equal sum of source column totals (within float precision)
4. **Count preservation** — row count must be unchanged
5. **Column monotonicity** — cleaning must only REMOVE columns, never ADD

### Semi-automated checks (prototype verified)

Before cleaning Kwara:
- col_A total: 60,625 (data only in 2020, 2025)
- col_B total: 547,686 (data in all 6 years)
- Sum: 608,311

After cleaning:
- merged column total: 608,311 ✅ matches
- Annual totals: 102k, 105k, 103k, 100k, 95k, 103k — temporally consistent ✅
- TPR: 77% → 61% over 6 years — plausible improvement trend ✅
- Numerator ≤ denominator: 18 violations (from source data, not us) ✅ expected

### Human validation (manual, after implementation)

1. Run the cleaner on Kwara
2. Ask the agent: "What was Kwara's under-5 malaria burden in each year from 2020-2025?"
3. Verify the response shows 6 years of data with reasonable values
4. Ask: "How has TPR changed over time?"
5. Verify trend analysis works and produces the 77%→61% declining trend
6. Run the full TPR workflow and verify Burden per 1,000 is higher than 100.2 (should be ~700 over 6 years or ~117/year)
7. Run risk analysis and verify rankings change from the buggy version
8. Visually spot-check 3-5 wards across the old vs new data

### Edge case testing

- Upload a CSV with ONE column named with `.1` (like `version_1.1`) — should not be flagged
- Upload a plain CSV with no DHIS2 signals — should pass through
- Upload a corrupted XLS — should error gracefully
- Upload an empty file — should error gracefully

---

## 14. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Summing duplicates double-counts real data | Medium | High | Rule 3 catches exact copies; temporal evidence validates summing for Kwara; feature flag for quick disable |
| Cleaning breaks working datasets (Adamawa-like) | Low | High | Conservative detection (2-of-3 signals required); pass-through tested |
| False positive on legitimate dotted column names | Low | Medium | Regex requires base column to exist; tested with `version_1.0/1.1` fixture |
| Mojibake regex false positive | Very low | Low | Whitelist of specific patterns, not generic replacement |
| Cleaner introduces bug that drops rows | Low | High | Check 4 (row count) blocks if violated |
| LLM schema still confused after cleaning | Very low | Medium | Cleaning removes the ambiguity (no more duplicates for LLM to see) |
| Performance on large files | Low | Low | Algorithms are O(n*cols), fine for millions of rows |
| User has domain knowledge we contradict | Medium | Medium | Cleaning report gives full transparency; user can disable flag |
| Source data quality issues (positive > tested) | High | Low | Report but don't fix; document as source issue |
| New DHIS2 format variant breaks detection | Medium | Medium | Conservative + logged; easy to add new patterns |

---

## 15. Open Questions

**Q1: Should we ever modify source data quality issues (positive > tested)?**
- Current plan: NO, just report. Let downstream (TPR calc) handle it.
- Alternative: cap positive at tested, log the fix.
- **Recommendation:** report only. Modifying source data without user consent is dangerous.

**Q2: What if a file has duplicates but ISN'T a DHIS2 export?**
- Current plan: skip cleaning entirely (conservative detection)
- Alternative: apply cleaning to any file with duplicates
- **Recommendation:** stay conservative. If a user has a weird CSV, they can flag it.

**Q3: Should we persist the cleaning report in the UI?**
- Current plan: Yes, as a collapsible section in upload response
- Alternative: just backend logs
- **Recommendation:** expose in UI. Trust is built through transparency.

**Q4: What happens when the user uploads a file that's PARTIALLY cleaned (some duplicates, no mojibake)?**
- Current plan: apply cleaner, it will merge what it finds
- This should work since detection is independent of fix-worthiness

**Q5: Do we need to clean Microscopy TPR column if it's a ratio?**
- For Kwara, `Test Positivity Rate(TPR) (Microscopy)` has no duplicate. Not applicable.
- Rule 2 would handle it if it did (combine_first for ratios)

**Q6: Should we handle the case where the period column itself has duplicates?**
- Extremely unlikely (source data would be fundamentally broken)
- Can add a safety check if it happens

**Q7: Does summing affect the LLM schema inference?**
- NO — the cleaner runs BEFORE schema inference. LLM sees clean columns.

---

## 16. Implementation Order (v4)

1. **Create `app/utils/dhis2_mojibake_patterns.py`** — data file with the regex patterns list

2. **Create `app/utils/dhis2_cleaner.py`** — implement the core algorithm
   - `get_cleaner_mode()` — env var parser
   - `is_dhis2_export()` — three-signal detector
   - `detect_period_column()` — heuristic finder (name-based + value-range)
   - `fix_mojibake()` — apply whitelist, return (renamed_df, rename_map)
   - `detect_duplicate_groups()` — regex for `col.N` pattern
   - `analyze_group()` — decision tree (Rules 1-5)
   - `merge_group()` — apply sum/combine_first/drop_extra
   - `validate_cleaning()` — blocking and warning checks
   - `apply_rename_map_to_schema()` — schema sync helper
   - `CleaningReport` dataclass + `to_dict()`
   - `clean_dhis2_export()` — main entry point, follows the ordering in Section 5.5

3. **Create `tests/test_dhis2_cleaner.py`** — write tests first, iterate until all pass
   - 16 unit tests from Section 11 + new Test 11a (schema coordination invariant)
   - Fixtures for Kwara snapshot and Adamawa snapshot
   - Run `pytest tests/test_dhis2_cleaner.py` until all pass

4. **Create `scripts/integration_test_kwara.py`** — manual end-to-end test (Section 11.5)
   - Run once locally with real Kwara XLS
   - All 9 integration checks must PASS before advancing

5. **Update system prompt** (`app/agent/prompts/system_prompt.py`)
   - Replace line 86 DHIS2 Column Conventions with generic guidance (Section 10.6)
   - Replace line 174 trend analysis hint (Section 10.6)

6. **Integrate into upload flow — Path A** (`app/api/data_analysis_routes.py:327-349`)
   - Call cleaner after `infer_schema_from_file()`
   - Apply `rename_map` to `schema_at_upload` via `apply_rename_map_to_schema()`
   - Save the UPDATED schema to state_manager
   - Save cleaning report JSON
   - Save cleaned df as `uploaded_data.csv`
   - Handle exceptions gracefully (never break upload)

7. **Integrate into Path B** (`app/api/data_analysis_routes.py:747-758`)
   - Prefer reading cleaned `uploaded_data.csv`
   - Fallback: read raw + apply cleaner + update local schema via `apply_rename_map_to_schema()`

8. **Integrate into Path C** (`app/api/data_analysis_routes.py:912-954`)
   - Prefer reading cleaned `uploaded_data.csv` (use saved schema, which already reflects cleaner)
   - Fallback: read raw + apply cleaner + update schema + persist schema back to state_manager

9. **Add cleaning report to agent context** (`app/agent/agent.py`)
   - Read `cleaning_report.json` in `_create_data_summary()`
   - Append cleaning notes to the data summary message

10. **Test end-to-end locally**
    - Set `CHATMRPT_DHIS2_CLEANER=full`
    - Upload Kwara → verify `uploaded_data.csv` has 28 cols, cleaning report generated
    - Start TPR workflow → verify no exceptions, raw_data.csv produced
    - Verify Burden per 1,000 is different from the previous 100.2 (broken) run
    - Ask agent trends → verify it produces a 6-year series
    - Run risk analysis → verify rankings produced
    - Generate maps → verify HTML files produced
    - Upload Adamawa → verify no changes
    - Run `scripts/integration_test_kwara.py` → all checks PASS

11. **Documentation**
    - Update `docs/architecture/ARCHITECTURE.md` with cleaner module
    - Add entry to `CLAUDE.md` Key Files section
    - Add a "DHIS2 data cleaning" section explaining what the cleaner does

12. **Deploy to AWS** (with `CHATMRPT_DHIS2_CLEANER=off` initially)

13. **Enable `log_only` on staging** per Phase 4 of the rollout plan

14. **Advance to `full` mode** once Phase 4 exit criteria are met

15. **Enable in production** per Phase 6

16. **Remove feature flag** after 1 month of stable production operation

---

## 17. Estimated Effort

- Cleaner module: ~200 lines
- Unit tests: ~150 lines
- Integration: ~20 lines
- Agent context update: ~15 lines
- Documentation: ~50 lines
- **Total: ~435 lines of code**

---

## 18. What This Does NOT Do (Non-Goals)

- **Does not clean non-DHIS2 files.** Generic CSVs pass through untouched.
- **Does not fix source data quality issues** (positive > tested, negative values, etc.). Just reports them.
- **Does not modify the original uploaded file.** Only affects `uploaded_data.csv`.
- **Does not change LLM schema inference logic.** Just feeds it cleaner input.
- **Does not add new analysis features.** Just makes existing features work correctly.
- **Does not replace domain expertise.** Users with knowledge of their DHIS2 setup can disable the flag and handle cleaning manually.
- **Does not detect state-level or national-level aggregated DHIS2 exports.** Such exports have no facility hierarchy column (`orgunitlevel*`, `organisationunit*`), so they fail signal 3 of the three-signal detection. These files pass through unchanged. This is acceptable because:
  (a) Pre-aggregated data rarely has the DHIS2 form-migration duplicate pattern that motivates this cleaner
  (b) False negatives on aggregated data are safer than risking false positives on non-DHIS2 files
  Users uploading aggregated data who DO have duplicate columns can disable the feature flag and clean manually.

- **Does not auto-migrate existing sessions.** Sessions created before the cleaner ships will have unmerged duplicates and mojibake in their `uploaded_data.csv`. The cleaner does NOT re-process these files on resume. Users who want corrected results must re-upload their data. Rationale:
  (a) Auto-migration would require running the cleaner on every session resume, with the risk of changing data the user has already validated
  (b) The Kwara bug is already in those sessions; re-running won't fix the already-computed `raw_data.csv` or `unified_dataset.csv`
  (c) Re-upload is a cheap, explicit, user-controlled action
  A banner or notice in the session list informing users of the fix would help adoption — consider adding this in a follow-up change.

---

## 19. Next Step

**Plan is v4 — ready for implementation after final review.**

### Changes from v3 to v4 summary

1. Fixed schema-cleaner coordination (the class of bug the user caught)
2. Added `column_rename_map` to the cleaning contract
3. Added `apply_rename_map_to_schema()` helper
4. Updated all 3 integration points to apply the rename-map
5. Reverted Rule 5 from `keep_both_renamed` to `sum` (keep_both would have broken the schema)
6. Reverted Rule 4b from `non-null` to `non-zero` (non-null empirically fails on Kwara)
7. Fixed cleaning phase ordering (mojibake runs BEFORE duplicate detection)
8. Added Section 10.5: Schema Coordination Contract
9. Added Section 10.6: System Prompt Updates
10. Added Section 11.5: Integration Test Script
11. Added upgrade compatibility note (no auto-migration)
12. Added Test 11a: schema coordination invariant
13. Updated implementation order with all new steps

### Final sign-off items

- [ ] Do you agree with the schema coordination contract (Section 10.5)?
- [ ] Is the integration test script (Section 11.5) sufficient as a smoke test?
- [ ] Are the system prompt changes (Section 10.6) acceptable? They remove hardcoded column references.
- [ ] Upgrade compatibility: accept "re-upload to fix" or demand auto-migration?
- [ ] Log-only mode exit criteria (Phase 4) acceptable?
- [ ] Ready to begin implementation with Step 1 of Section 16?

### Verification done

✅ Prototype run on real Kwara data — 32→28 cols, temporally consistent, 77%→61% TPR trend
✅ v4 simulation with schema rename-map — all 8 schema fields resolve correctly
✅ Adamawa pass-through tested — no false positives
✅ Non-DHIS2 fake CSV tested — no false positives
✅ Dotted-name CSV tested — not flagged as duplicates
✅ Rule 4a verified on all 4 Kwara duplicate pairs — all produce sum
✅ TPR calculation simulated with updated schema — annual totals correct
✅ Two rounds of independent code review applied
