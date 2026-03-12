# Kwara TPR Data Investigation

**Date:** 2026-03-10
**Data file:** `Kwara TPR data 2020 - 2025 ~ 2026-03-02.xls`
**Status:** FIXED — all critical bugs confirmed and patched in `data_analyzer.py`

---

## Background

Kwara State provided their Test Positivity Rate (TPR) facility data (2020–2025) exported from DHIS2.
The file failed in ChatMRPT. This document explains all confirmed failure points and what was changed.

---

## How TPR Actually Works in ChatMRPT

TPR is **not** triggered at upload time. The upload route simply stores the file as raw data.
TPR mode is triggered **during chat** when the user types a phrase like "start tpr workflow".

At that point, `app/web/routes/data_analysis_v3_routes.py` (lines 835–866):
1. Detects the trigger phrase
2. Loads the stored raw data file from the session folder
3. Creates a `TPRWorkflowHandler` + `TPRDataAnalyzer` with that dataframe
4. Activates TPR mode in the session state manager
5. Routes all subsequent messages through the TPR workflow

This is why other NMEP/DHIS2 files work — upload detection is irrelevant to the TPR path.

---

## The Data

| Metric | Value |
|--------|-------|
| Sheet name | Sheet 1 |
| Total records | 6,948 |
| Health facilities | 1,142 |
| LGAs | 16 |
| Wards with data | 186 |
| Years covered | 2020–2025 (annual) |
| Records with TPR data | 5,778 (83.2%) |
| Records with Ward = Unknown | 1,170 (16.8%) |
| TPR RDT mean | 23.7% |
| TPR RDT range | 0–100% |

**Column layout (DHIS2 export format):**

| Kwara DHIS2 column | Meaning | Notes |
|-------------------|---------|-------|
| `orgunitlevel2` | State ("kw Kwara State") | 1 unique value |
| `orgunitlevel3` | LGA (16 values) | Contains "level" in name — caused critical bug |
| `Ward` | Ward name | `kw ` prefix stripped by runtime/tpr/utils.py |
| `organisationunit0me` | Facility name (unicode corruption: `û`→`0`) | Not detected by old keyword scan |
| `period0me` | Year as integer (2020–2025) | Annual, not monthly |
| `Facility level` | primary / secondary / Tertiary | Correct column, was skipped |

Compare with Enugu (working reference): `State`, `LGA`, `WardName`, `HealthFacility`, `periodname` (date string), `FacilityLevel`

---

## Issue 1: Blank First Row (FIXED at file read)

**Confirmed by live test (2026-03-10):**
```
First 5 columns: ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4']
Execution completed — Output length: 0 chars
```
The Kwara XLS had a blank row at position 0. `pd.read_excel()` with default `header=0` produced
all-unnamed columns. **Fix:** User deleted the blank row from the file. A more robust fix would
detect `>80% Unnamed columns` at load time and retry with `header=1`.

---

## Issue 2: CRITICAL — `analyze_facility_levels()` picks LGA column instead of facility type column

**File:** `app/data_analysis_v3/tpr/data_analyzer.py`, method `analyze_facility_levels()`

**Root cause:** The method scans columns in order for any that contain keywords
`['facility', 'level', 'type', 'tier', 'category']`. Because `orgunitlevel3` (LGA) appears
**before** `Facility level` in the column list, and `orgunitlevel3` contains the keyword `level`,
it is selected as the facility level column with 16 unique LGA names as its values.

**Observed symptom:** User sees 16 LGA names ("kw Ilorin South Local Government Area", etc.)
in the facility selection step instead of primary / secondary / Tertiary.

**Downstream cascade:** When the user selects "primary", `_filter_data()` tries to match
"primary" against LGA names — no match — silently returns **all 6,948 rows unfiltered**.

**Fix applied:** Skip columns whose name starts with `orgunitlevel` in the level detection loop.
These are always org-unit hierarchy columns (State/LGA/Ward), never facility types.

---

## Issue 3: MODERATE — Facility name column not detected

**File:** `app/data_analysis_v3/tpr/data_analyzer.py`, method `analyze_states()`

**Root cause:** Facility name detection looks for keywords `['facility', 'clinic', 'hospital', 'health', 'center']`.
`Facility level` matches `facility` and is picked first (3 unique values: primary/secondary/Tertiary).
The actual facility name column `organisationunit0me` has no keyword match.

**Observed symptom:** Facility count per state shows 3 instead of 1,142.
Map labels and facility-level analysis cannot reference facility names correctly.

**Fix applied:** Added `organisationunit` to the facility name detection patterns (covers both
the standard `organisationunitName` and the unicode-corrupted `organisationunit0me`).
Also excluded columns with 'level' in their name from the facility-name scan.

---

## Issue 4: MINOR/GRACEFUL — `orgunitlevel2` not detected as state

**File:** `app/data_analysis_v3/tpr/data_analyzer.py`, method `analyze_states()`

**Root cause:** State detection looks for keywords `['state', 'region', 'province', 'area']`.
`orgunitlevel2` matches none of these. The method falls back to single-dataset mode.

**Observed symptom:** System treats entire dataset as "All Data" rather than Kwara State.
This is **graceful degradation** — the workflow continues, it just doesn't sub-select by state.

**Fix applied:** Added `orgunitlevel2` as an explicit priority check for the state column before
the generic keyword scan.

---

## Issue 5: KNOWN LIMITATION — Annual integer periods

**File:** Multiple (time-series features)

The Kwara file has `period0me` with integer years 2020–2025 (not date strings like "January 2023").
Monthly trend charts and time-series analysis will not work.

**What still works:** Ward-level TPR maps, LGA comparisons, facility rankings — all core
micro-planning outputs remain functional.

**No fix applied.** Graceful degradation is acceptable. Future improvement: detect integer
period columns and surface a user-facing message explaining the limitation.

---

## What Already Works (No Changes Needed)

The live `app/runtime/tpr/utils.py` handles the Kwara DHIS2 export format correctly:

- `orgunitlevel3` column recognised as LGA ✓
- `Ward` column with `kw ` state prefix stripped (`kw Afon Ward` → `Afon`) ✓
- Flexible RDT/Microscopy column detection by keyword ✓
- LGA name cleaning (`kw Ilorin West Local Government Area` → `Ilorin West`) ✓
- Ward TPR aggregation from facility-level data ✓

---

## Data Quality Notes for Kwara State

1. **16.8% of records have Ward = "Unknown"** (1,170 rows). These facilities have no ward
   assignment in DHIS2. They will not appear on ward-level maps. Recommend flagging to
   the Kwara state team.

2. **Unicode corruption in column/ward names.** `û` replaced with `0` in DHIS2 export:
   - Column: `organisationunit0me` (should be `organisationunitName`)
   - Column: `period0me` (should be `periodname`)
   - Ward names: `Gwa0be`, `Ade0`, `Ajan0ku`, `Igbon0`, etc.
   This is a DHIS2 export setting issue on the Kwara side. Not a blocker.

3. **Duplicate column names.** Two columns share "Persons presenting with fever & tested by
   RDT <5yrs" — pandas renames the second to `.1`. Harmless.

4. **Inconsistent `Facility level` casing.** Values are `primary`, `secondary`, `Tertiary`
   (mixed case). The fix handles this via case-insensitive filtering.

---

## Fix Summary

All changes are in `app/data_analysis_v3/tpr/data_analyzer.py`:

| Issue | Method | Change |
|-------|--------|--------|
| orgunitlevel3 picked as facility level | `analyze_facility_levels()` | Skip columns starting with `orgunitlevel` |
| Facility name not detected | `analyze_states()` | Add `organisationunit` to facility name patterns |
| orgunitlevel2 not detected as state | `analyze_states()` | Add explicit `orgunitlevel2` priority check |

---

## Note on Dead Code (Separate Issue)

`app/web/routes/upload_routes.py` contains a broken import block that tries to load
`tpr_module.integration` (a package that no longer exists). This silently sets
`TPR_MODULE_AVAILABLE = False` and disables an old upload-time TPR detection path.

This is dead code — it does **not** affect the working TPR flow (chat-triggered, not upload-triggered).
Tracked separately as Step 3 of the broader TPR dead code removal.
