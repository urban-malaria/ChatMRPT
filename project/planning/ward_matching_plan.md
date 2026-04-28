# Centralized Ward Name Matching — Implementation Plan
*Written: 2026-04-17 | Revised: 2026-04-17 (v3) | Status: AWAITING APPROVAL*
*Reviewed by: code-reviewer agent × 2 + manual code verification*

---

## Problem Statement

Per-year maps show ~115/193 wards. The join in `workflow_manager.py` is normalized on both sides — the root cause is unknown until measured.

Two plausible explanations that cannot be distinguished without data:

| Explanation | Mechanism | Fix |
|-------------|-----------|-----|
| A — Missing data | Ward had `total_tested <= 0` in that year → skipped from `tpr_time_series.csv` → NaN in per-year merge → dropped by map builder | Show "no data" grey trace on map — not a bug |
| B — Name mismatch | Normalized DHIS2 name still differs from normalized shapefile name (spelling, mojibake) → NaN after join → dropped | Fuzzy fallback at join |

Measure first. Fix what the data shows.

---

## What the code actually does (verified)

### Per-year join (`workflow_manager.py:852–873`)
```
raw_data['_ward_key'] = normalize(shapefile WardName)   ← normalized ✅
ts_df['WardName']     = normalized DHIS2 name           ← also normalized ✅
  (grouped on WardName_clean in calculate_ward_tpr_timeseries, utils.py:480)
```
Both sides are normalized. The join is not obviously broken.

### `total_tested <= 0` filter (`utils.py:503`)
```python
if total_tested <= 0:
    continue  # ward-year row never written to tpr_time_series.csv
```
A ward with no recorded tests in a given year is simply absent from the timeseries. The per-year merge produces NaN. `variable_distribution.py:327` drops it:
```python
clean_data = merged_data.dropna(subset=[variable])
```
This is a data gap, possibly not a bug.

### WardCode is available end-to-end
`analysis_tool.py:1173` writes WardCode into `raw_data.csv`. The per-year merge at `workflow_manager.py:864–868` does `raw_data.drop(['Burden','Total_Positive']).merge(year_data)` — all other columns including WardCode carry through. The map builder at `variable_distribution.py:304` tries join columns in order:
```python
join_columns = ['WardName', 'WardCode', 'LGACode', 'ward_name', 'ward_code']
```
WardName is tried **first** — but WardCode (numeric, no spelling variation) is more reliable and should be preferred.

### Current normalize implementations (two, verified)
| File | Behavior |
|------|----------|
| `app/tpr/utils.py:111` | Strips 2-letter state prefix (`^[a-z]{2}\s+`), removes `Ward`, unifies `-`/`/` → space, lowercase |
| `app/analysis/itn_pipeline.py:240` | Strips parentheses, underscores→space, full roman numerals I–IX with word boundaries, removes ` ward`/` wards` suffix, unifies separators |

These diverge on: parentheses, underscores, roman numerals I–IX. Neither is a strict superset.

`variable_distribution.py` does **not** import from `tpr/utils` — it merges directly on raw column names.

---

## Phase 0 — Measure before touching anything

**File**: `scripts/ward_match_audit.py` — standalone, reads existing session CSVs, writes report, makes no changes.

**What to count per year**:
- Exact normalized matches
- Fuzzy matches (difflib SequenceMatcher ≥ 0.70)
- Wards absent because `total_tested <= 0` (traceable from timeseries)
- Wards unmatched after fuzzy (genuine name mismatch)
- **Duplicate normalized keys** — wards that normalize to the same string on either the shapefile or DHIS2 side (ambiguity risk)
- Whether WardCode / LGACode are present on both sides (for disambiguation)

**Output**: `ward_match_report.json` per session:
```json
{
  "total_shapefile_wards": 193,
  "years_analyzed": [2020, 2021, 2022, 2023, 2024],
  "wardcode_available": true,
  "duplicate_normalized_keys_shapefile": 2,
  "duplicate_normalized_keys_dhis2": 3,
  "per_year": {
    "2022": {
      "exact_match": 89,
      "fuzzy_match": 14,
      "no_data_total_tested_zero": 61,
      "name_mismatch_unmatched": 29,
      "ambiguous_duplicate_key": 0
    }
  },
  "unmatched_pairs": [
    {"shapefile": "Birnin-Gwari", "shapefile_norm": "birnin gwari",
     "dhis2_closest": "birnin gwari", "score": 0.91, "method": "fuzzy"}
  ]
}
```

This tells us: which explanation dominates, and whether WardCode can safely replace WardName as the primary join key.

---

## Phase 1 — Create `app/utils/ward_matcher.py` (canonical module)

**Condition**: Proceed regardless of Phase 0 results — centralizing is worth doing either way.

Merges both existing implementations. Must not regress either.

From `tpr/utils.py`:
- Strip 2-letter state prefix: `re.sub(r'^[a-z]{2}\s+', '', name, flags=re.IGNORECASE)`
- Remove `Ward` keyword anywhere: `re.sub(r'\bward\b', '', name, flags=re.IGNORECASE)`
- Unify `-` and `/` → space

From `itn_pipeline.py`:
- Strip parenthetical suffixes: `name.split('(')[0].strip()`
- Replace underscores with spaces
- Full roman numeral conversion I–IX (order: viii→8 before vii→7 before vi→6, ix→9 before iv→4, v→5, then iii→3, ii→2, i→1)

```python
# app/utils/ward_matcher.py

import re
import difflib

_TWO_LETTER_PREFIX = re.compile(r'^[a-z]{2}\s+', re.IGNORECASE)
_WARD_KEYWORD      = re.compile(r'\bward\b', re.IGNORECASE)
_SEPARATORS        = re.compile(r'[/\-]+')

# Longer patterns before shorter to avoid partial replacement
_ROMAN = [
    (r'\bviii\b', '8'), (r'\bvii\b', '7'), (r'\bvi\b', '6'),
    (r'\bix\b',   '9'), (r'\biv\b',  '4'), (r'\bv\b',  '5'),
    (r'\biii\b',  '3'), (r'\bii\b',  '2'), (r'\bi\b',  '1'),
]

def normalize_ward_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return ''
    s = str(name).strip()
    s = s.split('(')[0]                      # strip parenthetical suffix
    s = _TWO_LETTER_PREFIX.sub('', s)        # strip 2-letter state prefix
    s = _WARD_KEYWORD.sub('', s)             # remove "Ward" keyword
    s = _SEPARATORS.sub(' ', s)              # unify - and /
    s = s.replace('_', ' ')                  # underscores to spaces
    s = s.lower()
    for pattern, replacement in _ROMAN:
        s = re.sub(pattern, replacement, s)
    return ' '.join(s.split())               # collapse whitespace


def fuzzy_match_ward(
    query: str,
    candidates: list[str],
    cutoff: float = 0.70,
) -> tuple[str, float] | tuple[None, None]:
    """
    Return (best_match, score) or (None, None) if no match above cutoff.
    Returns None if multiple candidates share the same top normalized key
    (ambiguous — caller must handle, not auto-pick).
    Uses difflib only — scores are not comparable to fuzzywuzzy.
    """
    q_norm = normalize_ward_name(query)

    # Build mapping: normalized → list of originals (preserves ambiguity)
    normed: dict[str, list[str]] = {}
    for c in candidates:
        key = normalize_ward_name(c)
        normed.setdefault(key, []).append(c)

    matches = difflib.get_close_matches(q_norm, normed.keys(), n=1, cutoff=cutoff)
    if not matches:
        return None, None

    originals = normed[matches[0]]
    if len(originals) > 1:
        # Ambiguous — multiple wards normalize to the same key
        return None, None

    score = difflib.SequenceMatcher(None, q_norm, matches[0]).ratio()
    return originals[0], round(score, 3)
```

**Replace duplicate normalizers in**:
- `app/tpr/utils.py` — thin wrapper: `from app.utils.ward_matcher import normalize_ward_name`
- `app/analysis/itn_pipeline.py` — replace local definition with import
- `app/services/shapefile_fetcher.py:176` — replace local helper with import

**Do not touch**: `data_access.py` INNER joins (intentional). `variable_distribution.py` merges on raw column names directly — update its join column order (see Phase 2b), not its imports.

---

## Phase 2 — Fix based on Phase 0 findings

### 2a — Map builder: prefer WardCode first (always apply)

`variable_distribution.py:304` currently tries WardName before WardCode. Since WardCode is numeric and has no spelling variation, and is confirmed present end-to-end, reorder:

```python
join_columns = ['WardCode', 'LGACode', 'WardName', 'ward_code', 'ward_name']
```

This is safe to apply regardless of Phase 0 results.

### 2b — If Phase 0 shows Explanation B dominates (name mismatches)

Add fuzzy fallback in `workflow_manager.py` per-year merge. Handles ambiguity explicitly — no `.iloc[0]` auto-pick:

```python
from app.utils.ward_matcher import normalize_ward_name, fuzzy_match_ward

raw_data['_ward_key'] = raw_data['WardName'].apply(normalize_ward_name)
year_rows = ts_df[ts_df['Period'] == year].copy()
year_rows['_ward_key'] = year_rows['WardName'].apply(normalize_ward_name)

# Exact merge first
year_data = year_rows[['_ward_key', 'Total_Positive', 'Burden']]
year_raw = raw_data.drop(columns=['Burden', 'Total_Positive'], errors='ignore') \
                   .merge(year_data, on='_ward_key', how='left')

# Fuzzy fallback for still-unmatched (skips ambiguous)
dhis2_keys = year_rows['_ward_key'].tolist()
for idx in year_raw[year_raw['Burden'].isna()].index:
    match, score = fuzzy_match_ward(year_raw.loc[idx, '_ward_key'], dhis2_keys)
    if match is not None:  # None = ambiguous or no match
        src = year_rows[year_rows['_ward_key'] == match].iloc[0]
        year_raw.loc[idx, ['Total_Positive', 'Burden']] = src[['Total_Positive', 'Burden']].values
```

**Do not lower `analysis_tool.py` cutoff (0.80) unless Phase 0 shows the aggregate path is also under-matching.** The aggregate currently produces 193/193 — lowering the cutoff risks false positives without evidence of under-matching.

### 2c — If Phase 0 shows Explanation A dominates (genuine no-data)

Fix the **map builder** to show all shapefile wards instead of dropping no-data ones. Uses the existing `add_trace()` helper at `variable_distribution.py:485` — add a second trace for no-data wards in grey rather than relying on NaN colorscale behavior (which is unreliable in Plotly Choroplethmapbox):

```python
# After clean_data = merged_data.dropna(subset=[variable])  ← keep this for the data trace

no_data = merged_data[merged_data[variable].isna() & merged_data.geometry.notna()]

# Existing data trace (unchanged)
add_trace(clean_data, show_scale=True, opacity=0.8)

# New no-data trace — grey, no colorscale, separate legend entry
if not no_data.empty:
    add_trace(no_data, show_scale=False, opacity=0.4,
              colorscale_override=[[0, 'lightgrey'], [1, 'lightgrey']],
              name_suffix='No data')
```

### 2d — If Phase 0 shows a mix

Apply 2b and 2c together.

---

## What is NOT changing

- `data_access.py` INNER joins — intentional, upstream is the fix
- `analysis_tool.py` cutoff (0.80) — keep unless Phase 0 shows aggregate under-matching
- fuzzywuzzy usage in `itn_pipeline.py` — difflib and fuzzywuzzy scores are not comparable; leave itn fuzzy logic alone

---

## Implementation Order

1. `scripts/ward_match_audit.py` — Phase 0: measure
2. Run audit on a real session, read `ward_match_report.json`
3. `app/utils/ward_matcher.py` — Phase 1: canonical module
4. Replace duplicate normalizers in `tpr/utils.py`, `itn_pipeline.py`, `shapefile_fetcher.py`
5. `variable_distribution.py:304` — reorder join columns (WardCode first)
6. Apply whichever Phase 2b/2c fix(es) the audit supports
7. Targeted tests: ward matching unit tests + visual spot-check of per-year maps

---

*Ready to implement Phase 0 on approval.*
