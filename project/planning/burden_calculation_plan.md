# Fixing High TPR Values: Replace with Malaria Burden Calculation

## The Issue

Our current TPR (Test Positivity Rate) implementation produces **unrealistically high values** (often 50-90%). This is because TPR uses tests performed as the denominator:

```
Current: TPR = (positive cases / tests performed) × 100
```

The problem is that testing effort varies widely between wards - wards with less testing capacity show artificially inflated rates.

---

## The Fix

Replace TPR with **Malaria Burden per 1,000 Population** - a standard epidemiological metric that uses ward population as the denominator:

```
New: Burden = (positive cases / ward population) × 1,000
```

This gives values in a sensible range (typically 5-100 per 1,000) and is comparable across wards regardless of testing effort.

---

## Data We Will Use

### Population Rasters (WorldPop)

| Age Group | Raster File | Status |
|-----------|-------------|--------|
| Total | `data/geospatial/nigeria.tif` | ✓ Already have |
| Under 5 | `data/geospatial/NGA_population_v2_0_agesex_under5.tif` | ✓ Already have |
| Women 15-49 | `data/geospatial/NGA_population_v2_0_agesex_f15_49.tif` | Need to copy from Downloads |

### Age-Specific Denominators

| User Selection | Numerator | Denominator |
|----------------|-----------|-------------|
| Under 5 (U5) | U5 positive cases | U5 population |
| Over 5 (O5) | O5 positive cases | Total - U5 population |
| Pregnant Women (PW) | PW positive cases | Women 15-49 population |
| All Ages | All positive cases | Total population |

---

## Formula

```python
# Old (problematic)
tpr = (positive / tested) * 100  # Percentage, often 50-90%

# New (fix)
burden = (positive / population) * 1000  # Per 1,000, typically 5-100
```

---

## Files to Modify

### 1. `requirements.txt`
Add: `rasterstats>=0.19.0`

### 2. `app/config/data_paths.py`
Add population raster paths:
```python
GEOSPATIAL_DIR = os.path.join(PROJECT_ROOT, 'data', 'geospatial')
POP_TOTAL_RASTER = os.path.join(GEOSPATIAL_DIR, 'nigeria.tif')
POP_U5_RASTER = os.path.join(GEOSPATIAL_DIR, 'NGA_population_v2_0_agesex_under5.tif')
POP_F15_49_RASTER = os.path.join(GEOSPATIAL_DIR, 'NGA_population_v2_0_agesex_f15_49.tif')
```

### 3. `app/core/tpr_utils.py`
- Add `extract_ward_population()` function to get population from rasters
- Modify `calculate_ward_tpr()` → change formula to use population denominator
- Modify `prepare_tpr_summary()` → rename keys from `mean_tpr` to `mean_burden` etc.

### 4. `app/core/tpr_precompute.py`
- Change schema: `tpr` column → `burden`, `total_tested` → `population`
- Update INSERT and SELECT queries

### 5. `app/data_analysis_v3/tools/tpr_analysis_tool.py`
- Update map title: "TPR Distribution" → "Malaria Burden per 1,000"
- Update hover text: "Ward TPR: 45%" → "Burden: 23.5 per 1,000"
- Update colorbar label: "TPR (%)" → "Burden (per 1,000)"
- Update completion message: "TPR Analysis Complete" → "Malaria Burden Analysis Complete"

---

## What Stays the Same

- User workflow (upload → select state → select facility → select age group → view map)
- The selection options
- The map visualization style
- File names (tpr_utils.py, etc.)

---

## Verification

After implementation:
1. Upload TPR data, complete the workflow
2. Check completion message says "Malaria Burden Analysis Complete"
3. Check values are per 1,000 (expect 5-100 range, not 50-90%)
4. Check map shows "Malaria Burden per 1,000" title
5. Check hover shows "Burden: X per 1,000"

---

## Review Findings (from second agent)

### Confirmed: Main Files to Modify
These are the active files used by the production workflow:

| File | Role | Import Chain |
|------|------|--------------|
| `app/core/tpr_utils.py` | Core calculation | Used by tpr_analysis_tool.py and tpr_precompute.py |
| `app/core/tpr_precompute.py` | Database storage | Imports calculate_ward_tpr from core |
| `app/data_analysis_v3/tools/tpr_analysis_tool.py` | Map & labels | Imports from app.core.tpr_utils |
| `app/config/data_paths.py` | File paths | Add raster paths |
| `requirements.txt` | Dependencies | Add rasterstats |

### Legacy/Duplicate Files (cleaned up)

| File | Status |
|------|--------|
| `app/data_analysis_v3/core/tpr_utils.py` | ✅ **DELETED** - was unused duplicate |
| `app/runtime/tpr/` | KEEP - wrapper for main workflow, not a duplicate |
| `app/tpr_module/` | Legacy - can delete later (only used for optional cleanup in session_utils.py) |

### Additional Checks Needed
- `app/core/request_interpreter.py:321` - May reference TPR
- `app/core/analysis_routes.py:751` - May reference TPR
- Two calls to `calculate_ward_tpr` in tpr_analysis_tool.py (lines ~600 and ~975)

### Schema Migration
When schema changes from `tpr/total_tested` to `burden/population`:
- Existing `tpr_precomputed.db` files will be incompatible
- They will need to be deleted and regenerated
- No backfill needed - just regenerate on next user session
