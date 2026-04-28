# LGA Implementation Issues - Investigation Report

**Date**: January 2026
**Reported by**: Grace Legris, Eniola Bamgboye

---

## Summary of Issues

| Issue | Reporter | State | Severity |
|-------|----------|-------|----------|
| ITN distribution gives 100% to one ward/LGA | Grace Legris | Kwara | High |
| "Municipal Area Council" LGA missing | Eniola Bamgboye | FCT | Medium |
| Ward hover shows LGA names instead of ward names | Eniola Bamgboye | FCT, Edo | High |

---

## Issue 1: ITN Distribution Giving 100% to One Ward/LGA

### Symptoms
- When requesting ITN distribution with 10,000 nets and household size of 5, all nets go to one ward
- Same behavior at LGA level - all nets to one LGA
- Distribution "changed" when revisited later (showed multiple LGAs receiving nets)

### Root Cause Analysis

**File**: `app/analysis/itn_pipeline.py`

The ITN allocation uses a two-tier system (lines 608-711):

```
Tier 1: Rural wards (urban% < threshold) - get priority
Tier 2: Urban wards (urban% >= threshold) - get surplus
```

**Potential causes:**

1. **Population Data Issue** (Most Likely)
   - If population data fails to load/match correctly, wards get estimated population
   - Line 582-590: `avg_population` is used when data is missing
   - If ONE ward has correct population data and others don't, that ward could consume all nets

2. **Single High-Population Ward**
   - `nets_needed = ceil(population / 1.8)` (line 351)
   - A ward with 18,000+ population would need 10,000+ nets
   - That single ward would consume the entire allocation

3. **Urban Threshold Misconfiguration**
   - If all wards are classified as "urban" (urban% >= 75%), they all go to Tier 2
   - First ward in Tier 2 sorted by rank could get everything

4. **Session/State Inconsistency** (explains "changed later")
   - Multi-worker environment with Redis state
   - Different requests may hit different workers with different cached state
   - Lines 803-822 store ITN params in Redis, but race conditions possible

### How to Verify

```python
# Check population data for Kwara
from app.data.population_data.itn_population_loader import get_population_loader
loader = get_population_loader()
pop_df = loader.load_state_population("Kwara")
print(pop_df[['WardName', 'Population']].describe())
print(pop_df.nlargest(10, 'Population'))  # Check for outliers
```

### Recommended Fix

1. Add logging to show allocation breakdown per ward
2. Cap single-ward allocation at a percentage of total (e.g., 20%)
3. Implement proportional distribution across all qualifying wards instead of sequential exhaustion

---

## Issue 2: Missing "Municipal Area Council" LGA in FCT

### Symptoms
- FCT visualization missing one LGA: "Municipal Area Council"
- Other 5 FCT LGAs appear correctly

### Root Cause Analysis

**File**: `app/utils/lga_boundaries.py`

The reference boundary file is: `app/reference_data/nga_lga_boundaries.gpkg`

LGA matching uses normalized name comparison (lines 110-125):

```python
normalized["lga_name_norm"] = normalized["LGAName"].astype(str).str.strip().str.lower()
# Merged on: ["state_name_norm", "lga_name_norm"]
```

**Problem**: Name mismatch between user data and reference file:
- User data likely has: `"Municipal Area Council"` or `"Municipal"`
- Reference file likely has: `"Abuja Municipal Area Council"` or `"AMAC"`

### How to Verify

```python
# Check what FCT LGAs are in the reference file
from app.utils.lga_boundaries import get_lga_boundaries
ref = get_lga_boundaries()
fct_lgas = ref[ref['state_name'].str.lower().str.contains('fct|abuja|federal capital')]
print(fct_lgas['lga_name'].unique())
```

### Recommended Fix

1. Add fuzzy matching for LGA names (like ward name matching in ITN pipeline)
2. Create alias mapping for known variations:
   ```python
   LGA_ALIASES = {
       "municipal area council": "abuja municipal area council",
       "municipal": "abuja municipal area council",
       "amac": "abuja municipal area council",
   }
   ```
3. Update reference file to include common name variations

---

## Issue 3: Ward Hover Shows LGA Names Instead of Ward Names

### Symptoms
- At WARD level view, hovering over a ward shows LGA name, not ward name
- Affects FCT and Edo states
- Expected: hover should show ward name at ward level, LGA name at LGA level

### Root Cause Analysis

**File**: `app/tools/variable_distribution.py`, line 423

```python
label_series = df.get('LGAName', df.get('WardName', df.get('ward_name', df.get('LGACode', df.index))))
```

**THE BUG**: Priority order is wrong!
- It checks for `LGAName` FIRST
- After `annotate_with_lga_names()` is called (line 327), `LGAName` column exists
- So it ALWAYS uses LGAName, even at ward level

The hovertemplate on line 430:
```python
hovertemplate=f'<b>%{{text}}</b><br>{variable}: %{{z}}<extra></extra>',
```

Uses `text=label_series`, so it shows LGA name instead of ward name.

### Recommended Fix

Change line 423 to respect geographic level:

```python
# BEFORE (buggy):
label_series = df.get('LGAName', df.get('WardName', ...))

# AFTER (fixed):
if plot_level == 'lga':
    label_series = df.get('LGAName', df.get('LGACode', df.index))
else:
    label_series = df.get('WardName', df.get('ward_name', df.get('LGACode', df.index)))
```

---

## Issue 4 (Suggestion): Color Scale on Risk Plot

### Feedback from Grace Legris
- Flip scale so high risk is at top
- Use sequential (light red → dark red) instead of diverging color scale

### Current Implementation

The color scale is set in various visualization tools. For risk maps, check:
- `app/tools/visualization_maps_tools.py`
- `app/tools/variable_distribution.py` (lines 356-364)

### Recommendation

For single-variable risk maps, use sequential scale:
```python
colorscale = [
    [0, '#fee5d9'],    # Light red (low risk)
    [0.5, '#fb6a4a'],  # Medium red
    [1, '#a50f15']     # Dark red (high risk)
]
```

---

## Priority Order for Fixes

| Priority | Issue | Effort | Impact |
|----------|-------|--------|--------|
| 1 | Ward hover bug | Low | High - affects all visualizations |
| 2 | ITN 100% allocation | Medium | High - breaks ITN planning |
| 3 | Missing LGA (FCT) | Medium | Medium - affects one state |
| 4 | Color scale | Low | Low - UX improvement |

---

## Files to Modify

1. **`app/tools/variable_distribution.py`** - Fix hover label priority (line 423)
2. **`app/analysis/itn_pipeline.py`** - Add allocation caps and better logging
3. **`app/utils/lga_boundaries.py`** - Add fuzzy matching or alias support
4. **`app/tools/visualization_maps_tools.py`** - Update color scales (optional)
