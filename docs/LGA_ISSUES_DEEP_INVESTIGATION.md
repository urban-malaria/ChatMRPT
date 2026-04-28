# LGA Implementation Issues - Deep Investigation Report

**Date**: January 14, 2026
**Investigator**: Claude (via AWS SSH)
**Server**: Production Instance 1 (3.21.167.170)

---

## Executive Summary

After thorough investigation of the AWS production environment, I've identified **root causes** for all three reported issues. These are not surface-level bugs but **architectural design decisions** that don't handle edge cases.

| Issue | Root Cause | Severity |
|-------|-----------|----------|
| ITN 100% to one ward | Greedy sequential allocation algorithm | **Critical** |
| Missing FCT LGA | Exact string matching without fuzzy fallback | **High** |
| Ward hover shows LGA names | Wrong column priority in label selection | **Medium** |

---

## Issue 1: ITN Distribution Giving 100% to One Ward (Kwara)

### Evidence Found

**Session**: `5918e5d6-2ac3-4c18-be5e-0ee6b43a46b0` (Kwara, Jan 13, 2026)

```json
{
  "total_nets": 10000,
  "prioritized_wards": 1,
  "prioritized": [{
    "WardName": "Gwanabe 1",
    "overall_rank": 1,
    "Population": 80670,
    "nets_needed": 44817,
    "nets_allocated": 10000,
    "coverage_percent": 22.3,
    "allocation_phase": "Rural Priority (Partial)"
  }]
}
```

**Only ONE ward received any nets.** All 10,000 nets went to "Gwanabe 1".

### Root Cause

**File**: `app/analysis/itn_pipeline.py` (lines 648-675)

The algorithm is **greedy sequential**:

```python
for idx, row in rural_wards.iterrows():
    nets_for_this_ward = row['nets_needed']

    if allocated + nets_for_this_ward <= total_nets:
        # Full coverage - give this ward everything it needs
        allocated += nets_for_this_ward
    else:
        # Partial coverage - give remaining nets to this ward
        remaining = total_nets - allocated
        row['nets_allocated'] = remaining  # ALL remaining nets
        break  # EXIT LOOP - no other wards get anything
```

**Problem**: When the first high-priority ward needs more than available nets, it:
1. Gets ALL remaining nets as partial allocation
2. Exits the loop immediately
3. No other wards are considered

**Kwara Population Analysis**:
```
Total wards: 193
Population range: 1,340 to 236,656
Top wards by population:
  - Akanbi 5: 236,656 (would need 131,475 nets)
  - Wara/Osin/Egbejila: 224,274
  - Gwanabe 1: 80,670 (needs 44,817 nets)
```

Even the ward that got all nets (Gwanabe 1, ranked #1 for vulnerability) needs 4.5x more nets than available.

### Why Distribution "Changed Later" (Grace's Observation)

Possible causes:
1. **Different session with different parameters** (more nets available)
2. **Different vulnerability rankings** (different ward ranked #1)
3. **Multi-worker state inconsistency** (different workers, different cached state)

### Recommended Fix

Change from greedy to **proportional allocation**:

```python
# Calculate proportional share based on need and priority
total_need = rankings['nets_needed'].sum()
rankings['share_of_need'] = rankings['nets_needed'] / total_need
rankings['proportional_allocation'] = (rankings['share_of_need'] * total_nets).astype(int)

# Or: Cap single-ward allocation at X% of total
MAX_SINGLE_WARD_PERCENT = 0.20
max_per_ward = total_nets * MAX_SINGLE_WARD_PERCENT
rankings['nets_allocated'] = rankings['nets_needed'].clip(upper=max_per_ward)
```

---

## Issue 2: Missing "Municipal Area Council" LGA in FCT

### Evidence Found

**Data comparison from AWS**:

| Source | LGA Names for FCT |
|--------|-------------------|
| Ward Shapefile (`www/complete_names_wards/wards.shp`) | Abaji, Bwari, Gwagwalada, Kuje, Kwali, **"Municipal Area Council"** |
| Reference Boundaries (`app/reference_data/nga_lga_boundaries.gpkg`) | Abaji, **"Abuja Municipal"**, Bwari, Gwagwalada, Kuje, Kwali |

**The SAME LGA has DIFFERENT names in different files.**

### Root Cause

**File**: `app/utils/lga_boundaries.py` (lines 110-124)

```python
# Normalization
normalized["lga_name_norm"] = normalized["LGAName"].astype(str).str.strip().str.lower()

# EXACT MATCH ONLY
merged = normalized.merge(
    reference[...],
    on=["state_name_norm", "lga_name_norm"],  # <-- Exact match
    how="left",
)
```

- User data: `"municipal area council"` (normalized)
- Reference: `"abuja municipal"` (normalized)
- **These don't match** → LGA geometry not found → LGA missing from visualization

### Recommended Fix

Add fuzzy matching or alias mapping:

```python
LGA_ALIASES = {
    "municipal area council": "abuja municipal",
    "amac": "abuja municipal",
    "municipal": "abuja municipal",
}

def normalize_lga_name(name):
    normalized = str(name).strip().lower()
    return LGA_ALIASES.get(normalized, normalized)
```

---

## Issue 3: Ward Hover Shows LGA Names Instead of Ward Names

### Evidence Found

**File**: `app/tools/variable_distribution.py` (line 423)

```python
label_series = df.get('LGAName', df.get('WardName', df.get('ward_name', df.get('LGACode', df.index))))
```

**The priority order is WRONG:**
1. First checks `LGAName` ← Problem!
2. Then checks `WardName`
3. Then checks `ward_name`
4. Then checks `LGACode`

### Root Cause

Earlier in the same function (line 327):
```python
clean_data = annotate_with_lga_names(clean_data)
```

This adds `LGAName` and `StateName` columns to the data. After this call, `LGAName` **always exists**, so it's always used for hover labels - even at ward level.

### Why This Affects FCT and Edo

All states are affected, but it's more noticeable in:
- **FCT**: Users know their ward names (e.g., "Gwarinpa") but see "Abuja Municipal"
- **Edo**: Same - users expect ward names but see LGA names

### Recommended Fix

Check geographic level before selecting label:

```python
# BEFORE (buggy)
label_series = df.get('LGAName', df.get('WardName', ...))

# AFTER (fixed)
if plot_level == 'lga':
    label_series = df.get('LGAName', df.get('LGACode', df.index))
else:  # ward level
    label_series = df.get('WardName', df.get('ward_name', df.get('LGACode', df.index)))
```

---

## Architectural Observations

### 1. No Fuzzy Matching for LGA Names

The system relies on **exact string matching** between user data and reference data. This is fragile because:
- Different data sources use different naming conventions
- Typos or abbreviations break matching
- No fallback when exact match fails

**Recommendation**: Implement fuzzy matching with configurable threshold (e.g., 85% similarity).

### 2. Sequential Greedy Allocation

The ITN allocation treats nets as indivisible blocks per ward. This works when:
- Many wards with small needs
- Total nets >> individual ward needs

It fails catastrophically when:
- Few wards with large populations
- Total nets < single ward's need

**Recommendation**: Switch to proportional allocation or implement per-ward caps.

### 3. Column Priority Hardcoded

The hover label logic has hardcoded column priorities that don't respect the visualization's geographic level.

**Recommendation**: Make column selection context-aware based on `geographic_level` parameter.

---

## Files to Modify

| File | Line(s) | Issue | Priority |
|------|---------|-------|----------|
| `app/tools/variable_distribution.py` | 423 | Hover label priority | High |
| `app/analysis/itn_pipeline.py` | 648-675 | Greedy allocation | Critical |
| `app/utils/lga_boundaries.py` | 110-124 | Exact matching only | High |

---

## Verification Queries (for future debugging)

### Check FCT LGA Names Mismatch
```python
# On AWS
import geopandas as gpd
ref = gpd.read_file('app/reference_data/nga_lga_boundaries.gpkg')
shp = gpd.read_file('www/complete_names_wards/wards.shp')
print("Reference:", sorted(ref[ref['state_name']=='Federal Capital Territory']['lga_name'].unique()))
print("Shapefile:", sorted(shp[shp['StateCode']=='FC']['LGAName'].unique()))
```

### Check ITN Allocation for Single-Ward Issue
```python
import json
with open('instance/uploads/{session_id}/itn_distribution_results.json') as f:
    data = json.load(f)
print(f"Wards allocated: {data['stats']['prioritized_wards']}")
print(f"Top allocation: {data['prioritized'][0]['nets_allocated']} nets")
```

---

## Summary

These issues stem from **design decisions that don't handle edge cases**:

1. **ITN**: Algorithm assumes total nets > any single ward's need
2. **LGA names**: System assumes consistent naming across data sources
3. **Hover text**: Logic assumes one column priority fits all contexts

All three are fixable with targeted changes to the specific files identified.
