# TPR to Malaria Burden Migration

**Date:** 2026-01-23
**Status:** Complete, Ready for Deployment

## Summary

Replaced Test Positivity Rate (TPR) calculation with Malaria Burden per 1,000 population. This provides a more epidemiologically meaningful metric that uses population as the denominator instead of tests performed.

## Formula Change

```
OLD (TPR):    (positive cases / tests performed) × 100     → Percentage
NEW (Burden): (positive cases / ward population) × 1,000   → Per 1,000 population
```

## Commits (in order)

| Commit | Description |
|--------|-------------|
| c911cc7 | feat: replace TPR calculation with Malaria Burden per 1,000 population |
| 38d871d | fix: update production system (data_analysis_v3) with Malaria Burden terminology |
| 54e2f06 | chore: remove deprecated app/tpr_module/ system (107 files, 15,096 lines deleted) |
| 56236ac | fix: update tpr_analysis_tool.py to use Burden columns instead of TPR |
| 6185dfe | fix: update TPR references to Burden and Positivity for UI consistency |
| 9042526 | fix: cap malaria burden at 1000 per 1,000 population |

## Files Modified

### Core Calculation
- `app/core/tpr_utils.py` - Main burden calculation with population extraction
- `app/config/data_paths.py` - Added population raster paths

### Production System (data_analysis_v3)
- `app/data_analysis_v3/tpr/workflow_manager.py` - UI messages and chart labels
- `app/data_analysis_v3/tools/tpr_analysis_tool.py` - Column references
- `app/data_analysis_v3/core/formatters.py` - Message formatting

### Deleted (deprecated)
- `app/tpr_module/` - Entire directory removed (was not used in production)

## Population Data Sources

| Age Group | Raster File | Location |
|-----------|-------------|----------|
| Total | nigeria.tif | data/geospatial/ |
| Under 5 | NGA_population_v2_0_agesex_under5.tif | data/geospatial/ |
| Women 15-49 | NGA_population_v2_0_agesex_f15_49.tif | data/geospatial/NGA_population_v2_0_agesex/ |

## Key Technical Details

1. **Population Extraction**: Uses `rasterstats.zonal_stats()` to sum population within ward boundaries
2. **Age-Specific Denominators**:
   - u5 → Under-5 population raster
   - o5 → Total - Under-5 population
   - pw → Women 15-49 population raster
   - all_ages → Total population raster
3. **Burden Cap**: Values capped at 1,000 (100% burden) since >1,000 is epidemiologically impossible

## Test Results (Niger State, Primary Facilities, U5)

- Ward matching: 95.8% (277/289 wards)
- Environmental variables: 6 extracted
- Average burden: ~180 per 1,000 (after cap applied)
- Output files: raw_data.csv, raw_shapefile.zip, burden distribution map

## Deployment Requirements

### AWS Instances Need:
1. **rasterstats package** - Run on both instances:
   ```bash
   source /home/ec2-user/ChatMRPT/venv/bin/activate
   pip install rasterstats>=0.19.0
   ```

2. **Population rasters** - Verify files exist in `data/geospatial/`:
   - nigeria.tif
   - NGA_population_v2_0_agesex_under5.tif
   - NGA_population_v2_0_agesex/NGA_population_v2_0_agesex_f15_49.tif

### Deployment Commands:
```bash
# Deploy to both instances
for ip in 3.21.167.170 18.220.103.20; do
    ssh -i /tmp/chatmrpt-key2.pem ec2-user@$ip '
        cd /home/ec2-user/ChatMRPT &&
        git pull origin bernard &&
        source venv/bin/activate &&
        pip install rasterstats>=0.19.0 &&
        sudo systemctl restart chatmrpt
    '
done
```

## What Users Will See

### During Workflow:
- "Malaria Burden Analysis Workflow" (not TPR)
- "Calculate Malaria Burden for each ward: (Positive cases ÷ Ward Population) × 1,000"
- Charts show "Positivity (%)" during selection (preview data)

### After Calculation:
- "Malaria Burden Analysis Complete"
- "X cases per 1,000 population across Y wards"
- Burden distribution map

## Rollback Plan

If issues occur:
```bash
git revert HEAD~6..HEAD  # Revert all 6 commits
# OR
git checkout 92ceba7  # Go back to pre-migration state
```
