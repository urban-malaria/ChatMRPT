"""
Malaria Burden Utility Functions

This module provides core functionality for detecting malaria data and calculating
burden per 1,000 population using ward-level case data and population rasters.
"""

import pandas as pd
import geopandas as gpd
import re
import os
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


def load_state_shapefile(state_name: str):
    """Load Nigeria shapefile and filter to a specific state.

    Args:
        state_name: Name of the state to filter to

    Returns:
        GeoDataFrame filtered to the state, or None if not found
    """
    try:
        from app.config.data_paths import NIGERIA_SHAPEFILE
    except ImportError:
        NIGERIA_SHAPEFILE = None

    # Try configured path first, then fallbacks
    candidates = []
    if NIGERIA_SHAPEFILE:
        candidates.append(NIGERIA_SHAPEFILE)
    candidates.append(os.path.join('www', 'complete_names_wards', 'wards.shp'))

    gdf = None
    for path in candidates:
        if os.path.exists(path):
            try:
                gdf = gpd.read_file(path)
                logger.info(f"Loaded shapefile from {path} with {len(gdf)} wards")
                break
            except Exception as e:
                logger.error(f"Error loading shapefile from {path}: {e}")

    if gdf is None:
        logger.error("Could not load Nigeria shapefile")
        return None

    # Filter to state - try StateName column first, then check other columns
    state_name_lower = state_name.lower()
    state_gdf = None

    for col in ['StateName', 'State', 'state', 'ADM1_EN']:
        if col in gdf.columns:
            state_gdf = gdf[gdf[col].str.lower() == state_name_lower].copy()
            if not state_gdf.empty:
                logger.info(f"Filtered to {len(state_gdf)} wards for {state_name}")
                return state_gdf

    logger.warning(f"Could not filter shapefile to state {state_name}")
    return None


def extract_ward_population(ward_gdf, age_group: str = 'all_ages'):
    """Extract population from rasters for each ward using zonal statistics.

    Args:
        ward_gdf: GeoDataFrame with ward geometries
        age_group: Age group for population ('all_ages', 'u5', 'o5', 'pw')

    Returns:
        pandas Series with population values, or None if extraction fails
    """
    try:
        from rasterstats import zonal_stats
        from app.config.data_paths import POP_TOTAL_RASTER, POP_U5_RASTER, POP_F15_49_RASTER
    except ImportError as e:
        logger.error(f"Missing required imports for population extraction: {e}")
        return None

    # Select appropriate raster based on age group
    if age_group == 'u5':
        raster = POP_U5_RASTER
    elif age_group == 'pw':
        raster = POP_F15_49_RASTER
    elif age_group == 'o5':
        # O5 = Total - U5
        if not os.path.exists(POP_TOTAL_RASTER) or not os.path.exists(POP_U5_RASTER):
            logger.warning("Population rasters not found for O5 calculation")
            return None
        total_stats = zonal_stats(ward_gdf, POP_TOTAL_RASTER, stats=['sum'])
        u5_stats = zonal_stats(ward_gdf, POP_U5_RASTER, stats=['sum'])
        return pd.Series([(t['sum'] or 0) - (u['sum'] or 0) for t, u in zip(total_stats, u5_stats)])
    else:  # all_ages
        raster = POP_TOTAL_RASTER

    if not os.path.exists(raster):
        logger.warning(f"Population raster not found: {raster}")
        return None

    stats = zonal_stats(ward_gdf, raster, stats=['sum'])
    return pd.Series([s['sum'] or 0 for s in stats])




def normalize_ward_name(name: str) -> str:
    """
    Normalize ward names for matching between TPR data and shapefiles.

    Handles DHIS2 naming quirks: state prefixes, 'Ward' anywhere in the
    name, inconsistent separators (hyphens/slashes/spaces), and trailing
    numbering variations.

    Args:
        name: Ward name to normalize

    Returns:
        Normalized ward name (lowercase, unified separators)
    """
    if pd.isna(name):
        return ''

    name = str(name).strip()

    # Remove state prefixes (ad, kw, os, etc.) - two letter codes
    name = re.sub(r'^[a-z]{2}\s+', '', name, flags=re.IGNORECASE)

    # Remove 'Ward' ANYWHERE in the string (not just suffix)
    # e.g., "balogun fulani ward 3" → "balogun fulani 3"
    name = re.sub(r'\bward\b', '', name, flags=re.IGNORECASE)

    # Unify separators: replace hyphens and slashes with spaces
    # e.g., "budo-egba" and "budo/egba" and "budo egba" all become "budo egba"
    name = name.replace('-', ' ').replace('/', ' ')

    # Collapse multiple spaces and strip
    name = ' '.join(name.split())

    return name.strip().lower()




def fix_column_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix common encoding issues in TPR column names.
    
    Args:
        df: DataFrame with potential encoding issues
        
    Returns:
        DataFrame with fixed column names
    """
    column_fixes = {
        'â‰¥5 years': '≥5 years',
        'â‰¤5 years': '≤5 years',
        'â‰¥5yrs': '≥5yrs',
        'â‰¤5yrs': '≤5yrs',
        'â‰¥': '≥',
        'â‰¤': '≤',
        # Additional common encoding issues
        ' â‰¥': ' ≥',
        ' â‰¤': ' ≤',
        'Ã¢â€°Â¥': '≥',  # Another variant
        'Ã¢â€°Â¤': '≤',  # Another variant
    }
    
    new_columns = []
    for col in df.columns:
        new_col = str(col)
        for bad, good in column_fixes.items():
            new_col = new_col.replace(bad, good)
        new_columns.append(new_col)
    
    df.columns = new_columns
    return df


def calculate_ward_tpr(df: pd.DataFrame, age_group: str = 'all_ages',
                      test_method: str = 'both', facility_level: str = 'all',
                      schema: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Calculate Malaria Burden per 1,000 population.

    Aggregates positive cases by ward, matches with shapefile to get geometries,
    extracts population from rasters, and calculates burden = (positives / population) * 1000.

    Args:
        df: DataFrame with malaria case data
        age_group: 'all_ages', 'u5', 'o5', or 'pw'
        test_method: 'rdt', 'microscopy', or 'both'
        facility_level: 'all', 'primary', 'secondary', or 'tertiary'
        schema: Column schema dict from TPRDataAnalyzer.infer_schema_from_file().
                Required for correct column detection.

    Returns:
        DataFrame with columns: WardName, LGA, Burden, Total_Positive, Population
    """
    if not schema:
        logger.error("No schema provided to calculate_ward_tpr — call infer_schema_from_file first")
        return pd.DataFrame(columns=['WardName', 'LGA', 'Burden', 'Total_Positive', 'Population'])

    df = fix_column_encoding(df)

    # --- Facility level filter ---
    if facility_level != 'all':
        fl_col = schema.get('facility_level')
        if fl_col and fl_col in df.columns:
            orig = len(df)
            filtered = df[df[fl_col].str.lower() == facility_level.lower()].copy()
            if filtered.empty:
                filtered = df[df[fl_col].str.lower().str.contains(facility_level.lower(), na=False)].copy()
            if not filtered.empty:
                df = filtered
                logger.info(f"Filtered to {facility_level}: {len(df)}/{orig} records")
            else:
                logger.warning(f"No facilities found for level '{facility_level}', using all")
        else:
            logger.warning(f"No facility_level column in schema, using all facilities")

    # --- Ward column ---
    ward_col = schema.get('ward')
    df = df.copy()
    if ward_col and ward_col in df.columns:
        df['WardName'] = df[ward_col]
        df['WardName_clean'] = df[ward_col].apply(normalize_ward_name)
    else:
        logger.warning("No ward column in schema — WardName will be 'Unknown'")
        df['WardName'] = 'Unknown'
        df['WardName_clean'] = 'unknown'

    # --- LGA column ---
    lga_col = schema.get('lga')
    if not (lga_col and lga_col in df.columns):
        logger.warning("No lga column in schema — LGA will be 'Unknown'")
        df['LGA'] = 'Unknown'
        lga_col = 'LGA'

    # --- Build test column lists from schema ---
    def _schema_cols(col_type: str) -> List[str]:
        """Return schema-mapped columns for the requested age group(s) and col_type."""
        prefixes = ['u5', 'o5', 'pw'] if age_group == 'all_ages' else [age_group]
        return [c for c in (schema.get(f'{p}_{col_type}') for p in prefixes)
                if c and c in df.columns]

    rdt_tested_cols   = _schema_cols('rdt_tested')   if test_method in ('rdt', 'both') else []
    rdt_positive_cols = _schema_cols('rdt_positive')  if test_method in ('rdt', 'both') else []
    micro_tested_cols = _schema_cols('microscopy_tested')  if test_method in ('microscopy', 'both') else []
    micro_positive_cols = _schema_cols('microscopy_positive') if test_method in ('microscopy', 'both') else []

    logger.info(
        "Schema column detection (age=%s, method=%s): rdt_tested=%s, rdt_pos=%s, "
        "micro_tested=%s, micro_pos=%s",
        age_group, test_method,
        rdt_tested_cols, rdt_positive_cols, micro_tested_cols, micro_positive_cols,
    )

    has_rdt_data = bool(rdt_tested_cols and rdt_positive_cols)
    has_micro_data = bool(micro_tested_cols and micro_positive_cols)

    if not has_rdt_data and not has_micro_data:
        logger.warning("No complete test data for age_group='%s', test_method='%s'", age_group, test_method)
        return pd.DataFrame({
            'WardName': ['No data'], 'LGA': ['No data'],
            'Burden': [0], 'Population': [0], 'Total_Positive': [0],
        })

    # --- Aggregate by ward + LGA ---
    def _sum_cols(group: pd.DataFrame, cols: List[str]) -> float:
        return sum(group[c].fillna(0).sum() for c in cols)

    results = []
    for (ward, lga), group in df.groupby(['WardName_clean', lga_col], dropna=False):
        rdt_tested   = _sum_cols(group, rdt_tested_cols)
        rdt_positive = _sum_cols(group, rdt_positive_cols)
        micro_tested   = _sum_cols(group, micro_tested_cols)
        micro_positive = _sum_cols(group, micro_positive_cols)

        if test_method == 'rdt':
            total_positive = rdt_positive
        elif test_method == 'microscopy':
            total_positive = micro_positive
        else:  # 'both' — pick method with higher positivity rate
            rdt_rate   = rdt_positive   / rdt_tested   if rdt_tested   > 0 else 0
            micro_rate = micro_positive / micro_tested if micro_tested > 0 else 0
            total_positive = rdt_positive if rdt_rate >= micro_rate else micro_positive

        results.append({'WardName': ward, 'LGA': lga, 'Total_Positive': int(total_positive)})

    result_df = pd.DataFrame(results)
    if result_df.empty:
        logger.warning("No results to calculate burden")
        return pd.DataFrame(columns=['WardName', 'LGA', 'Burden', 'Total_Positive', 'Population'])

    # --- State extraction from schema for shapefile lookup ---
    state_name = 'Unknown'
    state_col = schema.get('state')
    if state_col and state_col in df.columns:
        vals = df[state_col].dropna()
        if not vals.empty:
            raw_state = str(vals.iloc[0])
            raw_state = re.sub(r'^[a-z]{2}\s+', '', raw_state, flags=re.IGNORECASE)
            raw_state = re.sub(r'\s+State$', '', raw_state, flags=re.IGNORECASE)
            state_name = ' '.join(w.capitalize() for w in raw_state.replace('-', ' ').split())

    if state_name == 'Unknown':
        logger.warning("Could not determine state from schema, burden calculation may fail")

    # Load shapefile and match wards to get geometries for population extraction
    state_gdf = load_state_shapefile(state_name)

    if state_gdf is not None and not state_gdf.empty:
        # Normalize ward names for matching
        result_df['WardName_norm'] = result_df['WardName'].apply(normalize_ward_name)

        # Determine shapefile ward column
        ward_col = None
        for col in ['WardName', 'Ward', 'ADM3_EN']:
            if col in state_gdf.columns:
                ward_col = col
                break

        if ward_col:
            state_gdf['WardName_norm'] = state_gdf[ward_col].apply(normalize_ward_name)

            # Fuzzy fallback: for data wards that don't exact-match after
            # normalization, find the closest shapefile ward name.  This
            # handles DHIS2 encoding artifacts (digits replacing letters),
            # spelling variations, and minor typos.
            shp_norms = set(state_gdf['WardName_norm'].unique())
            shp_norms_list = list(shp_norms)
            from difflib import get_close_matches
            remap = {}
            for data_norm in result_df['WardName_norm'].unique():
                if data_norm and data_norm not in shp_norms:
                    # First try: direct fuzzy match
                    matches = get_close_matches(data_norm, shp_norms_list, n=1, cutoff=0.7)
                    if not matches:
                        # Second try: DHIS2 often replaces letters with '0'
                        # e.g. "ade0" should be "adena", "ajan0ku" → "ajanaku"
                        digit_cleaned = re.sub(r'0', 'a', data_norm)
                        matches = get_close_matches(digit_cleaned, shp_norms_list, n=1, cutoff=0.65)
                    if not matches:
                        # Third try: data ward is a substring of shapefile ward
                        # e.g. "ajanaku" matches "ajanaku malete"
                        candidates = [s for s in shp_norms_list
                                      if data_norm in s or s in data_norm]
                        if len(candidates) == 1:
                            matches = candidates
                    if matches:
                        remap[data_norm] = matches[0]
            if remap:
                logger.info(f"Fuzzy-matched {len(remap)} ward names: {dict(list(remap.items())[:5])}")
                result_df['WardName_norm'] = result_df['WardName_norm'].replace(remap)

            # Merge to get geometries
            merged = state_gdf.merge(result_df, on='WardName_norm', how='left', suffixes=('_shp', ''))

            # Filter out null geometries before population extraction
            # (shapefile contains wards with missing geometry data that crash zonal_stats)
            null_geom_count = merged.geometry.isna().sum()
            if null_geom_count > 0:
                logger.warning(f"Filtering out {null_geom_count} wards with null geometries")
                merged = merged[merged.geometry.notna()].copy()

            # Extract population based on age group
            pop_series = extract_ward_population(merged, age_group)

            if pop_series is not None:
                merged['Population'] = pop_series.values
                # Calculate Burden = (Total_Positive / Population) * 1000, capped at 1000
                # Cap at 1000 because burden > 1000 per 1,000 population is epidemiologically impossible
                # (would mean >100% of population tested positive, likely due to repeat testing)
                merged['Burden'] = merged.apply(
                    lambda r: min(round((r['Total_Positive'] / r['Population']) * 1000, 2), 1000.0)
                    if pd.notna(r.get('Total_Positive')) and r['Population'] > 0 else 0,
                    axis=1
                )

                # Extract result columns
                result_df = merged[['WardName', 'LGA', 'Burden', 'Total_Positive', 'Population']].copy()
                result_df = result_df.dropna(subset=['WardName'])
                result_df = result_df.sort_values('Burden', ascending=False)

                logger.info(f"Calculated burden for {len(result_df)} wards using {age_group} age group")
                return result_df
            else:
                logger.warning("Population extraction failed, returning results without burden")
        else:
            logger.warning("Could not find ward column in shapefile")
    else:
        logger.warning(f"Could not load shapefile for {state_name}")

    # Fallback: return with zero burden if population extraction failed
    result_df['Burden'] = 0
    result_df['Population'] = 0
    logger.info(f"Returning {len(result_df)} wards without burden calculation (population data unavailable)")

    return result_df




def get_geopolitical_zone(state: str) -> str:
    """
    Get the geopolitical zone for a Nigerian state.
    
    Args:
        state: State name
        
    Returns:
        Geopolitical zone name
    """
    zones = {
        'North-East': ['Adamawa', 'Bauchi', 'Borno', 'Gombe', 'Taraba', 'Yobe'],
        'North-West': ['Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Sokoto', 'Zamfara'],
        'North-Central': ['Benue', 'Kogi', 'Kwara', 'Nasarawa', 'Niger', 'Plateau', 'FCT'],
        'South-East': ['Abia', 'Anambra', 'Ebonyi', 'Enugu', 'Imo'],
        'South-South': ['Akwa Ibom', 'Bayelsa', 'Cross River', 'Delta', 'Edo', 'Rivers'],
        'South-West': ['Ekiti', 'Lagos', 'Ogun', 'Ondo', 'Osun', 'Oyo']
    }
    
    for zone, states in zones.items():
        if state in states:
            return zone
    
    return 'Unknown'


def prepare_tpr_summary(tpr_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Prepare a summary of burden results for reporting.

    Args:
        tpr_df: DataFrame with burden results (Burden, Total_Positive, Population)

    Returns:
        Dictionary with summary statistics
    """
    if tpr_df.empty:
        return {
            'total_wards': 0,
            'mean_burden': 0,
            'median_burden': 0,
            'max_burden': 0,
            'min_burden': 0,
            'high_risk_wards': [],
            'total_positive': 0,
            'total_population': 0
        }

    summary = {
        'total_wards': len(tpr_df),
        'mean_burden': round(tpr_df['Burden'].mean(), 2),
        'median_burden': round(tpr_df['Burden'].median(), 2),
        'max_burden': round(tpr_df['Burden'].max(), 2),
        'min_burden': round(tpr_df['Burden'].min(), 2),
        'std_burden': round(tpr_df['Burden'].std(), 2),
        'total_positive': int(tpr_df['Total_Positive'].sum()),
        'total_population': int(tpr_df['Population'].sum()) if 'Population' in tpr_df.columns else 0,
        'overall_burden': min(round(tpr_df['Total_Positive'].sum() / tpr_df['Population'].sum() * 1000, 2), 1000.0)
                          if 'Population' in tpr_df.columns and tpr_df['Population'].sum() > 0 else 0
    }

    # Identify high-burden wards dynamically based on data distribution
    threshold = summary['median_burden']

    if summary['mean_burden'] > summary['median_burden'] * 1.5:
        threshold = (summary['mean_burden'] + summary['median_burden']) / 2

    if summary['median_burden'] < 5 and summary['mean_burden'] > 10:
        threshold = summary['mean_burden']

    high_risk = tpr_df[tpr_df['Burden'] > threshold].copy()
    if not high_risk.empty:
        high_risk = high_risk.nlargest(10, 'Burden')
        summary['high_risk_wards'] = high_risk[['WardName', 'LGA', 'Burden']].to_dict('records')
        summary['risk_threshold'] = round(threshold, 1)
    else:
        summary['high_risk_wards'] = []
        summary['risk_threshold'] = round(threshold, 1)

    # Add LGA summary
    lga_summary = tpr_df.groupby('LGA').agg({
        'Burden': 'mean',
        'Total_Positive': 'sum',
        'Population': 'sum'
    }).round(2)

    summary['lga_summary'] = lga_summary.to_dict('index')

    return summary
