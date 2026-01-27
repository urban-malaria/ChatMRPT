"""
Malaria Burden Utility Functions

This module provides core functionality for detecting malaria data and calculating
burden per 1,000 population using ward-level case data and population rasters.
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import re
import os
from typing import Tuple, Dict, List, Optional, Any
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


def is_tpr_data(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """
    Detect if the DataFrame contains TPR (Test Positivity Rate) data.
    
    Args:
        df: DataFrame to check
        
    Returns:
        Tuple of (is_tpr, info_dict) where:
        - is_tpr: Boolean indicating if this is TPR data
        - info_dict: Dictionary with confidence score and matched columns
    """
    if df is None or df.empty:
        return False, {'confidence': 0.0, 'matched_columns': [], 'total_columns': 0}
    
    # TPR indicator columns to check for
    tpr_indicators = [
        'RDT', 'Microscopy', 'tested', 'positive', 'fever', 
        'LLIN', 'net', 'malaria', 'facility', 'health'
    ]
    
    # Check column names for TPR indicators
    matched_columns = []
    column_names_lower = [col.lower() for col in df.columns]
    
    for indicator in tpr_indicators:
        indicator_lower = indicator.lower()
        for col in column_names_lower:
            if indicator_lower in col:
                matched_columns.append(indicator)
                break
    
    # Calculate confidence score
    confidence = len(matched_columns) / len(tpr_indicators)
    
    # Check for specific TPR column patterns
    has_rdt = any('rdt' in col.lower() for col in df.columns)
    has_microscopy = any('microscopy' in col.lower() for col in df.columns)
    has_tested = any('tested' in col.lower() for col in df.columns)
    has_positive = any('positive' in col.lower() for col in df.columns)
    
    # Strong indicator if we have both test methods and results
    is_strong_tpr = (has_rdt or has_microscopy) and (has_tested or has_positive)
    
    # Consider it TPR data if confidence > 0.4 or strong indicators present
    is_tpr = confidence > 0.4 or is_strong_tpr
    
    info = {
        'confidence': confidence,
        'matched_columns': matched_columns,
        'total_columns': len(df.columns),
        'has_rdt': has_rdt,
        'has_microscopy': has_microscopy,
        'has_tested': has_tested,
        'has_positive': has_positive,
        'is_strong_tpr': is_strong_tpr
    }
    
    logger.info(f"TPR detection: is_tpr={is_tpr}, confidence={confidence:.2f}, matched={len(matched_columns)}")
    
    return is_tpr, info


def normalize_ward_name(name: str) -> str:
    """
    Normalize ward names for matching between TPR data and shapefiles.
    
    Removes prefixes like 'ad ', 'kw ', 'os ' and suffixes like ' Ward'.
    
    Args:
        name: Ward name to normalize
        
    Returns:
        Normalized ward name (lowercase)
    """
    if pd.isna(name):
        return ''
    
    name = str(name).strip()
    
    # Remove state prefixes (ad, kw, os, etc.) - two letter codes
    name = re.sub(r'^[a-z]{2}\s+', '', name, flags=re.IGNORECASE)
    
    # Remove 'Ward' suffix
    name = re.sub(r'\s+Ward$', '', name, flags=re.IGNORECASE)
    
    # Remove extra spaces and normalize
    name = ' '.join(name.split())
    
    # Return lowercase for consistent matching
    return name.strip().lower()


def extract_state_from_data(df: pd.DataFrame) -> str:
    """
    Extract state name from TPR data.

    Handles formats like 'ad Adamawa State', 'Adamawa State', 'Adamawa', 'Akwa-Ibom State'.

    Args:
        df: DataFrame with TPR data

    Returns:
        Clean state name with proper formatting (e.g., 'Adamawa', 'Akwa Ibom')
    """
    # Check for State column
    state_columns = ['State', 'state', 'STATE', 'StateName', 'state_name']

    for col in state_columns:
        if col in df.columns:
            # Get first non-null value
            state_values = df[col].dropna()
            if not state_values.empty:
                state = str(state_values.iloc[0])
                # Clean the state name
                state = re.sub(r'^[a-z]{2}\s+', '', state, flags=re.IGNORECASE)
                state = state.replace(' State', '').strip()

                # Normalize state name formatting (handle hyphens)
                state = state.replace('-', ' ')

                # Handle specific cases - HARDCODED FIXES for problematic states
                state_corrections = {
                    'akwa ibom': 'Akwa Ibom',
                    'cross river': 'Cross River',
                    'federal capital territory': 'Federal Capital Territory',
                    'fct': 'Federal Capital Territory',
                    # DIRECT HARDCODED FIXES
                    'ebonyi': 'Ebonyi',
                    'kebbi': 'Kebbi',
                    'plateau': 'Plateau',
                    'nasarawa': 'Nasarawa',
                    'nassarawa': 'Nasarawa',  # Common misspelling
                    'nasar awa': 'Nasarawa',
                    'plat eau': 'Plateau',
                    'keb bi': 'Kebbi',
                    'ebo nyi': 'Ebonyi'
                }

                state_lower = state.lower()
                if state_lower in state_corrections:
                    return state_corrections[state_lower]

                # Ensure proper capitalization
                state = ' '.join(word.capitalize() for word in state.split())
                return state

    # Try to extract from filename or other indicators
    # Check if any column contains state names - expanded list
    known_states = ['Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue',
                    'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu',
                    'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi',
                    'Kogi', 'Kwara', 'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo',
                    'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara']
    for state in known_states:
        for col in df.columns:
            if state.lower() in str(col).lower():
                return state

    return 'Unknown'


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
                      test_method: str = 'both', facility_level: str = 'all') -> pd.DataFrame:
    """
    Calculate Malaria Burden per 1,000 population.

    Aggregates positive cases by ward, matches with shapefile to get geometries,
    extracts population from rasters, and calculates burden = (positives / population) * 1000.

    Args:
        df: DataFrame with malaria case data
        age_group: Age group for calculation and population denominator
                  - 'all_ages': All age groups (total population)
                  - 'u5': Under 5 years (U5 population)
                  - 'o5': Over 5 years (Total - U5 population)
                  - 'pw': Pregnant women (Women 15-49 population)
        test_method: Test method to use ('rdt', 'microscopy', 'both')
        facility_level: Facility level to include ('all', 'primary', 'secondary', 'tertiary')

    Returns:
        DataFrame with columns: WardName, LGA, Burden, Total_Positive, Population
    """
    # Fix encoding issues first
    df = fix_column_encoding(df)
    
    # Log columns for debugging
    logger.debug(f"Available columns after encoding fix: {list(df.columns)[:10]}")
    
    # Also log a sample of test-related columns for debugging
    test_cols = [col for col in df.columns if any(
        keyword in col.lower() for keyword in ['test', 'rdt', 'microscop', 'fever', 'positive']
    )]
    if test_cols:
        logger.info(f"Sample test columns found: {test_cols[:3]}")
    
    # Filter by facility level if specified
    if facility_level != 'all':
        # Look for facility level column
        facility_cols = [col for col in df.columns if 'facility' in col.lower() and ('level' in col.lower() or 'type' in col.lower())]
        if facility_cols:
            facility_col = facility_cols[0]
            # Try to filter to selected level (be flexible with matching)
            original_len = len(df)
            
            # Try exact match first
            filtered_df = df[df[facility_col].str.lower() == facility_level.lower()].copy()
            
            # If no exact matches, try contains
            if filtered_df.empty:
                filtered_df = df[df[facility_col].str.lower().str.contains(facility_level.lower(), na=False)].copy()
            
            # If still no matches, warn and use all data
            if filtered_df.empty:
                logger.warning(f"No facilities found for level '{facility_level}', using all facilities")
                available_levels = df[facility_col].dropna().unique()
                logger.info(f"Available facility levels: {list(available_levels)[:10]}")
            else:
                df = filtered_df
                logger.info(f"Filtered to {facility_level} facilities: {len(df)}/{original_len} records")
        else:
            logger.warning(f"No facility level/type column found, using all facilities")
    
    # Normalize ward names - check for DHIS2 org unit levels first
    if 'orgunitlevel4' in df.columns:
        # DHIS2 format: orgunitlevel4 is ward
        df['WardName'] = df['orgunitlevel4']
        df['WardName_clean'] = df['orgunitlevel4'].apply(normalize_ward_name)
        logger.info("Using orgunitlevel4 as Ward column")
    elif 'WardName' in df.columns:
        df['WardName_clean'] = df['WardName'].apply(normalize_ward_name)
    elif 'Ward' in df.columns:
        df['WardName_clean'] = df['Ward'].apply(normalize_ward_name)
        df['WardName'] = df['Ward']
    else:
        # Try to find ward column
        ward_cols = [col for col in df.columns if 'ward' in col.lower()]
        if ward_cols:
            df['WardName'] = df[ward_cols[0]]
            df['WardName_clean'] = df['WardName'].apply(normalize_ward_name)
        else:
            logger.warning("No ward column found in TPR data")
            # Use facility level if available
            if 'orgunitlevel5' in df.columns:
                df['WardName'] = df['orgunitlevel5']
                df['WardName_clean'] = df['orgunitlevel5'].apply(normalize_ward_name)
                logger.info("Using orgunitlevel5 (facility) as grouping level")
            else:
                df['WardName'] = 'Unknown'
                df['WardName_clean'] = 'unknown'
    
    # Find LGA column - check for DHIS2 org unit levels first
    if 'orgunitlevel3' in df.columns:
        # DHIS2 format: orgunitlevel3 is LGA
        lga_col = 'orgunitlevel3'
        logger.info("Using orgunitlevel3 as LGA column")
    else:
        lga_cols = ['LGA', 'lga', 'LocalGovernment', 'local_government']
        lga_col = None
        for col in lga_cols:
            if col in df.columns:
                lga_col = col
                break
        
        if not lga_col:
            # Try to find LGA column
            lga_cols_found = [col for col in df.columns if 'lga' in col.lower()]
            if lga_cols_found:
                lga_col = lga_cols_found[0]
            else:
                logger.warning("No LGA column found in TPR data")
                df['LGA'] = 'Unknown'
                lga_col = 'LGA'
    
    # Identify test columns based on age group with flexible matching
    # Flexible patterns for different column naming conventions
    if age_group == 'all_ages':
        # For all_ages, we need to include ALL age-specific columns and sum them
        rdt_tested_cols = []
        rdt_positive_cols = []
        micro_tested_cols = []
        micro_positive_cols = []
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Use simpler, non-overlapping patterns like the old implementation
            # For tested: look for "tested by RDT/Microscopy" AND "fever" or "presenting"
            # For positive: look for "positive for malaria"
            
            # More flexible patterns that match actual DHIS2 column names
            # RDT tested columns - look for "presenting with fever" AND "tested by RDT"
            if ('presenting' in col_lower and 'fever' in col_lower and 'tested by rdt' in col_lower):
                rdt_tested_cols.append(col)
                logger.debug(f"Added RDT tested column: {col}")
            # RDT positive columns - look for "tested positive for malaria by RDT"
            elif ('tested positive' in col_lower and 'malaria' in col_lower and 'rdt' in col_lower):
                rdt_positive_cols.append(col)
                logger.debug(f"Added RDT positive column: {col}")
            # Microscopy tested columns - look for "presenting with fever" AND "tested by Microscopy"
            elif ('presenting' in col_lower and 'fever' in col_lower and 'tested by microscop' in col_lower):
                micro_tested_cols.append(col)
                logger.debug(f"Added Microscopy tested column: {col}")
            # Microscopy positive columns - look for "tested positive for malaria by Microscopy"
            elif ('tested positive' in col_lower and 'malaria' in col_lower and 'microscop' in col_lower):
                micro_positive_cols.append(col)
                logger.debug(f"Added Microscopy positive column: {col}")
    else:
        # Filter columns for specific age group
        # More comprehensive age patterns - check for <5yrs, ≥5yrs formats
        # Include both UTF-8 and mangled encodings of ≥ character
        age_suffix = {
            'u5': ['<5yrs', '<5 yrs', '<5', 'under5', 'u5', 'under 5', '5yrs', '5 yrs', '5 years', 'children',
                   '<5yr', 'less than 5', 'below 5'],
            'o5': ['≥5yrs', '≥5 yrs', '≥5', '>=5', '>5', 'over5', 'o5', 'over 5', '5+', 'excl pw', 'excluding pw', 
                   '≥5yr', 'above 5', '5 and above', '5 years and above', '(excl'],  # excl = excluding pregnant women
            'pw': ['preg women', 'women (pw)', 'pw', 'pregnant', 'anc', 'pregnant women', 'preg', '(pw)']
        }
        
        suffixes = age_suffix.get(age_group, [age_group])
        
        rdt_tested_cols = []
        rdt_positive_cols = []
        micro_tested_cols = []
        micro_positive_cols = []
        
        for col in df.columns:
            col_lower = col.lower()
            # Check if column matches age group (more flexible)
            matches_age = any(suffix in col_lower for suffix in suffixes)
            
            if matches_age:
                # RDT columns - check for 'positive' FIRST to avoid misclassification
                if 'rdt' in col_lower:
                    if 'positive' in col_lower and 'malaria' in col_lower:
                        # This is a positive column
                        rdt_positive_cols.append(col)
                        logger.debug(f"Added RDT positive column: {col}")
                    elif ('presenting' in col_lower and 'fever' in col_lower) or 'tested by rdt' in col_lower:
                        # This is a tested column (presenting with fever or tested by RDT)
                        rdt_tested_cols.append(col)
                        logger.debug(f"Added RDT tested column: {col}")
                # Microscopy columns - check for 'positive' FIRST
                elif 'microscop' in col_lower:
                    if 'positive' in col_lower and 'malaria' in col_lower:
                        # This is a positive column
                        micro_positive_cols.append(col)
                        logger.debug(f"Added Microscopy positive column: {col}")
                    elif ('presenting' in col_lower and 'fever' in col_lower) or 'tested by microscop' in col_lower:
                        # This is a tested column
                        micro_tested_cols.append(col)
                        logger.debug(f"Added Microscopy tested column: {col}")
    
    # Log what columns were detected
    logger.info(f"Column detection for age_group={age_group}, test_method={test_method}:")
    logger.info(f"  RDT tested columns: {len(rdt_tested_cols)} found")
    if rdt_tested_cols:
        logger.debug(f"    {rdt_tested_cols[:2]}")  # Show first 2 for debugging
    logger.info(f"  RDT positive columns: {len(rdt_positive_cols)} found")
    logger.info(f"  Microscopy tested columns: {len(micro_tested_cols)} found")
    logger.info(f"  Microscopy positive columns: {len(micro_positive_cols)} found")
    
    # Check if we have minimum required columns
    has_rdt_data = len(rdt_tested_cols) > 0 and len(rdt_positive_cols) > 0
    has_micro_data = len(micro_tested_cols) > 0 and len(micro_positive_cols) > 0
    
    if not has_rdt_data and not has_micro_data:
        # Provide helpful feedback about what columns exist
        test_related_cols = [col for col in df.columns if any(
            keyword in col.lower() for keyword in ['test', 'rdt', 'microscop', 'positive', 'fever']
        )]
        
        logger.warning(f"No complete test data found for age_group='{age_group}'")
        logger.info(f"Available test-related columns: {test_related_cols[:5]}")
        
        # Return empty results with informative message
        return pd.DataFrame({
            'WardName': ['No data'],
            'LGA': ['No data'],
            'Burden': [0],
            'Population': [0],
            'Total_Positive': [0]
        })
    
    if age_group == 'all_ages':
        # All ages: Sum first, then calculate TPR for each method, then max
        results = []
        
        # Group by ward and LGA
        group_cols = ['WardName_clean', lga_col]
        grouped = df.groupby(group_cols, dropna=False)
        
        for (ward, lga), group in grouped:
            # Calculate based on test method selection and available columns
            rdt_tested = 0
            rdt_positive = 0
            micro_tested = 0
            micro_positive = 0
            
            # Calculate RDT if needed and columns exist
            if test_method in ['rdt', 'both'] and rdt_tested_cols:
                for col in rdt_tested_cols:
                    if col in group.columns:
                        rdt_tested += group[col].fillna(0).sum()
                
                for col in rdt_positive_cols:
                    if col in group.columns:
                        rdt_positive += group[col].fillna(0).sum()
            
            # Calculate Microscopy if needed and columns exist
            if test_method in ['microscopy', 'both'] and micro_tested_cols:
                for col in micro_tested_cols:
                    if col in group.columns:
                        micro_tested += group[col].fillna(0).sum()
                
                for col in micro_positive_cols:
                    if col in group.columns:
                        micro_positive += group[col].fillna(0).sum()
            
            # Determine total_positive based on test method
            if test_method == 'rdt':
                total_positive = rdt_positive
            elif test_method == 'microscopy':
                total_positive = micro_positive
            else:  # both - use positives from method with higher rate
                rdt_rate = (rdt_positive / rdt_tested) if rdt_tested > 0 else 0
                micro_rate = (micro_positive / micro_tested) if micro_tested > 0 else 0
                total_positive = rdt_positive if rdt_rate >= micro_rate else micro_positive

            results.append({
                'WardName': ward,
                'LGA': lga,
                'Total_Positive': int(total_positive)
            })
    
    else:  # specific_age method
        # For specific age groups: Max at facility level, then sum
        results = []
        
        # Check if we have the necessary columns for specific age calculation
        if not rdt_tested_cols and not micro_tested_cols:
            logger.warning(f"No test data found for age group '{age_group}', returning empty results")
            return pd.DataFrame(columns=['WardName', 'LGA', 'Total_Positive'])
        
        # Need facility column for this method (but make it optional)
        facility_cols = [col for col in df.columns if 'facility' in col.lower()]
        if not facility_cols:
            logger.warning("No facility column found, calculating at ward level instead")
            # Fall back to ward-level calculation without facility grouping
            group_cols = ['WardName_clean', lga_col]
            grouped = df.groupby(group_cols, dropna=False)
            
            for (ward, lga), group in grouped:
                # Calculate based on available columns
                tested = 0
                positive = 0
                
                if test_method in ['rdt', 'both'] and rdt_tested_cols:
                    rdt_tested = sum(group[col].fillna(0).sum() for col in rdt_tested_cols if col in group.columns)
                    rdt_positive = sum(group[col].fillna(0).sum() for col in rdt_positive_cols if col in group.columns)
                    tested = max(tested, rdt_tested)
                    positive = max(positive, rdt_positive) if rdt_tested > 0 else positive
                
                if test_method in ['microscopy', 'both'] and micro_tested_cols:
                    micro_tested = sum(group[col].fillna(0).sum() for col in micro_tested_cols if col in group.columns)
                    micro_positive = sum(group[col].fillna(0).sum() for col in micro_positive_cols if col in group.columns)
                    tested = max(tested, micro_tested)
                    positive = max(positive, micro_positive) if micro_tested > 0 else positive
                
                results.append({
                    'WardName': ward,
                    'LGA': lga,
                    'Total_Positive': int(positive)
                })
        
        facility_col = facility_cols[0]
        
        # Group by facility first
        facility_groups = df.groupby([facility_col, 'WardName_clean', lga_col], dropna=False)
        
        ward_data = {}
        for (facility, ward, lga), group in facility_groups:
            if ward not in ward_data:
                ward_data[ward] = {
                    'lga': lga,
                    'tested': 0,
                    'positive': 0
                }
            
            # At facility level, take max of RDT and Microscopy
            facility_tested = 0
            facility_positive = 0
            
            # RDT values
            rdt_tested = sum(group[col].fillna(0).sum() for col in rdt_tested_cols if col in group.columns)
            rdt_positive = sum(group[col].fillna(0).sum() for col in rdt_positive_cols if col in group.columns)
            
            # Microscopy values
            micro_tested = sum(group[col].fillna(0).sum() for col in micro_tested_cols if col in group.columns)
            micro_positive = sum(group[col].fillna(0).sum() for col in micro_positive_cols if col in group.columns)
            
            # Take max at facility level
            facility_tested = max(rdt_tested, micro_tested)
            facility_positive = max(rdt_positive, micro_positive)
            
            # Add to ward totals
            ward_data[ward]['tested'] += facility_tested
            ward_data[ward]['positive'] += facility_positive
        
        # Collect results for each ward
        for ward, data in ward_data.items():
            results.append({
                'WardName': ward,
                'LGA': data['lga'],
                'Total_Positive': int(data['positive'])
            })

    # Convert to DataFrame
    result_df = pd.DataFrame(results)

    if result_df.empty:
        logger.warning("No results to calculate burden")
        return pd.DataFrame(columns=['WardName', 'LGA', 'Burden', 'Total_Positive', 'Population'])

    # Extract state from original data for shapefile loading
    state_name = extract_state_from_data(df)
    if state_name == 'Unknown':
        logger.warning("Could not determine state from data, burden calculation may fail")

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


def validate_tpr_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate TPR data for required columns and data quality.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check for empty DataFrame
    if df.empty:
        errors.append("DataFrame is empty")
        return False, errors
    
    # Check for ward identifier (including DHIS2 format)
    ward_cols = ['WardName', 'Ward', 'ward', 'ward_name', 'orgunitlevel4', 'orgunitlevel5']
    has_ward = any(col in df.columns for col in ward_cols)
    if not has_ward:
        ward_like = [col for col in df.columns if 'ward' in col.lower()]
        if not ward_like:
            # Check for org unit levels (DHIS2 format)
            org_unit_cols = [col for col in df.columns if 'orgunitlevel' in col.lower()]
            if not org_unit_cols:
                errors.append("No ward column found (checked for Ward, orgunitlevel4/5)")
    
    # Check for test data columns
    has_rdt = any('rdt' in col.lower() for col in df.columns)
    has_micro = any('microscopy' in col.lower() for col in df.columns)
    
    if not has_rdt and not has_micro:
        errors.append("No RDT or Microscopy columns found")
    
    # Check for tested/positive columns
    has_tested = any('tested' in col.lower() for col in df.columns)
    has_positive = any('positive' in col.lower() for col in df.columns)
    
    if not has_tested:
        errors.append("No 'tested' columns found")
    if not has_positive:
        errors.append("No 'positive' columns found")
    
    # Check data quality
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) == 0:
        errors.append("No numeric columns found")
    else:
        # Check for negative values in test columns
        test_cols = [col for col in numeric_cols if any(
            keyword in col.lower() for keyword in ['tested', 'positive', 'rdt', 'microscopy']
        )]
        
        for col in test_cols:
            if (df[col] < 0).any():
                errors.append(f"Negative values found in {col}")
    
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.info("TPR data validation passed")
    else:
        logger.warning(f"TPR data validation failed: {errors}")
    
    return is_valid, errors


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
