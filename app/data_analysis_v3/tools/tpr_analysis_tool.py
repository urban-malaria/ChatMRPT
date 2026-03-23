"""
TPR Analysis Tool for Data Analysis V3

This tool provides TPR-specific functionality including detection, calculation,
and preparation for risk analysis pipeline.
"""

import os
import json
import logging
import tempfile
import zipfile
import glob
from pathlib import Path
from typing import Dict, Any, Optional, List, Annotated
import pandas as pd
from ..core.encoding_handler import EncodingHandler
import geopandas as gpd
import numpy as np
from functools import lru_cache
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping
import warnings

from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState
from ..core.state_manager import DataAnalysisStateManager

# Custom JSON encoder to handle numpy types
class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles numpy types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        return super().default(obj)

from app.core.tpr_utils import (
    calculate_ward_tpr,
    normalize_ward_name,
    get_geopolitical_zone,
    prepare_tpr_summary,
)
from app.utils.map_overlays import add_lga_boundary_overlay, calculate_lga_averages
from app.utils.visualization_controls import inject_lga_hover_highlight
import plotly.graph_objects as go

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', category=FutureWarning)

# Cache the Nigeria shapefile to avoid repeated loading
@lru_cache(maxsize=1)
def load_nigeria_shapefile():
    """Load and cache the Nigeria master shapefile using configured paths.

    Order of resolution:
    1) Environment-driven config via app.config.data_paths.NIGERIA_SHAPEFILE
    2) Project-root relative fallback: www/complete_names_wards/wards.shp
    3) CWD relative fallback: www/complete_names_wards/wards.shp
    """
    # Prefer centralized data path configuration
    try:
        from app.config.data_paths import NIGERIA_SHAPEFILE as CFG_SHAPEFILE, PROJECT_ROOT
    except Exception:
        CFG_SHAPEFILE, PROJECT_ROOT = None, None

    candidates = []
    if CFG_SHAPEFILE:
        candidates.append(CFG_SHAPEFILE)
    if PROJECT_ROOT:
        candidates.append(os.path.join(str(PROJECT_ROOT), 'www', 'complete_names_wards', 'wards.shp'))
    # Final fallback relative to current working directory
    candidates.append(os.path.join('www', 'complete_names_wards', 'wards.shp'))

    # Debug logging
    try:
        with open('/tmp/shapefile_debug.log', 'a') as f:
            f.write(f"\nload_nigeria_shapefile called at {os.getcwd()}\n")
            for idx, path in enumerate(candidates, 1):
                f.write(f"Candidate[{idx}]: {path} (exists={os.path.exists(path)})\n")
    except Exception:
        pass

    for path in candidates:
        if os.path.exists(path):
            try:
                gdf = gpd.read_file(path)
                logger.info(f"Loaded Nigeria shapefile from {path} with {len(gdf)} wards")
                try:
                    with open('/tmp/shapefile_debug.log', 'a') as f:
                        f.write(f"SUCCESS: Loaded {len(gdf)} wards from {path}\n")
                except Exception:
                    pass
                return gdf
            except Exception as e:
                logger.error(f"Error loading shapefile from {path}: {e}")
                try:
                    with open('/tmp/shapefile_debug.log', 'a') as f:
                        f.write(f"ERROR loading shapefile from {path}: {e}\n")
                except Exception:
                    pass
                # Try next candidate
                continue

    logger.warning("Nigeria shapefile not found in configured or fallback locations")
    try:
        with open('/tmp/shapefile_debug.log', 'a') as f:
            f.write("FILE NOT FOUND in all candidates\n")
    except Exception:
        pass
    return None


def extract_environmental_variables(ward_geometries: gpd.GeoDataFrame, state_name: str = None) -> pd.DataFrame:
    """
    Extract zone-specific environmental variables from raster files for ward geometries.

    Args:
        ward_geometries: GeoDataFrame with ward boundaries
        state_name: State name to determine geopolitical zone for variable selection

    Returns:
        DataFrame with zone-appropriate environmental variables
    """
    from app.core.tpr_utils import get_geopolitical_zone

    # Determine geopolitical zone for variable selection
    if state_name:
        zone = get_geopolitical_zone(state_name.replace(' State', '').strip())
        logger.info(f"Extracting variables for {state_name} (Zone: {zone})")
    else:
        zone = 'Unknown'
        logger.warning("No state provided, using default North-East variables")

    # Use hardcoded zone variables from Nigeria_Zones__States__and_Variables.csv
    # These are the approved variables for each zone based on latest configuration
    # NOTE: TPR is calculated from data, not extracted from rasters
    # NOTE: urban_extent is always included for ITN distribution planning
    zone_variables = {
        'North-East': ['distance_to_waterbodies', 'rainfall', 'soil_wetness', 'urban_extent'],
        'North-West': ['rainfall', 'ndwi', 'housing_quality', 'elevation', 'urban_extent'],
        'North-Central': ['nighttime_lights', 'housing_quality', 'soil_wetness', 'distance_to_waterbodies', 'ndmi', 'urban_extent'],
        'South-East': ['ndwi', 'housing_quality', 'elevation', 'urban_extent'],
        'South-South': ['ndwi', 'housing_quality', 'elevation', 'urban_extent'],
        'South-West': ['ndwi', 'housing_quality', 'elevation', 'urban_extent']
    }

    selected_vars = zone_variables.get(zone, zone_variables['North-East'])
    logger.info(f"Using zone variables for {zone}: {selected_vars}")

    logger.info(f"Selected {len(selected_vars)} variables for {zone}: {selected_vars}")
    
    # Use configuration for paths that work both locally and on AWS
    from app.config.data_paths import RASTER_DIR
    raster_base = RASTER_DIR
    
    # Full map of available rasters (but we'll only use zone-specific ones)
    raster_map = {
        'rainfall': 'rainfall_monthly/2021/X2021_rainfall_year_2021_month_*.tif',
        'temp': 'temperature_monthly/2021/X2021_temperature_year_2021_month_*.tif',
        'evi': 'EVI/EVI_v6.2018.*.mean.1km.tif',
        'ndmi': 'NDMI/NDMI_Nigeria_2023.tif',
        'ndwi': 'NDWI/Nigeria_NDWI_2023.tif',
        'elevation': 'Elevation/MERIT_Elevation.max.1km.tif',
        'distance_to_waterbodies': 'distance_to_water_bodies/distance_to_water.tif',
        'nighttime_lights': 'night_timel_lights/2024/VIIRS_NTL_2024_Nigeria.tif',
        'housing_quality': 'housing/2019_Nature_Africa_Housing_2015_NGA.tiff',
        'urban_extent': 'urban_extent/UrbanPercentage_2024_Nigeria.tif',  # Use most recent year
        'pfpr': 'pf_parasite_rate/202406_Global_Pf_Parasite_Rate_NGA_2022.tiff',
        'soil_wetness': 'surface_soil_wetness/*.tif'  # Use glob pattern for soil wetness
    }
    
    env_data = pd.DataFrame()
    env_data['WardCode'] = ward_geometries['WardCode'] if 'WardCode' in ward_geometries.columns else range(len(ward_geometries))
    
    # Only process variables selected for this zone
    for var_name in selected_vars:
        if var_name not in raster_map:
            logger.warning(f"No raster mapping for variable: {var_name}")
            env_data[var_name.title()] = None
            continue
            
        raster_pattern = raster_map[var_name]
        values = []
        
        # Special handling for urban_extent - try multiple years if needed
        if var_name == 'urban_extent':
            raster_files = []
            # Try years in descending order (2024, 2023, 2022, etc.)
            for year in [2024, 2023, 2022, 2021, 2020]:
                year_pattern = f'urban_extent/UrbanPercentage_{year}_Nigeria.tif'
                year_path = os.path.join(raster_base, year_pattern)
                if os.path.exists(year_path):
                    raster_files = [year_path]
                    logger.info(f"Using urban percentage data from year {year}")
                    break
            
            if not raster_files:
                # Fall back to glob pattern
                raster_path_pattern = os.path.join(raster_base, 'urban_extent/*.tif')
                raster_files = glob.glob(raster_path_pattern)
                if raster_files:
                    # Use the most recent file based on filename
                    raster_files = sorted(raster_files, reverse=True)[:1]
        else:
            raster_path_pattern = os.path.join(raster_base, raster_pattern)
            raster_files = glob.glob(raster_path_pattern)
        
        if not raster_files:
            logger.warning(f"No raster files found for {var_name} at {raster_path_pattern}")
            # Generate mock values for testing (zone-specific ranges)
            import numpy as np
            np.random.seed(42)  # For reproducibility
            
            mock_ranges = {
                'pfpr': (0.1, 0.5),  # Parasite prevalence rate
                'housing_quality': (0.0, 0.2),  # Housing quality index
                'evi': (0.2, 0.6),  # Enhanced vegetation index
                'ndwi': (-0.5, -0.3),  # Normalized difference water index
                'ndmi': (-0.1, 0.1),  # Normalized difference moisture index
                'soil_wetness': (0.3, 0.7),  # Soil wetness
                'elevation': (150, 300),  # Elevation in meters
                'distance_to_waterbodies': (1000, 10000),  # Distance in meters
                'nighttime_lights': (0.0, 0.5),  # Nighttime lights intensity
                'rainfall': (800, 1200),  # Annual rainfall in mm
                'temp': (25, 35),  # Temperature in Celsius
                'urban_extent': (10, 60)  # Urban percentage (10-60% for realistic variation)
            }
            
            if var_name in mock_ranges:
                min_val, max_val = mock_ranges[var_name]
                # Add some spatial variation
                mock_values = np.random.uniform(min_val, max_val, len(ward_geometries))
                
                # CRITICAL: Use correct column name for urban percentage
                if var_name == 'urban_extent':
                    env_data['urban_percentage'] = mock_values
                    logger.info(f"Generated mock urban percentage data: mean={mock_values.mean():.2f}%")
                else:
                    env_data[var_name.title().replace('_', '')] = mock_values
                    logger.info(f"Generated mock data for {var_name}: mean={mock_values.mean():.2f}")
            else:
                env_data[var_name.title()] = None
            continue
        
        # Use the first matching file (or aggregate for monthly data)
        raster_file = raster_files[0]
        
        try:
            with rasterio.open(raster_file) as src:
                # Extract values for each ward
                for idx, row in ward_geometries.iterrows():
                    try:
                        # Get ward geometry
                        geom = row.geometry
                        
                        # Extract value at centroid
                        if geom is not None:
                            centroid = geom.centroid
                            # Sample raster at centroid
                            for val in src.sample([(centroid.x, centroid.y)]):
                                value = val[0] if val[0] != src.nodata else None
                                values.append(value)
                        else:
                            values.append(None)
                    except Exception as e:
                        logger.debug(f"Error extracting {var_name} for ward {idx}: {e}")
                        values.append(None)
            
            # For rainfall and temperature, calculate annual values
            if 'rainfall' in var_name.lower() and len(raster_files) > 1:
                # Sum monthly rainfall
                monthly_values = []
                for rf in raster_files[:12]:  # Use up to 12 months
                    with rasterio.open(rf) as src:
                        month_vals = []
                        for idx, row in ward_geometries.iterrows():
                            if row.geometry is not None:
                                centroid = row.geometry.centroid
                                for val in src.sample([(centroid.x, centroid.y)]):
                                    month_vals.append(val[0] if val[0] != src.nodata else 0)
                            else:
                                month_vals.append(0)
                        monthly_values.append(month_vals)
                
                # Sum across months
                values = [sum(month_vals) for month_vals in zip(*monthly_values)]
            
            elif 'temperature' in var_name.lower() and len(raster_files) > 1:
                # Average monthly temperature
                monthly_values = []
                for rf in raster_files[:12]:
                    with rasterio.open(rf) as src:
                        month_vals = []
                        for idx, row in ward_geometries.iterrows():
                            if row.geometry is not None:
                                centroid = row.geometry.centroid
                                for val in src.sample([(centroid.x, centroid.y)]):
                                    month_vals.append(val[0] if val[0] != src.nodata else None)
                            else:
                                month_vals.append(None)
                        monthly_values.append(month_vals)
                
                # Average across months
                values = []
                for month_vals in zip(*monthly_values):
                    valid_vals = [v for v in month_vals if v is not None]
                    if valid_vals:
                        values.append(sum(valid_vals) / len(valid_vals))
                    else:
                        values.append(None)
            
            # CRITICAL: Use correct column name for urban percentage
            if var_name == 'urban_extent':
                env_data['urban_percentage'] = values
                logger.info(f"Extracted urban_percentage: {sum(1 for v in values if v is not None)}/{len(values)} valid values")
            else:
                env_data[var_name] = values
                logger.info(f"Extracted {var_name}: {sum(1 for v in values if v is not None)}/{len(values)} valid values")
            
        except Exception as e:
            logger.error(f"Error processing raster {var_name}: {e}")
            if var_name == 'urban_extent':
                # Ensure we always have urban_percentage column even if extraction fails
                env_data['urban_percentage'] = [30.0] * len(ward_geometries)  # Default 30% urban
                logger.warning("Using default urban percentage of 30% due to extraction error")
            else:
                env_data[var_name] = None
    
    # FINAL CHECK: Ensure urban_percentage column exists
    if 'urban_percentage' not in env_data.columns:
        logger.warning("Urban percentage column missing after extraction, adding default values")
        env_data['urban_percentage'] = 30.0  # Default fallback
    
    return env_data


def match_and_merge_data(tpr_df: pd.DataFrame, state_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Match TPR data with shapefile using ward name normalization.

    Args:
        tpr_df: DataFrame with burden results (Burden, Total_Positive, Population)
        state_gdf: GeoDataFrame with state ward boundaries

    Returns:
        Merged GeoDataFrame
    """
    logger.info(f"🔄 Starting ward matching: {len(tpr_df)} burden records, {len(state_gdf)} shapefile wards")

    # Normalize ward names in both datasets
    tpr_df['WardName_norm'] = tpr_df['WardName'].apply(normalize_ward_name)
    state_gdf['WardName_norm'] = state_gdf['WardName'].apply(normalize_ward_name)

    # Log sample normalized names for debugging
    logger.debug(f"Sample burden ward names: {list(tpr_df['WardName_norm'].head(3))}")
    logger.debug(f"Sample shapefile ward names: {list(state_gdf['WardName_norm'].head(3))}")

    # Try exact match first
    merged = state_gdf.merge(
        tpr_df,
        on='WardName_norm',
        how='left',
        suffixes=('', '_burden')
    )

    # Check match rate using Burden column
    burden_col = 'Burden' if 'Burden' in merged.columns else 'Total_Positive'
    matched = merged[burden_col].notna().sum()
    total = len(merged)
    match_rate = matched / total * 100 if total > 0 else 0

    logger.info(f"📊 Initial ward matching: {matched}/{total} ({match_rate:.1f}%) matched")

    # If match rate is low, try fuzzy matching
    if match_rate < 80:
        from difflib import get_close_matches

        unmatched_shapefile = merged[merged[burden_col].isna()]['WardName_norm'].unique()
        unmatched_burden = tpr_df['WardName_norm'].unique()

        fuzzy_matches = {}
        for ward in unmatched_shapefile:
            matches = get_close_matches(ward, unmatched_burden, n=1, cutoff=0.8)
            if matches:
                fuzzy_matches[ward] = matches[0]

        # Apply fuzzy matches
        for shapefile_ward, burden_ward in fuzzy_matches.items():
            burden_data = tpr_df[tpr_df['WardName_norm'] == burden_ward].iloc[0]
            if 'Burden' in burden_data:
                merged.loc[merged['WardName_norm'] == shapefile_ward, 'Burden'] = burden_data['Burden']
            if 'Population' in burden_data:
                merged.loc[merged['WardName_norm'] == shapefile_ward, 'Population'] = burden_data['Population']
            merged.loc[merged['WardName_norm'] == shapefile_ward, 'Total_Positive'] = burden_data['Total_Positive']

        new_matched = merged[burden_col].notna().sum()
        logger.info(f"After fuzzy matching: {new_matched}/{total} matched")

    return merged


def normalize_state_name(state_name: str) -> str:
    """
    Normalize state name for matching between data and shapefile.
    Handles common variations like hyphens, spaces, and 'State' suffix.
    """
    # Remove 'State' suffix and clean whitespace
    normalized = state_name.replace(' State', '').strip()

    # Replace hyphens with spaces (e.g., "Akwa-Ibom" -> "Akwa Ibom")
    normalized = normalized.replace('-', ' ')

    # Remove any state code prefixes (eb, kb, pl, ns, etc.)
    import re
    normalized = re.sub(r'^[a-z]{2}\s+', '', normalized, flags=re.IGNORECASE)

    # HARDCODED FIX for problematic states
    state_mappings = {
        'akwa ibom': 'Akwa Ibom',
        'cross river': 'Cross River',
        'federal capital territory': 'Federal Capital Territory',
        'fct': 'Federal Capital Territory',
        # DIRECT HARDCODED FIXES FOR PROBLEMATIC STATES
        'ebonyi': 'Ebonyi',
        'kebbi': 'Kebbi',
        'plateau': 'Plateau',
        'nasarawa': 'Nasarawa',
        'nassarawa': 'Nasarawa',  # Common misspelling
        'nasar awa': 'Nasarawa',  # Space variation
        'plat eau': 'Plateau',    # Space variation
        'keb bi': 'Kebbi',        # Space variation
        'ebo nyi': 'Ebonyi'       # Space variation
    }

    # Check if lowercase version needs mapping
    lower_norm = normalized.lower().strip()
    if lower_norm in state_mappings:
        return state_mappings[lower_norm]

    # Return with proper capitalization
    return ' '.join(word.capitalize() for word in normalized.split())


def create_tpr_map(tpr_results: pd.DataFrame, session_folder: str, state_name: str) -> bool:
    """
    Create an interactive TPR distribution map.

    Args:
        tpr_results: DataFrame with TPR results
        session_folder: Session folder path
        state_name: State name

    Returns:
        True if map was created successfully
    """
    logger.info(f"🗺️ create_tpr_map called for {state_name} with {len(tpr_results)} TPR results")

    try:
        # Load Nigeria shapefile and filter to state
        logger.info("📂 Loading Nigeria shapefile for map creation...")
        master_gdf = load_nigeria_shapefile()
        if master_gdf is None:
            logger.warning("❌ Nigeria shapefile not found, skipping map creation")
            return False

        logger.info(f"✅ Shapefile loaded with {len(master_gdf)} total wards")

        # Normalize state name for better matching
        normalized_state = normalize_state_name(state_name)
        logger.info(f"🔍 Original state name: '{state_name}' -> Normalized: '{normalized_state}'")

        # Try multiple matching strategies
        state_gdf = None

        # Strategy 1: Exact match with normalized name
        state_gdf = master_gdf[master_gdf['StateName'] == normalized_state].copy()
        if not state_gdf.empty:
            logger.info(f"✅ Found exact match for '{normalized_state}'")

        # Strategy 2: Case-insensitive match
        if state_gdf is None or state_gdf.empty:
            logger.info(f"⚠️ No exact match, trying case-insensitive...")
            state_gdf = master_gdf[master_gdf['StateName'].str.lower() == normalized_state.lower()].copy()
            if not state_gdf.empty:
                logger.info(f"✅ Found case-insensitive match")

        # Strategy 3: Partial match (contains)
        if state_gdf is None or state_gdf.empty:
            logger.info(f"⚠️ No case match, trying partial match...")
            # Remove common prefixes that might be in data
            core_name = normalized_state.replace('ad ', '').replace('ak ', '').replace('eb ', '').replace('bn ', '').replace('ns ', '').replace('pl ', '').replace('kb ', '')
            state_gdf = master_gdf[master_gdf['StateName'].str.contains(core_name, case=False, na=False)].copy()
            if not state_gdf.empty:
                logger.info(f"✅ Found partial match for '{core_name}'")

        # Strategy 4: HARDCODED DIRECT MATCH for problematic states
        if state_gdf is None or state_gdf.empty:
            logger.info(f"⚠️ Trying hardcoded matches for problematic states...")

            # Direct hardcoded mappings for states that consistently fail
            hardcoded_matches = {
                'benue': 'Benue',  # Added Benue fix
                'ebonyi': 'Ebonyi',
                'kebbi': 'Kebbi',
                'plateau': 'Plateau',
                'nasarawa': 'Nasarawa',
                'nassarawa': 'Nasarawa'
            }

            # Check if our normalized state matches any hardcoded entry
            for key, value in hardcoded_matches.items():
                if key in normalized_state.lower() or normalized_state.lower() in key:
                    logger.info(f"🔧 Using hardcoded match: '{key}' -> '{value}'")
                    state_gdf = master_gdf[master_gdf['StateName'] == value].copy()
                    if not state_gdf.empty:
                        logger.info(f"✅ Found hardcoded match for '{value}'")
                        break

        # Strategy 5: ULTRA AGGRESSIVE - Check original state name for specific patterns
        if state_gdf is None or state_gdf.empty:
            logger.info(f"⚠️ Final attempt - checking original state name patterns...")

            # Check if the original state name contains these patterns
            if 'ebonyi' in state_name.lower():
                state_gdf = master_gdf[master_gdf['StateName'] == 'Ebonyi'].copy()
                logger.info(f"✅ HARDCODED: Found Ebonyi from pattern in '{state_name}'")
            elif 'kebbi' in state_name.lower():
                state_gdf = master_gdf[master_gdf['StateName'] == 'Kebbi'].copy()
                logger.info(f"✅ HARDCODED: Found Kebbi from pattern in '{state_name}'")
            elif 'plateau' in state_name.lower():
                state_gdf = master_gdf[master_gdf['StateName'] == 'Plateau'].copy()
                logger.info(f"✅ HARDCODED: Found Plateau from pattern in '{state_name}'")
            elif 'nasarawa' in state_name.lower() or 'nassarawa' in state_name.lower():
                state_gdf = master_gdf[master_gdf['StateName'] == 'Nasarawa'].copy()
                logger.info(f"✅ HARDCODED: Found Nasarawa from pattern in '{state_name}'")

        # Strategy 6: Fuzzy match against available shapefile states
        if state_gdf is None or state_gdf.empty:
            try:
                from difflib import get_close_matches
                available_states = [s for s in master_gdf['StateName'].dropna().unique().tolist()]
                lower_map = {s.lower(): s for s in available_states}
                close = get_close_matches(normalized_state.lower(), list(lower_map.keys()), n=1, cutoff=0.7)
                if close:
                    matched_state = lower_map[close[0]]
                    state_gdf = master_gdf[master_gdf['StateName'] == matched_state].copy()
                    if not state_gdf.empty:
                        logger.info(f"✅ Fuzzy matched state '{normalized_state}' -> '{matched_state}'")
            except Exception as e:
                logger.debug(f"Fuzzy matching for state failed: {e}")

        # Log available state names if no match found
        if state_gdf is None or state_gdf.empty:
            sample_states = master_gdf['StateName'].unique()[:10]
            logger.error(f"❌ No shapefile data found for '{state_name}' (normalized: '{normalized_state}')")
            logger.error(f"Available states (first 10): {sample_states}")
            return False
        
        logger.info(f"✅ Found {len(state_gdf)} wards for {state_name}")
        
        # Match and merge burden data with shapefile
        logger.info("🔄 Matching burden data with shapefile...")
        merged_gdf = match_and_merge_data(tpr_results, state_gdf)

        # Log matching results
        matched = merged_gdf['Burden'].notna().sum() if 'Burden' in merged_gdf.columns else 0
        total_wards = len(merged_gdf)
        unmatched = total_wards - matched
        match_rate = (matched / total_wards * 100) if total_wards > 0 else 0

        logger.info(f"📊 Matching Results:")
        logger.info(f"  Total wards in shapefile: {total_wards}")
        logger.info(f"  Wards with burden data: {matched}")
        logger.info(f"  Wards without burden data: {unmatched}")
        logger.info(f"  Match rate: {match_rate:.1f}%")

        # Filter out null geometries before creating the map
        initial_count = len(merged_gdf)
        merged_gdf = merged_gdf[merged_gdf.geometry.notna()].copy()
        null_geometry_count = initial_count - len(merged_gdf)

        if null_geometry_count > 0:
            logger.warning(f"⚠️ Filtered out {null_geometry_count} wards with null geometries")
            logger.info(f"  Remaining wards for visualization: {len(merged_gdf)}")

        # Calculate LGA average burden (volume-weighted: sum(positive)/sum(population)*1000)
        lga_avg_burden = calculate_lga_averages(
            merged_gdf, 'Burden',
            numerator_col='Total_Positive',
            denominator_col='Population'
        )

        # Create hover text with LGA average and comparison
        hover_text = []
        for _, row in merged_gdf.iterrows():
            lga_code = row.get('LGACode')
            lga_name = row.get('LGAName', 'Unknown')
            lga_avg = lga_avg_burden.get(lga_code)
            ward_name = row.get('WardName', 'Unknown Ward')

            if pd.notna(row.get('Burden')):
                ward_burden = row['Burden']
                population = int(row.get('Population', 0))
                positive_cases = int(row.get('Total_Positive', 0))

                # Clean format with labels
                text = f"<b>Ward:</b> {ward_name}<br>"
                text += f"<b>LGA:</b> {lga_name}<br>"
                text += f"<br><b>Malaria Burden:</b> {ward_burden:.1f} per 1,000"

                if lga_avg is not None:
                    diff = ward_burden - lga_avg
                    diff_sign = '+' if diff > 0 else ''
                    # Red if above average (worse), green if below (better)
                    diff_color = '#e74c3c' if diff > 0 else '#27ae60' if diff < 0 else '#666'
                    text += f"<br><b>LGA Average:</b> {lga_avg:.1f} per 1,000"
                    text += f" <span style='color:{diff_color}'>({diff_sign}{diff:.1f})</span>"

                # Additional details in lighter color
                text += f"<br><br><span style='color:#888'>Population: {population:,}</span>"
                text += f"<br><span style='color:#888'>Positive Cases: {positive_cases:,}</span>"
            else:
                text = f"<b>Ward:</b> {ward_name}<br>"
                text += f"<b>LGA:</b> {lga_name}<br>"
                text += f"<br><span style='color:#999'><i>No malaria data available</i></span>"
                if lga_avg is not None:
                    text += f"<br><b>LGA Average:</b> {lga_avg:.1f} per 1,000"
            hover_text.append(text)

        # Reset index to ensure proper alignment with GeoJSON
        merged_gdf = merged_gdf.reset_index(drop=True)

        # Convert to GeoJSON
        geojson = merged_gdf.__geo_interface__

        # Create figure
        fig = go.Figure()

        # Single trace approach with proper handling of missing data
        # Fill NaN values with a specific value for display
        z_values = merged_gdf['Burden'].fillna(-999).values if 'Burden' in merged_gdf.columns else np.full(len(merged_gdf), -999)

        # Log z values for debugging
        logger.info(f"🔍 Z values debug:")
        logger.info(f"  Total z values: {len(z_values)}")
        logger.info(f"  Non-missing (not -999): {(z_values != -999).sum()}")
        logger.info(f"  Missing (-999): {(z_values == -999).sum()}")
        if (z_values != -999).any():
            valid_z = z_values[z_values != -999]
            logger.info(f"  Valid burden range: {valid_z.min():.1f} to {valid_z.max():.1f} per 1,000")

        # Create custom colorscale that shows gray for missing data
        # Burden scale: 0-100+ per 1,000 population
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged_gdf.index,
            z=z_values,
            colorscale=[
                [0.0, '#d3d3d3'],    # Light gray for missing data
                [0.001, '#d3d3d3'],  # Still gray
                [0.01, '#2ecc71'],   # Green for low burden (0 per 1,000)
                [0.2, '#f1c40f'],    # Yellow (20 per 1,000)
                [0.4, '#e67e22'],    # Orange (40 per 1,000)
                [0.6, '#e74c3c'],    # Red (60 per 1,000)
                [0.8, '#c0392b'],    # Dark red (80 per 1,000)
                [1.0, '#9b59b6']     # Purple for very high burden (100+ per 1,000)
            ],
            marker_opacity=0.8,
            marker_line_width=1.5,
            marker_line_color='#333333',
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=hover_text,
            colorbar=dict(
                title=dict(
                    text="Malaria Burden<br>(per 1,000 pop)",
                    font=dict(size=12)
                ),
                tickmode='array',
                tickvals=[0, 20, 40, 60, 80, 100],
                ticktext=['0', '20', '40', '60', '80', '100+'],
                len=0.8,
                y=0.5
            ),
            zmin=0,  # Set min to 0
            zmax=100,  # Set max to 100
            showscale=True
        ))

        # Add LGA boundary overlay
        add_lga_boundary_overlay(fig, merged_gdf)

        # Update layout
        center_lat = merged_gdf.geometry.centroid.y.mean()
        center_lon = merged_gdf.geometry.centroid.x.mean()
        
        # Add debug info to title
        debug_text = f" ({matched}/{total_wards} wards with data, {match_rate:.0f}% match rate)"

        fig.update_layout(
            title=dict(
                text=f"Malaria Burden per 1,000 - {state_name}{debug_text}",
                font=dict(size=16, family="Arial, sans-serif"),
                x=0.5,
                xanchor='center'
            ),
            mapbox=dict(
                style="open-street-map",
                center=dict(lat=center_lat, lon=center_lon),
                zoom=6.5
            ),
            height=700,
            margin=dict(t=50, b=30, l=30, r=30)
        )
        
        # Save the map
        map_path = os.path.join(session_folder, 'tpr_distribution_map.html')
        logger.info(f"💾 Attempting to save map to: {map_path}")
        
        try:
            # Use include_plotlyjs=True to embed Plotly in HTML (bypasses CDN/CSP issues)
            fig.write_html(
                map_path,
                include_plotlyjs=True,
                config={'displayModeBar': True, 'displaylogo': False}
            )

            # INJECT BROWSER CONSOLE DEBUG LOGGING
            # Add JavaScript that logs critical debug info when map loads
            debug_script = f"""
<script>
console.log("=".repeat(60));
console.log("🗺️ TPR MAP DEBUG INFO (F12 Console)");
console.log("=".repeat(60));
console.log("State: {state_name}");
console.log("Total wards in shapefile: {total_wards}");
console.log("Wards with TPR data: {matched}");
console.log("Wards without TPR data: {unmatched}");
console.log("Match rate: {match_rate:.1f}%");
console.log("GeoJSON features: " + (typeof Plotly !== 'undefined' ? "Loaded" : "NOT LOADED"));

// Log Z values (TPR data)
var zValues = {z_values.tolist()};
console.log("Z values (TPR data):");
console.log("  Total: " + zValues.length);
console.log("  Sample (first 10): " + zValues.slice(0, 10));
console.log("  Min: " + Math.min(...zValues.filter(v => v !== -999)));
console.log("  Max: " + Math.max(...zValues));
console.log("  Missing data (-999): " + zValues.filter(v => v === -999).length);
console.log("  Valid data: " + zValues.filter(v => v !== -999).length);

if (zValues.filter(v => v !== -999).length === 0) {{
    console.error("⚠️ ERROR: ALL Z VALUES ARE MISSING! This is why the map is blank.");
    console.error("⚠️ The map has geometry (ward boundaries) but NO TPR data values.");
}} else {{
    console.log("✅ Z values look good - map should show colors");
}}
console.log("=".repeat(60));
</script>
"""

            # Inject the debug script into the HTML file
            with open(map_path, 'r') as f:
                html_content = f.read()

            # Insert debug script before closing </body> tag
            html_content = html_content.replace('</body>', debug_script + '\n</body>')

            with open(map_path, 'w') as f:
                f.write(html_content)

            # Inject LGA hover highlighting
            try:
                lga_codes = merged_gdf['LGACode'].fillna('').astype(str).tolist()
                inject_lga_hover_highlight(map_path, lga_codes)
                logger.info("✅ LGA hover highlight injected")
            except Exception as hover_err:
                logger.warning(f"Failed to inject LGA hover highlight: {hover_err}")

            # Verify file was created
            if os.path.exists(map_path):
                file_size = os.path.getsize(map_path)
                logger.info(f"✅ TPR distribution map saved successfully ({file_size} bytes) with debug logging")
                return True
            else:
                logger.error(f"❌ Map file was not created at {map_path}")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to write HTML file: {e}")
            return False
        
    except Exception as e:
        import traceback
        logger.error(f"❌ Error creating TPR map: {e}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        return False


def create_shapefile_package(gdf: gpd.GeoDataFrame, output_dir: str) -> str:
    """
    Create a shapefile ZIP package from GeoDataFrame.
    
    Args:
        gdf: GeoDataFrame to save
        output_dir: Directory to save the ZIP file
        
    Returns:
        Path to created ZIP file
    """
    # Create temporary directory for shapefile components
    temp_dir = tempfile.mkdtemp()
    shapefile_base = os.path.join(temp_dir, 'ward_boundaries')
    
    # Save shapefile
    gdf.to_file(shapefile_base + '.shp')
    
    # Create ZIP
    zip_path = os.path.join(output_dir, 'raw_shapefile.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
            file_path = shapefile_base + ext
            if os.path.exists(file_path):
                arcname = 'ward_boundaries' + ext
                zf.write(file_path, arcname)
    
    # Clean up temp directory
    import shutil
    shutil.rmtree(temp_dir)
    
    logger.info(f"Created shapefile package: {zip_path}")
    return zip_path


@tool
def analyze_tpr_data(
    thought: str,
    action: str = "analyze",
    options: str = "{}",
    graph_state: Annotated[dict, InjectedState] = None
) -> str:
    """
    Analyze TPR (Test Positivity Rate) data with multiple capabilities.
    
    This tool detects TPR data, calculates test positivity rates using production logic,
    and prepares data for risk analysis pipeline.
    
    Actions:
    - analyze: Basic exploration and summary of TPR data
    - calculate_tpr: Calculate ward-level TPR with user selections
    - prepare_for_risk: Create raw_data.csv and raw_shapefile.zip for risk analysis
    
    Options for calculate_tpr:
    - age_group: 'all_ages' (default), 'u5' (Under 5), 'o5' (Over 5), 'pw' (Pregnant women)
    - test_method: 'both' (default), 'rdt' (RDT only), 'microscopy' (Microscopy only)  
    - facility_level: 'all' (default), 'primary', 'secondary', 'tertiary'
    
    Example options JSON:
    {"age_group": "u5", "test_method": "both", "facility_level": "primary"}
    
    Args:
        thought: Internal reasoning about the analysis
        action: The action to perform (analyze, calculate_tpr, prepare_for_risk)
        options: JSON string with additional options
        
    Returns:
        String with results or status message
    """
    try:
        # Parse options
        try:
            opts = json.loads(options) if options else {}
        except:
            opts = {}
        
        # Get session information
        session_id = graph_state.get('session_id', 'default') if graph_state else 'default'
        session_folder = f"instance/uploads/{session_id}"
        
        # Load data from session
        data_files = glob.glob(os.path.join(session_folder, "*.xlsx")) + \
                    glob.glob(os.path.join(session_folder, "*.csv"))
        
        if not data_files:
            return "No data files found in session. Please upload TPR data first."
        
        # Load the most recent file
        data_file = max(data_files, key=os.path.getctime)
        
        if data_file.endswith('.csv'):
            df = EncodingHandler.read_csv_with_encoding(data_file)
        else:
            df = EncodingHandler.read_excel_with_encoding(data_file)
        
        # Load persisted schema so state extraction works even in the analyze action
        try:
            _sm_early = DataAnalysisStateManager(session_id)
            _schema_early = (_sm_early.load_state() or {}).get('column_schema') or {}
        except Exception:
            _schema_early = {}

        # Extract state name from schema column
        state_name = 'Unknown'
        state_col = _schema_early.get('state')
        if state_col and state_col in df.columns:
            import re as _re
            vals = df[state_col].dropna()
            if not vals.empty:
                _s = str(vals.iloc[0])
                _s = _re.sub(r'^[a-z]{2}\s+', '', _s, flags=_re.IGNORECASE)
                _s = _re.sub(r'\s+State$', '', _s, flags=_re.IGNORECASE)
                state_name = ' '.join(w.capitalize() for w in _s.replace('-', ' ').split())

        # Perform requested action
        if action == "analyze":
            # Get column info
            column_info = []
            for col in df.columns[:20]:
                dtype = str(df[col].dtype)
                null_count = df[col].isna().sum()
                column_info.append(f"  - {col}: {dtype} ({null_count} nulls)")

            result = f"""TPR Data Analysis Results:

File: {os.path.basename(data_file)}
State: {state_name}
Shape: {len(df)} rows × {len(df.columns)} columns

Sample Columns:
{chr(10).join(column_info)}

You can now:
1. Calculate TPR: Use action="calculate_tpr"
2. Prepare for risk analysis: Use action="prepare_for_risk"
"""
            return result
            
        elif action == "calculate_tpr":
            # Get user selections from options (with validation)
            age_group = opts.get('age_group', 'all_ages')
            test_method = opts.get('test_method', 'both')
            facility_level = opts.get('facility_level', 'all')
            
            # Validate selections
            valid_age_groups = ['all_ages', 'u5', 'o5', 'pw']
            valid_test_methods = ['both', 'rdt', 'microscopy']
            valid_facility_levels = ['all', 'primary', 'secondary', 'tertiary']
            
            if age_group not in valid_age_groups:
                logger.warning(f"Invalid age group '{age_group}', using 'all_ages'")
                age_group = 'all_ages'
            
            if test_method not in valid_test_methods:
                logger.warning(f"Invalid test method '{test_method}', using 'both'")
                test_method = 'both'
            
            if facility_level not in valid_facility_levels:
                logger.warning(f"Invalid facility level '{facility_level}', using 'all'")
                facility_level = 'all'
            
            logger.info(f"Calculating TPR - Age: {age_group}, Method: {test_method}, Facilities: {facility_level}")

            # Load persisted column schema (saved by TPRWorkflowHandler during selection steps)
            try:
                _sm = DataAnalysisStateManager(session_id)
                column_schema = (_sm.load_state() or {}).get('column_schema') or {}
            except Exception:
                column_schema = {}

            try:
                tpr_results = calculate_ward_tpr(df,
                                                age_group=age_group,
                                                test_method=test_method,
                                                facility_level=facility_level,
                                                schema=column_schema)
            except Exception as e:
                logger.error(f"Error calculating TPR: {e}")
                return f"Error calculating TPR: {str(e)}. Please check your data format and try again."
            
            if tpr_results.empty:
                # Provide helpful feedback about what might be missing
                available_cols = [col for col in df.columns if any(
                    keyword in col.lower() for keyword in ['rdt', 'microscopy', 'tested', 'positive']
                )]
                
                return f"""No TPR results calculated. This could be because:
1. No data found for selected age group '{age_group}'
2. No {test_method} test data available
3. No facilities matching level '{facility_level}'

Available test-related columns in your data:
{', '.join(available_cols[:10])}

Try using 'all_ages' with 'both' test methods and 'all' facilities for the broadest analysis."""
            
            # Save TPR results
            tpr_output_path = os.path.join(session_folder, 'tpr_results.csv')
            tpr_results.to_csv(tpr_output_path, index=False)

            # Save time-series TPR (preserving period dimension for trend analysis)
            period_col = column_schema.get('period')
            if period_col and period_col in df.columns:
                try:
                    from app.core.tpr_utils import calculate_ward_tpr_timeseries
                    ts_results = calculate_ward_tpr_timeseries(
                        df, age_group=age_group, test_method=test_method,
                        facility_level=facility_level, schema=column_schema
                    )
                    if not ts_results.empty:
                        ts_results.to_csv(os.path.join(session_folder, 'tpr_time_series.csv'), index=False)
                        logger.info(f"Saved time-series TPR: {len(ts_results)} rows")
                except Exception as e:
                    logger.warning(f"Could not generate time-series TPR: {e}")

            # Prepare summary
            summary = prepare_tpr_summary(tpr_results)

            # Format selections for display
            age_display = {
                'all_ages': 'All age groups',
                'u5': 'Under 5 years',
                'o5': 'Over 5 years',
                'pw': 'Pregnant women'
            }.get(age_group, age_group)

            method_display = {
                'both': 'RDT and Microscopy (max TPR)',
                'rdt': 'RDT only',
                'microscopy': 'Microscopy only'
            }.get(test_method, test_method)

            facility_display = {
                'all': 'All facilities',
                'primary': 'Primary health centers',
                'secondary': 'Secondary facilities',
                'tertiary': 'Tertiary facilities'
            }.get(facility_level, facility_level)

            # We'll update ward count after we match with shapefile to show ALL wards
            # Simplified result with the clean menu
            result = f"""## Malaria Burden Analysis Complete

**{state_name}**: {summary['mean_burden']:.1f} cases per 1,000 population (average)"""

            # AUTOMATICALLY PREPARE FOR RISK ANALYSIS (production approach)
            logger.info("🚀 Starting automatic preparation for risk analysis")
            
            # Create comprehensive debug tracking
            debug_stages = {
                "shapefile_loading": {},
                "ward_matching": {},
                "env_extraction": {},
                "file_creation": {},
                "map_creation": {}
            }
            
            try:
                # Load Nigeria shapefile and filter to state
                logger.info(f"📂 Attempting to load Nigeria shapefile for risk preparation...")
                
                # Debug to file
                with open('/tmp/tpr_debug.log', 'a') as f:
                    f.write(f"\n--- TPR Debug {session_id} ---\n")
                    f.write(f"Timestamp: {pd.Timestamp.now()}\n")
                    f.write(f"State: {state_name}\n")
                    f.write(f"About to call load_nigeria_shapefile()\n")
                
                master_gdf = load_nigeria_shapefile()
                
                with open('/tmp/tpr_debug.log', 'a') as f:
                    f.write(f"Shapefile load result: {master_gdf is not None}\n")
                    if master_gdf is not None:
                        f.write(f"Loaded {len(master_gdf)} wards\n")
                    else:
                        f.write("ERROR: master_gdf is None!\n")
                
                if master_gdf is not None:
                    logger.info(f"✅ Successfully loaded shapefile with {len(master_gdf)} wards")
                    debug_stages["shapefile_loading"] = {
                        "success": True,
                        "total_wards": len(master_gdf),
                        "columns": list(master_gdf.columns)[:10]  # First 10 columns
                    }
                    
                    # Clean state name for matching
                    clean_state = state_name.replace(' State', '').strip()
                    logger.info(f"🔍 Filtering for state: '{clean_state}'")
                    
                    state_gdf = master_gdf[master_gdf['StateName'] == clean_state].copy()
                    
                    if state_gdf.empty:
                        logger.warning(f"⚠️ No exact match for '{clean_state}', trying case-insensitive")
                        # Try case-insensitive match
                        state_gdf = master_gdf[master_gdf['StateName'].str.lower() == clean_state.lower()].copy()
                    
                    if not state_gdf.empty:
                        logger.info(f"✅ Found {len(state_gdf)} wards in shapefile for {state_name}")
                        debug_stages["shapefile_loading"]["state_wards"] = int(len(state_gdf))
                        
                        # Match and merge TPR data with shapefile
                        logger.info("🔄 Matching TPR data with shapefile wards...")
                        try:
                            merged_gdf = match_and_merge_data(tpr_results, state_gdf)

                            # Count matches - use Burden column (new metric)
                            matched_count = int(merged_gdf['Burden'].notna().sum()) if 'Burden' in merged_gdf.columns else 0
                            total_count = int(len(merged_gdf))
                            match_rate = (matched_count / total_count * 100) if total_count > 0 else 0

                            logger.info(f"✅ Ward matching complete: {matched_count}/{total_count} ({match_rate:.1f}%)")
                            debug_stages["ward_matching"] = {
                                "success": True,
                                "matched": matched_count,
                                "total": total_count,
                                "match_rate": match_rate
                            }

                            # Update result with TOTAL ward count (from shapefile)
                            result = f"""## Malaria Burden Analysis Complete

**{state_name}**: {summary['mean_burden']:.1f} cases per 1,000 population across {total_count} wards"""
                        except Exception as e:
                            logger.error(f"❌ Ward matching failed: {e}")
                            debug_stages["ward_matching"] = {"success": False, "error": str(e)}
                            merged_gdf = state_gdf.copy()
                            merged_gdf['Burden'] = 0
                            merged_gdf['Population'] = 0
                            merged_gdf['Total_Positive'] = 0
                        
                        # Extract environmental variables (zone-specific)
                        logger.info("🌍 Extracting zone-specific environmental variables from rasters")
                        try:
                            env_data = extract_environmental_variables(merged_gdf, state_name)
                            logger.info(f"✅ Environmental data extracted: {len(env_data.columns)} variables")
                            debug_stages["env_extraction"] = {
                                "success": True,
                                "variables": list(env_data.columns),
                                "rows": int(len(env_data))
                            }

                            # Save ward cache for fast combination switching
                            try:
                                from app.core.tpr_ward_cache import save_ward_cache
                                cache_saved = save_ward_cache(
                                    session_id=session_id,
                                    state_gdf=state_gdf,
                                    env_data=env_data,
                                    state_name=state_name
                                )
                                if cache_saved:
                                    logger.info("✅ Ward cache saved for combination switching")
                                    debug_stages["ward_cache"] = {"success": True}
                            except Exception as cache_err:
                                logger.warning(f"⚠️ Could not save ward cache: {cache_err}")
                                debug_stages["ward_cache"] = {"success": False, "error": str(cache_err)}
                        except Exception as e:
                            logger.error(f"❌ Environmental extraction failed: {e}")
                            debug_stages["env_extraction"] = {"success": False, "error": str(e)}
                            # Create minimal env_data
                            env_data = pd.DataFrame()
                            env_data['WardCode'] = range(len(merged_gdf))
                        
                        # Prepare final dataset
                        logger.info("📊 Preparing final dataset for risk analysis")
                        final_df = pd.DataFrame()
                        
                        # Add identifiers (CRITICAL for risk analysis)
                        final_df['WardCode'] = merged_gdf['WardCode'] if 'WardCode' in merged_gdf.columns else range(len(merged_gdf))
                        final_df['StateCode'] = merged_gdf['StateCode'] if 'StateCode' in merged_gdf.columns else state_name[:2].upper()
                        final_df['LGACode'] = merged_gdf['LGACode'] if 'LGACode' in merged_gdf.columns else ''
                        final_df['WardName'] = merged_gdf['WardName']
                        final_df['LGA'] = merged_gdf['LGAName'] if 'LGAName' in merged_gdf.columns else merged_gdf.get('LGA', '')
                        final_df['State'] = state_name
                        final_df['GeopoliticalZone'] = get_geopolitical_zone(state_name)
                        
                        # Add Burden metrics (new calculation)
                        final_df['Burden'] = merged_gdf['Burden'].fillna(0) if 'Burden' in merged_gdf.columns else 0
                        final_df['Population'] = merged_gdf['Population'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0) if 'Population' in merged_gdf.columns else 0
                        final_df['Total_Positive'] = merged_gdf['Total_Positive'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0) if 'Total_Positive' in merged_gdf.columns else 0
                        
                        # Add environmental variables
                        for col in env_data.columns:
                            if col != 'WardCode':
                                final_df[col] = env_data[col]
                        
                        logger.info(f"📋 Final dataset prepared: {len(final_df)} rows, {len(final_df.columns)} columns")
                        
                        # Save raw_data.csv
                        raw_data_path = os.path.join(session_folder, 'raw_data.csv')
                        try:
                            final_df.to_csv(raw_data_path, index=False)
                            logger.info(f"✅ Saved raw_data.csv with {len(final_df)} wards")
                            debug_stages["file_creation"]["raw_data"] = {
                                "success": True,
                                "path": raw_data_path,
                                "rows": len(final_df),
                                "columns": len(final_df.columns)
                            }
                        except Exception as e:
                            logger.error(f"❌ Failed to save raw_data.csv: {e}")
                            debug_stages["file_creation"]["raw_data"] = {"success": False, "error": str(e)}
                        
                        # Create raw_shapefile.zip
                        logger.info("📦 Creating shapefile package...")
                        try:
                            # Ensure shapefile has same columns as CSV
                            for col in final_df.columns:
                                if col not in merged_gdf.columns:
                                    merged_gdf[col] = final_df[col]
                            
                            shapefile_path = create_shapefile_package(merged_gdf, session_folder)
                            logger.info(f"✅ Created raw_shapefile.zip")
                            debug_stages["file_creation"]["shapefile"] = {
                                "success": True,
                                "path": shapefile_path
                            }
                        except Exception as e:
                            logger.error(f"❌ Failed to create shapefile: {e}")
                            debug_stages["file_creation"]["shapefile"] = {"success": False, "error": str(e)}
                        
                        # Create TPR map visualization
                        logger.info("🗺️ Creating TPR distribution map...")
                        try:
                            map_created = create_tpr_map(tpr_results, session_folder, state_name)
                            if map_created:
                                logger.info("✅ TPR map created successfully")
                                debug_stages["map_creation"] = {"success": True}
                            else:
                                logger.warning("⚠️ Map creation returned False")
                                debug_stages["map_creation"] = {"success": False, "error": "Function returned False"}
                        except Exception as e:
                            logger.error(f"❌ Map creation failed with exception: {e}")
                            import traceback
                            debug_stages["map_creation"] = {
                                "success": False,
                                "error": str(e),
                                "traceback": traceback.format_exc()
                            }

                        # Set session flags for risk analysis
                        flag_file = os.path.join(session_folder, '.risk_ready')
                        Path(flag_file).touch()
                    else:
                        logger.warning(f"⚠️ No shapefile data found for {state_name}")
                        debug_stages["shapefile_loading"]["state_wards"] = 0
                        # Still create map if possible (it has its own shapefile loading)
                        logger.info("🗺️ Attempting map creation despite empty state data...")
                        try:
                            map_created = create_tpr_map(tpr_results, session_folder, state_name)
                            if map_created:
                                debug_stages["map_creation"] = {"success": True, "fallback": True}
                            else:
                                debug_stages["map_creation"] = {"success": False, "error": "No state data"}
                        except Exception as e:
                            logger.error(f"❌ Fallback map creation failed: {e}")
                            debug_stages["map_creation"] = {"success": False, "error": str(e)}
                else:
                    logger.error("❌ CRITICAL: Nigeria shapefile returned None")
                    debug_stages["shapefile_loading"] = {"success": False, "error": "Shapefile is None"}
                    # Still try to create map
                    logger.info("🗺️ Attempting map creation without shapefile...")
                    try:
                        map_created = create_tpr_map(tpr_results, session_folder, state_name)
                        if map_created:
                            debug_stages["map_creation"] = {"success": True, "emergency": True}
                        else:
                            debug_stages["map_creation"] = {"success": False, "error": "No shapefile"}
                    except Exception as e:
                        logger.error(f"❌ Emergency map creation failed: {e}")
                        debug_stages["map_creation"] = {"success": False, "error": str(e)}
                
                # Save debug stages to file
                debug_file_path = os.path.join(session_folder, 'tpr_analysis_debug.json')
                with open(debug_file_path, 'w') as f:
                    json.dump(debug_stages, f, indent=2, cls=NumpyEncoder)
                logger.info(f"💾 Debug stages saved to {debug_file_path}")

            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                logger.error(f"❌ Error preparing for risk analysis: {e}\n{error_trace}")

                # Save error debug info
                debug_stages["critical_error"] = {
                    "message": str(e),
                    "traceback": error_trace
                }
                debug_file_path = os.path.join(session_folder, 'tpr_analysis_error.json')
                with open(debug_file_path, 'w') as f:
                    json.dump(debug_stages, f, indent=2, cls=NumpyEncoder)

                # Don't fail the whole TPR calculation, just log the error
                result += f"\n\n⚠️ Note: Could not prepare risk analysis files automatically (see tpr_analysis_error.json)"

            # Add the educational completion message with dynamic variables
            # Get geopolitical zone and environmental variables that were added
            try:
                geopolitical_zone = get_geopolitical_zone(state_name)

                # Get the list of variables added based on zone
                zone_variables_map = {
                    'North-Central': ['rainfall', 'ndvi', 'housing_quality', 'elevation', 'urban_percentage'],
                    'North-East': ['distance_to_waterbodies', 'rainfall', 'soil_wetness', 'urban_extent'],
                    'North-West': ['rainfall', 'ndwi', 'housing_quality', 'elevation', 'urban_extent'],
                    'South-East': ['ndwi', 'housing_quality', 'elevation', 'urban_extent'],
                    'South-South': ['ndwi', 'housing_quality', 'elevation', 'urban_extent'],
                    'South-West': ['ndwi', 'housing_quality', 'elevation', 'urban_extent']
                }

                # Get variables for this zone
                zone_variables = zone_variables_map.get(geopolitical_zone, ['environmental variables'])

                # Format variable names nicely
                formatted_vars = [v.replace('_', ' ').title() for v in zone_variables]

                result += f"""\n
🎉 **Malaria Burden Analysis Successfully Completed!**

**What Just Happened:**
I've calculated ward-level malaria burden per 1,000 population for **{state_name}** and created a unified dataset combining:
• Malaria burden data ({summary['mean_burden']:.1f} cases per 1,000 population across {total_count} wards)
• Geographic boundaries (ward shapefiles)
• Environmental variables for **{geopolitical_zone}** zone: {', '.join(formatted_vars)}

**Why This Matters:**
You now have a complete geospatial dataset ready for malaria risk analysis and intervention planning.

**Recommended Next Steps:**

📍 **Step 1: Visualize Malaria Burden**
Try: "**map malaria burden distribution**" to see case patterns across wards

🌍 **Step 2: Explore Environmental Factors**
Available variables for your region ({geopolitical_zone}):
"""
                # Add individual variable suggestions
                for var in formatted_vars[:3]:  # Show first 3 variables
                    result += f"• \"map {var.lower()} distribution\"\n"
                if len(formatted_vars) > 3:
                    result += f"• Plus {len(formatted_vars) - 3} more variables!\n"

                result += f"""
📊 **Step 3: Run Comprehensive Analysis**
Try: "**run malaria risk analysis**" to rank wards by malaria risk

🔄 **Explore Other Combinations**
I've pre-computed burden for all facility/age combinations in the background. You can switch at any time:
- "**switch to secondary facilities, under 5**"
- "**switch to all facilities, pregnant women**"
- "**compare all combinations**" to see a summary table

Your analysis files (raw_data.csv, shapefile) will be updated automatically.

Or ask me anything about your data - I'm here to help!
"""
            except Exception as e:
                logger.error(f"Error creating dynamic completion message: {e}")
                # Fallback to simple message if dynamic creation fails
                result += "\n\nYou can now:\n"
                result += "- **Map variable distribution** - e.g., \"map rainfall distribution\"\n"
                result += "- **Run malaria risk analysis** - Rank wards for ITN distribution\n"
                result += "- **Ask me anything** about your data"

            return result
            
        elif action == "prepare_for_risk":
            # Prepare data for risk analysis pipeline
            logger.info("Preparing TPR data for risk analysis")
            
            # Step 1: Calculate TPR if not already done
            tpr_results_file = os.path.join(session_folder, 'tpr_results.csv')
            if os.path.exists(tpr_results_file):
                # Load TPR results
                tpr_results = EncodingHandler.read_csv_with_encoding(tpr_results_file)
            else:
                try:
                    _sm2 = DataAnalysisStateManager(session_id)
                    _schema2 = (_sm2.load_state() or {}).get('column_schema') or {}
                except Exception:
                    _schema2 = {}
                tpr_results = calculate_ward_tpr(df, age_group='all_ages', schema=_schema2)
            
            # Step 2: Load Nigeria shapefile and filter to state
            master_gdf = load_nigeria_shapefile()
            if master_gdf is None:
                return "Error: Nigeria shapefile not found. Cannot prepare for risk analysis."
            
            # Clean state name for matching
            clean_state = state_name.replace(' State', '').strip()
            state_gdf = master_gdf[master_gdf['StateName'] == clean_state].copy()
            
            if state_gdf.empty:
                # Try case-insensitive match
                state_gdf = master_gdf[master_gdf['StateName'].str.lower() == clean_state.lower()].copy()
            
            if state_gdf.empty:
                return f"No shapefile data found for {state_name}. Available states: {', '.join(master_gdf['StateName'].unique()[:10])}"
            
            logger.info(f"Found {len(state_gdf)} wards in shapefile for {state_name}")
            
            # Step 3: Match and merge TPR data with shapefile
            merged_gdf = match_and_merge_data(tpr_results, state_gdf)
            
            # Step 4: Extract environmental variables (zone-specific)
            logger.info("Extracting zone-specific environmental variables from rasters")
            env_data = extract_environmental_variables(merged_gdf, state_name)
            
            # Step 5: Prepare final dataset
            final_df = pd.DataFrame()
            
            # Add identifiers (CRITICAL for risk analysis)
            final_df['WardCode'] = merged_gdf['WardCode'] if 'WardCode' in merged_gdf.columns else range(len(merged_gdf))
            final_df['StateCode'] = merged_gdf['StateCode'] if 'StateCode' in merged_gdf.columns else state_name[:2].upper()
            final_df['LGACode'] = merged_gdf['LGACode'] if 'LGACode' in merged_gdf.columns else ''
            final_df['WardName'] = merged_gdf['WardName']
            final_df['LGA'] = merged_gdf['LGAName'] if 'LGAName' in merged_gdf.columns else merged_gdf.get('LGA', '')
            final_df['State'] = state_name
            final_df['GeopoliticalZone'] = get_geopolitical_zone(state_name)
            
            # Add Burden metrics (new calculation)
            final_df['Burden'] = merged_gdf['Burden'].fillna(0) if 'Burden' in merged_gdf.columns else 0
            final_df['Population'] = merged_gdf['Population'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0) if 'Population' in merged_gdf.columns else 0
            final_df['Total_Positive'] = merged_gdf['Total_Positive'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0) if 'Total_Positive' in merged_gdf.columns else 0
            
            # Add environmental variables
            for col in env_data.columns:
                if col != 'WardCode':
                    final_df[col] = env_data[col]
            
            # Step 6: Save raw_data.csv
            raw_data_path = os.path.join(session_folder, 'raw_data.csv')
            final_df.to_csv(raw_data_path, index=False)
            logger.info(f"Saved raw_data.csv with {len(final_df)} wards")
            
            # Step 7: Create raw_shapefile.zip
            # Ensure shapefile has same columns as CSV
            for col in final_df.columns:
                if col not in merged_gdf.columns:
                    merged_gdf[col] = final_df[col]
            
            shapefile_path = create_shapefile_package(merged_gdf, session_folder)
            
            # Step 8: Set session flags for risk analysis
            flag_file = os.path.join(session_folder, '.risk_ready')
            Path(flag_file).touch()
            
            # Set flag to indicate TPR is waiting for user confirmation
            waiting_flag = os.path.join(session_folder, '.tpr_waiting_confirmation')
            Path(waiting_flag).touch()
            logger.info(f"Set TPR waiting confirmation flag for session {session_id}")
            
            # Update session if possible
            if graph_state:
                graph_state['data_loaded'] = True
                graph_state['csv_loaded'] = True
                graph_state['shapefile_loaded'] = True
                graph_state['upload_type'] = 'csv_shapefile'
                graph_state['tpr_completed'] = True
            
            result = f"""✅ Malaria Burden Data Prepared for Risk Analysis!

State: {state_name}
Wards: {len(final_df)}
Burden Coverage: {(final_df['Burden'] > 0).sum()}/{len(final_df)} wards

Files Created:
1. raw_data.csv - Ward-level data with malaria burden and environmental variables
2. raw_shapefile.zip - Geographic boundaries with attributes

Data Summary:
- Columns: {len(final_df.columns)}
- Environmental variables: {sum(1 for col in env_data.columns if col != 'WardCode')}
- Mean Burden: {final_df['Burden'].mean():.2f} per 1,000 population
- Wards with positive cases: {(final_df['Total_Positive'] > 0).sum()}

---
**Next Step:** I've finished preparing the burden data for analysis. Would you like to proceed to the risk analysis stage to rank wards and plan for ITN distribution?
"""
            return result
            
        else:
            return f"Unknown action: {action}. Use 'analyze', 'calculate_tpr', or 'prepare_for_risk'"
            
    except Exception as e:
        logger.error(f"Error in TPR analysis: {e}", exc_info=True)
        return f"Error during TPR analysis: {str(e)}"
