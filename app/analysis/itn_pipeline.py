"""ITN Distribution Pipeline for ChatMRPT."""
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import os
import json
import re
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import plotly.graph_objects as go
from pandas.api.types import is_datetime64_any_dtype
from shapely.geometry import LineString, MultiLineString, Polygon, MultiPolygon
from app.data.population_data.itn_population_loader import get_population_loader
from app.utils.geospatial_levels import (
    apply_lga_highlight,
    collect_lga_options,
    dissolve_to_lga,
    normalize_lga_code,
)
from app.utils.lga_boundaries import (
    annotate_with_lga_names,
    get_reference_lga_geometries,
)
from app.utils.map_overlays import add_lga_boundary_overlay
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

logger = logging.getLogger(__name__)

def detect_state(data_handler) -> Optional[str]:
    """Detect state from available data sources using the population loader."""
    loader = get_population_loader()
    state_code_map = loader.get_state_code_map()

    # Debug: Log state code map
    logger.info(f"🔍 Population loader returned {len(state_code_map)} states")
    if len(state_code_map) > 0:
        logger.info(f"🔍 Sample state codes: {list(state_code_map.keys())[:5]}")
    else:
        logger.error("❌ Population loader returned empty state code map!")

    # Prepare helper lookups for normalization
    code_to_name = {code.upper(): name for code, name in state_code_map.items()}
    name_to_name = {str(name).strip().lower(): name for name in state_code_map.values()}

    # Debug: Log what's in code_to_name
    logger.info(f"🔍 code_to_name has {len(code_to_name)} entries")
    if 'AD' in code_to_name:
        logger.info(f"🔍 'AD' maps to: {code_to_name['AD']}")
    else:
        logger.warning(f"❌ 'AD' NOT found in code_to_name. Available codes: {list(code_to_name.keys())[:10]}")

    def _normalize_state(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        state_str = str(value).strip()
        if not state_str:
            return None

        upper_value = state_str.upper()
        if len(upper_value) == 2 and upper_value in code_to_name:
            return code_to_name[upper_value]

        lower_value = state_str.lower()
        if lower_value in name_to_name:
            return name_to_name[lower_value]

        if lower_value.endswith(" state"):
            trimmed = lower_value[:-6].strip()
            if trimmed in name_to_name:
                return name_to_name[trimmed]

        # Handle FCT/Abuja variations specifically
        if lower_value in {"fct", "abuja", "abuja fct", "federal capital territory"}:
            return code_to_name.get("FC", "Federal Capital Territory")

        return None

    # Check shapefile data first
    if hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None:
        # Check for State column
        if 'State' in data_handler.shapefile_data.columns:
            state = _normalize_state(data_handler.shapefile_data['State'].iloc[0])
            if state:
                return state
        # Check for StateCode column
        if 'StateCode' in data_handler.shapefile_data.columns:
            state = _normalize_state(data_handler.shapefile_data['StateCode'].iloc[0])
            if state:
                return state

    # Check CSV data
    if hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None:
        # Check for State column
        if 'State' in data_handler.csv_data.columns:
            state = _normalize_state(data_handler.csv_data['State'].iloc[0])
            if state:
                return state
        # Check for StateCode column
        if 'StateCode' in data_handler.csv_data.columns:
            state = _normalize_state(data_handler.csv_data['StateCode'].iloc[0])
            if state:
                return state

    # Check unified dataset as another fallback
    if hasattr(data_handler, 'unified_dataset') and data_handler.unified_dataset is not None:
        logger.info(f"🔍 Unified dataset available: {len(data_handler.unified_dataset)} rows")
        logger.info(f"🔍 Unified dataset columns: {list(data_handler.unified_dataset.columns)}")

        # Check for State column first - scan for first non-null value
        state_candidates = ['State', 'state', 'StateName', 'state_name']
        for col_name in state_candidates:
            if col_name in data_handler.unified_dataset.columns:
                # Get first non-null value instead of just first row
                non_null_values = data_handler.unified_dataset[col_name].dropna()
                if len(non_null_values) > 0:
                    raw_state = non_null_values.iloc[0]
                    logger.info(f"🔍 Found non-null value in '{col_name}': '{raw_state}'")
                    state = _normalize_state(raw_state)
                    if state:
                        logger.info(f"✅ Detected state from unified dataset ({col_name} column): {state}")
                        return state
                    else:
                        logger.warning(f"❌ State normalization failed for value: '{raw_state}'")
                else:
                    logger.warning(f"❌ '{col_name}' column exists but all values are null")

        # Check for StateCode column - scan for first non-null value
        statecode_candidates = ['StateCode', 'state_code', 'STATECODE']
        for col_name in statecode_candidates:
            if col_name in data_handler.unified_dataset.columns:
                # Get first non-null value instead of just first row
                non_null_values = data_handler.unified_dataset[col_name].dropna()
                if len(non_null_values) > 0:
                    raw_code = non_null_values.iloc[0]
                    logger.info(f"🔍 Found non-null value in '{col_name}': '{raw_code}'")
                    state = _normalize_state(raw_code)
                    if state:
                        logger.info(f"✅ Detected state from unified dataset ({col_name} column): {state}")
                        return state
                    else:
                        logger.warning(f"❌ StateCode normalization failed for value: '{raw_code}'")
                else:
                    logger.warning(f"❌ '{col_name}' column exists but all values are null")
    else:
        logger.warning("❌ Unified dataset not available in data_handler")

    # Try to detect from session or file paths
    try:
        from flask import session
        if 'state_name' in session:
            state = _normalize_state(session['state_name'])
            if state:
                logger.info(f"Detected state from session: {state}")
                return state
    except:
        pass

    # Log error - state detection failed
    available_states = ', '.join(loader.get_available_states())
    logger.error(
        "Could not detect state from data. Available states: %s",
        available_states or 'None',
    )
    return None  # Return None to indicate detection failure

def load_population_data(state: str) -> Optional[pd.DataFrame]:
    """Load and aggregate population data for the detected state."""
    loader = get_population_loader()

    pop_df = loader.load_state_population(state)
    if pop_df is not None:
        logger.info(
            "Using unified national population dataset for %s (%s wards, total pop %s)",
            state,
            len(pop_df),
            f"{pop_df['Population'].sum():,.0f}",
        )
        return pop_df.copy()

    # Fall back to legacy per-state files if the unified dataset is unavailable
    logger.info(f"Unified dataset not available for {state}, attempting legacy population files")

    xlsx_path = f'app/data/population_data/pbi_distribution_{state}.xlsx'
    csv_path = f'app/data/population_data/pbi_distribution_{state}.csv'

    pop_data = None
    if os.path.exists(xlsx_path):
        pop_data = pd.read_excel(xlsx_path)
    elif os.path.exists(csv_path):
        try:
            pop_data = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            for encoding in ['latin-1', 'cp1252']:
                try:
                    pop_data = pd.read_csv(csv_path, encoding=encoding)
                    break
                except Exception:
                    continue
    else:
        available_states = loader.get_available_states()
        if available_states:
            logger.warning(
                "No population data for %s. Available states: %s",
                state,
                ', '.join(available_states),
            )
        else:
            logger.warning(f"No population data files found for {state}")
        return None

    if pop_data is None:
        logger.warning(f"Could not load legacy population data for {state}")
        return None

    ward_population = pop_data.groupby(['AdminLevel3', 'AdminLevel2']).agg({
        'N_FamilyMembers': 'sum',
        'AvgLatitude': 'mean',
        'AvgLongitude': 'mean'
    }).reset_index()
    ward_population.columns = ['WardName', 'AdminLevel2', 'Population', 'AvgLatitude', 'AvgLongitude']
    ward_population['WardName_lower'] = ward_population['WardName'].str.lower()

    duplicate_mask = ward_population.duplicated(subset=['WardName'], keep=False)
    duplicate_wards = ward_population[duplicate_mask]
    unique_wards = ward_population[~duplicate_mask]

    if len(duplicate_wards) > 0:
        logger.info(f"Found {len(duplicate_wards)} duplicate ward entries across LGAs in population data")
        dup_ward_names = duplicate_wards['WardName'].unique()
        logger.info(f"Duplicate ward names: {list(dup_ward_names)[:10]}")

    logger.info(f"Loaded population data for {len(ward_population)} ward-LGA combinations in {state}")
    logger.info(f"  Unique wards: {len(unique_wards)}, Duplicate entries: {len(duplicate_wards)}")

    return ward_population

def normalize_ward_name(ward_name: str) -> str:
    """
    Normalize ward name for better matching.
    
    Args:
        ward_name: Original ward name
        
    Returns:
        Normalized ward name
    """
    if pd.isna(ward_name):
        return ""
        
    # Convert to lowercase
    normalized = str(ward_name).lower().strip()
    
    # Remove content in parentheses
    normalized = normalized.split('(')[0].strip()
    
    # Replace roman numerals with numbers (order matters!)
    # Using regex to match word boundaries
    roman_replacements = [
        (r'\bviii\b', '8'), (r'\bvii\b', '7'), (r'\bvi\b', '6'), 
        (r'\biv\b', '4'), (r'\biii\b', '3'), (r'\bii\b', '2'), 
        (r'\bix\b', '9'), (r'\bv\b', '5'), (r'\bi\b', '1')
    ]
    for pattern, replacement in roman_replacements:
        normalized = re.sub(pattern, replacement, normalized)
    
    # Remove common suffixes
    suffixes = [' ward', ' wards']
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
    
    # Replace common separators with space
    normalized = normalized.replace('/', ' ').replace('-', ' ').replace('_', ' ')
    
    # Remove extra spaces
    normalized = ' '.join(normalized.split())
    
    return normalized

def fuzzy_match_ward_names(analysis_wards: List[str], population_wards: List[str], 
                          threshold: int = 70) -> Dict[str, Tuple[str, int]]:
    """
    Perform fuzzy matching between analysis ward names and population ward names.
    
    Args:
        analysis_wards: List of ward names from analysis data
        population_wards: List of ward names from population data
        threshold: Minimum matching score (0-100)
        
    Returns:
        Dictionary mapping analysis ward names to (matched_population_ward, score)
    """
    matches = {}
    unmatched = []
    
    # Normalize all ward names
    pop_ward_dict = {normalize_ward_name(w): w for w in population_wards}
    pop_normalized = list(pop_ward_dict.keys())
    
    for ward in analysis_wards:
        normalized_ward = normalize_ward_name(ward)
        
        # First try exact match
        if normalized_ward in pop_ward_dict:
            matches[ward] = (pop_ward_dict[normalized_ward], 100)
            continue
        
        # Try fuzzy matching
        best_match = process.extractOne(normalized_ward, pop_normalized, 
                                       scorer=fuzz.token_sort_ratio)
        
        if best_match and best_match[1] >= threshold:
            matched_original = pop_ward_dict[best_match[0]]
            matches[ward] = (matched_original, best_match[1])
        else:
            # Try partial ratio for substring matches
            best_partial = process.extractOne(normalized_ward, pop_normalized, 
                                            scorer=fuzz.partial_ratio)
            if best_partial and best_partial[1] >= 90:  # Higher threshold for partial matches
                matched_original = pop_ward_dict[best_partial[0]]
                matches[ward] = (matched_original, best_partial[1])
            else:
                unmatched.append(ward)
    
    if unmatched:
        logger.warning(f"Could not match {len(unmatched)} wards: {unmatched[:5]}...")
    
    return matches

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points on Earth (in km)."""
    from math import radians, cos, sin, asin, sqrt
    
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

def calculate_nets_needed(population: float, avg_household_size: float) -> int:
    """Calculate nets needed: 1 per 1.8 people, min 1 per household."""
    households = np.ceil(population / avg_household_size)
    return int(np.ceil(max(population / 1.8, households)))

def calculate_itn_distribution(data_handler, session_id: str, total_nets: int = 10000, avg_household_size: float = 5.0, urban_threshold: float = 75.0, method: str = 'composite') -> Dict[str, Any]:
    """Perform two-phase ITN distribution calculation."""
    state = detect_state(data_handler)
    
    if state is None:
        return {
            'status': 'error', 
            'message': 'Could not detect state from the data. Please ensure your data includes state information (State or StateCode column).'
        }
    
    logger.info(f"Detected state: {state}")
    
    pop_data = load_population_data(state)
    
    # Use unified dataset if available - it has all the data we need
    if hasattr(data_handler, 'unified_dataset') and data_handler.unified_dataset is not None:
        logger.info("Using unified dataset for ITN planning")
        # Get relevant columns from unified dataset
        if method == 'composite':
            rank_col = 'composite_rank'
            score_col = 'composite_score'
            category_col = 'composite_category'
        else:
            rank_col = 'pca_rank'
            score_col = 'pca_score' 
            category_col = 'pca_category'
        
        # Extract rankings with all necessary data from unified dataset
        required_cols = ['WardName', score_col, rank_col, category_col]

        # CRITICAL: Include WardCode if available - needed for population merging later
        if 'WardCode' in data_handler.unified_dataset.columns:
            required_cols.append('WardCode')
            logger.info("🔍 Including WardCode in rankings for population merge")
        else:
            logger.warning("⚠️ WardCode not found in unified dataset - will fall back to fuzzy matching")

        # ✅ ROBUST URBAN PERCENTAGE DETECTION
        # CRITICAL: 'urban_percentage' is the actual column name from database/TPR analysis
        urban_col_candidates = [
            'urban_percentage',  # PRIMARY - from database extraction
            'urbanPercentage', 'UrbanPercent',
            'urbanPercent', 'urban_percent', 'Urban_Percent',
            'Urban_Percentage', 'URBANPERCENT', 'UrbanPerce'
        ]

        urban_col_found = None
        for col in urban_col_candidates:
            if col in data_handler.unified_dataset.columns:
                required_cols.append(col)
                urban_col_found = col
                logger.info(f"✅ Found urban percentage column: {col}")
                break

        if not urban_col_found:
            logger.warning(f"⚠️ No urban percentage column found. Searched for: {urban_col_candidates}")
            logger.warning(f"📋 Available columns: {list(data_handler.unified_dataset.columns)[:20]}")
            # Check if any column contains 'urban' in name
            urban_like_cols = [c for c in data_handler.unified_dataset.columns if 'urban' in c.lower()]
            if urban_like_cols:
                logger.info(f"🔍 Found urban-like columns: {urban_like_cols}")
                # Use the first one that looks like a percentage
                for col in urban_like_cols:
                    if any(term in col.lower() for term in ['percent', 'pct', '%']):
                        required_cols.append(col)
                        urban_col_found = col
                        logger.info(f"✅ Using urban-like column: {col}")
                        break

        rankings = data_handler.unified_dataset[required_cols].copy()

        # Standardize column names
        rename_dict = {
            score_col: 'score',
            rank_col: 'overall_rank',
            category_col: 'vulnerability_category'
        }

        # ✅ STANDARDIZE URBAN PERCENTAGE COLUMN NAME
        if urban_col_found:
            rename_dict[urban_col_found] = 'urban_pct'
            logger.info(f"✅ Standardized urban column '{urban_col_found}' → 'urban_pct'")

        rankings = rankings.rename(columns=rename_dict)
        
        # Fix dtype mismatch - ensure numeric columns are numeric
        rankings['score'] = pd.to_numeric(rankings['score'], errors='coerce')
        rankings['overall_rank'] = pd.to_numeric(rankings['overall_rank'], errors='coerce')

        # ✅ ENSURE URBAN PERCENTAGE IS NUMERIC
        if 'urban_pct' in rankings.columns:
            rankings['urban_pct'] = pd.to_numeric(rankings['urban_pct'], errors='coerce')
            non_null_count = rankings['urban_pct'].notna().sum()
            logger.info(f"✅ Urban percentage: {non_null_count}/{len(rankings)} wards have valid values")
            if non_null_count > 0:
                logger.info(f"📊 Urban % range: {rankings['urban_pct'].min():.1f}% to {rankings['urban_pct'].max():.1f}%")
        else:
            logger.warning("⚠️ No urban_pct column after standardization - will use default values")

        rankings = rankings.dropna(subset=['score'])  # Drop any rows that became NaN
    else:
        # Fall back to original approach
        if method == 'composite':
            rankings = data_handler.vulnerability_rankings.copy()
        else:
            rankings = data_handler.vulnerability_rankings_pca.copy()
    
    shp_data = data_handler.shapefile_data
    
    # Merge population if available
    if pop_data is not None:
        ranking_ward_names = rankings['WardName'].unique().tolist()
        pop_ward_names = pop_data['WardName'].unique().tolist()

        # Prefer WardCode-based merging when codes are available in both datasets
        wardcode_available = 'WardCode' in rankings.columns and 'WardCode' in pop_data.columns
        pop_lookup = pop_data.set_index('WardCode') if wardcode_available else None

        if wardcode_available:
            logger.info("Merging population data using WardCode matches")

            rankings['Population'] = rankings['WardCode'].map(pop_lookup['Population'])
            rankings['PopWardName'] = rankings['WardCode'].map(pop_lookup['WardName'])
            if 'AdminLevel2' in pop_lookup.columns:
                rankings['AdminLevel2'] = rankings['WardCode'].map(pop_lookup['AdminLevel2'])

            matched_mask = rankings['Population'].notna()
            matched_count = matched_mask.sum()
            unmatched_codes = rankings.loc[~matched_mask, 'WardCode'].dropna().unique().tolist()

            match_df = pd.DataFrame({
                'WardName': rankings.loc[matched_mask, 'WardName'],
                'WardCode': rankings.loc[matched_mask, 'WardCode'],
                'PopWardName': rankings.loc[matched_mask, 'PopWardName'],
                'MatchScore': 100,
                'MatchMethod': 'WardCode'
            })

            logger.info(f"Matched population data for {matched_count} out of {len(rankings)} wards via WardCode")
            if unmatched_codes:
                logger.warning(f"Wards without population match (WardCode): {unmatched_codes[:10]}")
        else:
            logger.info(
                "Starting fuzzy matching between %d ranking wards and %d population wards",
                len(rankings),
                len(pop_data)
            )

            matches = fuzzy_match_ward_names(ranking_ward_names, pop_ward_names, threshold=70)
            match_df = pd.DataFrame([
                {'WardName': analysis_ward, 'PopWardName': pop_ward, 'MatchScore': score}
                for analysis_ward, (pop_ward, score) in matches.items()
            ])

            unmatched_wards = [w for w in ranking_ward_names if w not in matches]
            if unmatched_wards:
                logger.warning(f"Unmatched wards ({len(unmatched_wards)}): {unmatched_wards[:10]}")

            if len(match_df) > 0:
                avg_score = match_df['MatchScore'].mean()
                logger.info(f"Average match score: {avg_score:.1f}")

                examples = match_df.nlargest(5, 'MatchScore').head(3)
                for _, row in examples.iterrows():
                    logger.info(f"  ✓ '{row['WardName']}' -> '{row['PopWardName']}' (score: {row['MatchScore']})")

                poor_matches = match_df[match_df['MatchScore'] < 80]
                if len(poor_matches) > 0:
                    logger.warning(f"Poor matches ({len(poor_matches)} with score < 80):")
                    for _, row in poor_matches.head(3).iterrows():
                        logger.warning(f"  ? '{row['WardName']}' -> '{row['PopWardName']}' (score: {row['MatchScore']})")

            rankings = rankings.merge(match_df, on='WardName', how='left')
            pop_data_renamed = pop_data.rename(columns={'WardName': 'PopWardName'})
            rankings = rankings.merge(
                pop_data_renamed[['PopWardName', 'Population']],
                on='PopWardName',
                how='left'
            )

            matched_count = rankings['Population'].notna().sum()
            logger.info(f"Matched population data for {matched_count} out of {len(rankings)} wards via fuzzy matching")

        # Generate matching report
        if wardcode_available:
            unmatched_names = rankings.loc[rankings['Population'].isna(), 'WardName'].tolist()
            matching_report = {
                'total_ranking_wards': len(ranking_ward_names),
                'total_population_wards': len(pop_ward_names),
                'matched_wards': matched_count,
                'unmatched_wards': len(unmatched_names),
                'match_percentage': (matched_count / len(ranking_ward_names) * 100) if len(ranking_ward_names) > 0 else 0,
                'average_match_score': 100 if matched_count else 0,
                'match_method': 'WardCode',
                'unmatched_ward_names': unmatched_names[:20],
                'timestamp': datetime.now().isoformat()
            }
        else:
            matching_report = {
                'total_ranking_wards': len(ranking_ward_names),
                'total_population_wards': len(pop_ward_names),
                'matched_wards': len(match_df),
                'unmatched_wards': len(ranking_ward_names) - len(match_df),
                'match_percentage': (len(match_df) / len(ranking_ward_names) * 100) if len(ranking_ward_names) > 0 else 0,
                'average_match_score': match_df['MatchScore'].mean() if len(match_df) > 0 else 0,
                'match_method': 'Fuzzy',
                'unmatched_ward_names': [w for w in ranking_ward_names if w not in match_df['WardName'].tolist()][:20],
                'timestamp': datetime.now().isoformat()
            }

        # Save matching report
        try:
            report_path = f"instance/uploads/{session_id}/ward_matching_report.json"
            with open(report_path, 'w') as f:
                json.dump(matching_report, f, indent=2, default=str)
            logger.info(f"Saved ward matching report to {report_path}")
        except Exception as e:
            logger.warning(f"Could not save matching report: {e}")

        # If no matches, try to understand why
        if matched_count == 0:
            logger.warning(f"No matches found. Checking for common ward names...")
            rankings_wards = set(rankings['WardName'].str.lower())
            pop_wards = set(pop_data['WardName'].str.lower())
            common_wards = rankings_wards.intersection(pop_wards)
            logger.warning(f"Common ward names: {len(common_wards)} - {list(common_wards)[:5]}")
        
        # Don't remove wards without population data - mark them instead
        # Calculate a default population based on average if missing
        avg_population = rankings['Population'].dropna().mean() if rankings['Population'].notna().any() else 10000
        
        # Log wards without population data
        no_pop_wards = rankings[rankings['Population'].isna()]
        if len(no_pop_wards) > 0:
            logger.warning(f"Found {len(no_pop_wards)} wards without population data")
            logger.info(f"Using estimated population of {avg_population:.0f} for wards without data")
            # Fill missing population with average
            rankings['Population'] = rankings['Population'].fillna(avg_population)
            rankings['has_population_data'] = rankings['Population'].notna()
        
        # Don't fail if no population matches - continue with estimates
        if rankings['Population'].isna().all():
            logger.warning("No population data matched - using default estimates")
            rankings['Population'] = avg_population
            rankings['has_population_data'] = False
    else:
        # Get list of available states
        loader = get_population_loader()
        available_states = loader.get_available_states()
        if available_states:
            states_list = ', '.join(available_states)
            return {'status': 'error', 'message': f'Population data not available for {state}. Available states with population data: {states_list}'}
        else:
            return {'status': 'error', 'message': f'Population data not available for {state}. No population data files found in the system.'}
    
    # ✅ TWO-TIER ALLOCATION: Rural wards get priority, then urban wards
    # Urban percentage should already be standardized to 'urban_pct' from earlier processing
    if 'urban_pct' not in rankings.columns:
        logger.warning(f"⚠️ No urban_pct column found after standardization. Available columns: {rankings.columns.tolist()}")
        # Try to find urban percentage column again as last resort
        urban_col_candidates = ['UrbanPercent', 'urbanPercentage', 'urban_percentage', 'urbanPercent']
        urban_col = None
        for col in urban_col_candidates:
            if col in rankings.columns:
                urban_col = col
                rankings['urban_pct'] = pd.to_numeric(rankings[col], errors='coerce').fillna(50.0)
                logger.info(f"✅ Found and standardized {col} → urban_pct")
                break

        if urban_col is None:
            # No urban percentage found - use default
            logger.warning("⚠️ No urban percentage column found - using default 50% for all wards")
            rankings['urban_pct'] = 50.0

    # Ensure urban_pct is numeric and has no nulls
    rankings['urban_pct'] = pd.to_numeric(rankings['urban_pct'], errors='coerce').fillna(50.0)
    urban_col = 'urban_pct'
    
    # ✅ TWO-TIER ALLOCATION SYSTEM
    # Calculate nets needed for each ward
    rankings['nets_needed'] = rankings['Population'].apply(lambda p: calculate_nets_needed(p, avg_household_size))

    # Split wards into two tiers based on urban threshold
    rural_wards = rankings[rankings[urban_col] < urban_threshold].copy()
    urban_wards = rankings[rankings[urban_col] >= urban_threshold].copy()

    # Sort each tier by vulnerability ranking (highest risk first)
    rural_wards = rural_wards.sort_values('overall_rank')
    urban_wards = urban_wards.sort_values('overall_rank')

    logger.info(f"📊 Two-tier split: {len(rural_wards)} rural wards (urban% < {urban_threshold}%), {len(urban_wards)} urban wards (urban% >= {urban_threshold}%)")

    allocated = 0
    wards_with_allocation = []

    # ===== TIER 1: RURAL PRIORITY (urban% < threshold) =====
    logger.info(f"🌾 TIER 1: Allocating to {len(rural_wards)} rural wards (Priority)")
    for idx, row in rural_wards.iterrows():
        nets_for_this_ward = row['nets_needed']

        if allocated + nets_for_this_ward <= total_nets:
            # Full coverage
            row_copy = row.copy()
            row_copy['nets_allocated'] = nets_for_this_ward
            row_copy['coverage_percent'] = 100.0
            row_copy['allocation_phase'] = 'Rural Priority (Full)'
            row_copy['priority_tier'] = 1
            wards_with_allocation.append(row_copy)
            allocated += nets_for_this_ward
            logger.info(f"  ✓ {row['WardName']}: {nets_for_this_ward} nets (rank {row['overall_rank']}, urban% {row[urban_col]:.1f}%)")
        else:
            # Partial coverage for last rural ward
            remaining = total_nets - allocated
            if remaining > 0:
                row_copy = row.copy()
                row_copy['nets_allocated'] = remaining
                row_copy['coverage_percent'] = (remaining / nets_for_this_ward) * 100
                row_copy['allocation_phase'] = 'Rural Priority (Partial)'
                row_copy['priority_tier'] = 1
                wards_with_allocation.append(row_copy)
                allocated = total_nets
                logger.info(f"  ⚠️ {row['WardName']}: {remaining} nets ({row_copy['coverage_percent']:.1f}% coverage) - NETS EXHAUSTED")
            break

    # ===== TIER 2: URBAN SURPLUS (urban% >= threshold) =====
    if allocated < total_nets and len(urban_wards) > 0:
        remaining_nets = total_nets - allocated
        logger.info(f"🏙️ TIER 2: {remaining_nets} nets remaining for {len(urban_wards)} urban wards (Surplus)")

        for idx, row in urban_wards.iterrows():
            nets_for_this_ward = row['nets_needed']

            if allocated + nets_for_this_ward <= total_nets:
                # Full coverage
                row_copy = row.copy()
                row_copy['nets_allocated'] = nets_for_this_ward
                row_copy['coverage_percent'] = 100.0
                row_copy['allocation_phase'] = 'Urban Surplus (Full)'
                row_copy['priority_tier'] = 2
                wards_with_allocation.append(row_copy)
                allocated += nets_for_this_ward
                logger.info(f"  ✓ {row['WardName']}: {nets_for_this_ward} nets (rank {row['overall_rank']}, urban% {row[urban_col]:.1f}%)")
            else:
                # Partial coverage for last urban ward
                remaining = total_nets - allocated
                if remaining > 0:
                    row_copy = row.copy()
                    row_copy['nets_allocated'] = remaining
                    row_copy['coverage_percent'] = (remaining / nets_for_this_ward) * 100
                    row_copy['allocation_phase'] = 'Urban Surplus (Partial)'
                    row_copy['priority_tier'] = 2
                    wards_with_allocation.append(row_copy)
                    allocated = total_nets
                    logger.info(f"  ⚠️ {row['WardName']}: {remaining} nets ({row_copy['coverage_percent']:.1f}% coverage) - NETS EXHAUSTED")
                break
    else:
        if len(urban_wards) > 0:
            logger.info(f"❌ NO NETS REMAINING for {len(urban_wards)} urban wards")
    
    # Create the prioritized dataframe from wards that got allocations
    if wards_with_allocation:
        prioritized = pd.DataFrame(wards_with_allocation)
        # Separate rural vs urban for stats
        prioritized_rural = prioritized[prioritized[urban_col] < urban_threshold]
        prioritized_urban = prioritized[prioritized[urban_col] >= urban_threshold]
    else:
        prioritized = pd.DataFrame()
        prioritized_rural = pd.DataFrame()
        prioritized_urban = pd.DataFrame()
    
    # No reprioritized phase in full coverage strategy
    reprioritized = pd.DataFrame()
    
    # Calculate total allocated nets
    total_allocated = prioritized['nets_allocated'].sum()
    if not reprioritized.empty:
        total_allocated += reprioritized['nets_allocated'].sum()
    
    # Calculate population coverage
    total_population = rankings['Population'].sum()
    prioritized['population_covered'] = prioritized['nets_allocated'] * 1.8  # 1 net covers 1.8 people
    covered_population = prioritized['population_covered'].sum()
    
    if not reprioritized.empty:
        reprioritized['population_covered'] = reprioritized['nets_allocated'] * 1.8
        covered_population += reprioritized['population_covered'].sum()
    
    # Convert any datetime columns to strings to avoid JSON serialization issues
    for df in [prioritized, reprioritized]:
        if not df.empty:
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].astype(str)
    
    # Stats - updated for full coverage strategy
    fully_covered_wards = len(prioritized[prioritized['coverage_percent'] == 100.0]) if not prioritized.empty else 0
    partially_covered_wards = len(prioritized[prioritized['coverage_percent'] < 100.0]) if not prioritized.empty else 0
    
    # Add ward coverage statistics
    ward_coverage_stats = {}
    if not prioritized.empty:
        ward_coverage_stats = {
            'avg_coverage_percent': round(prioritized['coverage_percent'].mean(), 1),
            'min_coverage_percent': round(prioritized['coverage_percent'].min(), 1),
            'max_coverage_percent': round(prioritized['coverage_percent'].max(), 1)
        }
    
    stats = {
        'total_nets': total_nets,
        'allocated': int(total_allocated),
        'remaining': int(total_nets - total_allocated),
        'coverage_percent': round((covered_population / total_population) * 100, 1) if total_population > 0 else 0,
        'prioritized_wards': len(prioritized),
        'fully_covered_wards': fully_covered_wards,
        'partially_covered_wards': partially_covered_wards,
        'reprioritized_wards': 0,  # No longer used in full coverage strategy
        'total_population': int(total_population),
        'covered_population': int(covered_population),
        'ward_coverage_stats': ward_coverage_stats
    }
    
    # Generate map
    map_path = generate_itn_map(shp_data, prioritized, reprioritized, rankings,
                               session_id=session_id, urban_threshold=urban_threshold,
                               total_nets=total_nets, avg_household_size=avg_household_size,
                               method=method, stats=stats)
    
    # Save results for export (modular addition - doesn't affect existing functionality)
    try:
        results_to_save = {
            'status': 'success',
            'stats': stats,
            'method': method,
            'urban_threshold': urban_threshold,
            'avg_household_size': avg_household_size,
            'total_nets': total_nets,
            'total_allocated': int(total_allocated),
            'coverage_percentage': stats['coverage_percent'],
            'timestamp': datetime.now().isoformat(),
            'prioritized': prioritized.to_dict('records'),
            'reprioritized': reprioritized.to_dict('records') if not reprioritized.empty else [],
            'distribution': prioritized.to_dict('records'),  # Add distribution for backward compatibility
            'map_path': map_path
        }
        
        # Save to session folder
        results_path = f"instance/uploads/{session_id}/itn_distribution_results.json"
        with open(results_path, 'w') as f:
            json.dump(results_to_save, f, indent=2, default=str)
        logger.info(f"Saved ITN distribution results to {results_path}")
        
        # Store ITN parameters in Redis for multi-worker access
        try:
            from ..core.redis_state_manager import get_redis_state_manager
            redis_manager = get_redis_state_manager()
            itn_params = {
                'total_nets': total_nets,
                'avg_household_size': avg_household_size,
                'urban_threshold': urban_threshold,
                'method': method,
                'timestamp': datetime.now().isoformat()
            }
            redis_manager.set_custom_data(session_id, 'itn_parameters', itn_params)
            logger.info(f"Stored ITN parameters in Redis for session {session_id}")
        except Exception as redis_err:
            logger.warning(f"Could not store ITN parameters in Redis: {redis_err}")
            # Fall back to file-based storage
            params_path = f"instance/uploads/{session_id}/itn_parameters.json"
            with open(params_path, 'w') as f:
                json.dump(itn_params, f, indent=2)
            logger.info(f"Stored ITN parameters in file: {params_path}")
            
    except Exception as e:
        logger.warning(f"Failed to save ITN results for export: {e}")
        # Don't fail the main function if saving fails
    
    return {
        'status': 'success',
        'stats': stats,
        'prioritized': prioritized,
        'reprioritized': reprioritized,
        'map_path': map_path
    }

def generate_itn_map(
    shp_data: gpd.GeoDataFrame,
    prioritized: pd.DataFrame,
    reprioritized: pd.DataFrame,
    rankings: pd.DataFrame,
    session_id: str,
    urban_threshold: float = 75.0,
    total_nets: int = 10000,
    avg_household_size: float = 5.0,
    method: str = 'composite',
    stats: Dict[str, Any] = None,
    geographic_level: str = 'ward',
    selected_lgas: Optional[List[str]] = None,
) -> str:
    """Generate interactive Plotly map for ITN distribution with threshold info."""
    # Ensure we have the visualization directory
    os.makedirs('app/static/visualizations', exist_ok=True)
    
    # Merge allocation data with shapefile for visualization - deep copy to avoid modifying original
    shp_data = shp_data.copy(deep=True)

    # Ensure shp_data is a proper GeoDataFrame
    if not isinstance(shp_data, gpd.GeoDataFrame):
        logger.error("shp_data is not a GeoDataFrame!")
        return None

    try:
        shp_data = annotate_with_lga_names(shp_data)
    except Exception as exc:
        logger.warning(f"Failed to annotate LGA names on shapefile: {exc}")
    
    # Add lowercase column for merging
    shp_data['WardName_lower'] = shp_data['WardName'].str.lower()
    prioritized['WardName_lower'] = prioritized['WardName'].str.lower()

    # ✅ CRITICAL FIX: First merge rankings data (has ALL wards) to ensure all wards get urban_pct and Population
    # This ensures grey areas (unallocated wards) show their actual values
    if rankings is not None and not rankings.empty:
        logger.info(f"🔍 Merging rankings data for ALL {len(rankings)} wards to ensure complete data")
        rankings['WardName_lower'] = rankings['WardName'].str.lower()

        # Select essential columns from rankings
        rankings_merge_cols = ['WardName_lower']
        if 'urban_pct' in rankings.columns:
            rankings_merge_cols.append('urban_pct')
        if 'Population' in rankings.columns:
            rankings_merge_cols.append('Population')

        # Merge rankings data for ALL wards
        shp_data = shp_data.merge(
            rankings[rankings_merge_cols],
            on='WardName_lower',
            how='left',
            suffixes=('', '_from_rankings')
        )

        # Use rankings data as the base for urban_pct and Population
        if 'urban_pct_from_rankings' in shp_data.columns:
            shp_data['urban_pct'] = shp_data['urban_pct_from_rankings']
            logger.info(f"✅ Set urban_pct from rankings for {shp_data['urban_pct'].notna().sum()} wards")
        if 'Population_from_rankings' in shp_data.columns:
            shp_data['Population'] = shp_data['Population_from_rankings']
            logger.info(f"✅ Set Population from rankings for {shp_data['Population'].notna().sum()} wards")

    # Merge prioritized allocations - GeoDataFrame.merge preserves geometry automatically
    # Include priority_tier and urban_pct for display
    merge_cols = ['WardName_lower', 'nets_allocated', 'nets_needed']
    # Don't re-merge Population if we already got it from rankings
    if 'Population_from_rankings' not in shp_data.columns:
        merge_cols.append('Population')
    if 'priority_tier' in prioritized.columns:
        merge_cols.append('priority_tier')

    shp_data = shp_data.merge(
        prioritized[merge_cols],
        on='WardName_lower',
        how='left',
        suffixes=('', '_prioritized')
    )
    
    # Track allocation phase for hover text - using new full coverage strategy
    shp_data['allocation_phase'] = ''
    
    # Get allocation phase and coverage info from prioritized data
    if not prioritized.empty and 'allocation_phase' in prioritized.columns:
        # Merge with allocation phase and coverage percent from prioritized
        phase_cols = ['WardName_lower', 'allocation_phase']
        if 'coverage_percent' in prioritized.columns:
            phase_cols.append('coverage_percent')
        
        phase_data = prioritized[phase_cols].copy()
        shp_data = shp_data.merge(
            phase_data,
            on='WardName_lower',
            how='left',
            suffixes=('', '_from_prioritized')
        )
        
        # Use the allocation phase from prioritized data
        if 'allocation_phase_from_prioritized' in shp_data.columns:
            shp_data['allocation_phase'] = shp_data['allocation_phase_from_prioritized'].fillna('')
            shp_data = shp_data.drop(columns=['allocation_phase_from_prioritized'])
        
        # Use coverage percent from prioritized if available
        if 'coverage_percent_from_prioritized' in shp_data.columns:
            shp_data['coverage_percent'] = shp_data['coverage_percent_from_prioritized'].fillna(0)
            shp_data = shp_data.drop(columns=['coverage_percent_from_prioritized'])
    else:
        # Fallback logic
        shp_data.loc[shp_data['WardName_lower'].isin(prioritized['WardName_lower']), 'allocation_phase'] = 'Allocated'
    
    # Mark unallocated wards
    shp_data.loc[(shp_data['allocation_phase'] == '') & (shp_data['nets_allocated'] == 0), 'allocation_phase'] = 'Not Allocated'
    
    # Fill NaN values
    shp_data['nets_allocated'] = shp_data['nets_allocated'].fillna(0)
    
    # Only calculate coverage_percent if not already set from prioritized data
    if 'coverage_percent' not in shp_data.columns or shp_data['coverage_percent'].isna().all():
        shp_data['coverage_percent'] = (shp_data['nets_allocated'] * 1.8 / shp_data['Population'] * 100).fillna(0)
        shp_data['coverage_percent'] = shp_data['coverage_percent'].clip(upper=100)
    else:
        # For wards without coverage_percent, calculate it
        no_coverage_mask = shp_data['coverage_percent'].isna() | (shp_data['coverage_percent'] == 0)
        shp_data.loc[no_coverage_mask, 'coverage_percent'] = (
            shp_data.loc[no_coverage_mask, 'nets_allocated'] * 1.8 / 
            shp_data.loc[no_coverage_mask, 'Population'] * 100
        ).fillna(0).clip(upper=100)
    
    # Add urban percentage info for hover text
    # CRITICAL: We already merged urban_pct from rankings (which has ALL wards) - use it directly
    if 'urban_pct' in shp_data.columns:
        # We have urban_pct from rankings merge (includes ALL wards, both allocated and unallocated)
        shp_data['urban_pct_display'] = shp_data['urban_pct']
        valid_count = shp_data['urban_pct'].notna().sum()
        logger.info(f"✅ Using urban_pct from rankings for {valid_count} wards (includes unallocated wards)")
    elif 'urban_pct_prioritized' in shp_data.columns:
        # Handle the case where merge added _prioritized suffix
        shp_data['urban_pct_display'] = shp_data['urban_pct_prioritized']
        logger.info(f"✅ Using urban_pct_prioritized from merge for {shp_data['urban_pct_prioritized'].notna().sum()} wards")
    elif 'UrbanPercent' in shp_data.columns:
        # Fallback to original shapefile column
        shp_data['urban_pct_display'] = shp_data['UrbanPercent']
        logger.info(f"✅ Using UrbanPercent from shapefile for {shp_data['UrbanPercent'].notna().sum()} wards")
    elif 'urbanPercentage' in shp_data.columns:
        # Another fallback
        shp_data['urban_pct_display'] = shp_data['urbanPercentage']
        logger.info(f"✅ Using urbanPercentage from shapefile for {shp_data['urbanPercentage'].notna().sum()} wards")
    else:
        # Last resort - fill with 0
        logger.warning(f"⚠️ No urban percentage column found in shp_data. Available columns: {shp_data.columns.tolist()}")
        shp_data['urban_pct_display'] = 0.0
    
    # Ensure we have valid geometry data
    if 'geometry' not in shp_data.columns:
        logger.error("No geometry column found in shapefile data after merging!")
        return None
    
    # Remove any rows with invalid geometry
    shp_data = shp_data[shp_data['geometry'].notna()]
    if len(shp_data) == 0:
        logger.error("No valid geometries found in shapefile data!")
        return None
    
    # Determine available LGA metadata for overlays
    requested_level = (geographic_level or 'ward').lower()
    if requested_level not in {'ward', 'lga'}:
        requested_level = 'ward'
    available_lgas = collect_lga_options(shp_data)
    lga_label_map = {
        normalize_lga_code(item['code']): item['label']
        for item in available_lgas
        if item.get('code') and item.get('label')
    }
    normalized_selected_lgas = _normalize_selected_lgas(selected_lgas, available_lgas)

    # Get map center from shapefile bounds
    bounds = shp_data.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    # Early exit for requested LGA-level visualization
    if requested_level == 'lga':
        lga_fig = _build_lga_allocation_figure(
            shp_data.copy(deep=True),
            center_lat=center_lat,
            center_lon=center_lon,
            stats=stats,
            selected_lgas=normalized_selected_lgas,
            total_nets=total_nets,
            urban_threshold=urban_threshold,
            label_map=lga_label_map,
        )
        if lga_fig is not None:
            return _save_itn_map_html(
                lga_fig,
                session_id,
                total_nets,
                avg_household_size,
                method,
                urban_threshold,
                stats,
                normalized_selected_lgas,
                available_lgas,
                current_level='lga'
            )
        else:
            logger.warning("LGA-level ITN map generation failed; falling back to ward view")
    
    # Log some debugging info
    logger.info(f"Creating ITN map with {len(shp_data)} wards")
    logger.info(f"Nets allocated range: {shp_data['nets_allocated'].min()} to {shp_data['nets_allocated'].max()}")
    logger.info(f"Map center: lat={center_lat}, lon={center_lon}")
    
    # Convert any datetime/timestamp columns to strings to avoid JSON serialization issues
    for col in shp_data.columns:
        if col == 'geometry':
            continue  # Skip geometry column
        
        # Check for datetime columns using pandas API
        if is_datetime64_any_dtype(shp_data[col]):
            logger.info(f"Converting datetime column '{col}' to string for JSON serialization")
            shp_data[col] = shp_data[col].astype(str)
        elif shp_data[col].dtype == 'object':
            # Check if object column contains timestamp objects
            try:
                sample_vals = shp_data[col].dropna()
                if len(sample_vals) > 0:
                    sample_val = sample_vals.iloc[0]
                    # More comprehensive timestamp detection
                    if (hasattr(sample_val, 'timestamp') or 
                        'Timestamp' in str(type(sample_val)) or
                        isinstance(sample_val, pd.Timestamp) or
                        str(type(sample_val)) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>"):
                        logger.info(f"Converting timestamp column '{col}' to string for JSON serialization")
                        shp_data[col] = shp_data[col].astype(str)
            except Exception as e:
                logger.debug(f"Error checking column '{col}': {e}")
                # If any error, convert to string to be safe
                try:
                    shp_data[col] = shp_data[col].astype(str)
                except:
                    pass
    
    # Create plotly figure
    fig = go.Figure()
    
    # Filter out null geometries before creating map
    valid_geometry_mask = ~shp_data.geometry.isnull()
    shp_data_valid = shp_data[valid_geometry_mask].copy()

    if len(shp_data_valid) == 0:
        logger.error("No valid geometries found in shapefile data")
        return None

    # Convert LineString geometries to Polygons for choropleth maps
    # Choropleth maps require Polygon geometries, but some shapefiles have LineStrings
    def convert_to_polygon(geom):
        """Convert LineString to Polygon if needed"""
        if isinstance(geom, (LineString, MultiLineString)):
            try:
                if isinstance(geom, LineString) and geom.is_ring:
                    # If it's a closed ring, create polygon directly
                    return Polygon(geom)
                else:
                    # Buffer the line slightly to create a polygon
                    # Use a very small buffer (0.001 degrees ~100m)
                    return geom.buffer(0.001)
            except Exception as e:
                logger.warning(f"Could not convert linestring to polygon: {e}")
                return geom
        return geom

    # Apply geometry conversion
    logger.info(f"🔄 Converting geometries for ITN map - checking {len(shp_data_valid)} wards")
    original_geom_types = shp_data_valid.geometry.geom_type.value_counts().to_dict()
    logger.info(f"📍 Original geometry types: {original_geom_types}")

    shp_data_valid['geometry'] = shp_data_valid.geometry.apply(convert_to_polygon)

    converted_geom_types = shp_data_valid.geometry.geom_type.value_counts().to_dict()
    logger.info(f"📍 Converted geometry types: {converted_geom_types}")
    
    # Create separate traces for different data categories
    covered_mask = shp_data_valid['nets_allocated'] > 0
    uncovered_mask = shp_data_valid['nets_allocated'] == 0
    
    # Check for wards without population data (if has_population_data column exists)
    no_data_mask = pd.Series(False, index=shp_data_valid.index)
    if 'has_population_data' in shp_data_valid.columns:
        no_data_mask = ~shp_data_valid['has_population_data']
    
    # Add wards without population data first (with distinct styling)
    if no_data_mask.any():
        no_data_wards = shp_data_valid[no_data_mask].copy()
        # Get LGA names for hover
        if 'LGAName' in no_data_wards.columns:
            no_data_lga_names = no_data_wards['LGAName'].fillna('Unknown').astype(str)
        elif 'LGA' in no_data_wards.columns:
            no_data_lga_names = no_data_wards['LGA'].fillna('Unknown').astype(str)
        else:
            no_data_lga_names = pd.Series('Unknown', index=no_data_wards.index)

        fig.add_trace(go.Choroplethmapbox(
            geojson=no_data_wards.geometry.__geo_interface__,
            locations=no_data_wards.index,
            z=[0] * len(no_data_wards),
            colorscale=[[0, '#ffeeee'], [1, '#ffeeee']],  # Light red tint
            text=no_data_wards['WardName'],
            hovertemplate='<b>Ward:</b> %{text}<br>' +
                          '<b>LGA:</b> %{customdata[2]}<br>' +
                          '─────────────────<br>' +
                          '<b>Status:</b> ⚠️ No population data<br>' +
                          '<b>Estimated Pop:</b> %{customdata[0]:,.0f}<br>' +
                          '<b>Note:</b> Using estimated values<br>' +
                          '<extra></extra>',
            customdata=np.column_stack((
                no_data_wards['Population'].fillna(0),
                no_data_wards.get('urban_pct_display', pd.Series([0]*len(no_data_wards))).fillna(0),
                no_data_lga_names
            )),
            marker_opacity=0.6,  # Increased opacity for visibility
            marker_line_width=1.5,  # Thicker borders
            marker_line_color='#cc6666',  # Darker red border
            marker_line_dash='dash',  # Dashed border for no-data wards
            showscale=False,
            name='No Population Data'
        ))
    
    # Add uncovered areas (with data but no allocation)
    uncovered_with_data = uncovered_mask & ~no_data_mask
    if uncovered_with_data.any():
        uncovered_data = shp_data_valid[uncovered_with_data].copy()

        # ✅ FIX: Create DYNAMIC reason field based on each ward's actual urban percentage
        def get_no_allocation_reason(row):
            """Generate the correct reason why a ward didn't receive nets."""
            urban_pct = row.get('urban_pct_display', 0)
            if pd.isna(urban_pct):
                return "No urban data available"
            elif urban_pct < urban_threshold:
                # Ward is RURAL but still got no nets - must be low priority
                return f"Rural ward (urban {urban_pct:.1f}% < {urban_threshold}%), but all nets allocated to higher priority wards"
            else:
                # Ward is URBAN and got no nets
                return f"Urban ward (urban {urban_pct:.1f}% ≥ {urban_threshold}%), nets exhausted before reaching urban tier"

        uncovered_data['no_alloc_reason'] = uncovered_data.apply(get_no_allocation_reason, axis=1)

        # Get LGA names for hover
        if 'LGAName' in uncovered_data.columns:
            uncovered_lga_names = uncovered_data['LGAName'].fillna('Unknown').astype(str)
        elif 'LGA' in uncovered_data.columns:
            uncovered_lga_names = uncovered_data['LGA'].fillna('Unknown').astype(str)
        else:
            uncovered_lga_names = pd.Series('Unknown', index=uncovered_data.index)

        fig.add_trace(go.Choroplethmapbox(
            geojson=uncovered_data.geometry.__geo_interface__,
            locations=uncovered_data.index,
            z=[0] * len(uncovered_data),  # All zeros for consistent gray color
            colorscale=[[0, '#d0d0d0'], [1, '#d0d0d0']],  # More visible gray
            text=uncovered_data['WardName'],
            hovertemplate='<b>Ward:</b> %{text}<br>' +
                          '<b>LGA:</b> %{customdata[5]}<br>' +
                          '─────────────────<br>' +
                          '<b>Status:</b> No nets allocated<br>' +
                          '<b>Urban %:</b> %{customdata[3]:.1f}%<br>' +
                          '<b>Threshold:</b> ' + str(urban_threshold) + '%<br>' +
                          '<b>Population:</b> %{customdata[0]:,.0f}<br>' +
                          '<b>Reason:</b> %{customdata[4]}<br>' +
                          '<extra></extra>',
            customdata=np.column_stack((
                uncovered_data['Population'],  # [0] Population
                uncovered_data['coverage_percent'],  # [1] coverage_percent
                uncovered_data['allocation_phase'],  # [2] allocation_phase
                uncovered_data['urban_pct_display'],  # [3] urban_pct_display
                uncovered_data['no_alloc_reason'],  # [4] dynamic reason
                uncovered_lga_names  # [5] LGA name
            )),
            marker_opacity=0.7,  # Increased opacity for visibility
            marker_line_width=1.5,  # Thicker borders for visibility
            marker_line_color='#666666',  # Darker border color
            showscale=False,
            name='No Allocation'
        ))
    
    # ✅ ADD COVERED AREAS WITH DISTINCT COLORS FOR RURAL VS URBAN
    if covered_mask.any():
        covered_data = shp_data_valid[covered_mask].copy()

        # ✅ GET PRIORITY TIER FOR DISPLAY
        def get_tier_label(tier):
            if pd.isna(tier):
                return 'Unknown'
            tier_val = int(tier) if not pd.isna(tier) else 0
            if tier_val == 1:
                return 'Tier 1 (Rural Priority)'
            elif tier_val == 2:
                return 'Tier 2 (Urban Surplus)'
            else:
                return 'Unknown Tier'

        covered_data['tier_display'] = covered_data.get('priority_tier', pd.Series([0]*len(covered_data))).apply(get_tier_label)

        # Get LGA names for covered data
        if 'LGAName' in covered_data.columns:
            covered_lga_names = covered_data['LGAName'].fillna('Unknown').astype(str)
        elif 'LGA' in covered_data.columns:
            covered_lga_names = covered_data['LGA'].fillna('Unknown').astype(str)
        else:
            covered_lga_names = pd.Series('Unknown', index=covered_data.index)
        covered_data['lga_display'] = covered_lga_names

        # Split covered data into rural (tier 1) and urban (tier 2)
        tier1_mask = covered_data.get('priority_tier', pd.Series([0]*len(covered_data))) == 1
        tier2_mask = covered_data.get('priority_tier', pd.Series([0]*len(covered_data))) == 2

        tier1_data = covered_data[tier1_mask]
        tier2_data = covered_data[tier2_mask]

        # ===== TIER 1 (RURAL PRIORITY) - Green Color Scale =====
        if len(tier1_data) > 0:
            fig.add_trace(go.Choroplethmapbox(
                geojson=tier1_data.geometry.__geo_interface__,
                locations=tier1_data.index,
                z=tier1_data['nets_allocated'],
                colorscale=[
                    [0, '#d4edda'],   # Light green (low allocation)
                    [0.5, '#7bc96f'], # Medium green
                    [1, '#28a745']    # Dark green (high allocation)
                ],
                reversescale=False,
                text=tier1_data['WardName'],
                hovertemplate='<b>Ward:</b> %{text}<br>' +
                              '<b>LGA:</b> %{customdata[6]}<br>' +
                              '─────────────────<br>' +
                              '<b>Allocation Status:</b> %{customdata[2]}<br>' +
                              '<b>Priority Tier:</b> %{customdata[5]}<br>' +
                              '<b>Urban %:</b> %{customdata[3]:.1f}% (threshold: ' + str(urban_threshold) + '%)<br>' +
                              '─────────────────<br>' +
                              '<b>Nets Allocated:</b> %{z:,.0f}<br>' +
                              '<b>Nets Needed:</b> %{customdata[4]:,.0f}<br>' +
                              '<b>Net Ratio:</b> 1 net per 1.8 people<br>' +
                              '<b>Population:</b> %{customdata[0]:,.0f}<br>' +
                              '<b>Coverage:</b> %{customdata[1]:.1f}%<br>' +
                              '<extra></extra>',
                customdata=np.column_stack((
                    tier1_data['Population'].fillna(0),
                    tier1_data['coverage_percent'],
                    tier1_data['allocation_phase'],
                    tier1_data['urban_pct_display'].fillna(0),
                    tier1_data['nets_needed'].fillna(0),
                    tier1_data['tier_display'],
                    tier1_data['lga_display']
                )),
                marker_opacity=0.9,
                marker_line_width=2,
                marker_line_color='#1e7e34',  # Darker green border
                showscale=True,
                colorbar=dict(
                    title=dict(
                        text="Number of<br>Bed Nets",
                        font=dict(size=10)
                    ),
                    thickness=15,
                    len=0.3,
                    x=0.98,
                    y=0.75
                ),
                name='Rural Priority (Tier 1)'
            ))

        # ===== TIER 2 (URBAN SURPLUS) - Blue Color Scale =====
        if len(tier2_data) > 0:
            fig.add_trace(go.Choroplethmapbox(
                geojson=tier2_data.geometry.__geo_interface__,
                locations=tier2_data.index,
                z=tier2_data['nets_allocated'],
                colorscale=[
                    [0, '#cfe2ff'],   # Light blue (low allocation)
                    [0.5, '#6ea8fe'], # Medium blue
                    [1, '#0d6efd']    # Dark blue (high allocation)
                ],
                reversescale=False,
                text=tier2_data['WardName'],
                hovertemplate='<b>Ward:</b> %{text}<br>' +
                              '<b>LGA:</b> %{customdata[6]}<br>' +
                              '─────────────────<br>' +
                              '<b>Allocation Status:</b> %{customdata[2]}<br>' +
                              '<b>Priority Tier:</b> %{customdata[5]}<br>' +
                              '<b>Urban %:</b> %{customdata[3]:.1f}% (threshold: ' + str(urban_threshold) + '%)<br>' +
                              '─────────────────<br>' +
                              '<b>Nets Allocated:</b> %{z:,.0f}<br>' +
                              '<b>Nets Needed:</b> %{customdata[4]:,.0f}<br>' +
                              '<b>Net Ratio:</b> 1 net per 1.8 people<br>' +
                              '<b>Population:</b> %{customdata[0]:,.0f}<br>' +
                              '<b>Coverage:</b> %{customdata[1]:.1f}%<br>' +
                              '<extra></extra>',
                customdata=np.column_stack((
                    tier2_data['Population'].fillna(0),
                    tier2_data['coverage_percent'],
                    tier2_data['allocation_phase'],
                    tier2_data['urban_pct_display'].fillna(0),
                    tier2_data['nets_needed'].fillna(0),
                    tier2_data['tier_display'],
                    tier2_data['lga_display']
                )),
                marker_opacity=0.9,
                marker_line_width=2,
                marker_line_color='#084298',  # Darker blue border
                showscale=True,
                colorbar=dict(
                    title=dict(
                        text="Number of<br>Bed Nets",
                        font=dict(size=10)
                    ),
                    thickness=15,
                    len=0.3,
                    x=0.98,
                    y=0.35
                ),
                name='Urban Surplus (Tier 2)'
            ))
    
    # ✅ CALCULATE TIER BREAKDOWN STATISTICS
    tier1_wards = 0
    tier1_nets = 0
    tier2_wards = 0
    tier2_nets = 0

    if not prioritized.empty and 'priority_tier' in prioritized.columns:
        tier1_data = prioritized[prioritized['priority_tier'] == 1]
        tier2_data = prioritized[prioritized['priority_tier'] == 2]

        tier1_wards = len(tier1_data)
        tier1_nets = int(tier1_data['nets_allocated'].sum()) if len(tier1_data) > 0 else 0

        tier2_wards = len(tier2_data)
        tier2_nets = int(tier2_data['nets_allocated'].sum()) if len(tier2_data) > 0 else 0

    # Add LGA boundary overlay for visual clarity
    try:
        add_lga_boundary_overlay(fig, shp_data_valid)
        logger.info("✅ Added LGA boundary overlay to ITN map")
    except Exception as lga_err:
        logger.warning(f"Could not add LGA boundary overlay: {lga_err}")

    # Add annotations for threshold info
    annotations = [
        dict(
            text=f"<b>Allocation Strategy</b><br>" +
                 f"<b>Tier 1 (Rural Priority):</b> Urban < {urban_threshold}%<br>" +
                 f"  • Wards: {tier1_wards} | Nets: {tier1_nets:,}<br>" +
                 f"<b>Tier 2 (Urban Surplus):</b> Urban >= {urban_threshold}%<br>" +
                 f"  • Wards: {tier2_wards} | Nets: {tier2_nets:,}<br>" +
                 f"<b>Net Ratio:</b> 1 net per 1.8 people<br>" +
                 f"<i>Hover over wards for details</i>",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            xanchor="left", yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.95)",
            bordercolor="black",
            borderwidth=1.5,
            font=dict(size=10, family="Arial")
        ),
        dict(
            text=f"<b>Distribution Summary</b><br>" +
                 f"<b>Total Nets:</b> {total_nets:,}<br>" +
                 f"<b>Allocated:</b> {stats['allocated']:,}<br>" +
                 f"<b>Remaining:</b> {stats.get('remaining', 0):,}<br>" +
                 f"<b>Pop Coverage:</b> {stats['coverage_percent']}%<br>" +
                 f"<b>Wards Covered:</b> {stats.get('prioritized_wards', 0)}" if stats else f"Total Nets: {total_nets:,}",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.98, y=0.98,
            xanchor="right", yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.95)",
            bordercolor="black",
            borderwidth=1.5,
            font=dict(size=10, family="Arial")
        )
    ]
    
    # Update layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=8,
            bearing=0,
            pitch=0
        ),
        margin={"r": 0, "t": 60, "l": 0, "b": 0},
        title=dict(
            text=f"ITN Distribution Plan<br><sub>Highlighted areas receive bed nets | Grey areas have no allocation</sub>",
            x=0.5,
            xanchor='center',
            font=dict(size=18)
        ),
        height=700,
        annotations=annotations,
        showlegend=False
    )
    
    return _save_itn_map_html(
        fig,
        session_id,
        total_nets,
        avg_household_size,
        method,
        urban_threshold,
        stats,
        normalized_selected_lgas,
        available_lgas,
        current_level='ward'
    )


def _normalize_selected_lgas(selected: Optional[List[str]], available: List[Dict[str, str]]) -> List[str]:
    if not selected:
        return []
    available_codes = {normalize_lga_code(item.get('code')) for item in available}
    normalized: List[str] = []
    for code in selected:
        normalized_code = normalize_lga_code(code)
        if not normalized_code:
            continue
        if available_codes and normalized_code not in available_codes:
            continue
        normalized.append(normalized_code)
    return normalized


def _deduplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicated column labels while keeping the first instance."""
    if frame is None or not hasattr(frame, 'columns'):
        return frame
    duplicated_mask = frame.columns.duplicated()
    if duplicated_mask.any():
        frame = frame.loc[:, ~duplicated_mask]
    return frame


def _build_lga_allocation_figure(
    shp_data: gpd.GeoDataFrame,
    center_lat: Optional[float],
    center_lon: Optional[float],
    stats: Optional[Dict[str, Any]],
    selected_lgas: List[str],
    total_nets: int,
    urban_threshold: float,
    label_map: Dict[str, str],
) -> Optional[go.Figure]:
    aggregated = (
        shp_data.groupby('LGACode', dropna=True)
        .agg({
            'nets_allocated': 'sum',
            'nets_needed': 'sum',
            'Population': 'sum',
            'coverage_percent': 'mean',
            'urban_pct_display': 'mean',
            'StateName': 'first',
            'LGAName': 'first',
        })
        .reset_index()
    )

    reference_shapes = get_reference_lga_geometries(aggregated[['LGACode', 'StateName', 'LGAName']])
    if reference_shapes is None or reference_shapes.empty:
        try:
            fallback = dissolve_to_lga(
                shp_data,
                value_columns=[],
                sum_columns=['nets_allocated', 'nets_needed', 'Population'],
                mean_columns=['coverage_percent', 'urban_pct_display'],
            )
        except Exception as exc:
            logger.error(f"Failed to generate fallback LGA dissolve: {exc}")
            return None
        reference_shapes = fallback
    else:
        reference_shapes = _deduplicate_columns(reference_shapes)
        reference_shapes = reference_shapes.drop(columns=['StateName', 'LGAName'], errors='ignore')
        reference_shapes = reference_shapes.rename(
            columns={'state_name': 'StateName', 'lga_name': 'LGAName'}
        )
    reference_shapes = _deduplicate_columns(reference_shapes)

    aggregated['nets_allocated'] = aggregated['nets_allocated'].fillna(0)
    aggregated['nets_needed'] = aggregated['nets_needed'].fillna(0)
    aggregated['Population'] = aggregated['Population'].fillna(0)
    aggregated['coverage_percent'] = aggregated['coverage_percent'].fillna(0)
    aggregated['urban_pct_display'] = aggregated['urban_pct_display'].fillna(0)
    aggregated['LGACode_norm'] = aggregated['LGACode'].apply(normalize_lga_code)
    aggregated['LGAName'] = aggregated['LGAName'].fillna(
        aggregated['LGACode_norm'].map(label_map)
    )

    metrics = aggregated.drop(columns=['StateName', 'LGAName'])
    merged = reference_shapes.merge(metrics, on='LGACode', how='left')
    merged = _deduplicate_columns(merged)
    if 'LGAName' not in merged.columns:
        merged['LGAName'] = ''
    lga_name_values = merged['LGAName']
    if isinstance(lga_name_values, pd.DataFrame):
        # Pandas returns a DataFrame when duplicate column labels survive the merge
        lga_name_values = lga_name_values.iloc[:, 0]
    merged['display_name'] = lga_name_values.astype(str)
    if 'LGACode' in merged.columns:
        empty_mask = merged['display_name'].str.strip().eq('')
        merged.loc[empty_mask, 'display_name'] = merged.loc[empty_mask, 'LGACode'].astype(str)

    aggregated = apply_lga_highlight(merged, selected_lgas, 'LGACode')

    bounds = aggregated.total_bounds
    local_center_lat = (bounds[1] + bounds[3]) / 2
    local_center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = center_lat if center_lat is not None else local_center_lat
    center_lon = center_lon if center_lon is not None else local_center_lon

    fig = go.Figure()

    def _add_trace(data: gpd.GeoDataFrame, show_scale: bool, opacity: float, colorscale):
        if data is None or data.empty:
            return
        geojson = data.geometry.__geo_interface__
        fig.add_trace(
            go.Choroplethmapbox(
                geojson=geojson,
                locations=data.index.astype(str),
                z=data['nets_allocated'],
                colorscale=colorscale,
                text=data['display_name'],
                hovertemplate='<b>LGA:</b> %{text}<br>' +
                              '─────────────────<br>' +
                              '<b>Nets Allocated:</b> %{z:,.0f}<br>' +
                              '<b>Nets Needed:</b> %{customdata[0]:,.0f}<br>' +
                              '<b>Population:</b> %{customdata[1]:,.0f}<br>' +
                              '<b>Coverage:</b> %{customdata[2]:.1f}%<br>' +
                              '<b>Urban %:</b> %{customdata[3]:.1f}%<br>' +
                              '<extra></extra>',
                customdata=np.column_stack((
                    data['nets_needed'],
                    data['Population'],
                    data['coverage_percent'],
                    data['urban_pct_display'],
                )),
                marker_opacity=opacity,
                marker_line_width=1.5,
                marker_line_color='#ffffff',
                showscale=show_scale,
                colorbar=dict(
                    title=dict(text="Nets", font=dict(size=11)),
                    thickness=14,
                    len=0.5,
                ) if show_scale else None,
            )
        )

    if selected_lgas:
        faded = aggregated[~aggregated['_is_selected_lga']]
        highlighted = aggregated[aggregated['_is_selected_lga']]
        _add_trace(faded, show_scale=False, opacity=0.25, colorscale=[[0, '#e5e7eb'], [1, '#9ca3af']])
        _add_trace(
            highlighted,
            show_scale=True,
            opacity=0.85,
            colorscale='YlOrRd',
        )
    else:
        _add_trace(aggregated, show_scale=True, opacity=0.85, colorscale='YlOrRd')

    summary_text = ''
    if stats:
        summary_text = (
            f"<b>Total Nets:</b> {stats.get('total_nets', total_nets):,}<br>"
            f"<b>Allocated:</b> {stats.get('allocated', 0):,}<br>"
            f"<b>Coverage:</b> {stats.get('coverage_percent', 0)}%"
        )

    fig.update_layout(
        mapbox=dict(
            style='open-street-map',
            center=dict(lat=center_lat, lon=center_lon),
            zoom=7.2,
        ),
        margin={"r": 0, "t": 60, "l": 0, "b": 0},
        title=dict(
            text="ITN Distribution by LGA",
            x=0.5,
            xanchor='center',
            font=dict(size=18)
        ),
        showlegend=False,
        annotations=[
            dict(
                text=summary_text,
                showarrow=False,
                xref='paper', yref='paper',
                x=0.02, y=0.98,
                xanchor='left', yanchor='top',
                bgcolor='rgba(255,255,255,0.95)',
                bordercolor='#000',
                borderwidth=1,
                font=dict(size=11)
            )
        ] if summary_text else []
    )
    return fig


def _save_itn_map_html(
    fig: go.Figure,
    session_id: str,
    total_nets: int,
    avg_household_size: float,
    method: str,
    urban_threshold: float,
    stats: Optional[Dict[str, Any]],
    selected_lgas: List[str],
    available_lgas: List[Dict[str, str]],
    current_level: str,
) -> str:
    viz_dir = f'instance/uploads/{session_id}/visualizations'
    os.makedirs(viz_dir, exist_ok=True)
    filename = f'itn_distribution_map_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    path = os.path.join(viz_dir, filename)
    fig.write_html(path, include_plotlyjs=True)

    with open(path, 'r') as handle:
        html_content = handle.read()

    custom_css = """
        <style>
            .threshold-control {
                position: absolute;
                top: 120px;
                left: 20px;
                background: rgba(255, 255, 255, 0.9);
                padding: 15px;
                border-radius: 5px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
                z-index: 1000;
            }
            .threshold-control input {
                width: 60px;
                padding: 5px;
                margin: 0 10px;
            }
            .threshold-control button {
                background: #1f77b4;
                color: white;
                border: none;
                padding: 5px 15px;
                border-radius: 3px;
                cursor: pointer;
            }
            .threshold-control button:hover {
                background: #1a5490;
            }
        </style>
    """

    threshold_control = f"""
        <div class="threshold-control">
            <label><b>Urban Threshold:</b></label>
            <input type="number" id="thresholdInput" value="{urban_threshold}" min="0" max="100" step="5">%
            <button onclick="updateThreshold()">Update</button>
        </div>
    """

    update_script = f"""
        <script>
            window.itnParams = {{
                total_nets: {total_nets},
                avg_household_size: {avg_household_size},
                method: '{method}',
                session_id: '{session_id}'
            }};
            function updateThreshold() {{
                var newThreshold = document.getElementById('thresholdInput').value;
                var button = event.target;
                button.disabled = true;
                button.textContent = 'Updating...';
                fetch('/api/itn/update-distribution', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    credentials: 'same-origin',
                    body: JSON.stringify({{
                        urban_threshold: parseFloat(newThreshold),
                        total_nets: window.itnParams.total_nets,
                        avg_household_size: window.itnParams.avg_household_size,
                        method: window.itnParams.method,
                        session_id: window.itnParams.session_id
                    }})
                }})
                .then(response => response.json())
                .then(data => {{
                    if (data.status === 'success' && data.map_path) {{
                        if (window.parent !== window) {{
                            window.parent.postMessage({{ type: 'updateITNMap', mapPath: data.map_path }}, '*');
                        }} else {{
                            window.location.href = data.map_path;
                        }}
                    }} else {{
                        alert('Error: ' + (data.message || 'Failed to update distribution'));
                        button.disabled = false;
                        button.textContent = 'Update';
                    }}
                }})
                .catch(error => {{
                    console.error('Update error:', error);
                    alert('Error updating distribution: ' + error);
                    button.disabled = false;
                    button.textContent = 'Update';
                }});
            }}
        </script>
    """

    html_content = html_content.replace('</head>', custom_css + '</head>')
    html_content = html_content.replace('<body>', '<body>' + threshold_control)
    html_content = html_content.replace('</body>', update_script + '</body>')

    with open(path, 'w') as handle:
        handle.write(html_content)

    return f'/serve_viz_file/{session_id}/visualizations/{filename}'
