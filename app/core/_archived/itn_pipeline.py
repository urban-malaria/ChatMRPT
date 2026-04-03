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
from app.data.population_data.itn_population_loader import get_population_loader
from app.utils.map_overlays import add_lga_boundary_overlay
from app.utils.visualization_controls import inject_lga_hover_highlight
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

logger = logging.getLogger(__name__)

def detect_state(data_handler) -> Optional[str]:
    """Detect state from shapefile or session data using the population loader."""
    loader = get_population_loader()
    state_code_map = loader.get_state_code_map()

    code_to_name = {code.upper(): name for code, name in state_code_map.items()}
    name_to_name = {str(name).strip().lower(): name for name in state_code_map.values()}

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
        if 'StateCode' in data_handler.unified_dataset.columns:
            state = _normalize_state(data_handler.unified_dataset['StateCode'].iloc[0])
            if state:
                logger.info(f"Detected state from unified dataset: {state}")
                return state

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
    """Load and aggregate population data for the state."""
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
                          threshold: int = 80) -> Dict[str, Tuple[str, int]]:
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

def calculate_itn_distribution(data_handler, session_id: str, total_nets: int = 10000, avg_household_size: float = 5.0, urban_threshold: float = 30.0, method: str = 'composite') -> Dict[str, Any]:
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
        if 'urbanPercentage' in data_handler.unified_dataset.columns:
            required_cols.append('urbanPercentage')
        
        rankings = data_handler.unified_dataset[required_cols].copy()
        rankings = rankings.rename(columns={
            score_col: 'score',
            rank_col: 'overall_rank',
            category_col: 'vulnerability_category'
        })
        
        # Fix dtype mismatch - ensure numeric columns are numeric
        rankings['score'] = pd.to_numeric(rankings['score'], errors='coerce')
        rankings['overall_rank'] = pd.to_numeric(rankings['overall_rank'], errors='coerce')
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

        try:
            report_path = f"instance/uploads/{session_id}/ward_matching_report.json"
            with open(report_path, 'w') as f:
                json.dump(matching_report, f, indent=2, default=str)
            logger.info(f"Saved ward matching report to {report_path}")
        except Exception as e:
            logger.warning(f"Could not save matching report: {e}")

        if matched_count == 0:
            logger.warning(f"No matches found. Checking for common ward names...")
            rankings_wards = set(rankings['WardName'].str.lower())
            pop_wards = set(pop_data['WardName'].str.lower())
            common_wards = rankings_wards.intersection(pop_wards)
            logger.warning(f"Common ward names: {len(common_wards)} - {list(common_wards)[:5]}")

        avg_population = rankings['Population'].dropna().mean() if rankings['Population'].notna().any() else 10000
        no_pop_wards = rankings[rankings['Population'].isna()]
        if len(no_pop_wards) > 0:
            logger.warning(f"Found {len(no_pop_wards)} wards without population data")
            logger.info(f"Using estimated population of {avg_population:.0f} for wards without data")
            rankings['Population'] = rankings['Population'].fillna(avg_population)
            rankings['has_population_data'] = rankings['Population'].notna()

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
    
    # FULL COVERAGE STRATEGY: Give 100% coverage to highest risk wards until nets run out
    # Handle 'UrbanPercent', 'urbanPercentage', and 'urban_percentage' column names
    if 'UrbanPercent' in rankings.columns:
        urban_col = 'UrbanPercent'
    elif 'urbanPercentage' in rankings.columns:
        urban_col = 'urbanPercentage'
    elif 'urban_percentage' in rankings.columns:
        urban_col = 'urban_percentage'
    else:
        logger.warning(f"No urban percentage column found. Available columns: {rankings.columns.tolist()}")
        rankings['urban_pct'] = 50.0  # Default to 50% urban
        urban_col = 'urban_pct'
    
    # Sort ALL wards by risk rank (highest risk first)
    rankings_sorted = rankings.sort_values('overall_rank')
    rankings_sorted['nets_needed'] = rankings_sorted['Population'].apply(lambda p: calculate_nets_needed(p, avg_household_size))
    
    # Allocate nets - full coverage to each ward until we run out
    allocated = 0
    wards_with_allocation = []
    
    for idx, row in rankings_sorted.iterrows():
        nets_for_this_ward = row['nets_needed']
        
        # Can we fully cover this ward?
        if allocated + nets_for_this_ward <= total_nets:
            # Yes - give full coverage
            row_copy = row.copy()
            row_copy['nets_allocated'] = nets_for_this_ward
            row_copy['coverage_percent'] = 100.0
            row_copy['allocation_phase'] = 'Full Coverage'
            wards_with_allocation.append(row_copy)
            allocated += nets_for_this_ward
            logger.info(f"Allocated {nets_for_this_ward} nets to {row['WardName']} (rank {row['overall_rank']}, pop {row['Population']:.0f})")
        else:
            # No more full coverage possible
            remaining = total_nets - allocated
            if remaining > 0:
                # Give partial coverage to this last ward
                row_copy = row.copy()
                row_copy['nets_allocated'] = remaining
                row_copy['coverage_percent'] = (remaining / nets_for_this_ward) * 100
                row_copy['allocation_phase'] = 'Partial Coverage (Last Ward)'
                wards_with_allocation.append(row_copy)
                allocated = total_nets
                logger.info(f"Allocated remaining {remaining} nets to {row['WardName']} ({row_copy['coverage_percent']:.1f}% coverage)")
            break
    
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
    map_path = generate_itn_map(shp_data, prioritized, reprioritized, 
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
            'timestamp': datetime.now().isoformat(),
            'prioritized': prioritized.to_dict('records'),
            'reprioritized': reprioritized.to_dict('records') if not reprioritized.empty else [],
            'map_path': map_path
        }
        
        # Save to session folder
        results_path = f"instance/uploads/{session_id}/itn_distribution_results.json"
        with open(results_path, 'w') as f:
            json.dump(results_to_save, f, indent=2, default=str)
        logger.info(f"Saved ITN distribution results to {results_path}")
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

def generate_itn_map(shp_data: gpd.GeoDataFrame, prioritized: pd.DataFrame, reprioritized: pd.DataFrame, 
                     session_id: str, urban_threshold: float = 30.0, total_nets: int = 10000, 
                     avg_household_size: float = 5.0, method: str = 'composite', stats: Dict[str, Any] = None) -> str:
    """Generate interactive Plotly map for ITN distribution with threshold info."""
    # Ensure we have the visualization directory
    os.makedirs('app/static/visualizations', exist_ok=True)
    
    # Merge allocation data with shapefile for visualization - deep copy to avoid modifying original
    shp_data = shp_data.copy(deep=True)
    
    # Ensure shp_data is a proper GeoDataFrame
    if not isinstance(shp_data, gpd.GeoDataFrame):
        logger.error("shp_data is not a GeoDataFrame!")
        return None
    
    # Add lowercase column for merging
    shp_data['WardName_lower'] = shp_data['WardName'].str.lower()
    prioritized['WardName_lower'] = prioritized['WardName'].str.lower()
    
    # Merge prioritized allocations - GeoDataFrame.merge preserves geometry automatically
    shp_data = shp_data.merge(
        prioritized[['WardName_lower', 'nets_allocated', 'nets_needed', 'Population']],
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
    # Get urban percentage from the original data
    if 'UrbanPercent' in shp_data.columns:
        shp_data['urban_pct_display'] = shp_data['UrbanPercent']
    elif 'urbanPercentage' in shp_data.columns:
        shp_data['urban_pct_display'] = shp_data['urbanPercentage']
    else:
        # Try to get from the prioritized data
        if 'UrbanPercent' in prioritized.columns:
            urban_col = 'UrbanPercent'
        elif 'urbanPercentage' in prioritized.columns:
            urban_col = 'urbanPercentage' 
        else:
            urban_col = None
            
        if urban_col:
            urban_data = prioritized[['WardName', urban_col]].copy()
            urban_data['WardName_lower'] = urban_data['WardName'].str.lower()
            shp_data = shp_data.merge(
                urban_data[['WardName_lower', urban_col]],
                on='WardName_lower',
                how='left',
                suffixes=('', '_from_prioritized')
            )
            # Also get from reprioritized if available
            if not reprioritized.empty and urban_col in reprioritized.columns:
                urban_data_repri = reprioritized[['WardName', urban_col]].copy()
                urban_data_repri['WardName_lower'] = urban_data_repri['WardName'].str.lower()
                shp_data = shp_data.merge(
                    urban_data_repri[['WardName_lower', urban_col]],
                    on='WardName_lower',
                    how='left',
                    suffixes=('', '_from_reprioritized')
                )
                # Combine both sources
                shp_data['urban_pct_display'] = shp_data[urban_col].fillna(shp_data[urban_col + '_from_reprioritized'])
            else:
                shp_data['urban_pct_display'] = shp_data[urban_col]
        else:
            shp_data['urban_pct_display'] = np.nan
    
    # Ensure we have valid geometry data
    if 'geometry' not in shp_data.columns:
        logger.error("No geometry column found in shapefile data after merging!")
        return None
    
    # Remove any rows with invalid geometry
    shp_data = shp_data[shp_data['geometry'].notna()]
    if len(shp_data) == 0:
        logger.error("No valid geometries found in shapefile data!")
        return None
    
    # Get map center from shapefile bounds
    bounds = shp_data.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
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
    
    # Create separate traces for covered and uncovered areas for better visual distinction
    covered_mask = shp_data_valid['nets_allocated'] > 0
    uncovered_mask = shp_data_valid['nets_allocated'] == 0
    
    # Add uncovered areas first (so they appear behind covered areas)
    if uncovered_mask.any():
        uncovered_data = shp_data_valid[uncovered_mask]
        # Get LGA names for hover
        uncovered_lga_names = uncovered_data['LGAName'].fillna('Unknown').astype(str) if 'LGAName' in uncovered_data.columns else ['Unknown'] * len(uncovered_data)
        fig.add_trace(go.Choroplethmapbox(
            geojson=uncovered_data.geometry.__geo_interface__,
            locations=uncovered_data.index,
            z=[0] * len(uncovered_data),  # All zeros for consistent gray color
            colorscale=[[0, '#f0f0f0'], [1, '#f0f0f0']],  # Light gray
            text=uncovered_data['WardName'],
            hovertemplate='<b>%{text}</b><br>' +
                          'LGA: %{customdata[4]}<br>' +
                          '─────────────────<br>' +
                          '<b>Status:</b> No nets allocated<br>' +
                          '<b>Urban %:</b> %{customdata[3]:.1f}%<br>' +
                          '<b>Population:</b> %{customdata[0]:,.0f}<br>' +
                          '<extra></extra>',
            customdata=np.column_stack((
                uncovered_data['Population'].fillna(0),
                uncovered_data['coverage_percent'],
                uncovered_data['allocation_phase'],
                uncovered_data['urban_pct_display'].fillna(0),
                uncovered_lga_names
            )),
            marker_opacity=0.3,  # Much lower opacity for uncovered areas
            marker_line_width=0.5,
            marker_line_color='#cccccc',
            showscale=False,
            name='No Allocation'
        ))
    
    # Add covered areas with prominent colors
    if covered_mask.any():
        covered_data = shp_data_valid[covered_mask]
        # Get LGA names for hover
        covered_lga_names = covered_data['LGAName'].fillna('Unknown').astype(str) if 'LGAName' in covered_data.columns else ['Unknown'] * len(covered_data)
        fig.add_trace(go.Choroplethmapbox(
            geojson=covered_data.geometry.__geo_interface__,
            locations=covered_data.index,
            z=covered_data['nets_allocated'],
            colorscale='Plasma',  # Purple to Pink to Yellow
            reversescale=False,   # Yellow for high allocation
            text=covered_data['WardName'],
            hovertemplate='<b>%{text}</b><br>' +
                          'LGA: %{customdata[4]}<br>' +
                          '─────────────────<br>' +
                          '<b>Allocation Status:</b> %{customdata[2]}<br>' +
                          '<b>Urban %:</b> %{customdata[3]:.1f}%<br>' +
                          '<b>Threshold:</b> ' + str(urban_threshold) + '%<br>' +
                          '─────────────────<br>' +
                          '<b>Nets Allocated:</b> %{z:,.0f}<br>' +
                          '<b>Population:</b> %{customdata[0]:,.0f}<br>' +
                          '<b>Coverage:</b> %{customdata[1]:.1f}%<br>' +
                          '<extra></extra>',
            customdata=np.column_stack((
                covered_data['Population'].fillna(0),
                covered_data['coverage_percent'],
                covered_data['allocation_phase'],
                covered_data['urban_pct_display'].fillna(0),
                covered_lga_names
            )),
            marker_opacity=0.9,  # High opacity for covered areas
            marker_line_width=1.5,
            marker_line_color='white',
            showscale=True,
            colorbar=dict(
                title="Nets<br>Allocated",
                thickness=15,
                len=0.7,
                x=0.98,
                y=0.5
            ),
            name='Allocated'
        ))
    
    # Add annotations for threshold info
    annotations = [
        dict(
            text=f"<b>Allocation Strategy</b><br>" +
                 f"<b>Phase 1:</b> Rural wards (urban < {urban_threshold}%) get 100% coverage<br>" +
                 f"<b>Phase 2:</b> Remaining nets go to urban wards (urban ≥ {urban_threshold}%)<br>" +
                 f"<i>Hover over wards to see their allocation phase</i>",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            xanchor="left", yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="black",
            borderwidth=1,
            font=dict(size=11)
        ),
        dict(
            text=f"<b>Distribution Summary</b><br>" +
                 f"Total Nets: {total_nets:,}<br>" +
                 f"Allocated: {stats['allocated']:,}<br>" +
                 f"Coverage: {stats['coverage_percent']}%" if stats else f"Total Nets: {total_nets:,}",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.98, y=0.98,
            xanchor="right", yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="black",
            borderwidth=1,
            font=dict(size=11)
        )
    ]

    # Add LGA boundary overlay
    add_lga_boundary_overlay(fig, shp_data_valid)

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
            text=f"ITN Distribution Plan<br><sub>Highlighted areas receive bed nets | Faded areas have no allocation</sub>",
            x=0.5,
            xanchor='center',
            font=dict(size=18)
        ),
        height=700,
        annotations=annotations,
        showlegend=False
    )
    
    # Save map in session folder for better organization
    # Create visualizations directory if it doesn't exist
    viz_dir = f'instance/uploads/{session_id}/visualizations'
    os.makedirs(viz_dir, exist_ok=True)
    
    # Save map in session visualizations folder
    filename = f'itn_distribution_map_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    path = os.path.join(viz_dir, filename)
    
    # First save the figure normally
    fig.write_html(path, include_plotlyjs=True)
    
    # Then add the threshold control by modifying the saved HTML
    with open(path, 'r') as f:
        html_content = f.read()
    
    # Add custom CSS for threshold control
    custom_css = """
        <style>
            .threshold-control {
                position: absolute;
                top: 80px;
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
    
    # Add threshold control HTML
    threshold_control = f"""
        <div class="threshold-control">
            <label><b>Urban Threshold:</b></label>
            <input type="number" id="thresholdInput" value="{urban_threshold}" min="0" max="100" step="5">%
            <button onclick="updateThreshold()">Update</button>
        </div>
    """
    
    # Add update function
    update_script = f"""
        <script>
            function updateThreshold() {{
                var newThreshold = document.getElementById('thresholdInput').value;
                var button = event.target;
                button.disabled = true;
                button.textContent = 'Updating...';
                
                // Call backend API to update distribution
                fetch('/api/itn/update-distribution', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json',
                    }},
                    body: JSON.stringify({{
                        urban_threshold: parseFloat(newThreshold),
                        total_nets: {total_nets},
                        avg_household_size: {avg_household_size},
                        method: '{method}'
                    }})
                }})
                .then(response => response.json())
                .then(data => {{
                    if (data.status === 'success') {{
                        // Reload page with new map
                        window.location.reload();
                    }} else {{
                        alert('Error: ' + data.message);
                        button.disabled = false;
                        button.textContent = 'Update';
                    }}
                }})
                .catch(error => {{
                    alert('Error updating distribution: ' + error);
                    button.disabled = false;
                    button.textContent = 'Update';
                }});
            }}
        </script>
    """
    
    # Insert custom elements into the HTML
    html_content = html_content.replace('</head>', custom_css + '</head>')
    html_content = html_content.replace('<body>', '<body>' + threshold_control)
    html_content = html_content.replace('</body>', update_script + '</body>')
    
    # Write the modified HTML back
    with open(path, 'w') as f:
        f.write(html_content)

    # Inject LGA hover highlighting
    try:
        lga_codes = shp_data_valid['LGACode'].fillna('').astype(str).tolist() if 'LGACode' in shp_data_valid.columns else []
        if lga_codes:
            inject_lga_hover_highlight(path, lga_codes)
    except Exception as hover_err:
        logger.warning(f"Failed to inject LGA hover highlight for ITN map: {hover_err}")

    # Return path to serve the file
    return f'/serve_viz_file/{session_id}/visualizations/{filename}'
