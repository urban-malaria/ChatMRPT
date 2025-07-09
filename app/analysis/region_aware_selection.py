"""
Region-Aware Variable Selection for ChatMRPT

This module implements region-specific variable selection based on Nigeria's 
geopolitical zones as defined in selected_variables_region.md.

Functions:
- Detect geopolitical zone from uploaded data
- Select variables based on zone-specific priorities
- Apply regional importance weighting
"""

import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, List, Optional, Tuple, Any
from difflib import SequenceMatcher
import re
from ..core.variable_matcher import variable_matcher

logger = logging.getLogger(__name__)

# Nigeria Geopolitical Zone Mappings
GEOPOLITICAL_ZONES = {
    'North_Central': ['Benue', 'Kogi', 'Kwara', 'Nasarawa', 'Niger', 'Plateau', 'FCT', 'Abuja'],
    'North_East': ['Adamawa', 'Bauchi', 'Borno', 'Gombe', 'Taraba', 'Yobe'],
    'North_West': ['Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Sokoto', 'Zamfara'],
    'South_East': ['Abia', 'Anambra', 'Ebonyi', 'Enugu', 'Imo'],
    'South_South': ['Akwa Ibom', 'Bayelsa', 'Cross River', 'Delta', 'Edo', 'Rivers'],
    'South_West': ['Ekiti', 'Lagos', 'Ogun', 'Ondo', 'Osun', 'Oyo']
}

# Scientifically-validated variable sets by zone (from selected_variables_region.md Results section)
# These are statistically selected variables using AIC/BIC stepwise selection
ZONE_VARIABLES = {
    'North_Central': [
        'pfpr', 'nighttime_lights', 'evi', 'ndmi', 'ndwi', 'soil_wetness', 'rainfall', 'temp', 'u5_tpr_rdt'
    ],
    'North_East': [
        'pfpr', 'housing_quality', 'evi', 'ndwi', 'soil_wetness', 'u5_tpr_rdt'
    ],
    'North_West': [
        'pfpr', 'housing_quality', 'elevation', 'evi', 'distance_to_waterbodies', 'soil_wetness', 'u5_tpr_rdt'
    ],
    'South_East': [
        'pfpr', 'rainfall', 'elevation', 'evi', 'nighttime_lights', 'soil_wetness', 'temp', 'u5_tpr_rdt'
    ],
    'South_South': [
        'pfpr', 'elevation', 'housing_quality', 'temp', 'evi', 'ndwi', 'ndmi', 'u5_tpr_rdt'
    ],
    'South_West': [
        'pfpr', 'rainfall', 'elevation', 'evi', 'nighttime_lights', 'u5_tpr_rdt'  # Using BIC results
    ]
}

# Core malaria analysis variables that should always be included
CORE_VARIABLES = ['pfpr', 'u5_tpr_rdt']

# Identifier columns (not analysis variables) - expanded to catch common identifier patterns
IDENTIFIER_COLUMNS = [
    # Ward identifiers
    'WardName', 'StateCode', 'WardCode', 'LGACode', 'ward_name', 'ward_code',
    # Row/index identifiers 
    'X', 'X.1', 'X.2', 'X.3', 'X.4', 'X.5', 'index', 'Unnamed: 0',
    # Geographic identifiers
    'country_na', 'country_co', 'country_name', 'country_code',
    'state_name', 'state_code', 'lga_name', 'lga_code',
    # Metadata identifiers
    'global_id', 'source', 'source_dat', 'properties',
    # Other common non-analytical columns
    'geometry', 'Shape_Area', 'Shape_Leng'
]

def find_fuzzy_match(target_var: str, available_vars: List[str], threshold: float = 0.75) -> Optional[str]:
    """
    Find a fuzzy match for a target variable in the list of available variables.
    
    Args:
        target_var: The variable name to match
        available_vars: List of available variable names
        threshold: Minimum similarity score (0-1) to accept a match
        
    Returns:
        Best matching variable name or None if no good match found
    """
    # Use the enhanced variable matcher for better fuzzy matching
    match_results = variable_matcher.match_variables([target_var], available_vars, threshold)
    
    if target_var in match_results['matched']:
        return match_results['matched'][target_var]
    
    # Fallback to original implementation for backward compatibility
    best_match = None
    best_score = 0
    
    # Normalize target variable for comparison
    target_lower = target_var.lower()
    target_parts = re.split(r'[_\s-]+', target_lower)
    
    for available_var in available_vars:
        # Skip identifier columns
        if available_var in IDENTIFIER_COLUMNS:
            continue
            
        available_lower = available_var.lower()
        
        # Calculate various similarity scores
        scores = []
        
        # 1. Direct string similarity
        direct_score = SequenceMatcher(None, target_lower, available_lower).ratio()
        scores.append(direct_score)
        
        # 2. Check if target is substring or vice versa
        if target_lower in available_lower or available_lower in target_lower:
            scores.append(0.9)
        
        # 3. Check word overlap
        available_parts = re.split(r'[_\s-]+', available_lower)
        common_parts = set(target_parts) & set(available_parts)
        if common_parts:
            overlap_score = len(common_parts) / max(len(target_parts), len(available_parts))
            scores.append(overlap_score)
        
        # 4. Special cases for common variations
        if _check_special_variations(target_var, available_var):
            scores.append(0.95)
        
        # Use the highest score
        max_score = max(scores) if scores else 0
        
        if max_score > best_score and max_score >= threshold:
            best_score = max_score
            best_match = available_var
    
    return best_match

def _check_special_variations(var1: str, var2: str) -> bool:
    """
    Check for common variable name variations in malaria/health data.
    """
    variations = [
        # Temperature variations
        ('temp', 'temperature'),
        ('tmp', 'temperature'),
        ('temp', 'tmp'),
        # Rainfall variations
        ('rain', 'rainfall'),
        ('precip', 'precipitation'),
        ('rainfall', 'precipitation'),
        # Elevation variations
        ('elev', 'elevation'),
        ('alt', 'altitude'),
        ('elevation', 'altitude'),
        # Test positivity rate variations
        ('tpr', 'test_positivity_rate'),
        ('u5_tpr', 'under5_tpr'),
        ('u5_tpr_rdt', 'under5_test_positivity'),
        # Parasite prevalence variations
        ('pfpr', 'parasite_prevalence'),
        ('pfpr', 'p_falciparum_pr'),
        # Vegetation index variations
        ('evi', 'enhanced_vegetation_index'),
        ('ndvi', 'normalized_difference_vegetation_index'),
        # Water index variations
        ('ndwi', 'normalized_difference_water_index'),
        ('ndmi', 'normalized_difference_moisture_index'),
        # Housing variations
        ('housing_quality', 'housing_q'),
        ('housing_quality', 'hq'),
        # Distance variations
        ('distance_to_waterbodies', 'dist_water'),
        ('distance_to_waterbodies', 'water_distance'),
        # Nighttime lights variations
        ('nighttime_lights', 'night_lights'),
        ('nighttime_lights', 'ntl'),
        ('nighttime_lights', 'lights')
    ]
    
    var1_lower = var1.lower()
    var2_lower = var2.lower()
    
    for v1, v2 in variations:
        if (v1 in var1_lower and v2 in var2_lower) or (v2 in var1_lower and v1 in var2_lower):
            return True
    
    return False

def detect_geopolitical_zone(cleaned_data: pd.DataFrame, shapefile_data: Optional[gpd.GeoDataFrame] = None) -> Tuple[Optional[str], str]:
    """
    Detect the geopolitical zone from uploaded data.
    
    Args:
        cleaned_data: Cleaned CSV DataFrame with ward data
        shapefile_data: Optional shapefile GeoDataFrame
        
    Returns:
        Tuple of (zone_name, detection_method)
    """
    try:
        # Method 1: Look for StateCode column
        if 'StateCode' in cleaned_data.columns:
            state_codes = cleaned_data['StateCode'].dropna().unique()
            if len(state_codes) > 0:
                # Use first non-null state code
                state_code = str(state_codes[0]).strip()
                zone = get_zone_from_state_code(state_code)
                if zone:
                    logger.info(f"Detected zone '{zone}' from StateCode: {state_code}")
                    return zone, f"StateCode: {state_code}"
        
        # Method 2: Look for State column
        if 'State' in cleaned_data.columns:
            states = cleaned_data['State'].dropna().unique()
            if len(states) > 0:
                state_name = str(states[0]).strip()
                zone = get_zone_from_state_name(state_name)
                if zone:
                    logger.info(f"Detected zone '{zone}' from State: {state_name}")
                    return zone, f"State: {state_name}"
        
        # Method 3: Try to infer from WardName patterns
        if 'WardName' in cleaned_data.columns:
            ward_names = cleaned_data['WardName'].dropna().unique()
            zone = infer_zone_from_ward_names(ward_names)
            if zone:
                logger.info(f"Inferred zone '{zone}' from ward name patterns")
                return zone, "Ward name patterns"
        
        # Method 4: Check shapefile for state information
        if shapefile_data is not None:
            for col in ['State', 'STATE_NAME', 'StateName']:
                if col in shapefile_data.columns:
                    states = shapefile_data[col].dropna().unique()
                    if len(states) > 0:
                        state_name = str(states[0]).strip()
                        zone = get_zone_from_state_name(state_name)
                        if zone:
                            logger.info(f"Detected zone '{zone}' from shapefile {col}: {state_name}")
                            return zone, f"Shapefile {col}: {state_name}"
        
        logger.warning("Could not detect geopolitical zone from uploaded data")
        return None, "No zone detected"
        
    except Exception as e:
        logger.error(f"Error detecting geopolitical zone: {e}")
        return None, f"Error: {str(e)}"

def get_zone_from_state_code(state_code: str) -> Optional[str]:
    """Get zone from Nigerian state code."""
    # Common state code mappings
    state_code_mapping = {
        'KN': 'Kano', 'KD': 'Kaduna', 'JG': 'Jigawa', 'KT': 'Katsina',
        'KB': 'Kebbi', 'SK': 'Sokoto', 'ZF': 'Zamfara',  # North West
        'AD': 'Adamawa', 'BC': 'Bauchi', 'BO': 'Borno', 'GM': 'Gombe',
        'TR': 'Taraba', 'YB': 'Yobe',  # North East
        'BN': 'Benue', 'KG': 'Kogi', 'KW': 'Kwara', 'NS': 'Nasarawa',
        'NG': 'Niger', 'PL': 'Plateau', 'FCT': 'FCT',  # North Central
        'AB': 'Abia', 'AN': 'Anambra', 'EB': 'Ebonyi', 'EN': 'Enugu',
        'IM': 'Imo',  # South East
        'AK': 'Akwa Ibom', 'BY': 'Bayelsa', 'CR': 'Cross River',
        'DT': 'Delta', 'ED': 'Edo', 'RV': 'Rivers',  # South South
        'EK': 'Ekiti', 'LG': 'Lagos', 'OG': 'Ogun', 'ON': 'Ondo',
        'OS': 'Osun', 'OY': 'Oyo'  # South West
    }
    
    state_name = state_code_mapping.get(state_code.upper())
    if state_name:
        return get_zone_from_state_name(state_name)
    return None

def get_zone_from_state_name(state_name: str) -> Optional[str]:
    """Get zone from state name."""
    state_name = state_name.strip()
    
    # Direct exact matches first (case-insensitive)
    for zone, states in GEOPOLITICAL_ZONES.items():
        for state in states:
            if state_name.lower() == state.lower():
                return zone
    
    # Fuzzy matching for partial matches
    for zone, states in GEOPOLITICAL_ZONES.items():
        for state in states:
            if state.lower() in state_name.lower() or state_name.lower() in state.lower():
                return zone
    
    return None

def infer_zone_from_ward_names(ward_names: List[str]) -> Optional[str]:
    """Infer zone from ward name patterns (basic heuristic)."""
    # This is a simplified heuristic - could be enhanced with more sophisticated patterns
    ward_text = ' '.join(ward_names).lower()
    
    # Look for distinctive patterns
    if any(pattern in ward_text for pattern in ['kano', 'kaduna', 'sokoto']):
        return 'North_West'
    elif any(pattern in ward_text for pattern in ['borno', 'adamawa', 'bauchi']):
        return 'North_East'
    elif any(pattern in ward_text for pattern in ['lagos', 'ogun', 'oyo']):
        return 'South_West'
    elif any(pattern in ward_text for pattern in ['rivers', 'delta', 'bayelsa']):
        return 'South_South'
    elif any(pattern in ward_text for pattern in ['enugu', 'anambra', 'imo']):
        return 'South_East'
    elif any(pattern in ward_text for pattern in ['plateau', 'benue', 'niger']):
        return 'North_Central'
    
    return None

def select_variables_by_zone(zone: str, available_variables: List[str], 
                           llm_manager=None, cleaned_data=None, min_variables: int = 2) -> List[str]:
    """
    Select the exact scientifically-validated variables for the detected zone.
    Falls back to LLM selection if too many variables are missing.
    
    Args:
        zone: Geopolitical zone name
        available_variables: List of available variables in the data
        llm_manager: Optional LLM manager for fallback selection
        cleaned_data: Optional cleaned data for LLM context
        min_variables: Minimum number of variables required for robust analysis
        
    Returns:
        List of zone-specific variables that are available in the data
    """
    if zone not in ZONE_VARIABLES:
        logger.warning(f"No variable mapping for zone: {zone}")
        # Fallback to LLM selection if available, otherwise use all variables
        if llm_manager and cleaned_data is not None:
            logger.info("Attempting LLM fallback for unknown zone")
            return _fallback_to_llm_selection(available_variables, llm_manager, cleaned_data, zone, min_variables)
        else:
            # Use conservative filtering for unknown zones
            fallback_vars = [var for var in available_variables if var not in IDENTIFIER_COLUMNS]
            # If still empty, use numeric columns as emergency fallback
            if len(fallback_vars) == 0:
                numeric_cols = cleaned_data.select_dtypes(include=['number']).columns.tolist()
                fallback_vars = [col for col in numeric_cols if 'name' not in col.lower() and 'code' not in col.lower()]
                logger.warning(f"🚨 Emergency fallback for unknown zone: using {len(fallback_vars)} numeric variables")
            logger.info(f"Using basic fallback: {len(fallback_vars)} available variables")
            return _ensure_minimum_variables(fallback_vars, available_variables, cleaned_data, min_variables)
    
    # Get the scientifically-validated variable list for this zone
    zone_variable_list = ZONE_VARIABLES[zone]
    selected_variables = []
    missing_variables = []
    
    # ENHANCED: First ensure core malaria variables are prioritized
    prioritized_variables = []
    
    # Add core variables first (highest priority)
    for core_var in CORE_VARIABLES:
        if core_var in zone_variable_list:
            prioritized_variables.append(core_var)
    
    # Add remaining zone variables
    for var in zone_variable_list:
        if var not in prioritized_variables:
            prioritized_variables.append(var)
    
    # Select variables using fuzzy matching for flexibility
    for var in prioritized_variables:
        # First try exact match
        if var in available_variables:
            selected_variables.append(var)
        else:
            # Try fuzzy matching
            matched_var = find_fuzzy_match(var, available_variables)
            if matched_var:
                selected_variables.append(matched_var)
                logger.info(f"🔍 Fuzzy matched '{var}' to '{matched_var}'")
            else:
                missing_variables.append(var)
    
    # ENHANCED: Check if we have enough variables from the region-specific list
    missing_percentage = len(missing_variables) / len(zone_variable_list) if zone_variable_list else 0
    
    if missing_variables:
        logger.warning(f"Zone {zone}: Missing {len(missing_variables)} expected variables: {missing_variables}")
        
        # If more than 50% of expected variables are missing, fallback to LLM
        if missing_percentage > 0.5 and llm_manager and cleaned_data is not None:
            logger.info(f"Zone {zone}: {missing_percentage*100:.1f}% variables missing, falling back to LLM selection")
            return _fallback_to_llm_selection(available_variables, llm_manager, cleaned_data, zone, min_variables)
    
    # ENHANCED: Ensure we have minimum variables for analysis (composite needs at least 2)
    if len(selected_variables) < min_variables:
        logger.warning(f"Zone {zone}: Only {len(selected_variables)} variables selected, minimum {min_variables} required")
        
        # Try to find additional core variables using fuzzy matching
        additional_vars = []
        for core_var in CORE_VARIABLES:
            if core_var not in selected_variables:
                # First try exact match
                if core_var in available_variables:
                    additional_vars.append(core_var)
                else:
                    # Try fuzzy match
                    matched_var = find_fuzzy_match(core_var, available_variables)
                    if matched_var and matched_var not in selected_variables:
                        additional_vars.append(matched_var)
                        logger.info(f"🎯 Core variable fuzzy matched '{core_var}' to '{matched_var}'")
        
        selected_variables.extend(additional_vars)
        
        # If still too few variables, fallback to LLM or emergency selection
        if len(selected_variables) < min_variables:
            if llm_manager and cleaned_data is not None:
                logger.info(f"Zone {zone}: Only {len(selected_variables)} variables available, falling back to LLM selection")
                return _fallback_to_llm_selection(available_variables, llm_manager, cleaned_data, zone, min_variables)
            else:
                # Emergency fallback: add any numeric variables
                logger.warning(f"Zone {zone}: Emergency fallback - adding any available numeric variables")
                selected_variables = _ensure_minimum_variables(selected_variables, available_variables, cleaned_data, min_variables)
    
    # ENHANCED: Validate variable quality
    final_variables = _validate_variable_quality(selected_variables, cleaned_data)
    
    logger.info(f"Zone {zone}: Selected {len(final_variables)}/{len(zone_variable_list)} scientifically-validated variables")
    return final_variables

def get_zone_metadata(zone: str) -> Dict[str, Any]:
    """Get metadata about a geopolitical zone."""
    zone_metadata = {
        'North_Central': {
            'states': GEOPOLITICAL_ZONES['North_Central'],
            'climate': 'Guinea savanna, moderate rainfall',
            'key_risks': ['moderate malaria burden', 'seasonal flooding', 'temperature variation'],
            'priority_focus': 'balanced environmental and demographic factors'
        },
        'North_East': {
            'states': GEOPOLITICAL_ZONES['North_East'],
            'climate': 'Sudan/Sahel savanna, low rainfall, high temperature',
            'key_risks': ['extreme heat', 'water scarcity', 'drought conditions'],
            'priority_focus': 'climate stress and water access'
        },
        'North_West': {
            'states': GEOPOLITICAL_ZONES['North_West'],
            'climate': 'Sudan savanna, moderate to low rainfall',
            'key_risks': ['high population density', 'moderate malaria burden'],
            'priority_focus': 'population and settlement factors'
        },
        'South_East': {
            'states': GEOPOLITICAL_ZONES['South_East'],
            'climate': 'Forest/derived savanna, high rainfall',
            'key_risks': ['flooding', 'high humidity', 'dense urbanization'],
            'priority_focus': 'flood management and urban development'
        },
        'South_South': {
            'states': GEOPOLITICAL_ZONES['South_South'],
            'climate': 'Coastal/mangrove, very high rainfall',
            'key_risks': ['coastal flooding', 'extremely high humidity', 'water logging'],
            'priority_focus': 'flood and coastal management'
        },
        'South_West': {
            'states': GEOPOLITICAL_ZONES['South_West'],
            'climate': 'Forest/derived savanna, high rainfall',
            'key_risks': ['high urbanization', 'urban flooding', 'population density'],
            'priority_focus': 'urban development and infrastructure'
        }
    }
    
    return zone_metadata.get(zone, {})


def _ensure_minimum_variables(selected_variables: List[str], available_variables: List[str], 
                             cleaned_data: pd.DataFrame, min_variables: int) -> List[str]:
    """
    Ensure minimum number of variables for robust analysis.
    
    Args:
        selected_variables: Currently selected variables
        available_variables: All available variables
        cleaned_data: Cleaned data for validation
        min_variables: Minimum number of variables required
        
    Returns:
        List of variables meeting minimum requirement
    """
    if len(selected_variables) >= min_variables:
        return selected_variables
    
    # Get numeric columns that aren't already selected
    numeric_cols = cleaned_data.select_dtypes(include=['number']).columns.tolist()
    additional_vars = []
    
    for col in numeric_cols:
        if col not in selected_variables and col not in IDENTIFIER_COLUMNS:
            # Check if variable has sufficient variance
            if cleaned_data[col].var() > 0 and cleaned_data[col].notna().sum() > len(cleaned_data) * 0.5:
                additional_vars.append(col)
                if len(selected_variables) + len(additional_vars) >= min_variables:
                    break
    
    result = selected_variables + additional_vars
    logger.info(f"🔧 Ensured minimum variables: {len(result)} variables (added {len(additional_vars)})")
    return result


def _validate_variable_quality(selected_variables: List[str], cleaned_data: pd.DataFrame) -> List[str]:
    """
    Validate quality of selected variables.
    
    Args:
        selected_variables: Variables to validate
        cleaned_data: Cleaned data for validation
        
    Returns:
        List of validated variables
    """
    if cleaned_data is None:
        return selected_variables
    
    validated_vars = []
    
    for var in selected_variables:
        if var not in cleaned_data.columns:
            logger.warning(f"⚠️ Variable '{var}' not found in data")
            continue
        
        # Check for sufficient data
        non_null_count = cleaned_data[var].notna().sum()
        data_coverage = non_null_count / len(cleaned_data)
        
        if data_coverage < 0.5:
            logger.warning(f"⚠️ Variable '{var}' has insufficient data coverage: {data_coverage:.1%}")
            continue
        
        # Check for variance
        if pd.api.types.is_numeric_dtype(cleaned_data[var]):
            if cleaned_data[var].var() == 0:
                logger.warning(f"⚠️ Variable '{var}' has zero variance")
                continue
        
        validated_vars.append(var)
    
    logger.info(f"✅ Validated {len(validated_vars)}/{len(selected_variables)} variables")
    return validated_vars


def _fallback_to_llm_selection(available_variables: List[str], llm_manager, cleaned_data: pd.DataFrame, 
                              zone: str, min_vars: int = 2, max_vars: int = 7) -> List[str]:
    """
    Fallback to LLM-based variable selection when region-specific variables aren't available.
    
    Args:
        available_variables: List of available variables in the data
        llm_manager: LLM manager instance
        cleaned_data: Cleaned data for context
        zone: Detected zone (for context)
        min_vars: Minimum number of variables to select
        max_vars: Maximum number of variables to select
        
    Returns:
        List of LLM-selected variables
    """
    try:
        from ..core.llm_manager import select_optimal_variables_with_llm
        
        # Filter out identifier columns
        analysis_variables = [var for var in available_variables if var not in IDENTIFIER_COLUMNS]
        
        if len(analysis_variables) < min_vars:
            logger.warning(f"Not enough analysis variables ({len(analysis_variables)}) for LLM selection")
            return analysis_variables
        
        logger.info(f"🧠 LLM FALLBACK: Selecting optimal variables from {len(analysis_variables)} available")
        
        # Use LLM to select optimal variables
        selected_vars, explanation = select_optimal_variables_with_llm(
            llm_manager=llm_manager,
            available_vars=analysis_variables,
            csv_data=cleaned_data,
            min_vars=min_vars,
            max_vars=max_vars
        )
        
        # Ensure core malaria variables are included using fuzzy matching
        for core_var in CORE_VARIABLES:
            if core_var not in selected_vars and len(selected_vars) < max_vars:
                # First try exact match
                if core_var in analysis_variables:
                    selected_vars.append(core_var)
                else:
                    # Try fuzzy match
                    matched_var = find_fuzzy_match(core_var, analysis_variables)
                    if matched_var and matched_var not in selected_vars:
                        selected_vars.append(matched_var)
                        logger.info(f"🎯 LLM selection: Core variable fuzzy matched '{core_var}' to '{matched_var}'")
        
        logger.info(f"✅ LLM SELECTION: Selected {len(selected_vars)} variables: {selected_vars}")
        logger.info(f"📝 LLM REASONING: {explanation}")
        
        return selected_vars[:max_vars]  # Ensure we don't exceed max
        
    except Exception as e:
        logger.error(f"Error in LLM fallback selection: {e}")
        # Final fallback: return first available variables with core variables prioritized using fuzzy matching
        analysis_variables = [var for var in available_variables if var not in IDENTIFIER_COLUMNS]
        fallback_selection = []
        
        # First add core variables using fuzzy matching
        for core_var in CORE_VARIABLES:
            if core_var in analysis_variables:
                fallback_selection.append(core_var)
            else:
                matched_var = find_fuzzy_match(core_var, analysis_variables)
                if matched_var and matched_var not in fallback_selection:
                    fallback_selection.append(matched_var)
        
        # Then add other variables
        other_vars = [var for var in analysis_variables if var not in fallback_selection]
        fallback_selection.extend(other_vars)
        
        return fallback_selection[:max_vars]

def apply_region_aware_selection(cleaned_data: pd.DataFrame, 
                                shapefile_data: Optional[gpd.GeoDataFrame] = None,
                                llm_manager=None) -> Dict[str, Any]:
    """
    Apply complete region-aware variable selection workflow with LLM fallback.
    
    Args:
        cleaned_data: Cleaned CSV DataFrame with ward data
        shapefile_data: Optional shapefile GeoDataFrame
        llm_manager: Optional LLM manager for fallback selection
        
    Returns:
        Dictionary with selection results and metadata
    """
    try:
        # Step 1: Detect geopolitical zone
        zone, detection_method = detect_geopolitical_zone(cleaned_data, shapefile_data)
        
        # Step 2: Get available analysis variables (exclude identifiers)
        available_variables = [col for col in cleaned_data.columns if col not in IDENTIFIER_COLUMNS]
        
        # DEBUG: Log column filtering process
        logger.info(f"📊 COLUMN ANALYSIS: Total columns in data: {len(cleaned_data.columns)}")
        logger.info(f"🔍 IDENTIFIER COLUMNS: {IDENTIFIER_COLUMNS}")
        logger.info(f"✅ ANALYSIS VARIABLES: {len(available_variables)} available - {available_variables[:10]}{'...' if len(available_variables) > 10 else ''}")
        
        # Safety check: If no variables after filtering, use a more conservative approach
        if len(available_variables) == 0:
            logger.warning("❌ No variables found after identifier filtering - using conservative filtering")
            # Only exclude obvious identifier columns
            conservative_identifiers = ['WardName', 'ward_name', 'WardCode', 'ward_code']
            available_variables = [col for col in cleaned_data.columns if col not in conservative_identifiers]
            logger.info(f"🔄 CONSERVATIVE FILTER: {len(available_variables)} variables available")
        
        # Step 3: Select variables based on zone with LLM fallback
        selection_method = 'region_specific'
        if zone:
            selected_variables = select_variables_by_zone(zone, available_variables, llm_manager, cleaned_data)
            zone_metadata = get_zone_metadata(zone)
            
            # Check if LLM fallback was used
            if zone in ZONE_VARIABLES:
                expected_vars = ZONE_VARIABLES[zone]
                matched_vars = [var for var in selected_variables if var in expected_vars]
                if len(matched_vars) < len(expected_vars) * 0.5:  # Less than 50% match
                    selection_method = 'llm_fallback'
        else:
            # No zone detected - try LLM selection
            if llm_manager:
                logger.info("No zone detected, attempting LLM variable selection")
                selected_variables = _fallback_to_llm_selection(available_variables, llm_manager, cleaned_data, 'unknown')
                selection_method = 'llm_fallback'
            else:
                # Final fallback: use all available analysis variables
                selected_variables = available_variables
                selection_method = 'all_available'
            zone_metadata = {}
        
        # CRITICAL SAFETY CHECK: Never return empty variable list
        if len(selected_variables) == 0:
            logger.error("🚨 CRITICAL: Variable selection returned 0 variables - applying emergency fallback")
            # Emergency fallback: use any numeric columns that aren't obviously identifiers
            numeric_cols = cleaned_data.select_dtypes(include=['number']).columns.tolist()
            emergency_variables = [col for col in numeric_cols if 'name' not in col.lower() and 'code' not in col.lower()]
            
            if len(emergency_variables) >= 2:
                selected_variables = emergency_variables[:7]  # Limit to 7 variables max
                selection_method = 'emergency_numeric_fallback'
                logger.info(f"🆘 EMERGENCY FALLBACK: Using {len(selected_variables)} numeric variables: {selected_variables}")
            else:
                # Ultimate last resort - use any non-identifier columns
                all_non_id_cols = [col for col in cleaned_data.columns if 'name' not in col.lower() and 'code' not in col.lower()]
                if len(all_non_id_cols) >= 2:
                    selected_variables = all_non_id_cols[:7]
                    selection_method = 'ultimate_fallback'
                    logger.warning(f"🔥 ULTIMATE FALLBACK: Using {len(selected_variables)} columns: {selected_variables}")
                else:
                    # This should never happen but prevents complete failure
                    logger.critical("💥 CRITICAL ERROR: Cannot find any suitable variables for analysis")
                    return {
                        'status': 'error',
                        'message': 'No suitable variables found for analysis in the uploaded data',
                        'zone_detected': zone,
                        'detection_method': detection_method,
                        'selected_variables': [],
                        'selection_method': 'failed',
                        'zone_metadata': {},
                        'total_available_variables': len(available_variables),
                        'variables_selected': 0
                    }
        
        return {
            'status': 'success',
            'zone_detected': zone,
            'detection_method': detection_method,
            'selected_variables': selected_variables,
            'selection_method': selection_method,
            'zone_metadata': zone_metadata,
            'total_available_variables': len(available_variables),
            'variables_selected': len(selected_variables)
        }
        
    except Exception as e:
        logger.error(f"Error in region-aware selection: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'zone_detected': None,
            'selected_variables': [col for col in cleaned_data.columns if col not in IDENTIFIER_COLUMNS],
            'selection_method': 'error_fallback'
        }