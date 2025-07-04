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

# Identifier columns (not analysis variables)
IDENTIFIER_COLUMNS = ['WardName', 'StateCode', 'WardCode', 'LGACode', 'ward_name', 'ward_code']

def detect_geopolitical_zone(csv_data: pd.DataFrame, shapefile_data: Optional[gpd.GeoDataFrame] = None) -> Tuple[Optional[str], str]:
    """
    Detect the geopolitical zone from uploaded data.
    
    Args:
        csv_data: CSV DataFrame with ward data
        shapefile_data: Optional shapefile GeoDataFrame
        
    Returns:
        Tuple of (zone_name, detection_method)
    """
    try:
        # Method 1: Look for StateCode column
        if 'StateCode' in csv_data.columns:
            state_codes = csv_data['StateCode'].dropna().unique()
            if len(state_codes) > 0:
                # Use first non-null state code
                state_code = str(state_codes[0]).strip()
                zone = get_zone_from_state_code(state_code)
                if zone:
                    logger.info(f"Detected zone '{zone}' from StateCode: {state_code}")
                    return zone, f"StateCode: {state_code}"
        
        # Method 2: Look for State column
        if 'State' in csv_data.columns:
            states = csv_data['State'].dropna().unique()
            if len(states) > 0:
                state_name = str(states[0]).strip()
                zone = get_zone_from_state_name(state_name)
                if zone:
                    logger.info(f"Detected zone '{zone}' from State: {state_name}")
                    return zone, f"State: {state_name}"
        
        # Method 3: Try to infer from WardName patterns
        if 'WardName' in csv_data.columns:
            ward_names = csv_data['WardName'].dropna().unique()
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
                           llm_manager=None, csv_data=None) -> List[str]:
    """
    Select the exact scientifically-validated variables for the detected zone.
    Falls back to LLM selection if too many variables are missing.
    
    Args:
        zone: Geopolitical zone name
        available_variables: List of available variables in the data
        llm_manager: Optional LLM manager for fallback selection
        csv_data: Optional CSV data for LLM context
        
    Returns:
        List of zone-specific variables that are available in the data
    """
    if zone not in ZONE_VARIABLES:
        logger.warning(f"No variable mapping for zone: {zone}")
        # Fallback to LLM selection if available, otherwise use all variables
        if llm_manager and csv_data is not None:
            logger.info("Attempting LLM fallback for unknown zone")
            return _fallback_to_llm_selection(available_variables, llm_manager, csv_data, zone)
        else:
            fallback_vars = [var for var in available_variables if var not in IDENTIFIER_COLUMNS]
            logger.info(f"Using basic fallback: {len(fallback_vars)} available variables")
            return fallback_vars
    
    # Get the scientifically-validated variable list for this zone
    zone_variable_list = ZONE_VARIABLES[zone]
    selected_variables = []
    missing_variables = []
    
    # Select only the variables that are available in the uploaded data
    for var in zone_variable_list:
        if var in available_variables:
            selected_variables.append(var)
        else:
            missing_variables.append(var)
    
    # Check if we have enough variables from the region-specific list
    missing_percentage = len(missing_variables) / len(zone_variable_list) if zone_variable_list else 0
    
    if missing_variables:
        logger.warning(f"Zone {zone}: Missing {len(missing_variables)} expected variables: {missing_variables}")
        
        # If more than 50% of expected variables are missing, fallback to LLM
        if missing_percentage > 0.5 and llm_manager and csv_data is not None:
            logger.info(f"Zone {zone}: {missing_percentage*100:.1f}% variables missing, falling back to LLM selection")
            return _fallback_to_llm_selection(available_variables, llm_manager, csv_data, zone)
    
    # Ensure we have at least the core malaria variables if available
    if len(selected_variables) < 3:
        core_available = [var for var in CORE_VARIABLES if var in available_variables and var not in selected_variables]
        selected_variables.extend(core_available)
        
        # If still too few variables, fallback to LLM
        if len(selected_variables) < 3 and llm_manager and csv_data is not None:
            logger.info(f"Zone {zone}: Only {len(selected_variables)} variables available, falling back to LLM selection")
            return _fallback_to_llm_selection(available_variables, llm_manager, csv_data, zone)
    
    logger.info(f"Zone {zone}: Selected {len(selected_variables)}/{len(zone_variable_list)} scientifically-validated variables")
    return selected_variables

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


def _fallback_to_llm_selection(available_variables: List[str], llm_manager, csv_data: pd.DataFrame, 
                              zone: str, min_vars: int = 3, max_vars: int = 7) -> List[str]:
    """
    Fallback to LLM-based variable selection when region-specific variables aren't available.
    
    Args:
        available_variables: List of available variables in the data
        llm_manager: LLM manager instance
        csv_data: CSV data for context
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
            csv_data=csv_data,
            min_vars=min_vars,
            max_vars=max_vars
        )
        
        # Ensure core malaria variables are included if available
        core_vars_available = [var for var in CORE_VARIABLES if var in analysis_variables]
        for core_var in core_vars_available:
            if core_var not in selected_vars and len(selected_vars) < max_vars:
                selected_vars.append(core_var)
        
        logger.info(f"✅ LLM SELECTION: Selected {len(selected_vars)} variables: {selected_vars}")
        logger.info(f"📝 LLM REASONING: {explanation}")
        
        return selected_vars[:max_vars]  # Ensure we don't exceed max
        
    except Exception as e:
        logger.error(f"Error in LLM fallback selection: {e}")
        # Final fallback: return first available variables with core variables prioritized
        analysis_variables = [var for var in available_variables if var not in IDENTIFIER_COLUMNS]
        core_vars = [var for var in CORE_VARIABLES if var in analysis_variables]
        other_vars = [var for var in analysis_variables if var not in CORE_VARIABLES]
        
        fallback_selection = core_vars + other_vars
        return fallback_selection[:max_vars]

def apply_region_aware_selection(csv_data: pd.DataFrame, 
                                shapefile_data: Optional[gpd.GeoDataFrame] = None,
                                llm_manager=None) -> Dict[str, Any]:
    """
    Apply complete region-aware variable selection workflow with LLM fallback.
    
    Args:
        csv_data: CSV DataFrame with ward data
        shapefile_data: Optional shapefile GeoDataFrame
        llm_manager: Optional LLM manager for fallback selection
        
    Returns:
        Dictionary with selection results and metadata
    """
    try:
        # Step 1: Detect geopolitical zone
        zone, detection_method = detect_geopolitical_zone(csv_data, shapefile_data)
        
        # Step 2: Get available analysis variables (exclude identifiers)
        available_variables = [col for col in csv_data.columns if col not in IDENTIFIER_COLUMNS]
        
        # Step 3: Select variables based on zone with LLM fallback
        selection_method = 'region_specific'
        if zone:
            selected_variables = select_variables_by_zone(zone, available_variables, llm_manager, csv_data)
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
                selected_variables = _fallback_to_llm_selection(available_variables, llm_manager, csv_data, 'unknown')
                selection_method = 'llm_fallback'
            else:
                # Final fallback: use all available analysis variables
                selected_variables = available_variables
                selection_method = 'all_available'
            zone_metadata = {}
        
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
            'selected_variables': [col for col in csv_data.columns if col not in IDENTIFIER_COLUMNS],
            'selection_method': 'error_fallback'
        }