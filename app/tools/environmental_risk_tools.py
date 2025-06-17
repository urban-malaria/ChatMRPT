"""
Environmental Risk Analysis Tools for ChatMRPT

These tools analyze environmental factors and their correlation with malaria burden.
Handles requests related to environmental drivers like flooding, water proximity,
elevation, vegetation, and temperature.
"""

import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[pd.DataFrame]:
    """Get the unified dataset from session."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            logger.debug(f"✅ Unified dataset loaded: {len(unified_gdf)} rows")
            return unified_gdf
        else:
            logger.error(f"❌ Unified dataset not found for session {session_id}")
            return None
    except Exception as e:
        logger.error(f"❌ Error accessing unified dataset: {e}")
        return None


def get_flood_prone_wards(session_id: str, location_filter: Optional[str] = None, 
                         threshold: float = 0.5) -> Dict[str, Any]:
    """
    Identify flood-prone wards.
    
    Handles requests like:
    - "Which wards in Kano State are flood-prone?"
    - "List flood-prone wards"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply location filtering if specified
        if location_filter:
            location_filter = location_filter.upper()
            location_columns = [col for col in df.columns if any(term in col.lower() for term in ['state', 'lga', 'ward'])]
            
            mask = pd.Series([False] * len(df))
            for col in location_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            df = df[mask]
            
            if len(df) == 0:
                return {"error": f"No wards found matching location filter: {location_filter}"}
        
        # Check if flood column exists
        if 'flood' not in df.columns:
            return {"error": "Flood data not available in dataset"}
        
        # Find flood-prone wards (above threshold)
        flood_prone = df[df['flood'] >= threshold]
        
        if len(flood_prone) == 0:
            return {"error": f"No flood-prone wards found with threshold >= {threshold}"}
        
        # Get ward information
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        results = flood_prone[[ward_col, 'flood']].copy()
        if 'StateCode_x' in flood_prone.columns:
            results['State'] = flood_prone['StateCode_x']
        if 'composite_score' in flood_prone.columns:
            results['Burden_Score'] = flood_prone['composite_score']
        
        results = results.sort_values('flood', ascending=False)
        
        return {
            'status': 'success',
            'flood_prone_wards': results.to_dict('records'),
            'summary': {
                'total_flood_prone': len(flood_prone),
                'percentage_of_total': round((len(flood_prone) / len(df)) * 100, 1),
                'threshold_used': threshold,
                'highest_flood_risk': results.iloc[0][ward_col] if len(results) > 0 else None,
                'location_filter': location_filter
            }
        }
        
    except Exception as e:
        logger.error(f"Error identifying flood-prone wards: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def analyze_water_proximity_correlation(session_id: str, location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyze correlation between water proximity and malaria burden.
    
    Handles requests like:
    - "Does proximity to water bodies correlate with higher malaria burden in Kano?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply location filtering if specified
        if location_filter:
            location_filter = location_filter.upper()
            location_columns = [col for col in df.columns if any(term in col.lower() for term in ['state', 'lga', 'ward'])]
            
            mask = pd.Series([False] * len(df))
            for col in location_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            df = df[mask]
            
            if len(df) == 0:
                return {"error": f"No wards found matching location filter: {location_filter}"}
        
        # Check required columns
        if 'distance_to_water' not in df.columns:
            return {"error": "Water distance data not available in dataset"}
        
        burden_col = None
        if 'composite_score' in df.columns:
            burden_col = 'composite_score'
        elif 'pca_score' in df.columns:
            burden_col = 'pca_score'
        else:
            return {"error": "No burden indicator available for correlation analysis"}
        
        # Remove missing values
        clean_df = df[[burden_col, 'distance_to_water']].dropna()
        
        if len(clean_df) < 10:
            return {"error": "Insufficient data for correlation analysis (minimum 10 wards required)"}
        
        # Calculate correlation
        correlation, p_value = stats.pearsonr(clean_df[burden_col], clean_df['distance_to_water'])
        
        # Categorize wards by water proximity
        close_to_water = df[df['distance_to_water'] <= df['distance_to_water'].quantile(0.25)]
        far_from_water = df[df['distance_to_water'] >= df['distance_to_water'].quantile(0.75)]
        
        close_burden_mean = close_to_water[burden_col].mean()
        far_burden_mean = far_from_water[burden_col].mean()
        
        return {
            'status': 'success',
            'correlation_analysis': {
                'correlation_coefficient': float(correlation),
                'p_value': float(p_value),
                'significance': 'Significant' if p_value < 0.05 else 'Not significant',
                'interpretation': _interpret_water_correlation(correlation, p_value)
            },
            'proximity_comparison': {
                'close_to_water_mean_burden': float(close_burden_mean),
                'far_from_water_mean_burden': float(far_burden_mean),
                'difference': float(close_burden_mean - far_burden_mean),
                'wards_close_to_water': len(close_to_water),
                'wards_far_from_water': len(far_from_water)
            },
            'location_filter': location_filter,
            'total_wards_analyzed': len(clean_df)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing water proximity correlation: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def get_ward_elevation_profile(session_id: str, ward_name: str) -> Dict[str, Any]:
    """
    Get elevation profile for a specific ward.
    
    Handles requests like:
    - "What is the elevation profile of Ward X?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Find ward
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        ward_data = df[df[ward_col].str.contains(ward_name, case=False, na=False, regex=False)]
        
        if len(ward_data) == 0:
            return {"error": f"Ward '{ward_name}' not found in dataset"}
        
        if len(ward_data) > 1:
            ward_matches = ward_data[ward_col].tolist()
            return {"error": f"Multiple wards found: {ward_matches}. Please be more specific."}
        
        ward_info = ward_data.iloc[0]
        
        # Check elevation data
        if 'elevation' not in df.columns:
            return {"error": "Elevation data not available in dataset"}
        
        ward_elevation = ward_info['elevation']
        
        # Calculate elevation context
        all_elevations = df['elevation'].dropna()
        elevation_percentile = (all_elevations < ward_elevation).mean() * 100
        
        elevation_category = "High" if elevation_percentile >= 75 else "Medium" if elevation_percentile >= 25 else "Low"
        
        return {
            'status': 'success',
            'ward_name': ward_info[ward_col],
            'elevation_meters': float(ward_elevation) if pd.notna(ward_elevation) else None,
            'elevation_category': elevation_category,
            'elevation_percentile': float(elevation_percentile),
            'context': {
                'min_elevation_in_dataset': float(all_elevations.min()),
                'max_elevation_in_dataset': float(all_elevations.max()),
                'mean_elevation_in_dataset': float(all_elevations.mean())
            },
            'interpretation': f"{ward_info[ward_col]} has {elevation_category.lower()} elevation ({ward_elevation:.1f}m), ranking in the {elevation_percentile:.1f}th percentile"
        }
        
    except Exception as e:
        logger.error(f"Error getting elevation profile: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def get_high_vegetation_wards(session_id: str, location_filter: Optional[str] = None, 
                             vegetation_threshold: float = 0.75) -> Dict[str, Any]:
    """
    Find wards with high vegetation density.
    
    Handles requests like:
    - "List wards with high vegetation density and compact buildings"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply location filtering if specified
        if location_filter:
            location_filter = location_filter.upper()
            location_columns = [col for col in df.columns if any(term in col.lower() for term in ['state', 'lga', 'ward'])]
            
            mask = pd.Series([False] * len(df))
            for col in location_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            df = df[mask]
        
        # Check vegetation columns
        vegetation_cols = []
        if 'mean_EVI' in df.columns:
            vegetation_cols.append('mean_EVI')
        if 'mean_NDVI' in df.columns:
            vegetation_cols.append('mean_NDVI')
        
        if not vegetation_cols:
            return {"error": "No vegetation data (EVI/NDVI) available in dataset"}
        
        # Use NDVI as primary vegetation indicator
        veg_col = 'mean_NDVI' if 'mean_NDVI' in vegetation_cols else vegetation_cols[0]
        
        # Find high vegetation wards
        high_veg_threshold = df[veg_col].quantile(vegetation_threshold)
        high_veg_wards = df[df[veg_col] >= high_veg_threshold]
        
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        results = high_veg_wards[[ward_col, veg_col]].copy()
        if 'StateCode_x' in high_veg_wards.columns:
            results['State'] = high_veg_wards['StateCode_x']
        if 'composite_score' in high_veg_wards.columns:
            results['Burden_Score'] = high_veg_wards['composite_score']
        
        # Add building compactness if available
        if 'urbanPercentage' in high_veg_wards.columns:
            results['Urban_Percentage'] = high_veg_wards['urbanPercentage']
        
        results = results.sort_values(veg_col, ascending=False)
        
        return {
            'status': 'success',
            'high_vegetation_wards': results.to_dict('records'),
            'summary': {
                'total_high_vegetation_wards': len(high_veg_wards),
                'vegetation_indicator': veg_col,
                'threshold_percentile': vegetation_threshold,
                'threshold_value': float(high_veg_threshold),
                'location_filter': location_filter
            }
        }
        
    except Exception as e:
        logger.error(f"Error finding high vegetation wards: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def analyze_low_lying_areas_risk(session_id: str, location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyze if low-lying areas have higher malaria risk.
    
    Handles requests like:
    - "Are low-lying wards in Kano at higher malaria risk?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply location filtering if specified
        if location_filter:
            location_filter = location_filter.upper()
            location_columns = [col for col in df.columns if any(term in col.lower() for term in ['state', 'lga', 'ward'])]
            
            mask = pd.Series([False] * len(df))
            for col in location_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            df = df[mask]
        
        # Check required columns
        if 'elevation' not in df.columns:
            return {"error": "Elevation data not available in dataset"}
        
        burden_col = None
        if 'composite_score' in df.columns:
            burden_col = 'composite_score'
        elif 'pca_score' in df.columns:
            burden_col = 'pca_score'
        else:
            return {"error": "No burden indicator available for analysis"}
        
        # Categorize by elevation
        low_elevation = df[df['elevation'] <= df['elevation'].quantile(0.25)]
        high_elevation = df[df['elevation'] >= df['elevation'].quantile(0.75)]
        
        # Compare burden scores
        low_burden_mean = low_elevation[burden_col].mean()
        high_burden_mean = high_elevation[burden_col].mean()
        
        # Statistical test
        from scipy.stats import ttest_ind
        t_stat, p_value = ttest_ind(low_elevation[burden_col].dropna(), 
                                   high_elevation[burden_col].dropna())
        
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        return {
            'status': 'success',
            'elevation_risk_analysis': {
                'low_lying_mean_burden': float(low_burden_mean),
                'high_elevation_mean_burden': float(high_burden_mean),
                'difference': float(low_burden_mean - high_burden_mean),
                'statistical_significance': 'Significant' if p_value < 0.05 else 'Not significant',
                'p_value': float(p_value),
                'interpretation': _interpret_elevation_risk(low_burden_mean, high_burden_mean, p_value)
            },
            'ward_categories': {
                'low_lying_wards': len(low_elevation),
                'high_elevation_wards': len(high_elevation),
                'sample_low_lying': low_elevation[ward_col].head(5).tolist(),
                'sample_high_elevation': high_elevation[ward_col].head(5).tolist()
            },
            'location_filter': location_filter
        }
        
    except Exception as e:
        logger.error(f"Error analyzing elevation risk: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def _interpret_water_correlation(correlation: float, p_value: float) -> str:
    """Interpret water proximity correlation results."""
    if p_value >= 0.05:
        return "No significant correlation found between water proximity and malaria burden."
    
    if correlation < -0.3:
        return "Strong negative correlation: Areas closer to water have LOWER malaria burden (unexpected finding)."
    elif correlation < -0.1:
        return "Moderate negative correlation: Areas closer to water have somewhat lower malaria burden."
    elif correlation > 0.3:
        return "Strong positive correlation: Areas closer to water have HIGHER malaria burden (supports transmission theory)."
    elif correlation > 0.1:
        return "Moderate positive correlation: Areas closer to water have somewhat higher malaria burden."
    else:
        return "Weak correlation: Limited relationship between water proximity and malaria burden."


def _interpret_elevation_risk(low_mean: float, high_mean: float, p_value: float) -> str:
    """Interpret elevation risk analysis results."""
    if p_value >= 0.05:
        return "No significant difference in malaria burden between low-lying and high-elevation areas."
    
    difference = low_mean - high_mean
    if difference > 0.1:
        return "Low-lying areas have significantly HIGHER malaria burden than high-elevation areas."
    elif difference < -0.1:
        return "High-elevation areas have significantly HIGHER malaria burden than low-lying areas (unexpected finding)."
    else:
        return "Statistically significant but small difference in malaria burden between elevation categories."