"""
Group Analysis & Aggregation Tools for ChatMRPT

This module provides advanced group-based analysis capabilities including:
- Settlement type analysis and classification
- Geographic aggregation (state, LGA level)
- Cross-variable correlation and relationship analysis
- Group statistics and comparisons
- Environmental risk grouping

All functions work with the unified dataset and maintain privacy-first approach.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from flask import current_app
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
import warnings

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[pd.DataFrame]:
    """Get the unified dataset from session."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            logger.debug(f"✅ Unified dataset loaded: {len(unified_gdf)} rows, {len(unified_gdf.columns)} columns")
            return unified_gdf
        else:
            logger.error(f"❌ Unified dataset not found for session {session_id}")
            return None
    except Exception as e:
        logger.error(f"❌ Error accessing unified dataset: {e}")
        return None


def settlement_type_analysis(session_id: str, ward_name: str = None, city: str = None, state: str = None) -> Dict[str, Any]:
    """
    Analyze settlement types for wards, cities, or states.
    
    Args:
        session_id: Session identifier
        ward_name: Specific ward to analyze
        city: City to analyze
        state: State to analyze
        
    Returns:
        Dict with settlement type analysis results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for settlement type analysis'
            }
        
        # Settlement type columns (look for available columns)
        settlement_cols = []
        possible_cols = ['settlement_type', 'dominant_settlement_type', 'building_type', 'urban_type']
        for col in possible_cols:
            if col in unified_gdf.columns:
                settlement_cols.append(col)
        
        morphology_cols = []
        morph_cols = ['compactness', 'nearest_neighbor_index', 'building_density', 'shape_index']
        for col in morph_cols:
            if col in unified_gdf.columns:
                morphology_cols.append(col)
        
        if not settlement_cols and not morphology_cols:
            return {
                'status': 'error',
                'message': 'No settlement type or morphology columns found in dataset'
            }
        
        # Filter data based on parameters
        filtered_data = unified_gdf.copy()
        filter_description = "all wards"
        
        if ward_name:
            ward_col = 'WardName' if 'WardName' in unified_gdf.columns else None
            if ward_col:
                ward_mask = unified_gdf[ward_col].str.contains(ward_name, case=False, na=False)
                if ward_mask.any():
                    filtered_data = unified_gdf[ward_mask]
                    filter_description = f"ward '{ward_name}'"
                else:
                    available_wards = unified_gdf[ward_col].dropna().unique()[:10]
                    return {
                        'status': 'error',
                        'message': f'Ward "{ward_name}" not found. Available wards: {list(available_wards)}'
                    }
        
        elif city:
            city_col = 'city_name' if 'city_name' in unified_gdf.columns else 'LGAName'
            if city_col in unified_gdf.columns:
                city_mask = unified_gdf[city_col].str.contains(city, case=False, na=False)
                if city_mask.any():
                    filtered_data = unified_gdf[city_mask]
                    filter_description = f"city '{city}'"
        
        elif state:
            state_col = 'state_name' if 'state_name' in unified_gdf.columns else 'StateCode'
            if state_col in unified_gdf.columns:
                state_mask = unified_gdf[state_col].str.contains(state, case=False, na=False)
                if state_mask.any():
                    filtered_data = unified_gdf[state_mask]
                    filter_description = f"state '{state}'"
        
        logger.info(f"🏘️ Analyzing settlement types for {filter_description}: {len(filtered_data)} wards")
        
        results = {
            'status': 'success',
            'message': f'Settlement type analysis completed for {filter_description}',
            'filter': filter_description,
            'total_wards': len(filtered_data)
        }
        
        # Analyze settlement types if available
        if settlement_cols:
            settlement_analysis = {}
            for col in settlement_cols:
                col_data = filtered_data[col].dropna()
                if len(col_data) > 0:
                    value_counts = col_data.value_counts()
                    percentages = (value_counts / len(col_data) * 100).round(1)
                    
                    settlement_analysis[col] = {
                        'distribution': value_counts.to_dict(),
                        'percentages': percentages.to_dict(),
                        'dominant_type': value_counts.index[0] if len(value_counts) > 0 else 'Unknown',
                        'total_classified': len(col_data)
                    }
            
            results['settlement_types'] = settlement_analysis
        
        # Analyze morphology characteristics
        if morphology_cols:
            morphology_stats = {}
            for col in morphology_cols:
                col_data = filtered_data[col].dropna()
                if len(col_data) > 0:
                    morphology_stats[col] = {
                        'mean': float(col_data.mean()),
                        'median': float(col_data.median()),
                        'std': float(col_data.std()),
                        'min': float(col_data.min()),
                        'max': float(col_data.max()),
                        'count': len(col_data)
                    }
            
            results['morphology_characteristics'] = morphology_stats
        
        # Classification based on morphology (if available)
        if 'compactness' in filtered_data.columns and 'nearest_neighbor_index' in filtered_data.columns:
            compact_data = filtered_data[['compactness', 'nearest_neighbor_index']].dropna()
            
            if len(compact_data) > 0:
                # Simple classification
                high_compact = compact_data['compactness'] > compact_data['compactness'].median()
                low_nni = compact_data['nearest_neighbor_index'] < compact_data['nearest_neighbor_index'].median()
                
                classifications = []
                ward_col = 'WardName' if 'WardName' in filtered_data.columns else filtered_data.columns[0]
                
                for idx, row in compact_data.iterrows():
                    ward_name_val = filtered_data.loc[idx, ward_col] if ward_col in filtered_data.columns else f"Ward_{idx}"
                    compactness = row['compactness']
                    nni = row['nearest_neighbor_index']
                    
                    if compactness > compact_data['compactness'].quantile(0.75):
                        if nni < compact_data['nearest_neighbor_index'].quantile(0.25):
                            classification = "Dense Urban"
                        else:
                            classification = "Compact Residential"
                    elif compactness < compact_data['compactness'].quantile(0.25):
                        classification = "Sprawled/Rural"
                    else:
                        classification = "Mixed Development"
                    
                    classifications.append({
                        'ward': ward_name_val,
                        'classification': classification,
                        'compactness': float(compactness),
                        'nearest_neighbor_index': float(nni)
                    })
                
                # Count classifications
                class_counts = {}
                for c in classifications:
                    class_type = c['classification']
                    class_counts[class_type] = class_counts.get(class_type, 0) + 1
                
                results['morphology_classification'] = {
                    'ward_classifications': classifications[:20],  # Top 20
                    'classification_counts': class_counts,
                    'method': 'Compactness vs Nearest Neighbor Index'
                }
        
        return results
        
    except Exception as e:
        logger.error(f"Error in settlement type analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in settlement type analysis: {str(e)}'
        }


def cross_variable_analysis(session_id: str, variable1: str, variable2: str, group_by: str = None) -> Dict[str, Any]:
    """
    Analyze relationships between two variables with optional grouping.
    
    Args:
        session_id: Session identifier
        variable1: First variable for analysis
        variable2: Second variable for analysis
        group_by: Optional grouping variable
        
    Returns:
        Dict with cross-variable analysis results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for cross-variable analysis'
            }
        
        # Check if variables exist
        missing_vars = []
        for var in [variable1, variable2]:
            if var not in unified_gdf.columns:
                missing_vars.append(var)
        
        if missing_vars:
            available_vars = [col for col in unified_gdf.columns if unified_gdf[col].dtype in ['float64', 'int64']]
            return {
                'status': 'error',
                'message': f'Variables not found: {missing_vars}. Available numeric variables: {available_vars[:10]}'
            }
        
        logger.info(f"📊 Analyzing relationship between {variable1} and {variable2}")
        
        # Get clean data
        clean_data = unified_gdf[[variable1, variable2]].dropna()
        
        if len(clean_data) < 3:
            return {
                'status': 'error',
                'message': f'Insufficient data points for analysis (need at least 3, have {len(clean_data)})'
            }
        
        # Basic correlation analysis
        correlation_coef = clean_data[variable1].corr(clean_data[variable2])
        
        # Regression analysis
        from scipy.stats import linregress
        slope, intercept, r_value, p_value, std_err = linregress(clean_data[variable1], clean_data[variable2])
        
        # Classify relationship strength
        if abs(correlation_coef) > 0.7:
            strength = "Strong"
        elif abs(correlation_coef) > 0.4:
            strength = "Moderate"
        elif abs(correlation_coef) > 0.2:
            strength = "Weak"
        else:
            strength = "Very Weak"
        
        direction = "Positive" if correlation_coef > 0 else "Negative"
        
        results = {
            'status': 'success',
            'message': f'Cross-variable analysis completed for {variable1} vs {variable2}',
            'variable1': variable1,
            'variable2': variable2,
            'sample_size': len(clean_data),
            'correlation': {
                'coefficient': float(correlation_coef),
                'strength': strength,
                'direction': direction,
                'r_squared': float(r_value**2),
                'p_value': float(p_value),
                'is_significant': p_value < 0.05
            },
            'regression': {
                'slope': float(slope),
                'intercept': float(intercept),
                'equation': f"{variable2} = {slope:.4f} * {variable1} + {intercept:.4f}",
                'standard_error': float(std_err)
            }
        }
        
        # Group analysis if requested
        if group_by and group_by in unified_gdf.columns:
            logger.info(f"📋 Performing grouped analysis by {group_by}")
            
            grouped_data = unified_gdf[[variable1, variable2, group_by]].dropna()
            group_results = {}
            
            for group_name, group_df in grouped_data.groupby(group_by):
                if len(group_df) >= 3:  # Minimum for correlation
                    group_corr = group_df[variable1].corr(group_df[variable2])
                    
                    # Basic stats for each group
                    group_results[str(group_name)] = {
                        'correlation': float(group_corr) if not pd.isna(group_corr) else 0,
                        'sample_size': len(group_df),
                        'var1_mean': float(group_df[variable1].mean()),
                        'var2_mean': float(group_df[variable2].mean()),
                        'var1_std': float(group_df[variable1].std()),
                        'var2_std': float(group_df[variable2].std())
                    }
            
            results['group_analysis'] = {
                'grouped_by': group_by,
                'groups': group_results,
                'total_groups': len(group_results)
            }
        
        # Identify outliers using IQR method
        def find_outliers(series):
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            return series[(series < lower_bound) | (series > upper_bound)]
        
        var1_outliers = find_outliers(clean_data[variable1])
        var2_outliers = find_outliers(clean_data[variable2])
        
        results['outliers'] = {
            f'{variable1}_outliers': len(var1_outliers),
            f'{variable2}_outliers': len(var2_outliers),
            'outlier_percentage': round((len(var1_outliers) + len(var2_outliers)) / (2 * len(clean_data)) * 100, 1)
        }
        
        return results
        
    except Exception as e:
        logger.error(f"Error in cross-variable analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in cross-variable analysis: {str(e)}'
        }


def geographic_aggregation_analysis(session_id: str, aggregation_level: str, variable: str, statistic: str = 'mean') -> Dict[str, Any]:
    """
    Aggregate data by geographic levels (LGA, State, etc.).
    
    Args:
        session_id: Session identifier
        aggregation_level: 'lga', 'state', or available geographic column
        variable: Variable to aggregate
        statistic: Aggregation statistic ('mean', 'sum', 'count', 'median', 'std')
        
    Returns:
        Dict with geographic aggregation results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for geographic aggregation'
            }
        
        # Find appropriate geographic column
        geo_col = None
        if aggregation_level.lower() in ['lga', 'local_government']:
            possible_cols = ['LGAName', 'lga_name', 'LocalGovernment', 'LGA']
            for col in possible_cols:
                if col in unified_gdf.columns:
                    geo_col = col
                    break
        elif aggregation_level.lower() in ['state', 'state_name']:
            possible_cols = ['StateCode', 'state_name', 'State', 'StateName']
            for col in possible_cols:
                if col in unified_gdf.columns:
                    geo_col = col
                    break
        elif aggregation_level in unified_gdf.columns:
            geo_col = aggregation_level
        
        if not geo_col:
            available_geo_cols = [col for col in unified_gdf.columns if 'name' in col.lower() or 'code' in col.lower()]
            return {
                'status': 'error',
                'message': f'Geographic column not found for "{aggregation_level}". Available geographic columns: {available_geo_cols[:10]}'
            }
        
        if variable not in unified_gdf.columns:
            available_vars = [col for col in unified_gdf.columns if unified_gdf[col].dtype in ['float64', 'int64']]
            return {
                'status': 'error',
                'message': f'Variable "{variable}" not found. Available numeric variables: {available_vars[:10]}'
            }
        
        logger.info(f"🗺️ Aggregating {variable} by {geo_col} using {statistic}")
        
        # Perform aggregation
        clean_data = unified_gdf[[geo_col, variable]].dropna()
        
        if len(clean_data) == 0:
            return {
                'status': 'error',
                'message': f'No valid data for aggregation'
            }
        
        # Apply aggregation function
        if statistic == 'mean':
            agg_result = clean_data.groupby(geo_col)[variable].mean()
        elif statistic == 'sum':
            agg_result = clean_data.groupby(geo_col)[variable].sum()
        elif statistic == 'count':
            agg_result = clean_data.groupby(geo_col)[variable].count()
        elif statistic == 'median':
            agg_result = clean_data.groupby(geo_col)[variable].median()
        elif statistic == 'std':
            agg_result = clean_data.groupby(geo_col)[variable].std()
        else:
            agg_result = clean_data.groupby(geo_col)[variable].mean()  # Default to mean
        
        # Sort results
        agg_result = agg_result.sort_values(ascending=False)
        
        # Convert to list of dictionaries
        aggregated_data = []
        for geo_name, value in agg_result.items():
            # Get additional info for this geographic unit
            geo_data = clean_data[clean_data[geo_col] == geo_name]
            
            aggregated_data.append({
                'geographic_unit': str(geo_name),
                'aggregated_value': float(value) if not pd.isna(value) else 0,
                'ward_count': len(geo_data),
                'raw_total': float(geo_data[variable].sum()) if statistic != 'sum' else float(value),
                'min_value': float(geo_data[variable].min()),
                'max_value': float(geo_data[variable].max())
            })
        
        # Summary statistics
        summary_stats = {
            'total_geographic_units': len(agg_result),
            'mean_aggregated_value': float(agg_result.mean()),
            'std_aggregated_value': float(agg_result.std()),
            'min_aggregated_value': float(agg_result.min()),
            'max_aggregated_value': float(agg_result.max()),
            'total_wards_analyzed': len(clean_data)
        }
        
        # Identify top and bottom performers
        top_performers = aggregated_data[:10]  # Top 10
        bottom_performers = aggregated_data[-10:] if len(aggregated_data) > 10 else []
        
        return {
            'status': 'success',
            'message': f'Geographic aggregation completed for {variable} by {geo_col}',
            'aggregation_level': geo_col,
            'variable': variable,
            'statistic': statistic,
            'summary_statistics': summary_stats,
            'aggregated_data': aggregated_data,
            'top_performers': top_performers,
            'bottom_performers': bottom_performers,
            'interpretation': {
                'highest': f"{top_performers[0]['geographic_unit']}: {top_performers[0]['aggregated_value']:.2f}" if top_performers else "N/A",
                'lowest': f"{bottom_performers[-1]['geographic_unit']}: {bottom_performers[-1]['aggregated_value']:.2f}" if bottom_performers else "N/A",
                'analysis_method': f"{statistic.title()} aggregation by {geo_col}"
            }
        }
        
    except Exception as e:
        logger.error(f"Error in geographic aggregation analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in geographic aggregation analysis: {str(e)}'
        }


def environmental_risk_grouping(session_id: str, elevation_threshold: float = None, vegetation_threshold: float = None) -> Dict[str, Any]:
    """
    Group wards by environmental risk factors.
    
    Args:
        session_id: Session identifier
        elevation_threshold: Elevation threshold for low-lying areas
        vegetation_threshold: NDVI threshold for high vegetation
        
    Returns:
        Dict with environmental risk grouping results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for environmental risk grouping'
            }
        
        # Find environmental variables
        env_vars = {}
        
        # Look for elevation data
        elevation_cols = ['elevation', 'dem', 'altitude', 'height']
        for col in elevation_cols:
            if col in unified_gdf.columns:
                env_vars['elevation'] = col
                break
        
        # Look for vegetation data
        vegetation_cols = ['ndvi', 'vegetation', 'greenness', 'plant_cover']
        for col in vegetation_cols:
            if col in unified_gdf.columns:
                env_vars['vegetation'] = col
                break
        
        # Look for water proximity
        water_cols = ['water_distance', 'distance_to_water', 'water_proximity', 'hydro_distance']
        for col in water_cols:
            if col in unified_gdf.columns:
                env_vars['water'] = col
                break
        
        if not env_vars:
            return {
                'status': 'error',
                'message': 'No environmental variables found for risk grouping'
            }
        
        logger.info(f"🌍 Grouping wards by environmental risk factors: {list(env_vars.keys())}")
        
        # Set thresholds if not provided
        if 'elevation' in env_vars:
            elevation_col = env_vars['elevation']
            if elevation_threshold is None:
                elevation_threshold = unified_gdf[elevation_col].quantile(0.25)  # Bottom quartile
        
        if 'vegetation' in env_vars:
            vegetation_col = env_vars['vegetation']
            if vegetation_threshold is None:
                vegetation_threshold = unified_gdf[vegetation_col].quantile(0.75)  # Top quartile
        
        # Create risk groups
        ward_col = 'WardName' if 'WardName' in unified_gdf.columns else unified_gdf.columns[0]
        risk_groups = {
            'flood_prone': [],  # Low elevation
            'high_vegetation': [],  # High NDVI
            'water_proximity': [],  # Close to water
            'multiple_risks': [],  # Multiple risk factors
            'low_risk': []  # Few environmental risks
        }
        
        for idx, row in unified_gdf.iterrows():
            ward_name = row[ward_col] if ward_col in unified_gdf.columns else f"Ward_{idx}"
            risk_factors = []
            risk_scores = {}
            
            # Check elevation risk
            if 'elevation' in env_vars:
                elevation_val = row[env_vars['elevation']]
                if not pd.isna(elevation_val) and elevation_val <= elevation_threshold:
                    risk_factors.append('flood_prone')
                    risk_scores['elevation'] = float(elevation_val)
            
            # Check vegetation risk
            if 'vegetation' in env_vars:
                vegetation_val = row[env_vars['vegetation']]
                if not pd.isna(vegetation_val) and vegetation_val >= vegetation_threshold:
                    risk_factors.append('high_vegetation')
                    risk_scores['vegetation'] = float(vegetation_val)
            
            # Check water proximity (assuming lower values = closer to water)
            if 'water' in env_vars:
                water_val = row[env_vars['water']]
                if not pd.isna(water_val):
                    water_threshold = unified_gdf[env_vars['water']].quantile(0.25)  # Bottom quartile = close
                    if water_val <= water_threshold:
                        risk_factors.append('water_proximity')
                        risk_scores['water_distance'] = float(water_val)
            
            # Classify ward
            ward_info = {
                'ward': ward_name,
                'risk_factors': risk_factors,
                'risk_scores': risk_scores,
                'total_risks': len(risk_factors)
            }
            
            if len(risk_factors) >= 2:
                risk_groups['multiple_risks'].append(ward_info)
            elif len(risk_factors) == 0:
                risk_groups['low_risk'].append(ward_info)
            else:
                # Single risk factor
                for factor in risk_factors:
                    if factor in risk_groups:
                        risk_groups[factor].append(ward_info)
        
        # Summary statistics
        group_counts = {group: len(wards) for group, wards in risk_groups.items()}
        total_classified = sum(group_counts.values())
        
        # Calculate percentages
        group_percentages = {group: round(count/total_classified*100, 1) if total_classified > 0 else 0 
                           for group, count in group_counts.items()}
        
        return {
            'status': 'success',
            'message': 'Environmental risk grouping completed',
            'environmental_variables': env_vars,
            'thresholds': {
                'elevation_threshold': float(elevation_threshold) if elevation_threshold else None,
                'vegetation_threshold': float(vegetation_threshold) if vegetation_threshold else None
            },
            'risk_groups': {
                group: wards[:15]  # Limit to top 15 per group
                for group, wards in risk_groups.items()
            },
            'group_statistics': {
                'counts': group_counts,
                'percentages': group_percentages,
                'total_wards_classified': total_classified
            },
            'high_risk_areas': {
                'multiple_risks': len(risk_groups['multiple_risks']),
                'flood_prone': len(risk_groups['flood_prone']),
                'high_vegetation': len(risk_groups['high_vegetation']),
                'water_proximity': len(risk_groups['water_proximity'])
            }
        }
        
    except Exception as e:
        logger.error(f"Error in environmental risk grouping: {e}")
        return {
            'status': 'error',
            'message': f'Error in environmental risk grouping: {str(e)}'
        } 