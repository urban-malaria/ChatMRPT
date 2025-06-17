# app/analysis/urban_analysis.py
import logging
import pandas as pd
import traceback
from typing import Dict, List, Optional, Any, Union

# Set up logging
logger = logging.getLogger(__name__)


def analyze_urban_extent(data, shapefile_data, urban_percent_col=None, thresholds=None, metadata=None):
    """
    Analyze urban extent at different thresholds
    
    Args:
        data: DataFrame with urban percentage data
        shapefile_data: GeoDataFrame with spatial information
        urban_percent_col: Name of urban percentage column (if None, will attempt to find it)
        thresholds: List of thresholds to analyze (default: [30, 50, 75, 100])
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        Dict with results for each threshold
    """
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'data_rows': len(data),
                'shapefile_features': len(shapefile_data),
                'thresholds': thresholds,
                'urban_percent_col': urban_percent_col
            }
            step_id = metadata.record_step(
                'analyze_urban_extent',
                input_summary,
                None,  # Output summary will be updated later
                'urban_threshold_classification',
                {'thresholds': thresholds, 'urban_percent_col': urban_percent_col}
            )
        
        # Default thresholds if none provided
        if thresholds is None:
            thresholds = [30, 50, 75, 100]
            
            if metadata:
                metadata.record_decision(
                    step_id,
                    'threshold_selection',
                    options='custom thresholds',
                    criteria='no thresholds provided, using defaults',
                    selected_option=thresholds
                )
        
        # Find urban percentage column if not specified
        if urban_percent_col is None:
            potential_cols = ['UrbanPercentage', 'UrbanPercent', 'UrbanPerce', 'Urban_Percent', 
                            'urban_percent', 'urbanPercent', 'urbanpercent', 'urban_percentage', 
                            'percent_urban', 'urbanPercentage']
            
            # Check for matches in data columns
            data_cols_lower = {col.lower(): col for col in data.columns}
            for col in potential_cols:
                if col.lower() in data_cols_lower:
                    urban_percent_col = data_cols_lower[col.lower()]
                    
                    if metadata:
                        metadata.record_decision(
                            step_id,
                            'urban_column_detection',
                            options=potential_cols,
                            criteria="found matching column in data: {}".format(urban_percent_col),
                            selected_option=urban_percent_col
                        )
                    
                    break
            
            # If still not found, check for binary Urban column
            if urban_percent_col is None and 'urban' in data_cols_lower:
                urban_col = data_cols_lower['urban']
                urban_percent_col = 'UrbanPercent_Generated'
                
                # Convert binary to percentage
                data = data.copy()  # Create a copy to avoid modifying the original
                data[urban_percent_col] = data[urban_col].apply(
                    lambda x: 100 if str(x).lower() in ['yes', 'true', '1', 'y'] else 0
                )
                
                if metadata:
                    metadata.record_decision(
                        step_id,
                        'urban_column_generation',
                        options=['use_binary_column', 'skip'],
                        criteria="found binary urban column: {}".format(urban_col),
                        selected_option='use_binary_column'
                    )
        
        # Ensure urban_percent_col exists in data
        if urban_percent_col is None or urban_percent_col not in data.columns:
            error_msg = "Urban percentage column not found in data"
            
            if metadata:
                metadata.record_decision(
                    step_id,
                    'urban_column_not_found',
                    options=['abort'],
                    criteria='no suitable column found',
                    selected_option='abort'
                )
            
            raise ValueError(error_msg)
        
        # Make sure urban_percent_col is numeric
        if not pd.api.types.is_numeric_dtype(data[urban_percent_col]):
            data = data.copy()  # Create a copy to avoid modifying the original
            data[urban_percent_col] = pd.to_numeric(data[urban_percent_col], errors='coerce').fillna(0)
            
            if metadata:
                metadata.record_calculation(
                    step_id,
                    urban_percent_col,
                    'type_conversion',
                    {'original_type': str(data[urban_percent_col].dtype)},
                    {'new_type': 'numeric', 'fill_na': 0}
                )
        
        # Initialize results dictionary
        urban_extent_results = {}
        
        # Process each threshold
        for threshold in thresholds:
            # Create a subset of data for this threshold
            result_df = data[['WardName', urban_percent_col]].copy()
            
            # Add threshold classification column
            meets_threshold_field = "MeetsThreshold_{}".format(threshold)
            result_df[meets_threshold_field] = result_df[urban_percent_col] >= threshold
            
            # Count wards in each category
            meets_count = int(result_df[meets_threshold_field].sum())
            below_count = len(result_df) - meets_count
            
            # Get ward names for each category
            meets_wards = result_df[result_df[meets_threshold_field]]['WardName'].tolist()
            below_wards = result_df[~result_df[meets_threshold_field]]['WardName'].tolist()
            
            if metadata:
                metadata.record_calculation(
                    step_id,
                    'urban_extent',
                    "threshold_{}_classification".format(threshold),
                    {'threshold': threshold, 'ward_count': len(result_df)},
                    {'meets_count': meets_count, 'below_count': below_count}
                )
                
                if meets_count == 0:
                    metadata.record_anomaly(
                        "threshold_{}".format(threshold),
                        'no_wards_above_threshold',
                        expected_value='at least one ward above threshold',
                        actual_value="{} wards".format(meets_count),
                        significance='high',
                        context='This threshold might be too high for the dataset'
                    )
                elif below_count == 0:
                    metadata.record_anomaly(
                        "threshold_{}".format(threshold),
                        'no_wards_below_threshold',
                        expected_value='at least one ward below threshold',
                        actual_value="{} wards".format(below_count),
                        significance='high',
                        context='This threshold might be too low for the dataset'
                    )
            
            # Store results
            urban_extent_results[threshold] = {
                'threshold': threshold,
                'meets_threshold': meets_count,
                'below_threshold': below_count,
                'meets_threshold_wards': meets_wards,
                'below_threshold_wards': below_wards
            }
        
        # Update metadata with output summary
        if metadata:
            output_summary = {
                'thresholds_analyzed': len(thresholds),
                'urban_percent_column': urban_percent_col,
                'threshold_results': {
                    str(threshold): {
                        'meets_count': results['meets_threshold'],
                        'below_count': results['below_threshold']
                    } for threshold, results in urban_extent_results.items()
                }
            }
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        return urban_extent_results
    
    except Exception as e:
        logger.error("Error analyzing urban extent: {}".format(str(e)))
        traceback.print_exc()
        raise


def get_urban_extent_summary(urban_extent_results, thresholds=None):
    """
    Generate a summary of urban extent analysis results
    
    Args:
        urban_extent_results: Dict from analyze_urban_extent()
        thresholds: Optional list of thresholds to include in summary
        
    Returns:
        Dict: Summary of urban extent analysis
    """
    try:
        if not urban_extent_results:
            return {
                'error': 'No urban extent results provided',
                'thresholds_analyzed': 0,
                'total_wards': 0,
                'threshold_summaries': {}
            }
        
        # Get all thresholds if not specified
        if thresholds is None:
            thresholds = sorted(urban_extent_results.keys())
        
        total_wards = 0
        threshold_summaries = {}
        
        for threshold in thresholds:
            if threshold in urban_extent_results:
                result = urban_extent_results[threshold]
                total_wards = result['meets_threshold'] + result['below_threshold']
                
                threshold_summaries[threshold] = {
                    'meets_threshold_count': result['meets_threshold'],
                    'below_threshold_count': result['below_threshold'],
                    'meets_threshold_percentage': (result['meets_threshold'] / total_wards * 100) if total_wards > 0 else 0,
                    'below_threshold_percentage': (result['below_threshold'] / total_wards * 100) if total_wards > 0 else 0
                }
        
        # Identify trends across thresholds
        meets_counts = [urban_extent_results[t]['meets_threshold'] for t in sorted(thresholds) if t in urban_extent_results]
        
        summary = {
            'thresholds_analyzed': len(thresholds),
            'total_wards': total_wards,
            'threshold_summaries': threshold_summaries,
            'trends': {
                'decreasing_meets_count': meets_counts == sorted(meets_counts, reverse=True),
                'most_restrictive_threshold': max(thresholds) if thresholds else None,
                'least_restrictive_threshold': min(thresholds) if thresholds else None
            }
        }
        
        return summary
        
    except Exception as e:
        logger.error("Error generating urban extent summary: {}".format(str(e)))
        return {
            'error': str(e),
            'thresholds_analyzed': 0,
            'total_wards': 0,
            'threshold_summaries': {}
        }


def validate_urban_analysis_inputs(data, shapefile_data, urban_percent_col=None, thresholds=None):
    """
    Validate inputs for urban analysis functions
    
    Args:
        data: DataFrame with urban data
        shapefile_data: GeoDataFrame with spatial information
        urban_percent_col: Optional name of urban percentage column
        thresholds: Optional list of thresholds
        
    Returns:
        Dict: Validation results with warnings and recommendations
    """
    validation_results = {
        'is_valid': True,
        'warnings': [],
        'recommendations': [],
        'detected_urban_column': None,
        'potential_columns': []
    }
    
    try:
        # Check if data is provided
        if data is None or data.empty:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("No data provided or data is empty")
            return validation_results
        
        # Check if shapefile is provided
        if shapefile_data is None or shapefile_data.empty:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("No shapefile data provided or shapefile is empty")
        
        # Check for WardName column
        if 'WardName' not in data.columns:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("WardName column is required but not found in data")
        
        # Look for urban percentage columns
        potential_cols = ['UrbanPercentage', 'UrbanPercent', 'UrbanPerce', 'Urban_Percent', 
                         'urban_percent', 'urbanPercent', 'urbanpercent', 'urban_percentage', 
                         'percent_urban', 'urbanPercentage']
        
        data_cols_lower = {col.lower(): col for col in data.columns}
        found_cols = []
        
        for col in potential_cols:
            if col.lower() in data_cols_lower:
                found_cols.append(data_cols_lower[col.lower()])
        
        validation_results['potential_columns'] = found_cols
        
        if urban_percent_col:
            if urban_percent_col in data.columns:
                validation_results['detected_urban_column'] = urban_percent_col
            else:
                validation_results['warnings'].append("Specified urban column '{}' not found in data".format(urban_percent_col))
        elif found_cols:
            validation_results['detected_urban_column'] = found_cols[0]
        elif 'urban' in data_cols_lower:
            validation_results['detected_urban_column'] = data_cols_lower['urban']
            validation_results['warnings'].append("Only binary urban column found, will be converted to percentage")
        else:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("No urban percentage or binary urban column found")
            validation_results['recommendations'].append("Add a column with urban percentage data or binary urban classification")
        
        # Validate thresholds
        if thresholds:
            invalid_thresholds = [t for t in thresholds if not isinstance(t, (int, float)) or t < 0 or t > 100]
            if invalid_thresholds:
                validation_results['warnings'].append("Invalid thresholds found (must be 0-100): {}".format(invalid_thresholds))
                validation_results['recommendations'].append("Use thresholds between 0 and 100")
        
        # Check data quality
        if len(data) < 2:
            validation_results['warnings'].append("Very small dataset (less than 2 rows) may produce unreliable analysis")
        
        return validation_results
        
    except Exception as e:
        validation_results['is_valid'] = False
        validation_results['warnings'].append("Error during validation: {}".format(str(e)))
        return validation_results


def classify_urban_wards(data, urban_percent_col, threshold=50):
    """
    Classify wards as urban or rural based on a single threshold
    
    Args:
        data: DataFrame with urban percentage data
        urban_percent_col: Name of urban percentage column
        threshold: Threshold percentage for urban classification (default: 50)
        
    Returns:
        DataFrame with urban classification added
    """
    try:
        if urban_percent_col not in data.columns:
            raise ValueError("Urban percentage column '{}' not found in data".format(urban_percent_col))
        
        result = data.copy()
        
        # Add urban classification
        result['urban_classification'] = result[urban_percent_col].apply(
            lambda x: 'Urban' if pd.notna(x) and x >= threshold else 'Rural'
        )
        
        # Add threshold used for reference
        result['threshold_used'] = threshold
        
        return result
        
    except Exception as e:
        logger.error("Error classifying urban wards: {}".format(str(e)))
        raise


def get_urban_statistics(data, urban_percent_col):
    """
    Calculate basic statistics for urban percentage data
    
    Args:
        data: DataFrame with urban percentage data
        urban_percent_col: Name of urban percentage column
        
    Returns:
        Dict: Statistical summary of urban percentages
    """
    try:
        if urban_percent_col not in data.columns:
            raise ValueError("Urban percentage column '{}' not found in data".format(urban_percent_col))
        
        urban_data = data[urban_percent_col].dropna()
        
        if len(urban_data) == 0:
            return {
                'error': 'No valid urban percentage data found',
                'count': 0
            }
        
        stats = {
            'count': len(urban_data),
            'mean': float(urban_data.mean()),
            'median': float(urban_data.median()),
            'std': float(urban_data.std()),
            'min': float(urban_data.min()),
            'max': float(urban_data.max()),
            'q25': float(urban_data.quantile(0.25)),
            'q75': float(urban_data.quantile(0.75))
        }
        
        # Add distribution insights
        stats['distribution'] = {
            'highly_urban_count': int((urban_data >= 75).sum()),
            'moderately_urban_count': int(((urban_data >= 50) & (urban_data < 75)).sum()),
            'mixed_count': int(((urban_data >= 25) & (urban_data < 50)).sum()),
            'rural_count': int((urban_data < 25).sum())
        }
        
        return stats
        
    except Exception as e:
        logger.error("Error calculating urban statistics: {}".format(str(e)))
        return {
            'error': str(e),
            'count': 0
        } 