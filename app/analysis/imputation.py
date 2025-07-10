# app/analysis/imputation.py
import logging
import numpy as np
import pandas as pd
import traceback
from typing import Dict, List, Optional, Any, Union
from collections import defaultdict
import concurrent.futures

# Set up logging
from app.services.variable_resolution_service import variable_resolver
logger = logging.getLogger(__name__)


def process_ward_for_spatial_imputation(args):
    """
    Process a single ward for spatial imputation (worker function for parallel processing)
    
    Args:
        args: Tuple of (ward_idx, ward_name, df, merged, column, weights, ward_index_map, shp_index_map)
        
    Returns:
        Tuple of (ward_idx, imputed_value, method_used)
    """
    ward_idx, ward_name, df, merged, column, weights, ward_index_map, shp_index_map = args
    
    # Check if the ward exists in the shapefile
    if ward_name in shp_index_map:
        # Get shapefile index for this ward
        shp_idx = shp_index_map[ward_name]
        
        # Get neighbor indices from weights
        neighbor_indices = weights.neighbors[shp_idx]
        
        if neighbor_indices:
            # Convert shapefile neighbor indices to ward names
            neighbor_wards = [merged.iloc[i]['WardName'] for i in neighbor_indices]
            
            # Find these wards in the CSV data
            neighbor_values = []
            for nward in neighbor_wards:
                if nward in ward_index_map:
                    # Get the value for this ward from the CSV
                    csv_idx = ward_index_map[nward]
                    val = df.iloc[csv_idx][column]
                    if not pd.isna(val):
                        neighbor_values.append(val)
            
            # If we found valid neighbor values, use their mean
            if neighbor_values:
                imputed_value = sum(neighbor_values) / len(neighbor_values)
                return ward_idx, imputed_value, 'spatial_neighbor_mean'
            else:
                # No valid values from neighbors, fall back to global mean
                return ward_idx, df[column].mean(), 'global_mean_fallback'
        else:
            # Ward has no neighbors, fall back to global mean
            return ward_idx, df[column].mean(), 'global_mean_fallback'
    else:
        # Ward not found in shapefile, fall back to global mean
        return ward_idx, df[column].mean(), 'not_in_shapefile_fallback'


def handle_spatial_imputation(data, column, shapefile, n_jobs=-1, metadata=None):
    """
    Impute missing values in a column using spatial neighbors
    
    Args:
        data: DataFrame with missing values
        column: Column name to process
        shapefile: GeoDataFrame with spatial information
        n_jobs: Number of parallel jobs (-1 for all available cores)
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        DataFrame with values imputed using spatial information
    """
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'row_count': len(data),
                'column': column,
                'missing_values': int(data[column].isna().sum())
            }
            step_id = metadata.record_step(
                'spatial_imputation',
                input_summary,
                None,  # Output summary will be updated later
                'spatial_neighbor_mean',
                {'n_jobs': n_jobs, 'column': column}
            )
        
        # Skip for non-numeric columns
        if not pd.api.types.is_numeric_dtype(data[column]):
            if metadata:
                metadata.record_decision(
                    step_id,
                    'imputation_method',
                    options=['spatial_mean', 'mean', 'mode'],
                    criteria='column is not numeric',
                    selected_option='skip_spatial'
                )
            return data  # Return original data unchanged
        
        # Check required columns
        if 'WardName' not in data.columns or 'WardName' not in shapefile.columns:
            if metadata:
                metadata.record_decision(
                    step_id,
                    'imputation_method',
                    options=['spatial_mean', 'mean', 'mode'],
                    criteria='WardName column missing in data or shapefile',
                    selected_option='skip_spatial'
                )
            return data  # Return original data unchanged
        
        # Create a copy to avoid modifying original
        result = data.copy()
        
        # Merge data with shapefile for spatial analysis
        merged = shapefile.merge(data[['WardName', column]], on='WardName', how='inner')
        
        # Create spatial weights using queen contiguity (lazy import to avoid 15-second startup delay)
        from libpysal.weights import Queen
        weights = Queen.from_dataframe(merged)
        
        # Find rows with missing values
        missing_indices = result.index[result[column].isna()]
        
        if len(missing_indices) == 0:
            # No missing values to impute
            if metadata:
                metadata.record_calculation(
                    step_id,
                    column,
                    'missing_values_check',
                    {'row_count': len(data)},
                    {'missing_count': 0}
                )
            return result
        
        # Create mappings for efficient lookups
        ward_index_map = {result.loc[idx, 'WardName']: idx for idx in result.index}
        shp_index_map = {row['WardName']: i for i, row in merged.iterrows()}
        
        # Prepare arguments for parallel processing
        args_list = [
            (idx, result.loc[idx, 'WardName'], result, merged, column, weights, ward_index_map, shp_index_map)
            for idx in missing_indices
        ]
        
        # Process in parallel or sequentially based on n_jobs
        if n_jobs != 1 and len(missing_indices) > 1:
            # Use parallel processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs if n_jobs > 0 else None) as executor:
                results = list(executor.map(process_ward_for_spatial_imputation, args_list))
        else:
            # Sequential processing
            results = [process_ward_for_spatial_imputation(args) for args in args_list]
        
        # Track the imputation methods used
        method_counts = defaultdict(int)
        
        # Apply imputed values
        for ward_idx, imputed_value, method_used in results:
            result.loc[ward_idx, column] = imputed_value
            method_counts[method_used] += 1
            
            if metadata:
                metadata.record_calculation(
                    step_id,
                    column,
                    "imputation_{}".format(method_used),
                    {'ward_idx': int(ward_idx), 'ward_name': result.loc[ward_idx, 'WardName']},
                    {'imputed_value': float(imputed_value)}
                )
        
        # Update metadata with output summary
        if metadata:
            output_summary = {
                'missing_values_count': len(missing_indices),
                'imputation_methods': dict(method_counts)
            }
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        return result
    
    except Exception as e:
        logger.error("Error in handle_spatial_imputation for {}: {}".format(column, str(e)))
        traceback.print_exc()
        # Return original data on error
        return data


def handle_mean_imputation(data, column, metadata=None, step_id=None):
    """
    Impute missing values with column mean
    
    Args:
        data: DataFrame with missing values
        column: Column name to process
        metadata: Optional AnalysisMetadata instance for logging
        step_id: Optional step ID for linking to metadata
        
    Returns:
        DataFrame with missing values imputed
    """
    if not pd.api.types.is_numeric_dtype(data[column]):
        logger.warning("Column {} is not numeric, cannot apply mean imputation".format(column))
        return data  # Return original data unchanged
    
    # Create a copy to avoid modifying original
    result = data.copy()
    
    # Calculate mean excluding NaN values
    mean_value = result[column].mean()
    
    if pd.isna(mean_value):
        logger.warning("All values in column {} are NaN, using 0 as fallback".format(column))
        mean_value = 0
    
    # Record the calculation if metadata is provided
    if metadata and step_id:
        metadata.record_calculation(
            step_id,
            column,
            'mean_imputation',
            {'missing_count': int(result[column].isna().sum())},
            {'mean_value': float(mean_value)}
        )
    
    # Impute missing values
    result[column] = result[column].fillna(mean_value)
    
    return result


def handle_mode_imputation(data, column, metadata=None, step_id=None):
    """
    Impute missing values with column mode (most frequent value)
    
    Args:
        data: DataFrame with missing values
        column: Column name to process
        metadata: Optional AnalysisMetadata instance for logging
        step_id: Optional step ID for linking to metadata
        
    Returns:
        DataFrame with missing values imputed
    """
    # Create a copy to avoid modifying original
    result = data.copy()
    
    try:
        # Find the mode (most frequent value)
        mode_result = result[column].mode()
        
        if not mode_result.empty:
            mode_value = mode_result[0]
            
            # Record the calculation if metadata is provided
            if metadata and step_id:
                metadata.record_calculation(
                    step_id,
                    column,
                    'mode_imputation',
                    {'missing_count': int(result[column].isna().sum())},
                    {'mode_value': str(mode_value)}
                )
            
            # Impute missing values
            result[column] = result[column].fillna(mode_value)
        else:
            # Fallback if mode cannot be determined
            logger.warning("Could not determine mode for column {}. Using forward/backward fill.".format(column))
            
            if metadata and step_id:
                metadata.record_calculation(
                    step_id,
                    column,
                    'ffill_bfill_fallback',
                    {'missing_count': int(result[column].isna().sum())},
                    {'reason': 'no_mode_found'}
                )
            
            # Apply forward fill followed by backward fill
            result[column] = result[column].ffill().bfill()
            
            # Check if still NaN after ffill/bfill (e.g., all values were NaN)
            if result[column].isna().any():
                logger.warning("ffill/bfill failed for column {}. Using 'Unknown' placeholder.".format(column))
                
                if metadata and step_id:
                    metadata.record_calculation(
                        step_id,
                        column,
                        'unknown_placeholder',
                        {'missing_count': int(result[column].isna().sum())},
                        {'placeholder': 'Unknown'}
                    )
                
                result[column] = result[column].fillna('Unknown')
    except Exception as e:
        logger.error("Error in mode imputation for {}: {}".format(column, str(e)))
        # Try forward/backward fill as a last resort
        try:
            result[column] = result[column].ffill().bfill()
            if result[column].isna().any():
                result[column] = result[column].fillna('Unknown')
        except:
            pass
    
    return result


def handle_missing_values(data, methods=None, shapefile=None, n_jobs=-1, metadata=None):
    """
    Handle missing values using specified methods
    
    Args:
        data: DataFrame with missing values
        methods: Dict mapping column names to cleaning methods
                 ('mean', 'mode', 'spatial', 'knn')
        shapefile: GeoDataFrame with spatial information (required for spatial method)
        n_jobs: Number of parallel jobs (-1 for all available cores)
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        DataFrame with missing values handled
    """
    # Record analysis step if metadata is provided
    step_id = None
    if metadata:
        input_summary = {
            'row_count': len(data),
            'column_count': len(data.columns),
            'methods_specified': methods is not None
        }
        step_id = metadata.record_step(
            'handle_missing_values',
            input_summary,
            None,  # Output summary will be updated later
            'multiple_imputation_methods',
            {'methods': methods, 'has_shapefile': shapefile is not None, 'n_jobs': n_jobs}
        )
    
    # Create a copy to avoid modifying original
    cleaned_df = data.copy()
    
    # Find columns with missing values
    cols_with_missing = [col for col in cleaned_df.columns if cleaned_df[col].isna().any()]
    
    if not cols_with_missing:
        if metadata:
            metadata.record_calculation(
                step_id,
                'all_columns',
                'missing_values_check',
                {'row_count': len(data)},
                {'missing_columns_count': 0}
            )
        return cleaned_df  # No missing values
    
    # Default method if none specified
    if methods is None:
        methods = {}
    
    # Track actual methods used for reporting
    methods_used = {}
    
    # Process each column with missing values
    for col in cols_with_missing:
        # Determine method for this column
        if col in methods:
            method = methods[col]
        else:
            # Select default method based on data type
            if pd.api.types.is_numeric_dtype(cleaned_df[col]):
                method = 'spatial' if shapefile is not None else 'mean'
            else:
                method = 'mode'
        
        if metadata:
            metadata.record_decision(
                step_id,
                'imputation_method_selection',
                options=['spatial', 'mean', 'mode', 'knn'],
                criteria='data type and available resources',
                selected_option=method
            )
        
        # Apply appropriate method
        if method == 'spatial' and shapefile is not None:
            pre_missing = cleaned_df[col].isna().sum()
            cleaned_df = handle_spatial_imputation(cleaned_df, col, shapefile, n_jobs, metadata)
            post_missing = cleaned_df[col].isna().sum()
            
            # Check if spatial imputation was successful
            if post_missing < pre_missing:
                methods_used[col] = 'spatial'
            else:
                # Spatial imputation failed or didn't change anything, try mean as fallback
                logger.warning("Spatial imputation ineffective for {}, falling back to mean".format(col))
                if pd.api.types.is_numeric_dtype(cleaned_df[col]):
                    cleaned_df = handle_mean_imputation(cleaned_df, col, metadata, step_id)
                    methods_used[col] = 'spatial_then_mean'
                else:
                    cleaned_df = handle_mode_imputation(cleaned_df, col, metadata, step_id)
                    methods_used[col] = 'spatial_then_mode'
        elif method == 'knn':
            # KNN imputation is complex and requires additional libraries
            # For this example, we'll fall back to mean/mode
            logger.warning("KNN imputation not implemented, falling back to mean/mode for {}".format(col))
            if pd.api.types.is_numeric_dtype(cleaned_df[col]):
                cleaned_df = handle_mean_imputation(cleaned_df, col, metadata, step_id)
                methods_used[col] = 'knn_fallback_to_mean'
            else:
                cleaned_df = handle_mode_imputation(cleaned_df, col, metadata, step_id)
                methods_used[col] = 'knn_fallback_to_mode'
        elif method == 'mean':
            if pd.api.types.is_numeric_dtype(cleaned_df[col]):
                cleaned_df = handle_mean_imputation(cleaned_df, col, metadata, step_id)
                methods_used[col] = 'mean'
            else:
                # Mean doesn't make sense for non-numeric, fall back to mode
                logger.warning("Mean imputation not suitable for non-numeric column {}, using mode instead".format(col))
                cleaned_df = handle_mode_imputation(cleaned_df, col, metadata, step_id)
                methods_used[col] = 'mean_fallback_to_mode'
        else:  # Default to mode for non-numeric
            cleaned_df = handle_mode_imputation(cleaned_df, col, metadata, step_id)
            methods_used[col] = 'mode'
    
    # Update metadata with output summary
    if metadata:
        output_summary = {
            'columns_processed': len(cols_with_missing),
            'methods_used': methods_used,
            'remaining_missing': sum(cleaned_df[col].isna().sum() for col in cols_with_missing)
        }
        # Update the step with output information
        for step in metadata.steps:
            if step['step_id'] == step_id:
                step['output_summary'] = output_summary
                break
    
    return cleaned_df


def get_imputation_summary(data, cleaned_data, methods_used=None):
    """
    Generate a summary of the imputation process
    
    Args:
        data: Original DataFrame before imputation
        cleaned_data: DataFrame after imputation
        methods_used: Dict of methods used per column
        
    Returns:
        Dict: Summary of imputation results
    """
    try:
        # Find columns that had missing values
        original_missing = {}
        cleaned_missing = {}
        
        for col in data.columns:
            original_missing[col] = int(data[col].isna().sum())
            cleaned_missing[col] = int(cleaned_data[col].isna().sum())
        
        # Calculate improvement
        total_original_missing = sum(original_missing.values())
        total_cleaned_missing = sum(cleaned_missing.values())
        improvement = total_original_missing - total_cleaned_missing
        
        summary = {
            'original_missing_values': total_original_missing,
            'remaining_missing_values': total_cleaned_missing,
            'values_imputed': improvement,
            'improvement_percentage': (improvement / total_original_missing * 100) if total_original_missing > 0 else 100,
            'columns_processed': len([col for col, count in original_missing.items() if count > 0]),
            'methods_used': methods_used or {},
            'column_details': {}
        }
        
        # Add column-level details
        for col in data.columns:
            if original_missing[col] > 0:
                summary['column_details'][col] = {
                    'original_missing': original_missing[col],
                    'remaining_missing': cleaned_missing[col],
                    'values_imputed': original_missing[col] - cleaned_missing[col],
                    'method_used': methods_used.get(col, 'unknown') if methods_used else 'unknown'
                }
        
        return summary
        
    except Exception as e:
        logger.error("Error generating imputation summary: {}".format(str(e)))
        return {
            'error': str(e),
            'original_missing_values': 0,
            'remaining_missing_values': 0,
            'values_imputed': 0,
            'improvement_percentage': 0,
            'columns_processed': 0,
            'methods_used': {},
            'column_details': {}
        } 