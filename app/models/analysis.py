# app/models/analysis.py
import os
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import json
import time
import traceback
from typing import Dict, List, Tuple, Optional, Union, Any, Callable
from collections import defaultdict
from functools import partial
import concurrent.futures
from joblib import Parallel, delayed
from libpysal.weights import Queen
from scipy import stats
import itertools

# Set up logging
logger = logging.getLogger(__name__)

# Analysis metadata framework
class AnalysisMetadata:
    """Class to capture and record analysis metadata for explanation purposes"""
    
    def __init__(self, session_id=None, interaction_logger=None):
        self.session_id = session_id
        self.logger = interaction_logger
        self.steps = []
        self.decisions = []
        self.calculations = []
        self.anomalies = []
        self.start_time = time.time()
        
    def record_step(self, step_name, input_data_summary=None, output_data_summary=None, 
                   algorithm=None, parameters=None):
        """Record an analysis step"""
        step_info = {
            'step_id': len(self.steps) + 1,
            'step_name': step_name,
            'timestamp': time.time(),
            'execution_time': time.time() - self.start_time,
            'input_summary': input_data_summary,
            'output_summary': output_data_summary,
            'algorithm': algorithm,
            'parameters': parameters
        }
        self.steps.append(step_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                f"analysis_step_{step_name}",
                step_info,
                True
            )
        
        return step_info['step_id']
    
    def record_decision(self, step_id, decision_type, options=None, criteria=None, 
                       selected_option=None, confidence=None):
        """Record a decision made during analysis"""
        decision_info = {
            'decision_id': len(self.decisions) + 1,
            'step_id': step_id,
            'decision_type': decision_type,
            'timestamp': time.time(),
            'options': options,
            'criteria': criteria,
            'selected_option': selected_option,
            'confidence': confidence
        }
        self.decisions.append(decision_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                f"analysis_decision_{decision_type}",
                decision_info,
                True
            )
            
        return decision_info['decision_id']
    
    def record_calculation(self, step_id, variable, operation, input_values=None, 
                         output_value=None, context=None):
        """Record a calculation performed during analysis"""
        calc_info = {
            'calculation_id': len(self.calculations) + 1,
            'step_id': step_id,
            'variable': variable,
            'operation': operation,
            'timestamp': time.time(),
            'input_values': input_values,
            'output_value': output_value,
            'context': context
        }
        self.calculations.append(calc_info)
        return calc_info['calculation_id']
    
    def record_anomaly(self, entity_name, anomaly_type, expected_value=None, 
                     actual_value=None, significance=None, context=None):
        """Record an anomaly detected during analysis"""
        anomaly_info = {
            'anomaly_id': len(self.anomalies) + 1,
            'entity_name': entity_name,
            'anomaly_type': anomaly_type,
            'timestamp': time.time(),
            'expected_value': expected_value,
            'actual_value': actual_value,
            'significance': significance,
            'context': context
        }
        self.anomalies.append(anomaly_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                f"analysis_anomaly_{anomaly_type}",
                anomaly_info,
                True
            )
            
        return anomaly_info['anomaly_id']
    
    def get_step_summary(self, step_id=None):
        """Get summary of steps, or a specific step if ID provided"""
        if step_id is not None:
            for step in self.steps:
                if step['step_id'] == step_id:
                    return step
            return None
        return self.steps
    
    def get_entity_metadata(self, entity_type, entity_name):
        """Get all metadata related to a specific entity"""
        entity_metadata = {
            'calculations': [],
            'decisions': [],
            'anomalies': []
        }
        
        # Gather calculations
        for calc in self.calculations:
            if calc['variable'] == entity_name:
                entity_metadata['calculations'].append(calc)
        
        # Gather decisions
        for decision in self.decisions:
            if entity_name in str(decision['options']) or entity_name in str(decision['selected_option']):
                entity_metadata['decisions'].append(decision)
        
        # Gather anomalies
        for anomaly in self.anomalies:
            if anomaly['entity_name'] == entity_name:
                entity_metadata['anomalies'].append(anomaly)
                
        return entity_metadata
    
    def get_explanation_context(self, context_type, entity_name=None, step_id=None):
        """Assemble context package for LLM explanation generation"""
        context = {
            'type': context_type,
            'steps': self.get_step_summary(step_id),
            'entity_metadata': {}
        }
        
        if entity_name:
            context['entity_metadata'] = self.get_entity_metadata('variable', entity_name)
            
        return context

# Utility functions
def is_numeric_column(df, column_name):
    """Check if a column is numeric"""
    if column_name not in df.columns:
        return False
    return pd.api.types.is_numeric_dtype(df[column_name])

def get_column_stats(df, column_name):
    """Get statistical summary of a column"""
    if column_name not in df.columns:
        return None
    
    try:
        if is_numeric_column(df, column_name):
            values = df[column_name].values
            stats = {
                'min': float(np.min(values)),
                'max': float(np.max(values)),
                'mean': float(np.mean(values)),
                'median': float(np.median(values)),
                'std': float(np.std(values)),
                'missing': int(df[column_name].isna().sum()),
                'total': len(values)
            }
        else:
            # For categorical columns
            value_counts = df[column_name].value_counts()
            stats = {
                'unique_values': int(len(value_counts)),
                'most_common': str(value_counts.index[0]) if len(value_counts) > 0 else None,
                'most_common_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                'missing': int(df[column_name].isna().sum()),
                'total': len(df[column_name])
            }
        return stats
    except Exception as e:
        logger.error(f"Error getting stats for column {column_name}: {str(e)}")
        return None

def normalize_variable(values, relationship, metadata=None, step_id=None):
    """
    Normalize a single variable's values based on its relationship with malaria risk
    
    Args:
        values: NumPy array of values to normalize
        relationship: Relationship type ('direct'/'inverse')
        metadata: Optional AnalysisMetadata instance for logging
        step_id: Optional step ID for linking to metadata
        
    Returns:
        Normalized values as NumPy array
    """
    try:
        # Skip normalization if all values are the same
        if np.all(values == values[0]):
            logger.warning(f"All values identical in normalization, using default 0.5")
            
            if metadata:
                metadata.record_calculation(
                    step_id, 
                    'normalization', 
                    'default_value_assignment',
                    {'reason': 'all_values_identical', 'value': values[0]},
                    0.5
                )
                
            return np.full_like(values, 0.5)  # Default to middle value
        
        min_val = np.min(values)
        max_val = np.max(values)
        
        if metadata:
            metadata.record_calculation(
                step_id,
                'normalization',
                'range_calculation',
                {'array_size': len(values)},
                {'min': float(min_val), 'max': float(max_val)}
            )
        
        if relationship == 'inverse':
            # For inverse relationship, invert values then normalize
            # Add small constant to avoid division by zero
            inverted = 1 / (values + 1e-10)
            
            # Normalize inverted values
            inv_min = np.min(inverted)
            inv_max = np.max(inverted)
            
            if inv_min == inv_max:
                normalized = np.full_like(inverted, 0.5)  # Default to middle value
                
                if metadata:
                    metadata.record_calculation(
                        step_id,
                        'normalization',
                        'default_value_assignment',
                        {'reason': 'inverted_values_identical'},
                        0.5
                    )
            else:
                normalized = (inverted - inv_min) / (inv_max - inv_min)
                
                if metadata:
                    metadata.record_calculation(
                        step_id,
                        'normalization',
                        'inverse_normalization',
                        {'inverted_min': float(inv_min), 'inverted_max': float(inv_max)},
                        {'normalized_min': float(np.min(normalized)), 'normalized_max': float(np.max(normalized))}
                    )
        else:  # direct relationship
            # Normalize directly
            if min_val == max_val:
                normalized = np.full_like(values, 0.5)  # Default to middle value
                
                if metadata:
                    metadata.record_calculation(
                        step_id,
                        'normalization',
                        'default_value_assignment',
                        {'reason': 'min_equals_max'},
                        0.5
                    )
            else:
                normalized = (values - min_val) / (max_val - min_val)
                
                if metadata:
                    metadata.record_calculation(
                        step_id,
                        'normalization',
                        'direct_normalization',
                        {'min': float(min_val), 'max': float(max_val)},
                        {'normalized_min': float(np.min(normalized)), 'normalized_max': float(np.max(normalized))}
                    )
        
        return normalized
        
    except Exception as e:
        logger.error(f"Error in normalize_variable: {str(e)}")
        # Return original values scaled to 0-1 range as fallback
        try:
            min_val = np.min(values)
            max_val = np.max(values)
            if min_val == max_val:
                return np.full_like(values, 0.5)
            return (values - min_val) / (max_val - min_val)
        except:
            # If all else fails, return array of 0.5
            return np.full_like(values, 0.5)

def normalize_data(data, relationships, exclude_cols=None, n_jobs=-1, metadata=None):
    """
    Normalize data based on variable relationships with malaria risk
    
    Args:
        data: DataFrame with data to normalize
        relationships: Dict mapping variable names to relationships (direct/inverse)
        exclude_cols: List of columns to exclude from normalization
        n_jobs: Number of parallel jobs (-1 for all available cores)
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        DataFrame with normalized variables
    """
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'row_count': len(data),
                'column_count': len(data.columns),
                'relationships_count': len(relationships)
            }
            step_id = metadata.record_step(
                'normalize_data',
                input_summary,
                None,  # Output summary will be updated later
                'parallel_normalization',
                {'n_jobs': n_jobs, 'exclude_cols': exclude_cols}
            )
        
        # Create a copy to avoid modifying original
        normalized_df = data.copy()
        
        # Determine columns to normalize
        if 'WardName' in normalized_df.columns:
            # Get numeric columns excluding WardName
            numeric_cols = [col for col in normalized_df.columns 
                          if col != 'WardName' and pd.api.types.is_numeric_dtype(normalized_df[col])]
        else:
            numeric_cols = [col for col in normalized_df.columns 
                          if pd.api.types.is_numeric_dtype(normalized_df[col])]
        
        # Exclude specified columns
        if exclude_cols:
            numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        # Log the columns to be normalized
        if metadata:
            metadata.record_decision(
                step_id,
                'variable_selection',
                options=numeric_cols,
                criteria='numeric_non_excluded',
                selected_option=numeric_cols
            )
        
        # Define the worker function for parallel processing
        def normalize_column(col):
            if col in relationships:
                relationship = relationships[col]
                # Get column values
                values = normalized_df[col].values
                # Normalize based on relationship
                normalized = normalize_variable(values, relationship, metadata, step_id)
                # Create standardized column name
                norm_col_name = f"normalization_{col.lower()}"
                return norm_col_name, normalized
            return None, None
        
        # Use parallel processing for normalization
        if n_jobs != 1:  # Use parallel processing
            results = Parallel(n_jobs=n_jobs)(
                delayed(normalize_column)(col) for col in numeric_cols if col in relationships
            )
        else:  # Sequential processing
            results = [normalize_column(col) for col in numeric_cols if col in relationships]
        
        # Add normalized columns to dataframe
        normalized_cols = []
        for norm_col_name, normalized in results:
            if norm_col_name and normalized is not None:
                normalized_df[norm_col_name] = normalized
                normalized_cols.append(norm_col_name)
        
        # Update metadata with output summary
        if metadata:
            output_summary = {
                'row_count': len(normalized_df),
                'normalized_columns': len(normalized_cols),
                'normalized_column_names': normalized_cols
            }
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        return normalized_df
    
    except Exception as e:
        logger.error(f"Error in normalize_data: {str(e)}")
        traceback.print_exc()
        raise

def determine_variable_relationships(variables, descriptions=None, metadata=None):
    """
    Determine relationship of each variable with malaria risk
    
    Args:
        variables: List of variable names
        descriptions: Optional dict of variable descriptions to help determine relationships
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        Dict mapping variable names to relationships (direct/inverse)
    """
    # Record analysis step if metadata is provided
    step_id = None
    if metadata:
        input_summary = {
            'variable_count': len(variables),
            'has_descriptions': descriptions is not None
        }
        step_id = metadata.record_step(
            'determine_variable_relationships',
            input_summary,
            None,  # Output summary will be updated later
            'keyword_based_relationship_determination',
            {'has_descriptions': descriptions is not None}
        )
    
    # Keywords that typically indicate inverse relationship with malaria risk
    inverse_keywords = [
        'distance', 'elevation', 'altitude', 'slope', 'housing', 'quality',
        'income', 'education', 'access', 'urban', 'facility'
    ]
    
    # Keywords that typically indicate direct relationship with malaria risk
    direct_keywords = [
        'rainfall', 'precipitation', 'humidity', 'temperature', 'vegetation',
        'poverty', 'population', 'density', 'water', 'breeding'
    ]
    
    relationships = {}
    
    for var in variables:
        var_lower = var.lower()
        
        # Check description first if available
        relationship_found = False
        if descriptions and var in descriptions:
            desc = descriptions[var].lower()
            
            # Check each keyword in description
            for keyword in inverse_keywords:
                if keyword in desc:
                    relationships[var] = 'inverse'
                    relationship_found = True
                    
                    if metadata:
                        metadata.record_decision(
                            step_id,
                            'relationship_determination',
                            options=['direct', 'inverse'],
                            criteria=f'keyword "{keyword}" found in description',
                            selected_option='inverse'
                        )
                    break
                    
            if not relationship_found:
                for keyword in direct_keywords:
                    if keyword in desc:
                        relationships[var] = 'direct'
                        relationship_found = True
                        
                        if metadata:
                            metadata.record_decision(
                                step_id,
                                'relationship_determination',
                                options=['direct', 'inverse'],
                                criteria=f'keyword "{keyword}" found in description',
                                selected_option='direct'
                            )
                        break
        
        # If no relationship found from description, check variable name
        if not relationship_found:
            if any(keyword in var_lower for keyword in inverse_keywords):
                relationships[var] = 'inverse'
                
                if metadata:
                    matching_keywords = [keyword for keyword in inverse_keywords if keyword in var_lower]
                    metadata.record_decision(
                        step_id,
                        'relationship_determination',
                        options=['direct', 'inverse'],
                        criteria=f'keyword(s) {matching_keywords} found in variable name',
                        selected_option='inverse'
                    )
            else:
                # Default to direct relationship
                relationships[var] = 'direct'
                
                if metadata:
                    metadata.record_decision(
                        step_id,
                        'relationship_determination',
                        options=['direct', 'inverse'],
                        criteria='default to direct relationship (no inverse keywords found)',
                        selected_option='direct'
                    )
    
    # Update metadata with output summary
    if metadata:
        output_summary = {
            'variable_count': len(variables),
            'direct_relationships': sum(1 for rel in relationships.values() if rel == 'direct'),
            'inverse_relationships': sum(1 for rel in relationships.values() if rel == 'inverse')
        }
        # Update the step with output information
        for step in metadata.steps:
            if step['step_id'] == step_id:
                step['output_summary'] = output_summary
                break
    
    return relationships

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
        
        # Create spatial weights using queen contiguity
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
                    f'imputation_{method_used}',
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
        logger.error(f"Error in handle_spatial_imputation for {column}: {str(e)}")
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
        logger.warning(f"Column {column} is not numeric, cannot apply mean imputation")
        return data  # Return original data unchanged
    
    # Create a copy to avoid modifying original
    result = data.copy()
    
    # Calculate mean excluding NaN values
    mean_value = result[column].mean()
    
    if pd.isna(mean_value):
        logger.warning(f"All values in column {column} are NaN, using 0 as fallback")
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
            logger.warning(f"Could not determine mode for column {column}. Using forward/backward fill.")
            
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
                logger.warning(f"ffill/bfill failed for column {column}. Using 'Unknown' placeholder.")
                
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
        logger.error(f"Error in mode imputation for {column}: {str(e)}")
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
               logger.warning(f"Spatial imputation ineffective for {col}, falling back to mean")
               if pd.api.types.is_numeric_dtype(cleaned_df[col]):
                   cleaned_df = handle_mean_imputation(cleaned_df, col, metadata, step_id)
                   methods_used[col] = 'spatial_then_mean'
               else:
                   cleaned_df = handle_mode_imputation(cleaned_df, col, metadata, step_id)
                   methods_used[col] = 'spatial_then_mode'
       elif method == 'knn':
           # KNN imputation is complex and requires additional libraries
           # For this example, we'll fall back to mean/mode
           logger.warning(f"KNN imputation not implemented, falling back to mean/mode for {col}")
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
               logger.warning(f"Mean imputation not suitable for non-numeric column {col}, using mode instead")
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

def compute_composite_score_model(normalized_data, variables, metadata=None, step_id=None):
   """
   Calculate a single composite score model using selected normalized variables
   
   Args:
       normalized_data: DataFrame with normalized variables
       variables: List of normalized column names to use
       metadata: Optional AnalysisMetadata instance for logging
       step_id: Optional step ID for linking to metadata
       
   Returns:
       Series with composite scores
   """
   try:
       # Compute simple mean of normalized values
       composite_scores = normalized_data[variables].mean(axis=1)
       
       # Record the calculation if metadata is provided
       if metadata and step_id:
           variable_names = [var.replace('normalization_', '') for var in variables]
           metadata.record_calculation(
               step_id,
               'composite_score',
               'mean_aggregation',
               {'variables': variable_names, 'row_count': len(normalized_data)},
               {'min_score': float(composite_scores.min()), 
                'max_score': float(composite_scores.max()),
                'mean_score': float(composite_scores.mean())}
           )
       
       return composite_scores
   except Exception as e:
       logger.error(f"Error computing composite score model: {str(e)}")
       # Return zeros as fallback
       return pd.Series(0, index=normalized_data.index)

def compute_composite_scores(normalized_data, selected_vars=None, method='mean', n_jobs=-1, metadata=None):
   """
   Calculate composite scores using selected normalized variables
   
   Args:
       normalized_data: DataFrame with normalized variables
       selected_vars: List of variables to use (if None, use all normalized variables)
       method: Aggregation method ('mean', 'weighted_mean', 'pca')
       n_jobs: Number of parallel jobs (-1 for all available cores)
       metadata: Optional AnalysisMetadata instance for logging
       
   Returns:
       Dict with scores DataFrame and model formulas
   """
   try:
       # Record analysis step if metadata is provided
       step_id = None
       if metadata:
           input_summary = {
               'row_count': len(normalized_data),
               'selected_vars_count': len(selected_vars) if selected_vars else 0,
               'method': method
           }
           step_id = metadata.record_step(
               'compute_composite_scores',
               input_summary,
               None,  # Output summary will be updated later
               'composite_score_calculation',
               {'method': method, 'n_jobs': n_jobs}
           )
       
       # Make sure WardName column is present
       if 'WardName' not in normalized_data.columns:
           raise ValueError("WardName column must be present in normalized data")
       
       # Get normalized columns (starting with "normalization_")
       norm_cols = [col for col in normalized_data.columns if col.startswith('normalization_')]
       
       # If specific variables are selected, filter columns
       if selected_vars:
           selected_norm_cols = []
           for var in selected_vars:
               norm_col = f"normalization_{var.lower()}"
               if norm_col in norm_cols:
                   selected_norm_cols.append(norm_col)
               elif var in norm_cols:  # Allow already normalized column names
                   selected_norm_cols.append(var)
           norm_cols = selected_norm_cols
           
           if metadata:
               metadata.record_decision(
                   step_id,
                   'variable_selection',
                   options=[col.replace('normalization_', '') for col in normalized_data.columns 
                            if col.startswith('normalization_')],
                   criteria='user specified selection',
                   selected_option=[col.replace('normalization_', '') for col in norm_cols]
               )
       
       # Need at least 2 variables for composite score
       if len(norm_cols) < 2:
           raise ValueError(f"Need at least 2 normalized variables. Found {len(norm_cols)}.")
       
       # Initialize result dataframe with WardName
       result = pd.DataFrame({'WardName': normalized_data['WardName']})
       
       # Generate all combinations
       # Create a list to store model formulas
       model_formulas = []
       
       # If only 2 variables, use just one model
       if len(norm_cols) == 2:
           combinations = [norm_cols]
           if metadata:
               metadata.record_calculation(
                   step_id,
                   'combinations_generation',
                   'exact_two_variables',
                   {'variable_count': 2},
                   {'combination_count': 1}
               )
       else:
           # For 3+ variables, generate all valid combinations
           combinations = []
           for r in range(2, len(norm_cols) + 1):
               combinations.extend(list(itertools.combinations(norm_cols, r)))
           
           if metadata:
               metadata.record_calculation(
                   step_id,
                   'combinations_generation',
                   'all_combinations',
                   {'variable_count': len(norm_cols), 'min_size': 2, 'max_size': len(norm_cols)},
                   {'combination_count': len(combinations)}
               )
       
       # Define worker function for parallel processing
       def compute_model(i, combo):
           model_name = f"model_{i+1}"
           
           # Calculate composite score based on method
           if method == 'mean':
               # Simple mean of normalized values
               result_series = compute_composite_score_model(
                   normalized_data, list(combo), metadata, step_id
               )
           elif method == 'weighted_mean':
               # Weighted mean (equal weights for now)
               weights = np.ones(len(combo)) / len(combo)
               result_series = pd.Series(
                   np.average(normalized_data[list(combo)], axis=1, weights=weights),
                   index=normalized_data.index
               )
           elif method == 'pca':
               # Principal Component Analysis would be implemented here
               # For simplicity, fallback to mean
               logger.warning("PCA method not implemented, falling back to mean")
               result_series = compute_composite_score_model(
                   normalized_data, list(combo), metadata, step_id
               )
           else:
               # Default to mean
               result_series = compute_composite_score_model(
                   normalized_data, list(combo), metadata, step_id
               )
           
           # Create model formula
           variables_used = [col.replace('normalization_', '') for col in combo]
           formula = {
               'model': model_name,
               'variables': variables_used
           }
           
           return model_name, result_series, formula
       
       # Calculate composite score for each combination
       if n_jobs != 1 and len(combinations) > 1:
           # Use parallel processing
           with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs if n_jobs > 0 else None) as executor:
               model_results = list(executor.map(
                   lambda args: compute_model(*args), 
                   [(i, combo) for i, combo in enumerate(combinations)]
               ))
       else:
           # Sequential processing
           model_results = [compute_model(i, combo) for i, combo in enumerate(combinations)]
       
       # Process results
       for model_name, result_series, formula in model_results:
           result[model_name] = result_series
           model_formulas.append(formula)
       
       # Update metadata with output summary
       if metadata:
           output_summary = {
               'model_count': len(model_formulas),
               'ward_count': len(result),
               'method_used': method
           }
           # Update the step with output information
           for step in metadata.steps:
               if step['step_id'] == step_id:
                   step['output_summary'] = output_summary
                   break
       
       # Return dictionary with results and formulas
       return {
           'scores': result,
           'model_formulas': model_formulas
       }
   
   except Exception as e:
       logger.error(f"Error computing composite scores: {str(e)}")
       traceback.print_exc()
       raise

def analyze_vulnerability(composite_scores, n_categories=3, metadata=None):
   """
   Analyze vulnerability based on composite scores
   
   Args:
       composite_scores: Dict with scores DataFrame and model formulas
       n_categories: Number of vulnerability categories
       metadata: Optional AnalysisMetadata instance for logging
       
   Returns:
       DataFrame with vulnerability rankings
   """
   try:
       # Record analysis step if metadata is provided
       step_id = None
       if metadata:
           input_summary = {
               'model_count': len([col for col in composite_scores['scores'].columns 
                                  if col.startswith('model_')]),
               'ward_count': len(composite_scores['scores']),
               'n_categories': n_categories
           }
           step_id = metadata.record_step(
               'analyze_vulnerability',
               input_summary,
               None,  # Output summary will be updated later
               'median_rank_calculation',
               {'n_categories': n_categories}
           )
       
       # Extract scores dataframe
       scores_df = composite_scores['scores']
       
       # Get model columns
       model_cols = [col for col in scores_df.columns if col.startswith('model_')]
       
       if not model_cols:
           raise ValueError("No model scores found in composite scores")
       
       # Initialize results dataframe with WardName
       result = scores_df[['WardName']].copy()
       
       # Calculate median score across all models
       result['median_score'] = scores_df[model_cols].median(axis=1)
       
       if metadata:
           metadata.record_calculation(
               step_id,
               'vulnerability',
               'median_score_calculation',
               {'model_count': len(model_cols), 'ward_count': len(scores_df)},
               {'min_score': float(result['median_score'].min()), 
                'max_score': float(result['median_score'].max()),
                'mean_score': float(result['median_score'].mean())}
           )
       
       # Sort by median score (descending) to get overall rank
       result = result.sort_values('median_score', ascending=False)
       result['overall_rank'] = range(1, len(result) + 1)
       
       # Reset index
       result = result.reset_index(drop=True)
       
       # Categorize into vulnerability levels
       n_wards = len(result)
       category_bins = np.linspace(0, n_wards, n_categories + 1).astype(int)
       category_labels = ['High', 'Medium', 'Low'][:n_categories]
       
       result['vulnerability_category'] = pd.cut(
           result['overall_rank'],
           bins=category_bins,
           labels=category_labels,
           include_lowest=True
       )
       
       # Record category counts if metadata is provided
       if metadata:
           category_counts = result['vulnerability_category'].value_counts().to_dict()
           metadata.record_calculation(
               step_id,
               'vulnerability',
               'category_assignment',
               {'n_categories': n_categories, 'category_labels': category_labels},
               {'category_counts': category_counts}
           )
           
           # Identify notable wards (e.g., highest and lowest vulnerability)
           top_wards = result.head(5)['WardName'].tolist()
           bottom_wards = result.tail(5)['WardName'].tolist()
           
           metadata.record_calculation(
               step_id,
               'vulnerability',
               'notable_wards_identification',
               {'criterion': 'overall_rank'},
               {'most_vulnerable': top_wards, 'least_vulnerable': bottom_wards}
           )
       
       # Update metadata with output summary
       if metadata:
           output_summary = {
               'ward_count': len(result),
               'categories': {cat: int(count) for cat, count in category_counts.items()},
               'top_vulnerable_wards': top_wards
           }
           # Update the step with output information
           for step in metadata.steps:
               if step['step_id'] == step_id:
                   step['output_summary'] = output_summary
                   break
       
       return result
   
   except Exception as e:
       logger.error(f"Error analyzing vulnerability: {str(e)}")
       traceback.print_exc()
       raise

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
                           criteria=f'found matching column in data: {urban_percent_col}',
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
                       criteria=f'found binary urban column: {urban_col}',
                       selected_option='use_binary_column'
                   )
       
       # Ensure urban_percent_col exists in data
       if urban_percent_col is None or urban_percent_col not in data.columns:
           error_msg = f"Urban percentage column not found in data"
           
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
           meets_threshold_field = f'MeetsThreshold_{threshold}'
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
                   f'threshold_{threshold}_classification',
                   {'threshold': threshold, 'ward_count': len(result_df)},
                   {'meets_count': meets_count, 'below_count': below_count}
               )
               
               if meets_count == 0:
                   metadata.record_anomaly(
                       f'threshold_{threshold}',
                       'no_wards_above_threshold',
                       expected_value='at least one ward above threshold',
                       actual_value=f'{meets_count} wards',
                       significance='high',
                       context='This threshold might be too high for the dataset'
                   )
               elif below_count == 0:
                   metadata.record_anomaly(
                       f'threshold_{threshold}',
                       'no_wards_below_threshold',
                       expected_value='at least one ward below threshold',
                       actual_value=f'{below_count} wards',
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
       logger.error(f"Error analyzing urban extent: {str(e)}")
       traceback.print_exc()
       raise

def run_full_analysis_pipeline(data_handler, selected_variables=None, 
                             na_methods=None, custom_relationships=None, 
                             metadata=None, session_id=None, interaction_logger=None,
                             llm_manager=None):  # Added llm_manager parameter
   """
   Run the complete analysis pipeline
   
   Args:
       data_handler: DataHandler instance
       selected_variables: List of variables to use for composite scores
       na_methods: Dict mapping columns to methods for handling missing values
       custom_relationships: Dict mapping variables to relationships (direct/inverse)
       metadata: Optional AnalysisMetadata instance for logging
       session_id: Optional session ID for logging
       interaction_logger: Optional interaction logger instance
       llm_manager: Optional LLM manager for AI-driven variable selection
       
   Returns:
       Dict with analysis results
   """
   try:
       # Initialize metadata if not provided
       if metadata is None:
           metadata = AnalysisMetadata(session_id, interaction_logger)
       
       # Record analysis pipeline start
       pipeline_step_id = metadata.record_step(
           'full_analysis_pipeline',
           {'selected_variables': selected_variables,
            'na_methods': na_methods,
            'custom_relationships': custom_relationships,
            'llm_selection': llm_manager is not None and selected_variables is None},  # Track if LLM selection is used
           None,
           'complete_analysis',
           {'timestamp': time.time()}
       )
       
       # Track which steps need to be re-run
       rerun_stages = {
           'clean': True,
           'relationships': True,
           'normalize': True,
           'composite': True,
           'ranking': True,
           'urban': True
       }
       
       # If we already have cleaned data and no custom NA methods, skip cleaning
       if data_handler.cleaned_data is not None and na_methods is None:
           rerun_stages['clean'] = False
           logger.info("Reusing existing cleaned data (no custom NA methods specified)")
           metadata.record_decision(
               pipeline_step_id,
               'reuse_existing_data',
               options=['rerun_cleaning', 'use_existing_cleaned_data'],
               criteria='no custom NA methods specified',
               selected_option='use_existing_cleaned_data'
           )
       
       # If we already have variable relationships and no custom relationships, skip relationship determination
       if data_handler.variable_relationships and not custom_relationships:
           rerun_stages['relationships'] = False
           logger.info("Reusing existing variable relationships (no custom relationships specified)")
           metadata.record_decision(
               pipeline_step_id,
               'reuse_existing_relationships',
               options=['redetermine_relationships', 'use_existing_relationships'],
               criteria='no custom relationships specified',
               selected_option='use_existing_relationships'
           )
       
       # If selected_variables is provided but no other customizations,
       # reuse everything up to composite score calculation
       if (selected_variables and not custom_relationships and not na_methods and 
           data_handler.normalized_data is not None):
           rerun_stages['clean'] = False
           rerun_stages['relationships'] = False
           rerun_stages['normalize'] = False
           logger.info("Only rerunning composite score calculation with selected variables")
           metadata.record_decision(
               pipeline_step_id,
               'reuse_existing_normalized_data',
               options=['renormalize_data', 'use_existing_normalized_data'],
               criteria='only custom variable selection specified',
               selected_option='use_existing_normalized_data'
           )
       
       # 1. Clean data if needed
       logger.info("Step 1: Cleaning data")
       clean_result = None
       if rerun_stages['clean']:
           start_time = time.time()
           step_id = metadata.record_step(
               'clean_data',
               {'has_csv_data': data_handler.csv_data is not None,
                'has_shapefile_data': data_handler.shapefile_data is not None,
                'na_methods': na_methods},
               None,
               'data_cleaning',
               {'custom_na_methods': na_methods is not None}
           )
           
           try:
               # Ensure we have data
               if data_handler.csv_data is None:
                   return {
                       'status': 'error',
                       'message': 'No CSV data loaded'
                   }
               
               # Clean the data
               cleaned_data = handle_missing_values(
                   data_handler.csv_data,
                   na_methods,
                   data_handler.shapefile_data,
                   -1,  # Use all available cores
                   metadata
               )
               
               # Store cleaned data
               data_handler.cleaned_data = cleaned_data
               data_handler.na_handling_methods = metadata.calculations
               
               # Update step with results
               execution_time = time.time() - start_time
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'row_count': len(cleaned_data),
                           'column_count': len(cleaned_data.columns),
                           'na_methods_used': na_methods
                       }
                       break
               
               clean_result = {
                   'status': 'success',
                   'message': 'Successfully cleaned data',
                   'execution_time': execution_time
               }
           except Exception as e:
               logger.error(f"Error cleaning data: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error cleaning data: {str(e)}'
               }
       else:
           clean_result = {'status': 'success', 'message': 'Using previously cleaned data'}
       
       # 2. Determine variable relationships if needed
       logger.info("Step 2: Determining variable relationships")
       if rerun_stages['relationships']:
           start_time = time.time()
           step_id = metadata.record_step(
               'determine_variable_relationships',
               {'has_cleaned_data': data_handler.cleaned_data is not None},
               None,
               'relationship_determination',
               {'custom_relationships': custom_relationships is not None}
           )
           
           try:
               # Get variables from cleaned data
               variables = [col for col in data_handler.cleaned_data.columns 
                          if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.cleaned_data[col])]
               
               # Determine relationships
               relationships = determine_variable_relationships(variables, None, metadata)
               
               # Apply custom relationships if provided
               if custom_relationships:
                   for var, rel in custom_relationships.items():
                       if var in relationships:
                           old_rel = relationships[var]
                           relationships[var] = rel
                           
                           metadata.record_decision(
                               step_id,
                               'custom_relationship_override',
                               options=['direct', 'inverse'],
                               criteria=f'user specified override for {var}',
                               selected_option=rel
                           )
                           
                           logger.info(f"Changed relationship for {var} from {old_rel} to {rel}")
                       else:
                           relationships[var] = rel
                           
                           metadata.record_decision(
                               step_id,
                               'custom_relationship_addition',
                               options=['direct', 'inverse'],
                               criteria=f'user specified relationship for {var}',
                               selected_option=rel
                           )
                           
                           logger.info(f"Added new relationship for {var}: {rel}")
               
               # Store relationships
               data_handler.variable_relationships = relationships
               
               # Update step with results
               execution_time = time.time() - start_time
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'variable_count': len(variables),
                           'direct_relationships': sum(1 for rel in relationships.values() if rel == 'direct'),
                           'inverse_relationships': sum(1 for rel in relationships.values() if rel == 'inverse')
                       }
                       break
           except Exception as e:
               logger.error(f"Error determining variable relationships: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error determining variable relationships: {str(e)}'
               }
       
       # 3. Normalize data if needed
       logger.info("Step 3: Normalizing data")
       norm_result = None
       if rerun_stages['normalize']:
           start_time = time.time()
           step_id = metadata.record_step(
               'normalize_data',
               {'has_cleaned_data': data_handler.cleaned_data is not None,
                'has_relationships': bool(data_handler.variable_relationships)},
               None,
               'data_normalization',
               {'rerun_normalization': rerun_stages['normalize']}
           )
           
           try:
               # Ensure we have data and relationships
               if data_handler.cleaned_data is None:
                   return {
                       'status': 'error',
                       'message': 'No cleaned data available for normalization'
                   }
               
               if not data_handler.variable_relationships:
                   return {
                       'status': 'error',
                       'message': 'No variable relationships determined for normalization'
                   }
               
               # Normalize the data
               normalized_data = normalize_data(
                   data_handler.cleaned_data,
                   data_handler.variable_relationships,
                   None,  # No columns to exclude
                   -1,  # Use all available cores
                   metadata
               )
               
               # Store normalized data
               data_handler.normalized_data = normalized_data
               
               # Update step with results
               execution_time = time.time() - start_time
               norm_cols = [col for col in normalized_data.columns if col.startswith('normalization_')]
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'row_count': len(normalized_data),
                           'normalized_columns': len(norm_cols),
                           'normalized_column_names': norm_cols
                       }
                       break
               
               norm_result = {
                   'status': 'success',
                   'message': f'Successfully normalized {len(norm_cols)} variables',
                   'normalized_columns': norm_cols,
                   'execution_time': execution_time
               }
           except Exception as e:
               logger.error(f"Error normalizing data: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error normalizing data: {str(e)}'
               }
       else:
           norm_result = {'status': 'success', 'message': 'Using previously normalized data'}
       
       # 4. Calculate composite scores
       logger.info("Step 4: Calculating composite scores")
       composite_result = None
       if rerun_stages['composite'] or selected_variables:
           start_time = time.time()
           step_id = metadata.record_step(
               'compute_composite_scores',
               {'has_normalized_data': data_handler.normalized_data is not None,
                'selected_variables': selected_variables,
                'using_llm_selection': llm_manager is not None and selected_variables is None},
               None,
               'composite_score_calculation',
               {'selected_variables': selected_variables,
                'llm_selection': llm_manager is not None and selected_variables is None}
           )
           
           try:
               # Ensure we have normalized data
               if data_handler.normalized_data is None:
                   return {
                       'status': 'error',
                       'message': 'No normalized data available for composite score calculation'
                   }
               
               # Determine variables to use:
               # 1. User-provided selected_variables (highest priority)
               # 2. LLM-selected variables if llm_manager is provided
               # 3. All variables (fallback)
               
               clean_selected_vars = None
               variable_selection_method = "default"
               variable_explanations = {}
               
               # Case 1: User-provided variables
               if selected_variables:
                   # Convert to lowercase for more flexible matching
                   selected_vars_lower = [var.lower() for var in selected_variables]
                   
                   # Get normalized column names
                   norm_cols = [col for col in data_handler.normalized_data.columns 
                              if col.startswith('normalization_')]
                   
                   # Match selected variables to normalized columns
                   clean_selected_vars = []
                   for var in selected_vars_lower:
                       # Try direct match with normalized column name
                       norm_var = f"normalization_{var}"
                       if norm_var in norm_cols:
                           clean_selected_vars.append(var)
                           continue
                       
                       # Try partial match
                       for col in norm_cols:
                           if var in col.replace('normalization_', '').lower():
                               clean_selected_vars.append(col.replace('normalization_', ''))
                               break
                   
                   # Log selected variables
                   metadata.record_decision(
                       step_id,
                       'variable_selection_method',
                       options=['user_specified', 'llm_selected', 'all_variables'],
                       criteria='user specified variables provided',
                       selected_option='user_specified'
                   )
                   
                   metadata.record_decision(
                       step_id,
                       'variable_selection_cleaning',
                       options=selected_variables,
                       criteria='matching to available normalized variables',
                       selected_option=clean_selected_vars
                   )
                   
                   # Store selection method for later explanation
                   variable_selection_method = "user_specified"
                   variable_explanations = {"source": "User-specified variables"}
               
               # Case 2: LLM-selected variables if available and no user-provided variables
               elif llm_manager is not None:
                   # First, get all available variables
                   from app.ai_utils import select_optimal_variables_with_llm
                   
                   # Get all normalized variable names
                   norm_cols = [col for col in data_handler.normalized_data.columns 
                              if col.startswith('normalization_')]
                   
                   # Convert to base variable names
                   available_vars = [col.replace('normalization_', '') for col in norm_cols]
                   
                   # Use LLM to select the optimal variables
                   clean_selected_vars, explanations = select_optimal_variables_with_llm(
                       llm_manager=llm_manager,
                       available_vars=available_vars,
                       csv_data=data_handler.cleaned_data,
                       relationships=data_handler.variable_relationships,
                       min_vars=3,
                       max_vars=5
                   )
                   
                   # Log the LLM selection
                   metadata.record_decision(
                       step_id,
                       'variable_selection_method',
                       options=['user_specified', 'llm_selected', 'all_variables'],
                       criteria='using LLM to select optimal variables',
                       selected_option='llm_selected'
                   )
                   
                   # Store selection method and explanations for later
                   variable_selection_method = "llm_selected"
                   variable_explanations = explanations
               
               # Case 3: Fallback to all variables if no selection method is applicable
               if clean_selected_vars is None or len(clean_selected_vars) < 2:
                   # Fallback to all variables
                   clean_selected_vars = [col.replace('normalization_', '') for col in 
                                       data_handler.normalized_data.columns if col.startswith('normalization_')]
                   
                   # Log the fallback
                   metadata.record_decision(
                       step_id,
                       'variable_selection_method',
                       options=['user_specified', 'llm_selected', 'all_variables'],
                       criteria='no selection method provided or insufficient variables selected',
                       selected_option='all_variables'
                   )
                   
                   # Store selection method for later explanation
                   variable_selection_method = "all_variables"
                   variable_explanations = {"source": "Using all available variables"}
               
               # Ensure we have enough variables
               if len(clean_selected_vars) < 2:
                   return {
                       'status': 'error',
                       'message': f'Not enough valid variables selected. Found {len(clean_selected_vars)}, need at least 2.'
                   }
               
               # Store variable selection info for later use
               data_handler.variable_selection_method = variable_selection_method
               data_handler.variable_selection_explanations = variable_explanations
               
               # Calculate composite scores
               composite_scores = compute_composite_scores(
                   data_handler.normalized_data,
                   clean_selected_vars,
                   'mean',  # Use mean method
                   -1,  # Use all available cores
                   metadata
               )
               
               # Store composite scores and variables
               data_handler.composite_scores = composite_scores
               data_handler.composite_variables = clean_selected_vars
               
               # Update step with results
               execution_time = time.time() - start_time
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'model_count': len(composite_scores['model_formulas']),
                           'variables_used': data_handler.composite_variables,
                           'selection_method': variable_selection_method
                       }
                       break
               
               composite_result = {
                   'status': 'success',
                   'message': f'Successfully calculated {len(composite_scores["model_formulas"])} composite score models',
                   'variables_used': data_handler.composite_variables,
                   'selection_method': variable_selection_method,
                   'execution_time': execution_time
               }
           except Exception as e:
               logger.error(f"Error calculating composite scores: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error calculating composite scores: {str(e)}'
               }
       else:
           if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores:
               composite_result = {
                   'status': 'success', 
                   'message': 'Using previously calculated composite scores',
                   'variables_used': data_handler.composite_variables or [],
                   'selection_method': getattr(data_handler, 'variable_selection_method', 'default')
               }
           else:
               # If we don't have composite scores yet, calculate them
               composite_result = compute_composite_scores(
                   data_handler.normalized_data,
                   None,  # Use all normalized variables
                   'mean',  # Use mean method
                   -1,  # Use all available cores
                   metadata
               )
               
               if composite_result['status'] != 'success':
                   return composite_result
       
       # 5. Calculate vulnerability rankings
       logger.info("Step 5: Calculating vulnerability rankings")
       ranking_result = None
       if rerun_stages['ranking'] or selected_variables:
           start_time = time.time()
           step_id = metadata.record_step(
               'calculate_vulnerability_rankings',
               {'has_composite_scores': hasattr(data_handler, 'composite_scores') and 
                                       data_handler.composite_scores is not None},
               None,
               'vulnerability_ranking',
               {'n_categories': 3}  # Default to 3 categories
           )
           
           try:
               # Ensure we have composite scores
               if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
                   return {
                       'status': 'error',
                       'message': 'No composite scores available for vulnerability ranking'
                   }
               
               # Calculate vulnerability rankings
               vulnerability_rankings = analyze_vulnerability(
                   data_handler.composite_scores,
                   3,  # Use 3 categories (High, Medium, Low)
                   metadata
               )
               
               # Store vulnerability rankings
               data_handler.vulnerability_rankings = vulnerability_rankings
               
               # Get top vulnerable wards
               top_wards = vulnerability_rankings.sort_values('overall_rank').head(10)['WardName'].tolist()
               
               # Update step with results
               execution_time = time.time() - start_time
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'ward_count': len(vulnerability_rankings),
                           'top_vulnerable_wards': top_wards[:5],
                           'vulnerability_categories': vulnerability_rankings['vulnerability_category'].value_counts().to_dict()
                       }
                       break
               
               ranking_result = {
                   'status': 'success',
                   'message': f'Successfully ranked {len(vulnerability_rankings)} wards by vulnerability',
                   'vulnerable_wards': top_wards,
                   'execution_time': execution_time
               }
           except Exception as e:
               logger.error(f"Error calculating vulnerability rankings: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error calculating vulnerability rankings: {str(e)}'
               }
       else:
           if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
               top_wards = data_handler.vulnerability_rankings.sort_values('overall_rank').head(10)['WardName'].tolist()
               ranking_result = {
                   'status': 'success', 
                   'message': 'Using previously calculated vulnerability rankings',
                   'vulnerable_wards': top_wards[:5]
               }
           else:
               # If we don't have vulnerability rankings yet, calculate them
               ranking_result = analyze_vulnerability(
                   data_handler.composite_scores,
                   3,  # Use 3 categories (High, Medium, Low)
                   metadata
               )
               
               if ranking_result['status'] != 'success':
                   return ranking_result
       
       # 6. Process urban extent
       logger.info("Step 6: Processing urban extent")
       urban_result = None
       if rerun_stages['urban']:
           start_time = time.time()
           step_id = metadata.record_step(
               'process_urban_extent',
               {'has_csv_data': data_handler.csv_data is not None,
                'has_shapefile_data': data_handler.shapefile_data is not None},
               None,
               'urban_extent_analysis',
               {'thresholds': [30, 50, 75, 100]}  # Default thresholds
           )
           
           try:
               # Ensure we have data
               if data_handler.csv_data is None:
                   return {
                       'status': 'error',
                       'message': 'No CSV data available for urban extent analysis'
                   }
               
               if data_handler.shapefile_data is None:
                   return {
                       'status': 'error',
                       'message': 'No shapefile data available for urban extent analysis'
                   }
               
               # Process urban extent
               urban_extent_results = analyze_urban_extent(
                   data_handler.csv_data,
                   data_handler.shapefile_data,
                   None,  # Auto-detect urban percentage column
                   [30, 50, 75, 100],  # Use default thresholds
                   metadata
               )
               
               # Store urban extent results
               data_handler.urban_extent_results = urban_extent_results
               
               # Update step with results
               execution_time = time.time() - start_time
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['execution_time'] = execution_time
                       step['output_summary'] = {
                           'thresholds_analyzed': len(urban_extent_results),
                           'threshold_results': {
                               str(threshold): {
                                   'meets_count': results['meets_threshold'],
                                   'below_count': results['below_threshold']
                               } for threshold, results in urban_extent_results.items()
                           }
                       }
                       break
               
               urban_result = {
                   'status': 'success',
                   'message': f'Successfully analyzed urban extent at {len(urban_extent_results)} thresholds',
                   'thresholds': list(urban_extent_results.keys()),
                   'execution_time': execution_time
               }
           except Exception as e:
               logger.error(f"Error processing urban extent: {str(e)}")
               
               for step in metadata.steps:
                   if step['step_id'] == step_id:
                       step['error'] = str(e)
                       break
               
               return {
                   'status': 'error',
                   'message': f'Error processing urban extent: {str(e)}'
               }
       else:
           if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
               urban_result = {
                   'status': 'success', 
                   'message': 'Using previously calculated urban extent results',
                   'thresholds': list(data_handler.urban_extent_results.keys())
               }
           else:
               # If we don't have urban extent results yet, calculate them
               urban_result = analyze_urban_extent(
                   data_handler.csv_data,
                   data_handler.shapefile_data,
                   None,  # Auto-detect urban percentage column
                   [30, 50, 75, 100],  # Use default thresholds
                   metadata
               )
               
               if urban_result['status'] != 'success':
                   return urban_result
       
       # Update pipeline step with results
       total_execution_time = time.time() - metadata.start_time
       for step in metadata.steps:
           if step['step_id'] == pipeline_step_id:
               step['execution_time'] = total_execution_time
               step['output_summary'] = {
                   'clean_result': clean_result.get('status') if clean_result else None,
                   'norm_result': norm_result.get('status') if norm_result else None,
                   'composite_result': composite_result.get('status') if composite_result else None,
                   'ranking_result': ranking_result.get('status') if ranking_result else None,
                   'urban_result': urban_result.get('status') if urban_result else None,
                   'variables_used': composite_result.get('variables_used') if composite_result else [],
                   'selection_method': composite_result.get('selection_method', 'default'),
                   'vulnerable_wards': ranking_result.get('vulnerable_wards') if ranking_result else []
               }
               break
       
       # Compile summary of all steps
       logger.info("Analysis pipeline complete")
       summary = {
           'status': 'success',
           'message': 'Complete analysis pipeline successfully executed',
           'steps': {
               'clean': clean_result,
               'normalize': norm_result,
               'composite': composite_result,
               'ranking': ranking_result,
               'urban': urban_result
           },
           'variables_used': composite_result.get('variables_used') if composite_result else [],
           'selection_method': composite_result.get('selection_method', 'default'),
           'vulnerable_wards': ranking_result.get('vulnerable_wards') if ranking_result else [],
           'execution_time': total_execution_time,
           'metadata': metadata  # Include metadata for explanation
       }
       
       return summary
       
   except Exception as e:
       logger.error(f"Error in full analysis pipeline: {str(e)}")
       traceback.print_exc()
       return {
           'status': 'error',
           'message': f'Error in full analysis pipeline: {str(e)}'
       }

def get_explanation_for_visualization(visualization_type, visualization_data, question=None, metadata=None):
   """
   Generate explanation context for a visualization
   
   Args:
       visualization_type: Type of visualization (e.g., 'variable_map', 'composite_map')
       visualization_data: Data used to create the visualization
       question: Optional specific question about the visualization
       metadata: Optional AnalysisMetadata instance for context
       
   Returns:
       Dict with context for LLM explanation
   """
   context = {
       'visualization_type': visualization_type,
       'data_summary': {},
       'visual_elements': {},
       'analysis_context': {},
       'specific_question': question
   }
   
   # Add generic visualization type information
   if visualization_type == 'variable_map':
       context['visual_elements']['color_scale'] = 'Blues'
       context['visual_elements']['color_meaning'] = 'darker blue = higher values'
       
       if 'variable' in visualization_data:
           variable = visualization_data['variable']
           context['analysis_context']['variable_displayed'] = variable
           
           if 'full_variable_name' in visualization_data:
               context['analysis_context']['variable_full_name'] = visualization_data['full_variable_name']
           
           if 'missing_values' in visualization_data:
               context['data_summary']['missing_values'] = visualization_data['missing_values']
   
   elif visualization_type == 'normalized_map':
       context['visual_elements']['color_scale'] = 'YlOrRd'
       context['visual_elements']['color_meaning'] = 'darker red = higher normalized value'
       
       if 'variable' in visualization_data:
           variable = visualization_data['variable']
           context['analysis_context']['variable_displayed'] = variable
           
           if 'full_variable_name' in visualization_data:
               context['analysis_context']['variable_full_name'] = visualization_data['full_variable_name']
           
           if 'relationship' in visualization_data:
               context['analysis_context']['variable_relationship'] = visualization_data['relationship']
   
   elif visualization_type == 'composite_map':
       context['visual_elements']['color_scale'] = 'YlOrRd'
       context['visual_elements']['color_meaning'] = 'darker red = higher risk'
       
       if 'current_page' in visualization_data and 'total_pages' in visualization_data:
           context['data_summary']['current_page'] = visualization_data['current_page']
           context['data_summary']['total_pages'] = visualization_data['total_pages']
   
   elif visualization_type == 'vulnerability_map':
       context['visual_elements']['color_scale'] = 'Plasma_r'
       context['visual_elements']['color_meaning'] = 'darker colors = higher vulnerability'
   
   elif visualization_type == 'vulnerability_plot':
       context['visual_elements']['plot_type'] = 'Box and whisker plot'
       context['visual_elements']['color_meaning'] = 'color indicates vulnerability category'
       
       if 'current_page' in visualization_data and 'total_pages' in visualization_data:
           context['data_summary']['current_page'] = visualization_data['current_page']
           context['data_summary']['total_pages'] = visualization_data['total_pages']
   
   elif visualization_type == 'urban_extent_map':
       context['visual_elements']['color_scale'] = 'YlOrRd for urban areas, gray for non-urban'
       
       if 'threshold' in visualization_data:
           context['analysis_context']['urban_threshold'] = visualization_data['threshold']
       
       if 'meets_threshold' in visualization_data and 'below_threshold' in visualization_data:
           context['data_summary']['urban_wards'] = visualization_data['meets_threshold']
           context['data_summary']['non_urban_wards'] = visualization_data['below_threshold']
   
   # Add metadata if available
   if metadata:
       # Add variable-specific information from metadata
       if visualization_type in ['variable_map', 'normalized_map'] and 'variable' in visualization_data:
           variable = visualization_data['variable']
           variable_metadata = metadata.get_entity_metadata('variable', variable)
           
           if variable_metadata:
               context['analysis_context']['variable_metadata'] = variable_metadata
       
       # Add overall analysis metadata
       context['analysis_context']['analysis_steps'] = metadata.steps
   
   return context

def get_explanation_for_ward(ward_name, question_type=None, metadata=None, data_handler=None):
   """
   Generate explanation context for a specific ward
   
   Args:
       ward_name: Name of the ward to explain
       question_type: Type of question (e.g., 'ranking', 'not_ideal')
       metadata: Optional AnalysisMetadata instance for context
       data_handler: Optional DataHandler instance for data access
       
   Returns:
       Dict with context for LLM explanation
   """
   context = {
       'ward_name': ward_name,
       'question_type': question_type,
       'ward_data': {},
       'analysis_context': {},
       'comparative_data': {}
   }
   
   # Add data from data_handler if available
   if data_handler:
       # Get vulnerability ranking
       if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
           ward_ranking = data_handler.vulnerability_rankings[
               data_handler.vulnerability_rankings['WardName'] == ward_name
           ]
           
           if not ward_ranking.empty:
               context['ward_data']['overall_rank'] = int(ward_ranking['overall_rank'].values[0])
               context['ward_data']['median_score'] = float(ward_ranking['median_score'].values[0])
               context['ward_data']['vulnerability_category'] = str(ward_ranking['vulnerability_category'].values[0])
               
               # Add percentile information
               total_wards = len(data_handler.vulnerability_rankings)
               rank = context['ward_data']['overall_rank']
               percentile = (total_wards - rank) / total_wards * 100
               context['ward_data']['percentile'] = float(percentile)
       
       # Get urban data
       if data_handler.csv_data is not None:
           # Try to find urban percentage column
           urban_col = None
           for col in ['UrbanPercentage', 'UrbanPercent', 'Urban_Percent', 'urbanPercent']:
               if col in data_handler.csv_data.columns:
                   urban_col = col
                   break
           
           if urban_col and ward_name in data_handler.csv_data['WardName'].values:
               urban_value = data_handler.csv_data.loc[
                   data_handler.csv_data['WardName'] == ward_name, 
                   urban_col
               ].values[0]
               
               context['ward_data']['urban_percentage'] = float(urban_value)
       
       # Get variable values
       if data_handler.composite_variables and data_handler.csv_data is not None:
           variable_values = {}
           
           for var in data_handler.composite_variables:
               if var in data_handler.csv_data.columns and ward_name in data_handler.csv_data['WardName'].values:
                   value = data_handler.csv_data.loc[
                       data_handler.csv_data['WardName'] == ward_name, 
                       var
                   ].values[0]
                   
                   variable_values[var] = float(value) if pd.api.types.is_numeric_dtype(type(value)) else str(value)
           
           context['ward_data']['variable_values'] = variable_values
           
           # Add normalized values if available
           if hasattr(data_handler, 'normalized_data') and data_handler.normalized_data is not None:
               normalized_values = {}
               
               for var in data_handler.composite_variables:
                   norm_col = f"normalization_{var.lower()}"
                   
                   if norm_col in data_handler.normalized_data.columns and ward_name in data_handler.normalized_data['WardName'].values:
                       value = data_handler.normalized_data.loc[
                           data_handler.normalized_data['WardName'] == ward_name, 
                           norm_col
                       ].values[0]
                       
                       normalized_values[var] = float(value)
               
               context['ward_data']['normalized_values'] = normalized_values
       
       # Add "not ideal" flag information
       if question_type == 'not_ideal' and context['ward_data'].get('urban_percentage') is not None:
           # Check if this ward is flagged as "not ideal"
           is_not_ideal = False
           reasons = []
           
           # Check urban percentage vs. threshold
           urban_percentage = context['ward_data']['urban_percentage']
           if urban_percentage < 30:  # Default threshold
               is_not_ideal = True
               reasons.append({
                   'type': 'below_urban_threshold',
                   'urban_percentage': urban_percentage,
                   'threshold': 30,
                   'explanation': 'Urban percentage below threshold'
               })
           
           # Check if non-urban but high vulnerability
           if is_not_ideal and context['ward_data'].get('overall_rank') is not None:
               rank = context['ward_data']['overall_rank']
               if rank <= 5:  # In top 5 vulnerable
                   reasons.append({
                       'type': 'non_urban_high_vulnerability',
                       'rank': rank,
                       'explanation': 'Non-urban ward with high vulnerability ranking'
                   })
           
           context['ward_data']['is_not_ideal'] = is_not_ideal
           context['ward_data']['not_ideal_reasons'] = reasons
   
   # Add metadata if available
   if metadata:
       # Add ward-specific metadata
       ward_anomalies = []
       
       for anomaly in metadata.anomalies:
           if anomaly['entity_name'] == ward_name:
               ward_anomalies.append(anomaly)
       
       if ward_anomalies:
           context['analysis_context']['ward_anomalies'] = ward_anomalies
   
   return context

def get_explanation_for_analysis_result(analysis_result, question=None, metadata=None):
   """
   Generate explanation context for analysis results
   
   Args:
       analysis_result: Results from the analysis pipeline
       question: Optional specific question about the analysis
       metadata: Optional AnalysisMetadata instance for context
       
   Returns:
       Dict with context for LLM explanation
   """
   context = {
       'question': question,
       'analysis_summary': {},
       'variable_information': {},
       'vulnerability_information': {},
       'urban_extent_information': {}
   }
   
   # Add analysis summary
   if 'variables_used' in analysis_result:
       context['analysis_summary']['variables_used'] = analysis_result['variables_used']
   
   if 'vulnerable_wards' in analysis_result:
       context['analysis_summary']['top_vulnerable_wards'] = analysis_result['vulnerable_wards']
   
   if 'steps' in analysis_result:
       # Add summary of each step
       for step_name, step_result in analysis_result['steps'].items():
           if step_result and 'status' in step_result:
               context['analysis_summary'][f'{step_name}_status'] = step_result['status']
   
   # Add metadata if available
   if metadata:
       # Add overall analysis metadata
       context['analysis_summary']['analysis_steps'] = metadata.steps
       context['analysis_summary']['analysis_decisions'] = metadata.decisions
       
       # Add variable-specific information if question is about variables
       if question and any(var_word in question.lower() for var_word in ['variable', 'variables', 'factor', 'factors']):
           variable_decisions = [
               decision for decision in metadata.decisions 
               if decision['decision_type'] == 'relationship_determination'
           ]
           
           if variable_decisions:
               context['variable_information']['relationship_decisions'] = variable_decisions
   
   return context

def generate_analysis_report(data_handler, metadata=None, format="markdown"):
    """
    Generate a comprehensive analysis report
    
    Args:
        data_handler: DataHandler instance with analysis results
        metadata: Optional AnalysisMetadata instance for context
        format: Report format ("markdown", "html", "dict")
        
    Returns:
        Report content in the specified format
    """
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'has_cleaned_data': data_handler.cleaned_data is not None,
                'has_normalized_data': data_handler.normalized_data is not None,
                'has_composite_scores': hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None,
                'has_vulnerability_rankings': hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None,
                'has_urban_extent_results': hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results is not None
            }
            step_id = metadata.record_step(
                'generate_analysis_report',
                input_summary,
                None,  # Output summary will be updated later
                'report_generation',
                {'format': format}
            )
        
        # Prepare report sections
        sections = {
            'title': 'Malaria Risk Analysis Report',
            'summary': {},
            'data_cleaning': {},
            'normalization': {},
            'composite_scores': {},
            'vulnerability_analysis': {},
            'urban_extent_analysis': {}
        }
        
        # 1. Summary section
        sections['summary']['title'] = 'Executive Summary'
        sections['summary']['content'] = []
        
        # Add summary of the analysis
        if hasattr(data_handler, 'composite_variables') and data_handler.composite_variables:
            variables_used = data_handler.composite_variables
            sections['summary']['content'].append({
                'type': 'paragraph',
                'text': f"Analysis was performed using {len(variables_used)} variables: {', '.join(variables_used)}."
            })
        
        # Add top vulnerable wards
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            top_wards = data_handler.vulnerability_rankings.sort_values('overall_rank').head(5)['WardName'].tolist()
            sections['summary']['content'].append({
                'type': 'paragraph',
                'text': f"The top 5 most vulnerable wards identified are: {', '.join(top_wards)}."
            })
            
            # Add vulnerability category counts
            if 'vulnerability_category' in data_handler.vulnerability_rankings.columns:
                category_counts = data_handler.vulnerability_rankings['vulnerability_category'].value_counts().to_dict()
                sections['summary']['content'].append({
                    'type': 'paragraph',
                    'text': f"Vulnerability categories: " + 
                          ', '.join([f"{cat}: {count} wards" for cat, count in category_counts.items()])
                })
        
        # Add urban extent summary
        if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
            threshold_30 = data_handler.urban_extent_results.get(30)
            if threshold_30:
                sections['summary']['content'].append({
                    'type': 'paragraph',
                    'text': f"At the 30% urban threshold, {threshold_30['meets_threshold']} wards ({threshold_30['meets_threshold']/len(data_handler.csv_data)*100:.1f}%) are classified as urban."
                })
        
        # 2. Data Cleaning section
        sections['data_cleaning']['title'] = 'Data Cleaning and Preparation'
        sections['data_cleaning']['content'] = []
        
        if data_handler.cleaned_data is not None:
            sections['data_cleaning']['content'].append({
                'type': 'paragraph',
                'text': f"The dataset contains {len(data_handler.cleaned_data)} wards with {len(data_handler.cleaned_data.columns)} variables."
            })
            
            # Add information about missing value handling
            if hasattr(data_handler, 'na_handling_methods') and data_handler.na_handling_methods:
                methods_used = {}
                for calc in data_handler.na_handling_methods:
                    if 'operation' in calc and calc['operation'].startswith('imputation_'):
                        method = calc['operation'].replace('imputation_', '')
                        var = calc['variable']
                        if var not in methods_used:
                            methods_used[var] = method
                
                if methods_used:
                    methods_summary = ', '.join([f"{var}: {method}" for var, method in methods_used.items()])
                    sections['data_cleaning']['content'].append({
                        'type': 'paragraph',
                        'text': f"Missing values were handled using these methods: {methods_summary}."
                    })
        
        # 3. Normalization section
        sections['normalization']['title'] = 'Variable Normalization'
        sections['normalization']['content'] = []
        
        if data_handler.normalized_data is not None and data_handler.variable_relationships:
            # Add information about variable relationships
            direct_vars = [var for var, rel in data_handler.variable_relationships.items() if rel == 'direct']
            inverse_vars = [var for var, rel in data_handler.variable_relationships.items() if rel == 'inverse']
            
            if direct_vars:
                sections['normalization']['content'].append({
                    'type': 'paragraph',
                    'text': f"Variables with direct relationship to malaria risk (higher values = higher risk): {', '.join(direct_vars)}."
                })
            
            if inverse_vars:
                sections['normalization']['content'].append({
                    'type': 'paragraph',
                    'text': f"Variables with inverse relationship to malaria risk (higher values = lower risk): {', '.join(inverse_vars)}."
                })
        
        # 4. Composite Scores section
        sections['composite_scores']['title'] = 'Composite Risk Score Analysis'
        sections['composite_scores']['content'] = []
        
        if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores:
            # Get number of models
            model_count = len(data_handler.composite_scores['model_formulas'])
            
            sections['composite_scores']['content'].append({
                'type': 'paragraph',
                'text': f"A total of {model_count} composite score models were generated using different combinations of variables."
            })
            
            # Add example models
            example_models = data_handler.composite_scores['model_formulas'][:3]
            for i, model in enumerate(example_models):
                sections['composite_scores']['content'].append({
                    'type': 'paragraph',
                    'text': f"Model {i+1} example: Using variables {', '.join(model['variables'])}."
                })
        
        # 5. Vulnerability Analysis section
        sections['vulnerability_analysis']['title'] = 'Vulnerability Ranking Analysis'
        sections['vulnerability_analysis']['content'] = []
        
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            # Add information about ranking methodology
            sections['vulnerability_analysis']['content'].append({
                'type': 'paragraph',
                'text': "Vulnerability rankings were generated by calculating the median composite score across all models for each ward."
            })
            
            # Add top and bottom 5 wards
            top_wards = data_handler.vulnerability_rankings.sort_values('overall_rank').head(5)
            bottom_wards = data_handler.vulnerability_rankings.sort_values('overall_rank', ascending=False).head(5)
            
            top_wards_table = {
                'type': 'table',
                'headers': ['Ward Name', 'Rank', 'Median Score', 'Category'],
                'rows': []
            }
            
            for _, row in top_wards.iterrows():
                top_wards_table['rows'].append([
                    row['WardName'],
                    str(int(row['overall_rank'])),
                    f"{row['median_score']:.3f}",
                    str(row['vulnerability_category'])
                ])
            
            sections['vulnerability_analysis']['content'].append({
                'type': 'subheading',
                'text': 'Most Vulnerable Wards'
            })
            
            sections['vulnerability_analysis']['content'].append(top_wards_table)
            
            bottom_wards_table = {
                'type': 'table',
                'headers': ['Ward Name', 'Rank', 'Median Score', 'Category'],
                'rows': []
            }
            
            for _, row in bottom_wards.iterrows():
                bottom_wards_table['rows'].append([
                    row['WardName'],
                    str(int(row['overall_rank'])),
                    f"{row['median_score']:.3f}",
                    str(row['vulnerability_category'])
                ])
            
            sections['vulnerability_analysis']['content'].append({
                'type': 'subheading',
                'text': 'Least Vulnerable Wards'
            })
            
            sections['vulnerability_analysis']['content'].append(bottom_wards_table)
        
        # 6. Urban Extent Analysis section
        sections['urban_extent_analysis']['title'] = 'Urban Extent Analysis'
        sections['urban_extent_analysis']['content'] = []
        
        if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
            # Add information about thresholds
            sections['urban_extent_analysis']['content'].append({
                'type': 'paragraph',
                'text': f"Urban extent analysis was performed at {len(data_handler.urban_extent_results)} different thresholds."
            })
            
            # Create table of threshold results
            threshold_table = {
                'type': 'table',
                'headers': ['Threshold', 'Urban Wards', 'Non-Urban Wards', 'Urban Percentage'],
                'rows': []
            }
            
            for threshold, results in sorted(data_handler.urban_extent_results.items()):
                total_wards = results['meets_threshold'] + results['below_threshold']
                urban_percentage = results['meets_threshold'] / total_wards * 100 if total_wards > 0 else 0
                
                threshold_table['rows'].append([
                    f"{threshold}%",
                    str(results['meets_threshold']),
                    str(results['below_threshold']),
                    f"{urban_percentage:.1f}%"
                ])
            
            sections['urban_extent_analysis']['content'].append(threshold_table)
            
            # Add information about non-urban high vulnerability wards
            if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
                top_vulnerable = data_handler.vulnerability_rankings.sort_values('overall_rank').head(10)
                urban_col = None
                
                # Find urban percentage column
                for col in ['UrbanPercentage', 'UrbanPercent', 'Urban_Percent', 'urbanPercent']:
                    if col in data_handler.csv_data.columns:
                        urban_col = col
                        break
                
                if urban_col:
                    # Merge to get urban percentages
                    top_vulnerable_with_urban = top_vulnerable.merge(
                        data_handler.csv_data[['WardName', urban_col]],
                        on='WardName',
                        how='left'
                    )
                    
                    # Find non-urban high vulnerability wards
                    non_urban_high_vulnerable = top_vulnerable_with_urban[
                        top_vulnerable_with_urban[urban_col] < 30  # Default threshold
                    ]
                    
                    if not non_urban_high_vulnerable.empty:
                        non_urban_wards = non_urban_high_vulnerable['WardName'].tolist()
                        
                        sections['urban_extent_analysis']['content'].append({
                            'type': 'subheading',
                            'text': 'Non-Urban High Vulnerability Wards'
                        })
                        
                        sections['urban_extent_analysis']['content'].append({
                            'type': 'paragraph',
                            'text': f"The following high vulnerability wards are classified as non-urban (below 30% threshold): {', '.join(non_urban_wards)}."
                        })
                        
                        sections['urban_extent_analysis']['content'].append({
                            'type': 'paragraph',
                            'text': "These wards may present logistical challenges for urban-focused interventions and should be considered carefully in planning."
                        })
        
        # If metadata is available, add additional context
        if metadata:
            # Add timing information
            execution_times = {}
            for step in metadata.steps:
                if 'step_name' in step and 'execution_time' in step:
                    execution_times[step['step_name']] = step['execution_time']
            
            if execution_times:
                sections['summary']['content'].append({
                    'type': 'paragraph',
                    'text': f"Analysis completed in {sum(execution_times.values()):.2f} seconds."
                })
        
        # Update metadata with output summary
        if metadata and step_id:
            output_summary = {
                'format': format,
                'sections': list(sections.keys()),
                'section_counts': {section: len(content['content']) for section, content in sections.items()},
                'total_content_items': sum(len(content['content']) for content in sections.values())
            }
            
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        # Format the report based on requested format
        if format == "dict":
            return sections
        elif format == "html":
            html_report = generate_html_report(sections)
            return html_report
        else:  # Default to markdown
            markdown_report = generate_markdown_report(sections)
            return markdown_report
        
    except Exception as e:
        logger.error(f"Error generating analysis report: {str(e)}")
        traceback.print_exc()
        if format == "dict":
            return {'error': str(e)}
        elif format == "html":
            return f"<h1>Error Generating Report</h1><p>{str(e)}</p>"
        else:
            return f"# Error Generating Report\n\n{str(e)}"

def generate_markdown_report(sections):
    """
    Generate markdown report from section data
    
    Args:
        sections: Dict with report sections
        
    Returns:
        str: Markdown formatted report
    """
    report = []
    
    # Add title
    report.append(f"# {sections['title']}\n")
    
    # Add each section
    for section_key, section in sections.items():
        if section_key == 'title':
            continue
        
        if 'title' in section:
            report.append(f"## {section['title']}\n")
        
        if 'content' in section:
            for item in section['content']:
                if item['type'] == 'paragraph':
                    report.append(f"{item['text']}\n")
                elif item['type'] == 'subheading':
                    report.append(f"### {item['text']}\n")
                elif item['type'] == 'table':
                    # Create markdown table
                    headers = item['headers']
                    rows = item['rows']
                    
                    # Add header row
                    report.append('| ' + ' | '.join(headers) + ' |')
                    
                    # Add separator row
                    report.append('| ' + ' | '.join(['---' for _ in headers]) + ' |')
                    
                    # Add data rows
                    for row in rows:
                        report.append('| ' + ' | '.join(row) + ' |')
                    
                    report.append('')  # Add blank line after table
        
        report.append('')  # Add blank line after section
    
    return '\n'.join(report)

def generate_html_report(sections):
    """
    Generate HTML report from section data
    
    Args:
        sections: Dict with report sections
        
    Returns:
        str: HTML formatted report
    """
    html = []
    
    # Start HTML document
    html.append('<!DOCTYPE html>')
    html.append('<html lang="en">')
    html.append('<head>')
    html.append('    <meta charset="UTF-8">')
    html.append('    <meta name="viewport" content="width=device-width, initial-scale=1.0">')
    html.append(f'    <title>{sections["title"]}</title>')
    html.append('    <style>')
    html.append('        body { font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; }')
    html.append('        h1 { color: #003366; }')
    html.append('        h2 { color: #005599; border-bottom: 1px solid #ddd; padding-bottom: 10px; }')
    html.append('        h3 { color: #0077cc; }')
    html.append('        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }')
    html.append('        th, td { text-align: left; padding: 8px; border: 1px solid #ddd; }')
    html.append('        th { background-color: #f2f2f2; }')
    html.append('        tr:nth-child(even) { background-color: #f9f9f9; }')
    html.append('    </style>')
    html.append('</head>')
    html.append('<body>')
    
    # Add title
    html.append(f'<h1>{sections["title"]}</h1>')
    
    # Add each section
    for section_key, section in sections.items():
        if section_key == 'title':
            continue
        
        if 'title' in section:
            html.append(f'<h2>{section["title"]}</h2>')
        
        if 'content' in section:
            for item in section['content']:
                if item['type'] == 'paragraph':
                    html.append(f'<p>{item["text"]}</p>')
                elif item['type'] == 'subheading':
                    html.append(f'<h3>{item["text"]}</h3>')
                elif item['type'] == 'table':
                    # Create HTML table
                    html.append('<table>')
                    
                    # Add header row
                    html.append('<thead>')
                    html.append('<tr>')
                    for header in item['headers']:
                        html.append(f'    <th>{header}</th>')
                    html.append('</tr>')
                    html.append('</thead>')
                    
                    # Add data rows
                    html.append('<tbody>')
                    for row in item['rows']:
                        html.append('<tr>')
                        for cell in row:
                            html.append(f'    <td>{cell}</td>')
                        html.append('</tr>')
                    html.append('</tbody>')
                    
                    html.append('</table>')
    
    # End HTML document
    html.append('</body>')
    html.append('</html>')
    
    return '\n'.join(html)

def check_data_quality(data, metadata=None):
    """
    Check data quality and identify potential issues
    
    Args:
        data: DataFrame to check
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        Dict with data quality issues
    """
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'row_count': len(data),
                'column_count': len(data.columns)
            }
            step_id = metadata.record_step(
                'check_data_quality',
                input_summary,
                None,  # Output summary will be updated later
                'data_quality_assessment',
                {'check_types': ['missing_values', 'outliers', 'consistency']}
            )
        
        issues = {
            'missing_values': {},
            'outliers': {},
            'inconsistent_values': {},
            'constant_columns': [],
            'severe_issues': []
        }
        
        # Check for missing values
        for col in data.columns:
            missing_count = data[col].isna().sum()
            missing_percentage = missing_count / len(data) * 100
            
            if missing_count > 0:
                issues['missing_values'][col] = {
                    'count': int(missing_count),
                    'percentage': float(missing_percentage)
                }
                
                if missing_percentage > 50:
                    issues['severe_issues'].append({
                        'type': 'high_missing_values',
                        'column': col,
                        'missing_percentage': float(missing_percentage),
                        'recommendation': 'Consider dropping this column due to high percentage of missing values'
                    })
                
                if metadata:
                    metadata.record_calculation(
                        step_id,
                        col,
                        'missing_value_count',
                        {'row_count': len(data)},
                        {'missing_count': int(missing_count), 'missing_percentage': float(missing_percentage)}
                    )
        
        # Check for outliers in numeric columns
        for col in data.columns:
            if pd.api.types.is_numeric_dtype(data[col]) and col != 'WardName':
                # Use IQR method to detect outliers
                q1 = data[col].quantile(0.25)
                q3 = data[col].quantile(0.75)
                iqr = q3 - q1
                
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outliers = data[(data[col] < lower_bound) | (data[col] > upper_bound)][col]
                outlier_count = len(outliers)
                outlier_percentage = outlier_count / len(data[col].dropna()) * 100
                
                if outlier_count > 0:
                    issues['outliers'][col] = {
                        'count': int(outlier_count),
                        'percentage': float(outlier_percentage),
                        'lower_bound': float(lower_bound),
                        'upper_bound': float(upper_bound),
                        'min_outlier': float(outliers.min()) if not outliers.empty else None,
                        'max_outlier': float(outliers.max()) if not outliers.empty else None
                    }
                    
                    if outlier_percentage > 10:
                        issues['severe_issues'].append({
                            'type': 'high_outlier_percentage',
                            'column': col,
                            'outlier_percentage': float(outlier_percentage),
                            'recommendation': 'Investigate potential measurement errors or data entry issues'
                        })
                    
                    if metadata:
                        metadata.record_calculation(
                            step_id,
                            col,
                            'outlier_detection',
                            {'method': 'IQR', 'q1': float(q1), 'q3': float(q3), 'iqr': float(iqr)},
                            {'outlier_count': int(outlier_count), 'outlier_percentage': float(outlier_percentage)}
                        )
        
        # Check for inconsistent values in categorical columns
        for col in data.columns:
            if not pd.api.types.is_numeric_dtype(data[col]) and col != 'WardName':
                # Check for mixed case values that might be the same
                value_counts = data[col].value_counts()
                lowercase_counts = data[col].str.lower().value_counts() if hasattr(data[col], 'str') else None
                
                if lowercase_counts is not None and len(lowercase_counts) < len(value_counts):
                    issues['inconsistent_values'][col] = {
                        'type': 'case_inconsistency',
                        'unique_values': int(len(value_counts)),
                        'lowercase_unique': int(len(lowercase_counts)),
                        'examples': {
                            val: float(count) for val, count in value_counts.items() 
                            if any(val.lower() == other_val.lower() and val != other_val 
                                 for other_val in value_counts.index)
                        }
                    }
                    
                    if metadata:
                        metadata.record_calculation(
                            step_id,
                            col,
                            'case_inconsistency_check',
                            {'unique_values': int(len(value_counts))},
                            {'lowercase_unique': int(len(lowercase_counts))}
                        )
        
        # Check for constant columns
        for col in data.columns:
            if col != 'WardName':
                unique_values = data[col].dropna().nunique()
                
                if unique_values <= 1:
                    issues['constant_columns'].append(col)
                    
                    issues['severe_issues'].append({
                        'type': 'constant_column',
                        'column': col,
                        'recommendation': 'Consider dropping this column as it contains only one unique value'
                    })
                    
                    if metadata:
                        metadata.record_calculation(
                            step_id,
                            col,
                            'constant_column_check',
                            {'row_count': len(data)},
                            {'unique_values': int(unique_values)}
                        )
        
        # Update metadata with output summary
        if metadata and step_id:
            output_summary = {
                'missing_value_columns': len(issues['missing_values']),
                'outlier_columns': len(issues['outliers']),
                'inconsistent_columns': len(issues['inconsistent_values']),
                'constant_columns': len(issues['constant_columns']),
                'severe_issues': len(issues['severe_issues'])
            }
            
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        return issues
    
    except Exception as e:
        logger.error(f"Error checking data quality: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'severe_issues': [{
                'type': 'data_quality_check_error',
                'message': str(e),
                'recommendation': 'Review data format and structure'
            }]
        }