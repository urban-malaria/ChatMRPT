# app/analysis/normalization.py
import logging
import numpy as np
import pandas as pd
import traceback
from typing import Dict, List, Optional, Any, Union
from joblib import Parallel, delayed

# Set up logging
logger = logging.getLogger(__name__)


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
            logger.warning("All values identical in normalization, using default 0.5")
            
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
        logger.error("Error in normalize_variable: {}".format(str(e)))
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
                norm_col_name = "normalization_{}".format(col.lower())
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
        logger.error("Error in normalize_data: {}".format(str(e)))
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


def get_normalization_summary(data, relationships, normalized_data):
    """
    Generate a summary of the normalization process
    
    Args:
        data: Original DataFrame before normalization
        relationships: Dict of variable relationships used
        normalized_data: DataFrame after normalization
        
    Returns:
        Dict: Summary of normalization results
    """
    try:
        summary = {
            'original_columns': len(data.columns),
            'normalized_columns': len([col for col in normalized_data.columns if col.startswith('normalization_')]),
            'relationships_applied': len(relationships),
            'normalization_details': {}
        }
        
        # Analyze each normalized column
        for col in normalized_data.columns:
            if col.startswith('normalization_'):
                original_col = col.replace('normalization_', '').replace('_', ' ').title()
                
                # Find the original column name
                for orig_col in data.columns:
                    if orig_col.lower().replace(' ', '_') in col.lower():
                        original_col = orig_col
                        break
                
                if original_col in data.columns and original_col in relationships:
                    norm_values = normalized_data[col].values
                    orig_values = data[original_col].values
                    
                    summary['normalization_details'][original_col] = {
                        'relationship': relationships[original_col],
                        'original_range': [float(np.min(orig_values)), float(np.max(orig_values))],
                        'normalized_range': [float(np.min(norm_values)), float(np.max(norm_values))],
                        'original_mean': float(np.mean(orig_values)),
                        'normalized_mean': float(np.mean(norm_values))
                    }
        
        return summary
        
    except Exception as e:
        logger.error("Error generating normalization summary: {}".format(str(e)))
        return {
            'error': str(e),
            'original_columns': len(data.columns) if data is not None else 0,
            'normalized_columns': 0,
            'relationships_applied': len(relationships) if relationships else 0,
            'normalization_details': {}
        } 