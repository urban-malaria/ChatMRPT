# app/analysis/pipeline_stages/data_preparation.py

import logging
import time
from ..utils import is_numeric_column
from ..normalization import normalize_data, determine_variable_relationships
from ..imputation import handle_missing_values

logger = logging.getLogger(__name__)


def run_data_cleaning_stage(data_handler, metadata, pipeline_step_id, rerun_stages, na_methods=None):
    """Run the data cleaning stage of the pipeline"""
    logger.info("Step 1: Cleaning data")
    
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
            # Ensure we have data to clean
            if data_handler.csv_data is None:
                return {
                    'status': 'error',
                    'message': 'No CSV data available for cleaning'
                }
            
            # Clean the data using the original pipeline logic
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
            
            return {
                'status': 'success',
                'message': f'Successfully cleaned {len(cleaned_data)} rows',
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
        if hasattr(data_handler, 'cleaned_data') and data_handler.cleaned_data is not None:
            return {
                'status': 'success', 
                'message': 'Using previously cleaned data'
            }
        else:
            return {
                'status': 'error',
                'message': 'No cleaned data available'
            }


def run_relationship_stage(data_handler, metadata, pipeline_step_id, rerun_stages, custom_relationships=None):
    """Run the variable relationships determination stage"""
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
                       if col != 'WardName' and is_numeric_column(data_handler.cleaned_data, col)]
            
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
                        'relationship_counts': {
                            'direct': sum(1 for r in relationships.values() if r == 'direct'),
                            'inverse': sum(1 for r in relationships.values() if r == 'inverse')
                        },
                        'custom_overrides': len(custom_relationships) if custom_relationships else 0
                    }
                    break
            
            return {
                'status': 'success',
                'message': f'Successfully determined relationships for {len(relationships)} variables',
                'relationships': relationships,
                'execution_time': execution_time
            }
            
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
    else:
        if data_handler.variable_relationships:
            return {
                'status': 'success', 
                'message': 'Using previously determined variable relationships',
                'relationships': data_handler.variable_relationships
            }
        else:
            return {
                'status': 'error',
                'message': 'No variable relationships available'
            }


def run_normalization_stage(data_handler, metadata, pipeline_step_id, rerun_stages):
    """Run the data normalization stage of the pipeline"""
    logger.info("Step 3: Normalizing data")
    
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
            
            # Normalize the data using the original pipeline logic
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
                        'column_list': norm_cols[:10]  # First 10 for brevity
                    }
                    break
            
            return {
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
        if hasattr(data_handler, 'normalized_data') and data_handler.normalized_data is not None:
            norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
            return {
                'status': 'success', 
                'message': 'Using previously normalized data',
                'normalized_columns': norm_cols
            }
        else:
            return {
                'status': 'error',
                'message': 'No normalized data available'
            } 