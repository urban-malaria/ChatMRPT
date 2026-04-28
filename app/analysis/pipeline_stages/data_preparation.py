# app/analysis/pipeline_stages/data_preparation.py

import logging
import time
import pandas as pd
from ..utils import is_numeric_column
from ..normalization import normalize_data, determine_variable_relationships
from ..imputation import handle_missing_values

logger = logging.getLogger(__name__)




def _fix_duplicate_ward_names(data: pd.DataFrame) -> pd.DataFrame:
    """
    Fix duplicate ward names by appending WardCode in format: WardName (WardCode)
    
    Args:
        data: DataFrame with potential duplicate ward names
        
    Returns:
        DataFrame with fixed ward names
    """
    try:
        # Find ward name column
        ward_col = None
        for col in ['WardName', 'ward_name', 'Ward_Name', 'Ward']:
            if col in data.columns:
                ward_col = col
                break
        
        if not ward_col:
            logger.warning("No ward column found - skipping duplicate fixing")
            return data
        
        # Find ward code column
        code_col = None
        for col in ['WardCode', 'ward_code', 'Ward_Code']:
            if col in data.columns:
                code_col = col
                break
        
        if not code_col:
            logger.warning("No ward code column found - skipping duplicate fixing")
            return data
        
        # Make a copy to avoid modifying original
        fixed_data = data.copy()
        
        # Find duplicates
        duplicates = fixed_data[ward_col].duplicated(keep=False)
        n_duplicates = duplicates.sum()
        
        if n_duplicates == 0:
            logger.info("✅ No duplicate ward names found")
            return fixed_data
        
        logger.info(f"🔧 Found {n_duplicates} duplicate ward names - fixing with WardCode format")
        
        # Get unique ward names that have duplicates
        duplicate_ward_names = fixed_data[duplicates][ward_col].unique()
        
        # Fix each duplicate group
        for ward_name in duplicate_ward_names:
            # Get all rows with this ward name
            ward_mask = fixed_data[ward_col] == ward_name
            ward_rows = fixed_data[ward_mask]
            
            # Apply format: WardName (WardCode) for all instances
            for idx in ward_rows.index:
                ward_code = fixed_data.loc[idx, code_col]
                fixed_name = f"{ward_name} ({ward_code})"
                fixed_data.loc[idx, ward_col] = fixed_name
        
        # Verify fix
        remaining_duplicates = fixed_data[ward_col].duplicated().sum()
        if remaining_duplicates == 0:
            logger.info(f"✅ Successfully fixed all duplicate ward names using WardCode format")
            logger.info(f"📝 Fixed {len(duplicate_ward_names)} ward groups: {', '.join(duplicate_ward_names)}")
        else:
            logger.warning(f"⚠️ Still have {remaining_duplicates} duplicates after fixing")
        
        return fixed_data
        
    except Exception as e:
        logger.error(f"Error fixing duplicate ward names: {str(e)}")
        return data


def run_data_cleaning_stage(data_handler, metadata, pipeline_step_id, rerun_stages, na_methods=None):
    """Run the deferred data cleaning stage (ward mismatches + spatial imputation)"""
    logger.info("Step 1: Deferred cleaning (raw data → ward fixes + spatial imputation)")
    
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
            
            # Step 1: Handle ward name mismatches if both CSV and shapefile exist
            if data_handler.shapefile_data is not None:
                logger.info("Checking for ward name mismatches...")
                ward_mismatches = data_handler.check_wardname_mismatches()
                
                if ward_mismatches and len(ward_mismatches) > 0:
                    logger.info(f"Found {len(ward_mismatches)} ward name mismatches, fixing...")
                    # Apply ward name fixes to raw data
                    data_handler.csv_data = data_handler.fix_wardname_mismatches(data_handler.csv_data)
                    logger.info("Ward name mismatches fixed in raw data")
                else:
                    logger.info("No ward name mismatches detected")
            
            # Step 1.5: Handle duplicate ward names using WardCode format
            logger.info("Checking for duplicate ward names...")
            data_handler.csv_data = _fix_duplicate_ward_names(data_handler.csv_data)
            
            # Step 2: Handle missing values with spatial neighbor imputation
            logger.info("Applying spatial neighbor imputation for missing values...")
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
            
            # Determine what cleaning was performed
            cleaning_performed = []
            if data_handler.shapefile_data is not None:
                ward_mismatches = data_handler.check_wardname_mismatches()
                if ward_mismatches and len(ward_mismatches) > 0:
                    cleaning_performed.append(f"Fixed {len(ward_mismatches)} ward name mismatches")
                else:
                    cleaning_performed.append("No ward name mismatches")
            
            # Check duplicate handling results
            duplicate_count = cleaned_data['WardName'].duplicated().sum() if 'WardName' in cleaned_data.columns else 0
            if duplicate_count == 0:
                cleaning_performed.append("Fixed duplicate ward names with WardCode format")
            else:
                cleaning_performed.append(f"Warning: {duplicate_count} duplicate ward names remain")
            
            cleaning_performed.append("Applied spatial neighbor imputation for missing values")
            
            return {
                'status': 'success',
                'message': f'Deferred cleaning completed: {"; ".join(cleaning_performed)} - {len(cleaned_data)} rows processed',
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


def run_relationship_stage(data_handler, metadata, pipeline_step_id, rerun_stages, custom_relationships=None, selected_variables=None):
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
            if selected_variables is not None:
                # For custom analysis: use intelligent variable matching
                logger.info(f"🔧 CUSTOM ANALYSIS: Looking for variables {selected_variables} in cleaned data")
                
                available_columns = list(data_handler.cleaned_data.columns)
                logger.info(f"🔧 Available columns: {available_columns}")
                
                variables = []
                for var in selected_variables:
                    if var in available_columns and is_numeric_column(data_handler.cleaned_data, var):
                        variables.append(var)
                        logger.info(f"✅ Exact match: {var}")
                    else:
                        # Enhanced fuzzy matching with multiple strategies
                        matched_var = None
                        
                        # Strategy 1: Check if user variable is contained in any column name
                        for col in available_columns:
                            if var.lower() in col.lower() or col.lower() in var.lower():
                                if is_numeric_column(data_handler.cleaned_data, col):
                                    matched_var = col
                                    logger.info(f"🔍 Substring match: '{var}' → '{matched_var}'")
                                    break
                        
                        # Strategy 2: Difflib fuzzy matching with lower cutoff
                        if not matched_var:
                            from difflib import get_close_matches
                            close_matches = get_close_matches(var.lower(), [col.lower() for col in available_columns], n=1, cutoff=0.4)
                            if close_matches:
                                # Find the original column name
                                matched_var = next((col for col in available_columns if col.lower() == close_matches[0]), None)
                                if matched_var and is_numeric_column(data_handler.cleaned_data, matched_var):
                                    logger.info(f"🔍 Fuzzy match: '{var}' → '{matched_var}'")
                                else:
                                    matched_var = None
                        
                        # Strategy 3: Common malaria variable patterns
                        if not matched_var:
                            var_patterns = {
                                'evi': ['mean_EVI', 'enhanced_vegetation_index', 'EVI'],
                                'ndvi': ['mean_NDVI', 'normalized_difference_vegetation_index', 'NDVI'],
                                'rainfall': ['mean_rainfall', 'precipitation', 'rain'],
                                'temp': ['temp_mean', 'temperature', 'temp'],
                                'elevation': ['elevation', 'altitude', 'elev'],
                                'flood': ['flood', 'flooding'],
                                'pfpr': ['pfpr', 'parasite_prevalence'],
                                'tpr': ['u5_tpr_rdt', 'test_positivity_rate', 'tpr']
                            }
                            
                            if var.lower() in var_patterns:
                                for pattern in var_patterns[var.lower()]:
                                    for col in available_columns:
                                        if pattern.lower() == col.lower():
                                            if is_numeric_column(data_handler.cleaned_data, col):
                                                matched_var = col
                                                logger.info(f"🎯 Pattern match: '{var}' → '{matched_var}'")
                                                break
                                    if matched_var:
                                        break
                        
                        if matched_var:
                            variables.append(matched_var)
                        else:
                            logger.warning(f"❌ No match found for variable: {var}")
                
                logger.info(f"Using {len(variables)} selected variables for relationships: {variables}")
            else:
                # For standard analysis: use all available variables
                variables = [col for col in data_handler.cleaned_data.columns 
                           if col != 'WardName' and is_numeric_column(data_handler.cleaned_data, col)]
                logger.info(f"Using {len(variables)} available variables for relationships")
            
            # Check if we have any valid variables
            if not variables:
                if selected_variables is not None:
                    return {
                        'status': 'error',
                        'message': f'None of the selected variables {selected_variables} were found as numeric columns in the data. Available columns: {list(data_handler.cleaned_data.columns)}'
                    }
                else:
                    return {
                        'status': 'error', 
                        'message': 'No numeric variables found in the data for relationship determination'
                    }
            
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


def run_normalization_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables=None):
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
            
            # Determine which columns to exclude from normalization
            # CRITICAL FIX: Always normalize ALL variables, selection happens at scoring stage
            # The previous approach excluded custom variables from normalization which broke custom analysis
            exclude_cols = None  # Don't exclude any variables from normalization
            
            if selected_variables is not None:
                logger.info(f"Custom analysis: normalizing ALL variables, will select {selected_variables} for scoring later")
            else:
                logger.info("Standard analysis: normalizing all available variables")
            
            # Normalize ALL available variables - this ensures custom variables are available for scoring
            normalized_data = normalize_data(
                data_handler.cleaned_data,
                data_handler.variable_relationships,
                exclude_cols,  # None - normalize everything
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