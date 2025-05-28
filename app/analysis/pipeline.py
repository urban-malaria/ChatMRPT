# app/analysis/pipeline.py
import logging
import time
import traceback
import pandas as pd
from typing import Dict, List, Optional, Any, Union

# Import the individual analysis modules
from .metadata import AnalysisMetadata
from .utils import is_numeric_column, check_data_quality
from .normalization import normalize_data, determine_variable_relationships
from .imputation import handle_missing_values
from .scoring import compute_composite_scores, analyze_vulnerability
from .urban_analysis import analyze_urban_extent

# Set up logging
logger = logging.getLogger(__name__)


def run_full_analysis_pipeline(data_handler, selected_variables=None, 
                             na_methods=None, custom_relationships=None, 
                             metadata=None, session_id=None, interaction_logger=None,
                             llm_manager=None):
    """
    Run the complete analysis pipeline - EXACT LEGACY IMPLEMENTATION
    
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
             'llm_selection': llm_manager is not None and selected_variables is None},
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
            if data_handler.cleaned_data is not None:
                clean_result = {
                    'status': 'success', 
                    'message': 'Using previously cleaned data'
                }
            else:
                return {
                    'status': 'error',
                    'message': 'No cleaned data available'
                }
        
        # 2. Determine variable relationships if needed
        logger.info("Step 2: Determining variable relationships")
        norm_result = None
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
                
                norm_result = {
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
                norm_result = {
                    'status': 'success', 
                    'message': 'Using previously determined variable relationships',
                    'relationships': data_handler.variable_relationships
                }
            else:
                return {
                    'status': 'error',
                    'message': 'No variable relationships available'
                }
        
        # 3. Normalize data if needed
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
                            'column_list': norm_cols[:10]  # First 10 for brevity
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
            if data_handler.normalized_data is not None:
                norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
                norm_result = {
                    'status': 'success', 
                    'message': 'Using previously normalized data',
                    'normalized_columns': norm_cols
                }
            else:
                return {
                    'status': 'error',
                    'message': 'No normalized data available'
                }
        
        # 4. Compute composite scores
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
                
                if selected_variables:
                    # User specified variables - validate and clean them
                    available_vars = [col.replace('normalization_', '') 
                                    for col in data_handler.normalized_data.columns 
                                    if col.startswith('normalization_')]
                    
                    # Clean and validate the selected variables
                    clean_selected_vars = []
                    for var in selected_variables:
                        # Remove 'normalization_' prefix if present
                        clean_var = var.replace('normalization_', '')
                        if clean_var in available_vars:
                            clean_selected_vars.append(clean_var)
                        else:
                            logger.warning(f"Variable '{var}' not found in available variables")
                    
                    if not clean_selected_vars:
                        return {
                            'status': 'error',
                            'message': 'None of the selected variables are available in normalized data'
                        }
                    
                    variable_selection_method = "user_specified"
                    variable_explanations = {var: f"User selected {var} for analysis" for var in clean_selected_vars}
                    
                    logger.info(f"Using user-specified variables: {clean_selected_vars}")
                    
                elif llm_manager is not None:
                    # Use LLM to select optimal variables
                    try:
                        from app.core.llm_manager import select_optimal_variables_with_llm
                        
                        # Get available variables from normalized data
                        available_vars = [col.replace('normalization_', '') 
                                        for col in data_handler.normalized_data.columns 
                                        if col.startswith('normalization_')]
                        
                        if len(available_vars) < 3:
                            logger.warning(f"Only {len(available_vars)} variables available, need at least 3 for LLM selection")
                            clean_selected_vars = available_vars  # Use all if we have fewer than 3
                            variable_selection_method = "insufficient_variables"
                        else:
                            clean_selected_vars, variable_explanations = select_optimal_variables_with_llm(
                                llm_manager=llm_manager,
                                available_vars=available_vars,
                                csv_data=data_handler.cleaned_data,
                                min_vars=3,
                                max_vars=5
                            )
                            variable_selection_method = "llm_selected"
                            
                            logger.info(f"LLM selected {len(clean_selected_vars)} variables: {clean_selected_vars}")
                        
                    except Exception as e:
                        logger.warning(f"LLM variable selection failed: {str(e)}, using all available variables")
                        # Fallback to all variables
                        clean_selected_vars = [col.replace('normalization_', '') 
                                             for col in data_handler.normalized_data.columns 
                                             if col.startswith('normalization_')]
                        variable_selection_method = "llm_fallback"
                        variable_explanations = {}
                else:
                    # Use all available variables
                    clean_selected_vars = [col.replace('normalization_', '') 
                                         for col in data_handler.normalized_data.columns 
                                         if col.startswith('normalization_')]
                    variable_selection_method = "all_variables"
                    variable_explanations = {}
                    
                    logger.info(f"Using all available variables: {clean_selected_vars}")
                
                if not clean_selected_vars:
                    return {
                        'status': 'error',
                        'message': 'No variables available for composite score calculation'
                    }
                
                # Store variable selection information
                data_handler.composite_variables = clean_selected_vars
                data_handler.variable_selection_method = variable_selection_method
                data_handler.variable_selection_explanations = variable_explanations
                
                # Compute composite scores
                composite_scores = compute_composite_scores(
                    data_handler.normalized_data,
                    clean_selected_vars,
                    'mean',  # Use mean method
                    -1,  # Use all available cores
                    metadata
                )
                
                # Store composite scores
                data_handler.composite_scores = composite_scores
                
                # Update step with results
                execution_time = time.time() - start_time
                
                for step in metadata.steps:
                    if step['step_id'] == step_id:
                        step['execution_time'] = execution_time
                        step['output_summary'] = {
                            'ward_count': len(composite_scores['scores']) if isinstance(composite_scores, dict) and 'scores' in composite_scores else len(composite_scores),
                            'variables_used': clean_selected_vars,
                            'selection_method': variable_selection_method,
                            'composite_models': len([col for col in composite_scores['scores'].columns if col.startswith('composite_')]) if isinstance(composite_scores, dict) and 'scores' in composite_scores else 0
                        }
                        break
                
                composite_result = {
                    'status': 'success',
                    'message': f'Successfully computed composite scores using {len(clean_selected_vars)} variables',
                    'variables_used': clean_selected_vars,
                    'selection_method': variable_selection_method,
                    'variable_explanations': variable_explanations,
                    'execution_time': execution_time
                }
            except Exception as e:
                logger.error(f"Error computing composite scores: {str(e)}")
                
                for step in metadata.steps:
                    if step['step_id'] == step_id:
                        step['error'] = str(e)
                        break
                
                return {
                    'status': 'error',
                    'message': f'Error computing composite scores: {str(e)}'
                }
        else:
            if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None:
                composite_result = {
                    'status': 'success', 
                    'message': 'Using previously calculated composite scores',
                    'variables_used': data_handler.composite_variables or [],
                    'selection_method': getattr(data_handler, 'variable_selection_method', 'default')
                }
            else:
                return {
                    'status': 'error',
                    'message': 'No composite scores available'
                }
        
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
                
                # Store vulnerability rankings - THIS IS THE CRITICAL LINE THAT WAS MISSING!
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
                return {
                    'status': 'error',
                    'message': 'No vulnerability rankings available'
                }
        
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
                return {
                    'status': 'error',
                    'message': 'No urban extent results available'
                }
        
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
            'metadata': metadata,  # Include metadata for explanation
            # Add actual data results for the coordinator to extract
            'cleaned_data': data_handler.cleaned_data,
            'normalized_data': data_handler.normalized_data,
            'composite_scores': data_handler.composite_scores,
            'vulnerability_rankings': data_handler.vulnerability_rankings,
            'variable_relationships': data_handler.variable_relationships,
            'composite_variables': getattr(data_handler, 'composite_variables', [])
        }
        
        return summary
        
    except Exception as e:
        logger.error("Error in full analysis pipeline: {}".format(str(e)))
        traceback.print_exc()
        return {
            'status': 'error',
            'message': "Error in full analysis pipeline: {}".format(str(e))
        }


# Legacy functions for generating explanations
def get_explanation_for_visualization(visualization_type, visualization_data, question=None, metadata=None):
    """
    Generate explanation context for visualizations
    
    Args:
        visualization_type: Type of visualization (e.g., 'composite_map', 'vulnerability_map')
        visualization_data: Data associated with the visualization
        question: Optional specific question being asked
        metadata: Optional analysis metadata for context
        
    Returns:
        Dict with context for LLM explanation
    """
    context = {
        'visualization_type': visualization_type,
        'visualization_data': visualization_data,
        'question': question,
        'analysis_metadata': metadata.get_explanation_context('visualization') if metadata else {}
    }
    
    # Add type-specific context
    if visualization_type == 'composite_map':
        context.update({
            'description': 'A composite vulnerability map showing aggregated risk scores across all wards',
            'interpretation': 'Darker colors indicate higher vulnerability/risk levels',
            'purpose': 'Identify areas with the highest overall malaria transmission risk'
        })
    elif visualization_type == 'vulnerability_map':
        context.update({
            'description': 'A categorical vulnerability map showing wards classified by risk level',
            'interpretation': 'Different colors represent High, Medium, and Low vulnerability categories',
            'purpose': 'Clearly distinguish between risk categories for decision-making'
        })
    elif visualization_type == 'variable_map':
        variable_name = visualization_data.get('variable', 'unknown')
        context.update({
            'description': f'A map showing the distribution of {variable_name} across wards',
            'interpretation': f'Values represent {variable_name} levels in each ward',
            'purpose': f'Understand the spatial pattern of {variable_name} as a risk factor'
        })
    elif visualization_type == 'decision_tree':
        context.update({
            'description': 'A decision tree showing the hierarchy of vulnerability factors',
            'interpretation': 'Branches show how different factors contribute to overall risk assessment',
            'purpose': 'Understand the logical flow of risk assessment decisions'
        })
    
    return context


def get_explanation_for_ward(ward_name, question_type=None, metadata=None, data_handler=None):
    """
    Generate explanation context for specific ward analysis
    
    Args:
        ward_name: Name of the ward being analyzed
        question_type: Type of question being asked about the ward
        metadata: Optional analysis metadata for context
        data_handler: Optional data handler for accessing ward data
        
    Returns:
        Dict with context for LLM explanation
    """
    context = {
        'ward_name': ward_name,
        'question_type': question_type,
        'analysis_metadata': metadata.get_explanation_context('ward', ward_name) if metadata else {}
    }
    
    # Add ward-specific data if data_handler is available
    if data_handler:
        try:
            # Get ward vulnerability ranking
            if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
                ward_data = data_handler.vulnerability_rankings[
                    data_handler.vulnerability_rankings['WardName'] == ward_name
                ]
                if not ward_data.empty:
                    ward_info = ward_data.iloc[0]
                    context.update({
                        'vulnerability_rank': ward_info.get('overall_rank', 'Unknown'),
                        'vulnerability_category': ward_info.get('vulnerability_category', 'Unknown'),
                        'composite_score': ward_info.get('composite_score', 'Unknown')
                    })
            
            # Get raw variable values
            if hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None:
                ward_raw = data_handler.csv_data[data_handler.csv_data['WardName'] == ward_name]
                if not ward_raw.empty:
                    ward_values = ward_raw.iloc[0].to_dict()
                    context['raw_values'] = {k: v for k, v in ward_values.items() 
                                           if k != 'WardName' and not pd.isna(v)}
            
            # Get normalized values
            if hasattr(data_handler, 'normalized_data') and data_handler.normalized_data is not None:
                ward_norm = data_handler.normalized_data[data_handler.normalized_data['WardName'] == ward_name]
                if not ward_norm.empty:
                    norm_values = ward_norm.iloc[0].to_dict()
                    context['normalized_values'] = {k.replace('normalization_', ''): v 
                                                  for k, v in norm_values.items() 
                                                  if k.startswith('normalization_') and not pd.isna(v)}
            
            # Add urban extent information if available
            if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
                urban_info = {}
                for threshold, results in data_handler.urban_extent_results.items():
                    if ward_name in results.get('meets_threshold', []):
                        urban_info[f'{threshold}%_threshold'] = 'Meets'
                    elif ward_name in results.get('below_threshold', []):
                        urban_info[f'{threshold}%_threshold'] = 'Below'
                context['urban_extent'] = urban_info
                
        except Exception as e:
            context['data_error'] = str(e)
    
    return context


def get_explanation_for_analysis_result(analysis_result, question=None, metadata=None):
    """
    Generate explanation context for overall analysis results
    
    Args:
        analysis_result: Dictionary containing analysis results
        question: Optional specific question being asked
        metadata: Optional analysis metadata for context
        
    Returns:
        Dict with context for LLM explanation
    """
    context = {
        'analysis_result': analysis_result,
        'question': question,
        'analysis_metadata': metadata.get_explanation_context('analysis_result') if metadata else {}
    }
    
    # Extract key information from analysis results
    if 'variables_used' in analysis_result:
        context['variables_analyzed'] = analysis_result['variables_used']
    
    if 'vulnerable_wards' in analysis_result:
        context['top_vulnerable_wards'] = analysis_result['vulnerable_wards']
    
    if 'selection_method' in analysis_result:
        context['variable_selection_method'] = analysis_result['selection_method']
    
    # Add summary statistics if available
    if metadata:
        # Get summary from metadata steps
        for step in metadata.steps:
            if step.get('step_name') == 'calculate_vulnerability_rankings':
                if 'output_summary' in step:
                    context['vulnerability_summary'] = step['output_summary']
                break
    
    return context


def generate_analysis_report(data_handler, metadata=None, format="markdown"):
    """
    Generate a comprehensive analysis report
    
    Args:
        data_handler: DataHandler instance with analysis results
        metadata: Optional analysis metadata for detailed reporting
        format: Output format ('markdown', 'html', or 'dict')
        
    Returns:
        Dict with report content and metadata
    """
    try:
        # Collect report sections
        sections = {}
        
        # 1. Executive Summary
        summary_data = {
            'total_wards': len(data_handler.csv_data) if data_handler.csv_data is not None else 0,
            'variables_analyzed': len(data_handler.composite_variables) if hasattr(data_handler, 'composite_variables') else 0,
            'analysis_complete': hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None
        }
        
        if summary_data['analysis_complete']:
            vuln_counts = data_handler.vulnerability_rankings['vulnerability_category'].value_counts()
            summary_data.update({
                'high_risk_wards': vuln_counts.get('High', 0),
                'medium_risk_wards': vuln_counts.get('Medium', 0),
                'low_risk_wards': vuln_counts.get('Low', 0)
            })
        
        sections['executive_summary'] = summary_data
        
        # 2. Data Summary
        if data_handler.csv_data is not None:
            data_summary = {
                'csv_rows': len(data_handler.csv_data),
                'csv_columns': len(data_handler.csv_data.columns),
                'available_variables': data_handler.get_available_variables() if hasattr(data_handler, 'get_available_variables') else []
            }
            
            if data_handler.shapefile_data is not None:
                data_summary.update({
                    'shapefile_features': len(data_handler.shapefile_data),
                    'shapefile_crs': str(data_handler.shapefile_data.crs)
                })
            
            sections['data_summary'] = data_summary
        
        # 3. Data Cleaning Summary
        if hasattr(data_handler, 'cleaned_data') and data_handler.cleaned_data is not None:
            cleaning_summary = {
                'cleaned_rows': len(data_handler.cleaned_data),
                'cleaned_columns': len(data_handler.cleaned_data.columns)
            }
            
            if hasattr(data_handler, 'na_handling_methods'):
                cleaning_summary['na_methods'] = data_handler.na_handling_methods
            
            sections['data_cleaning'] = cleaning_summary
        
        # 4. Normalization Summary
        if hasattr(data_handler, 'normalized_data') and data_handler.normalized_data is not None:
            norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
            normalization_summary = {
                'normalized_variables': len(norm_cols),
                'variable_relationships': getattr(data_handler, 'variable_relationships', {})
            }
            sections['normalization'] = normalization_summary
        
        # 5. Composite Score Analysis
        if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None:
            composite_summary = {
                'variables_used': getattr(data_handler, 'composite_variables', []),
                'selection_method': getattr(data_handler, 'variable_selection_method', 'unknown'),
                'composite_models': len([col for col in data_handler.composite_scores.columns if col.startswith('composite_')])
            }
            
            if hasattr(data_handler, 'variable_selection_explanations'):
                composite_summary['selection_explanations'] = data_handler.variable_selection_explanations
            
            sections['composite_analysis'] = composite_summary
        
        # 6. Vulnerability Analysis
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            vuln_df = data_handler.vulnerability_rankings
            
            # Get top 10 most vulnerable wards
            top_wards = vuln_df.sort_values('overall_rank').head(10)
            
            vulnerability_summary = {
                'total_wards_ranked': len(vuln_df),
                'vulnerability_distribution': vuln_df['vulnerability_category'].value_counts().to_dict(),
                'top_vulnerable_wards': top_wards[['WardName', 'vulnerability_category', 'overall_rank']].to_dict('records')
            }
            
            sections['vulnerability_analysis'] = vulnerability_summary
        
        # 7. Urban Extent Analysis
        if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
            urban_summary = {
                'thresholds_analyzed': list(data_handler.urban_extent_results.keys()),
                'threshold_results': {}
            }
            
            for threshold, results in data_handler.urban_extent_results.items():
                urban_summary['threshold_results'][str(threshold)] = {
                    'meets_threshold': len(results.get('meets_threshold', [])),
                    'below_threshold': len(results.get('below_threshold', []))
                }
            
            sections['urban_extent'] = urban_summary
        
        # 8. Metadata Summary
        if metadata:
            metadata_summary = {
                'total_execution_time': getattr(metadata, 'execution_time', 0),
                'steps_completed': len(metadata.steps),
                'decisions_made': len(metadata.decisions),
                'calculations_performed': len(metadata.calculations)
            }
            sections['analysis_metadata'] = metadata_summary
        
        # Generate formatted report based on requested format
        if format.lower() == 'markdown':
            formatted_report = generate_markdown_report(sections)
        elif format.lower() == 'html':
            formatted_report = generate_html_report(sections)
        else:
            formatted_report = sections  # Return raw dict
        
        return {
            'status': 'success',
            'report': formatted_report,
            'format': format,
            'sections': list(sections.keys()),
            'generated_at': time.time()
        }
        
    except Exception as e:
        logger.error(f"Error generating analysis report: {str(e)}")
        return {
            'status': 'error',
            'message': f'Error generating analysis report: {str(e)}'
        }


def generate_markdown_report(sections):
    """Generate a markdown-formatted report from sections"""
    report_lines = ["# Malaria Risk Prioritization Analysis Report\n"]
    
    if 'executive_summary' in sections:
        summary = sections['executive_summary']
        report_lines.extend([
            "## Executive Summary\n",
            f"- **Total Wards Analyzed:** {summary.get('total_wards', 0)}",
            f"- **Variables Used:** {summary.get('variables_analyzed', 0)}",
            f"- **Analysis Status:** {'Complete' if summary.get('analysis_complete') else 'Incomplete'}\n"
        ])
        
        if summary.get('analysis_complete'):
            report_lines.extend([
                "### Vulnerability Classification:",
                f"- **High Risk:** {summary.get('high_risk_wards', 0)} wards",
                f"- **Medium Risk:** {summary.get('medium_risk_wards', 0)} wards", 
                f"- **Low Risk:** {summary.get('low_risk_wards', 0)} wards\n"
            ])
    
    # Add other sections as needed...
    return "\n".join(report_lines)


def generate_html_report(sections):
    """Generate an HTML-formatted report from sections"""
    html_lines = [
        "<div class='analysis-report'>",
        "<h1>Malaria Risk Prioritization Analysis Report</h1>"
    ]
    
    if 'executive_summary' in sections:
        summary = sections['executive_summary']
        html_lines.extend([
            "<div class='executive-summary'>",
            "<h2>Executive Summary</h2>",
            "<ul>",
            f"<li><strong>Total Wards Analyzed:</strong> {summary.get('total_wards', 0)}</li>",
            f"<li><strong>Variables Used:</strong> {summary.get('variables_analyzed', 0)}</li>",
            f"<li><strong>Analysis Status:</strong> {'Complete' if summary.get('analysis_complete') else 'Incomplete'}</li>",
            "</ul>"
        ])
        
        if summary.get('analysis_complete'):
            html_lines.extend([
                "<h3>Vulnerability Classification:</h3>",
                "<ul>",
                f"<li><strong>High Risk:</strong> {summary.get('high_risk_wards', 0)} wards</li>",
                f"<li><strong>Medium Risk:</strong> {summary.get('medium_risk_wards', 0)} wards</li>",
                f"<li><strong>Low Risk:</strong> {summary.get('low_risk_wards', 0)} wards</li>",
                "</ul>"
            ])
        
        html_lines.append("</div>")
    
    html_lines.append("</div>")
    return "\n".join(html_lines) 