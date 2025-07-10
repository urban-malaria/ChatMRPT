# app/analysis/pipeline_stages/scoring_stages.py

import logging
import time
from ..scoring import compute_composite_scores, analyze_vulnerability
from ..urban_analysis import analyze_urban_extent

logger = logging.getLogger(__name__)


def run_composite_scoring_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables, llm_manager):
    """Run the composite scoring stage of the pipeline"""
    logger.info("Step 4: Calculating composite scores")
    
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
            
            # Import here to avoid circular imports
            from app.core.utils import get_analysis_variables, select_composite_variables
            from ..variable_selection_coordinator import get_variable_coordinator
            
            # Use the selected_variables that were passed to this stage
            # (they've already been validated by the coordinator in the main pipeline)
            final_variables = selected_variables
            
            if not final_variables:
                logger.error("⚠️ COMPOSITE METHOD: No variables provided to scoring stage")
                return {
                    'status': 'error',
                    'message': 'No variables provided for composite analysis'
                }
            
            # Validate that the coordinator variables are available in normalized data
            norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
            all_variables = [col.replace('normalization_', '') for col in norm_cols]
            
            # Import variable resolver for intelligent matching
            from app.services.variable_resolution_service import variable_resolver
            
            # Match coordinator variables to normalized variables using intelligent resolution
            matched_variables = []
            for var in final_variables:
                # First try exact case-insensitive matching
                matched_var = None
                for norm_var in all_variables:
                    if var.lower() == norm_var.lower():
                        matched_var = norm_var
                        break
                
                # If no exact match, try fuzzy matching
                if not matched_var:
                    resolution = variable_resolver.resolve_variable(var, all_variables, threshold=0.7)
                    if resolution['matched']:
                        matched_var = resolution['matched']
                        logger.info(f"🔍 FUZZY COMPOSITE MATCH: '{var}' → '{matched_var}' (confidence: {resolution['confidence']:.0%})")
                
                if matched_var:
                    matched_variables.append(matched_var)
                    logger.info(f"✅ COMPOSITE MATCH: '{var}' → '{matched_var}'")
                else:
                    logger.warning(f"❌ COMPOSITE MISSING: '{var}' not found in normalized data")
                    # Show available variables for debugging
                    similar_vars = variable_resolver.suggest_similar_variables(var, all_variables, max_suggestions=3)
                    if similar_vars:
                        logger.info(f"   💡 Similar variables available: {similar_vars}")
            
            if not matched_variables:
                logger.error("⚠️ COMPOSITE METHOD: No coordinator variables found in normalized data")
                return {
                    'status': 'error',
                    'message': f'None of the unified variables {final_variables} are available in normalized data. Available: {all_variables[:10]}{"..." if len(all_variables) > 10 else ""}'
                }
            
            final_variables = matched_variables
            logger.info(f"🔄 COMPOSITE METHOD: Using {len(final_variables)} unified variables: {final_variables}")
            
            # Compute composite scores
            composite_scores = compute_composite_scores(
                data_handler.normalized_data,
                final_variables,
                'mean',
                -1,  # Use all available cores
                metadata
            )
            
            # Store composite scores
            data_handler.composite_scores_mean = composite_scores
            data_handler.composite_variables = final_variables
            data_handler.variable_selection_method = 'unified_coordinator'
            
            # Store variables used for composite method (for comparison with PCA)
            data_handler.composite_variables_used = final_variables.copy()
            
            # Update step with results
            execution_time = time.time() - start_time
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['execution_time'] = execution_time
                    step['output_summary'] = {
                        'ward_count': len(composite_scores['scores']) if isinstance(composite_scores, dict) and 'scores' in composite_scores else len(composite_scores),
                        'variables_used': final_variables,
                        'selection_method': data_handler.variable_selection_method,
                        'composite_models': len([col for col in composite_scores['scores'].columns if col.startswith('model_')]) if isinstance(composite_scores, dict) and 'scores' in composite_scores else 0
                    }
                    break
            
            return {
                'status': 'success',
                'message': f'Successfully computed composite scores using {len(final_variables)} variables',
                'variables_used': final_variables,
                'selection_method': data_handler.variable_selection_method,
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
        if hasattr(data_handler, 'composite_scores_mean') and data_handler.composite_scores_mean is not None:
            return {
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


def run_vulnerability_ranking_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables):
    """Run the vulnerability ranking stage of the pipeline"""
    logger.info("Step 5: Calculating vulnerability rankings")
    
    if rerun_stages['ranking'] or selected_variables:
        start_time = time.time()
        step_id = metadata.record_step(
            'calculate_vulnerability_rankings',
            {'has_composite_scores': hasattr(data_handler, 'composite_scores_mean') and 
                                    data_handler.composite_scores_mean is not None},
            None,
            'vulnerability_ranking',
            {'n_categories': 3}  # Default to 3 categories
        )
        
        try:
            # Ensure we have composite scores
            if not hasattr(data_handler, 'composite_scores_mean') or data_handler.composite_scores_mean is None:
                return {
                    'status': 'error',
                    'message': 'No composite scores available for vulnerability ranking'
                }
            
            # Calculate vulnerability rankings
            print(f"\n🧮 PIPELINE DEBUG: Starting vulnerability ranking calculation...")
            print(f"📊 PIPELINE DEBUG: Composite scores available: {data_handler.composite_scores_mean is not None}")
            
            if data_handler.composite_scores_mean:
                scores_df = data_handler.composite_scores_mean.get('scores')
                if scores_df is not None:
                    print(f"📈 PIPELINE DEBUG: Composite scores shape: {scores_df.shape}")
                    model_cols = [col for col in scores_df.columns if col.startswith('model_')]
                    print(f"🤖 PIPELINE DEBUG: Found {len(model_cols)} model columns: {model_cols}")
                else:
                    print("❌ PIPELINE DEBUG: Composite scores DataFrame is None!")
            
            vulnerability_rankings = analyze_vulnerability(
                data_handler.composite_scores_mean,
                3,  # Use 3 categories (High, Medium, Low)
                metadata
            )
            
            print(f"✅ PIPELINE DEBUG: Vulnerability rankings calculated, shape: {vulnerability_rankings.shape}")
            if 'vulnerability_category' in vulnerability_rankings.columns:
                category_counts = vulnerability_rankings['vulnerability_category'].value_counts()
                print(f"🎯 PIPELINE DEBUG: Category distribution: {dict(category_counts)}")
            else:
                print("❌ PIPELINE DEBUG: No vulnerability_category column found!")
            
            # Store vulnerability rankings
            data_handler.vulnerability_rankings = vulnerability_rankings
            print(f"💾 PIPELINE DEBUG: Stored vulnerability rankings in data_handler")
            
            # Get top vulnerable wards
            top_wards = vulnerability_rankings.sort_values('overall_rank').head(10)['WardName'].tolist()
            print(f"🏆 PIPELINE DEBUG: Top 5 vulnerable wards: {top_wards[:5]}")
            
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
            
            return {
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
            return {
                'status': 'success', 
                'message': 'Using previously calculated vulnerability rankings',
                'vulnerable_wards': top_wards[:5]
            }
        else:
            return {
                'status': 'error',
                'message': 'No vulnerability rankings available'
            }


def run_urban_analysis_stage(data_handler, metadata, pipeline_step_id, rerun_stages):
    """Run the urban extent analysis stage of the pipeline"""
    logger.info("Step 6: Processing urban extent")
    
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
                    'status': 'warning',
                    'message': 'No CSV data available for urban extent analysis'
                }
            
            if data_handler.shapefile_data is None:
                return {
                    'status': 'warning',
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
            
            return {
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
            
            # Make urban analysis failure non-fatal - continue with warning
            return {
                'status': 'warning',
                'message': f'Urban extent analysis skipped: {str(e)}'
            }
    else:
        if hasattr(data_handler, 'urban_extent_results') and data_handler.urban_extent_results:
            return {
                'status': 'success', 
                'message': 'Using previously calculated urban extent results',
                'thresholds': list(data_handler.urban_extent_results.keys())
            }
        else:
            # Make missing urban results non-fatal - continue with warning
            return {
                'status': 'warning',
                'message': 'No urban extent results available (shapefile data required)'
            } 