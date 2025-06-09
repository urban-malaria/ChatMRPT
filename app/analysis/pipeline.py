# app/analysis/pipeline.py
import logging
import time
import traceback
import pandas as pd
from typing import Dict, List, Optional, Any, Union
import sys
import os

# Import the individual analysis modules
from .metadata import AnalysisMetadata

# Import all stage functions from the new pipeline_stages package
from .pipeline_stages import (
    # Data preparation stages
    run_data_cleaning_stage,
    run_relationship_stage,
    run_normalization_stage,
    
    # Scoring stages
    run_composite_scoring_stage,
    run_vulnerability_ranking_stage,
    run_urban_analysis_stage,
    
    # Note: PCA stages removed - PCA uses dedicated pca_pipeline.py
    
    # Utilities
    apply_composite_scores_fix
)

# Set up logging
logger = logging.getLogger(__name__)


def run_full_analysis_pipeline(data_handler, selected_variables=None, 
                             na_methods=None, custom_relationships=None, 
                             metadata=None, session_id=None, interaction_logger=None,
                             llm_manager=None):
    """
    Run the complete analysis pipeline with DUAL METHOD APPROACH
    
    This pipeline automatically runs BOTH mean and PCA composite scoring methods,
    generating two complete sets of results that users can switch between viewing.
    
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
        Dict with analysis results containing BOTH method pathways
    """
    print(f"\nPIPELINE DEBUG: Starting run_full_analysis_pipeline")
    print(f"PIPELINE DEBUG: session_id={session_id}")
    print(f"PIPELINE DEBUG: selected_variables={selected_variables}")
    print(f"PIPELINE DEBUG: na_methods={na_methods}")
    print(f"PIPELINE DEBUG: custom_relationships={custom_relationships}")
    print(f"PIPELINE DEBUG: llm_manager={llm_manager is not None}")
    print(f"PIPELINE DEBUG: metadata={metadata is not None}")
    print(f"PIPELINE DEBUG: interaction_logger={interaction_logger is not None}")
    sys.stdout.flush()  # Force immediate output
    
    try:
        # Initialize metadata if not provided
        if metadata is None:
            metadata = AnalysisMetadata(session_id)
            metadata.logger = interaction_logger
        
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
        clean_result = run_data_cleaning_stage(data_handler, metadata, pipeline_step_id, rerun_stages, na_methods)
        if clean_result['status'] == 'error':
            return clean_result
        
        # 2. Determine variable relationships if needed
        relationship_result = run_relationship_stage(data_handler, metadata, pipeline_step_id, rerun_stages, custom_relationships)
        if relationship_result['status'] == 'error':
            return relationship_result
        
        # 3. Normalize data if needed
        normalization_result = run_normalization_stage(data_handler, metadata, pipeline_step_id, rerun_stages)
        if normalization_result['status'] == 'error':
            return normalization_result
        
        # 4. Compute composite scores
        composite_result = run_composite_scoring_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables, llm_manager)
        if composite_result['status'] == 'error':
            return composite_result
        
        print(f"PIPELINE DEBUG: Composite scoring completed successfully")
        print(f"PIPELINE DEBUG: Variables used: {composite_result.get('variables_used', [])}")
        
        # **APPLY THE CRITICAL FIX IMMEDIATELY AFTER COMPOSITE SCORING**
        print(f"PIPELINE DEBUG: Applying composite_scores fix immediately...")
        fix_applied = apply_composite_scores_fix(data_handler)
        if fix_applied:
            print(f"PIPELINE DEBUG: composite_scores fix applied successfully!")
        else:
            print(f"PIPELINE DEBUG: composite_scores fix failed!")
            
        # 5-9. Continue with other stages but don't return early
        print(f"PIPELINE DEBUG: Continuing with remaining stages...")
        
        # 5. Calculate vulnerability rankings
        ranking_result = run_vulnerability_ranking_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables)
        if ranking_result['status'] == 'error':
            return ranking_result
        
        # 6. Process urban extent
        urban_result = run_urban_analysis_stage(data_handler, metadata, pipeline_step_id, rerun_stages)
        if urban_result['status'] == 'error':
            return urban_result
        
        # Note: PCA analysis uses the dedicated pca_pipeline.py, not embedded composite scoring
        
        # Update pipeline step with results
        total_execution_time = time.time() - metadata.start_time
        for step in metadata.steps:
            if step['step_id'] == pipeline_step_id:
                step['execution_time'] = total_execution_time
                step['output_summary'] = {
                    'clean_result': clean_result.get('status') if clean_result else None,
                    'norm_result': normalization_result.get('status') if normalization_result else None,
                    'composite_result': composite_result.get('status') if composite_result else None,
                    'ranking_result': ranking_result.get('status') if ranking_result else None,
                    'urban_result': urban_result.get('status') if urban_result else None,
                    'variables_used': composite_result.get('variables_used') if composite_result else [],
                    'selection_method': composite_result.get('selection_method', 'default'),
                    'vulnerable_wards': ranking_result.get('vulnerable_wards') if ranking_result else [],
                    # PCA analysis is handled by dedicated pca_pipeline.py
                }
                break
        
        # Compile summary of all steps
        logger.info("Analysis pipeline complete")
        print("PIPELINE DEBUG: About to apply composite_scores fix...")
        
        # **CRITICAL FIX: Set the main composite_scores attribute for visualization compatibility**
        # The visualization functions expect data_handler.composite_scores, not composite_scores_mean
        if apply_composite_scores_fix(data_handler):
            print("PIPELINE DEBUG: composite_scores fix applied, creating summary...")
            
        # **CRITICAL FIX: Save analysis results to disk for supplementary tools**
        print("PIPELINE DEBUG: Saving analysis results to disk...")
        try:
            # Collect all results for saving
            results_to_save = {
                'cleaned_data': data_handler.cleaned_data,
                'normalized_data': data_handler.normalized_data,
                'composite_scores_mean': data_handler.composite_scores_mean,
                'composite_scores': data_handler.composite_scores,
                'vulnerability_rankings': data_handler.vulnerability_rankings,
                'composite_scores_pca': getattr(data_handler, 'composite_scores_pca', None),
                'vulnerability_rankings_pca': getattr(data_handler, 'vulnerability_rankings_pca', None),
                'urban_extent_results': getattr(data_handler, 'urban_extent_results', None),
                'urban_extent_results_pca': getattr(data_handler, 'urban_extent_results_pca', None)
            }
            
            # Use the coordinator's save method if available
            if hasattr(data_handler, 'coordinator') and hasattr(data_handler.coordinator, '_save_analysis_results'):
                data_handler.coordinator._save_analysis_results(results_to_save)
                print("PIPELINE DEBUG: Analysis results saved via coordinator")
            else:
                # Fallback: manual save of critical files
                session_folder = getattr(data_handler, 'session_folder', None)
                if session_folder:
                    # Save composite scores and formulas
                    if data_handler.composite_scores_mean and isinstance(data_handler.composite_scores_mean, dict):
                        if 'scores' in data_handler.composite_scores_mean:
                            scores_path = os.path.join(session_folder, 'composite_scores.csv')
                            data_handler.composite_scores_mean['scores'].to_csv(scores_path, index=False)
                            print(f"PIPELINE DEBUG: Saved composite scores to {scores_path}")
                            
                        if 'formulas' in data_handler.composite_scores_mean:
                            formulas_path = os.path.join(session_folder, 'model_formulas.csv')
                            formulas_df = pd.DataFrame(data_handler.composite_scores_mean['formulas'])
                            if 'variables' in formulas_df.columns:
                                formulas_df['variables'] = formulas_df['variables'].apply(
                                    lambda x: ','.join(x) if isinstance(x, list) else str(x)
                                )
                            formulas_df.to_csv(formulas_path, index=False)
                            print(f"PIPELINE DEBUG: Saved model formulas to {formulas_path}")
                    
                    # Save vulnerability rankings
                    if data_handler.vulnerability_rankings is not None:
                        rankings_path = os.path.join(session_folder, 'vulnerability_rankings.csv')
                        data_handler.vulnerability_rankings.to_csv(rankings_path, index=False)
                        print(f"PIPELINE DEBUG: Saved vulnerability rankings to {rankings_path}")
                        
                print("PIPELINE DEBUG: Analysis results saved via fallback method")
                
        except Exception as e:
            print(f"PIPELINE DEBUG: Error saving analysis results: {e}")
            # Don't fail the pipeline if save fails - just log it
            logger.warning(f"Could not save analysis results: {e}")
            
        summary = {
            'status': 'success',
            'message': 'Complete analysis pipeline successfully executed',
            'steps': {
                'clean': clean_result,
                'normalize': normalization_result,
                'composite': composite_result,
                'ranking': ranking_result,
                'urban': urban_result
                # Note: PCA analysis is handled by dedicated pca_pipeline.py
            },
            'variables_used': composite_result.get('variables_used') if composite_result else [],
            'selection_method': composite_result.get('selection_method', 'default'),
            'vulnerable_wards': ranking_result.get('vulnerable_wards') if ranking_result else [],
            'execution_time': total_execution_time,
            'metadata': metadata,  # Include metadata for explanation
            # Add actual data results for the coordinator to extract
            'cleaned_data': data_handler.cleaned_data,
            'normalized_data': data_handler.normalized_data,
            'composite_scores_mean': data_handler.composite_scores_mean,
            'vulnerability_rankings': data_handler.vulnerability_rankings,
            'variable_relationships': data_handler.variable_relationships,
            'composite_variables': getattr(data_handler, 'composite_variables', []),
            'composite_scores_pca': getattr(data_handler, 'composite_scores_pca', None),
            'vulnerability_rankings_pca': getattr(data_handler, 'vulnerability_rankings_pca', None),
            'urban_extent_results': getattr(data_handler, 'urban_extent_results', None),
            'urban_extent_results_pca': getattr(data_handler, 'urban_extent_results_pca', None),
            
            # **CRITICAL BACKWARD COMPATIBILITY FIX**
            # Visualization code expects 'composite_scores' (not 'composite_scores_mean')
            'composite_scores': data_handler.composite_scores
        }
        
        return summary
        
    except Exception as e:
        logger.error("Error in full analysis pipeline: {}".format(str(e)))
        traceback.print_exc()
        return {
            'status': 'error',
            'message': "Error in full analysis pipeline: {}".format(str(e))
        }