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
        # BUT: if selected_variables is provided, ensure existing relationships cover those variables
        if data_handler.variable_relationships and not custom_relationships:
            if selected_variables:
                # Check if existing relationships cover all selected variables
                existing_vars = set(data_handler.variable_relationships.keys())
                selected_vars_set = set(selected_variables)
                if selected_vars_set.issubset(existing_vars):
                    rerun_stages['relationships'] = False
                    logger.info(f"Reusing existing variable relationships (cover all {len(selected_variables)} selected variables)")
                else:
                    missing_vars = selected_vars_set - existing_vars
                    logger.info(f"Rerunning relationships - existing relationships missing {len(missing_vars)} selected variables: {list(missing_vars)}")
                    rerun_stages['relationships'] = True
            else:
                rerun_stages['relationships'] = False
                logger.info("Reusing existing variable relationships (no custom relationships specified)")
            
            if not rerun_stages['relationships']:
                metadata.record_decision(
                    pipeline_step_id,
                    'reuse_existing_relationships',
                    options=['redetermine_relationships', 'use_existing_relationships'],
                    criteria='no custom relationships specified and existing relationships cover selected variables',
                    selected_option='use_existing_relationships'
                )
        
        # If selected_variables is provided but no other customizations,
        # reuse everything up to composite score calculation
        # BUT: ensure relationships were already handled above for selected variables
        if (selected_variables and not custom_relationships and not na_methods and 
            data_handler.normalized_data is not None):
            rerun_stages['clean'] = False
            # relationships already handled above - don't override
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
        
        # 1.5. Apply unified variable selection using the coordinator
        from .variable_selection_coordinator import get_variable_coordinator
        
        coordinator = get_variable_coordinator(session_id or "default")
        
        if selected_variables is None:
            logger.info("Step 1.5: Applying unified variable selection")
            
            selection_result = coordinator.get_unified_variable_selection(
                data_handler.cleaned_data, 
                data_handler.shapefile_data,
                None,  # No custom variables
                llm_manager
            )
            
            if selection_result['status'] == 'success':
                selected_variables = selection_result['variables']
                selection_method = selection_result.get('selection_method', 'unified')
                logger.info(f"✅ UNIFIED SELECTION: {selection_result['zone_detected']} zone, "
                           f"{len(selected_variables)} variables selected via {selection_method}")
                
                # Store unified metadata for later use
                metadata.record_step(
                    'unified_variable_selection',
                    {
                        'zone_detected': selection_result['zone_detected'],
                        'variables_selected': len(selected_variables),
                        'selection_method': selection_method,
                        'coordinator_used': True
                    },
                    {
                        'selected_variables': selected_variables,
                        'zone_metadata': selection_result.get('zone_metadata', {})
                    },
                    'unified_variable_selection',
                    {'automated_selection': True}
                )
                
                # Save unified metadata to file for integration
                try:
                    session_folder = getattr(data_handler, 'session_folder', f"instance/uploads/{session_id}")
                    if session_folder:
                        import json
                        unified_metadata = {
                            'zone_detected': selection_result['zone_detected'],
                            'selected_variables': selected_variables,
                            'selection_method': selection_method,
                            'zone_metadata': selection_result.get('zone_metadata', {}),
                            'variables_count': len(selected_variables),
                            'coordinator_used': True,
                            'timestamp': time.time()
                        }
                        
                        metadata_path = os.path.join(session_folder, 'unified_variable_metadata.json')
                        with open(metadata_path, 'w') as f:
                            json.dump(unified_metadata, f, indent=2, default=str)
                        
                        logger.info(f"💾 Saved unified variable metadata to {metadata_path}")
                        
                except Exception as save_error:
                    logger.warning(f"Could not save unified variable metadata: {save_error}")
            else:
                logger.error(f"❌ UNIFIED SELECTION FAILED: {selection_result.get('message', 'Unknown error')}")
                return {
                    'status': 'error',
                    'message': f"Variable selection failed: {selection_result.get('message', 'Unknown error')}",
                    'data': None
                }
        else:
            # User provided custom variables - validate through coordinator
            logger.info("Step 1.5: Validating user-provided variables through coordinator")
            
            selection_result = coordinator.get_unified_variable_selection(
                data_handler.cleaned_data, 
                data_handler.shapefile_data,
                selected_variables,  # Custom variables
                llm_manager
            )
            
            if selection_result['status'] == 'success':
                # Update selected_variables with validated set
                selected_variables = selection_result['variables']
                logger.info(f"✅ CUSTOM VARIABLES VALIDATED: {len(selected_variables)} variables approved")
            else:
                logger.error(f"❌ CUSTOM VARIABLES VALIDATION FAILED: {selection_result.get('message', 'Unknown error')}")
                return {
                    'status': 'error',
                    'message': f"Custom variable validation failed: {selection_result.get('message', 'Unknown error')}",
                    'data': None
                }
        
        # 2. Determine variable relationships if needed
        relationship_result = run_relationship_stage(data_handler, metadata, pipeline_step_id, rerun_stages, custom_relationships, selected_variables)
        if relationship_result['status'] == 'error':
            return relationship_result
        
        # 3. Normalize data if needed
        normalization_result = run_normalization_stage(data_handler, metadata, pipeline_step_id, rerun_stages, selected_variables)
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
        
        # 7. Validate variable consistency across methods
        from .variable_comparison_validator import validate_analysis_consistency
        
        logger.info("Step 7: Validating variable consistency")
        consistency_result = validate_analysis_consistency(data_handler, session_id or "default")
        
        if not consistency_result['consistent']:
            logger.warning(f"⚠️ CONSISTENCY: {consistency_result['message']}")
            
            # If inconsistent, try to fix it
            if consistency_result['status'] == 'warning':
                logger.info("🔧 CONSISTENCY: Attempting to fix variable inconsistency")
                from .variable_comparison_validator import VariableComparisonValidator
                validator = VariableComparisonValidator(session_id or "default")
                fix_result = validator.fix_variable_inconsistency(data_handler)
                
                if fix_result['status'] == 'success':
                    logger.info(f"✅ CONSISTENCY: {fix_result['message']}")
                    if fix_result['requires_rerun']:
                        logger.info("🔄 CONSISTENCY: Variable fix requires analysis rerun")
                else:
                    logger.error(f"❌ CONSISTENCY: {fix_result['message']}")
        else:
            logger.info(f"✅ CONSISTENCY: {consistency_result['message']}")
        
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
                    'consistency_result': consistency_result.get('status'),
                    'variables_used': composite_result.get('variables_used') if composite_result else [],
                    'selection_method': composite_result.get('selection_method', 'unified_coordinator'),
                    'vulnerable_wards': ranking_result.get('vulnerable_wards') if ranking_result else [],
                    'variable_consistency': consistency_result.get('consistent', False),
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
        
        # Create/update unified dataset with region metadata
        try:
            print("PIPELINE DEBUG: Creating unified dataset with region metadata...")
            from ..data.unified_dataset_builder import build_unified_dataset, load_unified_dataset
            
            unified_result = build_unified_dataset(session_id)
            if unified_result['status'] == 'success':
                print(f"PIPELINE DEBUG: ✅ Unified dataset created successfully: {unified_result['message']}")
                
                # Load the created unified dataset and assign it to data_handler
                try:
                    unified_dataset = load_unified_dataset(session_id)
                    if unified_dataset is not None:
                        data_handler.unified_dataset = unified_dataset
                        print(f"PIPELINE DEBUG: ✅ Unified dataset loaded and assigned to data_handler: {len(unified_dataset)} rows, {len(unified_dataset.columns)} columns")
                    else:
                        print(f"PIPELINE DEBUG: ⚠️ Failed to load unified dataset from disk")
                except Exception as load_error:
                    print(f"PIPELINE DEBUG: Error loading unified dataset: {load_error}")
                    logger.warning(f"Could not load unified dataset: {load_error}")
            else:
                print(f"PIPELINE DEBUG: ⚠️ Unified dataset creation failed: {unified_result.get('message', 'Unknown error')}")
                
        except Exception as unified_error:
            print(f"PIPELINE DEBUG: Error creating unified dataset: {unified_error}")
            logger.warning(f"Could not create unified dataset: {unified_error}")
            
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
            'composite_scores': data_handler.composite_scores,
            
            # Add region-aware metadata
            'region_metadata': {
                'zone_detected': region_result.get('zone_detected') if 'region_result' in locals() else None,
                'selection_method': region_result.get('selection_method') if 'region_result' in locals() else 'default'
            }
        }
        
        return summary
        
    except Exception as e:
        logger.error("Error in full analysis pipeline: {}".format(str(e)))
        traceback.print_exc()
        return {
            'status': 'error',
            'message': "Error in full analysis pipeline: {}".format(str(e))
        }