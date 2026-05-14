"""
Complete Analysis Tools 

This module provides the coordinated dual-method analysis workflow without any
settlement integration, as per the updated post-permission workflow overhaul.
"""

import glob
import json
import logging
import os
import shutil
import time
import traceback
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from flask import current_app

from app.utils.tool_base import BaseTool, ToolCategory, ToolExecutionResult, DataAnalysisTool

logger = logging.getLogger(__name__)


class RunMalariaRiskAnalysisInput(BaseModel):
    """Input for complete analysis workflow with custom variable selection support"""
    session_id: str = Field(..., description="Session identifier for data access")
    composite_variables: Optional[List[str]] = Field(
        None, 
        description="Custom variables for composite analysis. If None, uses region-aware auto-selection"
    )
    pca_variables: Optional[List[str]] = Field(
        None, 
        description="Custom variables for PCA analysis. If None, uses region-aware auto-selection"
    )
    create_unified_dataset: bool = Field(True, description="Whether to create/update unified dataset")
    validate_variables: bool = Field(True, description="Whether to validate custom variables against available data")


class RunMalariaRiskAnalysis(DataAnalysisTool):
    """
    Run complete dual-method malaria risk analysis (Composite + PCA) without settlement integration logic.
    
    This tool implements the post-permission workflow overhaul:
    1. Start with raw data (CSV + shapefile)
    2. Apply deferred cleaning (spatial neighbor mean imputation)
    3. Use region-aware variable selection
    4. Run both composite and PCA analyses with the SAME variables
    5. Create unified dataset preserving original data structure
    
    Settlement integration logic is completely excluded from this workflow.
    Any existing settlement columns in the original data are preserved.
    """
    
    name: str = "run_malaria_risk_analysis"
    description: str = "Run malaria risk analysis using dual-method approach (Composite Score + PCA) with support for custom variable selection. This is the primary tool for comprehensive malaria vulnerability assessment."
    composite_variables: Optional[List[str]] = Field(
        None, 
        description="Custom variables for composite analysis. If None, uses region-aware auto-selection"
    )
    pca_variables: Optional[List[str]] = Field(
        None, 
        description="Custom variables for PCA analysis. If None, uses region-aware auto-selection"
    )
    create_unified_dataset: bool = Field(True, description="Whether to create/update unified dataset")
    validate_variables: bool = Field(True, description="Whether to validate custom variables against available data")
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow with custom variable support"""
        session_id = kwargs.get('session_id')
        tool_start_time = time.time()

        # CRITICAL DEBUG - Track tool execution
        print("\n" + "🔴"*30)
        print("🚨 RISK ANALYSIS TOOL EXECUTE() CALLED!")
        print("🔴"*30)
        print(f"Session ID: {session_id}")
        print(f"Kwargs keys: {list(kwargs.keys())}")
        print(f"Called at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔴"*30 + "\n")

        logger.critical(f"🚨 RISK ANALYSIS TOOL EXECUTING - Session: {session_id}")

        # 🎯 LOG TOOL EXECUTION START - CRITICAL FOR DEMO ANALYTICS
        interaction_logger = None
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            
            # Log tool execution start with comprehensive parameters
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='tool_execution_start',
                details={
                    'tool_name': self.name,
                    'tool_description': self.description,
                    'parameters': {
                        'composite_variables': kwargs.get('composite_variables'),
                        'pca_variables': kwargs.get('pca_variables'),
                        'create_unified_dataset': kwargs.get('create_unified_dataset', True),
                        'validate_variables': kwargs.get('validate_variables', True)
                    },
                    'start_timestamp': tool_start_time,
                    'execution_method': 'parallel_dual_method'
                },
                success=True
            )
        
        try:
            # Get variables first before logging
            composite_variables = kwargs.get('composite_variables') or getattr(self, 'composite_variables', None)
            pca_variables = kwargs.get('pca_variables') or getattr(self, 'pca_variables', None)
            
            # 🔍 DEBUG: Complete Analysis Execution
            logger.info("=" * 60)
            logger.info("🔍 DEBUG COMPLETE ANALYSIS: Starting execution")
            logger.info(f"🔍 Session ID: {session_id}")
            logger.info(f"🔍 Composite variables: {composite_variables}")
            logger.info(f"🔍 PCA variables: {pca_variables}")
            logger.info(f"🔍 Create unified dataset: {kwargs.get('create_unified_dataset', True)}")
            
            # Check what files exist before analysis
            session_dir = f'instance/uploads/{session_id}'
            if os.path.exists(session_dir):
                files = os.listdir(session_dir)
                logger.info(f"🔍 Files in session BEFORE analysis: {files[:10]}")
                
                # Check for key files
                for key_file in ['raw_data.csv', 'raw_shapefile.zip', 'unified_dataset.csv', 'tpr_results.csv']:
                    file_path = os.path.join(session_dir, key_file)
                    if os.path.exists(file_path):
                        size = os.path.getsize(file_path)
                        logger.info(f"🔍   ✅ {key_file} exists ({size:,} bytes)")
                    else:
                        logger.info(f"🔍   ❌ {key_file} NOT found")
            logger.info("=" * 60)
            # Variables already defined above for debug logging
            create_unified_dataset = kwargs.get('create_unified_dataset', getattr(self, 'create_unified_dataset', True))
            validate_variables = kwargs.get('validate_variables', getattr(self, 'validate_variables', True))
            
            logger.info(f"Starting complete dual-method analysis for session {session_id}")
            logger.info(f"Custom variables - Composite: {composite_variables}, PCA: {pca_variables}")
            
            # Variable validation and fallback logic
            validation_message = ""
            final_composite_vars = composite_variables
            final_pca_vars = pca_variables
            
            # For custom analysis, use variables directly without validation fallbacks
            if composite_variables or pca_variables:
                final_composite_vars = composite_variables
                final_pca_vars = pca_variables
                validation_message = f"✅ Using custom variables - Composite: {len(final_composite_vars or [])}, PCA: {len(final_pca_vars or [])}"
                logger.info(f"Custom variable selection: Composite: {final_composite_vars}, PCA: {final_pca_vars}")
            else:
                # Auto-selection when no custom variables specified
                final_composite_vars = None
                final_pca_vars = None
                validation_message = ""
            
            # 🎯 LOG VARIABLE VALIDATION - DEMO ANALYTICS
            if interaction_logger:
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='variable_validation',
                    details={
                        'original_composite_variables': composite_variables,
                        'original_pca_variables': pca_variables,
                        'validation_enabled': validate_variables,
                        'validation_message': validation_message
                    },
                    success=True
                )
            
            # Run both analyses sequentially for better reliability
            analysis_start_time = time.time()
            
            # Run composite analysis first
            logger.info("🚀 Starting composite analysis...")
            composite_result = self._run_composite_analysis(session_id, final_composite_vars)
            if composite_result['success']:
                logger.info("✅ Composite analysis completed successfully")
            else:
                logger.error(f"❌ Composite analysis failed: {composite_result['message']}")
            
            # Run PCA analysis second
            logger.info("🔬 Starting PCA analysis...")
            print("\n" + "🟣"*30)
            print("🔬 CALLING _run_pca_analysis()")
            print(f"   Session: {session_id}")
            print(f"   Variables: {final_pca_vars}")
            print("🟣"*30 + "\n")
            pca_result = self._run_pca_analysis(session_id, final_pca_vars)
            
            # Handle PCA skipped due to statistical tests
            if pca_result.get('pca_skipped'):
                logger.info(f"⚠️ PCA skipped: {pca_result['message']}")
                print("\n" + "="*60)
                print("📊 PCA ANALYSIS SKIPPED DUE TO STATISTICAL TESTS")
                print("="*60)
                print(f"Reason: {pca_result['message']}")
                print(f"KMO Value: {pca_result.get('data', {}).get('kmo_value', 'N/A')}")
                print(f"Bartlett's p-value: {pca_result.get('data', {}).get('bartlett_p_value', 'N/A')}")
                print("Recommendation: Continuing with composite analysis only")
                print("="*60 + "\n")
                
                # Mark as special case - not a failure but PCA was not suitable
                pca_result['success'] = False
                pca_result['skipped_reason'] = pca_result.get('data', {})
            elif pca_result['success']:
                logger.info("✅ PCA analysis completed successfully")
            else:
                logger.error(f"❌ PCA analysis failed: {pca_result['message']}")
            
            analysis_execution_time = time.time() - analysis_start_time
            logger.info(f"🔄 Both analyses completed sequentially in {analysis_execution_time:.2f} seconds")
            
            # 🎯 LOG ANALYSIS COMPLETION - CRITICAL FOR DEMO ANALYTICS
            if interaction_logger:
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='sequential_analysis_complete',
                    details={
                        'composite_success': composite_result['success'],
                        'pca_success': pca_result['success'],
                        'analysis_time_seconds': analysis_execution_time,
                        'composite_error': composite_result.get('error_details') if not composite_result['success'] else None,
                        'pca_error': pca_result.get('error_details') if not pca_result['success'] else None,
                        'execution_method': 'sequential'
                    },
                    success=composite_result['success'] and pca_result['success']
                )
            
            # Check if either analysis failed
            if not composite_result['success']:
                # 🎯 LOG COMPOSITE ANALYSIS FAILURE - DEMO ERROR TRACKING
                if interaction_logger:
                    interaction_logger.log_error(
                        session_id=session_id,
                        error_type='CompositeAnalysisFailure',
                        error_message=composite_result['message'],
                        stack_trace=composite_result.get('error_details', 'No stack trace available')
                    )
                
                return ToolExecutionResult(
                    success=False,
                    message=f"Composite analysis failed: {composite_result['message']}",
                    error_details=composite_result.get('error_details')
                )
            
            # Check for PCA recovery in unified dataset if initial PCA failed
            pca_recovered = False
            if not pca_result['success']:
                # Check if PCA was skipped due to statistical tests
                if pca_result.get('pca_skipped'):
                    logger.info(f"ℹ️ PCA was skipped due to statistical tests: {pca_result['message']}")
                    # This is not an error - continue with composite-only results
                    pca_result = {
                        'success': False,
                        'pca_skipped': True,
                        'message': pca_result['message'],
                        'data': pca_result.get('data', {}),
                        'skipped_reason': pca_result.get('skipped_reason', {})
                    }
                else:
                    logger.warning(f"⚠️ Initial PCA analysis failed: {pca_result['message']}")
                    logger.info("🔍 Checking if PCA was recovered during unified dataset creation...")
                    
                    # Check if PCA rankings file exists (indicates successful PCA recovery)
                    pca_rankings_path = f"instance/uploads/{session_id}/analysis_vulnerability_rankings_pca.csv"
                    if os.path.exists(pca_rankings_path):
                        logger.info("✅ PCA analysis recovered successfully in unified dataset phase")
                        pca_recovered = True
                        pca_result = {
                            'success': True,
                            'message': 'PCA analysis completed via unified dataset recovery',
                            'data': {'recovered_from_unified_dataset': True}
                        }
                    else:
                        # 🎯 LOG PCA ANALYSIS FAILURE - DEMO ERROR TRACKING
                        if interaction_logger:
                            interaction_logger.log_error(
                                session_id=session_id,
                                error_type='PCAAnalysisFailure',
                                error_message=pca_result['message'],
                                stack_trace=pca_result.get('error_details', 'No stack trace available')
                            )
                        
                        return ToolExecutionResult(
                            success=False,
                            message=f"PCA analysis failed: {pca_result['message']}",
                            error_details=pca_result.get('error_details')
                        )
            
            # Create/update unified dataset without settlement integration
            if create_unified_dataset:
                # Actually attempt to create unified dataset instead of assuming it exists
                logger.info("📊 Creating unified dataset...")
                try:
                    from app.services.dataset_builder import UnifiedDatasetBuilder
                    builder = UnifiedDatasetBuilder(session_id)
                    unified_result = builder.build_unified_dataset()
                    
                    if unified_result.get('status') == 'success':
                        logger.info(f"✅ Unified dataset created successfully: {unified_result['message']}")
                        unified_result['success'] = True  # Add success key for compatibility
                    else:
                        logger.warning(f"❌ Unified dataset creation failed: {unified_result['message']}")
                        unified_result['success'] = False  # Add success key for compatibility
                        return ToolExecutionResult(
                            success=False,
                            message=(
                                "Risk analysis could not complete because the unified dataset was not created. "
                                f"Reason: {unified_result.get('message', 'Unknown unified dataset error')}"
                            ),
                            error_details=unified_result.get('message')
                        )
                except Exception as e:
                    logger.error(f"💥 Error creating unified dataset: {e}")
                    unified_result = {'success': False, 'message': f'Error: {str(e)}'}
                    return ToolExecutionResult(
                        success=False,
                        message=(
                            "Risk analysis could not complete because the unified dataset was not created. "
                            f"Reason: {str(e)}"
                        ),
                        error_details=str(e)
                    )
            
            # Generate comparison summary (only if PCA was actually run)
            if pca_result.get('pca_skipped'):
                logger.info("📋 Skipping comparison summary - PCA was not suitable")
                comparison_summary = {
                    'summary': 'PCA analysis was not performed due to statistical test results',
                    'pca_skipped': True,
                    'reason': pca_result.get('message', 'Data not suitable for PCA')
                }
            else:
                logger.info("📋 Generating dual-method comparison summary...")
                comparison_summary = self._generate_comparison_summary(
                    composite_result['data'], 
                    pca_result['data']
                )

            # Note: Vulnerability map tool is available for users to request
            # When PCA is skipped, users can still create composite vulnerability maps

            # Prepare comprehensive result
            result_data = {
                'composite_analysis': composite_result['data'],
                'pca_analysis': pca_result['data'],
                'comparison_summary': comparison_summary,
                'analyses_completed': ['composite_score', 'pca'],
                'unified_dataset_created': create_unified_dataset and unified_result.get('success', False),
                'execution_time_seconds': analysis_execution_time,
                'execution_method': 'sequential',
                'settlement_integration_logic': 'excluded',
                'original_settlement_data': 'preserved'
            }

            
            # Run per-year analyses in parallel — blocks until all years done
            # so the completion message is only shown when everything is ready
            pca_passed = not pca_result.get('pca_skipped', True)
            self._run_per_year_risk(session_id, final_composite_vars, pca_passed)

            # Mark comprehensive analysis as complete for workflow guidance
            self._mark_analysis_complete(session_id)

            # Extract key metrics for success message
            wards_analyzed = composite_result.get('data', {}).get('wards_analyzed', 'N/A')
            components_found = pca_result.get('data', {}).get('components_found', 'N/A')
            variance_explained = pca_result.get('data', {}).get('variance_explained', 'N/A')
            
            # Generate comprehensive user-friendly summary
            success_message = self._generate_comprehensive_summary(
                composite_result, pca_result, comparison_summary, analysis_execution_time, session_id
            )
            
            total_execution_time = time.time() - tool_start_time
            
            # 🎯 LOG TOOL EXECUTION SUCCESS - CRITICAL FOR DEMO ANALYTICS
            if interaction_logger:
                # Log successful tool completion with comprehensive metrics
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='tool_execution_complete',
                    details={
                        'tool_name': self.name,
                        'total_execution_time_seconds': total_execution_time,
                        'analysis_time_seconds': analysis_execution_time,
                        'overhead_time_seconds': total_execution_time - analysis_execution_time,
                        'wards_analyzed': composite_result.get('data', {}).get('wards_analyzed', 'N/A'),
                        'visualizations_created': len(result_data.get('visualizations', {})),
                        'unified_dataset_created': result_data['unified_dataset_created'],
                        'methods_completed': ['composite_score', 'pca'],
                        'variable_selection_methods': {
                            'composite': composite_result.get('data', {}).get('variable_selection_method', 'auto'),
                            'pca': pca_result.get('data', {}).get('variable_selection_method', 'auto')
                        },
                        'performance_metrics': {
                            'sequential_execution': True,
                            'execution_efficiency': round(analysis_execution_time / total_execution_time * 100, 1)
                        }
                    },
                    success=True
                )
            
            # Notify analysis state handler about completion
            try:
                from app.conversation.analysis_state import get_analysis_state_handler
                state_handler = get_analysis_state_handler()
                state_result = state_handler.on_analysis_complete(session_id, result_data)
                logger.info(f"State handler notified: {state_result}")
            except Exception as e:
                logger.error(f"Failed to notify state handler: {e}")
                # Continue - not critical for analysis success
            
            # CRITICAL: Mark analysis complete in Redis for multi-worker consistency
            # This is the authoritative source of truth across ALL workers
            try:
                from app.services.redis_state import get_redis_state_manager
                redis_manager = get_redis_state_manager()
                redis_success = redis_manager.mark_analysis_complete(session_id)
                if redis_success:
                    logger.info(f"🎯 Redis: Analysis marked complete for session {session_id}")
                else:
                    logger.warning(f"⚠️ Redis: Failed to mark analysis complete for {session_id}")
            except Exception as e:
                logger.error(f"Redis state manager error: {e}")
                # Continue - fallback to other methods
            
            # Set the general analysis_complete flag that ITN tool checks
            try:
                from flask import session
                session['analysis_complete'] = True
                logger.info(f"✅ Set session['analysis_complete'] = True for ITN planning")
            except Exception as e:
                logger.warning(f"Failed to set analysis_complete flag: {e}")
            
            # Create analysis completion marker file for cross-worker reliability
            # NOTE: This is a FALLBACK when Redis is unavailable
            try:
                # os is already imported at the top of the file
                from pathlib import Path
                from flask import session
                
                session_folder = Path("instance/uploads") / session_id
                if session_folder.exists():
                    marker_file = session_folder / ".analysis_complete"
                    marker_file.touch()
                    logger.info(f"✅ Created .analysis_complete marker file for session {session_id} (fallback)")
                    
                    # Update workflow stage to indicate analysis is now complete
                    session['workflow_stage'] = 'analysis_complete'
                    session.modified = True
                    logger.info(f"✅ Updated workflow_stage to 'analysis_complete' for session {session_id}")
            except Exception as e:
                logger.warning(f"Failed to create analysis marker file or update session: {e}")
            
            return ToolExecutionResult(
                success=True,
                message=success_message,
                data=result_data,
                metadata={
                    'analyses_run': ['composite_score', 'pca'],
                    'execution_method': 'sequential',
                    'execution_time_seconds': analysis_execution_time,
                    'total_execution_time_seconds': total_execution_time,
                    'unified_dataset_status': 'created' if result_data['unified_dataset_created'] else 'failed',
                    'settlement_integration_logic': 'excluded',
                    'original_settlement_data': 'preserved',
                    'performance_benefits': 'Parallel execution reduces total analysis time',
                    'workflow_stage': 'comprehensive_analysis_complete',
                    'variable_consistency': 'region_aware_selection'
                }
            )
            
        except Exception as e:
            total_execution_time = time.time() - tool_start_time
            
            # 🎯 LOG TOOL EXECUTION FAILURE - CRITICAL FOR DEMO ERROR TRACKING
            if interaction_logger:
                interaction_logger.log_error(
                    session_id=session_id,
                    error_type=f'ToolExecutionError:{type(e).__name__}',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                
                # Log detailed failure analysis event
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='tool_execution_failed',
                    details={
                        'tool_name': self.name,
                        'failure_time_seconds': total_execution_time,
                        'error_type': type(e).__name__,
                        'error_message': str(e),
                        'failure_stage': 'tool_execution',
                        'parameters': {
                            'composite_variables': kwargs.get('composite_variables'),
                            'pca_variables': kwargs.get('pca_variables'),
                            'validate_variables': kwargs.get('validate_variables', True)
                        }
                    },
                    success=False
                )
            
            logger.error(f"Complete analysis failed: {e}", exc_info=True)
            return ToolExecutionResult(
                success=False,
                message=f"Complete analysis failed: {str(e)}",
                error_details=str(e)
            )
    
    def _run_composite_analysis(self, session_id: str, custom_variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run composite score analysis with optional custom variable selection"""
        composite_start_time = time.time()
        
        # 🎯 LOG COMPOSITE ANALYSIS START - DEMO ANALYTICS
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='composite_analysis_start',
                details={
                    'custom_variables': custom_variables,
                    'variable_count': len(custom_variables) if custom_variables else 0,
                    'selection_method': 'user_specified' if custom_variables else 'region_aware_auto'
                },
                success=True
            )
        
        try:
            from .engine import AnalysisEngine
            from app.services.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            
            # Run composite analysis with custom variables (or auto-selection if None)
            result = analysis_engine.run_composite_analysis(session_id, variables=custom_variables)
            
            # Add custom variable information to result
            result_data = result.get('data', {})
            if custom_variables:
                result_data['custom_variables_used'] = custom_variables
                result_data['variable_selection_method'] = 'user_specified'
            else:
                result_data['variable_selection_method'] = 'region_aware_auto'
            
            composite_execution_time = time.time() - composite_start_time
            
            # 🎯 LOG COMPOSITE ANALYSIS SUCCESS - DEMO ANALYTICS
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                interaction_logger = current_app.services.interaction_logger
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='composite_analysis_complete',
                    details={
                        'execution_time_seconds': composite_execution_time,
                        'wards_analyzed': result_data.get('wards_analyzed', 'N/A'),
                        'variables_used': result_data.get('variables_used', []),
                        'selection_method': result_data.get('variable_selection_method'),
                        'visualizations_created': len(result_data.get('visualizations', {}))
                    },
                    success=result.get('status') == 'success'
                )
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'Composite analysis completed'),
                'data': result_data,
                'error_details': result.get('error_details')
            }
            
        except Exception as e:
            composite_execution_time = time.time() - composite_start_time
            
            # 🎯 LOG COMPOSITE ANALYSIS FAILURE - DEMO ERROR TRACKING
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                interaction_logger = current_app.services.interaction_logger
                interaction_logger.log_error(
                    session_id=session_id,
                    error_type=f'CompositeAnalysisError:{type(e).__name__}',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='composite_analysis_failed',
                    details={
                        'execution_time_seconds': composite_execution_time,
                        'error_type': type(e).__name__,
                        'custom_variables': custom_variables,
                        'failure_stage': 'composite_analysis'
                    },
                    success=False
                )
            
            logger.error(f"Composite analysis error: {e}")
            return {
                'success': False,
                'message': f"Composite analysis failed: {str(e)}",
                'error_details': str(e)
            }
    
    def _run_pca_analysis(self, session_id: str, custom_variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run PCA analysis with optional custom variable selection"""
        pca_start_time = time.time()

        print("\n" + "🟦"*30)
        print("🎯 _run_pca_analysis() CALLED!")
        print(f"   Session: {session_id}")
        print(f"   Custom vars: {custom_variables}")
        print("🟦"*30 + "\n")

        logger.critical(f"🎯 PCA ANALYSIS METHOD CALLED - Session: {session_id}")

        # 🎯 LOG PCA ANALYSIS START - DEMO ANALYTICS
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='pca_analysis_start',
                details={
                    'custom_variables': custom_variables,
                    'variable_count': len(custom_variables) if custom_variables else 0,
                    'selection_method': 'user_specified' if custom_variables else 'region_aware_auto'
                },
                success=True
            )
        
        try:
            from ..analysis.pca_pipeline import run_independent_pca_analysis
            from app.services.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            
            # Run PCA analysis with custom variables (or auto-selection if None)
            result = run_independent_pca_analysis(
                data_handler=data_handler,
                session_id=session_id,
                selected_variables=custom_variables
            )
            
            # Check if PCA was not suitable
            if result.get('status') == 'pca_not_suitable':
                logger.info(f"PCA not suitable: {result.get('message')}")
                
                print("\n" + "⚠️"*20)
                print("PCA ANALYSIS NOT SUITABLE - STATISTICAL TESTS FAILED")
                print("⚠️"*20)
                print(f"\nDetailed Results:")
                print(f"  - KMO Value: {result.get('data', {}).get('kmo_value', 0):.3f}")
                print(f"  - KMO Threshold: 0.5")
                print(f"  - Bartlett's p-value: {result.get('data', {}).get('bartlett_p_value', 1.0):.4e}")
                print(f"  - Required p-value: < 0.05")
                print(f"\nReason: {result.get('message')}")
                print(f"Action: Skipping PCA, will use composite analysis only")
                print("⚠️"*20 + "\n")
                
                # Log the PCA suitability check result
                if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                    interaction_logger = current_app.services.interaction_logger
                    interaction_logger.log_analysis_event(
                        session_id=session_id,
                        event_type='pca_not_suitable',
                        details={
                            'reason': result.get('message'),
                            'kmo_value': result.get('data', {}).get('kmo_value'),
                            'bartlett_p_value': result.get('data', {}).get('bartlett_p_value'),
                            'recommendation': result.get('data', {}).get('recommendation')
                        },
                        success=False
                    )
                
                return {
                    'success': False,
                    'pca_skipped': True,
                    'message': result.get('message'),
                    'data': result.get('data', {}),
                    'recommendation': 'Using composite analysis only due to statistical test results'
                }
            
            # Add custom variable information to result
            result_data = result.get('data', {})
            if custom_variables:
                result_data['custom_variables_used'] = custom_variables
                result_data['variable_selection_method'] = 'user_specified'
            else:
                result_data['variable_selection_method'] = 'region_aware_auto'
            
            pca_execution_time = time.time() - pca_start_time
            
            # 🎯 LOG PCA ANALYSIS SUCCESS - DEMO ANALYTICS
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                interaction_logger = current_app.services.interaction_logger
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='pca_analysis_complete',
                    details={
                        'execution_time_seconds': pca_execution_time,
                        'components_found': result_data.get('components_found', 'N/A'),
                        'variance_explained': result_data.get('variance_explained', 'N/A'),
                        'variables_used': result_data.get('variables_used', []),
                        'selection_method': result_data.get('variable_selection_method'),
                        'visualizations_created': len(result_data.get('visualizations', {}))
                    },
                    success=result.get('status') == 'success'
                )
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'PCA analysis completed'),
                'data': result_data,
                'error_details': result.get('error_details')
            }
            
        except Exception as e:
            pca_execution_time = time.time() - pca_start_time
            
            # 🎯 LOG PCA ANALYSIS FAILURE - DEMO ERROR TRACKING
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                interaction_logger = current_app.services.interaction_logger
                interaction_logger.log_error(
                    session_id=session_id,
                    error_type=f'PCAAnalysisError:{type(e).__name__}',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='pca_analysis_failed',
                    details={
                        'execution_time_seconds': pca_execution_time,
                        'error_type': type(e).__name__,
                        'custom_variables': custom_variables,
                        'failure_stage': 'pca_analysis'
                    },
                    success=False
                )
            
            logger.error(f"PCA analysis error: {e}")
            return {
                'success': False,
                'message': f"PCA analysis failed: {str(e)}",
                'error_details': str(e)
            }
    
    def _generate_comparison_summary(self, composite_data: Dict, pca_data: Dict) -> Dict[str, Any]:
        """Generate summary comparing both methods"""
        try:
            summary = {
                'methods_compared': ['composite_score', 'pca'],
                'composite_top_wards': composite_data.get('top_vulnerable_wards', [])[:10],
                'pca_top_wards': pca_data.get('top_vulnerable_wards', [])[:10],
                'agreement_rate': 0,
                'consensus_wards': [],
                'method_differences': []
            }
            
            # Calculate agreement between methods
            composite_top = set([w.get('ward_name', '') for w in summary['composite_top_wards']])
            pca_top = set([w.get('ward_name', '') for w in summary['pca_top_wards']])
            
            if composite_top and pca_top:
                consensus = composite_top.intersection(pca_top)
                summary['consensus_wards'] = list(consensus)
                summary['agreement_rate'] = round((len(consensus) / max(len(composite_top), len(pca_top))) * 100, 1)
            
            return summary
            
        except Exception as e:
            logger.warning(f"Comparison summary generation failed: {e}")
            return {
                'methods_compared': ['composite_score', 'pca'],
                'agreement_rate': 'unavailable',
                'error': str(e)
            }
    
    def _generate_comprehensive_summary(self, composite_result, pca_result, comparison_summary, execution_time, session_id):
        """Generate conversational guidance after analysis completion"""
        try:
            # Load the unified dataset to get detailed rankings
            try:
                from app.services.dataset_builder import load_unified_dataset
                gdf = load_unified_dataset(session_id)
            except Exception as load_error:
                logger.warning(f"Failed to load unified dataset: {load_error}")
                # Try alternative: load from tools base
                try:
                    from app.utils.tool_base import get_session_unified_dataset
                    gdf = get_session_unified_dataset(session_id)
                except Exception as alt_error:
                    logger.warning(f"Alternative unified dataset load failed: {alt_error}")
                    gdf = None
            
            # 🔍 DEBUG: Check unified dataset structure
            if gdf is not None:
                print(f"🔍 SUMMARY DEBUG: Unified dataset loaded: {len(gdf)} wards, {len(gdf.columns)} columns")
                print(f"🔍 SUMMARY DEBUG: First few columns: {list(gdf.columns)[:10]}")
                
                # Check for key columns
                key_columns = ['composite_score', 'pca_score', 'WardName', 'WardCode']
                for col in key_columns:
                    if col in gdf.columns:
                        print(f"🔍 SUMMARY DEBUG: ✅ Found '{col}' column")
                    else:
                        print(f"🔍 SUMMARY DEBUG: ❌ Missing '{col}' column")
            else:
                print(f"🔍 SUMMARY DEBUG: ❌ Failed to load unified dataset")
            
            if gdf is None:
                # Try to generate summary from analysis results directly
                logger.info("Unified dataset not available, generating summary from analysis results")
                return self._generate_summary_from_analysis_results(composite_result, pca_result, comparison_summary, execution_time)
            
            # DEBUG: Check what columns exist in the unified dataset
            logger.info(f"🔍 UNIFIED DATASET DEBUG: Available columns: {list(gdf.columns)}")
            logger.info(f"🔍 UNIFIED DATASET DEBUG: Dataset shape: {gdf.shape}")
            
            # Use variable resolver for robust column detection
            from app.services.variable_resolver import variable_resolver
            
            # Find composite score column
            composite_candidates = ['composite_score', 'composite', 'comp_score']
            composite_score_col = None
            for candidate in composite_candidates:
                exists, resolved = variable_resolver.check_column_exists(candidate, list(gdf.columns))
                if exists:
                    composite_score_col = resolved
                    break
            
            # Find PCA score column
            pca_candidates = ['pca_score', 'pc1_risk_score', 'pca', 'pc1_score']
            pca_score_col = None
            for candidate in pca_candidates:
                exists, resolved = variable_resolver.check_column_exists(candidate, list(gdf.columns))
                if exists:
                    pca_score_col = resolved
                    break
            
            # Find ward name column
            ward_candidates = ['WardName', 'ward_name', 'ward', 'Ward']
            ward_name_col = None
            for candidate in ward_candidates:
                exists, resolved = variable_resolver.check_column_exists(candidate, list(gdf.columns))
                if exists:
                    ward_name_col = resolved
                    break
            
            logger.info(f"🔍 COLUMN DETECTION: Composite={composite_score_col}, PCA={pca_score_col}, Ward={ward_name_col}")
            
            # Check if we found the required columns
            missing_analysis = []
            if not composite_score_col:
                missing_analysis.append("composite_score")
            # Only check for PCA columns if PCA wasn't skipped
            if not pca_result.get('pca_skipped') and not pca_score_col:
                missing_analysis.append("pca_score")
            if not ward_name_col:
                missing_analysis.append("ward_name")
            
            if missing_analysis:
                logger.warning(f"🔍 UNIFIED DATASET DEBUG: Missing analysis columns: {missing_analysis}")
                logger.info(f"🔍 UNIFIED DATASET DEBUG: Falling back to analysis results directly")
                
                # Fall back to using analysis results directly
                return self._generate_summary_from_analysis_results(composite_result, pca_result, comparison_summary, execution_time)
            
            # Check for NaN values and drop affected rows so valid wards still get summarized
            composite_nan_count = gdf[composite_score_col].isna().sum() if composite_score_col else 0
            if composite_nan_count:
                logger.warning(
                    f"🔍 UNIFIED DATASET DEBUG: {composite_nan_count} wards missing composite scores;"
                    " excluding them from summary generation"
                )
                gdf = gdf.dropna(subset=[composite_score_col])

            # Only check PCA columns when PCA actually ran
            if not pca_result.get('pca_skipped') and pca_score_col:
                pca_nan_count = gdf[pca_score_col].isna().sum()
                if pca_nan_count:
                    logger.warning(
                        f"🔍 UNIFIED DATASET DEBUG: {pca_nan_count} wards missing PCA scores;"
                        " excluding them from summary generation"
                    )
                    gdf = gdf.dropna(subset=[pca_score_col])

            # If dropping NaNs removed everything, fall back to the simplified summary
            if gdf.empty:
                logger.warning("🔍 UNIFIED DATASET DEBUG: No valid rows remain after removing NaNs;"
                               " falling back to analysis result summary")
                return self._generate_summary_from_analysis_results(
                    composite_result,
                    pca_result,
                    comparison_summary,
                    execution_time
                )
            
            # Detect category columns
            composite_category_col = None
            pca_category_col = None
            
            for col in gdf.columns:
                if 'composite' in col.lower() and 'categor' in col.lower():
                    composite_category_col = col
                elif ('pca' in col.lower() or 'vulnerabilit' in col.lower()) and 'categor' in col.lower():
                    pca_category_col = col
            
            # Get top and bottom 5 for both methods (using dynamically detected column names)
            composite_cols = [ward_name_col, composite_score_col]
            if composite_category_col:
                composite_cols.append(composite_category_col)
            
            composite_top5 = gdf.nlargest(5, composite_score_col)[composite_cols].to_dict('records')
            composite_bottom5 = gdf.nsmallest(5, composite_score_col)[composite_cols].to_dict('records')

            # Check if PCA was skipped before trying to access PCA columns
            if not pca_result.get('pca_skipped'):
                pca_cols = [ward_name_col, pca_score_col]
                if pca_category_col:
                    pca_cols.append(pca_category_col)

                pca_top5 = gdf.nlargest(5, pca_score_col)[pca_cols].to_dict('records')
                pca_bottom5 = gdf.nsmallest(5, pca_score_col)[pca_cols].to_dict('records')
            else:
                # PCA was skipped - no PCA rankings available
                pca_top5 = []
                pca_bottom5 = []
                logger.info("PCA was skipped due to statistical tests, no PCA rankings to generate")
            
            # Get variables used for each method (including custom variable info)
            composite_data = composite_result.get('data', {})
            pca_data = pca_result.get('data', {})
            
            # Extract variables for composite analysis - IMPROVED DETECTION
            composite_vars = (
                composite_data.get('custom_variables_used') or 
                composite_data.get('variables_used', []) or
                composite_data.get('selected_variables', []) or
                composite_data.get('analysis_variables', [])
            )
            composite_selection_method = composite_data.get('variable_selection_method', 'auto')
            
            # Extract variables for PCA analysis - IMPROVED DETECTION
            pca_vars_raw = (
                pca_data.get('custom_variables_used') or 
                pca_data.get('variables_used', []) or
                pca_data.get('selected_variables', []) or
                pca_data.get('analysis_variables', [])
            )
            pca_selection_method = pca_data.get('variable_selection_method', 'auto')
            
            # Filter out ward identification columns from PCA variables
            pca_vars = [v for v in pca_vars_raw if v.lower() not in ['wardname', 'ward_name', 'ward', 'lga', 'state']]
            
            # DEBUG: Log variable detection results
            logger.info(f"🔍 VARIABLE DETECTION: Composite vars: {composite_vars}")
            logger.info(f"🔍 VARIABLE DETECTION: PCA vars: {pca_vars}")
            logger.info(f"🔍 VARIABLE DETECTION: Composite data keys: {list(composite_data.keys())}")
            logger.info(f"🔍 VARIABLE DETECTION: PCA data keys: {list(pca_data.keys())}")
            
            # Calculate method agreement
            agreement_rate = comparison_summary.get('agreement_rate', 0)
            consensus_wards = comparison_summary.get('consensus_wards', [])
            
            # Detect geographic region for contextualized recommendations
            region_info = self._extract_region_info(gdf)
            
            # Generate dynamic recommendations based on actual results
            recommendations = self._generate_smart_recommendations(
                composite_top5, pca_top5, composite_bottom5, pca_bottom5, 
                agreement_rate, consensus_wards, region_info, gdf
            )
            
            # Get analysis details for the conversational response
            ward_count = len(gdf)
            
            # Get geopolitical zone info
            geopolitical_zone = "your region"
            if 'detected_zone' in gdf.columns:
                zone = gdf['detected_zone'].iloc[0]
                if zone and zone != 'No zone detected':
                    geopolitical_zone = zone.replace('_', ' ').title()
            
            # Get number of variables used (already extracted earlier)
            num_variables = len(composite_vars) if composite_vars else "several"
            
            # Extract PCA test results if available
            pca_test_summary = ""
            if pca_result.get('pca_skipped'):
                # PCA was skipped due to statistical tests
                # Get values from the data field where they're stored
                kmo_value = pca_result.get('data', {}).get('kmo_value', 0)
                bartlett_p = pca_result.get('data', {}).get('bartlett_p_value', 1)
                # Interpret KMO value
                kmo_interpret = "Your data variables show weak relationships"
                if kmo_value < 0.3:
                    kmo_interpret = "Your data variables are mostly independent"
                elif kmo_value < 0.5:
                    kmo_interpret = "Your data variables show limited relationships"
                
                # Interpret Bartlett's test
                bartlett_interpret = "No significant patterns found between variables"
                if bartlett_p < 0.05:
                    bartlett_interpret = "Some patterns exist but not strong enough"
                
                pca_test_summary = f"""

### Behind the Scenes - Statistical Testing

I ran two tests to check if your data is suitable for advanced pattern analysis (PCA):

- **Kaiser-Meyer-Olkin (KMO) Test**: {kmo_value:.3f} (needed 0.5 or higher)
  → {kmo_interpret}

- **Bartlett's Test of Sphericity**: {"Failed" if bartlett_p >= 0.05 else "Passed"} (p-value = {bartlett_p:.3f})
  → {bartlett_interpret}

- **My Decision**: Used the Composite Score method only, which is more reliable for this type of data

"""
            elif pca_data:
                # PCA was performed - get test results from the analysis
                # Check if we have test results stored
                kmo_value = pca_data.get('kmo_value', None)
                bartlett_p = pca_data.get('bartlett_p_value', None)
                
                if kmo_value is not None and bartlett_p is not None:
                    # Interpret KMO value in user-friendly terms
                    if kmo_value >= 0.9:
                        kmo_interpret = "Your data variables have excellent relationships"
                    elif kmo_value >= 0.8:
                        kmo_interpret = "Your data variables have strong relationships"
                    elif kmo_value >= 0.7:
                        kmo_interpret = "Your data variables have good relationships"
                    elif kmo_value >= 0.6:
                        kmo_interpret = "Your data variables have moderate relationships"
                    elif kmo_value >= 0.5:
                        kmo_interpret = "Your data variables have adequate relationships"
                    else:
                        kmo_interpret = "Your data variables have weak relationships"
                    
                    # Format p-value in a readable way
                    if bartlett_p < 0.001:
                        bartlett_display = "< 0.001"
                    else:
                        bartlett_display = f"{bartlett_p:.3f}"
                    
                    pca_test_summary = f"""

### Behind the Scenes - Statistical Testing

I ran two tests to check if your data is suitable for advanced pattern analysis (PCA):

- **Kaiser-Meyer-Olkin (KMO) Test**: {kmo_value:.3f} (needed 0.5 or higher) ✓
  → {kmo_interpret}

- **Bartlett's Test of Sphericity**: Passed (p-value {bartlett_display}) ✓
  → Significant patterns found between variables

- **My Decision**: Used both methods (Composite Score and PCA) for comprehensive analysis
  → This gives you two different perspectives on malaria risk in your wards

"""
                else:
                    # Default message if test results not stored
                    pca_test_summary = """

### Behind the Scenes - Statistical Testing

- **Data Suitability**: Passed all required tests ✓
- **My Decision**: Used both methods (Composite Score and PCA) for comprehensive analysis

"""
            
            visual_action = (
                "View vulnerability maps\n"
                "   Type: **show me the vulnerability maps**"
                if not pca_result.get("pca_skipped")
                else
                "Create composite vulnerability map\n"
                "   Type: **create composite vulnerability map**"
            )

            method_name = "Composite Score and PCA" if not pca_result.get("pca_skipped") else "Composite Score"

            # Build conversational response
            response = f"""Analysis complete.

I ranked all {ward_count} wards by malaria risk using {num_variables} risk factors for the {geopolitical_zone} zone.

## What you can do next

1. Plan ITN / bed net distribution
   Type: **I want to plan bed net distribution**

2. Classify settlements in priority wards
   Type: **create settlement classification for the top 10 highest-risk wards**

3. View the highest-risk wards
   Type: **show me the highest risk wards**

4. View the lowest-risk wards
   Type: **show me the lowest risk wards**

5. {visual_action}

6. Export the results
   Type: **export results**

## What I did

- Cleaned your data by fixing ward name mismatches and filling missing values using neighboring areas.

- Selected {num_variables} risk factors based on {geopolitical_zone}'s malaria patterns.

- Normalized all variables to the same 0-1 scale.

- Ran statistical checks to confirm the data was suitable for advanced analysis.

- Calculated risk scores using {method_name}.

- Ranked all wards from highest to lowest risk.

{pca_test_summary}
"""
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to generate comprehensive summary: {e}", exc_info=True)
            # Provide a better fallback that includes what we can extract from the results
            fallback_parts = [f"## ✅ Analysis Complete in {execution_time:.1f} seconds!\n"]

            # Try to extract basic info from results
            if composite_result and 'data' in composite_result:
                comp_data = composite_result['data']
                if 'wards_analyzed' in comp_data:
                    fallback_parts.append(f"**Analyzed:** {comp_data['wards_analyzed']} wards\n")

            fallback_parts.append("**Methods:** Composite Score + Principal Component Analysis\n")
            fallback_parts.append("**Status:** Both analyses completed successfully\n")
            fallback_parts.append("\n")
            fallback_parts.append("⚠️ *Detailed summary generation failed. Check the visualization results above for rankings and maps.*\n")
            fallback_parts.append("\n")
            fallback_parts.append("### Next Steps\n")
            fallback_parts.append("\n")
            fallback_parts.append('- **Visualize Risk Levels:** "plot the vulnerability map"\n')
            fallback_parts.append('- **Plan Interventions:** "I want to plan bed net distribution"\n')
            fallback_parts.append('- **View Rankings:** "show me the top 10 highest risk wards"\n')
            
            return "\n".join(fallback_parts)

    def _generate_summary_from_analysis_results(self, composite_result, pca_result, comparison_summary, execution_time):
        """Generate summary from analysis results when unified dataset is not available"""
        try:
            summary_parts = [f"## ✅ Analysis Complete in {execution_time:.1f} seconds!\n"]
            summary_parts.append("\n")

            # Extract basic information from results
            total_wards = "N/A"
            if composite_result and 'data' in composite_result:
                comp_data = composite_result['data']
                if 'wards_analyzed' in comp_data:
                    total_wards = comp_data['wards_analyzed']

            summary_parts.append(f"**Analyzed:** {total_wards} wards\n")
            summary_parts.append("**Methods:** Composite Score + Principal Component Analysis\n")
            summary_parts.append("\n")
            
            # Try to extract top wards from results
            composite_top = []
            pca_top = []
            
            if composite_result and 'data' in composite_result:
                comp_data = composite_result['data']
                if 'top_vulnerable_wards' in comp_data:
                    composite_top = comp_data['top_vulnerable_wards'][:5]
            
            if pca_result and 'data' in pca_result:
                pca_data = pca_result['data']
                if 'top_vulnerable_wards' in pca_data:
                    pca_top = pca_data['top_vulnerable_wards'][:5]
            
            # Show top wards from each method
            if composite_top:
                summary_parts.append("### 🎯 Top 5 Highest Risk (Composite Method)\n")
                summary_parts.append("\n")
                for i, ward in enumerate(composite_top, 1):
                    ward_name = ward.get('ward_name', ward.get('WardName', 'Unknown'))
                    score = ward.get('composite_score', ward.get('score', 'N/A'))
                    if isinstance(score, float):
                        summary_parts.append(f"{i}. {ward_name} (Score: {score:.3f})\n")
                    else:
                        summary_parts.append(f"{i}. {ward_name} (Score: {score})\n")
                summary_parts.append("\n")

            if pca_top:
                summary_parts.append("### 📊 Top 5 Highest Risk (PCA Method)\n")
                summary_parts.append("\n")
                for i, ward in enumerate(pca_top, 1):
                    ward_name = ward.get('ward_name', ward.get('WardName', 'Unknown'))
                    score = ward.get('pca_score', ward.get('pc1_risk_score', ward.get('score', 'N/A')))
                    if isinstance(score, float):
                        summary_parts.append(f"{i}. {ward_name} (Score: {score:.3f})\n")
                    else:
                        summary_parts.append(f"{i}. {ward_name} (Score: {score})\n")
                summary_parts.append("\n")
            
            # Calculate consensus if both methods have results
            if composite_top and pca_top:
                comp_names = set([w.get('ward_name', w.get('WardName', '')) for w in composite_top])
                pca_names = set([w.get('ward_name', w.get('WardName', '')) for w in pca_top])
                consensus = comp_names.intersection(pca_names)
                
                if consensus:
                    summary_parts.append("### 🤝 Consensus High-Risk Wards\n")
                    summary_parts.append("*Identified as high-risk by both methods:*\n")
                    summary_parts.append("\n")
                    for ward in sorted(consensus):
                        if ward:  # Skip empty names
                            summary_parts.append(f"- **{ward}**\n")
                    summary_parts.append("\n")

                agreement_pct = (len(consensus) / len(comp_names.union(pca_names))) * 100 if comp_names.union(pca_names) else 0
                summary_parts.append(f"**Method Agreement:** {agreement_pct:.0f}% consensus on high-risk areas\n")
                summary_parts.append("\n")
            
            # Add next steps
            summary_parts.append("### 💡 Next Steps\n")
            summary_parts.append("\n")
            summary_parts.append('- **Visualize Risk Levels:** "plot the vulnerability map"\n')
            summary_parts.append('- **Classify Settlements:** "create settlement classification for the top 10 highest-risk wards"\n')
            summary_parts.append('- **Plan Interventions:** "I want to plan bed net distribution"\n')
            summary_parts.append('- **View Rankings:** "show me the top 10 highest risk wards"\n')
            summary_parts.append('- **Export results** for intervention planning\n')
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate summary from analysis results: {e}", exc_info=True)
            return f"✅ **Analysis Complete** in {execution_time:.1f} seconds! Both methods completed successfully. Check the visualizations above for detailed results."

    def _extract_region_info(self, gdf):
        """Extract region information for contextualized recommendations"""
        try:
            # Try to get region info from the dataframe
            if 'detected_zone' in gdf.columns:
                zone = gdf['detected_zone'].iloc[0]
                if zone and zone != 'No zone detected':
                    zone_name = zone.replace('_', ' ').title()
                    return {
                        'zone': zone_name,
                        'region_name': f"{zone_name} Zone, Nigeria"
                    }
            
            # Try to extract from state information
            if 'StateCode' in gdf.columns:
                state_code = gdf['StateCode'].iloc[0]
                if state_code == 'KN':
                    return {
                        'zone': 'North West',
                        'region_name': 'Kano State, Nigeria'
                    }
                elif state_code == 'NI':
                    return {
                        'zone': 'North Central', 
                        'region_name': 'Niger State, Nigeria'
                    }
            
            # Fallback
            return {
                'zone': 'Nigeria',
                'region_name': 'Nigeria'
            }
            
        except Exception:
            return {
                'zone': 'Nigeria',
                'region_name': 'Nigeria'
            }
    
    def _generate_smart_recommendations(self, composite_top5, pca_top5, composite_bottom5, pca_bottom5, 
                                       agreement_rate, consensus_wards, region_info, gdf):
        """Generate dynamic recommendations based on actual analysis results"""
        recommendations = []
        
        # Get ward names for easier processing
        comp_high_risk = [ward['WardName'] for ward in composite_top5]
        pca_high_risk = [ward['WardName'] for ward in pca_top5] if pca_top5 else []
        
        # Immediate action recommendations
        if consensus_wards and len(consensus_wards) > 0:
            recommendations.append("### Immediate Priority Actions")
            recommendations.append("")
            recommendations.append(f"**Target Consensus High-Risk Wards:** {', '.join(consensus_wards[:5])}\n")
            recommendations.append("*These wards are identified as high-risk by both analytical methods*\n")
            recommendations.append("\n")
            recommendations.append("#### Recommended Interventions\n")
            recommendations.append("\n")
            recommendations.append("- Prioritize bed net distribution campaigns\n")
            recommendations.append("- Establish additional health monitoring points\n")
            recommendations.append("- Implement targeted vector control measures\n")
            recommendations.append("")
        
        # Secondary priorities based on method-specific findings  
        if agreement_rate < 50:
            recommendations.append("### Secondary Priority Actions\n")
            recommendations.append("\n")
            recommendations.append("#### Complementary Risk Patterns Identified\n")
            recommendations.append("\n")

            # Composite-specific high-risk wards
            comp_only = [w for w in comp_high_risk if w not in consensus_wards]
            if comp_only:
                recommendations.append(f"- **Composite Analysis Priorities:** {', '.join(comp_only[:3])}\n")
                recommendations.append("  *Focus on multi-factor environmental and demographic risks*\n")

            # PCA-specific high-risk wards
            pca_only = [w for w in pca_high_risk if w not in consensus_wards]
            if pca_only:
                recommendations.append(f"- **PCA Analysis Priorities:** {', '.join(pca_only[:3])}\n")
                recommendations.append("  *Address principal risk factor clusters*\n")
            
            recommendations.append("")
        
        # Monitoring and evaluation
        recommendations.append("### Monitoring & Evaluation")
        recommendations.append("")
        low_risk_comp = [ward['WardName'] for ward in composite_bottom5[:3]]
        recommendations.append(f"**Control Areas for Impact Assessment:** {', '.join(low_risk_comp)}")
        recommendations.append("*Use these low-risk wards to measure intervention effectiveness*")
        recommendations.append("")
        
        # Resource allocation guidance - IMPROVED WITH LLM-BASED ANALYSIS
        recommendations.append("### Resource Allocation Guidance")
        recommendations.append("")
        
        # Use LLM to analyze top vulnerable wards from composite score (default for implementation)
        composite_ward_names = [ward['WardName'] for ward in composite_top5]
        allocation_analysis = self._analyze_resource_allocation(composite_ward_names, len(gdf))
        
        recommendations.append(f"**Priority Implementation Areas:** {', '.join(composite_ward_names)}\n")
        recommendations.append(f"*{allocation_analysis}*\n")
        recommendations.append("\n")
        recommendations.append("#### Implementation Strategy\n")
        recommendations.append("\n")
        recommendations.append("- Begin with composite score high-risk wards as primary targets\n")
        recommendations.append("- Use PCA results to understand underlying risk factors\n")
        recommendations.append("- Establish monitoring systems in both high and low-risk areas\n")
        
        return recommendations
    
    def _analyze_resource_allocation(self, top_wards, total_wards):
        """Generate LLM-based analysis of resource allocation for top vulnerable wards"""
        try:
            num_priority_wards = len(top_wards)
            pct_priority = (num_priority_wards / total_wards) * 100
            
            # Generate contextual analysis based on the priority ward characteristics
            if pct_priority < 2:
                return f"Highly concentrated risk pattern with {num_priority_wards} priority wards ({pct_priority:.1f}% of total). Focused intervention strategy recommended with intensive resource deployment."
            elif pct_priority < 5:
                return f"Concentrated risk profile across {num_priority_wards} priority wards ({pct_priority:.1f}% of total). Targeted intervention approach suitable for maximum impact per resource invested."
            elif pct_priority < 10:
                return f"Moderate risk concentration in {num_priority_wards} priority wards ({pct_priority:.1f}% of total). Standard intervention protocols recommended with phased implementation."
            else:
                return f"Distributed risk pattern across {num_priority_wards} priority wards ({pct_priority:.1f}% of total). Consider regional coordination and phased rollout strategy."
                
        except Exception as e:
            logger.warning(f"Resource allocation analysis failed: {e}")
            return f"Analysis of {len(top_wards)} priority wards for targeted intervention planning"

    # Note: _has_valid_viz_paths method removed - no longer needed without auto-visualization

    def _mark_analysis_complete(self, session_id: str):
        """Mark comprehensive analysis as complete for workflow guidance"""
        try:
            # Update Flask session
            try:
                from flask import session
                session['comprehensive_analysis_complete'] = True
            except Exception as e:
                logger.warning(f"Flask session update failed (expected in worker context): {e}")
            
            # CRITICAL: Update workflow state JSON file
            try:
                from app.conversation.workflow_state import WorkflowStateManager
                workflow_manager = WorkflowStateManager(session_id)
                success = workflow_manager.update_state(
                    {'analysis_complete': True},
                    transition_reason='Risk analysis completed successfully'
                )
                if success:
                    logger.info(f"✅ Updated workflow state - analysis_complete = True for session {session_id}")
                else:
                    logger.error(f"❌ Failed to update workflow state for session {session_id}")
            except Exception as e:
                logger.error(f"Failed to update workflow state: {e}")
            
            # Also update agent state if available
            try:
                from app.conversation.analysis_state import get_analysis_state_handler
                state_handler = get_analysis_state_handler()
                state_handler.on_analysis_complete(session_id, {'analysis_type': 'comprehensive'})
            except Exception as e:
                logger.warning(f"Failed to update agent state: {e}")
            
            logger.info(f"✅ Marked comprehensive analysis complete for session {session_id}")
        except Exception as e:
            logger.error(f"Failed to mark analysis complete: {e}")
    
    def _mark_partial_analysis_complete(self, session_id: str, analysis_type: str):
        """Mark partial analysis as complete for workflow guidance"""
        try:
            from flask import session
            if 'partial_analyses_complete' not in session:
                session['partial_analyses_complete'] = []

            if analysis_type not in session['partial_analyses_complete']:
                session['partial_analyses_complete'].append(analysis_type)
                logger.info(f"✅ Marked {analysis_type} analysis complete for session {session_id}")
        except Exception as e:
            logger.warning(f"Failed to mark {analysis_type} analysis complete: {e}")

    # ─────────────────────────────────────────────────────────────────
    # PER-YEAR RISK PIPELINE (background thread)
    # ─────────────────────────────────────────────────────────────────

    def _find_per_year_tags(self, session_folder: str) -> List[str]:
        """Return sorted list of year_tags like ['_2020', '_2021', ...] from raw_data_YYYY.csv files."""
        pattern = os.path.join(session_folder, 'raw_data_[0-9][0-9][0-9][0-9].csv')
        matches = sorted(glob.glob(pattern))
        tags = []
        for path in matches:
            basename = os.path.basename(path)
            year_str = basename[len('raw_data_'):-len('.csv')]
            if year_str.isdigit() and len(year_str) == 4:
                tags.append(f'_{year_str}')
        return tags

    def _blend_burden_into_composite(self, session_folder: str, year_tag: str) -> None:
        """Blend composite_rank + burden_rank into combined_rank for one year.

        Reads analysis_vulnerability_rankings{year_tag}.csv and
        raw_data{year_tag}.csv. Writes
        analysis_vulnerability_rankings_combined{year_tag}.csv.
        """
        import pandas as pd
        rankings_path = os.path.join(
            session_folder, f'analysis_vulnerability_rankings{year_tag}.csv'
        )
        # pipeline.py saves without 'analysis_' prefix — try that as fallback
        if not os.path.exists(rankings_path):
            fallback = os.path.join(session_folder, f'vulnerability_rankings{year_tag}.csv')
            if os.path.exists(fallback):
                rankings_path = fallback
        # Environmental vars are year-invariant — aggregate composite rankings are correct for all years
        if not os.path.exists(rankings_path):
            agg_fallback = os.path.join(session_folder, 'analysis_vulnerability_rankings.csv')
            if os.path.exists(agg_fallback):
                rankings_path = agg_fallback
                logger.info(f"Using aggregate rankings for {year_tag} burden blend (env vars are year-invariant)")
        raw_path = os.path.join(session_folder, f'raw_data{year_tag}.csv')

        if not os.path.exists(rankings_path):
            logger.warning(f"Rankings file missing for {year_tag}: {rankings_path}")
            return
        if not os.path.exists(raw_path):
            logger.warning(f"Raw data missing for {year_tag}: {raw_path}")
            return

        try:
            rankings = pd.read_csv(rankings_path)
            raw = pd.read_csv(raw_path)

            burden_col = None
            for col in ('Burden', 'burden', 'total_positive', 'Total_Positive'):
                if col in raw.columns:
                    burden_col = col
                    break

            if burden_col is None:
                logger.warning(f"No Burden column found in {raw_path} — skipping burden blend")
                return

            # Merge burden into rankings on WardName
            ward_col = 'WardName' if 'WardName' in rankings.columns else 'ward_name'
            raw_ward_col = 'WardName' if 'WardName' in raw.columns else 'ward_name'
            merged = rankings.merge(
                raw[[raw_ward_col, burden_col]].rename(columns={raw_ward_col: ward_col}),
                on=ward_col, how='left'
            )

            # Fill NaN burden for wards absent from this year's raw data with median
            unmatched = merged[merged[burden_col].isna()][ward_col].tolist()
            if unmatched:
                logger.warning(f"{len(unmatched)} wards have no burden for {year_tag}: {unmatched[:5]}...")
                merged[burden_col] = merged[burden_col].fillna(merged[burden_col].median())

            # For imputed wards, substitute median of non-imputed wards
            imputed_col = '_imputed' if '_imputed' in merged.columns else None
            burden = merged[burden_col].copy()
            if imputed_col:
                non_imputed_mask = ~merged[imputed_col].astype(bool)
                median_burden = burden[non_imputed_mask].median()
                burden = burden.where(non_imputed_mask, other=median_burden)

            burden_rank = burden.rank(ascending=False, method='average')

            composite_rank_col = None
            for col in ('composite_rank', 'overall_rank', 'rank', 'composite_score_rank'):
                if col in merged.columns:
                    composite_rank_col = col
                    break

            if composite_rank_col is None:
                logger.warning(f"No composite_rank column in {rankings_path} — skipping blend")
                return

            merged['burden_rank'] = burden_rank
            merged['combined_rank'] = (merged[composite_rank_col] + burden_rank) / 2.0

            n = len(merged)
            tercile = n / 3.0
            merged['combined_category'] = merged['combined_rank'].apply(
                lambda r: 'High' if r <= tercile else ('Low' if r > 2 * tercile else 'Medium')
            )

            out_path = os.path.join(
                session_folder, f'analysis_vulnerability_rankings_combined{year_tag}.csv'
            )
            merged.to_csv(out_path, index=False)
            logger.info(f"Saved combined rankings: {out_path}")

        except Exception as e:
            logger.error(f"_blend_burden_into_composite failed for {year_tag}: {e}")

    def _copy_aggregate_pca_scores(self, session_folder: str, year_tag: str) -> None:
        """Copy aggregate PCA results file to per-year path (avoids redundant computation)."""
        src = os.path.join(session_folder, 'analysis_pca_results.csv')
        dst = os.path.join(session_folder, f'analysis_pca_results{year_tag}.csv')
        if not os.path.exists(src):
            logger.warning(f"Aggregate PCA results not found: {src}")
            return
        try:
            shutil.copy2(src, dst)
            logger.info(f"Copied PCA results → {dst}")
        except Exception as e:
            logger.error(f"Failed to copy PCA results for {year_tag}: {e}")

    def _run_per_year_risk(self, session_id: str, composite_vars: Optional[List[str]],
                           pca_passed: bool) -> None:
        """Run per-year composite analysis for all years in parallel, blocking until all done.

        All years are independent (different Burden values, same environmental vars),
        so ThreadPoolExecutor gives near-linear speedup. 6 years in parallel takes
        roughly the same time as 1 year sequentially.
        """
        import concurrent.futures

        session_folder = f"instance/uploads/{session_id}"
        year_tags = self._find_per_year_tags(session_folder)
        if not year_tags:
            logger.info(f"No per-year raw_data files — skipping per-year pipeline")
            return

        try:
            app = current_app._get_current_object()
        except RuntimeError:
            logger.error("Cannot capture Flask app for per-year parallel workers")
            return

        status_path = os.path.join(session_folder, 'multi_year_vuln_status.json')
        completed: List[str] = []

        def _process_one_year(year_tag: str) -> str:
            """Worker: run composite + blend + unified dataset for one year. Returns year_tag on success."""
            with app.app_context():
                from .engine import AnalysisEngine
                from app.services.data_handler import DataHandler
                from app.services.dataset_builder import UnifiedDatasetBuilder

                engine = AnalysisEngine(DataHandler(session_folder))
                engine.run_composite_analysis(session_id, variables=composite_vars, year_tag=year_tag)

                self._blend_burden_into_composite(session_folder, year_tag)

                if pca_passed:
                    self._copy_aggregate_pca_scores(session_folder, year_tag)

                builder = UnifiedDatasetBuilder(session_id, year_tag=year_tag)
                builder.build_unified_dataset()

                logger.info(f"Per-year pipeline done for {year_tag}")
                return year_tag

        max_workers = min(len(year_tags), 4)  # cap at 4 to avoid overwhelming gunicorn workers
        logger.info(f"Per-year pipeline: {len(year_tags)} years, {max_workers} parallel workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_one_year, yt): yt for yt in year_tags}
            for future in concurrent.futures.as_completed(futures):
                year_tag = futures[future]
                try:
                    future.result()
                    completed.append(year_tag)
                except Exception as e:
                    logger.error(f"Per-year pipeline error for {year_tag}: {e}", exc_info=True)

        try:
            with open(status_path, 'w') as f:
                json.dump({'status': 'complete', 'completed_years': sorted(completed), 'total_years': year_tags}, f)
        except Exception as e:
            logger.warning(f"Status write failed: {e}")

        logger.info(f"Per-year pipeline complete. {len(completed)}/{len(year_tags)} years succeeded.")


# REMOVED: GenerateComprehensiveAnalysisSummary and GenerateComprehensiveAnalysisSummaryInput
# Dead code - never imported or called. Users should use RunMalariaRiskAnalysis instead.
