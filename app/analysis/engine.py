"""
Analysis Engine for ChatMRPT

Simple wrapper class that provides a consistent interface for analysis operations.
Supports standalone composite scoring and PCA analysis workflows.
Settlement integration is handled separately as per workflow diagram.
"""

import logging
from typing import Dict, Any, Optional, List
from .pipeline import run_full_analysis_pipeline
from .pca_pipeline import run_independent_pca_analysis
from .metadata import AnalysisMetadata

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """
    Main analysis engine that coordinates different analysis workflows.
    
    Provides standalone composite scoring and PCA analysis without automatic settlement integration.
    Settlement analysis is now handled separately as per the updated workflow diagram.
    """
    
    def __init__(self, data_handler=None):
        """Initialize the analysis engine."""
        self.logger = logger
        self.data_handler = data_handler
        
    def run_composite_analysis(self, session_id: str, variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run standalone composite scoring analysis.
        
        Settlement integration is now handled separately as per workflow diagram.
        
        Args:
            variables: Optional list of variables to use (auto-selected if None)
            
        Returns:
            Dictionary with composite analysis results
        """
        try:
            logger.info("📊 Starting standalone composite analysis (settlement integration removed)")
            
            # Run composite analysis without settlement integration
            result = run_full_analysis_pipeline(
                data_handler=self.data_handler,
                selected_variables=variables,
                session_id=session_id
            )
            
            if result.get('status') == 'error':
                return result
            
            return {
                'status': 'success',
                'message': 'Composite analysis completed successfully.',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'composite_scoring',
                'data': {
                    'variables_used': result.get('variables_used', []),
                    'variable_selection_method': result.get('selection_method', 'auto'),
                    'wards_analyzed': len(result.get('vulnerability_rankings', [])) if result.get('vulnerability_rankings') is not None else 0,
                    'results': result
                },
                'results': result
            }
            
        except Exception as e:
            logger.error(f"Composite analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Composite analysis failed: {str(e)}',
                'variables_used': variables or [],
                'analysis_type': 'composite_scoring',
                'data': {
                    'variables_used': variables or [],
                    'variable_selection_method': 'failed',
                    'error': str(e)
                }
            }
    
    def run_pca_analysis(self, session_id: str, variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run standalone PCA analysis.
        
        Settlement integration is now handled separately as per workflow diagram.
        
        Args:
            variables: Optional list of variables to use
            
        Returns:
            Dictionary with PCA analysis results
        """
        try:
            logger.info("📊 Starting standalone PCA analysis (settlement integration removed)")
            
            # Run PCA analysis without settlement integration
            result = run_independent_pca_analysis(
                data_handler=self.data_handler,
                selected_variables=variables,
                session_id=session_id
            )
            
            if result.get('status') == 'error':
                return result
            
            return {
                'status': 'success',
                'message': 'PCA analysis completed successfully.',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'pca_analysis',
                'results': result,
                'n_components': result.get('n_components', 0),
                'variance_explained': result.get('variance_explained', 0)
            }
            
        except Exception as e:
            logger.error(f"PCA analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'PCA analysis failed: {str(e)}',
                'variables_used': variables or [],
                'analysis_type': 'pca_analysis'
            }
        

    # ================= EXISTING METHODS (unchanged) =================

    def run_standard_analysis(self, data_handler, session_id: str = None) -> Dict[str, Any]:
        """
        Run standard composite analysis workflow.
        
        Args:
            data_handler: DataHandler instance with loaded data
            session_id: Session identifier
            
        Returns:
            Dictionary with analysis results
        """
        try:
            result = run_full_analysis_pipeline(
                data_handler=data_handler,
                session_id=session_id
            )
            
            return {
                'status': 'success',
                'message': 'Standard analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'composite_scoring',
                'results': result
            }
            
        except Exception as e:
            logger.error(f"Standard analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': [],
                'analysis_type': 'composite_scoring'
            }
    
    def run_custom_analysis(self, data_handler, selected_variables: List[str], 
                          session_id: str = None) -> Dict[str, Any]:
        """
        Run analysis with custom variable selection.
        
        Args:
            data_handler: DataHandler instance with loaded data
            selected_variables: List of variables to use in analysis
            session_id: Session identifier
            
        Returns:
            Dictionary with analysis results
        """
        try:
            result = run_full_analysis_pipeline(
                data_handler=data_handler,
                selected_variables=selected_variables,
                session_id=session_id
            )
            
            return {
                'status': 'success',
                'message': 'Custom analysis completed successfully',
                'variables_used': selected_variables,
                'analysis_type': 'custom_composite_scoring',
                'results': result
            }
            
        except Exception as e:
            logger.error(f"Custom analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': selected_variables,
                'analysis_type': 'custom_composite_scoring'
            }
    
    def explain_variable_selection(self, variables: List[str], data_handler) -> Dict[str, Any]:
        """
        Generate explanation for variable selection.
        
        Args:
            variables: List of variables used in analysis
            data_handler: DataHandler instance
            
        Returns:
            Dictionary with explanation results
        """
        try:
            # Simple explanation based on variable types and data
            explanations = {}
            
            for var in variables:
                if hasattr(data_handler, 'df') and data_handler.df is not None:
                    if var in data_handler.df.columns:
                        explanations[var] = f"Variable '{var}' selected for malaria risk analysis"
                    else:
                        explanations[var] = f"Variable '{var}' not found in dataset"
                else:
                    explanations[var] = f"Variable '{var}' selected for analysis"
            
            return {
                'status': 'success',
                'explanations': explanations,
                'message': 'Variable explanations generated'
            }
            
        except Exception as e:
            logger.error(f"Variable explanation failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to generate explanations: {str(e)}',
                'explanations': {}
            }
    
    def run_complete_analysis(self, session_id: str, variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run complete analysis using both composite scoring and PCA methods.
        
        This method runs both analysis types and returns combined results.
        
        Args:
            session_id: Session identifier
            variables: Optional list of variables to use (auto-selected if None)
            
        Returns:
            Dictionary with complete analysis results from both methods
        """
        try:
            logger.info("📊 Starting complete analysis (composite + PCA)")
            
            # Run composite analysis
            composite_result = self.run_composite_analysis(session_id, variables)
            
            # Run PCA analysis
            pca_result = self.run_pca_analysis(session_id, variables)
            
            # Combine results
            if composite_result.get('status') == 'success' and pca_result.get('status') == 'success':
                return {
                    'status': 'success',
                    'message': 'Complete analysis (composite + PCA) completed successfully.',
                    'variables_used': composite_result.get('variables_used', []),
                    'analysis_type': 'dual_method',
                    'composite_results': composite_result.get('results', {}),
                    'pca_results': pca_result.get('results', {}),
                    'results': {
                        'composite': composite_result.get('results', {}),
                        'pca': pca_result.get('results', {})
                    }
                }
            else:
                # If either analysis fails, return error with details
                error_messages = []
                if composite_result.get('status') == 'error':
                    error_messages.append(f"Composite: {composite_result.get('message', 'Unknown error')}")
                if pca_result.get('status') == 'error':
                    error_messages.append(f"PCA: {pca_result.get('message', 'Unknown error')}")
                
                return {
                    'status': 'error',
                    'message': f'Complete analysis failed: {"; ".join(error_messages)}',
                    'variables_used': variables or [],
                    'analysis_type': 'dual_method',
                    'composite_results': composite_result,
                    'pca_results': pca_result
                }
            
        except Exception as e:
            logger.error(f"Complete analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Complete analysis failed: {str(e)}',
                'variables_used': variables or [],
                'analysis_type': 'dual_method'
            } 