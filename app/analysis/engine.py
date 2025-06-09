"""
Analysis Engine for ChatMRPT

Simple wrapper class that provides a consistent interface for analysis operations.
"""

import logging
from typing import Dict, Any, Optional, List
from .pipeline import run_full_analysis_pipeline
from .metadata import AnalysisMetadata

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """
    Main analysis engine that coordinates different analysis workflows.
    
    This class provides a simple interface for running analysis operations
    while delegating to the specialized pipeline functions.
    """
    
    def __init__(self):
        """Initialize the analysis engine."""
        self.logger = logger
        
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