"""
Analysis module for ChatMRPT

This module provides refactored analysis functionality,
split from the monolithic analysis.py into focused modules.
"""

from .metadata import AnalysisMetadata
from .normalization import normalize_data, normalize_variable, determine_variable_relationships
from .utils import is_numeric_column, get_column_stats, check_data_quality
from .imputation import (
    handle_missing_values, 
    handle_spatial_imputation,
    handle_mean_imputation,
    handle_mode_imputation,
    get_imputation_summary,
    process_ward_for_spatial_imputation
)
from .scoring import (
    compute_composite_scores,
    compute_composite_score_model,
    analyze_vulnerability,
    get_scoring_summary,
    validate_scoring_inputs
)
from .urban_analysis import (
    analyze_urban_extent,
    get_urban_extent_summary,
    validate_urban_analysis_inputs,
    classify_urban_wards,
    get_urban_statistics
)
from .pipeline import run_full_analysis_pipeline

# Create a simple AnalysisEngine class inline
class AnalysisEngine:
    """Simple analysis engine wrapper for service container."""
    
    def __init__(self):
        self.data_handler = None
    
    def run_standard_analysis(self, data_handler, session_id=None):
        """Run standard analysis pipeline."""
        try:
            result = run_full_analysis_pipeline(data_handler=data_handler, session_id=session_id)
            return {
                'status': 'success',
                'message': 'Analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'results': result
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': []
            }
    
    def run_custom_analysis(self, data_handler, selected_variables, session_id=None):
        """Run analysis with custom variables."""
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
                'results': result
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': selected_variables
            }
    
    def run_composite_analysis(self, session_id=None, variables=None):
        """Run composite scoring analysis."""
        try:
            result = run_full_analysis_pipeline(
                data_handler=self.data_handler, 
                selected_variables=variables,
                session_id=session_id,
                analysis_type='composite'
            )
            return {
                'status': 'success',
                'message': 'Composite analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'results': result
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Composite analysis failed: {str(e)}',
                'variables_used': variables or []
            }
    
    def run_pca_analysis(self, session_id=None, variables=None):
        """Run PCA analysis."""
        try:
            result = run_full_analysis_pipeline(
                data_handler=self.data_handler,
                selected_variables=variables,
                session_id=session_id,
                analysis_type='pca'
            )
            return {
                'status': 'success',
                'message': 'PCA analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'results': result
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'PCA analysis failed: {str(e)}',
                'variables_used': variables or []
            }
    
    def run_complete_analysis(self, session_id=None, variables=None):
        """Run complete dual-method analysis."""
        try:
            result = run_full_analysis_pipeline(
                data_handler=self.data_handler,
                selected_variables=variables,
                session_id=session_id,
                analysis_type='complete'
            )
            return {
                'status': 'success',
                'message': 'Complete analysis finished successfully',
                'variables_used': result.get('variables_used', []),
                'results': result
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Complete analysis failed: {str(e)}',
                'variables_used': variables or []
            }
    
    def explain_variable_selection(self, variables, data_handler):
        """Generate simple variable explanations."""
        explanations = {}
        for var in variables:
            explanations[var] = f"Variable '{var}' used in malaria risk analysis"
        
        return {
            'status': 'success',
            'explanations': explanations,
            'message': 'Variable explanations generated'
        }

__all__ = [
    'AnalysisEngine',
    'AnalysisMetadata',
    'normalize_data',
    'normalize_variable', 
    'determine_variable_relationships',
    'is_numeric_column',
    'get_column_stats',
    'check_data_quality',
    'handle_missing_values',
    'handle_spatial_imputation',
    'handle_mean_imputation',
    'handle_mode_imputation',
    'get_imputation_summary',
    'process_ward_for_spatial_imputation',
    'compute_composite_scores',
    'compute_composite_score_model',
    'analyze_vulnerability',
    'get_scoring_summary',
    'validate_scoring_inputs',
    'analyze_urban_extent',
    'get_urban_extent_summary',
    'validate_urban_analysis_inputs',
    'classify_urban_wards',
    'get_urban_statistics',
    'run_full_analysis_pipeline'
] 