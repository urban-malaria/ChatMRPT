"""
Variable Comparison Validator for ChatMRPT
Ensures that both composite and PCA methods use identical variables for fair comparison
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class VariableComparisonValidator:
    """
    Validates that both analysis methods use the same variables for fair comparison.
    
    This validator ensures that:
    1. Both composite and PCA methods use identical variables
    2. Variables are properly validated and available
    3. Any discrepancies are reported and resolved
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.composite_variables = None
        self.pca_variables = None
        self.validation_results = {}
        
    def validate_method_consistency(self, data_handler) -> Dict[str, Any]:
        """
        Validate that both methods use the same variables.
        
        Args:
            data_handler: DataHandler with analysis results
            
        Returns:
            Dict with validation results
        """
        try:
            # Get variables used by each method
            self.composite_variables = getattr(data_handler, 'composite_variables_used', None)
            self.pca_variables = getattr(data_handler, 'pca_variables_used', None)
            
            logger.info(f"🔍 VALIDATION: Checking variable consistency")
            logger.info(f"   📊 Composite variables: {self.composite_variables}")
            logger.info(f"   🔬 PCA variables: {self.pca_variables}")
            
            # Check if both methods have variables
            if not self.composite_variables and not self.pca_variables:
                return {
                    'status': 'error',
                    'message': 'No variables found for either method',
                    'consistent': False
                }
            
            if not self.composite_variables:
                return {
                    'status': 'error',
                    'message': 'No variables found for composite method',
                    'consistent': False
                }
            
            if not self.pca_variables:
                return {
                    'status': 'error',
                    'message': 'No variables found for PCA method',
                    'consistent': False
                }
            
            # Check for consistency
            composite_set = set(self.composite_variables)
            pca_set = set(self.pca_variables)
            
            if composite_set == pca_set:
                logger.info(f"✅ VALIDATION: Methods use identical variables ({len(composite_set)} variables)")
                self.validation_results = {
                    'status': 'success',
                    'message': f'Both methods use identical {len(composite_set)} variables',
                    'consistent': True,
                    'variables_count': len(composite_set),
                    'variables_list': list(composite_set)
                }
            else:
                # Identify differences
                only_composite = composite_set - pca_set
                only_pca = pca_set - composite_set
                common_variables = composite_set & pca_set
                
                logger.warning(f"⚠️ VALIDATION: Variable mismatch detected!")
                logger.warning(f"   📊 Only in composite: {only_composite}")
                logger.warning(f"   🔬 Only in PCA: {only_pca}")
                logger.warning(f"   🤝 Common variables: {common_variables}")
                
                self.validation_results = {
                    'status': 'warning',
                    'message': f'Variable mismatch: {len(only_composite)} composite-only, {len(only_pca)} PCA-only',
                    'consistent': False,
                    'composite_variables': list(composite_set),
                    'pca_variables': list(pca_set),
                    'only_composite': list(only_composite),
                    'only_pca': list(only_pca),
                    'common_variables': list(common_variables),
                    'overlap_percentage': len(common_variables) / max(len(composite_set), len(pca_set)) * 100
                }
            
            return self.validation_results
            
        except Exception as e:
            logger.error(f"Error in variable consistency validation: {e}")
            return {
                'status': 'error',
                'message': f'Validation error: {str(e)}',
                'consistent': False
            }
    
    def generate_comparison_report(self) -> Dict[str, Any]:
        """
        Generate a detailed comparison report.
        
        Returns:
            Dict with detailed comparison report
        """
        if not self.validation_results:
            return {
                'status': 'error',
                'message': 'No validation results available'
            }
        
        try:
            report = {
                'session_id': self.session_id,
                'validation_timestamp': pd.Timestamp.now().isoformat(),
                'consistency_status': self.validation_results['consistent'],
                'summary': self.validation_results['message'],
                'details': {}
            }
            
            if self.validation_results['consistent']:
                report['details'] = {
                    'variables_count': self.validation_results['variables_count'],
                    'variables_list': self.validation_results['variables_list'],
                    'recommendation': 'Analysis results are directly comparable'
                }
            else:
                report['details'] = {
                    'composite_count': len(self.validation_results.get('composite_variables', [])),
                    'pca_count': len(self.validation_results.get('pca_variables', [])),
                    'overlap_percentage': self.validation_results.get('overlap_percentage', 0),
                    'discrepancies': {
                        'only_composite': self.validation_results.get('only_composite', []),
                        'only_pca': self.validation_results.get('only_pca', []),
                        'common': self.validation_results.get('common_variables', [])
                    },
                    'recommendation': 'Caution: Methods used different variables. Results may not be directly comparable.'
                }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating comparison report: {e}")
            return {
                'status': 'error',
                'message': f'Report generation error: {str(e)}'
            }
    
    def fix_variable_inconsistency(self, data_handler) -> Dict[str, Any]:
        """
        Attempt to fix variable inconsistencies by using the coordinator.
        
        Args:
            data_handler: DataHandler to fix
            
        Returns:
            Dict with fix results
        """
        try:
            from .variable_selection_coordinator import get_variable_coordinator
            
            coordinator = get_variable_coordinator(self.session_id)
            
            # Force reset and reselect variables
            coordinator.reset_selection()
            logger.info("🔄 FIXING: Reset variable coordinator")
            
            # Get unified selection
            selection_result = coordinator.get_unified_variable_selection(
                data_handler.cleaned_data,
                data_handler.shapefile_data,
                None,  # No custom variables
                None   # No LLM manager
            )
            
            if selection_result['status'] == 'success':
                unified_variables = selection_result['variables']
                
                # Update both methods to use the same variables
                data_handler.composite_variables_used = unified_variables.copy()
                data_handler.pca_variables_used = unified_variables.copy()
                
                logger.info(f"✅ FIXING: Both methods now use {len(unified_variables)} unified variables")
                
                return {
                    'status': 'success',
                    'message': f'Fixed: Both methods now use {len(unified_variables)} unified variables',
                    'variables_used': unified_variables,
                    'requires_rerun': True
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Could not fix inconsistency: {selection_result.get("message", "Unknown error")}',
                    'requires_rerun': False
                }
                
        except Exception as e:
            logger.error(f"Error fixing variable inconsistency: {e}")
            return {
                'status': 'error',
                'message': f'Fix error: {str(e)}',
                'requires_rerun': False
            }


def validate_analysis_consistency(data_handler, session_id: str) -> Dict[str, Any]:
    """
    Validate that both analysis methods use consistent variables.
    
    Args:
        data_handler: DataHandler with analysis results
        session_id: Session identifier
        
    Returns:
        Dict with validation results
    """
    validator = VariableComparisonValidator(session_id)
    return validator.validate_method_consistency(data_handler)


def generate_consistency_report(data_handler, session_id: str) -> Dict[str, Any]:
    """
    Generate a detailed consistency report.
    
    Args:
        data_handler: DataHandler with analysis results
        session_id: Session identifier
        
    Returns:
        Dict with detailed report
    """
    validator = VariableComparisonValidator(session_id)
    validator.validate_method_consistency(data_handler)
    return validator.generate_comparison_report()