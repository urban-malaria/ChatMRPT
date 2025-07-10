"""
Variable Selection Coordinator for ChatMRPT
Ensures consistent variable selection across all analysis methods (composite and PCA)
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from .region_aware_selection import apply_region_aware_selection

logger = logging.getLogger(__name__)


class VariableSelectionCoordinator:
    """
    Coordinates variable selection across all analysis methods to ensure consistency.
    
    This class ensures that both composite and PCA methods use the same variables
    for fair comparison and reproducible results.
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.selected_variables = None
        self.selection_method = None
        self.zone_detected = None
        self.zone_metadata = {}
        self.selection_timestamp = None
        
    def get_unified_variable_selection(self, cleaned_data: pd.DataFrame, 
                                     shapefile_data: Optional[pd.DataFrame] = None,
                                     custom_variables: Optional[List[str]] = None,
                                     llm_manager=None) -> Dict[str, Any]:
        """
        Get unified variable selection for all analysis methods.
        
        Args:
            cleaned_data: Cleaned CSV data
            shapefile_data: Optional shapefile data
            custom_variables: Optional user-specified variables
            llm_manager: Optional LLM manager for fallback selection
            
        Returns:
            Dictionary with unified variable selection results
        """
        try:
            # Check for custom variables first to respect user intent
            if custom_variables:
                logger.info(f"🎯 USER VARIABLES SPECIFIED: {custom_variables} - respecting user choice")
                result = self._validate_custom_variables(cleaned_data, custom_variables)
                self.selected_variables = result['variables']
                self.selection_method = 'user_specified'
                self.zone_detected = None
                self.zone_metadata = {}
                
                return {
                    'status': 'success',
                    'variables': self.selected_variables,
                    'zone_detected': self.zone_detected,
                    'selection_method': self.selection_method,
                    'zone_metadata': self.zone_metadata,
                    'total_available': len(cleaned_data.columns)
                }
            
            # Check for region-aware selection only when no custom variables
            logger.info("🌍 Checking for region-aware variable selection")
            region_result = apply_region_aware_selection(
                cleaned_data, 
                shapefile_data,
                llm_manager
            )
            
            if region_result['status'] == 'success' and region_result.get('zone_detected'):
                # Zone detected - use scientifically-validated variables
                logger.info(f"🎯 ZONE DETECTED: {region_result['zone_detected']} - using scientifically-validated variables")
                logger.info(f"🔬 ZONE VARIABLES: {region_result['selected_variables']}")
                self.selected_variables = region_result['selected_variables']
                self.selection_method = region_result.get('selection_method', 'region_aware')
                self.zone_detected = region_result['zone_detected']
                self.zone_metadata = region_result.get('zone_metadata', {})
                
                result = {
                    'status': 'success',
                    'variables': self.selected_variables,
                    'zone_detected': self.zone_detected,
                    'selection_method': self.selection_method,
                    'zone_metadata': self.zone_metadata,
                    'total_available': region_result.get('total_available_variables', 0)
                }
                
            # This block is now redundant since custom variables are handled first
            # elif custom_variables:
            #     # No zone detected - use custom variables as fallback
            #     logger.info(f"🎯 No zone detected - using custom variables: {custom_variables}")
            #     result = self._validate_custom_variables(cleaned_data, custom_variables)
            #     self.selected_variables = result['variables']
            #     self.selection_method = 'user_specified'
            #     self.zone_detected = None
            #     self.zone_metadata = {}
                
            elif self.selected_variables is not None:
                # Variables already selected - reuse them
                logger.info(f"♻️ Reusing previously selected variables: {self.selected_variables}")
                result = {
                    'status': 'success',
                    'variables': self.selected_variables,
                    'zone_detected': self.zone_detected,
                    'selection_method': self.selection_method,
                    'zone_metadata': self.zone_metadata
                }
                
            else:
                # No zone detected and no custom variables - use fallback
                logger.warning("No zone detected and no custom variables - using fallback")
                result = self._fallback_variable_selection(cleaned_data, llm_manager)
                self.selected_variables = result['variables']
                self.selection_method = result['selection_method']
                self.zone_detected = None
                self.zone_metadata = {}
            
            # Final validation
            if not self.selected_variables or len(self.selected_variables) < 2:
                logger.error("🚨 CRITICAL: Variable selection resulted in insufficient variables")
                return {
                    'status': 'error',
                    'message': 'Variable selection failed - insufficient variables for analysis',
                    'variables': [],
                    'zone_detected': None,
                    'selection_method': 'failed'
                }
            
            # Store selection timestamp
            import time
            self.selection_timestamp = time.time()
            
            logger.info(f"✅ UNIFIED SELECTION: {len(self.selected_variables)} variables selected via {self.selection_method}")
            logger.info(f"📋 VARIABLES: {self.selected_variables}")
            
            return {
                'status': 'success',
                'variables': self.selected_variables,
                'zone_detected': self.zone_detected,
                'selection_method': self.selection_method,
                'zone_metadata': self.zone_metadata,
                'variables_count': len(self.selected_variables),
                'selection_timestamp': self.selection_timestamp
            }
            
        except Exception as e:
            logger.error(f"Error in unified variable selection: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'variables': [],
                'zone_detected': None,
                'selection_method': 'error'
            }
    
    def _validate_custom_variables(self, cleaned_data: pd.DataFrame, 
                                  custom_variables: List[str]) -> Dict[str, Any]:
        """Validate user-specified variables with intelligent fuzzy matching."""
        if not custom_variables:
            return {
                'status': 'error',
                'message': 'No custom variables provided',
                'variables': []
            }
        
        # Import the variable matcher
        from ..core.variable_matcher import match_user_variables
        
        # Get all available columns for matching
        all_columns = list(cleaned_data.columns)
        
        # Use intelligent matching to find best matches
        match_results = match_user_variables(custom_variables, all_columns, threshold=0.7)
        
        logger.info(f"🔍 VARIABLE MATCHING: Processing {len(custom_variables)} user variables")
        
        # Process matched variables
        available_vars = []
        missing_vars = []
        matched_mapping = {}
        
        for user_var in custom_variables:
            if user_var in match_results['matched']:
                matched_var = match_results['matched'][user_var]
                confidence = match_results['confidence_scores'][user_var]
                
                logger.info(f"   ✅ Matched '{user_var}' → '{matched_var}' (confidence: {confidence:.0%})")
                
                # Validate the matched variable
                if pd.api.types.is_numeric_dtype(cleaned_data[matched_var]):
                    if cleaned_data[matched_var].var() > 0:  # Has variance
                        available_vars.append(matched_var)
                        matched_mapping[user_var] = matched_var
                    else:
                        logger.warning(f"   ⚠️ Variable '{matched_var}' has zero variance")
                        missing_vars.append(user_var)
                else:
                    logger.warning(f"   ⚠️ Variable '{matched_var}' is not numeric")
                    missing_vars.append(user_var)
            else:
                logger.warning(f"   ❌ No match found for '{user_var}'")
                if user_var in match_results['suggestions']:
                    suggestions = match_results['suggestions'][user_var]
                    logger.info(f"      💡 Did you mean: {suggestions[:3]}?")
                missing_vars.append(user_var)
        
        # Add any warnings from the matcher
        for warning in match_results.get('warnings', []):
            logger.warning(f"   ⚠️ {warning}")
        
        if len(available_vars) < 2:
            error_msg = f'Only {len(available_vars)} suitable variables found from {custom_variables}.'
            if missing_vars:
                error_msg += f' Could not match: {missing_vars}'
            if match_results.get('suggestions'):
                error_msg += ' Check variable names and try again.'
            
            return {
                'status': 'error',
                'message': error_msg,
                'variables': available_vars,
                'matched_mapping': matched_mapping,
                'suggestions': match_results.get('suggestions', {})
            }
        
        return {
            'status': 'success',
            'variables': available_vars,
            'missing_variables': missing_vars,
            'matched_mapping': matched_mapping,
            'match_confidence': match_results.get('confidence_scores', {})
        }
    
    def _fallback_variable_selection(self, cleaned_data: pd.DataFrame, 
                                   llm_manager=None) -> Dict[str, Any]:
        """Fallback variable selection when region-aware selection fails."""
        logger.info("🔄 Applying fallback variable selection")
        
        # Get all numeric columns
        numeric_cols = cleaned_data.select_dtypes(include=['number']).columns.tolist()
        
        # Filter out obvious identifier columns
        identifier_patterns = ['id', 'code', 'name', 'ward', 'lga', 'state', 'x', 'y', 'index']
        analysis_vars = []
        
        for col in numeric_cols:
            col_lower = col.lower()
            is_identifier = any(pattern in col_lower for pattern in identifier_patterns)
            
            if not is_identifier:
                # Check variance and data coverage
                if cleaned_data[col].var() > 0 and cleaned_data[col].notna().sum() > len(cleaned_data) * 0.5:
                    analysis_vars.append(col)
        
        # Prioritize malaria-relevant variables
        malaria_patterns = ['malaria', 'fever', 'parasite', 'pfpr', 'tpr', 'health', 'rainfall', 'temperature']
        prioritized_vars = []
        other_vars = []
        
        for var in analysis_vars:
            var_lower = var.lower()
            is_malaria_relevant = any(pattern in var_lower for pattern in malaria_patterns)
            
            if is_malaria_relevant:
                prioritized_vars.append(var)
            else:
                other_vars.append(var)
        
        # Take top variables (prioritized first)
        final_vars = prioritized_vars + other_vars
        
        # Limit to reasonable number
        if len(final_vars) > 8:
            final_vars = final_vars[:8]
        
        return {
            'variables': final_vars,
            'selection_method': 'fallback_numeric'
        }
    
    def get_variables_for_method(self, method: str) -> List[str]:
        """
        Get variables for a specific analysis method.
        
        Args:
            method: Analysis method ('composite' or 'pca')
            
        Returns:
            List of variables for the method
        """
        if not self.selected_variables:
            logger.error(f"No variables selected for {method} method")
            return []
        
        # Both methods use the same variables for consistency
        logger.info(f"🔄 {method.upper()} METHOD: Using {len(self.selected_variables)} unified variables")
        return self.selected_variables.copy()
    
    def get_selection_metadata(self) -> Dict[str, Any]:
        """Get metadata about the variable selection."""
        return {
            'variables': self.selected_variables,
            'selection_method': self.selection_method,
            'zone_detected': self.zone_detected,
            'zone_metadata': self.zone_metadata,
            'variables_count': len(self.selected_variables) if self.selected_variables else 0,
            'selection_timestamp': self.selection_timestamp
        }
    
    def reset_selection(self):
        """Reset the variable selection to allow new selection."""
        self.selected_variables = None
        self.selection_method = None
        self.zone_detected = None
        self.zone_metadata = {}
        self.selection_timestamp = None
        logger.info("🔄 Variable selection reset")


# Global coordinators by session
_coordinators = {}


def get_variable_coordinator(session_id: str) -> VariableSelectionCoordinator:
    """Get or create a variable selection coordinator for a session."""
    if session_id not in _coordinators:
        _coordinators[session_id] = VariableSelectionCoordinator(session_id)
    return _coordinators[session_id]


def clear_coordinator(session_id: str):
    """Clear the coordinator for a session."""
    if session_id in _coordinators:
        del _coordinators[session_id]
        logger.info(f"🗑️ Cleared variable coordinator for session {session_id}")