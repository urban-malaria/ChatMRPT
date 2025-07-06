"""
Variable Validation System for Custom Variable Selection

This module provides validation, suggestion, and fallback logic for user-specified
custom variables in malaria risk analysis.
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from difflib import get_close_matches

from app.data import DataHandler
from .region_aware_selection import apply_region_aware_selection

logger = logging.getLogger(__name__)


class VariableValidationResult:
    """Results of variable validation with suggestions and fallbacks"""
    
    def __init__(self, 
                 valid_variables: List[str], 
                 invalid_variables: List[str],
                 suggestions: Dict[str, List[str]],
                 fallback_variables: List[str],
                 message: str):
        self.valid_variables = valid_variables
        self.invalid_variables = invalid_variables
        self.suggestions = suggestions
        self.fallback_variables = fallback_variables
        self.message = message
        self.is_valid = len(invalid_variables) == 0
        self.has_suggestions = len(suggestions) > 0


def validate_custom_variables(
    session_id: str,
    composite_variables: Optional[List[str]] = None,
    pca_variables: Optional[List[str]] = None,
    data_handler: Optional[DataHandler] = None
) -> Dict[str, VariableValidationResult]:
    """
    Validate custom variable selections against available data with intelligent suggestions
    
    Args:
        session_id: Session identifier
        composite_variables: Custom variables for composite analysis
        pca_variables: Custom variables for PCA analysis  
        data_handler: Optional data handler (will create if None)
        
    Returns:
        Dictionary with validation results for composite and pca variables
    """
    
    logger.info(f"Validating custom variables for session {session_id}")
    
    # Get data handler if not provided
    if data_handler is None:
        session_folder = f"instance/uploads/{session_id}"
        data_handler = DataHandler(session_folder)
    
    if data_handler.csv_data is None or data_handler.csv_data.empty:
        logger.error("No CSV data available for variable validation")
        return {
            'composite': VariableValidationResult([], [], {}, [], "No data available for validation"),
            'pca': VariableValidationResult([], [], {}, [], "No data available for validation")
        }
    
    # Get available columns from the dataset
    available_columns = list(data_handler.csv_data.columns)
    
    # Remove non-analysis columns (geographic identifiers, etc.)
    analysis_columns = _filter_analysis_columns(available_columns)
    
    logger.info(f"Available analysis columns: {len(analysis_columns)} out of {len(available_columns)} total")
    
    # Validate composite variables
    composite_result = _validate_variable_list(
        composite_variables, 
        analysis_columns, 
        "composite",
        session_id,
        data_handler
    )
    
    # Validate PCA variables
    pca_result = _validate_variable_list(
        pca_variables, 
        analysis_columns, 
        "pca",
        session_id,
        data_handler
    )
    
    return {
        'composite': composite_result,
        'pca': pca_result
    }


def _validate_variable_list(
    custom_variables: Optional[List[str]],
    available_columns: List[str],
    method_name: str,
    session_id: str,
    data_handler: DataHandler
) -> VariableValidationResult:
    """Validate a single list of custom variables"""
    
    if custom_variables is None:
        # No custom variables - use auto-selection as fallback
        try:
            auto_selection = apply_region_aware_selection(
                data_handler.csv_data, 
                data_handler.shapefile_data
            )
            fallback_vars = auto_selection.get('selected_variables', [])
            
            return VariableValidationResult(
                valid_variables=[],
                invalid_variables=[],
                suggestions={},
                fallback_variables=fallback_vars,
                message=f"No custom variables specified for {method_name}. Using auto-selected variables: {', '.join(fallback_vars[:5])}"
            )
        except Exception as e:
            logger.error(f"Failed to get auto-selection fallback: {e}")
            return VariableValidationResult(
                valid_variables=[],
                invalid_variables=[],
                suggestions={},
                fallback_variables=[],
                message=f"No custom variables specified and auto-selection failed: {str(e)}"
            )
    
    # Validate each custom variable
    valid_variables = []
    invalid_variables = []
    suggestions = {}
    
    for variable in custom_variables:
        if variable in available_columns:
            valid_variables.append(variable)
        else:
            invalid_variables.append(variable)
            # Find close matches for suggestions
            close_matches = get_close_matches(
                variable, 
                available_columns, 
                n=3, 
                cutoff=0.6
            )
            if close_matches:
                suggestions[variable] = close_matches
    
    # Generate fallback variables if needed
    fallback_variables = []
    if invalid_variables:
        try:
            # Get auto-selection as fallback for invalid variables
            auto_selection = apply_region_aware_selection(
                data_handler.csv_data, 
                data_handler.shapefile_data
            )
            auto_vars = auto_selection.get('selected_variables', [])
            
            # Combine valid custom variables with auto-selected variables
            # Prioritize valid custom variables, supplement with auto-selection
            fallback_variables = valid_variables + [
                var for var in auto_vars 
                if var not in valid_variables and var in available_columns
            ]
            
        except Exception as e:
            logger.warning(f"Failed to get auto-selection for fallback: {e}")
            fallback_variables = valid_variables
    else:
        fallback_variables = valid_variables
    
    # Generate validation message
    if len(invalid_variables) == 0:
        message = f"✅ All {len(valid_variables)} custom variables are valid for {method_name} analysis"
    else:
        message = f"⚠️  {len(invalid_variables)} invalid variables for {method_name}: {', '.join(invalid_variables)}"
        if suggestions:
            message += f". Suggestions available for {len(suggestions)} variables"
        if fallback_variables:
            message += f". Using {len(fallback_variables)} fallback variables"
    
    return VariableValidationResult(
        valid_variables=valid_variables,
        invalid_variables=invalid_variables,
        suggestions=suggestions,
        fallback_variables=fallback_variables,
        message=message
    )


def _filter_analysis_columns(columns: List[str]) -> List[str]:
    """Filter out non-analysis columns like geographic identifiers"""
    
    # Columns to exclude from analysis (case-insensitive)
    exclude_patterns = [
        'wardcode', 'ward_code', 'lga', 'lgacode', 'lga_code',
        'state', 'statecode', 'state_code', 'wardname', 'ward_name',
        'geometry', 'objectid', 'id', 'fid', 'index'
    ]
    
    analysis_columns = []
    for col in columns:
        col_lower = col.lower()
        # Exclude if column name matches any exclude pattern
        if not any(pattern in col_lower for pattern in exclude_patterns):
            analysis_columns.append(col)
    
    return analysis_columns


def get_variable_suggestions(
    invalid_variable: str, 
    available_columns: List[str],
    max_suggestions: int = 3
) -> List[str]:
    """Get intelligent suggestions for an invalid variable name"""
    
    # First try exact fuzzy matching
    suggestions = get_close_matches(
        invalid_variable, 
        available_columns, 
        n=max_suggestions, 
        cutoff=0.6
    )
    
    if suggestions:
        return suggestions
    
    # If no close matches, try keyword-based matching
    keyword_suggestions = []
    invalid_lower = invalid_variable.lower()
    
    for col in available_columns:
        col_lower = col.lower()
        # Check if any part of the invalid variable appears in available columns
        if any(word in col_lower for word in invalid_lower.split('_')):
            keyword_suggestions.append(col)
            if len(keyword_suggestions) >= max_suggestions:
                break
    
    return keyword_suggestions


def create_validation_summary(
    composite_result: VariableValidationResult,
    pca_result: VariableValidationResult
) -> str:
    """Create a human-readable summary of variable validation results"""
    
    summary_parts = []
    
    # Header
    summary_parts.append("## 🔍 **Variable Validation Results**")
    summary_parts.append("")
    
    # Composite validation
    summary_parts.append("### 🎯 **Composite Score Variables:**")
    if composite_result.is_valid and composite_result.valid_variables:
        summary_parts.append(f"✅ Using {len(composite_result.valid_variables)} custom variables: {', '.join(composite_result.valid_variables)}")
    elif composite_result.fallback_variables:
        summary_parts.append(f"🔄 Using {len(composite_result.fallback_variables)} fallback variables: {', '.join(composite_result.fallback_variables[:5])}")
        if composite_result.invalid_variables:
            summary_parts.append(f"❌ Invalid variables: {', '.join(composite_result.invalid_variables)}")
    else:
        summary_parts.append("❌ No valid variables available for composite analysis")
    
    # PCA validation  
    summary_parts.append("")
    summary_parts.append("### 🔬 **PCA Variables:**")
    if pca_result.is_valid and pca_result.valid_variables:
        summary_parts.append(f"✅ Using {len(pca_result.valid_variables)} custom variables: {', '.join(pca_result.valid_variables)}")
    elif pca_result.fallback_variables:
        summary_parts.append(f"🔄 Using {len(pca_result.fallback_variables)} fallback variables: {', '.join(pca_result.fallback_variables[:5])}")
        if pca_result.invalid_variables:
            summary_parts.append(f"❌ Invalid variables: {', '.join(pca_result.invalid_variables)}")
    else:
        summary_parts.append("❌ No valid variables available for PCA analysis")
    
    # Suggestions
    all_suggestions = {**composite_result.suggestions, **pca_result.suggestions}
    if all_suggestions:
        summary_parts.append("")
        summary_parts.append("### 💡 **Variable Suggestions:**")
        for invalid_var, suggestions in all_suggestions.items():
            summary_parts.append(f"- **{invalid_var}** → {', '.join(suggestions)}")
    
    return "\n".join(summary_parts)


def apply_variable_validation_with_fallback(
    session_id: str,
    composite_variables: Optional[List[str]] = None,
    pca_variables: Optional[List[str]] = None
) -> Tuple[List[str], List[str], str]:
    """
    Apply variable validation and return final variable lists with explanation
    
    Returns:
        Tuple of (final_composite_vars, final_pca_vars, validation_message)
    """
    
    try:
        # Validate variables
        validation_results = validate_custom_variables(
            session_id=session_id,
            composite_variables=composite_variables,
            pca_variables=pca_variables
        )
        
        composite_result = validation_results['composite']
        pca_result = validation_results['pca']
        
        # Get final variable lists
        final_composite_vars = composite_result.fallback_variables or composite_result.valid_variables
        final_pca_vars = pca_result.fallback_variables or pca_result.valid_variables
        
        # Create validation summary
        validation_message = create_validation_summary(composite_result, pca_result)
        
        return final_composite_vars, final_pca_vars, validation_message
        
    except Exception as e:
        logger.error(f"Variable validation failed: {e}")
        # Emergency fallback - try basic auto-selection
        try:
            session_folder = f"instance/uploads/{session_id}"
            data_handler = DataHandler(session_folder)
            auto_selection = apply_region_aware_selection(
                data_handler.csv_data, 
                data_handler.shapefile_data
            )
            fallback_vars = auto_selection.get('selected_variables', [])
            
            error_message = f"❌ Variable validation failed: {str(e)}\n🔄 Using auto-selected variables: {', '.join(fallback_vars)}"
            return fallback_vars, fallback_vars, error_message
            
        except Exception as e2:
            logger.error(f"Emergency fallback also failed: {e2}")
            return [], [], f"❌ All variable validation failed: {str(e)} | {str(e2)}"