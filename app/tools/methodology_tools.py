"""
Methodology explanation tools for ChatMRPT.
Provides dynamic, context-aware explanations of analytical methods based on actual session data.
"""

import logging
import os
import pandas as pd
import numpy as np
import json
from typing import Dict, List, Optional, Any
from app.data import DataHandler

logger = logging.getLogger(__name__)

def _extract_pca_implementation_context(data_handler) -> Dict[str, Any]:
    """Extract actual PCA implementation details from the session."""
    context = {
        'method_available': False,
        'implementation_details': {
            'class_name': 'PCAAnalysisPipeline',
            'standardization_method': 'Z-score (StandardScaler)',
            'component_selection_rule': 'min(n_variables, max(2, int(n_variables * 0.8)))',
            'random_state': 42,
            'ranking_method': 'variance-weighted component combination',
            'categorization': 'thirds (High/Medium/Low risk)'
        }
    }
    
    # Check if PCA has been run (instance variables first)
    if hasattr(data_handler, 'pca_rankings') and data_handler.pca_rankings is not None:
        context['method_available'] = True
        context['actual_results'] = {
            'total_wards': len(data_handler.pca_rankings),
            'top_5_wards': data_handler.pca_rankings.head(5)['ward_name'].tolist() if len(data_handler.pca_rankings) >= 5 else data_handler.pca_rankings['ward_name'].tolist()
        }
    else:
        # Check for PCA results file in session directory (correct path and filename)
        pca_results_file = os.path.join(data_handler.session_folder, 'analysis_vulnerability_rankings_pca.csv')
        if os.path.exists(pca_results_file):
            try:
                pca_df = pd.read_csv(pca_results_file)
                context['method_available'] = True
                context['actual_results'] = {
                    'total_wards': len(pca_df),
                    'top_5_wards': pca_df.head(5)['WardName'].tolist() if len(pca_df) >= 5 else pca_df['WardName'].tolist()
                }
            except Exception as e:
                logger.warning(f"Found PCA results file but couldn't read it: {e}")
    
    # Get variable importance if available
    if hasattr(data_handler, 'pca_variable_importance') and data_handler.pca_variable_importance:
        context['variable_importance'] = data_handler.pca_variable_importance
        context['top_variables'] = list(data_handler.pca_variable_importance.keys())[:3]
    else:
        # Check for variable importance file in session directory
        var_importance_file = os.path.join(data_handler.session_folder, 'pca_variable_importance.json')
        if os.path.exists(var_importance_file):
            try:
                with open(var_importance_file, 'r') as f:
                    var_importance = json.load(f)
                context['variable_importance'] = var_importance
                context['top_variables'] = list(var_importance.keys())[:3]
            except Exception as e:
                logger.warning(f"Found variable importance file but couldn't read it: {e}")
    
    # Get explained variance if available
    if hasattr(data_handler, 'pca_explained_variance') and data_handler.pca_explained_variance:
        context['explained_variance'] = data_handler.pca_explained_variance
    else:
        # Check for explained variance file in session directory
        explained_var_file = os.path.join(data_handler.session_folder, 'pca_explained_variance.json')
        if os.path.exists(explained_var_file):
            try:
                with open(explained_var_file, 'r') as f:
                    explained_var = json.load(f)
                context['explained_variance'] = explained_var
            except Exception as e:
                logger.warning(f"Found explained variance file but couldn't read it: {e}")
    
    # Get actual variables used - check both instance variable and unified dataset file
    if hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None:
        numeric_cols = data_handler.csv_data.select_dtypes(include=[np.number]).columns.tolist()
        context['available_variables'] = [col for col in numeric_cols if col.lower() not in ['wardname', 'ward_name', 'ward']]
    else:
        # Check for unified dataset file in session directory
        unified_data_file = os.path.join(data_handler.session_folder, 'unified_dataset.csv')
        if os.path.exists(unified_data_file):
            try:
                unified_df = pd.read_csv(unified_data_file)
                numeric_cols = unified_df.select_dtypes(include=[np.number]).columns.tolist()
                context['available_variables'] = [col for col in numeric_cols if col.lower() not in ['wardname', 'ward_name', 'ward']]
            except Exception as e:
                logger.warning(f"Found unified dataset file but couldn't read it: {e}")
                context['available_variables'] = []
        else:
            context['available_variables'] = []
    
    return context

def _extract_composite_implementation_context(data_handler) -> Dict[str, Any]:
    """Extract actual composite score implementation details from the session."""
    context = {
        'method_available': False,
        'implementation_details': {
            'normalization_method': 'Min-max with relationship handling',
            'direct_formula': '(value - min) / (max - min)',
            'inverse_formula': 'normalize(1/(value + 1e-10)) then min-max',
            'aggregation_method': 'Simple arithmetic mean',
            'weighting': 'Equal weights (1/n)',
            'model_generation': 'Combinatorial (2+ variables)',
            'edge_case_handling': 'Default to 0.5 for identical values'
        }
    }
    
    # Check if composite scoring has been run (instance variables first)
    try:
        if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None:
            context['method_available'] = True
            # Ensure composite_scores is a DataFrame, not a dict
            if hasattr(data_handler.composite_scores, 'columns'):
                model_cols = [col for col in data_handler.composite_scores.columns if col.startswith('model_')]
                context['actual_results'] = {
                    'total_wards': len(data_handler.composite_scores),
                    'models_generated': len(model_cols),
                    'model_names': model_cols[:5]  # Show first 5 models
                }
            else:
                logger.warning(f"composite_scores exists but is not a DataFrame: {type(data_handler.composite_scores)}")
    except Exception as e:
        logger.warning(f"Error checking composite scores: {e}")
    
    # Always check for composite results file in session directory (regardless of in-memory state)
    try:
        composite_results_file = os.path.join(data_handler.session_folder, 'analysis_vulnerability_rankings.csv')
        if os.path.exists(composite_results_file):
            try:
                composite_df = pd.read_csv(composite_results_file)
                context['method_available'] = True
                # Only set actual_results if not already set from in-memory data
                if 'actual_results' not in context:
                    context['actual_results'] = {
                        'total_wards': len(composite_df),
                        'top_5_wards': composite_df.head(5)['WardName'].tolist() if len(composite_df) >= 5 else composite_df['WardName'].tolist()
                    }
                
            except Exception as e:
                logger.warning(f"Found composite results file but couldn't read it: {e}")
    except Exception as e:
        logger.warning(f"Error checking composite results file: {e}")
    
    # Always check for model formulas file to get actual variable selection approach
    try:
        model_formulas_file = os.path.join(data_handler.session_folder, 'model_formulas.csv')
        if os.path.exists(model_formulas_file):
            model_df = pd.read_csv(model_formulas_file)
            
            # Update models_generated count
            if 'actual_results' in context:
                context['actual_results']['models_generated'] = len(model_df)
            else:
                context['actual_results'] = {'models_generated': len(model_df)}
            
            # Extract actual variable selection approach used in this session
            if 'variables' in model_df.columns:
                all_vars = set()
                for var_string in model_df['variables']:
                    all_vars.update(var_string.split(','))
                context['session_variables_used'] = sorted(list(all_vars))
                
                # Determine the actual selection method used
                context['variable_selection_approach'] = {
                    'method': 'combinatorial_testing',
                    'description': f'Generated {len(model_df)} different variable combinations for testing',
                    'variable_count': len(context['session_variables_used']),
                    'total_combinations': len(model_df),
                    'approach_rationale': 'Tests multiple variable combinations to identify optimal risk indicators through empirical analysis'
                }
                
                # Extract actual implementation patterns
                context['implementation_patterns'] = {
                    'min_variables_per_model': min(len(vars.split(',')) for vars in model_df['variables']),
                    'max_variables_per_model': max(len(vars.split(',')) for vars in model_df['variables']),
                    'most_frequent_variables': _get_variable_frequencies(model_df['variables']),
                    'combination_strategy': 'systematic_combinations'
                }
    except Exception as e:
        logger.warning(f"Error checking model formulas file: {e}")
    
    # Extract actual variable selection method from data handler if available
    try:
        if hasattr(data_handler, 'variable_selection_method'):
            context['selection_method_used'] = data_handler.variable_selection_method
        
        if hasattr(data_handler, 'composite_variables'):
            context['variables_selected'] = data_handler.composite_variables
            
        # Get variable relationships if available (actual relationships used)
        if hasattr(data_handler, 'variable_relationships') and data_handler.variable_relationships:
            if isinstance(data_handler.variable_relationships, dict):
                context['variable_relationships'] = data_handler.variable_relationships
                context['direct_variables'] = [k for k, v in data_handler.variable_relationships.items() if v == 'direct']
                context['inverse_variables'] = [k for k, v in data_handler.variable_relationships.items() if v == 'inverse']
            else:
                logger.warning(f"variable_relationships exists but is not a dict: {type(data_handler.variable_relationships)}")
    except Exception as e:
        logger.warning(f"Error extracting actual selection method: {e}")
    
    # Get actual variables available in the dataset
    try:
        if hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None:
            if hasattr(data_handler.csv_data, 'select_dtypes'):
                numeric_cols = data_handler.csv_data.select_dtypes(include=[np.number]).columns.tolist()
                context['available_variables'] = [col for col in numeric_cols if col.lower() not in ['wardname', 'ward_name', 'ward']]
            else:
                logger.warning(f"csv_data exists but is not a DataFrame: {type(data_handler.csv_data)}")
                context['available_variables'] = []
        else:
            # Check for unified dataset file in session directory
            unified_data_file = os.path.join(data_handler.session_folder, 'unified_dataset.csv')
            if os.path.exists(unified_data_file):
                try:
                    unified_df = pd.read_csv(unified_data_file)
                    numeric_cols = unified_df.select_dtypes(include=[np.number]).columns.tolist()
                    context['available_variables'] = [col for col in numeric_cols if col.lower() not in ['wardname', 'ward_name', 'ward']]
                except Exception as e:
                    logger.warning(f"Found unified dataset file but couldn't read it: {e}")
                    context['available_variables'] = []
            else:
                context['available_variables'] = []
    except Exception as e:
        logger.warning(f"Error getting available variables: {e}")
        context['available_variables'] = []
    
    return context

def _get_variable_frequencies(variable_strings):
    """Helper method to get variable usage frequencies from model formulas"""
    try:
        var_counts = {}
        for var_string in variable_strings:
            for var in var_string.split(','):
                var = var.strip()
                var_counts[var] = var_counts.get(var, 0) + 1
        
        # Return top 5 most frequent variables
        sorted_vars = sorted(var_counts.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_vars[:5])
    except Exception as e:
        logger.warning(f"Error calculating variable frequencies: {e}")
        return {}

def explain_pca_methodology(session_id: str, technical_level: str = 'intermediate', 
                           include_variables: bool = True) -> Dict[str, Any]:
    """
    Explain PCA methodology with actual implementation context for natural LLM responses.
    
    Args:
        session_id: Session identifier
        technical_level: Level of explanation ('basic', 'intermediate', 'advanced')
        include_variables: Whether to include variable-specific analysis
        
    Returns:
        Dict containing PCA methodology context for LLM explanation
    """
    try:
        # Construct proper session folder path (correct structure)
        session_folder = os.path.join('instance', 'uploads', session_id)
        data_handler = DataHandler(session_folder)
        pca_context = _extract_pca_implementation_context(data_handler)
        
        # Structure the context for LLM understanding
        explanation_context = {
            'method_name': 'Principal Component Analysis (PCA)',
            'technical_level': technical_level,
            'session_id': session_id,
            'implementation_context': pca_context,
            'explanation_guidelines': {
                'basic': {
                    'focus': 'Simple concepts, practical understanding',
                    'avoid': 'Complex mathematical formulas, technical jargon',
                    'include': 'Step-by-step process, key benefits, real-world interpretation'
                },
                'intermediate': {
                    'focus': 'Implementation details, methodology steps, practical applications',
                    'include': 'Actual functions used, parameter choices, data flow',
                    'balance': 'Technical accuracy with practical understanding'
                },
                'advanced': {
                    'focus': 'Mathematical foundation, algorithmic details, implementation specifics',
                    'include': 'Formulas, complexity analysis, technical trade-offs',
                    'depth': 'Full technical detail with code-level understanding'
                }
            },
            'personalization_data': {
                'has_actual_results': pca_context['method_available'],
                'ward_count': pca_context.get('actual_results', {}).get('total_wards', 'unknown'),
                'variables_available': len(pca_context.get('available_variables', [])),
                'has_variable_importance': 'variable_importance' in pca_context,
                'has_explained_variance': 'explained_variance' in pca_context
            }
        }
        
        # Add specific talking points based on actual data
        talking_points = []
        
        if pca_context['method_available']:
            talking_points.append(f"PCA has been run on this session's data")
            if 'actual_results' in pca_context:
                talking_points.append(f"Analysis covered {pca_context['actual_results']['total_wards']} wards")
                if pca_context['actual_results']['top_5_wards']:
                    talking_points.append(f"Top vulnerable wards identified: {', '.join(pca_context['actual_results']['top_5_wards'][:3])}")
        
        if 'variable_importance' in pca_context:
            talking_points.append(f"Variable importance rankings available")
            if 'top_variables' in pca_context:
                talking_points.append(f"Most important variables: {', '.join(pca_context['top_variables'])}")
        
        if 'explained_variance' in pca_context:
            if 'total_explained' in pca_context['explained_variance']:
                total_var = pca_context['explained_variance']['total_explained'] * 100
                talking_points.append(f"Components explain {total_var:.1f}% of data variance")
        
        explanation_context['talking_points'] = talking_points
        
        return {
            'status': 'success',
            'context': explanation_context,
            'message': f'PCA methodology context prepared for {technical_level} level explanation'
        }
        
    except Exception as e:
        logger.error(f"Error preparing PCA methodology context: {e}")
        return {
            'status': 'error',
            'message': f'Error preparing PCA methodology context: {str(e)}'
        }

def explain_composite_score_methodology(session_id: str, technical_level: str = 'intermediate', 
                                      include_variables: bool = True) -> Dict[str, Any]:
    """
    Explain composite score methodology with actual implementation context for natural LLM responses.
    
    Args:
        session_id: Session identifier
        technical_level: Level of explanation ('basic', 'intermediate', 'advanced')
        include_variables: Whether to include variable-specific analysis
        
    Returns:
        Dict containing composite score methodology context for LLM explanation
    """
    try:
        # Construct proper session folder path (correct structure)
        session_folder = os.path.join('instance', 'uploads', session_id)
        data_handler = DataHandler(session_folder)
        composite_context = _extract_composite_implementation_context(data_handler)
        
        # Structure the context for LLM understanding
        explanation_context = {
            'method_name': 'Composite Score Calculation',
            'technical_level': technical_level,
            'session_id': session_id,
            'implementation_context': composite_context,
            'explanation_guidelines': {
                'basic': {
                    'focus': 'Simple averaging concept, practical interpretation',
                    'avoid': 'Complex normalization details, technical implementation',
                    'include': 'Why variables are combined, how scores are interpreted'
                },
                'intermediate': {
                    'focus': 'Normalization process, relationship handling, practical applications',
                    'include': 'Direct vs inverse relationships, actual formulas used',
                    'balance': 'Technical understanding with practical insights'
                },
                'advanced': {
                    'focus': 'Implementation details, mathematical foundations, edge cases',
                    'include': 'Exact formulas, error handling, computational aspects',
                    'depth': 'Complete technical understanding'
                }
            },
            'personalization_data': {
                'has_actual_results': composite_context['method_available'],
                'ward_count': composite_context.get('actual_results', {}).get('total_wards', 'unknown'),
                'models_generated': composite_context.get('actual_results', {}).get('models_generated', 'unknown'),
                'variables_available': len(composite_context.get('available_variables', [])),
                'has_relationships': 'variable_relationships' in composite_context,
                'direct_var_count': len(composite_context.get('direct_variables', [])),
                'inverse_var_count': len(composite_context.get('inverse_variables', []))
            }
        }
        
        # Add specific talking points based on actual data
        talking_points = []
        
        if composite_context['method_available']:
            talking_points.append(f"Composite scoring has been run on this session's data")
            if 'actual_results' in composite_context:
                talking_points.append(f"Analysis covered {composite_context['actual_results']['total_wards']} wards")
                talking_points.append(f"Generated {composite_context['actual_results']['models_generated']} different scoring models")
        
        if 'variable_relationships' in composite_context:
            talking_points.append(f"Variable relationships have been determined")
            if composite_context.get('direct_variables'):
                talking_points.append(f"Direct risk variables: {', '.join(composite_context['direct_variables'][:3])}")
            if composite_context.get('inverse_variables'):
                talking_points.append(f"Inverse risk variables: {', '.join(composite_context['inverse_variables'][:3])}")
        
        if composite_context.get('available_variables'):
            talking_points.append(f"{len(composite_context['available_variables'])} variables available for analysis")
        
        explanation_context['talking_points'] = talking_points
        
        return {
            'status': 'success',
            'context': explanation_context,
            'message': f'Composite score methodology context prepared for {technical_level} level explanation'
        }
        
    except Exception as e:
        logger.error(f"Error preparing composite score methodology context: {e}")
        return {
            'status': 'error',
            'message': f'Error preparing composite score methodology context: {str(e)}'
        }

def compare_methodologies(session_id: str, include_performance: bool = True) -> Dict[str, Any]:
    """
    Compare PCA and composite score methodologies with actual session context.
    
    Args:
        session_id: Session identifier
        include_performance: Whether to include performance comparison if available
        
    Returns:
        Dict containing methodology comparison context for LLM explanation
    """
    try:
        # Construct proper session folder path (correct structure)
        session_folder = os.path.join('instance', 'uploads', session_id)
        data_handler = DataHandler(session_folder)
        pca_context = _extract_pca_implementation_context(data_handler)
        composite_context = _extract_composite_implementation_context(data_handler)
        
        # Structure comparison context
        comparison_context = {
            'session_id': session_id,
            'methods_available': {
                'pca': pca_context['method_available'],
                'composite': composite_context['method_available']
            },
            'pca_context': pca_context,
            'composite_context': composite_context,
            'comparison_guidelines': {
                'focus_areas': [
                    'Data preprocessing approaches',
                    'Mathematical complexity',
                    'Interpretability trade-offs',
                    'Computational requirements',
                    'Variable handling strategies',
                    'Output characteristics'
                ],
                'personalization': 'Use actual session data to make comparisons concrete and relevant',
                'tone': 'Analytical but accessible, highlighting practical implications'
            }
        }
        
        # Add comparison talking points based on actual data
        talking_points = []
        
        if pca_context['method_available'] and composite_context['method_available']:
            talking_points.append("Both methods have been run on your data - can provide direct comparison")
            
            # Compare ward counts (should be same)
            pca_wards = pca_context.get('actual_results', {}).get('total_wards', 0)
            comp_wards = composite_context.get('actual_results', {}).get('total_wards', 0)
            if pca_wards == comp_wards:
                talking_points.append(f"Both methods analyzed the same {pca_wards} wards")
            
            # Compare variable usage
            pca_vars = len(pca_context.get('available_variables', []))
            comp_vars = len(composite_context.get('available_variables', []))
            if pca_vars == comp_vars:
                talking_points.append(f"Both methods used {pca_vars} variables")
            
        elif pca_context['method_available']:
            talking_points.append("PCA has been run on your data - can show concrete examples")
            talking_points.append("Composite scoring not yet run - comparison will be theoretical")
            
        elif composite_context['method_available']:
            talking_points.append("Composite scoring has been run on your data - can show concrete examples")
            talking_points.append("PCA not yet run - comparison will be theoretical")
            
        else:
            talking_points.append("Neither method has been run yet - comparison will be theoretical")
            if composite_context.get('available_variables'):
                talking_points.append(f"Data contains {len(composite_context['available_variables'])} variables for analysis")
        
        # Add method-specific insights
        if 'variable_importance' in pca_context and 'variable_relationships' in composite_context:
            talking_points.append("Can compare data-driven PCA importance vs expert-defined relationships")
        
        if 'explained_variance' in pca_context:
            total_var = pca_context['explained_variance'].get('total_explained', 0) * 100
            talking_points.append(f"PCA captures {total_var:.1f}% of data variance")
        
        if composite_context.get('actual_results', {}).get('models_generated'):
            models = composite_context['actual_results']['models_generated']
            talking_points.append(f"Composite method generated {models} different scoring models")
        
        comparison_context['talking_points'] = talking_points
        
        return {
            'status': 'success',
            'comparison': comparison_context,
            'message': 'Methodology comparison context prepared'
        }
        
    except Exception as e:
        logger.error(f"Error preparing methodology comparison context: {e}")
        return {
            'status': 'error',
            'message': f'Error preparing methodology comparison context: {str(e)}'
        }

def get_variable_importance_analysis(session_id: str, method: str = 'both') -> Dict[str, Any]:
    """
    Analyze variable importance with actual session context.
    
    Args:
        session_id: Session identifier
        method: Which method to analyze ('pca', 'composite', 'both')
        
    Returns:
        Dict containing variable importance analysis context for LLM explanation
    """
    try:
        # Construct proper session folder path (correct structure)
        session_folder = os.path.join('instance', 'uploads', session_id)
        data_handler = DataHandler(session_folder)
        
        analysis_context = {
            'session_id': session_id,
            'method_requested': method,
            'available_data': {},
            'analysis_guidelines': {
                'focus': 'Practical interpretation of variable importance for malaria risk assessment',
                'include': 'Actual rankings, scores, and epidemiological relevance',
                'personalize': 'Use session-specific data to make analysis concrete and actionable'
            }
        }
        
        talking_points = []
        
        # Check PCA variable importance
        if method in ['pca', 'both'] and hasattr(data_handler, 'pca_variable_importance'):
            if data_handler.pca_variable_importance:
                analysis_context['available_data']['pca'] = {
                    'importance_scores': data_handler.pca_variable_importance,
                    'calculation_method': 'Weighted sum of absolute loadings across components',
                    'interpretation': 'Higher scores indicate greater contribution to data variance patterns'
                }
                
                top_vars = list(data_handler.pca_variable_importance.keys())[:3]
                talking_points.append(f"PCA importance analysis available - top variables: {', '.join(top_vars)}")
                
                # Get actual scores for context
                scores = list(data_handler.pca_variable_importance.values())
                if len(scores) >= 2:
                    talking_points.append(f"Importance scores range from {min(scores):.3f} to {max(scores):.3f}")
        
        # Check composite variable relationships
        if method in ['composite', 'both'] and hasattr(data_handler, 'variable_relationships'):
            if data_handler.variable_relationships:
                analysis_context['available_data']['composite'] = {
                    'variable_relationships': data_handler.variable_relationships,
                    'calculation_method': 'Expert-defined direct/inverse relationships',
                    'interpretation': 'Direct variables increase risk, inverse variables decrease risk'
                }
                
                direct_vars = [k for k, v in data_handler.variable_relationships.items() if v == 'direct']
                inverse_vars = [k for k, v in data_handler.variable_relationships.items() if v == 'inverse']
                
                if direct_vars:
                    talking_points.append(f"Direct risk variables identified: {', '.join(direct_vars[:3])}")
                if inverse_vars:
                    talking_points.append(f"Inverse risk variables identified: {', '.join(inverse_vars[:3])}")
        
        analysis_context['talking_points'] = talking_points
        
        return {
            'status': 'success',
            'analysis': analysis_context,  # Changed from 'context' to 'analysis'
            'message': f'Variable importance analysis context prepared for {method} method(s)'
        }
        
    except Exception as e:
        logger.error(f"Error preparing variable importance analysis context: {e}")
        return {
            'status': 'error',
            'message': f'Error preparing variable importance analysis context: {str(e)}'
        } 