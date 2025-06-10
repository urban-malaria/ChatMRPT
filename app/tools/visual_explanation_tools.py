"""
Visual Explanation Tools for ChatMRPT

These tools provide AI-powered explanations of visualizations,
tailored for malaria epidemiology with dynamic geographic context.
"""

import logging
from typing import Dict, Any, Optional
from flask import current_app

logger = logging.getLogger(__name__)


def explain_last_visualization(session_id: str, question: Optional[str] = None) -> Dict[str, Any]:
    """
    Explain the most recently created visualization with epidemiological insights.
    
    Args:
        session_id: Session identifier
        question: Optional specific question about the visualization
        
    Returns:
        Dictionary with explanation and educational insights
    """
    try:
        # Get the visual explanation service
        from ..services.visual_explanation import get_visual_explanation_service
        
        # Get LLM manager from current app
        llm_manager = getattr(current_app, 'llm_manager', None)
        explanation_service = get_visual_explanation_service(llm_manager)
        
        # Get the last visualization from session
        last_viz = _get_last_visualization(session_id)
        
        if not last_viz:
            return {
                'status': 'error',
                'message': 'No recent visualization found to explain. Please create a chart first.'
            }
        
        # Generate explanation
        result = explanation_service.explain_visualization(
            session_id=session_id,
            viz_result=last_viz,
            user_question=question
        )
        
        if result['status'] == 'success':
            return {
                'status': 'success',
                'message': 'Visualization explanation generated successfully',
                'explanation': result['explanation'],
                'educational_insights': result['educational_insights'],
                'intervention_recommendations': result['intervention_recommendations'],
                'chart_info': {
                    'type': result['chart_type'],
                    'variable': result['variable']
                }
            }
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error explaining visualization: {e}")
        return {
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }


def explain_specific_visualization(session_id: str, chart_type: str, variable: str, 
                                 group_by: Optional[str] = None, 
                                 question: Optional[str] = None) -> Dict[str, Any]:
    """
    Explain a specific type of visualization with given parameters.
    
    Args:
        session_id: Session identifier
        chart_type: Type of chart (boxplot, histogram, scatter_plot, etc.)
        variable: Primary variable being visualized
        group_by: Optional grouping variable
        question: Optional specific question about the visualization
        
    Returns:
        Dictionary with explanation and educational insights
    """
    try:
        # Get the visual explanation service
        from ..services.visual_explanation import get_visual_explanation_service
        
        # Get LLM manager from current app
        llm_manager = getattr(current_app, 'llm_manager', None)
        explanation_service = get_visual_explanation_service(llm_manager)
        
        # Create mock visualization result for explanation
        viz_result = {
            'chart_type': chart_type,
            'variable': variable,
            'group_by': group_by,
            'status': 'success'
        }
        
        # Generate explanation
        result = explanation_service.explain_visualization(
            session_id=session_id,
            viz_result=viz_result,
            user_question=question
        )
        
        if result['status'] == 'success':
            return {
                'status': 'success',
                'message': f'Explanation generated for {chart_type} of {variable}',
                'explanation': result['explanation'],
                'educational_insights': result['educational_insights'],
                'intervention_recommendations': result['intervention_recommendations'],
                'chart_info': {
                    'type': result['chart_type'],
                    'variable': result['variable'],
                    'group_by': group_by
                }
            }
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error explaining specific visualization: {e}")
        return {
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }


def get_visualization_recommendations(session_id: str, analysis_goal: str) -> Dict[str, Any]:
    """
    Get recommendations for which visualizations to create based on analysis goals.
    
    Args:
        session_id: Session identifier
        analysis_goal: Description of what the user wants to analyze
        
    Returns:
        Dictionary with visualization recommendations
    """
    try:
        # Get available variables
        available_vars = _get_available_variables(session_id)
        
        if not available_vars:
            return {
                'status': 'error',
                'message': 'No data available for analysis recommendations'
            }
        
        # Get LLM manager and generate recommendations
        llm_manager = getattr(current_app, 'llm_manager', None)
        
        if not llm_manager:
            return _get_fallback_recommendations(analysis_goal, available_vars)
        
        # Get geographic context for dynamic recommendations
        from ..services.visual_explanation import get_visual_explanation_service
        explanation_service = get_visual_explanation_service(llm_manager)
        geographic_context = explanation_service._detect_geographic_context(session_id)
        location = geographic_context.get('location', 'the study area')
        
        recommendation_prompt = f"""
Based on the analysis goal and available variables, recommend the best visualizations for malaria analysis in {location}.

ANALYSIS GOAL: {analysis_goal}

AVAILABLE VARIABLES: {', '.join(available_vars)}

GEOGRAPHIC CONTEXT: {location} ({geographic_context.get('scale', 'regional')} scale)

Provide 3-4 specific visualization recommendations with:
1. Chart type (boxplot, histogram, scatter_plot, map_plot, heatmap)
2. Primary variable to visualize
3. Grouping variable (if applicable)
4. Epidemiological rationale for why this visualization is useful

Focus on visualizations that will provide actionable insights for malaria control in this specific context.
"""
        
        recommendations = llm_manager.generate_response(
            prompt=recommendation_prompt,
            system_message=f"You are a malaria epidemiologist providing visualization guidance for surveillance data analysis in {location}.",
            temperature=0.3,
            max_tokens=600
        )
        
        return {
            'status': 'success',
            'message': 'Visualization recommendations generated',
            'recommendations': recommendations,
            'available_variables': available_vars,
            'analysis_goal': analysis_goal
        }
        
    except Exception as e:
        logger.error(f"Error getting visualization recommendations: {e}")
        return {
            'status': 'error',
            'message': f'Error generating recommendations: {str(e)}'
        }


def _get_last_visualization(session_id: str) -> Optional[Dict[str, Any]]:
    """Get the most recently created visualization for this session."""
    try:
        # Check if we have session memory of last visualization
        if hasattr(current_app, 'session_memory'):
            session_memory = current_app.session_memory.get(session_id, {})
            return session_memory.get('last_visualization')
        
        # Fallback: return None - user will need to specify visualization details
        return None
        
    except Exception as e:
        logger.error(f"Error getting last visualization: {e}")
        return None


def _get_available_variables(session_id: str) -> list:
    """Get list of available variables in the dataset."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        df = load_unified_dataset(session_id)
        
        if df is not None:
            # Filter out geometry and technical columns
            excluded_cols = ['geometry', 'ward_id', 'index']
            available_vars = [col for col in df.columns if col not in excluded_cols]
            return available_vars
        
        return []
        
    except Exception as e:
        logger.error(f"Error getting available variables: {e}")
        return []


def _get_fallback_recommendations(analysis_goal: str, available_vars: list) -> Dict[str, Any]:
    """Provide fallback recommendations when LLM is not available."""
    
    # Basic recommendations based on common analysis patterns
    recommendations = []
    
    if 'tpr' in available_vars:
        recommendations.append("📊 Box plot of TPR by settlement_type - Compare malaria transmission between urban and rural areas")
        recommendations.append("🗺️ Map plot of TPR - Visualize geographic distribution of malaria risk")
    
    if 'composite_score' in available_vars:
        recommendations.append("📈 Histogram of composite_score - Understand the distribution of overall malaria risk")
        recommendations.append("🗺️ Map plot of composite_score - Identify highest priority wards for intervention")
    
    if 'population' in available_vars and 'tpr' in available_vars:
        recommendations.append("⚡ Scatter plot of population vs TPR - Explore relationship between population size and malaria risk")
    
    if len(available_vars) >= 3:
        recommendations.append("🔥 Heatmap of numeric variables - Explore correlations between malaria indicators")
    
    fallback_text = "Here are some recommended visualizations based on your available data:\n\n" + "\n".join(recommendations)
    
    return {
        'status': 'success',
        'message': 'Basic visualization recommendations provided',
        'recommendations': fallback_text,
        'available_variables': available_vars,
        'analysis_goal': analysis_goal
    } 