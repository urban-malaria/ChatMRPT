"""
Visual Explanation Tools for ChatMRPT

These tools provide AI-powered explanations of visualizations,
tailored for malaria epidemiology with dynamic geographic context.
"""

import logging
from datetime import datetime
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
        # Get the last visualization from session
        last_viz = _get_last_visualization(session_id)
        
        if not last_viz:
            return {
                'status': 'error',
                'message': 'No recent visualization found to explain. Please create a chart first.'
            }
        
        # Get LLM manager from current app
        llm_manager = getattr(current_app, 'llm_manager', None)
        
        if not llm_manager:
            return _get_fallback_explanation(last_viz, question)
        
        # Generate enhanced explanation using LLM
        explanation = _generate_llm_explanation(llm_manager, session_id, last_viz, question)
        
        return {
            'status': 'success',
            'message': 'Visualization explanation generated successfully',
            'explanation': explanation,
            'chart_info': {
                'type': last_viz.get('chart_type', 'unknown'),
                'variable': last_viz.get('variable', 'unknown')
            }
        }
            
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
        # Create mock visualization result for explanation
        viz_result = {
            'chart_type': chart_type,
            'variable': variable,
            'group_by': group_by,
            'status': 'success'
        }
        
        # Get LLM manager from current app
        llm_manager = getattr(current_app, 'llm_manager', None)
        
        if not llm_manager:
            return _get_fallback_explanation(viz_result, question)
        
        # Generate enhanced explanation using LLM
        explanation = _generate_llm_explanation(llm_manager, session_id, viz_result, question)
        
        return {
            'status': 'success',
            'message': f'Explanation generated for {chart_type} of {variable}',
            'explanation': explanation,
            'chart_info': {
                'type': chart_type,
                'variable': variable,
                'group_by': group_by
            }
        }
            
    except Exception as e:
        logger.error(f"Error explaining specific visualization: {e}")
        return {
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }


def track_visualization_creation(session_id: str, viz_type: str, variable: str = None, 
                                group_by: str = None, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Track visualization creation in session memory for future reference.
    
    Args:
        session_id: Session identifier
        viz_type: Type of visualization created
        variable: Primary variable visualized
        group_by: Optional grouping variable
        metadata: Additional visualization metadata
        
    Returns:
        Dictionary confirming visualization was tracked
    """
    try:
        from ..services.session_memory import SessionMemory, MessageType
        memory = SessionMemory(session_id, storage_path="instance/memory")
        
        # Create comprehensive metadata
        viz_metadata = {
            'visualization_type': viz_type,
            'variable': variable,
            'group_by': group_by,
            'created_timestamp': datetime.utcnow().isoformat(),
            **(metadata or {})
        }
        
        # Create a descriptive message for the conversation history
        viz_description = f"Created {viz_type}"
        if variable:
            viz_description += f" of {variable}"
        if group_by:
            viz_description += f" grouped by {group_by}"
        
        # Add to conversation history
        memory.add_message(
            MessageType.ANALYSIS,
            viz_description,
            viz_metadata
        )
        
        return {
            'status': 'success',
            'message': 'Visualization tracked in session memory',
            'tracked_metadata': viz_metadata
        }
        
    except Exception as e:
        logger.error(f"Error tracking visualization: {e}")
        return {
            'status': 'error',
            'message': f'Failed to track visualization: {str(e)}'
        }


def get_session_visualizations(session_id: str, viz_type: str = None) -> Dict[str, Any]:
    """
    Get all visualizations created in this session.
    
    Args:
        session_id: Session identifier
        viz_type: Optional filter by visualization type
        
    Returns:
        Dictionary with all session visualizations
    """
    try:
        from ..services.session_memory import SessionMemory
        memory = SessionMemory(session_id, storage_path="instance/memory")
        
        # Find all visualization messages
        visualizations = []
        for msg in memory.conversation_history:
            if msg.metadata and 'visualization_type' in msg.metadata:
                viz_data = {
                    'timestamp': msg.timestamp,
                    'description': msg.content,
                    'type': msg.metadata.get('visualization_type'),
                    'variable': msg.metadata.get('variable'),
                    'group_by': msg.metadata.get('group_by'),
                    'metadata': msg.metadata
                }
                
                # Apply filter if specified
                if not viz_type or viz_data['type'] == viz_type:
                    visualizations.append(viz_data)
        
        return {
            'status': 'success',
            'visualizations': visualizations,
            'total_count': len(visualizations),
            'filter_applied': viz_type,
            'message': f"Found {len(visualizations)} visualizations in session"
        }
        
    except Exception as e:
        logger.error(f"Error getting session visualizations: {e}")
        return {
            'status': 'error',
            'message': f'Failed to retrieve visualizations: {str(e)}',
            'visualizations': []
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
        
        # Generate LLM-powered recommendations
        recommendation_prompt = f"""
Based on the analysis goal and available variables, recommend the best visualizations for malaria analysis.

ANALYSIS GOAL: {analysis_goal}

AVAILABLE VARIABLES: {', '.join(available_vars)}

Provide 3-4 specific visualization recommendations with:
1. Chart type (boxplot, histogram, scatter_plot, map_plot, heatmap)
2. Primary variable to visualize
3. Grouping variable (if applicable)
4. Epidemiological rationale for why this visualization is useful

Focus on visualizations that will provide actionable insights for malaria control.
"""
        
        recommendations = llm_manager.generate_response(
            prompt=recommendation_prompt,
            system_message="You are a malaria epidemiologist providing visualization guidance for surveillance data analysis.",
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
        # First try to get from session memory system
        from ..services.session_memory import SessionMemory
        memory = SessionMemory(session_id, storage_path="instance/memory")
        
        # Look for the most recent visualization message in conversation history
        for msg in reversed(memory.conversation_history):
            if msg.metadata and 'visualization_type' in msg.metadata:
                return {
                    'chart_type': msg.metadata.get('visualization_type'),
                    'variable': msg.metadata.get('variable'),
                    'group_by': msg.metadata.get('group_by'),
                    'timestamp': msg.timestamp,
                    'metadata': msg.metadata
                }
        
        # Fallback: Check if we have session memory of last visualization
        if hasattr(current_app, 'session_memory'):
            session_memory = current_app.session_memory.get(session_id, {})
            return session_memory.get('last_visualization')
        
        # Final fallback: return None - user will need to specify visualization details
        return None
        
    except Exception as e:
        logger.error(f"Error getting last visualization: {e}")
        return None


def _get_available_variables(session_id: str) -> list:
    """Get list of available variables in the dataset."""
    try:
        # Try to get dataset from session memory first (for testing)
        if hasattr(current_app, 'session_memory'):
            session_data = current_app.session_memory.get(session_id, {})
            if 'unified_dataset' in session_data:
                df = session_data['unified_dataset']
                if df is not None:
                    # Filter out geometry and technical columns
                    excluded_cols = ['geometry', 'ward_id', 'index']
                    available_vars = [col for col in df.columns if col not in excluded_cols]
                    return available_vars
        
        # Try to get dataset through normal means
        try:
            from .visual_tools import _get_unified_dataset
            df = _get_unified_dataset(session_id)
            
            if df is not None:
                # Filter out geometry and technical columns
                excluded_cols = ['geometry', 'ward_id', 'index']
                available_vars = [col for col in df.columns if col not in excluded_cols]
                return available_vars
        except Exception as e:
            logger.debug(f"Could not load dataset through normal means: {e}")
        
        # Fallback: return common malaria variables
        return ['tpr', 'composite_score', 'population', 'rainfall', 'temperature', 'settlement_type']
        
    except Exception as e:
        logger.error(f"Error getting available variables: {e}")
        # Return common malaria variables as fallback
        return ['tpr', 'composite_score', 'population', 'rainfall', 'temperature', 'settlement_type']


def _generate_llm_explanation(llm_manager, session_id: str, viz_result: Dict[str, Any], question: Optional[str] = None) -> str:
    """Generate explanation using LLM with malaria epidemiology context."""
    try:
        chart_type = viz_result.get('chart_type', 'unknown')
        variable = viz_result.get('variable', 'unknown')
        group_by = viz_result.get('group_by')
        
        # Get session context for better explanations
        session_context = _get_session_context(session_id)
        
        # Build context-aware prompt with session information
        prompt = f"""
Explain this malaria epidemiology visualization in a clear, educational way:

CHART TYPE: {chart_type}
PRIMARY VARIABLE: {variable}
GROUPING VARIABLE: {group_by if group_by else 'None'}

SESSION CONTEXT:
{session_context}

USER QUESTION: {question if question else 'General explanation requested'}

Provide:
1. What this visualization shows
2. How to interpret the patterns
3. Epidemiological significance for malaria control
4. Actionable insights for public health decision-making
5. How this relates to previous analysis or visualizations in this session

Keep the explanation accessible but scientifically accurate.
"""
        
        explanation = llm_manager.generate_response(
            prompt=prompt,
            system_message="You are a malaria epidemiologist explaining data visualizations to public health professionals. Use session context to provide more relevant explanations.",
            temperature=0.3,
            max_tokens=600,
            session_id=session_id
        )
        
        # Save explanation to memory for future reference
        _save_explanation_to_memory(session_id, chart_type, variable, explanation)
        
        return explanation
        
    except Exception as e:
        logger.error(f"Error generating LLM explanation: {e}")
        return _get_fallback_explanation(viz_result, question)['explanation']


def _get_session_context(session_id: str) -> str:
    """Get relevant session context for better explanations."""
    try:
        from ..services.session_memory import SessionMemory
        memory = SessionMemory(session_id, storage_path="instance/memory")
        
        context_parts = []
        
        # Add analysis context if available
        if memory.analysis_context:
            context_parts.append(f"Analysis completed using {memory.analysis_context.composite_method} method")
            if memory.analysis_context.variables_used:
                context_parts.append(f"Variables analyzed: {', '.join(memory.analysis_context.variables_used[:5])}")
            if memory.analysis_context.top_risk_wards:
                context_parts.append(f"Top risk wards identified: {', '.join(memory.analysis_context.top_risk_wards[:3])}")
        
        # Add recent visualizations
        viz_count = sum(1 for msg in memory.conversation_history 
                       if msg.metadata and 'visualization_type' in msg.metadata)
        if viz_count > 0:
            context_parts.append(f"Previous visualizations created in session: {viz_count}")
        
        return '\n'.join(context_parts) if context_parts else "No prior analysis context available"
        
    except Exception as e:
        logger.error(f"Error getting session context: {e}")
        return "Session context unavailable"


def _save_explanation_to_memory(session_id: str, chart_type: str, variable: str, explanation: str):
    """Save explanation to session memory for future reference."""
    try:
        from ..services.session_memory import SessionMemory, MessageType
        memory = SessionMemory(session_id, storage_path="instance/memory")
        
        metadata = {
            'type': 'explanation',
            'chart_type': chart_type,
            'variable': variable,
            'explanation_timestamp': datetime.utcnow().isoformat()
        }
        
        memory.add_message(
            MessageType.ASSISTANT,
            f"Explained {chart_type} visualization: {explanation[:100]}...",
            metadata
        )
        
    except Exception as e:
        logger.error(f"Error saving explanation to memory: {e}")


def _get_fallback_explanation(viz_result: Dict[str, Any], question: Optional[str] = None) -> Dict[str, Any]:
    """Provide fallback explanation when LLM is not available."""
    
    chart_type = viz_result.get('chart_type', 'unknown')
    variable = viz_result.get('variable', 'unknown')
    group_by = viz_result.get('group_by')
    
    # Basic explanations for different chart types
    explanations = {
        'histogram': f"This histogram shows the distribution of {variable}. It helps identify patterns like normal distribution, skewness, or outliers in the data.",
        'boxplot': f"This box plot displays the distribution of {variable}" + (f" across different {group_by} categories" if group_by else "") + ". It shows median, quartiles, and outliers.",
        'scatter_plot': f"This scatter plot explores the relationship between variables. It helps identify correlations and patterns in the data.",
        'map_plot': f"This map visualization shows the geographic distribution of {variable}. It helps identify spatial patterns and hotspots.",
        'heatmap': "This heatmap shows correlations between multiple variables. Darker colors indicate stronger relationships.",
        'bar_chart': f"This bar chart compares {variable} across different categories, making it easy to identify differences."
    }
    
    base_explanation = explanations.get(chart_type, f"This {chart_type} visualization helps analyze {variable} patterns in your malaria surveillance data.")
    
    # Add epidemiological context
    epi_context = {
        'tpr': "Test Positivity Rate (TPR) is a key indicator of malaria transmission intensity.",
        'composite_score': "Composite scores combine multiple risk factors to identify priority areas for intervention.",
        'population': "Population data helps understand the scale of potential malaria burden.",
        'rainfall': "Rainfall patterns influence mosquito breeding and malaria transmission cycles.",
        'temperature': "Temperature affects mosquito development and malaria parasite growth."
    }
    
    if variable in epi_context:
        base_explanation += f" {epi_context[variable]}"
    
    if question:
        base_explanation += f"\n\nRegarding your question about '{question}': This visualization can help answer that by showing patterns and relationships in the data."
    
    return {
        'status': 'success',
        'explanation': base_explanation,
        'chart_info': {
            'type': chart_type,
            'variable': variable,
            'group_by': group_by
        }
    }


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