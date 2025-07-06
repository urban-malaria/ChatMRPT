"""
Chat-Accessible Settlement Visualization Tools for ChatMRPT

These tools provide user-friendly interfaces to settlement visualization functions
that can be called directly from the chat interface via the LLM.
"""

import logging
from typing import Dict, Any, Optional
from flask import session

logger = logging.getLogger(__name__)

def create_settlement_map(session_id: str, ward_name: Optional[str] = None, 
                         zoom_level: int = 11) -> Dict[str, Any]:
    """
    Create an interactive settlement classification map showing building types.
    
    This function creates a map that displays:
    - Building polygons colored by settlement type (formal/informal/non-residential)
    - Interactive toggles to show/hide settlement types
    - Satellite imagery background with street names
    - Optional ward highlighting
    
    Args:
        session_id: User session identifier
        ward_name: Optional specific ward to highlight and focus on
        zoom_level: Map zoom level (11=city view, 13=ward view, 15=detailed)
    
    Returns:
        Dict with status, message, file paths, and map metadata
    """
    try:
        logger.info(f"🗺️ Creating settlement map for session {session_id}")
        
        # Check dependencies
        dependency_check = check_settlement_dependencies()
        if dependency_check['status'] == 'error':
            return dependency_check
        
        from .settlement_validation_tools import create_building_classification_map
        
        # Create the map
        result = create_building_classification_map(
            session_id=session_id,
            ward_name=ward_name,
            zoom_level=zoom_level
        )
        
        if result['status'] == 'success':
            # Enhance response with user-friendly information
            result['ai_response'] = generate_settlement_map_explanation(result, ward_name)
            result['tool_used'] = 'create_settlement_map'
            
        return result
        
    except Exception as e:
        logger.error(f"Error creating settlement map: {e}")
        return {
            'status': 'error',
            'message': f'Failed to create settlement map: {str(e)}',
            'ai_response': 'I encountered an error while creating the settlement map. Please ensure the settlement data is available and try again.'
        }

def show_settlement_statistics(session_id: str) -> Dict[str, Any]:
    """
    Get comprehensive statistics about available settlement data.
    
    Returns:
        Dict with settlement data statistics and summary
    """
    try:
        logger.info(f"📊 Getting settlement statistics for session {session_id}")
        
        # Check dependencies
        dependency_check = check_settlement_dependencies()
        if dependency_check['status'] == 'error':
            return dependency_check
        
        from .settlement_validation_tools import get_building_statistics
        
        # Get statistics
        result = get_building_statistics(session_id)
        
        if result['status'] == 'success':
            # Enhance with AI explanation
            result['ai_response'] = generate_settlement_stats_explanation(result)
            result['tool_used'] = 'show_settlement_statistics'
            
        return result
        
    except Exception as e:
        logger.error(f"Error getting settlement statistics: {e}")
        return {
            'status': 'error',
            'message': f'Failed to get settlement statistics: {str(e)}',
            'ai_response': 'I could not retrieve settlement statistics. Please ensure settlement data is available.'
        }

def create_ward_specific_settlement_map(session_id: str, ward_name: str) -> Dict[str, Any]:
    """
    Create a detailed settlement map focused on a specific ward.
    
    Args:
        session_id: User session identifier
        ward_name: Name of the ward to focus on
    
    Returns:
        Dict with status, message, file paths, and map metadata
    """
    try:
        logger.info(f"🎯 Creating ward-specific settlement map for {ward_name}")
        
        # Create map with ward focus and higher zoom
        result = create_settlement_map(
            session_id=session_id,
            ward_name=ward_name,
            zoom_level=13  # More detailed view for ward-specific maps
        )
        
        if result['status'] == 'success':
            result['ai_response'] = f"Here's a detailed settlement map for {ward_name} ward. " + \
                                  result.get('ai_response', '')
            result['tool_used'] = 'create_ward_specific_settlement_map'
            
        return result
        
    except Exception as e:
        logger.error(f"Error creating ward-specific settlement map: {e}")
        return {
            'status': 'error',
            'message': f'Failed to create ward-specific settlement map: {str(e)}',
            'ai_response': f'I could not create a settlement map for {ward_name} ward. Please check that the ward name is correct and settlement data is available.'
        }

def integrate_settlement_data_with_analysis(session_id: str) -> Dict[str, Any]:
    """
    Integrate settlement data with existing analysis to enhance vulnerability assessment.
    
    This function:
    - Loads available settlement data
    - Integrates it with existing ward data
    - Enhances both composite scoring and PCA analysis
    - Provides summary of enhancement effects
    
    Returns:
        Dict with integration results and enhancement summary
    """
    try:
        logger.info(f"🔗 Integrating settlement data with analysis for session {session_id}")
        
        # Check dependencies
        dependency_check = check_settlement_dependencies()
        if dependency_check['status'] == 'error':
            return dependency_check
        
        # Get data handler
        from flask import current_app
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return {
                'status': 'error',
                'message': 'No data available for settlement integration',
                'ai_response': 'Please upload and analyze your data first before integrating settlement data.'
            }
        
        return {
            'status': 'error',
            'message': 'Settlement data integration feature has been removed',
            'ai_response': 'Settlement data integration is not available in this version.'
        }
        
    except Exception as e:
        logger.error(f"Error integrating settlement data: {e}")
        return {
            'status': 'error',
            'message': f'Failed to integrate settlement data: {str(e)}',
            'ai_response': 'I encountered an error while integrating settlement data with your analysis. Please ensure both your main data and settlement data are available.'
        }

def check_settlement_dependencies() -> Dict[str, Any]:
    """
    Check if required dependencies for settlement analysis are available.
    
    Returns:
        Dict with dependency check status
    """
    try:
        from ..core.dependency_validator import validator
        return validator.validate_settlement_dependencies()
        
    except Exception as e:
        logger.error(f"Error checking dependencies: {e}")
        return {
            'status': 'error',
            'message': f'Dependency check failed: {str(e)}',
            'ai_response': 'I could not verify that the required libraries are available for settlement analysis.'
        }

def generate_settlement_map_explanation(result: Dict[str, Any], ward_name: Optional[str] = None) -> str:
    """Generate user-friendly explanation for settlement map creation."""
    if result['status'] != 'success':
        return "I was unable to create the settlement map."
    
    building_count = result.get('building_count', 0)
    settlement_types = result.get('settlement_types', [])
    
    explanation = f"I've created an interactive settlement map showing {building_count:,} building footprints"
    
    if ward_name:
        explanation += f" for {ward_name} ward"
    
    explanation += ". The map displays:\n\n"
    explanation += "🟢 **Formal settlements** - Planned residential areas with better infrastructure\n"
    explanation += "🔴 **Informal settlements** - Unplanned areas that may have higher malaria risk\n"
    explanation += "🔵 **Non-residential buildings** - Commercial, industrial, and other non-housing structures\n\n"
    explanation += "**Interactive Features:**\n"
    explanation += "• Toggle settlement types on/off using the controls\n"
    explanation += "• Switch between **Street Map** and **High-Resolution Satellite Imagery** (Esri World Imagery) to validate classifications\n"
    explanation += "• Toggle ward boundaries and names for administrative context\n"
    explanation += "• Transparent overlays allow you to see actual rooftops for ground-truthing\n"
    explanation += "• Satellite view provides high-resolution imagery for visual validation of AI classifications"
    
    if building_count > 50000:
        explanation += f"\n\nNote: For performance, I'm showing a sample of {building_count:,} buildings. "
        explanation += "For more detailed analysis, you can focus on specific wards."
    
    return explanation

def generate_settlement_stats_explanation(result: Dict[str, Any]) -> str:
    """Generate user-friendly explanation for settlement statistics."""
    if result['status'] != 'success':
        return "I could not retrieve settlement statistics."
    
    total_buildings = result.get('total_buildings', 0)
    settlement_dist = result.get('settlement_distribution', {})
    
    explanation = f"Settlement data summary for {total_buildings:,} building footprints:\n\n"
    
    for settlement_type, count in settlement_dist.items():
        percentage = (count / total_buildings * 100) if total_buildings > 0 else 0
        explanation += f"• **{settlement_type.title()}**: {count:,} buildings ({percentage:.1f}%)\n"
    
    explanation += "\n**Malaria Risk Implications:**\n"
    explanation += "• Informal settlements typically have higher transmission risk due to poor drainage and water storage\n"
    explanation += "• Formal settlements usually have better infrastructure and healthcare access\n"
    explanation += "• Building density patterns can indicate overcrowding and potential breeding sites"
    
    return explanation

def generate_settlement_integration_explanation(result: Dict[str, Any]) -> str:
    """Generate user-friendly explanation for settlement data integration."""
    if result['status'] != 'success':
        return "Settlement data integration was not successful."
    
    variables_added = result.get('settlement_variables_added', [])
    enhanced_wards = result.get('enhanced_wards', 0)
    footprints = result.get('settlement_footprints', 0)
    
    explanation = f"Successfully integrated settlement data! Added {len(variables_added)} new variables "
    explanation += f"to {enhanced_wards} wards using {footprints:,} building footprints.\n\n"
    
    explanation += "**New settlement variables added:**\n"
    for var in variables_added[:5]:  # Show first 5 variables
        explanation += f"• {var.replace('_', ' ').title()}\n"
    
    if len(variables_added) > 5:
        explanation += f"• ... and {len(variables_added) - 5} more variables\n"
    
    explanation += "\n**Enhanced analysis capabilities:**\n"
    explanation += "• Composite vulnerability scores now include settlement risk factors\n"
    explanation += "• PCA analysis incorporates settlement morphology patterns\n"
    explanation += "• Ward rankings reflect both environmental AND settlement-based risks\n\n"
    explanation += "You can now run analysis to see how settlement patterns affect malaria vulnerability rankings."
    
    return explanation

# Export all user-facing tools
__all__ = [
    'create_settlement_map',
    'show_settlement_statistics', 
    'create_ward_specific_settlement_map',
    'integrate_settlement_data_with_analysis',
    'check_settlement_dependencies'
]# This file has been removed during ChatMRPT streamlining
# Settlement visualization tools were identified as non-essential and removed to focus on core functionality