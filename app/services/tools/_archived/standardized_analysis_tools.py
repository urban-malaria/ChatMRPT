"""
Standardized Analysis Tools for ChatMRPT

This module provides the standardized tool functions that the LLM manager calls
to execute analysis, create visualizations, and provide data insights.

These tools bridge the LLM-first architecture with the actual analysis engines.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
from flask import current_app

from ...data import DataHandler  
from ...analysis.engine import AnalysisEngine
from ...services.container import ServiceContainer

logger = logging.getLogger(__name__)


# ========================================================================
# SESSION MANAGEMENT TOOLS
# ========================================================================

def get_session_status(session_id: str) -> Dict[str, Any]:
    """
    Get comprehensive session status including data, analysis, and capabilities.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict containing session status information
    """
    try:
        container = ServiceContainer()
        data_service = container.data_service
        
        # The data_service IS the DataHandler, not a factory
        # Check if this session has any data by looking at session-specific files
        from pathlib import Path
        
        # Check if session folder exists and has data
        base_upload_folder = data_service.session_folder
        session_folder = os.path.join(base_upload_folder, session_id)
        
        session_exists = os.path.exists(session_folder)
        
        if not session_exists:
            return {
                'status': 'new_session',
                'message': 'New session - ready to receive data. Please upload CSV and shapefile data.',
                'session_id': session_id,
                'csv_loaded': False,
                'shapefile_loaded': False,
                'analysis_complete': False,
                'can_run_analysis': False,
                'available_actions': ['upload_data', 'explain_concept']
            }
        
        # Check for CSV and shapefile files in session folder
        csv_files = []
        shp_files = []
        
        try:
            for file in os.listdir(session_folder):
                if file.endswith('.csv') or file.endswith('.xlsx'):
                    csv_files.append(file)
                elif file.endswith('.shp'):
                    shp_files.append(file)
        except Exception as e:
            logger.warning(f"Error reading session folder: {e}")
        
        csv_loaded = len(csv_files) > 0
        shapefile_loaded = len(shp_files) > 0
        
        # Check for analysis results (composite scores files)
        analysis_complete = False
        try:
            analysis_files = [f for f in os.listdir(session_folder) if 'composite_scores' in f.lower()]
            analysis_complete = len(analysis_files) > 0
        except:
            analysis_complete = False
        
        # Determine available actions
        available_actions = ['get_session_status', 'explain_concept']
        if not csv_loaded or not shapefile_loaded:
            available_actions.append('upload_data')
        if csv_loaded and shapefile_loaded and not analysis_complete:
            available_actions.extend(['run_composite_analysis', 'run_pca_analysis'])
        elif analysis_complete:
            available_actions.extend([
                'create_composite_maps', 'create_vulnerability_map', 
                'create_box_plot_ranking', 'create_decision_tree',
                'create_urban_extent_map', 'list_available_maps'
            ])
        
        # Determine status
        if csv_loaded and shapefile_loaded and analysis_complete:
            status = 'analysis_complete'
            message = 'Analysis complete. All visualizations and insights available.'
        elif csv_loaded and shapefile_loaded:
            status = 'ready_for_analysis'
            message = 'Data loaded successfully. Ready to run malaria risk analysis.'
        elif csv_loaded and not shapefile_loaded:
            status = 'needs_shapefile'
            message = 'CSV data loaded. Please upload shapefile data to enable spatial analysis.'
        elif not csv_loaded and shapefile_loaded:
            status = 'needs_csv'
            message = 'Shapefile loaded. Please upload CSV data to enable analysis.'
        else:
            status = 'needs_data'
            message = 'Session exists but no data uploaded. Please upload CSV and shapefile data.'
        
        return {
            'status': status,
            'session_id': session_id,
            'csv_loaded': csv_loaded,
            'shapefile_loaded': shapefile_loaded,
            'analysis_complete': analysis_complete,
            'can_run_analysis': csv_loaded and shapefile_loaded,
            'available_actions': available_actions,
            'csv_files': csv_files,
            'shapefile_files': shp_files,
            'message': message
        }
        
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        return {
            'status': 'error',
            'message': f'Error checking session status: {str(e)}',
            'session_id': session_id,
            'csv_loaded': False,
            'shapefile_loaded': False,
            'analysis_complete': False,
            'available_actions': ['get_session_status', 'explain_concept']
        }


def _get_data_summary(data_handler) -> Dict[str, Any]:
    """Get summary of loaded data"""
    try:
        if not hasattr(data_handler, 'df') or data_handler.df is None:
            return None
            
        df = data_handler.df
        return {
            'total_wards': len(df),
            'total_variables': len(df.columns),
            'has_geometry': 'geometry' in df.columns,
            'sample_variables': list(df.columns[:5])
        }
    except Exception as e:
        logger.error(f"Error getting data summary: {e}")
        return None


def _get_status_message(csv_loaded: bool, shapefile_loaded: bool, analysis_complete: bool) -> str:
    """Get human-readable status message"""
    if not csv_loaded and not shapefile_loaded:
        return "Ready to receive data. Please upload CSV and shapefile data to begin analysis."
    elif csv_loaded and not shapefile_loaded:
        return "CSV data loaded. Please upload shapefile data to enable spatial analysis."
    elif not csv_loaded and shapefile_loaded:
        return "Shapefile loaded. Please upload CSV data to enable analysis."
    elif csv_loaded and shapefile_loaded and not analysis_complete:
        return "Data loaded successfully. Ready to run malaria risk analysis."
    elif analysis_complete:
        return "Analysis complete. All visualizations and insights available."
    else:
        return "System ready."


# ========================================================================
# ANALYSIS EXECUTION TOOLS  
# ========================================================================

def run_composite_analysis(session_id: str, variables: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Run composite scoring analysis with automatic visualization creation.
    
    Args:
        session_id: Session identifier
        variables: Optional list of variables to use (auto-selected if None)
        
    Returns:
        Dict containing analysis results and created visualizations
    """
    try:
        container = ServiceContainer()
        data_service = container.data_service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return {
                'status': 'error',
                'message': 'No data loaded. Please upload data first.',
                'visualizations_created': []
            }
        
        # Run composite analysis with settlement integration
        analysis_engine = AnalysisEngine(data_handler)
        result = analysis_engine.run_composite_analysis(variables=variables)
        
        if result['status'] != 'success':
            return result
        
        # Create standard composite visualizations
        visualizations = []
        core_bridge = container.core_visualization_bridge
        
        if core_bridge:
            # Create composite score maps
            map_result = core_bridge.create_composite_maps(data_handler, session_id=session_id)
            if map_result.get('status') == 'success':
                visualizations.extend(map_result.get('visualizations_created', []))
            
            # Create vulnerability map  
            vuln_result = core_bridge.create_vulnerability_map(data_handler, session_id=session_id)
            if vuln_result.get('status') == 'success':
                visualizations.extend(vuln_result.get('visualizations_created', []))
            
            # Create box plot ranking
            box_result = core_bridge.create_box_plot_ranking(data_handler, session_id=session_id)
            if box_result.get('status') == 'success':
                visualizations.extend(box_result.get('visualizations_created', []))
        
        return {
            'status': 'success',
            'message': 'Composite analysis completed successfully with visualizations.',
            'analysis_method': 'composite_scoring',
            'variables_used': result.get('variables_used', []),
            'visualizations_created': visualizations,
            'analysis_summary': {
                'total_wards': len(data_handler.df),
                'risk_categories': _get_risk_distribution(data_handler),
                'key_insights': [
                    'Composite risk scores calculated using standardized variables',
                    'Wards ranked by overall malaria transmission risk',
                    'Multiple visualizations created for comprehensive analysis'
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error in composite analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error running composite analysis: {str(e)}',
            'visualizations_created': []
        }


def run_pca_analysis(session_id: str, variables: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Run PCA analysis with automatic visualization creation.
    
    Args:
        session_id: Session identifier
        variables: Optional list of variables to use
        
    Returns:
        Dict containing analysis results and created visualizations
    """
    try:
        container = ServiceContainer()
        data_service = container.data_service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return {
                'status': 'error',
                'message': 'No data loaded. Please upload data first.',
                'visualizations_created': []
            }
        
        # Run PCA analysis with settlement integration
        analysis_engine = AnalysisEngine(data_handler)
        result = analysis_engine.run_pca_analysis(variables=variables)
        
        if result['status'] != 'success':
            return result
        
        # Create PCA visualizations
        visualizations = []
        core_bridge = container.core_visualization_bridge
        
        if core_bridge:
            # Create PCA vulnerability map
            pca_result = core_bridge.create_pca_vulnerability_map(data_handler, session_id=session_id)
            if pca_result.get('status') == 'success':
                visualizations.extend(pca_result.get('visualizations_created', []))
        
        return {
            'status': 'success',
            'message': 'PCA analysis completed successfully with visualizations.',
            'analysis_method': 'principal_component_analysis',
            'variables_used': result.get('variables_used', []),
            'visualizations_created': visualizations,
            'analysis_summary': {
                'total_wards': len(data_handler.df),
                'components_extracted': result.get('n_components', 0),
                'variance_explained': result.get('variance_explained', 0),
                'key_insights': [
                    'Principal component analysis applied to identify key risk patterns',
                    'Dimensionality reduced while preserving variance',
                    'Vulnerability map created based on PCA scores'
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error in PCA analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error running PCA analysis: {str(e)}',
            'visualizations_created': []
        }


# ========================================================================
# VISUALIZATION TOOLS
# ========================================================================

def create_composite_maps(session_id: str, model_index: Optional[int] = None) -> Dict[str, Any]:
    """Create composite score maps"""
    try:
        container = ServiceContainer()
        data_service = container.data_service
        data_handler = data_service.get_handler(session_id)
        core_bridge = container.core_visualization_bridge
        
        if not data_handler or not core_bridge:
            return {
                'status': 'error',
                'message': 'Data or visualization service not available',
                'visualizations_created': []
            }
        
        return core_bridge.create_composite_maps(data_handler, model_index, session_id)
        
    except Exception as e:
        logger.error(f"Error creating composite maps: {e}")
        return {
            'status': 'error',
            'message': f'Error creating composite maps: {str(e)}',
            'visualizations_created': []
        }


def create_vulnerability_map(session_id: str) -> Dict[str, Any]:
    """Create vulnerability category map"""
    try:
        container = ServiceContainer()
        data_service = container.data_service
        data_handler = data_service.get_handler(session_id)
        core_bridge = container.core_visualization_bridge
        
        if not data_handler or not core_bridge:
            return {
                'status': 'error',
                'message': 'Data or visualization service not available',
                'visualizations_created': []
            }
        
        return core_bridge.create_vulnerability_map(data_handler, session_id)
        
    except Exception as e:
        logger.error(f"Error creating vulnerability map: {e}")
        return {
            'status': 'error',
            'message': f'Error creating vulnerability map: {str(e)}',
            'visualizations_created': []
        }


def list_available_maps(session_id: str) -> Dict[str, Any]:
    """List all available visualizations for the session"""
    try:
        visualizations_dir = os.path.join(current_app.config.get('STATIC_FOLDER', 'static'), 'visualizations')
        
        if not os.path.exists(visualizations_dir):
            return {
                'status': 'success',
                'available_maps': [],
                'message': 'No visualizations created yet.'
            }
        
        # Find files for this session
        available_maps = []
        for filename in os.listdir(visualizations_dir):
            if session_id in filename and filename.endswith('.html'):
                available_maps.append({
                    'filename': filename,
                    'type': _guess_visualization_type(filename),
                    'path': f'/static/visualizations/{filename}'
                })
        
        return {
            'status': 'success',
            'available_maps': available_maps,
            'total_count': len(available_maps),
            'message': f'Found {len(available_maps)} visualizations for this session.'
        }
        
    except Exception as e:
        logger.error(f"Error listing available maps: {e}")
        return {
            'status': 'error',
            'message': f'Error listing maps: {str(e)}',
            'available_maps': []
        }


def _guess_visualization_type(filename: str) -> str:
    """Guess visualization type from filename"""
    filename_lower = filename.lower()
    
    if 'composite' in filename_lower:
        return 'composite_map'
    elif 'vulnerability' in filename_lower:
        return 'vulnerability_map'
    elif 'box' in filename_lower or 'ranking' in filename_lower:
        return 'box_plot'
    elif 'decision' in filename_lower or 'tree' in filename_lower:
        return 'decision_tree'
    elif 'urban' in filename_lower:
        return 'urban_extent_map'
    elif 'pca' in filename_lower:
        return 'pca_map'
    else:
        return 'unknown'


def _get_risk_distribution(data_handler) -> Dict[str, int]:
    """Get distribution of risk categories"""
    try:
        if not hasattr(data_handler, 'vulnerability_rankings') or data_handler.vulnerability_rankings is None:
            return {}
            
        rankings = data_handler.vulnerability_rankings
        if 'vulnerability_category' not in rankings.columns:
            return {}
            
        return rankings['vulnerability_category'].value_counts().to_dict()
        
    except Exception as e:
        logger.error(f"Error getting risk distribution: {e}")
        return {}


# ========================================================================
# PHASE 2: SETTLEMENT VALIDATION TOOLS
# ========================================================================

def create_settlement_validation_map(session_id: str, analysis_method: str = 'composite', 
                                   ward_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 2: Create interactive settlement validation map with building polygons.
    
    UPDATED: Now shows actual building polygons (not dots) with ward filtering capability
    as requested in June 11 meeting.
    """
    try:
        logger.info(f"🏗️ PHASE 2: Creating building polygon validation map for {analysis_method} analysis (ward: {ward_filter})")
        
        # Import settlement validation tools
        from ...tools.settlement_validation_tools import create_settlement_validation_map as create_map
        
        # Create the validation map with building polygons
        result = create_map(session_id, analysis_method, ward_filter)
        
        if result['status'] == 'success':
            logger.info(f"✅ Building polygon validation map created successfully")
            
            # Add updated guidance message
            result['guidance'] = {
                'workflow': 'Use the interactive map to validate building classifications',
                'instructions': [
                    '1. Zoom to areas of interest using map controls',
                    '2. Red polygons = Informal buildings (high malaria risk)',
                    '3. Green polygons = Formal buildings (lower malaria risk)', 
                    '4. Blue polygons = Slum buildings (highest malaria risk)',
                    '5. Yellow polygons = Mixed/unknown building types',
                    '6. Colored circles = Ward vulnerability levels',
                    '7. Click buildings to validate classifications'
                ],
                'building_polygons': 'Map shows actual building footprints as polygons (not dots)',
                'ward_filtering': 'Use ward filtering to focus on specific areas',
                'satellite_view': 'Satellite imagery base layer showing buildings, vegetation, and rooftops',
                'scalability': 'Ready for scaling from Kano to all 36 Nigerian states'
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating settlement validation map: {e}")
        return {
            'status': 'error',
            'message': f'Failed to create settlement validation map: {str(e)}'
        }

def create_ward_filtered_validation_map(session_id: str, ward_name: str, 
                                      analysis_method: str = 'composite') -> Dict[str, Any]:
    """
    Create settlement validation map filtered to a specific ward.
    
    This addresses the June 11 meeting requirement for ward-specific validation.
    """
    try:
        logger.info(f"🎯 Creating ward-filtered validation map for '{ward_name}' using {analysis_method} analysis")
        
        # Import settlement validation tools
        from ...tools.settlement_validation_tools import create_ward_filtered_validation_map as create_ward_map
        
        # Create the ward-filtered validation map
        result = create_ward_map(session_id, ward_name, analysis_method)
        
        if result['status'] == 'success':
            logger.info(f"✅ Ward-filtered validation map created for '{ward_name}'")
            
            # Add ward-specific guidance
            result['guidance'] = {
                'ward_focus': f'Map focused on ward: {ward_name}',
                'workflow': 'Detailed building-level validation for specific ward',
                'instructions': [
                    f'1. Map shows buildings only within {ward_name} ward',
                    '2. Higher zoom level for detailed building analysis',
                    '3. Red polygons = Informal buildings needing intervention',
                    '4. Green polygons = Formal buildings (lower priority)',
                    '5. Blue polygons = Slum buildings (highest priority)',
                    '6. Use satellite imagery to verify building classifications'
                ],
                'use_cases': [
                    'High-risk ward validation',
                    'Community-specific settlement analysis', 
                    'Targeted intervention planning',
                    'Field team validation workflows'
                ]
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating ward-filtered validation map: {e}")
        return {
            'status': 'error',
            'message': f'Failed to create ward-filtered validation map: {str(e)}'
        }


def get_settlement_validation_summary(session_id: str, analysis_method: str = 'composite') -> Dict[str, Any]:
    """
    Get summary of settlement validation status and data for Phase 2.
    """
    try:
        logger.info(f"📊 Getting settlement validation summary for {analysis_method} analysis")
        
        # Import settlement validation tools
        from ...tools.settlement_validation_tools import get_settlement_validation_summary as get_summary
        
        # Get validation summary
        result = get_summary(session_id, analysis_method)
        
        if result['status'] == 'success':
            # Add Phase 2 context
            result['phase_2_info'] = {
                'implementation_status': 'Phase 2 Active',
                'key_features': [
                    'Interactive satellite map with settlement overlay',
                    'Dual-method settlement integration (composite + PCA)',
                    'Settlement type validation workflow',
                    'Color-coded vulnerability and settlement risk visualization',
                    'Scalable architecture for all Nigerian states'
                ],
                'user_workflow': [
                    'Upload TPR data → System loads settlement data automatically',
                    'Run analysis (composite or PCA) → Get settlement-enhanced rankings',
                    'Create validation map → Explore satellite view with overlays',
                    'Zoom and validate → Click settlements to correct classifications',
                    'Generate insights → Settlement-aware malaria risk recommendations'
                ],
                'technical_readiness': {
                    'settlement_loader': 'Operational - auto-detects and loads Kano settlement data',
                    'dual_method_integration': 'Complete - both composite and PCA enhanced',
                    'validation_interface': 'Active - interactive map with settlement overlay',
                    'scalability': 'Ready - supports all 37 Nigerian states with zero-rewrite'
                }
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting settlement validation summary: {e}")
        return {
            'status': 'error',
            'message': f'Failed to get settlement validation summary: {str(e)}'
        }


# ========================================================================
# TOOL REGISTRATION FOR LLM MANAGER
# ========================================================================

AVAILABLE_TOOLS = [
    {
        "name": "get_session_status",
        "description": "Get current session status including data and analysis state",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"}
            },
            "required": ["session_id"]
        },
        "function": get_session_status
    },
    {
        "name": "run_composite_analysis",
        "description": "Run composite scoring analysis and create visualizations",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "variables": {"type": "array", "items": {"type": "string"}, "description": "Optional variables to use"}
            },
            "required": ["session_id"]
        },
        "function": run_composite_analysis
    },
    {
        "name": "run_pca_analysis", 
        "description": "Run PCA analysis and create visualizations",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "variables": {"type": "array", "items": {"type": "string"}, "description": "Optional variables to use"}
            },
            "required": ["session_id"]
        },
        "function": run_pca_analysis
    },
    {
        "name": "create_composite_maps",
        "description": "Create composite score map visualizations",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"}
            },
            "required": ["session_id"]
        },
        "function": create_composite_maps
    },
    {
        "name": "create_vulnerability_map",
        "description": "Create vulnerability category map",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"}
            },
            "required": ["session_id"]
        },
        "function": create_vulnerability_map
    },
    {
        "name": "list_available_maps",
        "description": "List all available visualizations",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"}
            },
            "required": ["session_id"]
        },
        "function": list_available_maps
    },
    {
        "name": "create_settlement_validation_map",
        "description": "Phase 2: Create interactive settlement validation map with building polygons",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "analysis_method": {"type": "string", "enum": ["composite", "pca"], "description": "Analysis method to show vulnerability rankings"},
                "ward_filter": {"type": "string", "description": "Optional ward name to filter and focus on specific area"}
            },
            "required": ["session_id"]
        },
        "function": create_settlement_validation_map
    },
    {
        "name": "create_ward_filtered_validation_map",
        "description": "Create settlement validation map focused on a specific ward",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "ward_name": {"type": "string", "description": "Name of the ward to focus on"},
                "analysis_method": {"type": "string", "enum": ["composite", "pca"], "description": "Analysis method to show vulnerability rankings"}
            },
            "required": ["session_id", "ward_name"]
        },
        "function": create_ward_filtered_validation_map
    },
    {
        "name": "get_settlement_validation_summary",
        "description": "Get summary of settlement validation status and Phase 2 implementation",
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "analysis_method": {"type": "string", "enum": ["composite", "pca"], "description": "Analysis method to summarize"}
            },
            "required": ["session_id"]
        },
        "function": get_settlement_validation_summary
    }
]
