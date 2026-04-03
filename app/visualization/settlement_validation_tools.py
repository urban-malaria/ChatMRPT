"""
Settlement Validation Tools for ChatMRPT - Building Classification Maps

Creates interactive maps showing:
- Building polygons colored by settlement type (formal/informal/non-residential)
- Transparent overlay for roof visibility
- Interactive toggles for settlement types
- Satellite imagery with street names
"""

import logging
import os
import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
from typing import Dict, Any, Optional, List
import numpy as np
from flask import current_app

logger = logging.getLogger(__name__)

# Settlement type colors with transparency for roof visibility
SETTLEMENT_TYPE_COLORS = {
    'formal': '#00FF00',       # Bright Green 
    'informal': '#FF0000',     # Bright Red
    'non residential': '#0000FF'  # Bright Blue (matching actual data name)
}

# Alternative lookup for consistent access
def get_settlement_color(settlement_type):
    # Handle variations in settlement type names
    color_map = {
        'formal': '#00FF00',
        'informal': '#FF0000', 
        'non residential': '#0000FF',
        'non-residential': '#0000FF',
        'nonresidential': '#0000FF'
    }
    return color_map.get(settlement_type, '#CCCCCC')

# Transparency settings for better roof visibility
SETTLEMENT_FILL_OPACITY = 0.4  # Balanced transparency for visibility while showing roofs
SETTLEMENT_BORDER_OPACITY = 0.9  # Strong borders for clarity

def load_building_data(session_id: str = None) -> Optional[gpd.GeoDataFrame]:
    """Load building polygon data dynamically from any available source"""
    try:
        # Use SettlementLoader for dynamic path resolution
        from ..data.settlement_loader import SettlementLoader
        
        # Initialize loader with session (or dummy session if none provided)
        loader = SettlementLoader(session_id or "default")
        
        # Load settlements using auto-detection
        buildings = loader.load_settlements("auto")
        
        if buildings is None:
            logger.error("No building/settlement data found in any search location")
            return None
        
        logger.info(f"🏗️ Loaded building data from dynamic source")
        
        # Ensure we have the required columns for visualization
        required_cols = ['geometry']
        settlement_type_cols = ['sttlmn_', 'settlement_type']
        
        # Check for geometry
        if 'geometry' not in buildings.columns:
            logger.error("No geometry column found in building data")
            return None
        
        # Find settlement type column
        settlement_col = None
        for col in settlement_type_cols:
            if col in buildings.columns:
                settlement_col = col
                break
        
        if settlement_col is None:
            logger.error("No settlement type column found in building data")
            return None
        
        # Standardize to 'sttlmn_' for consistency with existing code
        if settlement_col != 'sttlmn_':
            buildings['sttlmn_'] = buildings[settlement_col]
        
        # Clean the data
        buildings = buildings.dropna(subset=['sttlmn_'])
        
        # Clean settlement type names
        buildings['sttlmn_'] = buildings['sttlmn_'].astype(str).str.strip().str.lower()
        
        # Standardize settlement type names
        settlement_mapping = {
            'formal': 'formal',
            'informal': 'informal',
            'non residential': 'non residential',
            'non-residential': 'non residential',
            'nonresidential': 'non residential',
            'mixed': 'non residential'
        }
        
        buildings['sttlmn_'] = buildings['sttlmn_'].map(settlement_mapping).fillna('informal')
        
        logger.info(f"✅ Loaded {len(buildings):,} building polygons")
        logger.info(f"🏘️ Settlement types: {sorted(buildings['sttlmn_'].unique())}")
        
        # Log settlement type counts
        settlement_counts = buildings['sttlmn_'].value_counts()
        for settlement_type, count in settlement_counts.items():
            logger.info(f"   {settlement_type}: {count:,}")
        
        return buildings
        
    except Exception as e:
        logger.error(f"Error loading building data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def load_ward_boundaries(session_id: str = None) -> Optional[gpd.GeoDataFrame]:
    """Load ward boundaries dynamically from available sources"""
    try:
        # Define search locations for ward boundaries
        search_locations = [
            "data",  # Project data directory
            "instance/uploads" + (f"/{session_id}" if session_id else ""),
            "sessions" + (f"/{session_id}" if session_id else ""),
            ".",  # Current directory
        ]
        
        # Define ward boundary file patterns
        ward_patterns = [
            "*ward*.shp",
            "*Ward*.shp", 
            "*WARD*.shp",
            "*boundaries*.shp",
            "*Boundaries*.shp",
            "Kano_State.shp",  # Specific Kano file
            "kano_state.shp",
            "processed.shp",  # Session processed file
            "*.shp"  # Any shapefile as fallback
        ]
        
        import glob
        
        for location in search_locations:
            if not os.path.exists(location):
                continue
                
            for pattern in ward_patterns:
                search_pattern = os.path.join(location, pattern)
                matching_files = glob.glob(search_pattern)
                
                for file_path in matching_files:
                    try:
                        # Test if this is actually a ward boundary file
                        test_gdf = gpd.read_file(file_path, rows=5)
                        
                        # Check if it looks like ward boundaries
                        ward_indicators = ['ward', 'boundary', 'admin', 'lga', 'state']
                        has_ward_data = any(
                            any(indicator in col.lower() for indicator in ward_indicators)
                            for col in test_gdf.columns
                        )
                        
                        # Must have geometry and some administrative data
                        if 'geometry' in test_gdf.columns and has_ward_data:
                            # Load full file
                            wards = gpd.read_file(file_path)
                            logger.info(f"🗺️ Loading ward boundaries from: {file_path}")
                            logger.info(f"✅ Loaded {len(wards)} ward boundaries")
                            
                            # Log available columns for debugging
                            ward_name_cols = [col for col in wards.columns 
                                            if any(term in col.lower() for term in ['ward', 'name', 'area'])]
                            logger.info(f"Ward name columns: {ward_name_cols}")
                            
                            return wards
                            
                    except Exception as e:
                        logger.debug(f"Could not load {file_path} as ward boundaries: {e}")
                        continue
        
        logger.warning("No ward boundaries found in any search location")
        return None
        
    except Exception as e:
        logger.error(f"Error loading ward boundaries: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_building_classification_map(
    session_id: str, 
    ward_name: Optional[str] = None,
    zoom_level: int = 11
) -> Dict[str, Any]:
    """
    Create building classification map with satellite background
    
    Args:
        session_id: User session ID
        ward_name: Optional ward name to highlight (dims others)
        zoom_level: Map zoom level (higher = more zoomed in)
    
    Returns:
        Dictionary with map creation results
    """
    try:
        logger.info(f"🗺️ Creating building classification map for session {session_id}")
        
        # Load building data
        buildings = load_building_data(session_id)
        if buildings is None:
            return {
                'status': 'error',
                'message': 'Failed to load building polygon data'
            }
        
        # Optimize for performance - sample buildings ONLY for full city view
        original_count = len(buildings)
        if ward_name is None and len(buildings) > 50000:  # Only sample for full city view
            logger.info(f"🔄 Sampling {len(buildings):,} buildings for full city performance...")
            buildings = buildings.sample(n=50000, random_state=42)
            logger.info(f"✅ Using {len(buildings):,} sampled buildings for full city view")
        elif ward_name is not None:
            logger.info(f"🎯 Ward-specific view: Using ALL {len(buildings):,} buildings for detailed analysis")
        
        # Load ward boundaries
        wards = load_ward_boundaries(session_id)
        
        # Filter buildings to specific ward if requested
        if ward_name and wards is not None:
            target_ward = wards[wards['WardName'].str.contains(ward_name, case=False, na=False)]
            if not target_ward.empty:
                ward_geom = target_ward.iloc[0].geometry
                ward_bounds = ward_geom.bounds
                building_bounds = buildings.total_bounds
                
                # Check if ward overlaps with building data area
                ward_overlap_x = (building_bounds[0] <= ward_bounds[2]) and (building_bounds[2] >= ward_bounds[0])
                ward_overlap_y = (building_bounds[1] <= ward_bounds[3]) and (building_bounds[3] >= ward_bounds[1])
                
                if ward_overlap_x and ward_overlap_y:
                    # Ward overlaps with building area - use spatial intersection
                    try:
                        buildings_in_ward = buildings[buildings.geometry.intersects(ward_geom)]
                        if not buildings_in_ward.empty:
                            buildings = buildings_in_ward
                            logger.info(f"🎯 Filtered to {len(buildings):,} buildings intersecting {ward_name} ward")
                        else:
                            # Try with small buffer
                            ward_buffered = ward_geom.buffer(0.005)  # ~500m buffer
                            buildings_in_ward = buildings[buildings.geometry.intersects(ward_buffered)]
                            if not buildings_in_ward.empty:
                                buildings = buildings_in_ward
                                logger.info(f"🎯 Filtered to {len(buildings):,} buildings near {ward_name} ward")
                    except Exception as e:
                        logger.error(f"Spatial intersection failed for {ward_name}: {e}")
                else:
                    # Ward doesn't overlap - find closest buildings by geographic proximity
                    logger.info(f"🌍 {ward_name} ward is outside building data area, finding closest buildings...")
                    
                    ward_center = ward_geom.centroid
                    building_centroids = buildings.geometry.centroid
                    
                    # Calculate distances from ward center to all buildings
                    distances = building_centroids.distance(ward_center)
                    
                    # Take buildings within reasonable distance (e.g., closest 5% or within 0.1 degrees)
                    distance_threshold = min(distances.quantile(0.05), 0.1)  # 5% closest or 0.1 degrees (~11km)
                    
                    closest_buildings = buildings[distances <= distance_threshold]
                    if not closest_buildings.empty:
                        buildings = closest_buildings
                        logger.info(f"🎯 Using {len(buildings):,} buildings closest to {ward_name} ward (within {distance_threshold:.3f} degrees)")
                    else:
                        # No buildings found - return empty dataset instead of all buildings
                        logger.warning(f"❌ No buildings found near {ward_name} ward - returning empty map")
                        buildings = buildings.iloc[0:0]  # Empty GeoDataFrame with same structure
            else:
                logger.warning(f"Ward '{ward_name}' not found in boundaries")
        
        # Handle empty buildings dataset
        if buildings.empty:
            logger.warning(f"📍 No buildings to display for {ward_name or 'full city'}")
            # Use ward center if available, otherwise default to Kano center
            if ward_name and wards is not None:
                target_ward = wards[wards['WardName'].str.contains(ward_name, case=False, na=False)]
                if not target_ward.empty:
                    ward_bounds = target_ward.total_bounds
                    center_lat = (ward_bounds[1] + ward_bounds[3]) / 2
                    center_lon = (ward_bounds[0] + ward_bounds[2]) / 2
                else:
                    center_lat, center_lon = 12.0, 8.5  # Default Kano center
            else:
                center_lat, center_lon = 12.0, 8.5  # Default Kano center
        else:
            # Calculate map center from buildings
            bounds = buildings.total_bounds
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2
        
        logger.info(f"📍 Map center: {center_lat:.4f}, {center_lon:.4f}")
        
        # Create the map
        fig = go.Figure()
        
        # Add building polygons by settlement type
        for settlement_type in sorted(buildings['sttlmn_'].unique()):
            if pd.isna(settlement_type):
                continue
                
            settlement_buildings = buildings[buildings['sttlmn_'] == settlement_type]
            
            # Convert geometries to plotly format
            lons, lats = [], []
            for geom in settlement_buildings.geometry:
                if geom.geom_type == 'Polygon':
                    x, y = geom.exterior.coords.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
                elif geom.geom_type == 'MultiPolygon':
                    for poly in geom.geoms:
                        x, y = poly.exterior.coords.xy
                        lons.extend(list(x) + [None])
                        lats.extend(list(y) + [None])
            
            # Add settlement type layer with transparency
            color = get_settlement_color(settlement_type)
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor=color,
                line=dict(width=1.5, color=color),
                opacity=SETTLEMENT_FILL_OPACITY,
                name=f'{settlement_type.title()} Settlement',
                hovertemplate=f'<b>{settlement_type.title()} Settlement</b><br>' +
                             'Building footprint - transparent overlay<br>' +
                             '<extra></extra>',
                showlegend=True,
                visible=True  # Will be controlled by toggles
            ))
        
        # Add all ward boundaries for context (always show administrative boundaries)
        if wards is not None:
            logger.info(f"🏛️ Adding {len(wards)} ward boundaries to map")
            
            # Add all ward boundaries as thin gray lines
            all_ward_lons, all_ward_lats = [], []
            for _, ward_row in wards.iterrows():
                ward_geom = ward_row.geometry
                
                if ward_geom.geom_type == 'Polygon':
                    x, y = ward_geom.exterior.coords.xy
                    all_ward_lons.extend(list(x) + [None])
                    all_ward_lats.extend(list(y) + [None])
                elif ward_geom.geom_type == 'MultiPolygon':
                    for poly in ward_geom.geoms:
                        x, y = poly.exterior.coords.xy
                        all_ward_lons.extend(list(x) + [None])
                        all_ward_lats.extend(list(y) + [None])
            
            # Add ward boundaries layer
            fig.add_trace(go.Scattermapbox(
                lon=all_ward_lons,
                lat=all_ward_lats,
                mode='lines',
                line=dict(width=2, color='rgba(60, 60, 60, 0.8)'),  # Dark gray boundaries for better contrast
                name='Ward Boundaries',
                hovertemplate='<b>Ward Boundary</b><br>' +
                             'Administrative boundary<br>' +
                             '<extra></extra>',
                showlegend=True,
                visible=True
            ))
            
            # Add ward centroids with names for major wards (sample to avoid overcrowding)
            if len(wards) > 20:
                # For large number of wards, sample every 10th ward for labels
                label_wards = wards.iloc[::10]
            else:
                # For smaller number of wards, label all
                label_wards = wards
            
            ward_center_lons, ward_center_lats, ward_names = [], [], []
            for _, ward_row in label_wards.iterrows():
                try:
                    centroid = ward_row.geometry.centroid
                    ward_center_lons.append(centroid.x)
                    ward_center_lats.append(centroid.y)
                    ward_names.append(ward_row.get('WardName', 'Unknown Ward'))
                except Exception as e:
                    logger.debug(f"Could not get centroid for ward: {e}")
                    continue
            
            if ward_center_lons:
                fig.add_trace(go.Scattermapbox(
                    lon=ward_center_lons,
                    lat=ward_center_lats,
                    mode='text',
                    text=ward_names,
                    textfont=dict(size=10, color='rgba(40, 40, 40, 0.9)', family='Arial, sans-serif'),  # Improved text styling
                    name='Ward Names',
                    hovertemplate='<b>%{text}</b><br>' +
                                 'Ward centroid<br>' +
                                 '<extra></extra>',
                    showlegend=True,
                    visible=True
                ))
        
        # Add ward highlighting if specified
        if ward_name and wards is not None:
            # Look for ward by name (case-insensitive)
            target_ward = wards[wards['WardName'].str.contains(ward_name, case=False, na=False)]
            
            if not target_ward.empty:
                ward_geom = target_ward.iloc[0].geometry
                
                # Add highlighted ward boundary
                if ward_geom.geom_type == 'Polygon':
                    x, y = ward_geom.exterior.coords.xy
                    ward_lons, ward_lats = list(x), list(y)
                elif ward_geom.geom_type == 'MultiPolygon':
                    ward_lons, ward_lats = [], []
                    for poly in ward_geom.geoms:
                        x, y = poly.exterior.coords.xy
                        ward_lons.extend(list(x) + [None])
                        ward_lats.extend(list(y) + [None])
                
                fig.add_trace(go.Scattermapbox(
                    lon=ward_lons,
                    lat=ward_lats,
                    mode='lines',
                    line=dict(width=3, color='red'),
                    name=f'{ward_name} Ward Boundary',
                    hovertemplate=f'<b>{ward_name} Ward</b><br>' +
                                 'Highlighted ward boundary<br>' +
                                 '<extra></extra>',
                    showlegend=True
                ))
                
                # Update center to ward
                ward_bounds = target_ward.total_bounds
                center_lat = (ward_bounds[1] + ward_bounds[3]) / 2
                center_lon = (ward_bounds[0] + ward_bounds[2]) / 2
                zoom_level = 13  # Zoom in for ward view
        
        # Configure map layout starting with OpenStreetMap (reliable, no token)
        fig.update_layout(
            mapbox=dict(
                style="open-street-map",  # Start with reliable street map, users can switch to satellite
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom_level,
                # Add custom Esri World Imagery layer
                layers=[
                    dict(
                        below="traces",
                        sourcetype="raster",
                        sourceattribution="© Esri, Maxar, Earthstar Geographics",
                        source=["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                        visible=False,  # Hidden by default
                        name="esri-satellite"
                    )
                ]
            ),
            title=dict(
                text=f"Building Classification Map{f' - {ward_name} Ward' if ward_name else ''}",
                x=0.5,
                font=dict(size=16, color='#2c3e50')
            ),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            height=700,
            width=1200
        )
        
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        map_name = f"building_classification_map_{ward_name or 'full'}_{timestamp}"
        
        session_folder = f"instance/uploads/{session_id}"
        os.makedirs(session_folder, exist_ok=True)
        
        filename = f"{map_name}.html"
        file_path = os.path.join(session_folder, filename)
        
        # Use fig.write_html with custom configuration for compatibility
        fig.write_html(
            file_path,
            include_plotlyjs=True,  # Use CDN to avoid inline scripts
            config={
                'responsive': True,
                'displayModeBar': True,
                'modeBarButtonsToAdd': ['drawrect', 'eraseshape'],
                'scrollZoom': True,
                'displaylogo': False
            },
            # Add custom HTML in the div_id parameter area
            div_id="plotly-div"
        )
        
        web_path = f"/serve_viz_file/{session_id}/{filename}"
        
        logger.info(f"✅ Building classification map created: {filename}")
        
        return {
            'status': 'success',
            'message': f'Building classification map created successfully{f" for {ward_name} ward" if ward_name else ""}',
            'file_path': file_path,
            'web_path': web_path,
            'filename': filename,
            'map_type': 'building_classification',
            'ward_name': ward_name,
            'building_count': len(buildings),
            'settlement_types': sorted(buildings['sttlmn_'].unique()),
            'note': 'Use map controls to toggle layers and switch between street/satellite view'
        }
        
        
    except Exception as e:
        logger.error(f"Error creating building classification map: {e}")
        return {
            'status': 'error',
            'message': f'Failed to create building classification map: {str(e)}'
        }

def create_settlement_validation_map(session_id: str, ward_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Create settlement validation map - wrapper for building classification map
    
    Args:
        session_id: User session ID
        ward_name: Optional ward name to highlight
    
    Returns:
        Dictionary with map creation results
    """
    return create_building_classification_map(session_id, ward_name)

def get_building_statistics(session_id: str) -> Dict[str, Any]:
    """Get statistics about building data"""
    try:
        buildings = load_building_data(session_id)
        if buildings is None:
            return {'status': 'error', 'message': 'Failed to load building data'}
        
        # Calculate statistics
        settlement_stats = buildings['sttlmn_'].value_counts().to_dict()
        
        return {
            'status': 'success',
            'total_buildings': len(buildings),
            'settlement_distribution': settlement_stats,
            'geographic_bounds': buildings.total_bounds.tolist()
        }
        
    except Exception as e:
        logger.error(f"Error getting building statistics: {e}")
        return {'status': 'error', 'message': str(e)}# This file has been removed during ChatMRPT streamlining
# Settlement validation tools were identified as non-essential and removed to focus on core functionality