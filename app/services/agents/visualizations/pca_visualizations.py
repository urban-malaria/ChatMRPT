"""
PCA Method Visualization Functions

Clean implementations of PCA analysis visualizations designed specifically
for the VisualizationAgent. These functions work directly with the unified dataset
structure and provide agent-friendly interfaces.

Includes:
1. PCA Vulnerability Map - Risk classification mapping based on PCA analysis
"""

import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
from typing import Dict, Any, List, Optional

from .core_utils import (
    prepare_unified_dataset,
    save_agent_visualization,
    get_vulnerability_colors,
    create_geojson_from_gdf,
    calculate_data_statistics
)

logger = logging.getLogger(__name__)

def create_agent_pca_vulnerability_map(unified_dataset: gpd.GeoDataFrame,
                                     session_id: str = 'default') -> Dict[str, Any]:
    """
    Create PCA vulnerability classification map
    
    Args:
        unified_dataset: Enhanced unified dataset GeoDataFrame
        session_id: Session identifier
        
    Returns:
        Dictionary with visualization results
    """
    try:
        logger.info("🗺️ Creating agent PCA vulnerability map...")
        
        # Prepare dataset
        required_columns = ['pca_score', 'pca_rank', 'pca_category', 'WardName']
        prep_result = prepare_unified_dataset(unified_dataset, required_columns)
        
        if prep_result['status'] != 'success':
            return prep_result
        
        data = prep_result['data']
        
        # Get vulnerability colors
        vuln_colors = get_vulnerability_colors()
        
        # Create GeoJSON
        geojson = create_geojson_from_gdf(data)
        
        # Create hover text - handle NaN values properly
        hover_text = []
        for _, row in data.iterrows():
            # Handle NaN values in pca_rank and pca_score
            pca_score_str = f"{row['pca_score']:.3f}" if pd.notna(row['pca_score']) else "N/A"
            pca_rank_str = f"{int(row['pca_rank'])}" if pd.notna(row['pca_rank']) else "N/A"
            pca_category_str = str(row['pca_category']) if pd.notna(row['pca_category']) else "N/A"
            
            text = (f"<b>{row['WardName']}</b><br>"
                   f"PCA Score: {pca_score_str}<br>"
                   f"Rank: {pca_rank_str}<br>"
                   f"Category: {pca_category_str}")
            hover_text.append(text)
        
        # Create choropleth with custom colors based on vulnerability categories
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=data.index,
            z=data['pca_rank'],  # Use rank for coloring (lower rank = higher risk)
            colorscale=[[0, vuln_colors['Low']], 
                       [0.33, vuln_colors['Medium']], 
                       [0.66, vuln_colors['High']], 
                       [1, vuln_colors['Very High']]],
            marker_line_color='black',
            marker_line_width=0.5,
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=hover_text,
            colorbar=dict(
                title=dict(
                    text="Vulnerability Rank",
                    font=dict(size=12)
                ),
                tickmode='array',
                tickvals=[data['pca_rank'].min(), data['pca_rank'].median(), data['pca_rank'].max()],
                ticktext=["High Risk", "Medium Risk", "Low Risk"]
            ),
            zmin=data['pca_rank'].min(),
            zmax=data['pca_rank'].max()
        ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': "Vulnerability Classification Map (PCA Method)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            mapbox=dict(
                style="carto-positron",
                center=prep_result['map_center'],
                zoom=prep_result['zoom_level']
            ),
            height=600,
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        # Calculate statistics
        category_counts = data['pca_category'].value_counts().to_dict()
        score_stats = calculate_data_statistics(data['pca_score'])
        
        # Save visualization
        filename = "pca_vulnerability_map"
        save_result = save_agent_visualization(
            fig, filename, session_id, 'vulnerability_map'
        )
        
        if save_result['status'] == 'success':
            return {
                'status': 'success',
                'message': 'PCA vulnerability map created successfully',
                'visualization_type': 'pca_vulnerability_map',
                'method': 'pca',
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'plotly_json': save_result['plotly_json'],
                'statistics': {
                    'total_wards': len(data),
                    'category_distribution': category_counts,
                    'score_statistics': score_stats
                },
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent PCA vulnerability map: {e}")
        return {
            'status': 'error',
            'message': f'PCA vulnerability map creation failed: {str(e)}',
            'visualization_type': 'pca_vulnerability_map'
        } 