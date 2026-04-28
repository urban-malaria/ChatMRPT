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

from .geo_utils import (
    prepare_unified_dataset,
    save_agent_visualization,
    get_vulnerability_colors,
    create_geojson_from_gdf,
    calculate_data_statistics
)
from app.utils.map_overlays import add_lga_boundary_overlay
from app.utils.visualization_controls import inject_lga_hover_highlight

logger = logging.getLogger(__name__)

def create_agent_pca_vulnerability_map(unified_dataset: gpd.GeoDataFrame,
                                     session_id: str = 'default',
                                     return_figure: bool = False) -> Dict[str, Any]:
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
        
        # Calculate LGA-level rankings (average ward rank per LGA, then rank LGAs)
        lga_col = 'LGAName' if 'LGAName' in data.columns else ('LGA' if 'LGA' in data.columns else None)
        if lga_col and lga_col in data.columns:
            # Calculate mean rank per LGA (lower rank = higher risk)
            lga_mean_ranks = data.groupby(lga_col)['pca_rank'].mean()
            # Rank LGAs by their average ward rank (1 = highest risk LGA)
            lga_rankings = lga_mean_ranks.rank(method='min').astype(int)
            # Determine LGA category based on ranking
            total_lgas = len(lga_rankings)
            def get_lga_category(lga_rank, total):
                if pd.isna(lga_rank):
                    return 'unknown'
                pct = lga_rank / total
                if pct <= 0.25:
                    return 'very high'
                elif pct <= 0.50:
                    return 'high'
                elif pct <= 0.75:
                    return 'medium'
                else:
                    return 'low'
            lga_categories = {lga: get_lga_category(rank, total_lgas) for lga, rank in lga_rankings.items()}
            # Map back to wards
            data['lga_rank'] = data[lga_col].map(lga_rankings)
            data['lga_category'] = data[lga_col].map(lga_categories)
        else:
            data['lga_rank'] = pd.NA
            data['lga_category'] = 'unknown'

        # Create hover text in user's requested format:
        # Ward: Ndiagbo
        # LGA: Mashegu
        # Ward rank: 113 (medium risk)
        # LGA rank: 8 (high)
        hover_text = []
        for _, row in data.iterrows():
            ward_name = str(row['WardName'])

            # Try multiple LGA column names
            if 'LGAName' in data.columns:
                lga_name = str(row.get('LGAName', 'Unknown'))
            elif 'LGA' in data.columns:
                lga_name = str(row.get('LGA', 'Unknown'))
            else:
                lga_name = 'Unknown'

            pca_rank = row['pca_rank']
            pca_category = str(row.get('pca_category', 'Unknown')).lower()
            lga_rank = row.get('lga_rank', pd.NA)
            lga_cat = str(row.get('lga_category', 'unknown')).lower()

            text = f"<b>Ward:</b> {ward_name}<br>"
            text += f"<b>LGA:</b> {lga_name}<br>"

            if pd.notna(pca_rank):
                text += f"<b>Ward rank:</b> {int(pca_rank)} ({pca_category})<br>"
            else:
                text += f"<b>Ward rank:</b> Not ranked<br>"

            if pd.notna(lga_rank):
                text += f"<b>LGA rank:</b> {int(lga_rank)} ({lga_cat})"

            hover_text.append(text)

        # Get rank range for colorbar
        min_rank = data['pca_rank'].min()
        max_rank = data['pca_rank'].max()
        median_rank = data['pca_rank'].median()

        # Invert z-values so highest risk (rank 1) appears at TOP of colorbar
        z_inverted = max_rank + 1 - data['pca_rank']

        # Create choropleth with custom colors based on vulnerability categories
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=data.index,
            z=z_inverted,  # Inverted: higher value = higher risk (at top)
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
                # Inverted tickvals: bottom=1 (low risk), top=max_rank (high risk)
                tickvals=[1, max_rank + 1 - median_rank, max_rank],
                ticktext=[f"Low Risk ({int(max_rank)})", f"Medium ({int(median_rank)})", f"High Risk ({int(min_rank)})"]
            ),
            zmin=1,
            zmax=max_rank
        ))
        
        # Add LGA boundary overlay
        add_lga_boundary_overlay(fig, data)

        # Update layout
        fig.update_layout(
            title={
                'text': "Vulnerability Classification Map (PCA Method)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            mapbox=dict(
                style="open-street-map",
                center=prep_result['map_center'],
                zoom=prep_result['zoom_level']
            ),
            height=600,
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        # Return figure object directly when caller handles its own HTML output
        if return_figure:
            return {'status': 'success', 'fig': fig}

        # Calculate statistics
        category_counts = data['pca_category'].value_counts().to_dict()
        score_stats = calculate_data_statistics(data['pca_score'])

        # Save visualization
        filename = "pca_vulnerability_map"
        save_result = save_agent_visualization(
            fig, filename, session_id, 'vulnerability_map'
        )

        # Inject LGA hover highlighting
        if save_result.get('status') == 'success' and save_result.get('file_path'):
            try:
                lga_codes = data['LGACode'].fillna('').astype(str).tolist() if 'LGACode' in data.columns else []
                if lga_codes:
                    inject_lga_hover_highlight(save_result['file_path'], lga_codes)
            except Exception as hover_err:
                logger.warning(f"Failed to inject LGA hover highlight: {hover_err}")

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