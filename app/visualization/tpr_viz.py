"""
TPR Visualization Service for generating maps and charts.
"""

import os
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
from pathlib import Path
from typing import Dict, Any, Optional

from app.services.shapefile_fetcher import ShapefileExtractor

logger = logging.getLogger(__name__)

class TPRVisualizationService:
    """Service for generating TPR visualizations."""
    
    def __init__(self, session_id: str):
        """Initialize visualization service."""
        self.session_id = session_id
        self.output_dir = Path(f'instance/uploads/{session_id}/visualizations')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.shapefile_extractor = ShapefileExtractor()
        
    def create_tpr_distribution_map(self, 
                                  tpr_df: pd.DataFrame,
                                  state_name: str,
                                  title: str = None) -> str:
        """
        Create an interactive TPR distribution map.
        
        Args:
            tpr_df: DataFrame with TPR results (WardName, LGA, TPR columns)
            state_name: Name of the state
            title: Map title
            
        Returns:
            Path to the generated HTML map file
        """
        try:
            logger.info(f"Creating TPR distribution map for {state_name}")
            
            # Load shapefile for the state
            state_shapefile = self.shapefile_extractor._filter_state_data(state_name)
            
            if state_shapefile is None or state_shapefile.empty:
                logger.warning(f"No shapefile data for {state_name}, creating table view instead")
                return self._create_table_view(tpr_df, state_name, title)
            
            # Merge TPR data with shapefile
            merged_gdf = state_shapefile.merge(
                tpr_df[['WardName', 'TPR', 'Tested', 'Positive', 'DataCompleteness']],
                on='WardName',
                how='left'
            )
            
            # Create hover text
            hover_text = []
            for _, row in merged_gdf.iterrows():
                if pd.notna(row.get('TPR')):
                    text = f"<b>{row['WardName']}</b><br>"
                    text += f"LGA: {row.get('LGAName', 'Unknown')}<br>"
                    text += f"TPR: {row['TPR']:.1f}%<br>"
                    text += f"Tested: {int(row.get('Tested', 0)):,}<br>"
                    text += f"Positive: {int(row.get('Positive', 0)):,}<br>"
                    text += f"Data Quality: {row.get('DataCompleteness', 0):.0f}%"
                else:
                    text = f"<b>{row['WardName']}</b><br>No TPR data available"
                hover_text.append(text)
            
            # Convert to GeoJSON
            geojson = merged_gdf.__geo_interface__
            
            # Determine color scale and ranges
            tpr_values = merged_gdf['TPR'].dropna()
            if len(tpr_values) > 0:
                min_tpr = tpr_values.min()
                max_tpr = tpr_values.max()
            else:
                min_tpr, max_tpr = 0, 50
            
            # Create figure
            fig = go.Figure()
            
            # Add choropleth layer
            fig.add_trace(go.Choroplethmapbox(
                geojson=geojson,
                locations=merged_gdf.index,
                z=merged_gdf['TPR'],
                colorscale=[
                    [0, '#2ecc71'],      # Green for low TPR
                    [0.2, '#f1c40f'],    # Yellow
                    [0.4, '#e67e22'],    # Orange
                    [0.6, '#e74c3c'],    # Red
                    [1.0, '#9b59b6']     # Purple for very high TPR
                ],
                marker_opacity=0.8,
                marker_line_width=0.5,
                marker_line_color='black',
                hovertemplate='%{hovertext}<extra></extra>',
                hovertext=hover_text,
                colorbar=dict(
                    title=dict(
                        text="TPR (%)",
                        font=dict(size=12)
                    ),
                    tickmode='array',
                    tickvals=[0, 10, 20, 30, 40, 50],
                    ticktext=['0%', '10%', '20%', '30%', '40%', '50%+']
                ),
                zmin=0,
                zmax=50  # Cap at 50% for better visualization
            ))
            
            # Update layout
            center_lat = merged_gdf.geometry.centroid.y.mean()
            center_lon = merged_gdf.geometry.centroid.x.mean()
            
            fig.update_layout(
                title=dict(
                    text=title or f"TPR Distribution - {state_name}",
                    font=dict(size=16, family="Arial, sans-serif"),
                    x=0.5,
                    xanchor='center'
                ),
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=6.5
                ),
                height=700,
                margin=dict(t=50, b=30, l=30, r=30)
            )
            
            # Save the map
            filename = f"tpr_distribution_map_{state_name.lower().replace(' ', '_')}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(
                str(filepath),
                include_plotlyjs=True,
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            logger.info(f"TPR distribution map saved to {filepath}")
            
            # Return web-accessible path
            return f"/serve_viz_file/{self.session_id}/visualizations/{filename}"
            
        except Exception as e:
            logger.error(f"Error creating TPR distribution map: {e}")
            return self._create_error_view(str(e))
    
    def _create_table_view(self, tpr_df: pd.DataFrame, state_name: str, title: str) -> str:
        """Create a table view when map cannot be generated."""
        try:
            # Sort by TPR descending
            tpr_df_sorted = tpr_df.sort_values('TPR', ascending=False)
            
            # Create plotly table
            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=['Ward', 'LGA', 'TPR (%)', 'Tested', 'Positive', 'Data Quality (%)'],
                    fill_color='paleturquoise',
                    align='left'
                ),
                cells=dict(
                    values=[
                        tpr_df_sorted['WardName'],
                        tpr_df_sorted['LGA'],
                        tpr_df_sorted['TPR'].round(1),
                        tpr_df_sorted['Tested'].astype(int),
                        tpr_df_sorted['Positive'].astype(int),
                        tpr_df_sorted['DataCompleteness'].round(0)
                    ],
                    fill_color='lavender',
                    align='left'
                )
            )])
            
            fig.update_layout(
                title=title or f"TPR Results - {state_name}",
                height=600
            )
            
            filename = f"tpr_table_{state_name.lower().replace(' ', '_')}.html"
            filepath = self.output_dir / filename
            
            fig.write_html(str(filepath))
            
            return f"/serve_viz_file/{self.session_id}/visualizations/{filename}"
            
        except Exception as e:
            logger.error(f"Error creating table view: {e}")
            return self._create_error_view(str(e))
    
    def _create_error_view(self, error_msg: str) -> str:
        """Create an error view HTML."""
        html_content = f"""
        <html>
        <head><title>Visualization Error</title></head>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2>Unable to Generate Visualization</h2>
            <p style="color: red;">Error: {error_msg}</p>
            <p>Please try again or contact support if the issue persists.</p>
        </body>
        </html>
        """
        
        filename = "tpr_error.html"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        return f"/serve_viz_file/{self.session_id}/visualizations/{filename}"