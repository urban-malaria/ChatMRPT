"""
Advanced Interactive Mapping Tools for ChatMRPT

These tools provide sophisticated mapping capabilities including multi-layer visualizations,
environmental analysis, settlement patterns, and spatial analytics.
"""

import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import numpy as np
import os
from typing import Dict, Any, Optional, List
from pydantic import Field
from scipy import stats
from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import logging

from .base import BaseTool, ToolExecutionResult, VisualizationTool
from ..data.unified_dataset_builder import load_unified_dataset

logger = logging.getLogger(__name__)


class CreateMultiLayerRiskMap(VisualizationTool):
    """
    Create an interactive multi-layer map comparing composite, PCA, and consensus risk assessments.
    
    Allows users to toggle between different risk methodologies and visualize agreement/disagreement
    patterns across wards with interactive controls and detailed hover information.
    """
    
    comparison_type: str = Field(
        default="all_methods",
        description="Type of comparison: 'all_methods', 'composite_vs_pca', 'consensus_focus', or 'method_agreement'"
    )
    
    show_agreement_overlay: bool = Field(
        default=True,
        description="Whether to show method agreement as overlay patterns"
    )
    
    highlight_disagreements: bool = Field(
        default=False,
        description="Whether to highlight wards where methods disagree significantly"
    )

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute multi-layer risk mapping"""
        try:
            gdf = load_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No unified dataset available")
            
            # Check required columns
            required_cols = ['composite_score', 'pca_score', 'method_agreement', 'consensus_risk_level']
            missing_cols = [col for col in required_cols if col not in gdf.columns]
            if missing_cols:
                return self._create_error_result(f"Missing required columns: {missing_cols}")
            
            if self.comparison_type == "all_methods":
                fig = self._create_all_methods_comparison(gdf)
            elif self.comparison_type == "composite_vs_pca":
                fig = self._create_composite_pca_comparison(gdf)
            elif self.comparison_type == "consensus_focus":
                fig = self._create_consensus_focus_map(gdf)
            else:  # method_agreement
                fig = self._create_method_agreement_map(gdf)
            
            # Save the visualization
            session_folder = f"instance/uploads/{session_id}"
            filename = f"multi_layer_risk_map_{self.comparison_type}.html"
            filepath = os.path.join(session_folder, filename)
            fig.write_html(filepath)
            
            # Generate summary statistics
            stats = self._generate_comparison_stats(gdf)
            
            return self._create_success_result(
                message=f"Multi-layer risk map created ({self.comparison_type})",
                data={
                    "comparison_stats": stats,
                    "total_wards": len(gdf),
                    "agreement_distribution": gdf['method_agreement'].value_counts().to_dict()
                },
                web_path=f"/serve_viz_file/{session_id}/{filename}",
                chart_type="interactive_choropleth_map"
            )
            
        except Exception as e:
            logger.error(f"Error in CreateMultiLayerRiskMap: {e}")
            return self._create_error_result(f"Failed to create multi-layer risk map: {str(e)}")
    
    def _create_all_methods_comparison(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create comprehensive comparison of all risk assessment methods"""
        
        # Create subplots with dropdown menu
        fig = go.Figure()
        
        # Composite Risk Layer
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf['composite_score'],
            colorscale='RdYlBu_r',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Composite Score: %{z:.3f}<br>" +
                         "Composite Rank: %{customdata[1]}<br>" +
                         "Risk Category: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf.get('composite_rank', 'N/A'),
                gdf.get('composite_category', 'N/A')
            )),
            name="Composite Risk",
            visible=True
        ))
        
        # PCA Risk Layer
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf['pca_score'],
            colorscale='Viridis',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "PCA Score: %{z:.3f}<br>" +
                         "PCA Rank: %{customdata[1]}<br>" +
                         "Risk Category: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf.get('pca_rank', 'N/A'),
                gdf.get('pca_category', 'N/A')
            )),
            name="PCA Risk",
            visible=False
        ))
        
        # Method Agreement Layer
        agreement_colors = {'High Agreement': 'green', 'Moderate Agreement': 'orange', 'Low Agreement': 'red'}
        agreement_numeric = gdf['method_agreement'].map({'High Agreement': 3, 'Moderate Agreement': 2, 'Low Agreement': 1})
        
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=agreement_numeric,
            colorscale=[[0, 'red'], [0.5, 'orange'], [1, 'green']],
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Method Agreement: %{customdata[1]}<br>" +
                         "Rank Difference: %{customdata[2]}<br>" +
                         "Consensus Risk: %{customdata[3]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf['method_agreement'],
                gdf.get('rank_difference_abs', 'N/A'),
                gdf.get('consensus_risk_level', 'N/A')
            )),
            name="Method Agreement",
            visible=False
        ))
        
        # Add dropdown menu
        fig.update_layout(
            updatemenus=[{
                'buttons': [
                    {'label': 'Composite Risk', 'method': 'update', 'args': [{'visible': [True, False, False]}]},
                    {'label': 'PCA Risk', 'method': 'update', 'args': [{'visible': [False, True, False]}]},
                    {'label': 'Method Agreement', 'method': 'update', 'args': [{'visible': [False, False, True]}]}
                ],
                'direction': 'down',
                'showactive': True,
                'x': 0.1,
                'y': 1.02
            }]
        )
        
        # Update layout for mapbox
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Multi-Layer Risk Assessment Comparison",
            height=700
        )
        
        return fig
    
    def _create_composite_pca_comparison(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create side-by-side comparison of composite vs PCA"""
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Composite Risk Assessment', 'PCA Risk Assessment'),
            specs=[[{"type": "mapbox"}, {"type": "mapbox"}]]
        )
        
        # Composite map
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf['composite_score'],
            colorscale='RdYlBu_r',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Composite Score: %{z:.3f}<br>" +
                         "Rank: %{customdata[1]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((gdf['WardName'], gdf.get('composite_rank', 'N/A'))),
            name="Composite"
        ), row=1, col=1)
        
        # PCA map
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf['pca_score'],
            colorscale='Viridis',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "PCA Score: %{z:.3f}<br>" +
                         "Rank: %{customdata[1]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((gdf['WardName'], gdf.get('pca_rank', 'N/A'))),
            name="PCA"
        ), row=1, col=2)
        
        # Update layout
        center_lat, center_lon = gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()
        
        fig.update_layout(
            mapbox1=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
            mapbox2=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
            title="Composite vs PCA Risk Assessment Comparison",
            height=600
        )
        
        return fig
    
    def _create_consensus_focus_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create map focused on consensus risk levels"""
        
        # Create consensus risk color mapping
        consensus_colors = {
            'High Risk (Consensus)': '#8B0000',
            'High Risk (Mixed)': '#CD5C5C',
            'Medium Risk (Consensus)': '#FF8C00',
            'Medium Risk (Mixed)': '#FFD700',
            'Low Risk (Consensus)': '#228B22',
            'Low Risk (Mixed)': '#90EE90'
        }
        
        fig = go.Figure()
        
        # Create traces for each consensus level
        for consensus_level in gdf['consensus_risk_level'].unique():
            mask = gdf['consensus_risk_level'] == consensus_level
            subset = gdf[mask]
            
            fig.add_trace(go.Choroplethmapbox(
                geojson=subset.__geo_interface__,
                locations=subset.index,
                z=[1] * len(subset),  # Uniform color per category
                colorscale=[[0, consensus_colors.get(consensus_level, '#808080')], 
                           [1, consensus_colors.get(consensus_level, '#808080')]],
                showscale=False,
                hovertemplate="<b>%{customdata[0]}</b><br>" +
                             "Consensus Risk: " + consensus_level + "<br>" +
                             "Method Agreement: %{customdata[1]}<br>" +
                             "Combined Priority: %{customdata[2]}<br>" +
                             "<extra></extra>",
                customdata=np.column_stack((
                    subset['WardName'],
                    subset['method_agreement'],
                    subset.get('combined_priority', 'N/A')
                )),
                name=consensus_level
            ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Consensus Risk Level Assessment",
            height=700
        )
        
        return fig
    
    def _create_method_agreement_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create map highlighting method agreement patterns"""
        
        fig = go.Figure()
        
        # Method agreement with rank difference overlay
        agreement_numeric = gdf['method_agreement'].map({
            'High Agreement': 3, 
            'Moderate Agreement': 2, 
            'Low Agreement': 1
        })
        
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=agreement_numeric,
            colorscale='RdYlGn',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Method Agreement: %{customdata[1]}<br>" +
                         "Rank Difference: %{customdata[2]}<br>" +
                         "Composite Rank: %{customdata[3]}<br>" +
                         "PCA Rank: %{customdata[4]}<br>" +
                         "Better Method: %{customdata[5]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf['method_agreement'],
                gdf.get('rank_difference_abs', 'N/A'),
                gdf.get('composite_rank', 'N/A'),
                gdf.get('pca_rank', 'N/A'),
                gdf.get('better_ranking_method', 'N/A')
            )),
            name="Agreement Level"
        ))
        
        # Highlight high disagreement areas if requested
        if self.highlight_disagreements:
            high_disagreement = gdf[gdf['method_agreement'] == 'Low Agreement']
            if len(high_disagreement) > 0:
                fig.add_trace(go.Scattermapbox(
                    lat=high_disagreement.geometry.centroid.y,
                    lon=high_disagreement.geometry.centroid.x,
                    mode='markers',
                    marker=dict(size=8, color='red', symbol='x'),
                    text=high_disagreement['WardName'],
                    name="High Disagreement"
                ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Method Agreement Analysis",
            height=700
        )
        
        return fig
    
    def _generate_comparison_stats(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Generate summary statistics for method comparison"""
        
        stats = {}
        
        # Agreement distribution
        agreement_dist = gdf['method_agreement'].value_counts()
        stats['agreement_distribution'] = agreement_dist.to_dict()
        
        # Average rank differences
        if 'rank_difference_abs' in gdf.columns:
            stats['avg_rank_difference'] = gdf['rank_difference_abs'].mean()
            stats['max_rank_difference'] = gdf['rank_difference_abs'].max()
        
        # Consensus risk distribution
        if 'consensus_risk_level' in gdf.columns:
            consensus_dist = gdf['consensus_risk_level'].value_counts()
            stats['consensus_distribution'] = consensus_dist.to_dict()
        
        # Method performance
        if 'better_ranking_method' in gdf.columns:
            method_performance = gdf['better_ranking_method'].value_counts()
            stats['method_performance'] = method_performance.to_dict()
        
        return stats


class CreateEnvironmentalDriverMap(VisualizationTool):
    """
    Create comprehensive environmental driver visualization showing climate, terrain, 
    vegetation, and water factors that influence malaria transmission risk.
    
    Combines multiple environmental variables into thematic maps with correlation analysis.
    """
    
    focus_factor: str = Field(
        default="all_environmental",
        description="Environmental focus: 'all_environmental', 'climate', 'water_terrain', 'vegetation', or 'flood_risk'"
    )
    
    show_correlations: bool = Field(
        default=True,
        description="Whether to include correlation analysis with health outcomes"
    )
    
    create_combined_index: bool = Field(
        default=False,
        description="Whether to create a combined environmental risk index"
    )

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute environmental driver mapping"""
        try:
            gdf = load_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No unified dataset available")
            
            # Define environmental variables by category
            env_vars = self._get_environmental_variables(gdf)
            if not env_vars:
                return self._create_error_result("No environmental variables found in dataset")
            
            if self.focus_factor == "all_environmental":
                fig = self._create_comprehensive_environmental_map(gdf, env_vars)
            elif self.focus_factor == "climate":
                fig = self._create_climate_map(gdf, env_vars)
            elif self.focus_factor == "water_terrain":
                fig = self._create_water_terrain_map(gdf, env_vars)
            elif self.focus_factor == "vegetation":
                fig = self._create_vegetation_map(gdf, env_vars)
            else:  # flood_risk
                fig = self._create_flood_risk_map(gdf, env_vars)
            
            # Create combined environmental index if requested
            environmental_stats = {}
            if self.create_combined_index:
                gdf, environmental_stats = self._create_environmental_index(gdf, env_vars)
            
            # Save visualization
            session_folder = f"instance/uploads/{session_id}"
            filename = f"environmental_driver_map_{self.focus_factor}.html"
            filepath = os.path.join(session_folder, filename)
            fig.write_html(filepath)
            
            # Generate correlation analysis
            correlation_stats = {}
            if self.show_correlations:
                correlation_stats = self._analyze_environmental_correlations(gdf, env_vars)
            
            return self._create_success_result(
                message=f"Environmental driver map created ({self.focus_factor})",
                data={
                    "environmental_variables": env_vars,
                    "correlation_analysis": correlation_stats,
                    "environmental_index_stats": environmental_stats,
                    "total_wards": len(gdf)
                },
                web_path=f"/serve_viz_file/{session_id}/{filename}",
                chart_type="environmental_analysis_map"
            )
            
        except Exception as e:
            logger.error(f"Error in CreateEnvironmentalDriverMap: {e}")
            return self._create_error_result(f"Failed to create environmental driver map: {str(e)}")
    
    def _get_environmental_variables(self, gdf: gpd.GeoDataFrame) -> Dict[str, List[str]]:
        """Identify available environmental variables by category"""
        
        env_vars = {
            'climate': [],
            'water': [],
            'terrain': [],
            'vegetation': [],
            'infrastructure': []
        }
        
        for col in gdf.columns:
            # Only process numeric columns for environmental analysis
            if gdf[col].dtype not in ['float64', 'int64', 'float32', 'int32']:
                continue
                
            col_lower = col.lower()
            if any(x in col_lower for x in ['temp', 'rainfall', 'humidity', 'rh_']):
                env_vars['climate'].append(col)
            elif any(x in col_lower for x in ['water', 'ndwi', 'distance_to_water']):
                env_vars['water'].append(col)
            elif any(x in col_lower for x in ['elevation', 'flood', 'soil', 'wetness']):
                env_vars['terrain'].append(col)
            elif any(x in col_lower for x in ['ndvi', 'evi', 'vegetation']):
                env_vars['vegetation'].append(col)
            elif any(x in col_lower for x in ['urban', 'built', 'housing', 'infrastructure']):
                env_vars['infrastructure'].append(col)
        
        return {k: v for k, v in env_vars.items() if v}  # Remove empty categories
    
    def _create_comprehensive_environmental_map(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> go.Figure:
        """Create comprehensive multi-layer environmental map"""
        
        fig = go.Figure()
        
        # Add layers for each environmental category
        layer_count = 0
        for category, variables in env_vars.items():
            if not variables:
                continue
                
            # Use first variable as representative for the category
            main_var = variables[0]
            
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf[main_var],
                colorscale=self._get_colorscale_for_category(category),
                hovertemplate="<b>%{customdata[0]}</b><br>" +
                             f"{main_var}: %{{z:.3f}}<br>" +
                             self._build_category_hover_text(gdf, variables) +
                             "<extra></extra>",
                customdata=np.column_stack([gdf['WardName']] + [gdf[var] for var in variables[:3]]),
                name=f"{category.title()} ({main_var})",
                visible=True if layer_count == 0 else False
            ))
            layer_count += 1
        
        # Add dropdown menu for layer selection
        buttons = []
        for i, (category, variables) in enumerate(env_vars.items()):
            if variables:
                visibility = [False] * len(env_vars)
                visibility[i] = True
                buttons.append({
                    'label': category.title(),
                    'method': 'update',
                    'args': [{'visible': visibility}]
                })
        
        fig.update_layout(
            updatemenus=[{
                'buttons': buttons,
                'direction': 'down',
                'showactive': True,
                'x': 0.1,
                'y': 1.02
            }],
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Comprehensive Environmental Drivers Analysis",
            height=700
        )
        
        return fig
    
    def _create_climate_map(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> go.Figure:
        """Create focused climate variables map"""
        
        climate_vars = env_vars.get('climate', [])
        if not climate_vars:
            return self._create_no_data_figure("No climate variables found")
        
        fig = make_subplots(
            rows=1, cols=min(3, len(climate_vars)),
            subplot_titles=[var.replace('_', ' ').title() for var in climate_vars[:3]],
            specs=[[{"type": "mapbox"}] * min(3, len(climate_vars))]
        )
        
        for i, var in enumerate(climate_vars[:3]):
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf[var],
                colorscale='RdYlBu_r' if 'temp' in var.lower() else 'Blues',
                hovertemplate=f"<b>%{{customdata[0]}}</b><br>{var}: %{{z:.2f}}<br><extra></extra>",
                customdata=gdf['WardName'],
                name=var
            ), row=1, col=i+1)
        
        # Update layout for multiple mapboxes
        center_lat, center_lon = gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()
        for i in range(min(3, len(climate_vars))):
            fig.update_layout({
                f'mapbox{i+1}': dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=8
                )
            })
        
        fig.update_layout(title="Climate Variables Analysis", height=600)
        return fig
    
    def _create_water_terrain_map(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> go.Figure:
        """Create water and terrain analysis map"""
        
        water_vars = env_vars.get('water', [])
        terrain_vars = env_vars.get('terrain', [])
        all_vars = water_vars + terrain_vars
        
        if not all_vars:
            return self._create_no_data_figure("No water or terrain variables found")
        
        fig = go.Figure()
        
        for i, var in enumerate(all_vars):
            colorscale = 'Blues' if var in water_vars else 'terrain'
            
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf[var],
                colorscale=colorscale,
                hovertemplate=f"<b>%{{customdata[0]}}</b><br>{var}: %{{z:.3f}}<br><extra></extra>",
                customdata=gdf['WardName'],
                name=var.replace('_', ' ').title(),
                visible=True if i == 0 else False
            ))
        
        # Add dropdown for variable selection
        buttons = []
        for i, var in enumerate(all_vars):
            visibility = [False] * len(all_vars)
            visibility[i] = True
            buttons.append({
                'label': var.replace('_', ' ').title(),
                'method': 'update',
                'args': [{'visible': visibility}]
            })
        
        fig.update_layout(
            updatemenus=[{
                'buttons': buttons,
                'direction': 'down',
                'showactive': True,
                'x': 0.1,
                'y': 1.02
            }],
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Water and Terrain Analysis",
            height=700
        )
        
        return fig
    
    def _create_vegetation_map(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> go.Figure:
        """Create vegetation analysis map"""
        
        veg_vars = env_vars.get('vegetation', [])
        if not veg_vars:
            return self._create_no_data_figure("No vegetation variables found")
        
        fig = go.Figure()
        
        # If we have both NDVI and EVI, create comparison
        if len(veg_vars) >= 2:
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=['NDVI Analysis', 'EVI Analysis'],
                specs=[[{"type": "mapbox"}, {"type": "mapbox"}]]
            )
            
            for i, var in enumerate(veg_vars[:2]):
                fig.add_trace(go.Choroplethmapbox(
                    geojson=gdf.__geo_interface__,
                    locations=gdf.index,
                    z=gdf[var],
                    colorscale='Greens',
                    hovertemplate=f"<b>%{{customdata[0]}}</b><br>{var}: %{{z:.0f}}<br><extra></extra>",
                    customdata=gdf['WardName'],
                    name=var
                ), row=1, col=i+1)
            
            center_lat, center_lon = gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()
            fig.update_layout(
                mapbox1=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
                mapbox2=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
                title="Vegetation Index Comparison"
            )
        else:
            # Single vegetation variable
            var = veg_vars[0]
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf[var],
                colorscale='Greens',
                hovertemplate=f"<b>%{{customdata[0]}}</b><br>{var}: %{{z:.0f}}<br><extra></extra>",
                customdata=gdf['WardName'],
                name=var
            ))
            
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(
                    center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                    zoom=8
                ),
                title="Vegetation Analysis"
            )
        
        fig.update_layout(height=600)
        return fig
    
    def _create_flood_risk_map(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> go.Figure:
        """Create focused flood risk analysis"""
        
        # Identify flood-related variables
        flood_vars = []
        for category, variables in env_vars.items():
            for var in variables:
                if any(x in var.lower() for x in ['flood', 'elevation', 'soil_wetness', 'water']):
                    flood_vars.append(var)
        
        if not flood_vars:
            return self._create_no_data_figure("No flood risk variables found")
        
        # Create composite flood risk if we have the right variables
        if any('flood' in var.lower() for var in flood_vars):
            flood_var = next(var for var in flood_vars if 'flood' in var.lower())
            
            fig = go.Figure()
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf[flood_var],
                colorscale='Blues',
                hovertemplate="<b>%{customdata[0]}</b><br>" +
                             f"Flood Risk: %{{z:.3f}}<br>" +
                             self._build_flood_hover_text(gdf, flood_vars) +
                             "<extra></extra>",
                customdata=gdf['WardName'],
                name="Flood Risk"
            ))
            
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(
                    center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                    zoom=8
                ),
                title="Flood Risk Analysis",
                height=700
            )
        
        return fig
    
    def _create_environmental_index(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> tuple:
        """Create combined environmental risk index"""
        
        # Collect all environmental variables
        all_env_vars = []
        for variables in env_vars.values():
            all_env_vars.extend(variables)
        
        # Select numeric environmental variables
        numeric_env_vars = []
        for var in all_env_vars:
            if var in gdf.columns and gdf[var].dtype in ['float64', 'int64']:
                numeric_env_vars.append(var)
        
        if len(numeric_env_vars) < 2:
            return gdf, {"error": "Not enough numeric environmental variables for index creation"}
        
        # Normalize variables and create index
        env_data = gdf[numeric_env_vars].fillna(gdf[numeric_env_vars].median())
        scaler = StandardScaler()
        env_normalized = scaler.fit_transform(env_data)
        
        # Create simple average environmental index
        gdf['environmental_risk_index'] = np.mean(env_normalized, axis=1)
        
        # Statistics
        stats = {
            "variables_used": numeric_env_vars,
            "index_range": [gdf['environmental_risk_index'].min(), gdf['environmental_risk_index'].max()],
            "index_mean": gdf['environmental_risk_index'].mean(),
            "index_std": gdf['environmental_risk_index'].std()
        }
        
        return gdf, stats
    
    def _analyze_environmental_correlations(self, gdf: gpd.GeoDataFrame, env_vars: Dict[str, List[str]]) -> Dict[str, Any]:
        """Analyze correlations between environmental variables and health outcomes"""
        
        # Get health variables
        health_vars = []
        for col in gdf.columns:
            if any(x in col.lower() for x in ['tpr', 'pfpr', 'malaria', 'health']):
                health_vars.append(col)
        
        if not health_vars:
            return {"error": "No health variables found for correlation analysis"}
        
        # Get all environmental variables
        all_env_vars = []
        for variables in env_vars.values():
            all_env_vars.extend(variables)
        
        # Calculate correlations
        correlations = {}
        for health_var in health_vars:
            if health_var in gdf.columns:
                health_corrs = {}
                for env_var in all_env_vars:
                    if env_var in gdf.columns:
                        corr = gdf[health_var].corr(gdf[env_var])
                        if not np.isnan(corr):
                            health_corrs[env_var] = corr
                
                if health_corrs:
                    correlations[health_var] = health_corrs
        
        return correlations
    
    def _get_colorscale_for_category(self, category: str) -> str:
        """Get appropriate colorscale for environmental category"""
        
        colorscales = {
            'climate': 'RdYlBu_r',
            'water': 'Blues',
            'terrain': 'Earth',  # Changed from 'terrain' to valid 'Earth'
            'vegetation': 'Greens',
            'infrastructure': 'Greys'
        }
        return colorscales.get(category, 'Viridis')
    
    def _build_category_hover_text(self, gdf: gpd.GeoDataFrame, variables: List[str]) -> str:
        """Build hover text for environmental category"""
        
        hover_parts = []
        for var in variables[:3]:  # Show up to 3 variables
            if var in gdf.columns:
                hover_parts.append(f"{var}: %{{customdata[{len(hover_parts)+1}]:.3f}}")
        
        return "<br>".join(hover_parts)
    
    def _build_flood_hover_text(self, gdf: gpd.GeoDataFrame, flood_vars: List[str]) -> str:
        """Build hover text for flood risk analysis"""
        
        hover_parts = []
        for var in flood_vars[:3]:
            if var in gdf.columns:
                if 'elevation' in var.lower():
                    hover_parts.append(f"Elevation: {gdf[var].iloc[0]:.0f}m")
                elif 'wetness' in var.lower():
                    hover_parts.append(f"Soil Wetness: {gdf[var].iloc[0]:.3f}")
                elif 'distance' in var.lower():
                    hover_parts.append(f"Water Distance: {gdf[var].iloc[0]:.0f}m")
        
        return "<br>".join(hover_parts)
    
    def _create_no_data_figure(self, message: str) -> go.Figure:
        """Create a placeholder figure when no data is available"""
        
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            title="No Data Available",
            height=400
        )
        return fig