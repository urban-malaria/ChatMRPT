"""
Spatial Autocorrelation Tools for ChatMRPT

Advanced spatial analysis tools for detecting clustering patterns, hotspots, 
and spatial dependencies in malaria risk factors.
"""

import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os
from typing import Dict, Any, Optional, List
from pydantic import Field
from scipy.spatial.distance import pdist, squareform
from sklearn.linear_model import LinearRegression
import logging

from .base import BaseTool, ToolExecutionResult, VisualizationTool
from ..data.unified_dataset_builder import load_unified_dataset

logger = logging.getLogger(__name__)


class CreateSpatialAutocorrelationMap(VisualizationTool):
    """
    Create spatial autocorrelation analysis maps showing clustering patterns,
    hotspots, and spatial dependencies in risk factors and health outcomes.
    
    Uses Moran's I and LISA statistics to identify spatial patterns.
    """
    
    analysis_variable: str = Field(
        default="composite_score",
        description="Variable to analyze: 'composite_score', 'pca_score', 'tpr', or custom variable name"
    )
    
    analysis_type: str = Field(
        default="morans_i",
        description="Analysis type: 'morans_i', 'lisa_clusters', 'hotspot_analysis', or 'spatial_trends'"
    )
    
    neighborhood_type: str = Field(
        default="queen",
        description="Neighborhood definition: 'queen', 'rook', or 'k_nearest'"
    )
    
    k_neighbors: int = Field(
        default=8,
        description="Number of neighbors for k_nearest neighborhood",
        ge=1,
        le=20
    )

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute spatial autocorrelation analysis"""
        try:
            gdf = load_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No unified dataset available")
            
            # Check if analysis variable exists
            if self.analysis_variable not in gdf.columns:
                available_vars = [col for col in gdf.columns if gdf[col].dtype in ['float64', 'int64']]
                return self._create_error_result(
                    f"Variable '{self.analysis_variable}' not found. Available numeric variables: {available_vars[:10]}"
                )
            
            # Check for geometry
            if 'geometry' not in gdf.columns or gdf.geometry.isna().all():
                return self._create_error_result("No spatial geometry available for autocorrelation analysis")
            
            if self.analysis_type == "morans_i":
                fig = self._create_morans_i_map(gdf)
            elif self.analysis_type == "lisa_clusters":
                fig = self._create_lisa_clusters_map(gdf)
            elif self.analysis_type == "hotspot_analysis":
                fig = self._create_hotspot_analysis_map(gdf)
            else:  # spatial_trends
                fig = self._create_spatial_trends_map(gdf)
            
            # Generate spatial statistics
            spatial_stats = self._calculate_spatial_statistics(gdf)
            
            # Save visualization
            session_folder = f"instance/uploads/{session_id}"
            filename = f"spatial_autocorrelation_{self.analysis_type}_{self.analysis_variable}.html"
            filepath = os.path.join(session_folder, filename)
            fig.write_html(filepath)
            
            return self._create_success_result(
                message=f"Spatial autocorrelation analysis created ({self.analysis_type})",
                data={
                    "spatial_statistics": spatial_stats,
                    "analysis_variable": self.analysis_variable,
                    "neighborhood_type": self.neighborhood_type,
                    "total_wards": len(gdf)
                },
                web_path=f"/serve_viz_file/{session_id}/{filename}",
                chart_type="spatial_autocorrelation_map"
            )
            
        except Exception as e:
            logger.error(f"Error in CreateSpatialAutocorrelationMap: {e}")
            return self._create_error_result(f"Failed to create spatial autocorrelation map: {str(e)}")
    
    def _create_morans_i_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create Moran's I global autocorrelation visualization"""
        
        # Calculate spatial weights and Moran's I
        spatial_analysis = self._calculate_morans_i(gdf)
        
        fig = go.Figure()
        
        # Base variable choropleth
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf[self.analysis_variable],
            colorscale='RdYlBu_r',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         f"{self.analysis_variable}: %{{z:.3f}}<br>" +
                         "Local Moran's I: %{customdata[1]:.3f}<br>" +
                         "Spatial Lag: %{customdata[2]:.3f}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                spatial_analysis['local_morans_i'],
                spatial_analysis['spatial_lag']
            )),
            name=f"{self.analysis_variable}"
        ))
        
        # Add annotation with global Moran's I result
        fig.add_annotation(
            text=f"Global Moran's I: {spatial_analysis['global_morans_i']:.4f}<br>" +
                 f"p-value: {spatial_analysis['p_value']:.4f}<br>" +
                 f"Interpretation: {spatial_analysis['interpretation']}",
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            showarrow=False,
            bgcolor="rgba(255,255,255,0.9)",
            font=dict(size=12)
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=f"Moran's I Spatial Autocorrelation: {self.analysis_variable}",
            height=700
        )
        
        return fig
    
    def _create_lisa_clusters_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create LISA (Local Indicators of Spatial Association) clusters map"""
        
        # Calculate LISA clusters
        lisa_analysis = self._calculate_lisa_clusters(gdf)
        
        fig = go.Figure()
        
        # LISA cluster types with specific colors
        cluster_colors = {
            'High-High': '#d7191c',  # Red
            'Low-Low': '#2c7bb6',    # Blue
            'High-Low': '#fdae61',   # Orange
            'Low-High': '#abd9e9',   # Light blue
            'Not Significant': '#ffffbf'  # Light yellow
        }
        
        # Create traces for each cluster type
        for cluster_type in lisa_analysis['cluster_types'].unique():
            mask = lisa_analysis['cluster_types'] == cluster_type
            subset = gdf[mask]
            
            if len(subset) > 0:
                fig.add_trace(go.Choroplethmapbox(
                    geojson=subset.__geo_interface__,
                    locations=subset.index,
                    z=[1] * len(subset),  # Uniform color per cluster
                    colorscale=[[0, cluster_colors.get(cluster_type, '#808080')], 
                               [1, cluster_colors.get(cluster_type, '#808080')]],
                    showscale=False,
                    hovertemplate="<b>%{customdata[0]}</b><br>" +
                                 f"LISA Cluster: {cluster_type}<br>" +
                                 f"{self.analysis_variable}: %{{customdata[1]:.3f}}<br>" +
                                 "Local Moran's I: %{customdata[2]:.3f}<br>" +
                                 "<extra></extra>",
                    customdata=np.column_stack((
                        subset['WardName'],
                        subset[self.analysis_variable],
                        lisa_analysis['local_morans_i'][mask]
                    )),
                    name=cluster_type
                ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=f"LISA Cluster Analysis: {self.analysis_variable}",
            height=700
        )
        
        return fig
    
    def _create_hotspot_analysis_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create hotspot analysis using Getis-Ord Gi* statistic"""
        
        # Calculate Getis-Ord Gi* statistic
        hotspot_analysis = self._calculate_getis_ord(gdf)
        
        fig = go.Figure()
        
        # Hotspot choropleth
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=hotspot_analysis['gi_star'],
            colorscale='RdBu_r',  # Red for hot spots, blue for cold spots
            zmid=0,
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         f"{self.analysis_variable}: %{{customdata[1]:.3f}}<br>" +
                         "Gi* Statistic: %{z:.3f}<br>" +
                         "Hotspot Type: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf[self.analysis_variable],
                hotspot_analysis['hotspot_type']
            )),
            name="Gi* Statistic"
        ))
        
        # Highlight significant hotspots and coldspots
        significant_hot = gdf[hotspot_analysis['gi_star'] > 1.96]  # 95% confidence
        significant_cold = gdf[hotspot_analysis['gi_star'] < -1.96]
        
        if len(significant_hot) > 0:
            fig.add_trace(go.Scattermapbox(
                lat=significant_hot.geometry.centroid.y,
                lon=significant_hot.geometry.centroid.x,
                mode='markers',
                marker=dict(size=8, color='red', symbol='star'),
                text=significant_hot['WardName'],
                name="Significant Hotspots"
            ))
        
        if len(significant_cold) > 0:
            fig.add_trace(go.Scattermapbox(
                lat=significant_cold.geometry.centroid.y,
                lon=significant_cold.geometry.centroid.x,
                mode='markers',
                marker=dict(size=8, color='blue', symbol='star'),
                text=significant_cold['WardName'],
                name="Significant Coldspots"
            ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=f"Hotspot Analysis (Getis-Ord Gi*): {self.analysis_variable}",
            height=700
        )
        
        return fig
    
    def _create_spatial_trends_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create spatial trends analysis"""
        
        # Calculate spatial trends
        trend_analysis = self._calculate_spatial_trends(gdf)
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=['North-South Trend', 'East-West Trend'],
            specs=[[{"type": "mapbox"}, {"type": "mapbox"}]]
        )
        
        # North-South trend
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=trend_analysis['ns_residuals'],
            colorscale='RdBu_r',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "N-S Residual: %{z:.3f}<br>" +
                         f"{self.analysis_variable}: %{{customdata[1]:.3f}}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((gdf['WardName'], gdf[self.analysis_variable])),
            name="N-S Trend"
        ), row=1, col=1)
        
        # East-West trend
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=trend_analysis['ew_residuals'],
            colorscale='RdBu_r',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "E-W Residual: %{z:.3f}<br>" +
                         f"{self.analysis_variable}: %{{customdata[1]:.3f}}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((gdf['WardName'], gdf[self.analysis_variable])),
            name="E-W Trend"
        ), row=1, col=2)
        
        # Update layout
        center_lat, center_lon = gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()
        
        fig.update_layout(
            mapbox1=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
            mapbox2=dict(style="open-street-map", center=dict(lat=center_lat, lon=center_lon), zoom=8),
            title=f"Spatial Trends Analysis: {self.analysis_variable}",
            height=600
        )
        
        return fig
    
    def _calculate_morans_i(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Calculate Moran's I spatial autocorrelation"""
        
        try:
            from libpysal.weights import Queen, Rook, KNN
            from esda import Moran, Moran_Local
            
            # Create spatial weights
            if self.neighborhood_type == "queen":
                w = Queen.from_dataframe(gdf)
            elif self.neighborhood_type == "rook":
                w = Rook.from_dataframe(gdf)
            else:  # k_nearest
                w = KNN.from_dataframe(gdf, k=self.k_neighbors)
            
            # Calculate global Moran's I
            y = gdf[self.analysis_variable].values
            moran_global = Moran(y, w)
            
            # Calculate local Moran's I
            moran_local = Moran_Local(y, w)
            
            # Interpret result
            if moran_global.p_norm < 0.05:
                if moran_global.I > 0:
                    interpretation = "Significant positive spatial autocorrelation (clustering)"
                else:
                    interpretation = "Significant negative spatial autocorrelation (dispersion)"
            else:
                interpretation = "No significant spatial autocorrelation (random)"
            
            return {
                'global_morans_i': moran_global.I,
                'p_value': moran_global.p_norm,
                'interpretation': interpretation,
                'local_morans_i': moran_local.Is,
                'spatial_lag': moran_local.y_lag,
                'weights_type': self.neighborhood_type
            }
            
        except ImportError:
            # Fallback calculation without libpysal
            return self._simple_spatial_autocorrelation(gdf)
    
    def _calculate_lisa_clusters(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Calculate LISA cluster classification"""
        
        try:
            from libpysal.weights import Queen, Rook, KNN
            from esda import Moran_Local
            
            # Create spatial weights
            if self.neighborhood_type == "queen":
                w = Queen.from_dataframe(gdf)
            elif self.neighborhood_type == "rook":
                w = Rook.from_dataframe(gdf)
            else:  # k_nearest
                w = KNN.from_dataframe(gdf, k=self.k_neighbors)
            
            # Calculate local Moran's I
            y = gdf[self.analysis_variable].values
            moran_local = Moran_Local(y, w)
            
            # Classify clusters
            cluster_types = []
            for i in range(len(gdf)):
                if moran_local.p_sim[i] < 0.05:  # Significant
                    if moran_local.q[i] == 1:
                        cluster_types.append('High-High')
                    elif moran_local.q[i] == 2:
                        cluster_types.append('Low-High')
                    elif moran_local.q[i] == 3:
                        cluster_types.append('Low-Low')
                    else:  # q == 4
                        cluster_types.append('High-Low')
                else:
                    cluster_types.append('Not Significant')
            
            return {
                'cluster_types': pd.Series(cluster_types),
                'local_morans_i': moran_local.Is,
                'p_values': moran_local.p_sim,
                'quadrants': moran_local.q
            }
            
        except ImportError:
            # Fallback: simple clustering
            return self._simple_lisa_clustering(gdf)
    
    def _calculate_getis_ord(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Calculate Getis-Ord Gi* hotspot analysis"""
        
        try:
            from libpysal.weights import Queen, Rook, KNN
            from esda import G_Local
            
            # Create spatial weights
            if self.neighborhood_type == "queen":
                w = Queen.from_dataframe(gdf)
            elif self.neighborhood_type == "rook":
                w = Rook.from_dataframe(gdf)
            else:  # k_nearest
                w = KNN.from_dataframe(gdf, k=self.k_neighbors)
            
            # Calculate Getis-Ord Gi*
            y = gdf[self.analysis_variable].values
            getis_ord = G_Local(y, w, star=True)
            
            # Classify hotspot types
            hotspot_types = []
            for gi_star in getis_ord.Zs:
                if gi_star > 2.58:
                    hotspot_types.append('Very Hot Spot (99%)')
                elif gi_star > 1.96:
                    hotspot_types.append('Hot Spot (95%)')
                elif gi_star > 1.65:
                    hotspot_types.append('Hot Spot (90%)')
                elif gi_star < -2.58:
                    hotspot_types.append('Very Cold Spot (99%)')
                elif gi_star < -1.96:
                    hotspot_types.append('Cold Spot (95%)')
                elif gi_star < -1.65:
                    hotspot_types.append('Cold Spot (90%)')
                else:
                    hotspot_types.append('Not Significant')
            
            return {
                'gi_star': getis_ord.Zs,
                'hotspot_type': hotspot_types,
                'p_values': getis_ord.p_sim
            }
            
        except ImportError:
            # Fallback calculation
            return self._simple_hotspot_analysis(gdf)
    
    def _calculate_spatial_trends(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Calculate spatial trends using coordinate regression"""
        
        # Get centroids
        centroids = gdf.geometry.centroid
        x_coords = centroids.x.values
        y_coords = centroids.y.values
        values = gdf[self.analysis_variable].values
        
        # North-South trend (latitude)
        ns_reg = LinearRegression().fit(y_coords.reshape(-1, 1), values)
        ns_predicted = ns_reg.predict(y_coords.reshape(-1, 1))
        ns_residuals = values - ns_predicted
        
        # East-West regression
        ew_reg = LinearRegression().fit(x_coords.reshape(-1, 1), values)
        ew_predicted = ew_reg.predict(x_coords.reshape(-1, 1))
        ew_residuals = values - ew_predicted
        
        return {
            'ns_residuals': ns_residuals,
            'ew_residuals': ew_residuals,
            'ns_trend_strength': ns_reg.score(y_coords.reshape(-1, 1), values),
            'ew_trend_strength': ew_reg.score(x_coords.reshape(-1, 1), values)
        }
    
    def _simple_spatial_autocorrelation(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Simple spatial autocorrelation calculation without libpysal"""
        
        # Calculate distance-based weights (simplified)
        centroids = gdf.geometry.centroid
        coords = np.column_stack([centroids.x, centroids.y])
        distances = pdist(coords)
        dist_matrix = squareform(distances)
        
        # Create inverse distance weights
        w_matrix = 1 / (dist_matrix + 1e-10)  # Avoid division by zero
        np.fill_diagonal(w_matrix, 0)  # No self-weights
        
        # Row-standardize weights
        row_sums = w_matrix.sum(axis=1)
        w_matrix = w_matrix / row_sums[:, np.newaxis]
        
        # Calculate simple Moran's I approximation
        y = gdf[self.analysis_variable].values
        y_mean = y.mean()
        y_centered = y - y_mean
        
        # Global Moran's I
        numerator = np.sum(w_matrix * np.outer(y_centered, y_centered))
        denominator = np.sum(y_centered**2)
        morans_i = numerator / denominator if denominator > 0 else 0
        
        # Local indicators
        local_morans = np.diag(np.outer(y_centered, y_centered) @ w_matrix) / (y_centered**2 + 1e-10)
        spatial_lag = w_matrix @ y
        
        return {
            'global_morans_i': morans_i,
            'p_value': 0.05,  # Placeholder
            'interpretation': "Simplified calculation (install libpysal for full analysis)",
            'local_morans_i': local_morans,
            'spatial_lag': spatial_lag,
            'weights_type': 'distance_based_simplified'
        }
    
    def _simple_lisa_clustering(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Simple LISA clustering without libpysal"""
        
        # Use simplified spatial autocorrelation
        spatial_result = self._simple_spatial_autocorrelation(gdf)
        
        y = gdf[self.analysis_variable].values
        y_mean = y.mean()
        spatial_lag = spatial_result['spatial_lag']
        lag_mean = spatial_lag.mean()
        
        # Simple quadrant classification
        cluster_types = []
        for i in range(len(gdf)):
            if y[i] > y_mean and spatial_lag[i] > lag_mean:
                cluster_types.append('High-High')
            elif y[i] < y_mean and spatial_lag[i] < lag_mean:
                cluster_types.append('Low-Low')
            elif y[i] > y_mean and spatial_lag[i] < lag_mean:
                cluster_types.append('High-Low')
            else:
                cluster_types.append('Low-High')
        
        return {
            'cluster_types': pd.Series(cluster_types),
            'local_morans_i': spatial_result['local_morans_i'],
            'p_values': [0.05] * len(gdf),  # Placeholder
            'quadrants': [1] * len(gdf)  # Placeholder
        }
    
    def _simple_hotspot_analysis(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Simple hotspot analysis without libpysal"""
        
        # Use z-scores as proxy for Gi* statistic
        y = gdf[self.analysis_variable].values
        z_scores = (y - y.mean()) / y.std()
        
        # Classify based on z-scores
        hotspot_types = []
        for z in z_scores:
            if z > 2.58:
                hotspot_types.append('Very Hot Spot (99%)')
            elif z > 1.96:
                hotspot_types.append('Hot Spot (95%)')
            elif z > 1.65:
                hotspot_types.append('Hot Spot (90%)')
            elif z < -2.58:
                hotspot_types.append('Very Cold Spot (99%)')
            elif z < -1.96:
                hotspot_types.append('Cold Spot (95%)')
            elif z < -1.65:
                hotspot_types.append('Cold Spot (90%)')
            else:
                hotspot_types.append('Not Significant')
        
        return {
            'gi_star': z_scores,
            'hotspot_type': hotspot_types,
            'p_values': [0.05] * len(gdf)  # Placeholder
        }
    
    def _calculate_spatial_statistics(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Calculate comprehensive spatial statistics"""
        
        stats = {
            'analysis_variable': self.analysis_variable,
            'neighborhood_type': self.neighborhood_type,
            'total_wards': len(gdf)
        }
        
        # Basic variable statistics
        var_data = gdf[self.analysis_variable]
        stats['variable_stats'] = {
            'mean': var_data.mean(),
            'std': var_data.std(),
            'min': var_data.min(),
            'max': var_data.max(),
            'range': var_data.max() - var_data.min()
        }
        
        # Spatial extent
        bounds = gdf.bounds
        stats['spatial_extent'] = {
            'min_lat': bounds.miny.min(),
            'max_lat': bounds.maxy.max(),
            'min_lon': bounds.minx.min(),
            'max_lon': bounds.maxx.max()
        }
        
        try:
            # Try to calculate proper spatial statistics
            moran_result = self._calculate_morans_i(gdf)
            stats['morans_i'] = {
                'global_i': moran_result['global_morans_i'],
                'p_value': moran_result['p_value'],
                'interpretation': moran_result['interpretation']
            }
        except Exception:
            stats['morans_i'] = {'error': 'Could not calculate (install libpysal for full analysis)'}
        
        return stats