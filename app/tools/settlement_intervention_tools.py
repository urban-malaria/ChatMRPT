"""
Settlement Analysis and Intervention Targeting Tools for ChatMRPT

Advanced tools for settlement pattern analysis and intervention optimization.
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
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import logging

from .base import BaseTool, ToolExecutionResult, VisualizationTool
from ..data.unified_dataset_builder import load_unified_dataset
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)


class CreateSettlementAnalysisMap(VisualizationTool):
    """
    Create comprehensive settlement type analysis with urban-rural gradients,
    housing quality assessment, and building pattern analysis.
    
    Visualizes the 60+ settlement types with clustering and infrastructure metrics.
    """
    
    analysis_focus: str = Field(
        default="settlement_types",
        description="Analysis focus: 'settlement_types', 'urban_gradient', 'housing_quality', or 'building_patterns'"
    )
    
    cluster_similar_settlements: bool = Field(
        default=True,
        description="Whether to cluster similar settlement types for visualization"
    )
    
    show_infrastructure_overlay: bool = Field(
        default=False,
        description="Whether to overlay infrastructure quality metrics"
    )

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute settlement analysis mapping"""
        try:
            gdf = load_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No unified dataset available")
            
            # Check for settlement-related columns
            settlement_cols = self._identify_settlement_columns(gdf)
            if not settlement_cols:
                return self._create_error_result("No settlement analysis columns found")
            
            if self.analysis_focus == "settlement_types":
                fig = self._create_settlement_types_map(gdf, settlement_cols)
            elif self.analysis_focus == "urban_gradient":
                fig = self._create_urban_gradient_map(gdf, settlement_cols)
            elif self.analysis_focus == "housing_quality":
                fig = self._create_housing_quality_map(gdf, settlement_cols)
            else:  # building_patterns
                fig = self._create_building_patterns_map(gdf, settlement_cols)
            
            # Generate settlement statistics
            settlement_stats = self._analyze_settlement_patterns(gdf, settlement_cols)
            
            # Save visualization
            session_folder = f"instance/uploads/{session_id}"
            filename = f"settlement_analysis_map_{self.analysis_focus}.html"
            filepath = os.path.join(session_folder, filename)
            fig.write_html(filepath)
            
            return self._create_success_result(
                message=f"Settlement analysis map created ({self.analysis_focus})",
                data={
                    "settlement_statistics": settlement_stats,
                    "settlement_columns": settlement_cols,
                    "total_wards": len(gdf)
                },
                web_path=f"/serve_viz_file/{session_id}/{filename}",
                chart_type="settlement_analysis_map"
            )
            
        except Exception as e:
            logger.error(f"Error in CreateSettlementAnalysisMap: {e}")
            return self._create_error_result(f"Failed to create settlement analysis map: {str(e)}")
    
    def _identify_settlement_columns(self, gdf: gpd.GeoDataFrame) -> Dict[str, str]:
        """Identify settlement-related columns in the dataset"""
        
        settlement_cols = {}
        
        for col in gdf.columns:
            col_lower = col.lower()
            if 'settlement' in col_lower and 'type' in col_lower:
                settlement_cols['settlement_type'] = col
            elif any(x in col_lower for x in ['urban', 'percentage', 'perce']):
                settlement_cols['urban_percentage'] = col
            elif 'housing' in col_lower and 'quality' in col_lower:
                settlement_cols['housing_quality'] = col
            elif 'building' in col_lower and 'height' in col_lower:
                settlement_cols['building_height'] = col
            elif col_lower in ['urban_x', 'urban_y', 'urban']:
                settlement_cols['urban_classification'] = col
        
        return settlement_cols
    
    def _create_settlement_types_map(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> go.Figure:
        """Create map showing settlement type distribution"""
        
        settlement_type_col = settlement_cols.get('settlement_type')
        if not settlement_type_col:
            return self._create_no_data_figure("No settlement type data available")
        
        # Get unique settlement types
        settlement_types = gdf[settlement_type_col].unique()
        
        if self.cluster_similar_settlements and len(settlement_types) > 20:
            # Cluster similar settlement types
            clustered_settlements = self._cluster_settlement_types(gdf, settlement_type_col)
            gdf['settlement_cluster'] = clustered_settlements
            
            fig = go.Figure()
            
            # Create traces for each cluster
            for cluster in gdf['settlement_cluster'].unique():
                if pd.isna(cluster):
                    continue
                    
                mask = gdf['settlement_cluster'] == cluster
                subset = gdf[mask]
                
                fig.add_trace(go.Choroplethmapbox(
                    geojson=subset.__geo_interface__,
                    locations=subset.index,
                    z=[cluster] * len(subset),
                    colorscale='Viridis',  # Changed from 'Set3' to valid 'Viridis'
                    hovertemplate="<b>%{customdata[0]}</b><br>" +
                                 "Settlement Type: %{customdata[1]}<br>" +
                                 f"Cluster: {cluster}<br>" +
                                 "<extra></extra>",
                    customdata=np.column_stack((subset['WardName'], subset[settlement_type_col])),
                    name=f"Cluster {cluster}",
                    showscale=False
                ))
            
            title = "Settlement Types (Clustered)"
        else:
            # Show individual settlement types
            fig = go.Figure()
            
            # Use color mapping for settlement types
            settlement_type_mapping = {stype: i for i, stype in enumerate(settlement_types)}
            gdf['settlement_type_numeric'] = gdf[settlement_type_col].map(settlement_type_mapping)
            
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=gdf['settlement_type_numeric'],
                colorscale='Viridis',  # Changed from 'Set3' to valid 'Viridis'
                hovertemplate="<b>%{customdata[0]}</b><br>" +
                             "Settlement Type: %{customdata[1]}<br>" +
                             self._build_settlement_hover(gdf, settlement_cols) +
                             "<extra></extra>",
                customdata=np.column_stack((gdf['WardName'], gdf[settlement_type_col])),
                name="Settlement Types"
            ))
            
            title = "Settlement Type Distribution"
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=title,
            height=700
        )
        
        return fig
    
    def _create_urban_gradient_map(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> go.Figure:
        """Create urban-rural gradient visualization"""
        
        urban_pct_col = settlement_cols.get('urban_percentage')
        if not urban_pct_col:
            return self._create_no_data_figure("No urban percentage data available")
        
        fig = go.Figure()
        
        # Urban gradient choropleth
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf[urban_pct_col],
            colorscale='RdYlGn_r',  # Red for high urban, green for rural
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Urban Percentage: %{z:.1f}%<br>" +
                         self._build_urban_hover(gdf, settlement_cols) +
                         "<extra></extra>",
            customdata=gdf['WardName'],
            name="Urban Gradient"
        ))
        
        # Add urban classification overlay if available
        urban_class_col = settlement_cols.get('urban_classification')
        if urban_class_col and self.show_infrastructure_overlay:
            urban_wards = gdf[gdf[urban_class_col].isin(['Yes', 'Urban', 1])]
            if len(urban_wards) > 0:
                fig.add_trace(go.Scattermapbox(
                    lat=urban_wards.geometry.centroid.y,
                    lon=urban_wards.geometry.centroid.x,
                    mode='markers',
                    marker=dict(size=6, color='red', symbol='circle'),
                    text=urban_wards['WardName'],
                    name="Classified Urban"
                ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Urban-Rural Gradient Analysis",
            height=700
        )
        
        return fig
    
    def _create_housing_quality_map(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> go.Figure:
        """Create housing quality assessment map"""
        
        housing_col = settlement_cols.get('housing_quality')
        if not housing_col:
            return self._create_no_data_figure("No housing quality data available")
        
        fig = go.Figure()
        
        # Housing quality choropleth
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf[housing_col],
            colorscale='RdYlGn',  # Red for poor quality, green for good quality
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Housing Quality: %{z:.3f}<br>" +
                         self._build_infrastructure_hover(gdf, settlement_cols) +
                         "<extra></extra>",
            customdata=gdf['WardName'],
            name="Housing Quality"
        ))
        
        # Add quality categories if we can determine them
        exists, resolved_col = variable_resolver.check_column_exists(housing_col, list(gdf.columns))
        if exists:
            # Create quality categories based on quartiles
            quality_quartiles = gdf[housing_col].quantile([0.25, 0.5, 0.75])
            
            def categorize_quality(value):
                if pd.isna(value):
                    return "Unknown"
                elif value <= quality_quartiles[0.25]:
                    return "Poor Quality"
                elif value <= quality_quartiles[0.5]:
                    return "Fair Quality"
                elif value <= quality_quartiles[0.75]:
                    return "Good Quality"
                else:
                    return "Excellent Quality"
            
            gdf['housing_quality_category'] = gdf[housing_col].apply(categorize_quality)
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Housing Quality Assessment",
            height=700
        )
        
        return fig
    
    def _create_building_patterns_map(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> go.Figure:
        """Create building patterns and height analysis"""
        
        building_height_col = settlement_cols.get('building_height')
        if not building_height_col:
            return self._create_no_data_figure("No building height data available")
        
        fig = go.Figure()
        
        # Building height visualization
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=gdf[building_height_col],
            colorscale='Plasma',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Avg Building Height: %{z:.2f}m<br>" +
                         self._build_building_hover(gdf, settlement_cols) +
                         "<extra></extra>",
            customdata=gdf['WardName'],
            name="Building Height"
        ))
        
        # Add building density patterns if available
        if 'area_km2' in gdf.columns and building_height_col in gdf.columns:
            # Calculate building density proxy
            gdf['building_density_proxy'] = gdf[building_height_col] / gdf['area_km2']
            
            # Identify high-density areas
            high_density_threshold = gdf['building_density_proxy'].quantile(0.8)
            high_density_wards = gdf[gdf['building_density_proxy'] >= high_density_threshold]
            
            if len(high_density_wards) > 0:
                fig.add_trace(go.Scattermapbox(
                    lat=high_density_wards.geometry.centroid.y,
                    lon=high_density_wards.geometry.centroid.x,
                    mode='markers',
                    marker=dict(size=8, color='orange', symbol='square'),
                    text=high_density_wards['WardName'],
                    name="High Building Density"
                ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Building Patterns and Height Analysis",
            height=700
        )
        
        return fig
    
    def _cluster_settlement_types(self, gdf: gpd.GeoDataFrame, settlement_type_col: str) -> List:
        """Cluster similar settlement types based on patterns"""
        
        try:
            # Simple clustering based on settlement type names
            settlement_types = gdf[settlement_type_col].unique()
            
            # Create clusters based on common keywords
            clusters = []
            cluster_mapping = {}
            current_cluster = 0
            
            keywords = [
                ['residential', 'housing', 'compound'],
                ['commercial', 'market', 'trade'],
                ['industrial', 'factory', 'manufacturing'],
                ['mixed', 'development'],
                ['informal', 'slum', 'squatter'],
                ['rural', 'village', 'traditional']
            ]
            
            for settlement_type in settlement_types:
                if pd.isna(settlement_type):
                    cluster_mapping[settlement_type] = -1
                    continue
                    
                settlement_lower = str(settlement_type).lower()
                assigned = False
                
                for i, keyword_group in enumerate(keywords):
                    if any(keyword in settlement_lower for keyword in keyword_group):
                        cluster_mapping[settlement_type] = i
                        assigned = True
                        break
                
                if not assigned:
                    cluster_mapping[settlement_type] = len(keywords)  # "Other" cluster
            
            return gdf[settlement_type_col].map(cluster_mapping).tolist()
            
        except Exception:
            # Fallback: no clustering
            return [0] * len(gdf)
    
    def _analyze_settlement_patterns(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> Dict[str, Any]:
        """Analyze settlement patterns and generate statistics"""
        
        stats = {}
        
        # Settlement type distribution
        if 'settlement_type' in settlement_cols:
            settlement_type_col = settlement_cols['settlement_type']
            type_distribution = gdf[settlement_type_col].value_counts().head(10).to_dict()
            stats['top_settlement_types'] = type_distribution
            stats['total_settlement_types'] = gdf[settlement_type_col].nunique()
        
        # Urban-rural distribution
        if 'urban_percentage' in settlement_cols:
            urban_pct_col = settlement_cols['urban_percentage']
            stats['urban_stats'] = {
                'mean_urban_percentage': gdf[urban_pct_col].mean(),
                'median_urban_percentage': gdf[urban_pct_col].median(),
                'highly_urban_wards': (gdf[urban_pct_col] >= 70).sum(),
                'rural_wards': (gdf[urban_pct_col] <= 30).sum()
            }
        
        # Housing quality distribution
        if 'housing_quality' in settlement_cols:
            housing_col = settlement_cols['housing_quality']
            stats['housing_stats'] = {
                'mean_quality': gdf[housing_col].mean(),
                'quality_range': [gdf[housing_col].min(), gdf[housing_col].max()],
                'poor_quality_wards': (gdf[housing_col] <= gdf[housing_col].quantile(0.25)).sum()
            }
        
        # Building height patterns
        if 'building_height' in settlement_cols:
            height_col = settlement_cols['building_height']
            stats['building_stats'] = {
                'mean_height': gdf[height_col].mean(),
                'height_range': [gdf[height_col].min(), gdf[height_col].max()],
                'high_rise_wards': (gdf[height_col] >= gdf[height_col].quantile(0.9)).sum()
            }
        
        return stats
    
    def _build_settlement_hover(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> str:
        """Build hover text for settlement information"""
        
        hover_parts = []
        
        if 'urban_percentage' in settlement_cols:
            hover_parts.append(f"Urban %: {{customdata[2]:.1f}}")
        if 'housing_quality' in settlement_cols:
            hover_parts.append(f"Housing Quality: {{customdata[3]:.3f}}")
        
        return "<br>".join(hover_parts)
    
    def _build_urban_hover(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> str:
        """Build hover text for urban analysis"""
        
        hover_parts = []
        
        if 'settlement_type' in settlement_cols:
            hover_parts.append("Settlement Type: {customdata[1]}")
        if 'housing_quality' in settlement_cols:
            hover_parts.append("Housing Quality: {customdata[2]:.3f}")
        
        return "<br>".join(hover_parts)
    
    def _build_infrastructure_hover(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> str:
        """Build hover text for infrastructure analysis"""
        
        hover_parts = []
        
        if 'urban_percentage' in settlement_cols:
            hover_parts.append("Urban %: {customdata[1]:.1f}")
        if 'building_height' in settlement_cols:
            hover_parts.append("Avg Building Height: {customdata[2]:.2f}m")
        
        return "<br>".join(hover_parts)
    
    def _build_building_hover(self, gdf: gpd.GeoDataFrame, settlement_cols: Dict[str, str]) -> str:
        """Build hover text for building analysis"""
        
        hover_parts = []
        
        if 'settlement_type' in settlement_cols:
            hover_parts.append("Settlement Type: {customdata[1]}")
        if 'housing_quality' in settlement_cols:
            hover_parts.append("Housing Quality: {customdata[2]:.3f}")
        if 'area_km2' in gdf.columns:
            hover_parts.append("Ward Area: {customdata[3]:.2f} km²")
        
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


class CreateInterventionTargetingMap(VisualizationTool):
    """
    Create intervention targeting optimization maps showing priority wards,
    resource allocation scenarios, and coverage gap analysis for malaria interventions.
    
    Supports ITN distribution, IRS targeting, and other intervention strategies.
    """
    
    intervention_type: str = Field(
        default="itn_targeting",
        description="Intervention type: 'itn_targeting', 'irs_targeting', 'coverage_gaps', or 'resource_optimization'"
    )
    
    priority_method: str = Field(
        default="composite",
        description="Priority ranking method: 'composite', 'pca', 'consensus', or 'combined'"
    )
    
    resource_constraint: Optional[int] = Field(
        default=None,
        description="Resource constraint (e.g., number of ITNs available)"
    )
    
    top_n_wards: int = Field(
        default=50,
        description="Number of top priority wards to highlight",
        ge=1,
        le=500
    )

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute intervention targeting mapping"""
        try:
            gdf = load_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No unified dataset available")
            
            # Check for required columns based on priority method
            required_cols = self._get_required_columns()
            missing_cols = [col for col in required_cols if col not in gdf.columns]
            if missing_cols:
                return self._create_error_result(f"Missing required columns for {self.priority_method}: {missing_cols}")
            
            if self.intervention_type == "itn_targeting":
                fig = self._create_itn_targeting_map(gdf)
            elif self.intervention_type == "irs_targeting":
                fig = self._create_irs_targeting_map(gdf)
            elif self.intervention_type == "coverage_gaps":
                fig = self._create_coverage_gaps_map(gdf)
            else:  # resource_optimization
                fig = self._create_resource_optimization_map(gdf)
            
            # Generate targeting statistics
            targeting_stats = self._analyze_targeting_effectiveness(gdf)
            
            # Save visualization
            session_folder = f"instance/uploads/{session_id}"
            filename = f"intervention_targeting_{self.intervention_type}_{self.priority_method}.html"
            filepath = os.path.join(session_folder, filename)
            fig.write_html(filepath)
            
            return self._create_success_result(
                message=f"Intervention targeting map created ({self.intervention_type})",
                data={
                    "targeting_statistics": targeting_stats,
                    "priority_method": self.priority_method,
                    "top_wards_count": self.top_n_wards,
                    "resource_constraint": self.resource_constraint
                },
                web_path=f"/serve_viz_file/{session_id}/{filename}",
                chart_type="intervention_targeting_map"
            )
            
        except Exception as e:
            logger.error(f"Error in CreateInterventionTargetingMap: {e}")
            return self._create_error_result(f"Failed to create intervention targeting map: {str(e)}")
    
    def _get_required_columns(self) -> List[str]:
        """Get required columns based on priority method"""
        
        base_cols = ['WardName']
        
        if self.priority_method == "composite":
            return base_cols + ['composite_score', 'composite_rank']
        elif self.priority_method == "pca":
            return base_cols + ['pca_score', 'pca_rank']
        elif self.priority_method == "consensus":
            return base_cols + ['consensus_risk_level', 'combined_priority']
        else:  # combined
            return base_cols + ['composite_score', 'pca_score', 'composite_rank', 'pca_rank']
    
    def _create_itn_targeting_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create ITN distribution targeting map"""
        
        # Get priority scores based on method
        priority_data = self._get_priority_data(gdf)
        
        # Select top priority wards (get the corresponding GeoDataFrame rows)
        top_priority_indices = priority_data.nsmallest(self.top_n_wards, 'priority_rank').index
        top_wards = gdf.loc[top_priority_indices]
        
        fig = go.Figure()
        
        # Base risk layer
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=priority_data['priority_score'],
            colorscale='YlOrRd',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Priority Score: %{z:.3f}<br>" +
                         "Priority Rank: %{customdata[1]}<br>" +
                         "Risk Level: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                priority_data['priority_rank'],
                priority_data.get('risk_category', 'Unknown')
            )),
            name="Risk Level"
            # Removed opacity parameter as it's not supported for Choroplethmapbox
        ))
        
        # Highlight top priority wards
        fig.add_trace(go.Choroplethmapbox(
            geojson=top_wards.__geo_interface__,
            locations=top_wards.index,
            z=[1] * len(top_wards),
            colorscale=[[0, 'red'], [1, 'red']],
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "TOP PRIORITY for ITNs<br>" +
                         "Priority Rank: %{customdata[1]}<br>" +
                         "Expected ITNs Needed: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                top_wards['WardName'],
                priority_data.loc[top_priority_indices, 'priority_rank'],
                self._estimate_itn_needs(top_wards)
            )),
            name=f"Top {self.top_n_wards} Priority Wards",
            showscale=False
            # Removed opacity parameter as it's not supported for Choroplethmapbox
        ))
        
        # Add resource constraint information if provided
        if self.resource_constraint:
            resource_allocation = self._calculate_resource_allocation(top_wards, self.resource_constraint)
            
            # Add text annotations for resource allocation
            fig.add_annotation(
                text=f"ITNs Available: {self.resource_constraint:,}<br>" +
                     f"Wards Covered: {resource_allocation['wards_covered']}<br>" +
                     f"Population Covered: {resource_allocation['population_covered']:,}",
                xref="paper", yref="paper",
                x=0.02, y=0.98,
                showarrow=False,
                bgcolor="rgba(255,255,255,0.8)",
                font=dict(size=12)
            )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=f"ITN Distribution Targeting ({self.priority_method.title()} Method)",
            height=700
        )
        
        return fig
    
    def _create_irs_targeting_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create IRS targeting map focusing on suitable areas"""
        
        # Get priority data
        priority_data = self._get_priority_data(gdf)
        
        # IRS is more suitable for certain settlement types
        irs_suitable = self._identify_irs_suitable_wards(gdf)
        
        fig = go.Figure()
        
        # Base suitability layer
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=irs_suitable['suitability_score'],
            colorscale='Blues',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "IRS Suitability: %{z:.3f}<br>" +
                         "Settlement Type: %{customdata[1]}<br>" +
                         "Housing Quality: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                gdf.get('settlement_type', 'Unknown'),
                gdf.get('housing_quality', 'Unknown')
            )),
            name="IRS Suitability"
            # Removed opacity parameter
        ))
        
        # Highlight high-priority + high-suitability wards
        combined_priority = priority_data['priority_rank'] * (1 / (irs_suitable['suitability_score'] + 0.1))
        top_irs_wards = gdf.loc[combined_priority.nsmallest(self.top_n_wards).index]
        
        fig.add_trace(go.Scattermapbox(
            lat=top_irs_wards.geometry.centroid.y,
            lon=top_irs_wards.geometry.centroid.x,
            mode='markers',
            marker=dict(size=10, color='darkblue', symbol='circle'),
            text=top_irs_wards['WardName'],
            hovertemplate="<b>%{text}</b><br>" +
                         "PRIORITY for IRS<br>" +
                         "Combined Score: %{customdata[0]:.3f}<br>" +
                         "<extra></extra>",
            customdata=combined_priority.loc[top_irs_wards.index],
            name=f"Top {self.top_n_wards} IRS Targets"
        ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title=f"IRS Targeting Analysis ({self.priority_method.title()} Method)",
            height=700
        )
        
        return fig
    
    def _create_coverage_gaps_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create coverage gaps analysis map"""
        
        # Get priority data
        priority_data = self._get_priority_data(gdf)
        
        # Identify coverage gaps (high risk + low intervention coverage)
        coverage_gaps = self._identify_coverage_gaps(gdf, priority_data)
        
        fig = go.Figure()
        
        # Base coverage layer
        if 'current_coverage' in coverage_gaps.columns:
            fig.add_trace(go.Choroplethmapbox(
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                z=coverage_gaps['current_coverage'],
                colorscale='RdYlGn',
                hovertemplate="<b>%{customdata[0]}</b><br>" +
                             "Current Coverage: %{z:.1f}%<br>" +
                             "Risk Level: %{customdata[1]}<br>" +
                             "Gap Score: %{customdata[2]:.3f}<br>" +
                             "<extra></extra>",
                customdata=np.column_stack((
                    gdf['WardName'],
                    priority_data.get('risk_category', 'Unknown'),
                    coverage_gaps['gap_score']
                )),
                name="Current Coverage"
                # Removed opacity parameter as it's not supported for Choroplethmapbox
            ))
        
        # Highlight significant coverage gaps
        high_gap_wards = coverage_gaps[coverage_gaps['gap_score'] >= coverage_gaps['gap_score'].quantile(0.8)]
        
        if len(high_gap_wards) > 0:
            fig.add_trace(go.Scattermapbox(
                lat=high_gap_wards.geometry.centroid.y,
                lon=high_gap_wards.geometry.centroid.x,
                mode='markers',
                marker=dict(size=12, color='red', symbol='x'),
                text=high_gap_wards['WardName'],
                hovertemplate="<b>%{text}</b><br>" +
                             "CRITICAL COVERAGE GAP<br>" +
                             "Gap Score: %{customdata[0]:.3f}<br>" +
                             "<extra></extra>",
                customdata=high_gap_wards['gap_score'],
                name="Critical Coverage Gaps"
            ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Intervention Coverage Gaps Analysis",
            height=700
        )
        
        return fig
    
    def _create_resource_optimization_map(self, gdf: gpd.GeoDataFrame) -> go.Figure:
        """Create resource optimization scenario map"""
        
        # Get priority data
        priority_data = self._get_priority_data(gdf)
        
        # Create optimization scenarios
        scenarios = self._create_optimization_scenarios(gdf, priority_data)
        
        fig = go.Figure()
        
        # Base priority layer
        fig.add_trace(go.Choroplethmapbox(
            geojson=gdf.__geo_interface__,
            locations=gdf.index,
            z=priority_data['priority_score'],
            colorscale='YlOrRd',
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                         "Priority Score: %{z:.3f}<br>" +
                         "Optimal Scenario: %{customdata[1]}<br>" +
                         "Resource Efficiency: %{customdata[2]}<br>" +
                         "<extra></extra>",
            customdata=np.column_stack((
                gdf['WardName'],
                scenarios['optimal_scenario'],
                scenarios['efficiency_score']
            )),
            name="Priority Level"
            # Removed opacity parameter as it's not supported for Choroplethmapbox
        ))
        
        # Add scenario-specific overlays
        for scenario_name, scenario_wards in scenarios['scenario_wards'].items():
            if len(scenario_wards) > 0:
                colors = {'High Impact': 'green', 'Medium Impact': 'orange', 'Low Impact': 'yellow'}
                color = colors.get(scenario_name, 'blue')
                
                fig.add_trace(go.Scattermapbox(
                    lat=scenario_wards.geometry.centroid.y,
                    lon=scenario_wards.geometry.centroid.x,
                    mode='markers',
                    marker=dict(size=8, color=color, symbol='circle'),
                    text=scenario_wards['WardName'],
                    name=scenario_name
                ))
        
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(lat=gdf.geometry.centroid.y.mean(), lon=gdf.geometry.centroid.x.mean()),
                zoom=8
            ),
            title="Resource Optimization Scenarios",
            height=700
        )
        
        return fig
    
    def _get_priority_data(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """Get priority data based on selected method"""
        
        priority_data = pd.DataFrame(index=gdf.index)
        priority_data['WardName'] = gdf['WardName']
        
        if self.priority_method == "composite":
            priority_data['priority_score'] = gdf['composite_score']
            priority_data['priority_rank'] = gdf['composite_rank']
            priority_data['risk_category'] = gdf.get('composite_category', 'Unknown')
        elif self.priority_method == "pca":
            priority_data['priority_score'] = gdf['pca_score']
            priority_data['priority_rank'] = gdf['pca_rank']
            priority_data['risk_category'] = gdf.get('pca_category', 'Unknown')
        elif self.priority_method == "consensus":
            priority_data['priority_score'] = (gdf['composite_score'] + gdf['pca_score']) / 2
            priority_data['priority_rank'] = (gdf['composite_rank'] + gdf['pca_rank']) / 2
            priority_data['risk_category'] = gdf.get('consensus_risk_level', 'Unknown')
        else:  # combined
            priority_data['priority_score'] = (gdf['composite_score'] + gdf['pca_score']) / 2
            priority_data['priority_rank'] = (gdf['composite_rank'] + gdf['pca_rank']) / 2
            priority_data['risk_category'] = gdf.get('combined_priority', 'Unknown')
        
        return priority_data
    
    def _identify_irs_suitable_wards(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """Identify wards suitable for IRS based on settlement characteristics"""
        
        suitability = pd.DataFrame(index=gdf.index)
        suitability['suitability_score'] = 0.5  # Default moderate suitability
        
        # Housing quality factor (better quality = more suitable for IRS)
        if 'housing_quality' in gdf.columns:
            housing_normalized = (gdf['housing_quality'] - gdf['housing_quality'].min()) / \
                               (gdf['housing_quality'].max() - gdf['housing_quality'].min())
            suitability['suitability_score'] += housing_normalized * 0.3
        
        # Settlement type factor
        if 'settlement_type' in gdf.columns:
            # Higher suitability for formal settlements
            formal_keywords = ['residential', 'formal', 'planned', 'estate']
            is_formal = gdf['settlement_type'].str.lower().str.contains('|'.join(formal_keywords), na=False)
            suitability.loc[is_formal, 'suitability_score'] += 0.2
        
        # Urban percentage factor (moderate urban areas are ideal)
        if 'urbanPercentage' in gdf.columns:
            urban_pct = gdf['urbanPercentage']
            # Optimal range: 40-70% urban
            optimal_mask = (urban_pct >= 40) & (urban_pct <= 70)
            suitability.loc[optimal_mask, 'suitability_score'] += 0.2
        
        # Ensure scores are between 0 and 1
        suitability['suitability_score'] = suitability['suitability_score'].clip(0, 1)
        
        return suitability
    
    def _identify_coverage_gaps(self, gdf: gpd.GeoDataFrame, priority_data: pd.DataFrame) -> pd.DataFrame:
        """Identify intervention coverage gaps"""
        
        coverage_gaps = pd.DataFrame(index=gdf.index)
        
        # Simulate current coverage (in real implementation, this would come from data)
        np.random.seed(42)  # For reproducible results
        coverage_gaps['current_coverage'] = np.random.uniform(20, 80, len(gdf))
        
        # Calculate gap score (high risk + low coverage = high gap)
        risk_normalized = (priority_data['priority_score'] - priority_data['priority_score'].min()) / \
                         (priority_data['priority_score'].max() - priority_data['priority_score'].min())
        coverage_normalized = coverage_gaps['current_coverage'] / 100
        
        # Gap score: high risk and low coverage = high gap
        coverage_gaps['gap_score'] = risk_normalized * (1 - coverage_normalized)
        
        return coverage_gaps
    
    def _create_optimization_scenarios(self, gdf: gpd.GeoDataFrame, priority_data: pd.DataFrame) -> Dict[str, Any]:
        """Create resource optimization scenarios"""
        
        scenarios = {
            'scenario_wards': {},
            'optimal_scenario': [],
            'efficiency_score': []
        }
        
        # Calculate efficiency score (impact per unit cost)
        # Higher priority + lower cost = higher efficiency
        if 'area_km2' in gdf.columns:
            # Use area as proxy for intervention cost
            cost_proxy = gdf['area_km2']
            efficiency = priority_data['priority_score'] / (cost_proxy + 0.1)  # Avoid division by zero
        else:
            efficiency = priority_data['priority_score']
        
        scenarios['efficiency_score'] = efficiency.tolist()
        
        # Create scenarios based on efficiency quartiles
        efficiency_quartiles = efficiency.quantile([0.25, 0.5, 0.75])
        
        scenarios['scenario_wards']['High Impact'] = gdf[efficiency >= efficiency_quartiles[0.75]]
        scenarios['scenario_wards']['Medium Impact'] = gdf[(efficiency >= efficiency_quartiles[0.25]) & 
                                                          (efficiency < efficiency_quartiles[0.75])]
        scenarios['scenario_wards']['Low Impact'] = gdf[efficiency < efficiency_quartiles[0.25]]
        
        # Assign optimal scenario for each ward
        optimal_scenario = []
        for eff in efficiency:
            if eff >= efficiency_quartiles[0.75]:
                optimal_scenario.append('High Impact')
            elif eff >= efficiency_quartiles[0.25]:
                optimal_scenario.append('Medium Impact')
            else:
                optimal_scenario.append('Low Impact')
        
        scenarios['optimal_scenario'] = optimal_scenario
        
        return scenarios
    
    def _estimate_itn_needs(self, wards: gpd.GeoDataFrame) -> List[int]:
        """Estimate ITN needs for wards"""
        
        # Simple estimation: assume 2 people per ITN and population density
        if 'area_km2' in wards.columns:
            # Estimate population based on area and average density
            estimated_population = wards['area_km2'] * 1000  # Rough estimate
            itn_needs = (estimated_population / 2).astype(int)
            return itn_needs.tolist()
        else:
            # Default estimate
            return [500] * len(wards)
    
    def _calculate_resource_allocation(self, wards: gpd.GeoDataFrame, total_resources: int) -> Dict[str, Any]:
        """Calculate resource allocation based on priority"""
        
        itn_needs = self._estimate_itn_needs(wards)
        cumulative_needs = np.cumsum(itn_needs)
        
        # Find how many wards can be fully covered
        wards_covered = np.sum(cumulative_needs <= total_resources)
        
        # Estimate population covered
        if 'area_km2' in wards.columns:
            covered_population = wards.iloc[:wards_covered]['area_km2'].sum() * 1000
        else:
            covered_population = wards_covered * 1000  # Rough estimate
        
        return {
            'wards_covered': wards_covered,
            'population_covered': int(covered_population),
            'resource_utilization': min(100, (sum(itn_needs[:wards_covered]) / total_resources) * 100)
        }
    
    def _analyze_targeting_effectiveness(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Analyze targeting effectiveness metrics"""
        
        priority_data = self._get_priority_data(gdf)
        
        stats = {
            'total_wards': len(gdf),
            'high_priority_wards': len(priority_data[priority_data['priority_rank'] <= self.top_n_wards]),
            'priority_method_used': self.priority_method,
            'coverage_statistics': {}
        }
        
        # Risk distribution in top priority wards
        top_priority_indices = priority_data.nsmallest(self.top_n_wards, 'priority_rank').index
        top_wards_priority_data = priority_data.loc[top_priority_indices]
        if 'risk_category' in priority_data.columns:
            risk_dist = top_wards_priority_data['risk_category'].value_counts().to_dict()
            stats['top_wards_risk_distribution'] = risk_dist
        
        # Efficiency metrics
        if self.resource_constraint:
            allocation = self._calculate_resource_allocation(
                gdf.loc[top_priority_indices], 
                self.resource_constraint
            )
            stats['resource_allocation'] = allocation
        
        return stats