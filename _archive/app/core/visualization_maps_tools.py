"""
Visualization Maps Tools for ChatMRPT - Phase 2 Implementation

This module provides the CORE visualization tools that ChatMRPT users actually need:

1. CreateVulnerabilityMap - Risk classification maps (composite & PCA methods)
2. CreatePCAMap - PCA-specific vulnerability visualization 
3. CreateUrbanExtentMap - Urban vs rural risk patterns (with user-specifiable thresholds)
4. CreateInterventionMap - Coverage gaps visualization
5. CreateDecisionTree - Risk factor decision logic
6. CreateCompositeScoreMaps - Individual model breakdowns (paginated)
7. CreateBoxPlot - Statistical distribution analysis (vulnerability plot)

All tools leverage the EXISTING agent-based visualization functions from:
app/services/agents/visualizations/composite_visualizations.py
app/services/agents/visualizations/pca_visualizations.py
"""

import logging
import os
from typing import Dict, Any, Optional, List
from pydantic import Field, validator
import pandas as pd
import numpy as np
from flask import current_app

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)


class CreateVulnerabilityMap(BaseTool):
    """
    Create vulnerability/risk classification map using existing agent functions.
    
    Uses create_agent_vulnerability_map() for composite method visualization.
    Creates choropleth maps showing wards colored by their risk categories.
    """
    
    classification_method: str = Field(
        "quantile",
        description="Classification method: 'quantile', 'natural_breaks', 'equal_interval'",
        pattern="^(quantile|natural_breaks|equal_interval)$"
    )
    
    risk_categories: int = Field(
        3,
        description="Number of risk categories (3-5)",
        ge=3,
        le=5
    )
    
    highlight_high_risk: bool = Field(
        True,
        description="Highlight high-risk areas with special styling"
    )
    
    include_statistics: bool = Field(
        True,
        description="Include statistics panel on the map"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create a vulnerability map showing risk levels",
            "Show me a map of high risk areas",
            "Generate risk classification map",
            "Create map showing ward risk categories"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create vulnerability classification map using existing agent function"""
        try:
            # 🔍 DEBUG: Vulnerability Map Creation
            logger.info("=" * 60)
            logger.info("🔍 DEBUG VULNERABILITY MAP: Starting")
            logger.info(f"🔍 Session ID: {session_id}")
            logger.info(f"🔍 Risk categories: {self.risk_categories}")
            logger.info(f"🔍 Classification method: {self.classification_method}")
            
            # Check for unified dataset
            unified_path = f'instance/uploads/{session_id}/unified_dataset.csv'
            raw_path = f'instance/uploads/{session_id}/raw_data.csv'
            
            logger.info(f"🔍 Checking for unified dataset at: {unified_path}")
            if os.path.exists(unified_path):
                logger.info(f"🔍 ✅ Unified dataset EXISTS")
            else:
                logger.error(f"🔍 ❌ Unified dataset NOT FOUND")
                if os.path.exists(raw_path):
                    logger.info(f"🔍 💡 raw_data.csv EXISTS - could use as fallback")
            logger.info("=" * 60)
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for composite scores or vulnerability classifications
            exists, resolved_col = variable_resolver.check_column_exists('composite_score', list(gdf.columns))
            if not exists:
                error_msg = variable_resolver.create_variable_error_message(
                    'composite_score', list(gdf.columns), 
                    context="for vulnerability mapping. Please run composite analysis first"
                )
                return self._create_error_result(error_msg)
            
            # Check for geometry
            if not hasattr(gdf, 'geometry') or gdf.geometry.isnull().all():
                return self._create_error_result("No geographic boundaries found. Please upload shapefile data.")
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_vulnerability_map
            
            # Create vulnerability map using existing agent function
            map_result = create_agent_vulnerability_map(
                unified_dataset=gdf,
                session_id=session_id,
                method='composite'
            )
            
            if map_result.get('status') == 'error':
                return self._create_error_result(
                    f"Vulnerability map creation failed: {map_result.get('message', 'Unknown error')}"
                )
            
            # Get category counts if available
            category_counts = {}
            if 'vulnerability_category' in gdf.columns:
                category_counts = gdf['vulnerability_category'].value_counts().to_dict()
            elif 'composite_category' in gdf.columns:
                category_counts = gdf['composite_category'].value_counts().to_dict()
            
            
            result_data = {
                'classification_method': self.classification_method,
                'risk_categories': self.risk_categories,
                'total_wards': len(gdf),
                'category_counts': category_counts,
                'web_path': map_result.get('web_path'),
                'file_path': map_result.get('file_path'),
                'map_type': 'vulnerability_map'
            }
            
            message = f"Created vulnerability classification map with {self.risk_categories} risk categories for {len(gdf)} wards"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating vulnerability map: {e}")
            return self._create_error_result(f"Vulnerability map creation failed: {str(e)}")
    


class CreatePCAMap(BaseTool):
    """
    Create PCA-specific vulnerability map using existing agent functions.
    
    Uses create_agent_pca_vulnerability_map() for PCA method visualization.
    """
    
    color_scheme: str = Field(
        "viridis",
        description="Color scheme: 'Reds', 'YlOrRd', 'plasma', 'viridis', 'Blues', 'Greens'",
        pattern="^(Reds|YlOrRd|plasma|viridis|Blues|Greens)$"
    )
    
    include_labels: bool = Field(
        True,
        description="Include ward labels on the map"
    )
    
    show_legend: bool = Field(
        True,
        description="Show color scale legend"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create PCA score map",
            "Show me principal component analysis visualization",
            "Generate map using PCA methodology",
            "PCA analysis map"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create PCA score map using existing agent function"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for PCA scores
            exists, resolved_col = variable_resolver.check_column_exists('pca_score', list(gdf.columns))
            if not exists:
                # Try alternative PCA score column names
                alt_names = ['pc1_risk_score', 'pca_risk_score', 'pc1_score']
                found_alt = False
                for alt_name in alt_names:
                    alt_exists, alt_resolved = variable_resolver.check_column_exists(alt_name, list(gdf.columns))
                    if alt_exists:
                        resolved_col = alt_resolved
                        found_alt = True
                        break
                
                if not found_alt:
                    error_msg = variable_resolver.create_variable_error_message(
                        'pca_score', list(gdf.columns),
                        context="for PCA mapping. Please run PCA analysis first"
                    )
                    return self._create_error_result(error_msg)
            
            # Check for geometry
            if not hasattr(gdf, 'geometry') or gdf.geometry.isnull().all():
                return self._create_error_result("No geographic boundaries found. Please upload shapefile data.")
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_pca_vulnerability_map
            
            # Create PCA map using existing agent function
            map_result = create_agent_pca_vulnerability_map(
                unified_dataset=gdf,
                session_id=session_id
            )
            
            if map_result.get('status') == 'error':
                return self._create_error_result(
                    f"PCA map creation failed: {map_result.get('message', 'Unknown error')}"
                )
            
            # Calculate PCA statistics
            pca_stats = {
                'min': float(gdf['pca_score'].min()),
                'max': float(gdf['pca_score'].max()),
                'mean': float(gdf['pca_score'].mean()),
                'std': float(gdf['pca_score'].std()),
                'median': float(gdf['pca_score'].median())
            }
            
            
            result_data = {
                'color_scheme': self.color_scheme,
                'total_wards': len(gdf),
                'pca_statistics': pca_stats,
                'score_range': f"{pca_stats['min']:.3f} to {pca_stats['max']:.3f}",
                'web_path': map_result.get('web_path'),
                'file_path': map_result.get('file_path'),
                'map_type': 'pca_map'
            }
            
            message = f"Created PCA score map for {len(gdf)} wards. Score range: {pca_stats['min']:.3f} to {pca_stats['max']:.3f}"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating PCA map: {e}")
            return self._create_error_result(f"PCA map creation failed: {str(e)}")


class CreateVulnerabilityMapComparison(BaseTool):
    """
    Create side-by-side comparison of vulnerability maps using both composite and PCA methods.
    
    Shows both methodologies in a single view for easy comparison of risk assessments.
    This is the default view when users ask for vulnerability maps without specifying a method.
    """
    
    include_statistics: bool = Field(
        True,
        description="Include statistics panel on each map"
    )
    
    sync_zoom: bool = Field(
        True,
        description="Synchronize zoom and pan between the two maps"
    )
    
    @classmethod
    def get_tool_name(cls) -> str:
        return "create_vulnerability_map_comparison"
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_description(cls) -> str:
        return "Create side-by-side vulnerability map comparison showing both composite and PCA methods"
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me the vulnerability maps",
            "Create vulnerability map comparison",
            "Compare composite and PCA vulnerability maps",
            "Show both vulnerability analysis methods"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create side-by-side vulnerability map comparison"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for both composite and PCA scores
            composite_exists, composite_col = variable_resolver.check_column_exists('composite_score', list(gdf.columns))
            if not composite_exists:
                return self._create_error_result(
                    "Composite scores not found. Please run composite analysis first."
                )
            
            pca_exists, pca_col = variable_resolver.check_column_exists('pca_score', list(gdf.columns))
            if not pca_exists:
                # Try alternative PCA column names
                alt_names = ['pc1_risk_score', 'pca_risk_score', 'pc1_score']
                found_alt = False
                for alt_name in alt_names:
                    alt_exists, alt_resolved = variable_resolver.check_column_exists(alt_name, list(gdf.columns))
                    if alt_exists:
                        pca_col = alt_resolved
                        found_alt = True
                        break
                
                if not found_alt:
                    return self._create_error_result(
                        "PCA scores not found. Please run PCA analysis first."
                    )
            
            # Check for geometry
            if not hasattr(gdf, 'geometry') or gdf.geometry.isnull().all():
                return self._create_error_result("No geographic boundaries found. Please upload shapefile data.")
            
            # Create side-by-side comparison using plotly subplots
            from plotly.subplots import make_subplots
            import plotly.graph_objects as go
            
            # Create subplot figure with 1 row and 2 columns of map type
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Composite Score Method', 'PCA Method'),
                specs=[[{'type': 'mapbox'}, {'type': 'mapbox'}]],
                horizontal_spacing=0.05
            )
            
            # Get map bounds
            bounds = gdf.total_bounds
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2
            
            # Add composite score map (left)
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=gdf.geometry.__geo_interface__,
                    locations=gdf.index,
                    z=gdf[composite_col],
                    colorscale='Reds',
                    reversescale=True,
                    text=gdf['WardName'] if 'WardName' in gdf.columns else gdf.index,
                    hovertemplate='<b>%{text}</b><br>' +
                                  'Composite Score: %{z:.3f}<br>' +
                                  'Rank: %{customdata[0]}<br>' +
                                  'Category: %{customdata[1]}<br>' +
                                  '<extra></extra>',
                    customdata=np.column_stack((
                        gdf['composite_rank'] if 'composite_rank' in gdf.columns else np.arange(len(gdf)),
                        gdf['composite_category'] if 'composite_category' in gdf.columns else [''] * len(gdf)
                    )),
                    marker_opacity=0.8,
                    marker_line_width=1,
                    marker_line_color='white',
                    showscale=True,
                    colorbar=dict(
                        title="Composite<br>Score",
                        x=0.45,
                        len=0.8,
                        thickness=15
                    ),
                    name='Composite'
                ),
                row=1, col=1
            )
            
            # Add PCA score map (right)
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=gdf.geometry.__geo_interface__,
                    locations=gdf.index,
                    z=gdf[pca_col],
                    colorscale='Viridis',
                    reversescale=False,
                    text=gdf['WardName'] if 'WardName' in gdf.columns else gdf.index,
                    hovertemplate='<b>%{text}</b><br>' +
                                  'PCA Score: %{z:.3f}<br>' +
                                  'Rank: %{customdata[0]}<br>' +
                                  'Category: %{customdata[1]}<br>' +
                                  '<extra></extra>',
                    customdata=np.column_stack((
                        gdf['pca_rank'] if 'pca_rank' in gdf.columns else np.arange(len(gdf)),
                        gdf['pca_category'] if 'pca_category' in gdf.columns else [''] * len(gdf)
                    )),
                    marker_opacity=0.8,
                    marker_line_width=1,
                    marker_line_color='white',
                    showscale=True,
                    colorbar=dict(
                        title="PCA<br>Score",
                        x=1.0,
                        len=0.8,
                        thickness=15
                    ),
                    name='PCA'
                ),
                row=1, col=2
            )
            
            # Update layout
            fig.update_layout(
                title=dict(
                    text="Vulnerability Assessment Comparison: Composite vs PCA Methods",
                    x=0.5,
                    xanchor='center',
                    font=dict(size=20)
                ),
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=8
                ),
                mapbox2=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=8
                ),
                height=700,
                margin={"r": 0, "t": 60, "l": 0, "b": 0},
                showlegend=False
            )
            
            # Skip statistics annotations to avoid categorical data errors
            # Per user request, removing high/low risk statistics that cause categorical errors
            
            # Save the comparison map
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            
            # Save in session folder
            viz_dir = f'instance/uploads/{session_id}/visualizations'
            import os
            os.makedirs(viz_dir, exist_ok=True)
            
            filename = f'vulnerability_comparison_{timestamp}.html'
            file_path = os.path.join(viz_dir, filename)
            
            # Save the figure
            fig.write_html(file_path)
            logger.info(f"Saved vulnerability comparison map to {file_path}")
            
            # Create web path - include visualizations subdirectory
            web_path = f"/serve_viz_file/{session_id}/visualizations/{filename}"
            
            result_data = {
                'total_wards': len(gdf),
                'composite_score_range': f"{gdf[composite_col].min():.3f} to {gdf[composite_col].max():.3f}",
                'pca_score_range': f"{gdf[pca_col].min():.3f} to {gdf[pca_col].max():.3f}",
                'web_path': web_path,
                'file_path': file_path,
                'map_type': 'vulnerability_comparison'
            }
            
            message = f"Created side-by-side vulnerability map comparison for {len(gdf)} wards showing both composite and PCA methods"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating vulnerability map comparison: {e}")
            return self._create_error_result(f"Vulnerability map comparison failed: {str(e)}")


class CreateUrbanExtentMap(BaseTool):
    """
    Create urban extent map using existing agent functions.
    
    Uses create_agent_urban_extent_map() with user-specifiable thresholds.
    Shows vulnerability patterns overlaid with urban/rural distinctions.
    """
    
    threshold: float = Field(
        50.0,
        description="Urban percentage threshold (0-100). Areas below threshold are greyed out.",
        ge=0.0,
        le=100.0
    )
    
    show_vulnerability: bool = Field(
        True,
        description="Show underlying vulnerability patterns"
    )
    
    analysis_method: str = Field(
        "percentage",
        description="Analysis method: 'percentage' or 'categorical'",
        pattern="^(percentage|categorical)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create urban extent map",
            "Show urbanization levels across wards",
            "Map urban vs rural areas with 60% threshold",
            "Visualize settlement patterns with 30% threshold"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create urban extent map using existing agent function"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for urban indicators
            urban_cols = [col for col in gdf.columns if any(term in col.lower() 
                         for term in ['urban', 'percentage', 'settlement'])]
            
            if not urban_cols:
                return self._create_error_result("No urban indicators found in data")
            
            # Check for geometry
            if not hasattr(gdf, 'geometry') or gdf.geometry.isnull().all():
                return self._create_error_result("No geographic boundaries found. Please upload shapefile data.")
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_urban_extent_map
            
            # Create urban extent map using existing agent function
            map_result = create_agent_urban_extent_map(
                unified_dataset=gdf,
                session_id=session_id,
                threshold=self.threshold
            )
            
            if map_result.get('status') == 'error':
                return self._create_error_result(
                    f"Urban extent map creation failed: {map_result.get('message', 'Unknown error')}"
                )
            
            # Calculate urban statistics
            urban_stats = {}
            for col in urban_cols:
                if 'percent' in col.lower():
                    urban_stats = {
                        'mean_urban_percentage': float(gdf[col].mean()),
                        'median_urban_percentage': float(gdf[col].median()),
                        'max_urban_percentage': float(gdf[col].max()),
                        'urban_variable': col,
                        'wards_above_threshold': len(gdf[gdf[col] >= self.threshold])
                    }
                    break
            
            result_data = {
                'threshold': self.threshold,
                'analysis_method': self.analysis_method,
                'total_wards': len(gdf),
                'urban_statistics': urban_stats,
                'web_path': map_result.get('web_path'),
                'file_path': map_result.get('file_path'),
                'map_type': 'urban_extent_map'
            }
            
            message = f"Created urban extent map with {self.threshold}% threshold"
            if urban_stats:
                message += f". {urban_stats['wards_above_threshold']} wards above threshold"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating urban extent map: {e}")
            return self._create_error_result(f"Urban extent map creation failed: {str(e)}")


class CreateDecisionTree(BaseTool):
    """
    Create decision tree visualization using existing agent functions.
    
    Uses create_agent_decision_tree() to show risk factor decision logic.
    """
    
    target_variable: str = Field(
        "auto",
        description="Target variable for classification (auto-detect if 'auto')"
    )
    
    include_workflow: bool = Field(
        True,
        description="Include analysis workflow visualization"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create decision tree",
            "Show decision tree for vulnerability",
            "Generate classification tree",
            "Show analysis workflow"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create decision tree visualization using existing agent function"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_decision_tree
            
            # Create decision tree using existing agent function
            tree_result = create_agent_decision_tree(
                unified_dataset=gdf,
                session_id=session_id
            )
            
            if tree_result.get('status') == 'error':
                return self._create_error_result(
                    f"Decision tree creation failed: {tree_result.get('message', 'Unknown error')}"
                )
            
            result_data = {
                'target_variable': self.target_variable,
                'include_workflow': self.include_workflow,
                'web_path': tree_result.get('web_path'),
                'file_path': tree_result.get('file_path'),
                'map_type': 'decision_tree'
            }
            
            message = f"Created decision tree visualization for {self.target_variable}"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating decision tree: {e}")
            return self._create_error_result(f"Decision tree creation failed: {str(e)}")


class CreateCompositeScoreMaps(BaseTool):
    """
    Create composite score maps showing individual model breakdowns.
    
    Uses create_agent_composite_score_maps() for paginated model visualization.
    """
    
    models_per_page: int = Field(
        4,
        description="Number of model maps per page",
        ge=1,
        le=8
    )
    
    page: int = Field(
        1,
        description="Page number to display",
        ge=1
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create composite score maps",
            "Show individual model breakdowns",
            "Generate model comparison maps",
            "Show page 2 of composite maps"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create composite score maps using existing agent function"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for composite scores
            exists, resolved_col = variable_resolver.check_column_exists('composite_score', list(gdf.columns))
            if not exists:
                error_msg = variable_resolver.create_variable_error_message(
                    'composite_score', list(gdf.columns),
                    context="for analysis. Please run composite analysis first"
                )
                return self._create_error_result(error_msg)
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_composite_score_maps
            
            # Create composite maps using existing agent function
            maps_result = create_agent_composite_score_maps(
                unified_dataset=gdf,
                session_id=session_id,
                models_per_page=self.models_per_page,
                page=self.page
            )
            
            if maps_result.get('status') == 'error':
                return self._create_error_result(
                    f"Composite score maps creation failed: {maps_result.get('message', 'Unknown error')}"
                )
            
            result_data = {
                'models_per_page': self.models_per_page,
                'current_page': self.page,
                'total_pages': maps_result.get('total_pages', 1),
                'models_shown': maps_result.get('models_shown', []),
                'web_path': maps_result.get('web_path'),
                'file_path': maps_result.get('file_path'),
                'map_type': 'composite_score_maps'
            }
            
            message = f"Created composite score maps page {self.page} showing {len(result_data['models_shown'])} models"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating composite score maps: {e}")
            return self._create_error_result(f"Composite score maps creation failed: {str(e)}")


class CreateBoxPlot(BaseTool):
    """
    Create box plot/vulnerability plot showing statistical distributions.
    
    Uses create_agent_box_plot_ranking() for ward ranking visualization.
    """
    
    top_n_wards: int = Field(
        20,
        description="Number of top wards to show per page",
        ge=5,
        le=50
    )
    
    page: int = Field(
        1,
        description="Page number to display",
        ge=1
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create box plot",
            "Show vulnerability plot",
            "Generate ward rankings plot",
            "Create statistical distribution chart"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create box plot using existing agent function"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for composite scores
            exists, resolved_col = variable_resolver.check_column_exists('composite_score', list(gdf.columns))
            if not exists:
                error_msg = variable_resolver.create_variable_error_message(
                    'composite_score', list(gdf.columns),
                    context="for analysis. Please run composite analysis first"
                )
                return self._create_error_result(error_msg)
            
            # Import the agent function
            from app.services.agents.visualizations import create_agent_box_plot_ranking
            
            # Create box plot using existing agent function
            plot_result = create_agent_box_plot_ranking(
                unified_dataset=gdf,
                session_id=session_id,
                top_n_wards=self.top_n_wards,
                page=self.page
            )
            
            if plot_result.get('status') == 'error':
                return self._create_error_result(
                    f"Box plot creation failed: {plot_result.get('message', 'Unknown error')}"
                )
            
            result_data = {
                'top_n_wards': self.top_n_wards,
                'current_page': self.page,
                'total_pages': plot_result.get('total_pages', 1),
                'wards_shown': plot_result.get('wards_shown', []),
                'web_path': plot_result.get('web_path'),
                'file_path': plot_result.get('file_path'),
                'map_type': 'box_plot'
            }
            
            message = f"Created box plot page {self.page} showing top {self.top_n_wards} wards"
            
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating box plot: {e}")
            return self._create_error_result(f"Box plot creation failed: {str(e)}")


# Legacy intervention map - keeping for compatibility
class CreateInterventionMap(BaseTool):
    """
    Create intervention coverage gaps visualization map.
    
    Custom implementation for intervention targeting analysis.
    """
    
    intervention_type: str = Field(
        "itn",
        description="Intervention type: 'itn' (bed nets), 'irs' (indoor spraying), or 'combined'",
        pattern="^(itn|irs|combined)$"
    )
    
    risk_threshold: float = Field(
        0.5,
        description="Risk score threshold for high-risk classification",
        ge=0.0,
        le=1.0
    )
    
    coverage_threshold: float = Field(
        50.0,
        description="Coverage percentage threshold for adequate coverage",
        ge=0.0,
        le=100.0
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show intervention coverage gaps",
            "Map areas needing bed net distribution",
            "Identify high-risk low-coverage wards",
            "Create intervention targeting map"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create intervention coverage gaps map"""
        try:
            # Check if session has data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset with geometry required for map visualization
            gdf = get_session_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for composite scores
            exists, resolved_col = variable_resolver.check_column_exists('composite_score', list(gdf.columns))
            if not exists:
                error_msg = variable_resolver.create_variable_error_message(
                    'composite_score', list(gdf.columns),
                    context="for analysis. Please run composite analysis first"
                )
                return self._create_error_result(error_msg)
            
            # Simple intervention gap analysis
            df_analysis = gdf.copy()
            df_analysis['high_risk'] = df_analysis['composite_score'] >= self.risk_threshold
            
            # Create mock coverage data if not available
            if not any('coverage' in col.lower() for col in df_analysis.columns):
                # Create synthetic coverage based on urban percentage
                if 'UrbanPerce' in df_analysis.columns:
                    df_analysis['synthetic_coverage'] = np.random.normal(
                        df_analysis['UrbanPerce'], 10, len(df_analysis)
                    ).clip(0, 100)
                    coverage_col = 'synthetic_coverage'
                    synthetic_data = True
                else:
                    return self._create_error_result("No coverage data available and cannot create synthetic data.")
            else:
                synthetic_data = False
                coverage_col = [col for col in df_analysis.columns if 'coverage' in col.lower()][0]
            
            df_analysis['low_coverage'] = df_analysis[coverage_col] < self.coverage_threshold
            df_analysis['priority_gap'] = df_analysis['high_risk'] & df_analysis['low_coverage']
            
            # Calculate gap analysis
            gap_wards = len(df_analysis[df_analysis['priority_gap']])
            total_wards = len(df_analysis)
            
            result_data = {
                'intervention_type': self.intervention_type,
                'coverage_variable': coverage_col,
                'synthetic_data': synthetic_data,
                'gap_analysis': {
                    'total_wards': total_wards,
                    'high_priority_wards': gap_wards,
                    'priority_percentage': round((gap_wards / total_wards) * 100, 1)
                },
                'coverage_statistics': {
                    'mean_coverage': float(df_analysis[coverage_col].mean()),
                    'median_coverage': float(df_analysis[coverage_col].median())
                },
                'map_type': 'intervention_gaps_map'
            }
            
            message = f"Analyzed intervention gaps: {gap_wards} high priority wards ({result_data['gap_analysis']['priority_percentage']}%)"
            if synthetic_data:
                message += " (using synthetic coverage data)"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating intervention map: {e}")
            return self._create_error_result(f"Intervention map creation failed: {str(e)}")


# Register tools for discovery
__all__ = [
    'CreateVulnerabilityMap',
    'CreatePCAMap', 
    'CreateUrbanExtentMap',
    'CreateDecisionTree',
    'CreateCompositeScoreMaps',
    'CreateBoxPlot',
    'CreateInterventionMap',
    'CreateVulnerabilityMapComparison'
]