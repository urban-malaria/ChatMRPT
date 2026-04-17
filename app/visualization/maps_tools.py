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

import glob
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
from pydantic import Field, validator
import pandas as pd
import numpy as np
from flask import current_app

from app.utils.tool_base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)
from app.services.variable_resolver import variable_resolver

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

    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate."
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
            unified_path = f'instance/uploads/{session_id}/unified_dataset{self.year_tag}.csv'
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
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=self.year_tag)
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
            from app.visualization import create_agent_vulnerability_map
            
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
            
            # Visualization will be rendered by frontend using web_path
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating vulnerability map: {e}")
            return self._create_error_result(f"Vulnerability map creation failed: {str(e)}")



# REMOVED: CreateCompositeVulnerabilityMap - dead code, never imported or called


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

    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate."
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
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=self.year_tag)
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
            from app.visualization import create_agent_pca_vulnerability_map
            
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
            
            # Visualization will be rendered by frontend using web_path
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating PCA map: {e}")
            return self._create_error_result(f"PCA map creation failed: {str(e)}")


# REMOVED: CreateVulnerabilityMapComparison - dead code, never imported or called


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

    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate."
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
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=self.year_tag)
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
            from app.visualization import create_agent_urban_extent_map
            
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
            
            # Visualization will be rendered by frontend using web_path
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating urban extent map: {e}")
            return self._create_error_result(f"Urban extent map creation failed: {str(e)}")


# REMOVED: CreateDecisionTree - dead code, never imported or called


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

    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate."
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
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=self.year_tag)
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
            from app.visualization import create_agent_composite_score_maps
            
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
            
            # Visualization will be rendered by frontend using web_path
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating composite score maps: {e}")
            return self._create_error_result(f"Composite score maps creation failed: {str(e)}")


# REMOVED: CreateInterventionMap - dead code, never imported or called


def _build_choropleth_fig(gdf, color_col: str, category_col: str, title: str,
                          colorscale: str = 'RdYlGn_r') -> 'go.Figure':
    """Build a Plotly Choroplethmapbox for a unified dataset GeoDataFrame."""
    import plotly.graph_objects as go
    import json as _json

    geojson = _json.loads(gdf.to_json())
    gdf = gdf.copy()
    gdf['_idx'] = gdf.index.astype(str)

    ward_col = 'WardName' if 'WardName' in gdf.columns else gdf.columns[0]
    hover_cols = [ward_col, color_col]
    if category_col and category_col in gdf.columns:
        hover_cols.append(category_col)

    customdata = gdf[hover_cols].values
    hover_template = '<br>'.join(f'<b>{c}</b>: %{{customdata[{i}]}}' for i, c in enumerate(hover_cols))
    hover_template += '<extra></extra>'

    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()

    cat_order = {'High': 3, 'Medium': 2, 'Low': 1}
    if category_col and category_col in gdf.columns:
        color_vals = gdf[category_col].map(cat_order).fillna(0)
    else:
        color_vals = gdf[color_col]

    fig = go.Figure(go.Choroplethmapbox(
        geojson=geojson,
        locations=gdf['_idx'],
        z=color_vals,
        featureidkey='id',
        colorscale=colorscale,
        showscale=False,
        customdata=customdata,
        hovertemplate=hover_template,
    ))
    fig.update_layout(
        mapbox_style='carto-positron',
        mapbox_center={'lat': center_lat, 'lon': center_lon},
        mapbox_zoom=7,
        margin={'r': 0, 't': 40, 'l': 0, 'b': 0},
        title={'text': title, 'x': 0.5, 'xanchor': 'center', 'font': {'size': 16}},
    )
    return fig


class CreateMultiYearVulnerabilityMap(BaseTool):
    """Create a tabbed HTML vulnerability map with one tab per available year plus All Years aggregate."""

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION

    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create multi-year vulnerability map",
            "Show vulnerability maps for all years",
        ]

    def execute(self, session_id: str) -> ToolExecutionResult:
        from app.visualization.tab_html_builder import build_tabbed_html
        from app.utils.tool_base import get_session_unified_dataset

        session_folder = f'instance/uploads/{session_id}'
        status_path = os.path.join(session_folder, 'multi_year_vuln_status.json')

        # Determine which years are ready
        completed_tags: List[str] = []
        if os.path.exists(status_path):
            try:
                with open(status_path) as f:
                    st = json.load(f)
                completed_tags = st.get('completed_years', [])
            except Exception:
                pass

        if not completed_tags:
            # Fall back to detecting geoparquet files directly
            pattern = os.path.join(session_folder, 'unified_dataset_*.geoparquet')
            completed_tags = sorted([
                '_' + os.path.basename(p)[len('unified_dataset_'):-len('.geoparquet')]
                for p in glob.glob(pattern)
            ])

        tabs = []

        # Aggregate (All Years) tab
        agg_gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag='')
        if agg_gdf is not None and 'composite_score' in agg_gdf.columns:
            cat_col = 'vulnerability_category' if 'vulnerability_category' in agg_gdf.columns else ''
            fig = _build_choropleth_fig(
                agg_gdf, 'composite_score', cat_col,
                'Composite Vulnerability — All Years (Aggregate)'
            )
            tabs.append(('agg', 'All Years', fig))

        for year_tag in completed_tags:
            year_label = year_tag.lstrip('_')
            # Try combined_category (burden-blended) first, fall back to composite
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=year_tag)
            if gdf is None:
                continue

            score_col = 'composite_score' if 'composite_score' in gdf.columns else None
            if score_col is None:
                continue

            cat_col = 'combined_category' if 'combined_category' in gdf.columns else \
                      'vulnerability_category' if 'vulnerability_category' in gdf.columns else ''

            title = (f'Combined Environmental + Epidemiological Risk — {year_label}'
                     if cat_col == 'combined_category'
                     else f'Composite Vulnerability — {year_label}')

            fig = _build_choropleth_fig(gdf, score_col, cat_col, title)
            tabs.append((int(year_label) if year_label.isdigit() else year_tag, year_label, fig))

        if not tabs:
            return self._create_error_result(
                "No vulnerability data found. Please run risk analysis first."
            )

        if len(tabs) == 1:
            # Only aggregate — still useful, just one tab
            pass

        filename = f"vulnerability_map_composite_multi_year_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        save = build_tabbed_html(
            tabs=tabs,
            nav_label='Composite Vulnerability by year:',
            filename=filename,
            session_id=session_id,
        )

        year_labels = [label for _, label, _ in tabs if label != 'All Years']
        method_note = 'Composite Environmental Vulnerability'
        if any('Combined' in (_, lab, _)[1] for _, lab, _ in tabs if lab != 'All Years'):
            method_note = 'Combined Environmental + Epidemiological Risk (per year)'

        message = (
            f"Created composite vulnerability map with {len(tabs)} tab(s): "
            f"All Years aggregate" +
            (f" + {len(year_labels)} individual years ({', '.join(year_labels)})" if year_labels else '') +
            f".\n\nMethod: {method_note}."
        )

        return self._create_success_result(
            message=message,
            data={
                'web_path': save['web_path'],
                'file_path': save['file_path'],
                'tabs': [label for _, label, _ in tabs],
                'map_type': 'multi_year_vulnerability_map',
            }
        )


class CreateMultiYearPCAMap(BaseTool):
    """Create a tabbed HTML PCA vulnerability map with one tab per available year plus All Years aggregate."""

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION

    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create multi-year PCA map",
            "Show PCA vulnerability maps for all years",
        ]

    def execute(self, session_id: str) -> ToolExecutionResult:
        from app.visualization.tab_html_builder import build_tabbed_html
        from app.utils.tool_base import get_session_unified_dataset

        session_folder = f'instance/uploads/{session_id}'
        status_path = os.path.join(session_folder, 'multi_year_vuln_status.json')

        completed_tags: List[str] = []
        if os.path.exists(status_path):
            try:
                with open(status_path) as f:
                    st = json.load(f)
                completed_tags = st.get('completed_years', [])
            except Exception:
                pass

        if not completed_tags:
            pattern = os.path.join(session_folder, 'unified_dataset_*.geoparquet')
            completed_tags = sorted([
                '_' + os.path.basename(p)[len('unified_dataset_'):-len('.geoparquet')]
                for p in glob.glob(pattern)
            ])

        tabs = []

        pca_cols = ('pca_score', 'pc1_risk_score', 'pca_risk_score', 'pc1_score')

        def _find_pca_col(gdf):
            for c in pca_cols:
                if c in gdf.columns:
                    return c
            return None

        agg_gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag='')
        if agg_gdf is not None:
            pca_col = _find_pca_col(agg_gdf)
            if pca_col:
                cat_col = 'pca_category' if 'pca_category' in agg_gdf.columns else ''
                fig = _build_choropleth_fig(
                    agg_gdf, pca_col, cat_col,
                    'PCA Vulnerability — All Years (Aggregate)',
                    colorscale='Viridis'
                )
                tabs.append(('agg', 'All Years', fig))

        for year_tag in completed_tags:
            year_label = year_tag.lstrip('_')
            gdf = get_session_unified_dataset(session_id, require_geometry=True, year_tag=year_tag)
            if gdf is None:
                continue
            pca_col = _find_pca_col(gdf)
            if pca_col is None:
                continue
            cat_col = 'pca_category' if 'pca_category' in gdf.columns else ''
            fig = _build_choropleth_fig(
                gdf, pca_col, cat_col,
                f'PCA Vulnerability — {year_label}',
                colorscale='Viridis'
            )
            tabs.append((int(year_label) if year_label.isdigit() else year_tag, year_label, fig))

        if not tabs:
            return self._create_error_result(
                "No PCA data found. PCA may not have been suitable for this dataset, or run risk analysis first."
            )

        filename = f"vulnerability_map_pca_multi_year_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        save = build_tabbed_html(
            tabs=tabs,
            nav_label='PCA Vulnerability by year:',
            filename=filename,
            session_id=session_id,
        )

        year_labels = [label for _, label, _ in tabs if label != 'All Years']
        message = (
            f"Created PCA vulnerability map with {len(tabs)} tab(s): "
            f"All Years aggregate" +
            (f" + {len(year_labels)} individual years ({', '.join(year_labels)})" if year_labels else '') +
            f".\n\nMethod: Principal Component Analysis (PCA) of environmental variables."
        )

        return self._create_success_result(
            message=message,
            data={
                'web_path': save['web_path'],
                'file_path': save['file_path'],
                'tabs': [label for _, label, _ in tabs],
                'map_type': 'multi_year_pca_map',
            }
        )


# Register tools for discovery
__all__ = [
    'CreateVulnerabilityMap',
    'CreatePCAMap',
    'CreateUrbanExtentMap',
    'CreateCompositeScoreMaps',
    'CreateMultiYearVulnerabilityMap',
    'CreateMultiYearPCAMap',
]