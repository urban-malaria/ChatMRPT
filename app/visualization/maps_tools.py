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


# Register tools for discovery
__all__ = [
    'CreateVulnerabilityMap',
    'CreatePCAMap',
    'CreateUrbanExtentMap',
    'CreateCompositeScoreMaps',
]