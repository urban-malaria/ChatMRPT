"""Spatial and coordinate-based visualizations."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import plotly.express as px
from app.services.variable_resolution_service import variable_resolver

from app.tools.base import (
    BaseTool,
    ToolExecutionResult,
    ToolCategory,
    get_session_unified_dataset,
    validate_session_data_exists,
)
from app.tools.visualization_charts.common import (
    save_plotly_chart,
    validate_numeric_column,
    get_color_scheme,
)

logger = logging.getLogger(__name__)


class CreateBubbleMap(BaseTool):
    """
    Create bubble map showing geographic distribution with size coding.
    
    Plots bubbles on lat/lon coordinates with size representing a variable.
    """
    
    size_variable: str = Field(
        ...,
        description="Numeric column for bubble sizes"
    )
    
    color_variable: Optional[str] = Field(
        None,
        description="Optional column for bubble colors"
    )
    
    hover_data: List[str] = Field(
        default_factory=lambda: ['WardName'],
        description="Columns to show on hover"
    )
    
    map_style: str = Field(
        "open-street-map",
        description="Map style: 'open-street-map', 'carto-positron', 'stamen-terrain'",
        pattern="^(open-street-map|carto-positron|stamen-terrain)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create bubble map of population by ward",
            "Show malaria cases as bubbles on map",
            "Display risk scores as bubble sizes with color coding"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create bubble map visualization."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Check for coordinates
            lat_col = None
            lon_col = None
            for lat in ['centroid_lat', 'latitude', 'lat', 'Latitude']:
                exists, resolved_col = variable_resolver.check_column_exists(lat, list(df.columns))
                if exists:
                    lat_col = lat
                    break
            for lon in ['centroid_lon', 'longitude', 'lon', 'Longitude']:
                exists, resolved_col = variable_resolver.check_column_exists(lon, list(df.columns))
                if exists:
                    lon_col = lon
                    break
            
            if not lat_col or not lon_col:
                return self._create_error_result("No latitude/longitude columns found in data.")
            
            # Validate size variable
            if not validate_numeric_column(df, self.size_variable):
                return self._create_error_result(f"Size variable '{self.size_variable}' not found or not numeric.")
            
            # Prepare data
            plot_data = df.copy()
            plot_data = plot_data.dropna(subset=[lat_col, lon_col, self.size_variable])
            
            # Normalize sizes
            size_data = plot_data[self.size_variable]
            normalized_sizes = 20 + (size_data - size_data.min()) / (size_data.max() - size_data.min()) * 50
            
            # Create figure
            fig = px.scatter_mapbox(
                plot_data,
                lat=lat_col,
                lon=lon_col,
                size=normalized_sizes,
                color=self.color_variable if self.color_variable and self.color_variable in df.columns else None,
                hover_data=self.hover_data,
                title=f"Bubble Map: {self.size_variable}",
                mapbox_style=self.map_style
            )
            
            # Update layout
            fig.update_layout(
                height=700,
                margin={"r": 0, "t": 30, "l": 0, "b": 0}
            )
            
            # Save chart
            paths = save_plotly_chart(fig, session_id, 'bubble_map')
            
            result_data = {
                'size_variable': self.size_variable,
                'color_variable': self.color_variable,
                'n_points': len(plot_data),
                'center_lat': float(plot_data[lat_col].mean()),
                'center_lon': float(plot_data[lon_col].mean()),
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'bubble_map'
            }
            
            message = f"Created bubble map with {len(plot_data)} points sized by {self.size_variable}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating bubble map: {e}")
            return self._create_error_result(f"Bubble map creation failed: {str(e)}")




class CreateCoordinatePlot(BaseTool):
    """
    Create coordinate scatter plot for spatial pattern analysis.
    
    Simple X-Y plot of geographic coordinates with optional color/size coding.
    """
    
    color_by: Optional[str] = Field(
        None,
        description="Column for color coding points"
    )
    
    size_by: Optional[str] = Field(
        None,
        description="Column for size coding points"
    )
    
    show_labels: bool = Field(
        False,
        description="Show ward labels on points"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Plot ward coordinates colored by risk level",
            "Show spatial distribution of settlements",
            "Create coordinate scatter with population sizes"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create coordinate plot visualization."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Find coordinate columns
            lat_col = None
            lon_col = None
            for lat in ['centroid_lat', 'latitude', 'lat', 'Latitude']:
                exists, resolved_col = variable_resolver.check_column_exists(lat, list(df.columns))
                if exists:
                    lat_col = lat
                    break
            for lon in ['centroid_lon', 'longitude', 'lon', 'Longitude']:
                exists, resolved_col = variable_resolver.check_column_exists(lon, list(df.columns))
                if exists:
                    lon_col = lon
                    break
            
            if not lat_col or not lon_col:
                return self._create_error_result("No latitude/longitude columns found in data.")
            
            # Prepare hover data
            hover_data = ['WardName'] if 'WardName' in df.columns else []
            
            # Create scatter plot
            fig = px.scatter(
                df,
                x=lon_col,
                y=lat_col,
                color=self.color_by if self.color_by and self.color_by in df.columns else None,
                size=self.size_by if self.size_by and self.size_by in df.columns else None,
                hover_data=hover_data,
                title="Geographic Coordinate Plot"
            )
            
            # Add labels if requested
            if self.show_labels and 'WardName' in df.columns:
                fig.update_traces(
                    textposition='top center',
                    text=df['WardName'],
                    mode='markers+text'
                )
            
            # Update layout
            fig.update_layout(
                xaxis_title="Longitude",
                yaxis_title="Latitude",
                height=700,
                width=700
            )
            
            # Make axes equal aspect ratio
            fig.update_yaxes(scaleanchor="x", scaleratio=1)
            
            # Save chart
            paths = save_plotly_chart(fig, session_id, 'coordinate_plot')
            
            # Calculate spatial extent
            extent = {
                'min_lat': float(df[lat_col].min()),
                'max_lat': float(df[lat_col].max()),
                'min_lon': float(df[lon_col].min()),
                'max_lon': float(df[lon_col].max())
            }
            
            result_data = {
                'n_points': len(df),
                'spatial_extent': extent,
                'color_by': self.color_by,
                'size_by': self.size_by,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'coordinate_plot'
            }
            
            message = f"Created coordinate plot with {len(df)} points"
            if self.color_by:
                message += f" colored by {self.color_by}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating coordinate plot: {e}")
            return self._create_error_result(f"Coordinate plot creation failed: {str(e)}")


# Additional chart tools can be added here following the same pattern...

# Register all tools for discovery
__all__ = [
    # Statistical Distribution Charts
    'CreateHistogram',
    'CreateViolinPlot',
    'CreateDensityPlot',
    
    # Correlation & Relationship Charts
    'CreateScatterPlot',
    'CreateCorrelationHeatmap',
    'CreatePairPlot',
    'CreateRegressionPlot',
    
    # Comparative Charts
    'CreateBarChart',
    'CreateGroupedBarChart',
    'CreateStackedBarChart',
    
    # Ranking & Performance Charts
    'CreateLollipopChart',
    
    # Categorical Analysis
    'CreatePieChart',
    'CreateDonutChart',
    
    # Advanced Statistical Charts
    'CreateQQPlot',
    'CreateResidualPlot',
    'CreateBoxPlotGrid',
    
    # Geographic/Spatial Charts
    'CreateBubbleMap',
    'CreateCoordinatePlot'
]

