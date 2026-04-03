"""Distribution-focused chart tools (histogram, violin, density)."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import scipy.stats as stats

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


class CreateHistogram(BaseTool):
    """
    Create histogram for distribution analysis of numeric variables.
    
    Shows frequency distribution with customizable bins and color schemes.
    """
    
    variable: str = Field(
        ...,
        description="Numeric column to analyze (e.g., 'composite_score', 'pfpr', 'urbanPercentage')"
    )
    
    bins: int = Field(
        30,
        description="Number of bins for the histogram",
        ge=5,
        le=100
    )
    
    color_by: Optional[str] = Field(
        None,
        description="Optional categorical column for color coding (e.g., 'Urban', 'composite_category')"
    )
    
    filter_column: Optional[str] = Field(
        None,
        description="Column to filter data by"
    )
    
    filter_value: Optional[str] = Field(
        None,
        description="Value to filter for"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show distribution of composite scores",
            "Create histogram of malaria prevalence",
            "Plot urban percentage distribution",
            "Show rainfall distribution by urban/rural"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create histogram visualization."""
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
            
            # Validate numeric column
            if not validate_numeric_column(df, self.variable):
                return self._create_error_result(
                    f"Column '{self.variable}' not found or not numeric. "
                    f"Available numeric columns: {[col for col in df.select_dtypes(include=[np.number]).columns][:10]}"
                )
            
            # Apply filter if specified
            if self.filter_column and self.filter_value:
                if self.filter_column in df.columns:
                    df = df[df[self.filter_column] == self.filter_value]
                    if len(df) == 0:
                        return self._create_error_result(
                            f"No data found for {self.filter_column} = {self.filter_value}"
                        )
            
            # Create histogram
            title = f"Distribution of {self.variable}"
            if self.filter_column and self.filter_value:
                title += f" (filtered by {self.filter_column} = {self.filter_value})"
            
            if self.color_by and self.color_by in df.columns:
                fig = px.histogram(
                    df,
                    x=self.variable,
                    color=self.color_by,
                    nbins=self.bins,
                    title=title,
                    labels={self.variable: self.variable.replace('_', ' ').title()},
                    marginal="box",
                    hover_data=['WardName'] if 'WardName' in df.columns else None
                )
            else:
                fig = px.histogram(
                    df,
                    x=self.variable,
                    nbins=self.bins,
                    title=title,
                    labels={self.variable: self.variable.replace('_', ' ').title()},
                    marginal="box"
                )
            
            # Update layout
            fig.update_layout(
                showlegend=True,
                height=600,
                hovermode='closest'
            )
            
            # Save chart
            paths = save_plotly_chart(fig, session_id, 'histogram')
            
            # Calculate statistics
            stats_data = {
                'mean': float(df[self.variable].mean()),
                'median': float(df[self.variable].median()),
                'std': float(df[self.variable].std()),
                'min': float(df[self.variable].min()),
                'max': float(df[self.variable].max()),
                'count': len(df)
            }
            
            result_data = {
                'variable': self.variable,
                'bins': self.bins,
                'statistics': stats_data,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'histogram'
            }
            
            message = f"Created histogram for {self.variable} with {self.bins} bins. "
            message += f"Mean: {stats_data['mean']:.2f}, Std: {stats_data['std']:.2f}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating histogram: {e}")
            return self._create_error_result(f"Histogram creation failed: {str(e)}")




class CreateViolinPlot(BaseTool):
    """
    Create violin plot showing distribution shapes with embedded box plots.
    
    Combines aspects of box plots and density plots for detailed distribution analysis.
    """
    
    variable: str = Field(
        ...,
        description="Numeric column to analyze"
    )
    
    category: Optional[str] = Field(
        None,
        description="Categorical column to group by (e.g., 'Urban', 'composite_category', 'LGACode')"
    )
    
    show_box: bool = Field(
        True,
        description="Show box plot inside violin"
    )
    
    show_points: bool = Field(
        False,
        description="Show individual data points"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create violin plot of risk scores by urban/rural",
            "Show distribution shape of malaria prevalence by LGA",
            "Compare composite scores across risk categories"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create violin plot visualization."""
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
            
            # Validate numeric column
            if not validate_numeric_column(df, self.variable):
                return self._create_error_result(
                    f"Column '{self.variable}' not found or not numeric."
                )
            
            # Create violin plot
            if self.category and self.category in df.columns:
                # Group by category
                fig = px.violin(
                    df,
                    y=self.variable,
                    x=self.category,
                    title=f"Distribution of {self.variable} by {self.category}",
                    box=self.show_box,
                    points='all' if self.show_points else False,
                    hover_data=['WardName'] if 'WardName' in df.columns else None
                )
            else:
                # Single violin
                fig = px.violin(
                    df,
                    y=self.variable,
                    title=f"Distribution of {self.variable}",
                    box=self.show_box,
                    points='all' if self.show_points else False
                )
            
            # Update layout
            fig.update_layout(
                showlegend=False,
                height=600,
                hovermode='closest'
            )
            
            # Save chart
            paths = save_plotly_chart(fig, session_id, 'violin_plot')
            
            result_data = {
                'variable': self.variable,
                'category': self.category,
                'show_box': self.show_box,
                'show_points': self.show_points,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'violin_plot'
            }
            
            message = f"Created violin plot for {self.variable}"
            if self.category:
                message += f" grouped by {self.category}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating violin plot: {e}")
            return self._create_error_result(f"Violin plot creation failed: {str(e)}")




class CreateDensityPlot(BaseTool):
    """
    Create density plot showing smooth distribution curves.
    
    Uses kernel density estimation for continuous probability density visualization.
    """
    
    variable: str = Field(
        ...,
        description="Numeric column to analyze"
    )
    
    group_by: Optional[str] = Field(
        None,
        description="Optional categorical column to create multiple density curves"
    )
    
    show_rug: bool = Field(
        False,
        description="Show data points as rug plot at bottom"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show density curve of composite scores",
            "Create density plot of rainfall by urban/rural",
            "Plot smooth distribution of malaria prevalence"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create density plot visualization."""
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
            
            # Validate numeric column
            if not validate_numeric_column(df, self.variable):
                return self._create_error_result(
                    f"Column '{self.variable}' not found or not numeric."
                )
            
            # Create figure
            fig = go.Figure()
            
            # Create density plot(s)
            if self.group_by and self.group_by in df.columns:
                # Multiple density curves
                groups = df[self.group_by].unique()
                colors = px.colors.qualitative.Set1[:len(groups)]
                
                for i, group in enumerate(groups):
                    group_data = df[df[self.group_by] == group][self.variable].dropna()
                    
                    # Calculate KDE
                    kde = stats.gaussian_kde(group_data)
                    x_range = np.linspace(group_data.min(), group_data.max(), 200)
                    density = kde(x_range)
                    
                    # Add density curve
                    fig.add_trace(go.Scatter(
                        x=x_range,
                        y=density,
                        mode='lines',
                        name=str(group),
                        fill='tozeroy',
                        line=dict(color=colors[i % len(colors)], width=2),
                        fillcolor=colors[i % len(colors)],
                        opacity=0.6
                    ))
                    
                    # Add rug plot if requested
                    if self.show_rug:
                        fig.add_trace(go.Scatter(
                            x=group_data,
                            y=[-0.01 * max(density)] * len(group_data),
                            mode='markers',
                            marker=dict(
                                color=colors[i % len(colors)],
                                size=3,
                                symbol='line-ns-open'
                            ),
                            showlegend=False,
                            hoverinfo='skip'
                        ))
                
                title = f"Density Plot of {self.variable} by {self.group_by}"
            else:
                # Single density curve
                data = df[self.variable].dropna()
                kde = stats.gaussian_kde(data)
                x_range = np.linspace(data.min(), data.max(), 200)
                density = kde(x_range)
                
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=density,
                    mode='lines',
                    fill='tozeroy',
                    line=dict(color='blue', width=2),
                    fillcolor='lightblue',
                    opacity=0.6
                ))
                
                if self.show_rug:
                    fig.add_trace(go.Scatter(
                        x=data,
                        y=[-0.01 * max(density)] * len(data),
                        mode='markers',
                        marker=dict(color='blue', size=3, symbol='line-ns-open'),
                        showlegend=False,
                        hoverinfo='skip'
                    ))
                
                title = f"Density Plot of {self.variable}"
            
            # Update layout
            fig.update_layout(
                title=title,
                xaxis_title=self.variable.replace('_', ' ').title(),
                yaxis_title='Density',
                showlegend=bool(self.group_by),
                height=600,
                hovermode='x unified'
            )
            
            # Save chart
            paths = save_plotly_chart(fig, session_id, 'density_plot')
            
            result_data = {
                'variable': self.variable,
                'group_by': self.group_by,
                'show_rug': self.show_rug,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'density_plot'
            }
            
            message = f"Created density plot for {self.variable}"
            if self.group_by:
                message += f" grouped by {self.group_by}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating density plot: {e}")
            return self._create_error_result(f"Density plot creation failed: {str(e)}")


# 2. CORRELATION & RELATIONSHIP CHARTS


