"""
Visualization Charts Tools for ChatMRPT - Phase 3 Implementation

This module provides comprehensive chart visualization tools for data analysis:

1. Statistical Distribution Charts - Histograms, Violin plots, Density plots
2. Correlation & Relationship Charts - Scatter plots, Heatmaps, Pair plots, Regression
3. Comparative Charts - Bar charts (single, grouped, stacked)
4. Ranking Charts - Lollipop charts, Bullet charts
5. Categorical Analysis - Pie charts, Donut charts, Treemaps
6. Advanced Statistical - QQ plots, Residual plots, Box plot grids
7. Geographic/Spatial - Bubble maps, Coordinate plots

All tools leverage Plotly for interactive visualizations and work with the unified dataset.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from pydantic import Field, validator
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import scipy.stats as stats
from sklearn.linear_model import LinearRegression
import os
from datetime import datetime

from flask import current_app

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)


def _save_plotly_chart(fig: go.Figure, session_id: str, chart_name: str) -> Dict[str, str]:
    """Save a Plotly figure as HTML and return paths."""
    try:
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{chart_name}_{timestamp}.html"
        
        # Ensure session directory exists
        session_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        file_path = os.path.join(session_dir, filename)
        
        # Save the figure
        fig.write_html(file_path)
        
        # Generate web path
        web_path = f"/serve_viz_file/{session_id}/{filename}"
        
        return {
            'file_path': file_path,
            'web_path': web_path,
            'filename': filename
        }
    except Exception as e:
        logger.error(f"Error saving chart: {e}")
        raise


def _validate_numeric_column(df: pd.DataFrame, column: str) -> bool:
    """Validate that a column exists and is numeric."""
    if column not in df.columns:
        return False
    return pd.api.types.is_numeric_dtype(df[column])


def _get_color_scheme(scheme: str) -> str:
    """Get valid Plotly color scheme."""
    valid_schemes = {
        'viridis': 'viridis',
        'plasma': 'plasma',
        'inferno': 'inferno',
        'blues': 'Blues',
        'reds': 'Reds',
        'greens': 'Greens',
        'plotly': 'plotly',
        'set1': 'Set1',
        'set2': 'Set2',
        'set3': 'Set3'
    }
    return valid_schemes.get(scheme.lower(), 'viridis')


# 1. STATISTICAL DISTRIBUTION CHARTS

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
            if not _validate_numeric_column(df, self.variable):
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
            paths = _save_plotly_chart(fig, session_id, 'histogram')
            
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
            if not _validate_numeric_column(df, self.variable):
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
            paths = _save_plotly_chart(fig, session_id, 'violin_plot')
            
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
            if not _validate_numeric_column(df, self.variable):
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
            paths = _save_plotly_chart(fig, session_id, 'density_plot')
            
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

class CreateScatterPlot(BaseTool):
    """
    Create scatter plot for analyzing relationships between two numeric variables.
    
    Includes optional trend lines, color coding, and size coding.
    """
    
    x_variable: str = Field(
        ...,
        description="Variable for X-axis"
    )
    
    y_variable: str = Field(
        ...,
        description="Variable for Y-axis"
    )
    
    color_by: Optional[str] = Field(
        None,
        description="Optional categorical column for color coding"
    )
    
    size_by: Optional[str] = Field(
        None,
        description="Optional numeric column for size coding"
    )
    
    trendline: str = Field(
        "none",
        description="Type of trendline: 'none', 'ols' (linear), 'lowess' (smooth)",
        pattern="^(none|ols|lowess)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Plot rainfall vs malaria prevalence",
            "Show correlation between temperature and composite score",
            "Create scatter plot of urban percentage vs risk score with trendline"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create scatter plot visualization."""
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
            
            # Validate numeric columns
            if not _validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not _validate_numeric_column(df, self.y_variable):
                return self._create_error_result(f"Y variable '{self.y_variable}' not found or not numeric.")
            
            # Prepare hover data
            hover_data = ['WardName'] if 'WardName' in df.columns else []
            
            # Create scatter plot
            try:
                trendline_arg = self.trendline if self.trendline != "none" else None
                
                fig = px.scatter(
                    df,
                    x=self.x_variable,
                    y=self.y_variable,
                    color=self.color_by if self.color_by and self.color_by in df.columns else None,
                    size=self.size_by if self.size_by and self.size_by in df.columns else None,
                    hover_data=hover_data,
                    trendline=trendline_arg,
                    title=f"{self.y_variable} vs {self.x_variable}"
                )
            except Exception as e:
                # If trendline fails (e.g., missing statsmodels), create without it
                if "statsmodels" in str(e).lower():
                    fig = px.scatter(
                        df,
                        x=self.x_variable,
                        y=self.y_variable,
                        color=self.color_by if self.color_by and self.color_by in df.columns else None,
                        size=self.size_by if self.size_by and self.size_by in df.columns else None,
                        hover_data=hover_data,
                        title=f"{self.y_variable} vs {self.x_variable} (no trendline)"
                    )
                    
                    # Add manual linear trendline if requested
                    if self.trendline == "ols":
                        from sklearn.linear_model import LinearRegression
                        model = LinearRegression()
                        X = df[self.x_variable].values.reshape(-1, 1)
                        y = df[self.y_variable].values
                        model.fit(X, y)
                        
                        x_range = np.linspace(X.min(), X.max(), 100)
                        y_pred = model.predict(x_range.reshape(-1, 1))
                        
                        fig.add_trace(go.Scatter(
                            x=x_range.flatten(),
                            y=y_pred,
                            mode='lines',
                            name='Trendline',
                            line=dict(color='red', width=2)
                        ))
                else:
                    raise e
            
            # Update layout
            fig.update_layout(
                height=600,
                hovermode='closest'
            )
            
            # Calculate correlation
            correlation = df[self.x_variable].corr(df[self.y_variable])
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'scatter_plot')
            
            result_data = {
                'x_variable': self.x_variable,
                'y_variable': self.y_variable,
                'correlation': float(correlation),
                'trendline': self.trendline,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'scatter_plot'
            }
            
            message = f"Created scatter plot: {self.y_variable} vs {self.x_variable}. "
            message += f"Correlation: {correlation:.3f}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating scatter plot: {e}")
            return self._create_error_result(f"Scatter plot creation failed: {str(e)}")


class CreateCorrelationHeatmap(BaseTool):
    """
    Create correlation heatmap showing relationships between multiple numeric variables.
    
    Displays correlation matrix as an interactive heatmap.
    """
    
    variables: Optional[List[str]] = Field(
        None,
        description="List of numeric columns to include. If None, uses top 20 numeric columns."
    )
    
    method: str = Field(
        "pearson",
        description="Correlation method: 'pearson', 'spearman', or 'kendall'",
        pattern="^(pearson|spearman|kendall)$"
    )
    
    color_scale: str = Field(
        "RdBu",
        description="Color scale for heatmap"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create correlation heatmap of environmental variables",
            "Show correlations between all risk indicators",
            "Generate correlation matrix for health and demographic variables"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create correlation heatmap visualization."""
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
            
            # Select numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if self.variables:
                # Use specified variables
                valid_vars = [v for v in self.variables if v in numeric_cols]
                if not valid_vars:
                    return self._create_error_result(
                        "None of the specified variables are numeric columns."
                    )
                selected_cols = valid_vars
            else:
                # Use top 20 numeric columns (excluding IDs and coordinates)
                exclude_patterns = ['_id', 'code', 'lat', 'lon', 'x', 'y']
                selected_cols = [col for col in numeric_cols 
                               if not any(pattern in col.lower() for pattern in exclude_patterns)][:20]
            
            # Calculate correlation matrix
            corr_matrix = df[selected_cols].corr(method=self.method)
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                colorscale=self.color_scale,
                zmid=0,
                text=np.round(corr_matrix.values, 2),
                texttemplate='%{text}',
                textfont={"size": 10},
                hoverongaps=False,
                hovertemplate='%{x} vs %{y}: %{z:.3f}<extra></extra>'
            ))
            
            # Update layout
            fig.update_layout(
                title=f"Correlation Heatmap ({self.method.title()} method)",
                height=800,
                width=800,
                xaxis=dict(tickangle=-45),
                yaxis=dict(tickangle=0)
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'correlation_heatmap')
            
            # Find strongest correlations
            corr_values = corr_matrix.values
            np.fill_diagonal(corr_values, 0)  # Exclude self-correlations
            max_corr_idx = np.unravel_index(np.abs(corr_values).argmax(), corr_values.shape)
            strongest_corr = {
                'variables': (corr_matrix.columns[max_corr_idx[0]], corr_matrix.columns[max_corr_idx[1]]),
                'value': float(corr_values[max_corr_idx])
            }
            
            result_data = {
                'variables_count': len(selected_cols),
                'method': self.method,
                'strongest_correlation': strongest_corr,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'correlation_heatmap'
            }
            
            message = f"Created {self.method} correlation heatmap with {len(selected_cols)} variables. "
            message += f"Strongest correlation: {strongest_corr['variables'][0]} vs {strongest_corr['variables'][1]} ({strongest_corr['value']:.3f})"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating correlation heatmap: {e}")
            return self._create_error_result(f"Correlation heatmap creation failed: {str(e)}")


class CreatePairPlot(BaseTool):
    """
    Create pair plot (scatter plot matrix) for multiple variable relationships.
    
    Shows pairwise relationships between multiple numeric variables.
    """
    
    variables: List[str] = Field(
        ...,
        description="List of numeric columns to include (2-6 recommended)",
        min_items=2,
        max_items=6
    )
    
    hue: Optional[str] = Field(
        None,
        description="Categorical column for color coding"
    )
    
    plot_type: str = Field(
        "scatter",
        description="Type of plot: 'scatter' or 'kde' (density)",
        pattern="^(scatter|kde)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create pair plot of environmental variables",
            "Show relationships between risk scores and health indicators",
            "Generate scatter matrix of key malaria predictors"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create pair plot visualization."""
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
            
            # Validate variables
            valid_vars = [v for v in self.variables if _validate_numeric_column(df, v)]
            if len(valid_vars) < 2:
                return self._create_error_result(
                    "At least 2 valid numeric variables required for pair plot."
                )
            
            # Create subplots
            n_vars = len(valid_vars)
            fig = make_subplots(
                rows=n_vars, cols=n_vars,
                shared_xaxes=True,
                shared_yaxes=True,
                horizontal_spacing=0.05,
                vertical_spacing=0.05
            )
            
            # Add plots
            for i, var1 in enumerate(valid_vars):
                for j, var2 in enumerate(valid_vars):
                    row = i + 1
                    col = j + 1
                    
                    if i == j:
                        # Diagonal - show histogram
                        hist_data = df[var1].dropna()
                        fig.add_trace(
                            go.Histogram(x=hist_data, nbinsx=20, name=var1, showlegend=False),
                            row=row, col=col
                        )
                    else:
                        # Off-diagonal - show scatter
                        if self.plot_type == "scatter":
                            if self.hue and self.hue in df.columns:
                                # Color by category
                                for category in df[self.hue].unique():
                                    mask = df[self.hue] == category
                                    fig.add_trace(
                                        go.Scatter(
                                            x=df.loc[mask, var2],
                                            y=df.loc[mask, var1],
                                            mode='markers',
                                            name=str(category),
                                            showlegend=(i == 0 and j == 1),
                                            marker=dict(size=5, opacity=0.6)
                                        ),
                                        row=row, col=col
                                    )
                            else:
                                fig.add_trace(
                                    go.Scatter(
                                        x=df[var2],
                                        y=df[var1],
                                        mode='markers',
                                        showlegend=False,
                                        marker=dict(size=5, opacity=0.6)
                                    ),
                                    row=row, col=col
                                )
                    
                    # Add axis labels
                    if i == n_vars - 1:
                        fig.update_xaxes(title_text=var2, row=row, col=col)
                    if j == 0:
                        fig.update_yaxes(title_text=var1, row=row, col=col)
            
            # Update layout
            fig.update_layout(
                title=f"Pair Plot of {', '.join(valid_vars[:3])}{'...' if len(valid_vars) > 3 else ''}",
                height=200 * n_vars,
                width=200 * n_vars,
                showlegend=bool(self.hue)
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'pair_plot')
            
            result_data = {
                'variables': valid_vars,
                'plot_type': self.plot_type,
                'hue': self.hue,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'pair_plot'
            }
            
            message = f"Created pair plot with {len(valid_vars)} variables"
            if self.hue:
                message += f" colored by {self.hue}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating pair plot: {e}")
            return self._create_error_result(f"Pair plot creation failed: {str(e)}")


class CreateRegressionPlot(BaseTool):
    """
    Create regression plot with fitted line and confidence intervals.
    
    Shows linear regression analysis between two variables.
    """
    
    x_variable: str = Field(
        ...,
        description="Independent variable (X-axis)"
    )
    
    y_variable: str = Field(
        ...,
        description="Dependent variable (Y-axis)"
    )
    
    show_confidence: bool = Field(
        True,
        description="Show 95% confidence interval"
    )
    
    show_equation: bool = Field(
        True,
        description="Show regression equation and R-squared"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create regression plot of temperature vs malaria prevalence",
            "Show linear relationship between rainfall and risk score",
            "Plot regression of urban percentage against composite score"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create regression plot visualization."""
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
            
            # Validate columns
            if not _validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not _validate_numeric_column(df, self.y_variable):
                return self._create_error_result(f"Y variable '{self.y_variable}' not found or not numeric.")
            
            # Prepare data
            data = df[[self.x_variable, self.y_variable]].dropna()
            X = data[self.x_variable].values.reshape(-1, 1)
            y = data[self.y_variable].values
            
            # Fit regression
            model = LinearRegression()
            model.fit(X, y)
            y_pred = model.predict(X)
            
            # Calculate R-squared
            from sklearn.metrics import r2_score
            r2 = r2_score(y, y_pred)
            
            # Create figure
            fig = go.Figure()
            
            # Add scatter points
            fig.add_trace(go.Scatter(
                x=data[self.x_variable],
                y=data[self.y_variable],
                mode='markers',
                name='Data',
                marker=dict(size=8, opacity=0.6),
                text=df.loc[data.index, 'WardName'] if 'WardName' in df.columns else None,
                hovertemplate='%{text}<br>%{x}: %{x:.2f}<br>%{y}: %{y:.2f}<extra></extra>'
            ))
            
            # Add regression line
            x_range = np.linspace(X.min(), X.max(), 100)
            y_range = model.predict(x_range.reshape(-1, 1))
            
            fig.add_trace(go.Scatter(
                x=x_range,
                y=y_range,
                mode='lines',
                name='Regression Line',
                line=dict(color='red', width=3)
            ))
            
            # Add confidence interval if requested
            if self.show_confidence:
                # Calculate standard error
                n = len(X)
                residuals = y - y_pred
                mse = np.sum(residuals**2) / (n - 2)
                x_mean = X.mean()
                
                # Calculate confidence interval
                t_val = stats.t.ppf(0.975, n - 2)  # 95% CI
                
                ci = []
                for x_val in x_range:
                    se = np.sqrt(mse * (1/n + (x_val - x_mean)**2 / np.sum((X - x_mean)**2)))
                    ci.append(t_val * se)
                
                ci = np.array(ci)
                
                # Add CI bands
                fig.add_trace(go.Scatter(
                    x=np.concatenate([x_range, x_range[::-1]]),
                    y=np.concatenate([y_range + ci, (y_range - ci)[::-1]]),
                    fill='toself',
                    fillcolor='rgba(255,0,0,0.2)',
                    line=dict(color='rgba(255,0,0,0)'),
                    name='95% CI',
                    showlegend=True,
                    hoverinfo='skip'
                ))
            
            # Add equation if requested
            equation_text = ""
            if self.show_equation:
                equation_text = f"y = {model.coef_[0]:.3f}x + {model.intercept_:.3f}<br>R² = {r2:.3f}"
            
            # Update layout
            fig.update_layout(
                title=f"Regression Analysis: {self.y_variable} vs {self.x_variable}",
                xaxis_title=self.x_variable,
                yaxis_title=self.y_variable,
                height=600,
                annotations=[
                    dict(
                        x=0.05,
                        y=0.95,
                        xref='paper',
                        yref='paper',
                        text=equation_text,
                        showarrow=False,
                        bgcolor='white',
                        bordercolor='black',
                        borderwidth=1
                    )
                ] if self.show_equation else []
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'regression_plot')
            
            result_data = {
                'x_variable': self.x_variable,
                'y_variable': self.y_variable,
                'slope': float(model.coef_[0]),
                'intercept': float(model.intercept_),
                'r_squared': float(r2),
                'n_points': len(data),
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'regression_plot'
            }
            
            message = f"Created regression plot. R² = {r2:.3f}, "
            message += f"Equation: y = {model.coef_[0]:.3f}x + {model.intercept_:.3f}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating regression plot: {e}")
            return self._create_error_result(f"Regression plot creation failed: {str(e)}")


# 3. COMPARATIVE CHARTS

class CreateBarChart(BaseTool):
    """
    Create bar chart for categorical comparisons or rankings.
    
    Can show top/bottom N items or compare categories.
    """
    
    category: str = Field(
        ...,
        description="Categorical column (e.g., 'WardName', 'LGACode', 'composite_category')"
    )
    
    value: str = Field(
        ...,
        description="Numeric column for bar heights"
    )
    
    top_n: Optional[int] = Field(
        20,
        description="Show only top N categories (by value)",
        ge=1,
        le=50
    )
    
    sort_order: str = Field(
        "descending",
        description="Sort order: 'ascending' or 'descending'",
        pattern="^(ascending|descending)$"
    )
    
    orientation: str = Field(
        "vertical",
        description="Bar orientation: 'vertical' or 'horizontal'",
        pattern="^(vertical|horizontal)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show top 20 wards by composite score",
            "Create bar chart of average risk by LGA",
            "Display malaria prevalence by settlement type"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create bar chart visualization."""
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
            
            # Validate columns
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            if not _validate_numeric_column(df, self.value):
                return self._create_error_result(f"Value column '{self.value}' not found or not numeric.")
            
            # Prepare data
            if self.category == self.value:
                # Simple value ranking
                data = df[[self.category, self.value]].copy()
                data = data.rename(columns={self.category: 'Category'})
                x_col = 'Category'
                y_col = self.value
            else:
                # Aggregate by category
                data = df.groupby(self.category)[self.value].mean().reset_index()
                x_col = self.category
                y_col = self.value
            
            # Sort and limit
            ascending = (self.sort_order == "ascending")
            data = data.sort_values(y_col, ascending=ascending)
            
            if self.top_n and len(data) > self.top_n:
                data = data.head(self.top_n) if not ascending else data.tail(self.top_n)
            
            # Create bar chart
            if self.orientation == "horizontal":
                fig = px.bar(
                    data,
                    x=y_col,
                    y=x_col,
                    orientation='h',
                    title=f"{'Top' if not ascending else 'Bottom'} {len(data)} {self.category} by {self.value}",
                    labels={y_col: self.value.replace('_', ' ').title()}
                )
            else:
                fig = px.bar(
                    data,
                    x=x_col,
                    y=y_col,
                    title=f"{'Top' if not ascending else 'Bottom'} {len(data)} {self.category} by {self.value}",
                    labels={y_col: self.value.replace('_', ' ').title()}
                )
            
            # Update layout
            fig.update_layout(
                height=600 if self.orientation == "vertical" else max(400, len(data) * 25),
                showlegend=False,
                xaxis_tickangle=-45 if self.orientation == "vertical" else 0
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'bar_chart')
            
            result_data = {
                'category': self.category,
                'value': self.value,
                'n_items': len(data),
                'sort_order': self.sort_order,
                'orientation': self.orientation,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'bar_chart'
            }
            
            message = f"Created bar chart showing {len(data)} {self.category} items by {self.value}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating bar chart: {e}")
            return self._create_error_result(f"Bar chart creation failed: {str(e)}")


class CreateGroupedBarChart(BaseTool):
    """
    Create grouped bar chart for multi-category comparisons.
    
    Shows multiple series side-by-side for each category.
    """
    
    category: str = Field(
        ...,
        description="Main grouping category (X-axis)"
    )
    
    values: List[str] = Field(
        ...,
        description="List of numeric columns to compare",
        min_items=2,
        max_items=5
    )
    
    normalize: bool = Field(
        False,
        description="Normalize values to percentages"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Compare composite and PCA scores by LGA",
            "Show environmental indicators by settlement type",
            "Display multiple risk factors by urban/rural"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create grouped bar chart visualization."""
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
            
            # Validate columns
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            
            valid_values = [v for v in self.values if _validate_numeric_column(df, v)]
            if len(valid_values) < 2:
                return self._create_error_result("At least 2 valid numeric columns required.")
            
            # Aggregate data
            agg_data = df.groupby(self.category)[valid_values].mean().reset_index()
            
            # Melt for grouped bar chart
            melted = agg_data.melt(
                id_vars=[self.category],
                value_vars=valid_values,
                var_name='Metric',
                value_name='Value'
            )
            
            # Normalize if requested
            if self.normalize:
                totals = melted.groupby(self.category)['Value'].transform('sum')
                melted['Value'] = (melted['Value'] / totals * 100)
                y_label = 'Percentage'
            else:
                y_label = 'Value'
            
            # Create grouped bar chart
            fig = px.bar(
                melted,
                x=self.category,
                y='Value',
                color='Metric',
                title=f"Comparison of {', '.join(valid_values[:2])}{'...' if len(valid_values) > 2 else ''} by {self.category}",
                labels={'Value': y_label},
                barmode='group'
            )
            
            # Update layout
            fig.update_layout(
                height=600,
                xaxis_tickangle=-45,
                legend_title_text='Metrics'
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'grouped_bar_chart')
            
            result_data = {
                'category': self.category,
                'metrics': valid_values,
                'n_categories': len(agg_data),
                'normalized': self.normalize,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'grouped_bar_chart'
            }
            
            message = f"Created grouped bar chart comparing {len(valid_values)} metrics across {len(agg_data)} {self.category} categories"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating grouped bar chart: {e}")
            return self._create_error_result(f"Grouped bar chart creation failed: {str(e)}")


class CreateStackedBarChart(BaseTool):
    """
    Create stacked bar chart showing composition of categories.
    
    Useful for showing proportions within categories.
    """
    
    category: str = Field(
        ...,
        description="Main category for X-axis"
    )
    
    stack_by: str = Field(
        ...,
        description="Category to create stacks (e.g., 'composite_category', 'Urban')"
    )
    
    normalize: bool = Field(
        True,
        description="Normalize to 100% stacked bars"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show risk category distribution by LGA",
            "Display urban/rural composition by settlement type",
            "Create stacked bar of vulnerability levels by district"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create stacked bar chart visualization."""
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
            
            # Validate columns
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            if self.stack_by not in df.columns:
                return self._create_error_result(f"Stack column '{self.stack_by}' not found.")
            
            # Create crosstab
            ct = pd.crosstab(df[self.category], df[self.stack_by])
            
            if self.normalize:
                ct = ct.div(ct.sum(axis=1), axis=0) * 100
            
            # Prepare data for plotting
            data = []
            for stack_cat in ct.columns:
                data.append(go.Bar(
                    name=str(stack_cat),
                    x=ct.index,
                    y=ct[stack_cat]
                ))
            
            # Create figure
            fig = go.Figure(data=data)
            
            # Update layout
            fig.update_layout(
                barmode='stack',
                title=f"Distribution of {self.stack_by} by {self.category}",
                xaxis_title=self.category,
                yaxis_title='Percentage' if self.normalize else 'Count',
                height=600,
                xaxis_tickangle=-45
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'stacked_bar_chart')
            
            result_data = {
                'category': self.category,
                'stack_by': self.stack_by,
                'n_categories': len(ct.index),
                'n_stacks': len(ct.columns),
                'normalized': self.normalize,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'stacked_bar_chart'
            }
            
            message = f"Created stacked bar chart showing {self.stack_by} distribution across {len(ct.index)} {self.category} categories"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating stacked bar chart: {e}")
            return self._create_error_result(f"Stacked bar chart creation failed: {str(e)}")


# 4. RANKING & PERFORMANCE CHARTS

class CreateLollipopChart(BaseTool):
    """
    Create lollipop chart for elegant ranking visualization.
    
    Alternative to bar charts with cleaner appearance for rankings.
    """
    
    category: str = Field(
        ...,
        description="Category column (e.g., 'WardName')"
    )
    
    value: str = Field(
        ...,
        description="Numeric column for values"
    )
    
    top_n: int = Field(
        15,
        description="Number of items to show",
        ge=5,
        le=30
    )
    
    color_threshold: Optional[float] = Field(
        None,
        description="Value threshold for color coding"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create lollipop chart of top risk wards",
            "Show ward rankings with color threshold",
            "Display elegant ranking of LGAs by average score"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create lollipop chart visualization."""
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
            
            # Validate columns
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            if not _validate_numeric_column(df, self.value):
                return self._create_error_result(f"Value column '{self.value}' not found or not numeric.")
            
            # Prepare data
            if self.category != self.value:
                data = df.groupby(self.category)[self.value].mean().reset_index()
            else:
                data = df[[self.category, self.value]].copy()
            
            # Sort and limit
            data = data.sort_values(self.value, ascending=False).head(self.top_n)
            
            # Create figure
            fig = go.Figure()
            
            # Add lollipop elements
            for idx, row in data.iterrows():
                color = 'red' if self.color_threshold and row[self.value] > self.color_threshold else 'blue'
                
                # Add line
                fig.add_trace(go.Scatter(
                    x=[0, row[self.value]],
                    y=[row[self.category], row[self.category]],
                    mode='lines',
                    line=dict(color=color, width=2),
                    showlegend=False,
                    hoverinfo='skip'
                ))
                
                # Add dot
                fig.add_trace(go.Scatter(
                    x=[row[self.value]],
                    y=[row[self.category]],
                    mode='markers',
                    marker=dict(color=color, size=12),
                    showlegend=False,
                    text=f"{row[self.value]:.2f}",
                    hovertemplate=f"{row[self.category]}: {row[self.value]:.2f}<extra></extra>"
                ))
            
            # Update layout
            fig.update_layout(
                title=f"Top {self.top_n} {self.category} by {self.value}",
                xaxis_title=self.value,
                yaxis_title="",
                height=max(400, self.top_n * 30),
                showlegend=False,
                yaxis=dict(autorange="reversed")
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'lollipop_chart')
            
            result_data = {
                'category': self.category,
                'value': self.value,
                'n_items': len(data),
                'color_threshold': self.color_threshold,
                'max_value': float(data[self.value].max()),
                'min_value': float(data[self.value].min()),
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'lollipop_chart'
            }
            
            message = f"Created lollipop chart showing top {len(data)} {self.category} by {self.value}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating lollipop chart: {e}")
            return self._create_error_result(f"Lollipop chart creation failed: {str(e)}")


# 5. CATEGORICAL ANALYSIS

class CreatePieChart(BaseTool):
    """
    Create pie chart for proportional breakdown of categories.
    
    Shows composition of a categorical variable.
    """
    
    category: str = Field(
        ...,
        description="Categorical column to analyze"
    )
    
    values: Optional[str] = Field(
        None,
        description="Optional numeric column to sum (default: count)"
    )
    
    top_n: int = Field(
        10,
        description="Show top N categories, rest grouped as 'Other'",
        ge=3,
        le=20
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show distribution of risk categories",
            "Create pie chart of settlement types",
            "Display ward distribution by LGA"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create pie chart visualization."""
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
            
            # Validate category column
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            
            # Prepare data
            if self.values and self.values in df.columns:
                # Sum values by category
                data = df.groupby(self.category)[self.values].sum().reset_index()
                data.columns = ['Category', 'Value']
            else:
                # Count occurrences
                data = df[self.category].value_counts().reset_index()
                data.columns = ['Category', 'Value']
            
            # Sort and limit
            data = data.sort_values('Value', ascending=False)
            
            if len(data) > self.top_n:
                top_data = data.head(self.top_n - 1)
                other_value = data.iloc[self.top_n - 1:]['Value'].sum()
                other_row = pd.DataFrame({'Category': ['Other'], 'Value': [other_value]})
                data = pd.concat([top_data, other_row], ignore_index=True)
            
            # Create pie chart
            fig = px.pie(
                data,
                values='Value',
                names='Category',
                title=f"Distribution of {self.category}"
            )
            
            # Update layout
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='%{label}: %{value}<br>%{percent}<extra></extra>'
            )
            
            fig.update_layout(
                height=600,
                showlegend=True
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'pie_chart')
            
            result_data = {
                'category': self.category,
                'n_categories': len(data),
                'total_value': float(data['Value'].sum()),
                'largest_category': data.iloc[0]['Category'],
                'largest_percentage': float(data.iloc[0]['Value'] / data['Value'].sum() * 100),
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'pie_chart'
            }
            
            message = f"Created pie chart showing {len(data)} categories. "
            message += f"Largest: {result_data['largest_category']} ({result_data['largest_percentage']:.1f}%)"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating pie chart: {e}")
            return self._create_error_result(f"Pie chart creation failed: {str(e)}")


class CreateDonutChart(BaseTool):
    """
    Create donut chart - modern alternative to pie chart.
    
    Ring-style chart with center text for key metric.
    """
    
    category: str = Field(
        ...,
        description="Categorical column to analyze"
    )
    
    center_text: Optional[str] = Field(
        None,
        description="Text to display in center (e.g., total count)"
    )
    
    hole_size: float = Field(
        0.4,
        description="Size of center hole (0-1)",
        ge=0.2,
        le=0.8
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create donut chart of risk categories",
            "Show urban/rural distribution as donut",
            "Display settlement types in modern donut style"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create donut chart visualization."""
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
            
            # Validate category column
            if self.category not in df.columns:
                return self._create_error_result(f"Category column '{self.category}' not found.")
            
            # Prepare data
            data = df[self.category].value_counts().reset_index()
            data.columns = ['Category', 'Count']
            
            # Create donut chart
            fig = px.pie(
                data,
                values='Count',
                names='Category',
                title=f"Distribution of {self.category}",
                hole=self.hole_size
            )
            
            # Add center text if provided
            if self.center_text:
                center_text_display = self.center_text
            else:
                center_text_display = f"Total: {data['Count'].sum()}"
            
            fig.update_layout(
                annotations=[dict(
                    text=center_text_display,
                    x=0.5, y=0.5,
                    font_size=20,
                    showarrow=False
                )],
                height=600
            )
            
            # Update traces
            fig.update_traces(
                textposition='auto',
                textinfo='percent+label',
                hovertemplate='%{label}: %{value}<br>%{percent}<extra></extra>'
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'donut_chart')
            
            result_data = {
                'category': self.category,
                'n_categories': len(data),
                'total_count': int(data['Count'].sum()),
                'hole_size': self.hole_size,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'donut_chart'
            }
            
            message = f"Created donut chart showing {len(data)} {self.category} categories"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating donut chart: {e}")
            return self._create_error_result(f"Donut chart creation failed: {str(e)}")


# 6. ADVANCED STATISTICAL CHARTS

class CreateQQPlot(BaseTool):
    """
    Create Q-Q plot for testing normality of distributions.
    
    Compares sample quantiles against theoretical normal distribution.
    """
    
    variable: str = Field(
        ...,
        description="Numeric column to test for normality"
    )
    
    show_line: bool = Field(
        True,
        description="Show reference line for perfect normality"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Test normality of composite scores",
            "Create QQ plot for rainfall distribution",
            "Check if malaria prevalence follows normal distribution"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create QQ plot visualization."""
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
            if not _validate_numeric_column(df, self.variable):
                return self._create_error_result(
                    f"Column '{self.variable}' not found or not numeric."
                )
            
            # Prepare data
            data = df[self.variable].dropna()
            
            # Calculate QQ values
            qq_data = stats.probplot(data, dist="norm")
            theoretical_quantiles = qq_data[0][0]
            sample_quantiles = qq_data[0][1]
            
            # Create figure
            fig = go.Figure()
            
            # Add QQ points
            fig.add_trace(go.Scatter(
                x=theoretical_quantiles,
                y=sample_quantiles,
                mode='markers',
                name='Data',
                marker=dict(size=8, opacity=0.6)
            ))
            
            # Add reference line if requested
            if self.show_line:
                # Calculate best fit line
                slope = qq_data[1][0]
                intercept = qq_data[1][1]
                r_squared = qq_data[1][2] ** 2
                
                x_range = np.array([theoretical_quantiles.min(), theoretical_quantiles.max()])
                y_range = slope * x_range + intercept
                
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=y_range,
                    mode='lines',
                    name='Normal Line',
                    line=dict(color='red', dash='dash')
                ))
                
                # Add R-squared annotation
                fig.add_annotation(
                    x=0.05,
                    y=0.95,
                    xref='paper',
                    yref='paper',
                    text=f"R² = {r_squared:.3f}",
                    showarrow=False,
                    bgcolor='white',
                    bordercolor='black',
                    borderwidth=1
                )
            
            # Update layout
            fig.update_layout(
                title=f"Q-Q Plot: {self.variable}",
                xaxis_title="Theoretical Quantiles",
                yaxis_title="Sample Quantiles",
                height=600,
                showlegend=True
            )
            
            # Perform Shapiro-Wilk test
            if len(data) < 5000:  # Shapiro-Wilk is for smaller samples
                statistic, p_value = stats.shapiro(data)
                normality_test = {
                    'test': 'Shapiro-Wilk',
                    'statistic': float(statistic),
                    'p_value': float(p_value),
                    'is_normal': p_value > 0.05
                }
            else:
                # Use Kolmogorov-Smirnov for larger samples
                statistic, p_value = stats.kstest(data, 'norm', args=(data.mean(), data.std()))
                normality_test = {
                    'test': 'Kolmogorov-Smirnov',
                    'statistic': float(statistic),
                    'p_value': float(p_value),
                    'is_normal': p_value > 0.05
                }
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'qq_plot')
            
            result_data = {
                'variable': self.variable,
                'n_points': len(data),
                'normality_test': normality_test,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'qq_plot'
            }
            
            message = f"Created Q-Q plot for {self.variable}. "
            message += f"{normality_test['test']}: p-value = {normality_test['p_value']:.4f} "
            message += f"({'normally distributed' if normality_test['is_normal'] else 'not normally distributed'})"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating QQ plot: {e}")
            return self._create_error_result(f"QQ plot creation failed: {str(e)}")


class CreateResidualPlot(BaseTool):
    """
    Create residual plot for regression diagnostics.
    
    Shows residuals vs fitted values to check regression assumptions.
    """
    
    x_variable: str = Field(
        ...,
        description="Independent variable"
    )
    
    y_variable: str = Field(
        ...,
        description="Dependent variable"
    )
    
    show_smoother: bool = Field(
        True,
        description="Show LOWESS smoother line"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create residual plot for rainfall vs malaria model",
            "Check regression assumptions for temperature model",
            "Analyze residual patterns in risk prediction"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create residual plot visualization."""
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
            
            # Validate columns
            if not _validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not _validate_numeric_column(df, self.y_variable):
                return self._create_error_result(f"Y variable '{self.y_variable}' not found or not numeric.")
            
            # Prepare data
            data = df[[self.x_variable, self.y_variable]].dropna()
            X = data[self.x_variable].values.reshape(-1, 1)
            y = data[self.y_variable].values
            
            # Fit model
            model = LinearRegression()
            model.fit(X, y)
            y_pred = model.predict(X)
            residuals = y - y_pred
            
            # Create figure
            fig = go.Figure()
            
            # Add residual scatter
            fig.add_trace(go.Scatter(
                x=y_pred,
                y=residuals,
                mode='markers',
                name='Residuals',
                marker=dict(size=8, opacity=0.6)
            ))
            
            # Add zero line
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            
            # Add smoother if requested
            if self.show_smoother:
                try:
                    from statsmodels.nonparametric.smoothers_lowess import lowess
                    smoothed = lowess(residuals, y_pred, frac=0.3)
                    
                    fig.add_trace(go.Scatter(
                        x=smoothed[:, 0],
                        y=smoothed[:, 1],
                        mode='lines',
                        name='LOWESS',
                        line=dict(color='red', width=3)
                    ))
                except ImportError:
                    # Simple moving average as fallback
                    import pandas as pd
                    sorted_indices = np.argsort(y_pred)
                    sorted_pred = y_pred[sorted_indices]
                    sorted_resid = residuals[sorted_indices]
                    
                    # Simple rolling mean
                    window = max(5, len(y_pred) // 20)
                    smoothed_resid = pd.Series(sorted_resid).rolling(window=window, center=True).mean()
                    
                    fig.add_trace(go.Scatter(
                        x=sorted_pred,
                        y=smoothed_resid,
                        mode='lines',
                        name='Smoothed',
                        line=dict(color='red', width=3)
                    ))
            
            # Update layout
            fig.update_layout(
                title=f"Residual Plot: {self.y_variable} ~ {self.x_variable}",
                xaxis_title="Fitted Values",
                yaxis_title="Residuals",
                height=600,
                showlegend=True
            )
            
            # Calculate residual statistics
            residual_stats = {
                'mean': float(residuals.mean()),
                'std': float(residuals.std()),
                'min': float(residuals.min()),
                'max': float(residuals.max())
            }
            
            # Test for heteroscedasticity (Breusch-Pagan test)
            try:
                from statsmodels.stats.diagnostic import het_breuschpagan
                bp_test = het_breuschpagan(residuals, X)
                heteroscedasticity_test = {
                    'statistic': float(bp_test[0]),
                    'p_value': float(bp_test[1]),
                    'significant': bp_test[1] < 0.05
                }
            except ImportError:
                # Simple variance test as fallback
                sorted_indices = np.argsort(y_pred)
                n = len(residuals)
                first_half_var = np.var(residuals[sorted_indices[:n//2]])
                second_half_var = np.var(residuals[sorted_indices[n//2:]])
                f_stat = max(first_half_var, second_half_var) / min(first_half_var, second_half_var)
                
                heteroscedasticity_test = {
                    'test': 'variance_ratio',
                    'statistic': float(f_stat),
                    'p_value': None,
                    'significant': f_stat > 2.0  # Simple threshold
                }
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'residual_plot')
            
            result_data = {
                'x_variable': self.x_variable,
                'y_variable': self.y_variable,
                'residual_stats': residual_stats,
                'heteroscedasticity_test': heteroscedasticity_test,
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'residual_plot'
            }
            
            message = f"Created residual plot for {self.y_variable} ~ {self.x_variable}. "
            message += f"Residual std: {residual_stats['std']:.3f}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating residual plot: {e}")
            return self._create_error_result(f"Residual plot creation failed: {str(e)}")


class CreateBoxPlotGrid(BaseTool):
    """
    Create grid of box plots for multiple variables.
    
    Shows distribution summaries for multiple numeric variables.
    """
    
    variables: List[str] = Field(
        ...,
        description="List of numeric columns to plot",
        min_items=2,
        max_items=12
    )
    
    group_by: Optional[str] = Field(
        None,
        description="Optional categorical column to group by"
    )
    
    cols: int = Field(
        3,
        description="Number of columns in grid",
        ge=1,
        le=4
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create box plot grid of environmental variables",
            "Show distribution of all risk indicators",
            "Compare multiple health metrics by urban/rural"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Create box plot grid visualization."""
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
            
            # Validate variables
            valid_vars = [v for v in self.variables if _validate_numeric_column(df, v)]
            if len(valid_vars) < 2:
                return self._create_error_result("At least 2 valid numeric variables required.")
            
            # Calculate grid dimensions
            n_vars = len(valid_vars)
            rows = (n_vars + self.cols - 1) // self.cols
            
            # Create subplots
            subplot_titles = [v.replace('_', ' ').title() for v in valid_vars]
            fig = make_subplots(
                rows=rows, cols=self.cols,
                subplot_titles=subplot_titles,
                vertical_spacing=0.15,
                horizontal_spacing=0.1
            )
            
            # Add box plots
            for idx, var in enumerate(valid_vars):
                row = idx // self.cols + 1
                col = idx % self.cols + 1
                
                if self.group_by and self.group_by in df.columns:
                    # Grouped box plot
                    for group in df[self.group_by].unique():
                        mask = df[self.group_by] == group
                        fig.add_trace(
                            go.Box(
                                y=df.loc[mask, var],
                                name=str(group),
                                showlegend=(idx == 0),
                                legendgroup=str(group)
                            ),
                            row=row, col=col
                        )
                else:
                    # Single box plot
                    fig.add_trace(
                        go.Box(
                            y=df[var],
                            name=var,
                            showlegend=False
                        ),
                        row=row, col=col
                    )
            
            # Update layout
            title = "Box Plot Grid: " + ", ".join(valid_vars[:3])
            if len(valid_vars) > 3:
                title += f" and {len(valid_vars) - 3} more"
            
            fig.update_layout(
                title=title,
                height=300 * rows,
                showlegend=bool(self.group_by)
            )
            
            # Save chart
            paths = _save_plotly_chart(fig, session_id, 'boxplot_grid')
            
            result_data = {
                'variables': valid_vars,
                'group_by': self.group_by,
                'grid_size': f"{rows}x{self.cols}",
                'web_path': paths['web_path'],
                'file_path': paths['file_path'],
                'chart_type': 'boxplot_grid'
            }
            
            message = f"Created box plot grid with {len(valid_vars)} variables in {rows}x{self.cols} layout"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error creating box plot grid: {e}")
            return self._create_error_result(f"Box plot grid creation failed: {str(e)}")


# 7. GEOGRAPHIC/SPATIAL CHARTS

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
            if not _validate_numeric_column(df, self.size_variable):
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
            paths = _save_plotly_chart(fig, session_id, 'bubble_map')
            
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
            paths = _save_plotly_chart(fig, session_id, 'coordinate_plot')
            
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