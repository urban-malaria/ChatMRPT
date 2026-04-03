"""Correlation and relationship visualizations."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import scipy.stats as stats
from sklearn.linear_model import LinearRegression

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
            if not validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not validate_numeric_column(df, self.y_variable):
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
            paths = save_plotly_chart(fig, session_id, 'scatter_plot')
            
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
            paths = save_plotly_chart(fig, session_id, 'correlation_heatmap')
            
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
            valid_vars = [v for v in self.variables if validate_numeric_column(df, v)]
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
            paths = save_plotly_chart(fig, session_id, 'pair_plot')
            
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
            if not validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not validate_numeric_column(df, self.y_variable):
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
            paths = save_plotly_chart(fig, session_id, 'regression_plot')
            
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


