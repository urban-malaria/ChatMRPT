"""Advanced statistical charting utilities."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import numpy as np
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
            if not validate_numeric_column(df, self.variable):
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
            paths = save_plotly_chart(fig, session_id, 'qq_plot')
            
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
            if not validate_numeric_column(df, self.x_variable):
                return self._create_error_result(f"X variable '{self.x_variable}' not found or not numeric.")
            if not validate_numeric_column(df, self.y_variable):
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
            paths = save_plotly_chart(fig, session_id, 'residual_plot')
            
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
            valid_vars = [v for v in self.variables if validate_numeric_column(df, v)]
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
            paths = save_plotly_chart(fig, session_id, 'boxplot_grid')
            
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


