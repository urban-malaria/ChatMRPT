"""Comparative bar-based visualizations."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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
            if not validate_numeric_column(df, self.value):
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
            paths = save_plotly_chart(fig, session_id, 'bar_chart')
            
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
            
            valid_values = [v for v in self.values if validate_numeric_column(df, v)]
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
            paths = save_plotly_chart(fig, session_id, 'grouped_bar_chart')
            
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
            paths = save_plotly_chart(fig, session_id, 'stacked_bar_chart')
            
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


