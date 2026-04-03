"""Categorical distribution visualizations."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
import plotly.express as px

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
            paths = save_plotly_chart(fig, session_id, 'pie_chart')
            
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
            paths = save_plotly_chart(fig, session_id, 'donut_chart')
            
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


