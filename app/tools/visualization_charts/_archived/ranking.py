"""Ranking and performance chart tools."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, List

from pydantic import Field

import pandas as pd
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
            if not validate_numeric_column(df, self.value):
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
            paths = save_plotly_chart(fig, session_id, 'lollipop_chart')
            
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


