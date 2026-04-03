"""
Data Description Tools for ChatMRPT
====================================

Tools to describe uploaded data, including column information, data types, and basic statistics.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, ClassVar, List
from pydantic import BaseModel, Field

from .base import BaseTool, ToolCategory, ToolExecutionResult

logger = logging.getLogger(__name__)


class DescribeDataInput(BaseModel):
    """Input for data description"""
    session_id: str = Field(..., description="Session identifier")
    include_sample: bool = Field(True, description="Whether to include sample values")
    include_statistics: bool = Field(True, description="Whether to include summary statistics")


class DescribeData(BaseTool):
    """
    Describe uploaded data including columns, data types, and basic statistics.

    Provides comprehensive information about:
    - Column names and data types
    - Row and column counts
    - Missing value counts
    - Basic statistics for numeric columns
    - Sample values from the data
    """

    name: ClassVar[str] = "describe_data"
    description: ClassVar[str] = "Describe uploaded data including column names, types, and statistics"
    category: ClassVar[ToolCategory] = ToolCategory.DATA_ANALYSIS
    input_model: ClassVar[type] = DescribeDataInput

    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute data description"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)

    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute data description"""
        try:
            session_id = kwargs.get('session_id')
            include_sample = kwargs.get('include_sample', True)
            include_statistics = kwargs.get('include_statistics', True)

            # Check for uploaded data
            data_path = Path(f'instance/uploads/{session_id}/raw_data.csv')

            if not data_path.exists():
                # Try alternative paths
                alt_paths = [
                    Path(f'instance/uploads/{session_id}/tpr_data.csv'),
                    Path(f'instance/uploads/{session_id}/processed_data.csv'),
                    Path(f'instance/uploads/{session_id}/data.csv')
                ]

                for alt_path in alt_paths:
                    if alt_path.exists():
                        data_path = alt_path
                        break
                else:
                    return ToolExecutionResult(
                        success=False,
                        message="No data file found. Please upload a CSV file first.",
                        error_details="Data file not found in session folder"
                    )

            # Load the data
            df = pd.read_csv(data_path)

            # Prepare description
            description = {
                'file_info': {
                    'filename': data_path.name,
                    'rows': len(df),
                    'columns': len(df.columns)
                },
                'columns': self._get_column_info(df),
                'data_quality': self._get_data_quality(df),
            }

            if include_statistics:
                description['statistics'] = self._get_statistics(df)

            if include_sample:
                description['sample'] = self._get_sample_data(df)

            # Create readable message
            message = self._format_message(description)

            return ToolExecutionResult(
                success=True,
                message=message,
                data={
                    'description': description,
                    'session_id': session_id
                }
            )

        except Exception as e:
            logger.error(f"Error describing data: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"Failed to describe data: {str(e)}",
                error_details=str(e)
            )

    def _get_column_info(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Get information about each column"""
        columns_info = []

        for col in df.columns:
            col_info = {
                'name': col,
                'type': str(df[col].dtype),
                'non_null_count': df[col].notna().sum(),
                'null_count': df[col].isna().sum(),
                'null_percentage': round(df[col].isna().mean() * 100, 2),
                'unique_values': df[col].nunique()
            }

            # Add specific info based on data type
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info['min'] = df[col].min()
                col_info['max'] = df[col].max()
                col_info['mean'] = round(df[col].mean(), 4) if df[col].notna().any() else None
            elif pd.api.types.is_object_dtype(df[col]):
                # For string columns, show most common values
                top_values = df[col].value_counts().head(3)
                col_info['top_values'] = [{'value': idx, 'count': count}
                                          for idx, count in top_values.items()]

            columns_info.append(col_info)

        return columns_info

    def _get_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get data quality metrics"""
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isna().sum().sum()

        return {
            'total_cells': total_cells,
            'missing_cells': missing_cells,
            'completeness': round((1 - missing_cells/total_cells) * 100, 2),
            'columns_with_missing': df.columns[df.isna().any()].tolist(),
            'duplicate_rows': df.duplicated().sum()
        }

    def _get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for numeric columns"""
        numeric_df = df.select_dtypes(include=[np.number])

        if numeric_df.empty:
            return {'message': 'No numeric columns found for statistics'}

        stats = {}
        for col in numeric_df.columns:
            if numeric_df[col].notna().any():
                stats[col] = {
                    'mean': round(numeric_df[col].mean(), 4),
                    'std': round(numeric_df[col].std(), 4),
                    'min': round(numeric_df[col].min(), 4),
                    '25%': round(numeric_df[col].quantile(0.25), 4),
                    'median': round(numeric_df[col].median(), 4),
                    '75%': round(numeric_df[col].quantile(0.75), 4),
                    'max': round(numeric_df[col].max(), 4)
                }

        return stats

    def _get_sample_data(self, df: pd.DataFrame, n_rows: int = 5) -> Dict[str, Any]:
        """Get sample data from the dataframe"""
        sample = {
            'first_rows': df.head(n_rows).to_dict('records'),
            'random_sample': df.sample(min(n_rows, len(df))).to_dict('records') if len(df) > n_rows else []
        }

        return sample

    def _format_message(self, description: Dict[str, Any]) -> str:
        """Format description into a readable message"""
        lines = []

        # File info
        info = description['file_info']
        lines.append(f"## Data Overview")
        lines.append(f"- **File**: {info['filename']}")
        lines.append(f"- **Rows**: {info['rows']:,}")
        lines.append(f"- **Columns**: {info['columns']:,}")
        lines.append("")

        # Data quality
        quality = description['data_quality']
        lines.append(f"## Data Quality")
        lines.append(f"- **Completeness**: {quality['completeness']}%")
        lines.append(f"- **Missing cells**: {quality['missing_cells']:,} out of {quality['total_cells']:,}")
        lines.append(f"- **Duplicate rows**: {quality['duplicate_rows']}")
        if quality['columns_with_missing']:
            lines.append(f"- **Columns with missing values**: {', '.join(quality['columns_with_missing'][:5])}")
        lines.append("")

        # Column information
        lines.append(f"## Column Information")
        for i, col in enumerate(description['columns'][:15], 1):  # Show first 15 columns
            lines.append(f"\n**{i}. {col['name']}**")
            lines.append(f"   - Type: {col['type']}")
            lines.append(f"   - Non-null: {col['non_null_count']:,} ({100 - col['null_percentage']:.1f}%)")
            lines.append(f"   - Unique values: {col['unique_values']}")

            if 'mean' in col:
                lines.append(f"   - Range: [{col.get('min', 'N/A'):.2f}, {col.get('max', 'N/A'):.2f}]")
                if col.get('mean') is not None:
                    lines.append(f"   - Mean: {col['mean']:.2f}")
            elif 'top_values' in col:
                top_vals = ", ".join([f"{v['value']} ({v['count']})"
                                     for v in col['top_values'][:3]])
                lines.append(f"   - Most common: {top_vals}")

        if len(description['columns']) > 15:
            lines.append(f"\n... and {len(description['columns']) - 15} more columns")

        # Statistics summary
        if 'statistics' in description and isinstance(description['statistics'], dict):
            lines.append(f"\n## Key Statistics")
            for col_name, stats in list(description['statistics'].items())[:5]:
                lines.append(f"- **{col_name}**: mean={stats['mean']:.2f}, std={stats['std']:.2f}, median={stats['median']:.2f}")

        return "\n".join(lines)


# Export the tool
__all__ = ['DescribeData']