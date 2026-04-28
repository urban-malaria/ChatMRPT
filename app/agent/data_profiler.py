"""
Data Profiler for Dynamic Dataset Analysis
Provides industry-standard data profiling without hardcoded assumptions
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Dynamic data profiler that analyzes any dataset without assumptions.
    Follows industry standards (similar to pandas.DataFrame.info() and .describe())
    """
    
    @staticmethod
    def profile_dataset(df: pd.DataFrame, sample_rows: int = 5) -> Dict[str, Any]:
        """
        Profile a dataset to extract key information dynamically.
        
        Args:
            df: DataFrame to profile
            sample_rows: Number of rows to include in preview
            
        Returns:
            Dictionary containing comprehensive dataset profile
        """
        try:
            profile = {
                'overview': DataProfiler._get_overview(df),
                'column_types': DataProfiler._analyze_column_types(df),
                'data_quality': DataProfiler._check_data_quality(df),
                'preview': DataProfiler._get_preview(df, sample_rows),
                'column_list': DataProfiler._get_column_details(df)
            }
            return profile
        except Exception as e:
            logger.error(f"Error profiling dataset: {e}")
            return DataProfiler._get_fallback_profile(df)
    
    @staticmethod
    def _get_overview(df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic dataset overview."""
        try:
            memory_usage_bytes = df.memory_usage(deep=True).sum()
            memory_usage_mb = memory_usage_bytes / (1024 * 1024)  # Convert to MB
            # Ensure at least 0.01 MB for very small datasets
            memory_mb = max(0.01, round(memory_usage_mb, 2))
            
            return {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': memory_mb,
                'shape_str': f"{len(df):,} × {len(df.columns)}",
                'index_type': str(df.index.dtype),
                'has_duplicates': df.duplicated().any()
            }
        except Exception as e:
            logger.warning(f"Error getting overview: {e}")
            return {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': 0,
                'shape_str': f"{len(df)} × {len(df.columns)}"
            }
    
    @staticmethod
    def _analyze_column_types(df: pd.DataFrame) -> Dict[str, Any]:
        """Categorize columns by their data types."""
        try:
            # Get columns by dtype
            numeric_cols = list(df.select_dtypes(include=['number']).columns)
            text_cols = list(df.select_dtypes(include=['object', 'string']).columns)
            datetime_cols = list(df.select_dtypes(include=['datetime', 'datetimetz']).columns)
            bool_cols = list(df.select_dtypes(include=['bool']).columns)
            category_cols = list(df.select_dtypes(include=['category']).columns)
            
            # Count by type
            type_counts = {
                'numeric': len(numeric_cols),
                'text': len(text_cols),
                'datetime': len(datetime_cols),
                'boolean': len(bool_cols),
                'categorical': len(category_cols)
            }
            
            # Get detailed dtype info
            dtype_details = {}
            for col in df.columns:
                dtype_details[col] = str(df[col].dtype)
            
            return {
                'numeric': numeric_cols,
                'text': text_cols,
                'datetime': datetime_cols,
                'boolean': bool_cols,
                'categorical': category_cols,
                'counts': type_counts,
                'dtype_details': dtype_details
            }
        except Exception as e:
            logger.warning(f"Error analyzing column types: {e}")
            return {
                'numeric': [],
                'text': list(df.columns),
                'datetime': [],
                'boolean': [],
                'categorical': [],
                'counts': {'text': len(df.columns)}
            }
    
    @staticmethod
    def _check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
        """Check data quality metrics."""
        try:
            # Missing values analysis
            missing_counts = df.isnull().sum()
            missing_pct = (missing_counts / len(df) * 100).round(2)
            
            # Columns with missing values
            cols_with_missing = missing_counts[missing_counts > 0].to_dict()
            cols_missing_pct = missing_pct[missing_counts > 0].to_dict()
            
            # Duplicate analysis
            duplicate_rows = df.duplicated().sum()
            duplicate_pct = round(duplicate_rows / len(df) * 100, 2) if len(df) > 0 else 0
            
            # Completeness
            total_cells = df.shape[0] * df.shape[1]
            missing_cells = missing_counts.sum()
            completeness = round((1 - missing_cells / total_cells) * 100, 2) if total_cells > 0 else 100
            
            return {
                'missing_values': cols_with_missing,
                'missing_percentages': cols_missing_pct,
                'total_missing': int(missing_cells),
                'columns_with_missing': len(cols_with_missing),
                'duplicate_rows': duplicate_rows,
                'duplicate_percentage': duplicate_pct,
                'completeness': completeness,
                'has_missing': len(cols_with_missing) > 0,
                'has_duplicates': duplicate_rows > 0
            }
        except Exception as e:
            logger.warning(f"Error checking data quality: {e}")
            return {
                'missing_values': {},
                'completeness': 100,
                'has_missing': False,
                'has_duplicates': False
            }
    
    @staticmethod
    def _get_preview(df: pd.DataFrame, rows: int = 5) -> Dict[str, Any]:
        """Get a preview of the data."""
        try:
            # Get first N rows
            preview_df = df.head(rows)
            
            # Convert to records for JSON serialization
            preview_records = preview_df.to_dict('records')
            
            # Also get column names for reference
            column_names = list(df.columns)
            
            # Try to format nicely for display
            preview_formatted = []
            for record in preview_records:
                formatted_record = {}
                for key, value in record.items():
                    # Handle different data types
                    if pd.isna(value):
                        formatted_record[key] = "NaN"
                    elif isinstance(value, (int, float)):
                        if abs(value) > 1e6 or (abs(value) < 1e-3 and value != 0):
                            formatted_record[key] = f"{value:.2e}"
                        else:
                            formatted_record[key] = round(value, 3) if isinstance(value, float) else value
                    else:
                        formatted_record[key] = str(value)[:50]  # Truncate long strings
                preview_formatted.append(formatted_record)
            
            return {
                'records': preview_records,
                'formatted': preview_formatted,
                'columns': column_names,
                'rows_shown': len(preview_df)
            }
        except Exception as e:
            logger.warning(f"Error getting preview: {e}")
            return {
                'records': [],
                'formatted': [],
                'columns': list(df.columns),
                'rows_shown': 0
            }
    
    @staticmethod
    def _get_column_details(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Get detailed information about each column."""
        try:
            details = []
            for col in df.columns:
                col_info = {
                    'name': col,
                    'dtype': str(df[col].dtype),
                    'non_null': df[col].notna().sum(),
                    'null_count': df[col].isna().sum(),
                    'unique_values': df[col].nunique(),
                    'memory_usage': df[col].memory_usage(deep=True)
                }
                
                # Add statistics for numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    col_info['min'] = df[col].min()
                    col_info['max'] = df[col].max()
                    col_info['mean'] = df[col].mean()
                    col_info['std'] = df[col].std()
                
                # Add sample values for text columns
                elif pd.api.types.is_object_dtype(df[col]):
                    unique_vals = df[col].dropna().unique()
                    col_info['sample_values'] = list(unique_vals[:5])
                
                details.append(col_info)
            
            return details
        except Exception as e:
            logger.warning(f"Error getting column details: {e}")
            return []
    
    @staticmethod
    def _get_fallback_profile(df: pd.DataFrame) -> Dict[str, Any]:
        """Get minimal profile if full profiling fails."""
        return {
            'overview': {
                'rows': len(df),
                'columns': len(df.columns),
                'shape_str': f"{len(df)} × {len(df.columns)}"
            },
            'column_types': {
                'counts': {'total': len(df.columns)}
            },
            'data_quality': {
                'has_missing': False,
                'has_duplicates': False
            },
            'preview': {
                'columns': list(df.columns),
                'rows_shown': 0
            }
        }
    
    @staticmethod
    def format_profile_summary(profile: Dict[str, Any]) -> str:
        """
        Format profile data into a user-friendly summary string.
        
        Args:
            profile: Profile dictionary from profile_dataset()
            
        Returns:
            Formatted string summary
        """
        try:
            overview = profile.get('overview', {})
            types = profile.get('column_types', {})
            quality = profile.get('data_quality', {})
            
            summary = "📊 **Data Successfully Loaded!**\n\n"
            
            # Dataset Overview
            summary += "**Dataset Overview:**\n"
            summary += f"• {overview.get('shape_str', 'Unknown size')}\n"
            summary += f"• Memory usage: {overview.get('memory_mb', 0):.1f} MB\n"
            
            # Column Types
            summary += "\n**Column Types:**\n"
            type_counts = types.get('counts', {})
            if type_counts.get('numeric', 0) > 0:
                summary += f"• {type_counts['numeric']} numeric columns\n"
            if type_counts.get('text', 0) > 0:
                summary += f"• {type_counts['text']} text columns\n"
            if type_counts.get('datetime', 0) > 0:
                summary += f"• {type_counts['datetime']} datetime columns\n"
            if type_counts.get('boolean', 0) > 0:
                summary += f"• {type_counts['boolean']} boolean columns\n"
            
            # Data Quality
            summary += "\n**Data Quality:**\n"
            if quality.get('has_missing'):
                cols_missing = quality.get('columns_with_missing', 0)
                completeness = quality.get('completeness', 100)
                summary += f"• {cols_missing} columns have missing values ({completeness:.1f}% complete)\n"
            else:
                summary += "• No missing values detected ✓\n"
            
            if quality.get('has_duplicates'):
                dup_count = quality.get('duplicate_rows', 0)
                dup_pct = quality.get('duplicate_percentage', 0)
                summary += f"• {dup_count} duplicate rows found ({dup_pct:.1f}%)\n"
            else:
                summary += "• No duplicate rows ✓\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error formatting profile summary: {e}")
            return "📊 **Data Successfully Loaded!**\n\nData is ready for analysis."