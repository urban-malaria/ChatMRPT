import logging
import numpy as np
import pandas as pd
from typing import Dict, Optional, Any

# Set up logging
from app.services.variable_resolution_service import variable_resolver
logger = logging.getLogger(__name__)


def is_numeric_column(df, column_name):
    """
    Check if a column is numeric
    
    Args:
        df: DataFrame to check
        column_name: Name of the column to check
        
    Returns:
        bool: True if column is numeric, False otherwise
    """
    if column_name not in df.columns:
        return False
    return pd.api.types.is_numeric_dtype(df[column_name])


def get_column_stats(df, column_name):
    """
    Get statistical summary of a column
    
    Args:
        df: DataFrame containing the column
        column_name: Name of the column to analyze
        
    Returns:
        dict: Statistical summary or None if column doesn't exist
    """
    if column_name not in df.columns:
        return None
    
    try:
        if is_numeric_column(df, column_name):
            values = df[column_name].values
            non_null_values = values[~pd.isna(values)]
            
            if len(non_null_values) == 0:
                return {
                    'min': None,
                    'max': None,
                    'mean': None,
                    'median': None,
                    'std': None,
                    'missing': len(values),
                    'total': len(values),
                    'type': 'numeric_empty'
                }
            
            stats = {
                'min': float(np.min(non_null_values)),
                'max': float(np.max(non_null_values)),
                'mean': float(np.mean(non_null_values)),
                'median': float(np.median(non_null_values)),
                'std': float(np.std(non_null_values)),
                'missing': int(df[column_name].isna().sum()),
                'total': len(values),
                'type': 'numeric'
            }
        else:
            values = df[column_name].values
            unique_values = df[column_name].dropna().unique()
            value_counts = df[column_name].value_counts()
            
            stats = {
                'unique_count': len(unique_values),
                'most_common': value_counts.index[0] if len(value_counts) > 0 else None,
                'most_common_count': value_counts.iloc[0] if len(value_counts) > 0 else 0,
                'missing': int(df[column_name].isna().sum()),
                'total': len(values),
                'type': 'categorical'
            }
        
        return stats
        
    except Exception as e:
        logger.error("Error calculating stats for column {}: {}".format(column_name, str(e)))
        return {
            'error': str(e),
            'missing': int(df[column_name].isna().sum()) if column_name in df.columns else None,
            'total': len(df) if column_name in df.columns else None,
            'type': 'error'
        }


def check_data_quality(data, metadata=None):
    """
    Check data quality and identify potential issues
    
    Args:
        data: DataFrame to check
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        dict: Data quality assessment results
    """
    try:
        step_id = None
        if metadata:
            step_id = metadata.record_step(
                'data_quality_check',
                input_data_summary={'rows': len(data), 'columns': len(data.columns)},
                algorithm='data_quality_assessment'
            )
        
        quality_issues = []
        column_quality = {}
        
        for col in data.columns:
            col_stats = get_column_stats(data, col)
            column_quality[col] = col_stats
            
            if col_stats and col_stats.get('type') == 'numeric':
                # Check for high missing values
                missing_pct = (col_stats['missing'] / col_stats['total']) * 100
                if missing_pct > 50:
                    issue = {
                        'type': 'high_missing_values',
                        'column': col,
                        'severity': 'high',
                        'description': f'Column {col} has {missing_pct:.1f}% missing values'
                    }
                    quality_issues.append(issue)
                    
                    if metadata:
                        metadata.record_anomaly(
                            col, 'high_missing_values',
                            expected_value='<50%',
                            actual_value=f'{missing_pct:.1f}%',
                            significance='high'
                        )
                
                # Check for zero variance
                if col_stats['std'] == 0:
                    issue = {
                        'type': 'zero_variance',
                        'column': col,
                        'severity': 'medium',
                        'description': f'Column {col} has zero variance (all values are the same)'
                    }
                    quality_issues.append(issue)
                    
                    if metadata:
                        metadata.record_anomaly(
                            col, 'zero_variance',
                            expected_value='>0',
                            actual_value='0',
                            significance='medium'
                        )
                
                # Check for extreme outliers (values beyond 3 standard deviations)
                if col_stats['std'] > 0:
                    z_scores = np.abs((data[col] - col_stats['mean']) / col_stats['std'])
                    extreme_outliers = (z_scores > 3).sum()
                    if extreme_outliers > 0:
                        outlier_pct = (extreme_outliers / col_stats['total']) * 100
                        if outlier_pct > 5:  # More than 5% extreme outliers
                            issue = {
                                'type': 'extreme_outliers',
                                'column': col,
                                'severity': 'medium',
                                'description': f'Column {col} has {extreme_outliers} extreme outliers ({outlier_pct:.1f}%)'
                            }
                            quality_issues.append(issue)
                            
                            if metadata:
                                metadata.record_anomaly(
                                    col, 'extreme_outliers',
                                    expected_value='<5%',
                                    actual_value=f'{outlier_pct:.1f}%',
                                    significance='medium'
                                )
        
        # Overall assessment
        total_issues = len(quality_issues)
        high_severity_issues = len([issue for issue in quality_issues if issue['severity'] == 'high'])
        
        if high_severity_issues > 0:
            overall_quality = 'poor'
        elif total_issues > len(data.columns) * 0.3:  # More than 30% of columns have issues
            overall_quality = 'fair'
        elif total_issues > 0:
            overall_quality = 'good'
        else:
            overall_quality = 'excellent'
        
        result = {
            'overall_quality': overall_quality,
            'total_issues': total_issues,
            'high_severity_issues': high_severity_issues,
            'issues': quality_issues,
            'column_quality': column_quality,
            'summary': {
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'numeric_columns': len([col for col in data.columns if is_numeric_column(data, col)]),
                'categorical_columns': len([col for col in data.columns if not is_numeric_column(data, col)])
            }
        }
        
        if metadata and step_id:
            metadata.record_step(
                'data_quality_assessment_complete',
                output_data_summary=result['summary'],
                algorithm='comprehensive_quality_check'
            )
        
        return result
        
    except Exception as e:
        logger.error("Error in data quality check: {}".format(str(e)))
        return {
            'overall_quality': 'error',
            'error': str(e),
            'total_issues': 0,
            'issues': [],
            'column_quality': {},
            'summary': {}
        } 