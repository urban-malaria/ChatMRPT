"""
Statistical Analysis Tools for ChatMRPT - Unified Dataset Compatible

These tools provide comprehensive statistical analysis capabilities
that work with the unified dataset structure and smart metadata.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from flask import current_app
import pandas as pd
import numpy as np
from scipy import stats
import warnings

logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=RuntimeWarning)


def _get_unified_dataset(session_id: str) -> Optional[pd.DataFrame]:
    """Get the unified dataset for statistical analysis."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            return unified_gdf
        
        # Fallback to DataHandler
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id)
        
        if data_handler and hasattr(data_handler, 'df') and data_handler.df is not None:
            return data_handler.df
        
        return None
        
    except Exception as e:
        logger.error(f"Error accessing dataset for statistics: {e}")
        return None


def _get_numeric_columns(df: pd.DataFrame, exclude_ids: bool = True) -> List[str]:
    """Get numeric columns suitable for statistical analysis."""
    numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
    
    if exclude_ids:
        # Remove ID-like columns
        numeric_cols = [col for col in numeric_cols 
                       if not any(term in col.lower() for term in ['id', 'index', '_id', 'fid'])]
    
    return numeric_cols


def _get_categorical_columns(df: pd.DataFrame, exclude_ids: bool = True) -> List[str]:
    """Get categorical columns suitable for statistical analysis."""
    categorical_cols = list(df.select_dtypes(include=['object', 'category']).columns)
    
    if exclude_ids:
        # Keep only meaningful categorical variables
        categorical_cols = [col for col in categorical_cols 
                           if df[col].nunique() < len(df) * 0.8]  # Not too many unique values
    
    return categorical_cols


def summary_stats(session_id: str, variable: str = None, category: str = None) -> Dict[str, Any]:
    """
    Generate comprehensive summary statistics.
    
    Args:
        session_id: Session identifier
        variable: Specific variable to analyze (optional)
        category: Variable category to analyze ('health', 'environmental', etc.) (optional)
        
    Returns:
        Dict with summary statistics
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable and variable in df.columns:
            # Single variable summary
            if df[variable].dtype in ['object', 'category']:
                # Categorical variable
                summary = {
                    'variable': variable,
                    'type': 'categorical',
                    'count': len(df[variable]),
                    'unique_values': df[variable].nunique(),
                    'missing_count': df[variable].isnull().sum(),
                    'missing_percent': (df[variable].isnull().sum() / len(df)) * 100,
                    'value_counts': df[variable].value_counts().head(10).to_dict(),
                    'mode': df[variable].mode().iloc[0] if len(df[variable].mode()) > 0 else None
                }
            else:
                # Numeric variable
                desc = df[variable].describe()
                summary = {
                    'variable': variable,
                    'type': 'numeric',
                    'count': desc['count'],
                    'mean': desc['mean'],
                    'std': desc['std'],
                    'min': desc['min'],
                    'q25': desc['25%'],
                    'median': desc['50%'],
                    'q75': desc['75%'],
                    'max': desc['max'],
                    'missing_count': df[variable].isnull().sum(),
                    'missing_percent': (df[variable].isnull().sum() / len(df)) * 100,
                    'skewness': stats.skew(df[variable].dropna()),
                    'kurtosis': stats.kurtosis(df[variable].dropna())
                }
            
            return {
                'status': 'success',
                'message': f'Summary statistics for {variable}',
                'summary': summary
            }
        
        elif category:
            # Category-based summary
            from ..data.unified_dataset_builder import get_columns_by_category
            category_columns = get_columns_by_category(session_id, category)
            
            if not category_columns:
                return {
                    'status': 'error',
                    'message': f'No columns found in category: {category}'
                }
            
            # Filter to columns that exist in dataset
            available_columns = [col for col in category_columns if col in df.columns]
            numeric_columns = [col for col in available_columns if df[col].dtype in [np.number]]
            
            if not numeric_columns:
                return {
                    'status': 'error',
                    'message': f'No numeric columns found in category: {category}'
                }
            
            # Summary for numeric columns in category
            category_summary = df[numeric_columns].describe().to_dict()
            
            return {
                'status': 'success',
                'message': f'Summary statistics for {category} category',
                'category': category,
                'columns_analyzed': numeric_columns,
                'summary': category_summary
            }
        
        else:
            # Overall dataset summary
            numeric_cols = _get_numeric_columns(df)[:10]  # Limit to first 10 for overview
            
            if not numeric_cols:
                return {
                    'status': 'error',
                    'message': 'No numeric variables found for summary statistics'
                }
            
            overall_summary = df[numeric_cols].describe().to_dict()
            
            return {
                'status': 'success',
                'message': 'Overall dataset summary statistics',
                'columns_analyzed': numeric_cols,
                'summary': overall_summary,
                'total_columns': len(df.columns),
                'total_rows': len(df)
            }
        
    except Exception as e:
        logger.error(f"Error generating summary statistics: {e}")
        return {
            'status': 'error',
            'message': f'Error generating summary statistics: {str(e)}'
        }


def correlation(session_id: str, variables: List[str] = None, method: str = 'pearson') -> Dict[str, Any]:
    """
    Calculate correlation matrix for variables.
    
    Args:
        session_id: Session identifier
        variables: List of variables to correlate (optional)
        method: Correlation method ('pearson', 'spearman', 'kendall')
        
    Returns:
        Dict with correlation results
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variables:
            # Use specified variables
            available_vars = [var for var in variables if var in df.columns]
            if not available_vars:
                return {
                    'status': 'error',
                    'message': 'None of the specified variables found in dataset'
                }
        else:
            # Use all numeric variables
            available_vars = _get_numeric_columns(df)[:15]  # Limit for performance
        
        if len(available_vars) < 2:
            return {
                'status': 'error',
                'message': 'Need at least 2 numeric variables for correlation analysis'
            }
        
        # Calculate correlation matrix
        corr_matrix = df[available_vars].corr(method=method)
        
        # Find strongest correlations
        corr_pairs = []
        for i in range(len(available_vars)):
            for j in range(i+1, len(available_vars)):
                var1, var2 = available_vars[i], available_vars[j]
                corr_value = corr_matrix.loc[var1, var2]
                if not np.isnan(corr_value):
                    corr_pairs.append({
                        'variable1': var1,
                        'variable2': var2,
                        'correlation': corr_value,
                        'strength': 'strong' if abs(corr_value) > 0.7 else ('moderate' if abs(corr_value) > 0.3 else 'weak')
                    })
        
        # Sort by absolute correlation value
        corr_pairs.sort(key=lambda x: abs(x['correlation']), reverse=True)
        
        return {
            'status': 'success',
            'message': f'Correlation analysis completed using {method} method',
            'method': method,
            'variables_analyzed': available_vars,
            'correlation_matrix': corr_matrix.to_dict(),
            'strongest_correlations': corr_pairs[:10],  # Top 10
            'total_pairs': len(corr_pairs)
        }
        
    except Exception as e:
        logger.error(f"Error calculating correlation: {e}")
        return {
            'status': 'error',
            'message': f'Error calculating correlation: {str(e)}'
        }


def chi_square(session_id: str, variable1: str, variable2: str) -> Dict[str, Any]:
    """
    Perform chi-square test of independence.
    
    Args:
        session_id: Session identifier
        variable1: First categorical variable
        variable2: Second categorical variable
        
    Returns:
        Dict with chi-square test results
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable1 not in df.columns or variable2 not in df.columns:
            return {
                'status': 'error',
                'message': 'One or both variables not found in dataset'
            }
        
        # Create contingency table
        contingency_table = pd.crosstab(df[variable1], df[variable2])
        
        # Perform chi-square test
        chi2_stat, p_value, dof, expected = stats.chi2_contingency(contingency_table)
        
        # Effect size (Cramér's V)
        n = contingency_table.sum().sum()
        cramers_v = np.sqrt(chi2_stat / (n * (min(contingency_table.shape) - 1)))
        
        return {
            'status': 'success',
            'message': f'Chi-square test of independence between {variable1} and {variable2}',
            'variable1': variable1,
            'variable2': variable2,
            'chi2_statistic': chi2_stat,
            'p_value': p_value,
            'degrees_of_freedom': dof,
            'cramers_v': cramers_v,
            'significant': p_value < 0.05,
            'contingency_table': contingency_table.to_dict(),
            'interpretation': 'Variables are significantly associated' if p_value < 0.05 else 'No significant association found'
        }
        
    except Exception as e:
        logger.error(f"Error performing chi-square test: {e}")
        return {
            'status': 'error',
            'message': f'Error performing chi-square test: {str(e)}'
        }


def t_test(session_id: str, variable: str, group_variable: str, group1: str = None, group2: str = None) -> Dict[str, Any]:
    """
    Perform independent samples t-test.
    
    Args:
        session_id: Session identifier
        variable: Numeric variable to test
        group_variable: Categorical variable defining groups
        group1: First group value (optional - will use first two groups if not specified)
        group2: Second group value (optional)
        
    Returns:
        Dict with t-test results
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns or group_variable not in df.columns:
            return {
                'status': 'error',
                'message': 'Variable or group variable not found in dataset'
            }
        
        if df[variable].dtype not in [np.number]:
            return {
                'status': 'error',
                'message': f'{variable} is not a numeric variable'
            }
        
        # Determine groups
        unique_groups = df[group_variable].unique()
        if group1 is None or group2 is None:
            if len(unique_groups) < 2:
                return {
                    'status': 'error',
                    'message': f'Group variable {group_variable} has fewer than 2 groups'
                }
            group1 = unique_groups[0]
            group2 = unique_groups[1]
        
        # Extract data for each group
        data1 = df[df[group_variable] == group1][variable].dropna()
        data2 = df[df[group_variable] == group2][variable].dropna()
        
        if len(data1) < 2 or len(data2) < 2:
            return {
                'status': 'error',
                'message': 'Insufficient data in one or both groups (need at least 2 observations each)'
            }
        
        # Perform t-test
        t_stat, p_value = stats.ttest_ind(data1, data2)
        
        # Effect size (Cohen's d)
        pooled_std = np.sqrt(((len(data1) - 1) * np.var(data1, ddof=1) + (len(data2) - 1) * np.var(data2, ddof=1)) / (len(data1) + len(data2) - 2))
        cohens_d = (np.mean(data1) - np.mean(data2)) / pooled_std
        
        return {
            'status': 'success',
            'message': f'Independent samples t-test for {variable} by {group_variable}',
            'variable': variable,
            'group_variable': group_variable,
            'group1': group1,
            'group2': group2,
            'group1_stats': {
                'n': len(data1),
                'mean': np.mean(data1),
                'std': np.std(data1, ddof=1)
            },
            'group2_stats': {
                'n': len(data2),
                'mean': np.mean(data2),
                'std': np.std(data2, ddof=1)
            },
            't_statistic': t_stat,
            'p_value': p_value,
            'cohens_d': cohens_d,
            'significant': p_value < 0.05,
            'interpretation': f'Significant difference between groups' if p_value < 0.05 else 'No significant difference between groups'
        }
        
    except Exception as e:
        logger.error(f"Error performing t-test: {e}")
        return {
            'status': 'error',
            'message': f'Error performing t-test: {str(e)}'
        }


def anova(session_id: str, variable: str, group_variable: str) -> Dict[str, Any]:
    """
    Perform one-way ANOVA.
    
    Args:
        session_id: Session identifier
        variable: Numeric variable to test
        group_variable: Categorical variable defining groups
        
    Returns:
        Dict with ANOVA results
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns or group_variable not in df.columns:
            return {
                'status': 'error',
                'message': 'Variable or group variable not found in dataset'
            }
        
        if df[variable].dtype not in [np.number]:
            return {
                'status': 'error',
                'message': f'{variable} is not a numeric variable'
            }
        
        # Get groups
        groups = []
        group_names = []
        for group_name in df[group_variable].unique():
            if pd.notna(group_name):
                group_data = df[df[group_variable] == group_name][variable].dropna()
                if len(group_data) > 0:
                    groups.append(group_data)
                    group_names.append(group_name)
        
        if len(groups) < 2:
            return {
                'status': 'error',
                'message': 'Need at least 2 groups for ANOVA'
            }
        
        # Perform ANOVA
        f_stat, p_value = stats.f_oneway(*groups)
        
        # Group statistics
        group_stats = {}
        for i, group_name in enumerate(group_names):
            group_stats[str(group_name)] = {
                'n': len(groups[i]),
                'mean': np.mean(groups[i]),
                'std': np.std(groups[i], ddof=1)
            }
        
        return {
            'status': 'success',
            'message': f'One-way ANOVA for {variable} by {group_variable}',
            'variable': variable,
            'group_variable': group_variable,
            'f_statistic': f_stat,
            'p_value': p_value,
            'num_groups': len(groups),
            'group_statistics': group_stats,
            'significant': p_value < 0.05,
            'interpretation': f'Significant differences between groups' if p_value < 0.05 else 'No significant differences between groups'
        }
        
    except Exception as e:
        logger.error(f"Error performing ANOVA: {e}")
        return {
            'status': 'error',
            'message': f'Error performing ANOVA: {str(e)}'
        }


def distribution_test(session_id: str, variable: str, test_type: str = 'shapiro') -> Dict[str, Any]:
    """
    Test if variable follows a specific distribution.
    
    Args:
        session_id: Session identifier
        variable: Variable to test
        test_type: Type of test ('shapiro', 'normaltest', 'kstest')
        
    Returns:
        Dict with distribution test results
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns:
            return {
                'status': 'error',
                'message': f'Variable {variable} not found in dataset'
            }
        
        if df[variable].dtype not in [np.number]:
            return {
                'status': 'error',
                'message': f'{variable} is not a numeric variable'
            }
        
        data = df[variable].dropna()
        
        if len(data) < 3:
            return {
                'status': 'error',
                'message': 'Insufficient data for distribution test (need at least 3 observations)'
            }
        
        if test_type == 'shapiro':
            if len(data) > 5000:
                # Shapiro-Wilk test is not suitable for large samples
                return {
                    'status': 'error',
                    'message': 'Shapiro-Wilk test not suitable for samples > 5000. Use normaltest instead.'
                }
            statistic, p_value = stats.shapiro(data)
            test_name = 'Shapiro-Wilk test for normality'
            
        elif test_type == 'normaltest':
            statistic, p_value = stats.normaltest(data)
            test_name = "D'Agostino and Pearson's normality test"
            
        elif test_type == 'kstest':
            statistic, p_value = stats.kstest(data, 'norm', args=(np.mean(data), np.std(data)))
            test_name = 'Kolmogorov-Smirnov test for normality'
            
        else:
            return {
                'status': 'error',
                'message': f'Unknown test type: {test_type}. Use shapiro, normaltest, or kstest.'
            }
        
        return {
            'status': 'success',
            'message': f'{test_name} for {variable}',
            'variable': variable,
            'test_type': test_type,
            'test_name': test_name,
            'statistic': statistic,
            'p_value': p_value,
            'sample_size': len(data),
            'normal_distribution': p_value > 0.05,
            'interpretation': f'Data appears normally distributed (p > 0.05)' if p_value > 0.05 else 'Data does not appear normally distributed (p ≤ 0.05)'
        }
        
    except Exception as e:
        logger.error(f"Error performing distribution test: {e}")
        return {
            'status': 'error',
            'message': f'Error performing distribution test: {str(e)}'
        }


def group_summary(session_id: str, variable: str, group_variable: str) -> Dict[str, Any]:
    """
    Generate summary statistics by groups.
    
    Args:
        session_id: Session identifier
        variable: Variable to summarize
        group_variable: Variable defining groups
        
    Returns:
        Dict with grouped summary statistics
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns or group_variable not in df.columns:
            return {
                'status': 'error',
                'message': 'Variable or group variable not found in dataset'
            }
        
        if df[variable].dtype in ['object', 'category']:
            # Categorical variable - use value counts by group
            group_summary = df.groupby(group_variable)[variable].apply(lambda x: x.value_counts().to_dict()).to_dict()
            
            return {
                'status': 'success',
                'message': f'Group summary for categorical variable {variable} by {group_variable}',
                'variable': variable,
                'group_variable': group_variable,
                'variable_type': 'categorical',
                'group_summary': group_summary
            }
        
        else:
            # Numeric variable - use descriptive statistics by group
            grouped = df.groupby(group_variable)[variable]
            
            group_stats = {}
            for group_name, group_data in grouped:
                group_stats[str(group_name)] = {
                    'count': len(group_data),
                    'mean': np.mean(group_data),
                    'std': np.std(group_data, ddof=1),
                    'min': np.min(group_data),
                    'q25': np.percentile(group_data, 25),
                    'median': np.median(group_data),
                    'q75': np.percentile(group_data, 75),
                    'max': np.max(group_data),
                    'missing': group_data.isnull().sum()
                }
            
            return {
                'status': 'success',
                'message': f'Group summary for numeric variable {variable} by {group_variable}',
                'variable': variable,
                'group_variable': group_variable,
                'variable_type': 'numeric',
                'group_statistics': group_stats,
                'total_groups': len(group_stats)
            }
        
    except Exception as e:
        logger.error(f"Error generating group summary: {e}")
        return {
            'status': 'error',
            'message': f'Error generating group summary: {str(e)}'
        }


def descriptive_stats(session_id: str, variables: List[str] = None) -> Dict[str, Any]:
    """
    Calculate descriptive statistics for specified variables.
    
    Args:
        session_id: Session identifier
        variables: List of variable names to analyze (if None, analyze all numeric)
        
    Returns:
        Dict with descriptive statistics
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available'
            }
        
        # If no variables specified, use all numeric columns
        if variables is None:
            variables = _get_numeric_columns(df)
        else:
            # Filter to only existing numeric columns
            available_numeric = _get_numeric_columns(df)
            variables = [var for var in variables if var in available_numeric]
        
        if not variables:
            return {
                'status': 'error',
                'message': 'No valid numeric variables found for analysis'
            }
        
        # Calculate descriptive statistics
        stats_dict = {}
        for var in variables:
            if var in df.columns:
                series = df[var].dropna()
                stats_dict[var] = {
                    'count': len(series),
                    'mean': round(series.mean(), 4),
                    'median': round(series.median(), 4),
                    'std': round(series.std(), 4),
                    'min': round(series.min(), 4),
                    'max': round(series.max(), 4),
                    'q25': round(series.quantile(0.25), 4),
                    'q75': round(series.quantile(0.75), 4),
                    'missing_count': df[var].isnull().sum(),
                    'missing_percent': round((df[var].isnull().sum() / len(df)) * 100, 2)
                }
        
        return {
            'status': 'success',
            'message': f'Descriptive statistics calculated for {len(stats_dict)} variables',
            'variables_analyzed': list(stats_dict.keys()),
            'statistics': stats_dict,
            'total_records': len(df)
        }
        
    except Exception as e:
        logger.error(f"Error calculating descriptive statistics: {e}")
        return {
            'status': 'error',
            'message': f'Error calculating statistics: {str(e)}'
        }


def regression_analysis(session_id: str, dependent_var: str, independent_vars: List[str]) -> Dict[str, Any]:
    """
    Perform linear regression analysis.
    
    Args:
        session_id: Session identifier
        dependent_var: Dependent variable name
        independent_vars: List of independent variable names
        
    Returns:
        Dict with regression results
    """
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import r2_score, mean_squared_error
        
        df = _get_unified_dataset(session_id)
        if df is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available'
            }
        
        # Check if variables exist
        all_vars = [dependent_var] + independent_vars
        missing_vars = [var for var in all_vars if var not in df.columns]
        
        if missing_vars:
            return {
                'status': 'error',
                'message': f'Variables not found: {missing_vars}'
            }
        
        # Prepare data - remove rows with any missing values
        data_subset = df[all_vars].dropna()
        
        if len(data_subset) < 10:
            return {
                'status': 'error',
                'message': f'Insufficient data for regression (only {len(data_subset)} complete records)'
            }
        
        X = data_subset[independent_vars]
        y = data_subset[dependent_var]
        
        # Fit regression model
        model = LinearRegression()
        model.fit(X, y)
        
        # Make predictions
        y_pred = model.predict(X)
        
        # Calculate metrics
        r2 = r2_score(y, y_pred)
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        
        # Prepare coefficient results
        coefficients = {}
        for i, var in enumerate(independent_vars):
            coefficients[var] = {
                'coefficient': round(model.coef_[i], 6),
                'variable_type': 'numeric'
            }
        
        return {
            'status': 'success',
            'message': f'Regression analysis completed with R² = {r2:.4f}',
            'dependent_variable': dependent_var,
            'independent_variables': independent_vars,
            'sample_size': len(data_subset),
            'intercept': round(model.intercept_, 6),
            'coefficients': coefficients,
            'model_performance': {
                'r_squared': round(r2, 4),
                'rmse': round(rmse, 4),
                'adjusted_r_squared': round(1 - (1 - r2) * (len(data_subset) - 1) / (len(data_subset) - len(independent_vars) - 1), 4)
            },
            'data_info': {
                'total_records': len(df),
                'complete_records_used': len(data_subset),
                'missing_data_excluded': len(df) - len(data_subset)
            }
        }
        
    except Exception as e:
        logger.error(f"Error performing regression analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in regression analysis: {str(e)}'
        } 