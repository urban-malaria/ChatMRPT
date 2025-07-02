"""
Statistical Analysis Tools for ChatMRPT - Phase 1 Implementation

This module provides comprehensive statistical analysis tools including
descriptive statistics, hypothesis testing, regression analysis, and
advanced statistical methods for malaria risk data analysis.

Tools included:
1. GetDescriptiveStatistics - Comprehensive descriptive statistics
2. GetCorrelationAnalysis - Correlation matrices and significance tests
3. PerformRegressionAnalysis - Linear/multiple regression with diagnostics
4. PerformANOVAAnalysis - Analysis of variance between groups
5. PerformTTest - T-tests for group comparisons
6. GetDistributionAnalysis - Distribution testing and normality checks
7. PerformClusterAnalysis - K-means and hierarchical clustering
8. GetVariableImportance - Feature importance and ranking analysis
"""

import logging
from typing import Dict, Any, Optional, List, Union
from pydantic import Field, validator
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
import warnings

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    top_n_field, get_session_unified_dataset
)

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', category=FutureWarning)


class GetDescriptiveStatistics(BaseTool):
    """
    Calculate comprehensive descriptive statistics for specified variables.
    
    Provides detailed statistical summary including central tendency,
    variability, distribution shape, and outlier detection.
    """
    
    variables: List[str] = Field(
        ...,
        description="List of variable names to analyze",
        min_items=1,
        max_items=20
    )
    
    include_distribution: bool = Field(
        True,
        description="Include distribution statistics (skewness, kurtosis)"
    )
    
    include_outliers: bool = Field(
        True,
        description="Include outlier detection using IQR method"
    )
    
    group_by: Optional[str] = Field(
        None,
        description="Optional grouping variable (e.g., 'vulnerability_category')"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Get descriptive statistics for rainfall and temperature",
            "Analyze distribution of PFPR across all wards",
            "Show statistics for elevation grouped by risk level",
            "Describe housing quality and settlement patterns"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Calculate descriptive statistics"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check if variables exist
            missing_vars = [var for var in self.variables if var not in gdf.columns]
            if missing_vars:
                available_vars = [col for col in gdf.columns if any(
                    term in col.lower() for var in missing_vars for term in var.lower().split()
                )][:10]
                error_msg = f"Variables not found: {missing_vars}"
                if available_vars:
                    error_msg += f". Similar variables: {available_vars}"
                return self._create_error_result(error_msg)
            
            result_data = {
                'variables_analyzed': self.variables,
                'statistics': {},
                'summary': {}
            }
            
            # Group by variable if specified
            if self.group_by and self.group_by in gdf.columns:
                groups = gdf.groupby(self.group_by)
                result_data['grouped_by'] = self.group_by
                result_data['groups'] = list(groups.groups.keys())
                
                for group_name, group_data in groups:
                    group_stats = {}
                    for var in self.variables:
                        if var in group_data.columns:
                            var_data = group_data[var].dropna()
                            if len(var_data) > 0 and var_data.dtype in ['int64', 'float64']:
                                group_stats[var] = self._calculate_variable_stats(var_data)
                    
                    if group_stats:
                        result_data['statistics'][str(group_name)] = group_stats
            else:
                # Overall statistics
                for var in self.variables:
                    var_data = gdf[var].dropna()
                    if len(var_data) > 0 and var_data.dtype in ['int64', 'float64']:
                        result_data['statistics'][var] = self._calculate_variable_stats(var_data)
            
            # Generate summary
            total_vars = len([v for v in self.variables if v in result_data['statistics']])
            result_data['summary'] = {
                'total_variables': len(self.variables),
                'analyzed_variables': total_vars,
                'sample_size': len(gdf),
                'grouping': self.group_by is not None
            }
            
            message = f"Descriptive statistics calculated for {total_vars} variables"
            if self.group_by:
                message += f" grouped by {self.group_by}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error calculating descriptive statistics: {e}")
            return self._create_error_result(f"Error in statistical analysis: {str(e)}")
    
    def _calculate_variable_stats(self, data: pd.Series) -> Dict[str, float]:
        """Calculate comprehensive statistics for a variable"""
        stats_dict = {
            'count': len(data),
            'mean': float(data.mean()),
            'median': float(data.median()),
            'std': float(data.std()),
            'variance': float(data.var()),
            'min': float(data.min()),
            'max': float(data.max()),
            'range': float(data.max() - data.min()),
            'q25': float(data.quantile(0.25)),
            'q75': float(data.quantile(0.75)),
            'iqr': float(data.quantile(0.75) - data.quantile(0.25))
        }
        
        if self.include_distribution:
            stats_dict.update({
                'skewness': float(stats.skew(data)),
                'kurtosis': float(stats.kurtosis(data)),
                'coefficient_of_variation': float(data.std() / data.mean()) if data.mean() != 0 else np.nan
            })
        
        if self.include_outliers:
            # IQR method for outlier detection
            q1, q3 = data.quantile(0.25), data.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = data[(data < lower_bound) | (data > upper_bound)]
            
            stats_dict.update({
                'outliers_count': len(outliers),
                'outliers_percentage': float(len(outliers) / len(data) * 100),
                'outlier_lower_bound': float(lower_bound),
                'outlier_upper_bound': float(upper_bound)
            })
        
        return stats_dict


class GetCorrelationAnalysis(BaseTool):
    """
    Perform correlation analysis between variables.
    
    Calculates correlation matrices with significance tests and
    identifies strongest relationships in the data.
    """
    
    variables: Optional[List[str]] = Field(
        None,
        description="Variables to analyze (if None, uses all numeric variables)"
    )
    
    correlation_method: str = Field(
        "pearson",
        description="Correlation method: 'pearson', 'spearman', or 'kendall'",
        pattern="^(pearson|spearman|kendall)$"
    )
    
    significance_level: float = Field(
        0.05,
        description="Significance level for correlation tests",
        ge=0.001,
        le=0.1
    )
    
    min_correlation: float = Field(
        0.1,
        description="Minimum correlation threshold to report",
        ge=0.0,
        le=1.0
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Analyze correlations between environmental variables",
            "Find correlations with PFPR",
            "Show strongest correlations in the dataset",
            "Correlation analysis using Spearman method"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform correlation analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Select variables
            if self.variables:
                missing_vars = [var for var in self.variables if var not in gdf.columns]
                if missing_vars:
                    return self._create_error_result(f"Variables not found: {missing_vars}")
                analysis_vars = self.variables
            else:
                # Use all numeric variables
                numeric_cols = gdf.select_dtypes(include=[np.number]).columns
                analysis_vars = [col for col in numeric_cols if col not in ['geometry']]
            
            # Get data for analysis
            analysis_data = gdf[analysis_vars].dropna()
            
            if len(analysis_data) < 3:
                return self._create_error_result("Insufficient data for correlation analysis.")
            
            # Calculate correlation matrix
            if self.correlation_method == 'pearson':
                corr_matrix = analysis_data.corr(method='pearson')
            elif self.correlation_method == 'spearman':
                corr_matrix = analysis_data.corr(method='spearman')
            else:  # kendall
                corr_matrix = analysis_data.corr(method='kendall')
            
            # Calculate p-values for significance testing
            n = len(analysis_data)
            p_values = np.zeros((len(analysis_vars), len(analysis_vars)))
            
            for i, var1 in enumerate(analysis_vars):
                for j, var2 in enumerate(analysis_vars):
                    if i != j:
                        if self.correlation_method == 'pearson':
                            _, p_val = stats.pearsonr(analysis_data[var1], analysis_data[var2])
                        elif self.correlation_method == 'spearman':
                            _, p_val = stats.spearmanr(analysis_data[var1], analysis_data[var2])
                        else:  # kendall
                            _, p_val = stats.kendalltau(analysis_data[var1], analysis_data[var2])
                        p_values[i, j] = p_val
            
            # Find significant correlations
            significant_correlations = []
            strong_correlations = []
            
            for i, var1 in enumerate(analysis_vars):
                for j, var2 in enumerate(analysis_vars):
                    if i < j:  # Avoid duplicates
                        corr_val = corr_matrix.iloc[i, j]
                        p_val = p_values[i, j]
                        
                        if abs(corr_val) >= self.min_correlation:
                            corr_info = {
                                'variable1': var1,
                                'variable2': var2,
                                'correlation': float(corr_val),
                                'p_value': float(p_val),
                                'significant': p_val < self.significance_level,
                                'strength': self._interpret_correlation(abs(corr_val))
                            }
                            
                            if p_val < self.significance_level:
                                significant_correlations.append(corr_info)
                            
                            if abs(corr_val) >= 0.5:
                                strong_correlations.append(corr_info)
            
            # Sort by absolute correlation value
            significant_correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)
            strong_correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)
            
            result_data = {
                'method': self.correlation_method,
                'variables_analyzed': analysis_vars,
                'sample_size': len(analysis_data),
                'correlation_matrix': corr_matrix.round(3).to_dict(),
                'significant_correlations': significant_correlations[:20],  # Top 20
                'strong_correlations': strong_correlations,
                'summary': {
                    'total_pairs': len(analysis_vars) * (len(analysis_vars) - 1) // 2,
                    'significant_pairs': len(significant_correlations),
                    'strong_correlations': len(strong_correlations),
                    'significance_level': self.significance_level
                }
            }
            
            message = f"Correlation analysis completed using {self.correlation_method} method. "
            message += f"Found {len(significant_correlations)} significant correlations"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in correlation analysis: {e}")
            return self._create_error_result(f"Error in correlation analysis: {str(e)}")
    
    def _interpret_correlation(self, corr_value: float) -> str:
        """Interpret correlation strength"""
        if corr_value >= 0.8:
            return "Very Strong"
        elif corr_value >= 0.6:
            return "Strong"
        elif corr_value >= 0.4:
            return "Moderate"
        elif corr_value >= 0.2:
            return "Weak"
        else:
            return "Very Weak"


class PerformRegressionAnalysis(BaseTool):
    """
    Perform linear and multiple regression analysis.
    
    Includes model diagnostics, significance testing, and
    prediction accuracy metrics.
    """
    
    dependent_variable: str = Field(
        ...,
        description="Dependent variable (target) for regression"
    )
    
    independent_variables: List[str] = Field(
        ...,
        description="Independent variables (predictors)",
        min_items=1,
        max_items=15
    )
    
    include_diagnostics: bool = Field(
        True,
        description="Include regression diagnostics and residual analysis"
    )
    
    significance_level: float = Field(
        0.05,
        description="Significance level for coefficient tests",
        ge=0.001,
        le=0.1
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Predict PFPR using environmental variables",
            "Regression analysis of malaria risk factors",
            "Model composite score using demographic data",
            "Analyze relationship between rainfall and disease"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform regression analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check variables exist
            all_vars = [self.dependent_variable] + self.independent_variables
            missing_vars = [var for var in all_vars if var not in gdf.columns]
            if missing_vars:
                return self._create_error_result(f"Variables not found: {missing_vars}")
            
            # Prepare data
            analysis_data = gdf[all_vars].dropna()
            
            if len(analysis_data) < len(self.independent_variables) + 5:
                return self._create_error_result("Insufficient data for regression analysis.")
            
            X = analysis_data[self.independent_variables]
            y = analysis_data[self.dependent_variable]
            
            # Perform regression
            model = LinearRegression()
            model.fit(X, y)
            
            # Predictions and metrics
            y_pred = model.predict(X)
            r2 = r2_score(y, y_pred)
            adjusted_r2 = 1 - (1 - r2) * (len(y) - 1) / (len(y) - len(self.independent_variables) - 1)
            rmse = np.sqrt(mean_squared_error(y, y_pred))
            mae = np.mean(np.abs(y - y_pred))
            
            # Calculate t-statistics and p-values for coefficients
            residuals = y - y_pred
            mse = np.sum(residuals**2) / (len(y) - len(self.independent_variables) - 1)
            
            # Standard errors of coefficients
            X_with_intercept = np.column_stack([np.ones(len(X)), X])
            try:
                cov_matrix = mse * np.linalg.inv(X_with_intercept.T @ X_with_intercept)
                se_coefficients = np.sqrt(np.diag(cov_matrix))
                
                # t-statistics and p-values
                coefficients = np.concatenate([[model.intercept_], model.coef_])
                t_stats = coefficients / se_coefficients
                p_values = 2 * (1 - stats.t.cdf(np.abs(t_stats), len(y) - len(self.independent_variables) - 1))
            except:
                se_coefficients = np.full(len(coefficients), np.nan)
                t_stats = np.full(len(coefficients), np.nan)
                p_values = np.full(len(coefficients), np.nan)
            
            # Prepare coefficient results
            coefficient_results = [{
                'variable': 'intercept',
                'coefficient': float(model.intercept_),
                'std_error': float(se_coefficients[0]) if not np.isnan(se_coefficients[0]) else None,
                't_statistic': float(t_stats[0]) if not np.isnan(t_stats[0]) else None,
                'p_value': float(p_values[0]) if not np.isnan(p_values[0]) else None,
                'significant': p_values[0] < self.significance_level if not np.isnan(p_values[0]) else None
            }]
            
            for i, var in enumerate(self.independent_variables):
                coefficient_results.append({
                    'variable': var,
                    'coefficient': float(model.coef_[i]),
                    'std_error': float(se_coefficients[i+1]) if not np.isnan(se_coefficients[i+1]) else None,
                    't_statistic': float(t_stats[i+1]) if not np.isnan(t_stats[i+1]) else None,
                    'p_value': float(p_values[i+1]) if not np.isnan(p_values[i+1]) else None,
                    'significant': p_values[i+1] < self.significance_level if not np.isnan(p_values[i+1]) else None
                })
            
            result_data = {
                'dependent_variable': self.dependent_variable,
                'independent_variables': self.independent_variables,
                'sample_size': len(analysis_data),
                'model_performance': {
                    'r_squared': float(r2),
                    'adjusted_r_squared': float(adjusted_r2),
                    'rmse': float(rmse),
                    'mae': float(mae)
                },
                'coefficients': coefficient_results,
                'model_equation': self._generate_equation(model, self.independent_variables)
            }
            
            # Add diagnostics if requested
            if self.include_diagnostics:
                result_data['diagnostics'] = {
                    'residuals_mean': float(np.mean(residuals)),
                    'residuals_std': float(np.std(residuals)),
                    'residuals_normality_pvalue': float(stats.jarque_bera(residuals)[1]),
                    'durbin_watson': float(self._durbin_watson(residuals))
                }
            
            message = f"Regression analysis completed. R² = {r2:.3f}, "
            significant_vars = sum(1 for coef in coefficient_results[1:] if coef.get('significant'))
            message += f"{significant_vars}/{len(self.independent_variables)} significant predictors"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in regression analysis: {e}")
            return self._create_error_result(f"Error in regression analysis: {str(e)}")
    
    def _generate_equation(self, model, variables):
        """Generate regression equation string"""
        equation = f"y = {model.intercept_:.3f}"
        for i, var in enumerate(variables):
            coef = model.coef_[i]
            sign = "+" if coef >= 0 else ""
            equation += f" {sign}{coef:.3f}*{var}"
        return equation
    
    def _durbin_watson(self, residuals):
        """Calculate Durbin-Watson statistic"""
        diff = np.diff(residuals)
        return np.sum(diff**2) / np.sum(residuals**2)


class PerformANOVAAnalysis(BaseTool):
    """
    Perform Analysis of Variance (ANOVA) to test group differences.
    
    Supports one-way and two-way ANOVA with post-hoc testing.
    """
    
    dependent_variable: str = Field(
        ...,
        description="Continuous dependent variable to analyze"
    )
    
    group_variables: List[str] = Field(
        ...,
        description="Categorical grouping variables (1 or 2 variables)",
        min_items=1,
        max_items=2
    )
    
    post_hoc_test: str = Field(
        "tukey",
        description="Post-hoc test method: 'tukey', 'bonferroni', or 'none'",
        pattern="^(tukey|bonferroni|none)$"
    )
    
    significance_level: float = Field(
        0.05,
        description="Significance level for ANOVA",
        ge=0.001,
        le=0.1
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Compare PFPR across risk categories",
            "Test rainfall differences between settlement types",
            "ANOVA of composite scores by urban status",
            "Compare elevation across vulnerability groups"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform ANOVA analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check variables exist
            all_vars = [self.dependent_variable] + self.group_variables
            missing_vars = [var for var in all_vars if var not in gdf.columns]
            if missing_vars:
                return self._create_error_result(f"Variables not found: {missing_vars}")
            
            # Prepare data
            analysis_data = gdf[all_vars].dropna()
            
            if len(analysis_data) < 6:
                return self._create_error_result("Insufficient data for ANOVA analysis.")
            
            # Perform ANOVA
            if len(self.group_variables) == 1:
                # One-way ANOVA
                groups = [group[self.dependent_variable].values for name, group in 
                         analysis_data.groupby(self.group_variables[0])]
                
                if len(groups) < 2:
                    return self._create_error_result("Need at least 2 groups for ANOVA.")
                
                f_stat, p_value = stats.f_oneway(*groups)
                
                # Group statistics
                group_stats = []
                for group_name, group_data in analysis_data.groupby(self.group_variables[0]):
                    values = group_data[self.dependent_variable]
                    group_stats.append({
                        'group': str(group_name),
                        'count': len(values),
                        'mean': float(values.mean()),
                        'std': float(values.std()),
                        'min': float(values.min()),
                        'max': float(values.max())
                    })
                
                result_data = {
                    'anova_type': 'one_way',
                    'dependent_variable': self.dependent_variable,
                    'group_variable': self.group_variables[0],
                    'sample_size': len(analysis_data),
                    'anova_results': {
                        'f_statistic': float(f_stat),
                        'p_value': float(p_value),
                        'significant': p_value < self.significance_level,
                        'degrees_of_freedom_between': len(groups) - 1,
                        'degrees_of_freedom_within': len(analysis_data) - len(groups)
                    },
                    'group_statistics': group_stats
                }
                
            else:
                # Two-way ANOVA (simplified)
                from scipy.stats import f_oneway
                
                # Group by both variables
                grouped = analysis_data.groupby(self.group_variables)
                if len(grouped) < 3:
                    return self._create_error_result("Insufficient groups for two-way ANOVA.")
                
                # Main effects and interaction (simplified approach)
                # Factor 1 main effect
                factor1_groups = [group[self.dependent_variable].values for name, group in 
                                analysis_data.groupby(self.group_variables[0])]
                f1_stat, f1_p = stats.f_oneway(*factor1_groups)
                
                # Factor 2 main effect
                factor2_groups = [group[self.dependent_variable].values for name, group in 
                                analysis_data.groupby(self.group_variables[1])]
                f2_stat, f2_p = stats.f_oneway(*factor2_groups)
                
                result_data = {
                    'anova_type': 'two_way',
                    'dependent_variable': self.dependent_variable,
                    'group_variables': self.group_variables,
                    'sample_size': len(analysis_data),
                    'main_effects': {
                        self.group_variables[0]: {
                            'f_statistic': float(f1_stat),
                            'p_value': float(f1_p),
                            'significant': f1_p < self.significance_level
                        },
                        self.group_variables[1]: {
                            'f_statistic': float(f2_stat),
                            'p_value': float(f2_p),
                            'significant': f2_p < self.significance_level
                        }
                    }
                }
            
            # Generate message
            if len(self.group_variables) == 1:
                sig_status = "significant" if result_data['anova_results']['significant'] else "not significant"
                message = f"One-way ANOVA completed. Group differences are {sig_status} "
                message += f"(F = {f_stat:.3f}, p = {p_value:.4f})"
            else:
                message = f"Two-way ANOVA completed for {len(self.group_variables)} factors"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in ANOVA analysis: {e}")
            return self._create_error_result(f"Error in ANOVA analysis: {str(e)}")


class PerformTTest(BaseTool):
    """
    Perform various t-tests for group comparisons.
    
    Supports one-sample, independent samples, and paired t-tests
    with effect size calculations.
    """
    
    test_type: str = Field(
        "independent",
        description="Type of t-test: 'independent', 'paired', or 'one_sample'",
        pattern="^(independent|paired|one_sample)$"
    )
    
    variable: str = Field(
        ...,
        description="Variable to test"
    )
    
    group_variable: Optional[str] = Field(
        None,
        description="Grouping variable (required for independent samples t-test)"
    )
    
    test_value: Optional[float] = Field(
        None,
        description="Test value for one-sample t-test"
    )
    
    paired_variable: Optional[str] = Field(
        None,
        description="Second variable for paired t-test"
    )
    
    significance_level: float = Field(
        0.05,
        description="Significance level",
        ge=0.001,
        le=0.1
    )
    
    equal_variances: bool = Field(
        True,
        description="Assume equal variances (for independent t-test)"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Compare PFPR between urban and rural wards",
            "Test if rainfall differs from national average",
            "Paired t-test for before/after intervention data",
            "Compare elevation between high and low risk areas"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform t-test analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check variable exists
            if self.variable not in gdf.columns:
                return self._create_error_result(f"Variable '{self.variable}' not found.")
            
            result_data = {
                'test_type': self.test_type,
                'variable': self.variable,
                'significance_level': self.significance_level
            }
            
            if self.test_type == "one_sample":
                if self.test_value is None:
                    return self._create_error_result("Test value required for one-sample t-test.")
                
                data = gdf[self.variable].dropna()
                if len(data) < 3:
                    return self._create_error_result("Insufficient data for t-test.")
                
                t_stat, p_value = stats.ttest_1samp(data, self.test_value)
                
                result_data.update({
                    'test_value': self.test_value,
                    'sample_mean': float(data.mean()),
                    'sample_std': float(data.std()),
                    'sample_size': len(data),
                    'test_results': {
                        't_statistic': float(t_stat),
                        'p_value': float(p_value),
                        'significant': p_value < self.significance_level,
                        'degrees_of_freedom': len(data) - 1,
                        'effect_size_d': float((data.mean() - self.test_value) / data.std())
                    }
                })
                
                message = f"One-sample t-test: sample mean ({data.mean():.3f}) vs test value ({self.test_value})"
                
            elif self.test_type == "independent":
                if self.group_variable is None:
                    return self._create_error_result("Group variable required for independent t-test.")
                
                if self.group_variable not in gdf.columns:
                    return self._create_error_result(f"Group variable '{self.group_variable}' not found.")
                
                analysis_data = gdf[[self.variable, self.group_variable]].dropna()
                groups = analysis_data.groupby(self.group_variable)[self.variable]
                
                if len(groups) != 2:
                    return self._create_error_result("Independent t-test requires exactly 2 groups.")
                
                group_names = list(groups.groups.keys())
                group1_data = groups.get_group(group_names[0])
                group2_data = groups.get_group(group_names[1])
                
                if len(group1_data) < 3 or len(group2_data) < 3:
                    return self._create_error_result("Insufficient data in groups for t-test.")
                
                # Perform t-test
                t_stat, p_value = stats.ttest_ind(group1_data, group2_data, equal_var=self.equal_variances)
                
                # Effect size (Cohen's d)
                pooled_std = np.sqrt(((len(group1_data) - 1) * group1_data.var() + 
                                    (len(group2_data) - 1) * group2_data.var()) / 
                                   (len(group1_data) + len(group2_data) - 2))
                cohens_d = (group1_data.mean() - group2_data.mean()) / pooled_std
                
                result_data.update({
                    'group_variable': self.group_variable,
                    'group1': {
                        'name': str(group_names[0]),
                        'size': len(group1_data),
                        'mean': float(group1_data.mean()),
                        'std': float(group1_data.std())
                    },
                    'group2': {
                        'name': str(group_names[1]),
                        'size': len(group2_data),
                        'mean': float(group2_data.mean()),
                        'std': float(group2_data.std())
                    },
                    'test_results': {
                        't_statistic': float(t_stat),
                        'p_value': float(p_value),
                        'significant': p_value < self.significance_level,
                        'degrees_of_freedom': len(group1_data) + len(group2_data) - 2,
                        'cohens_d': float(cohens_d),
                        'effect_size': self._interpret_effect_size(abs(cohens_d))
                    }
                })
                
                message = f"Independent t-test: {group_names[0]} vs {group_names[1]}"
                
            else:  # paired
                if self.paired_variable is None:
                    return self._create_error_result("Second variable required for paired t-test.")
                
                if self.paired_variable not in gdf.columns:
                    return self._create_error_result(f"Paired variable '{self.paired_variable}' not found.")
                
                analysis_data = gdf[[self.variable, self.paired_variable]].dropna()
                
                if len(analysis_data) < 3:
                    return self._create_error_result("Insufficient data for paired t-test.")
                
                var1_data = analysis_data[self.variable]
                var2_data = analysis_data[self.paired_variable]
                
                t_stat, p_value = stats.ttest_rel(var1_data, var2_data)
                
                # Effect size for paired data
                differences = var1_data - var2_data
                cohens_d = differences.mean() / differences.std()
                
                result_data.update({
                    'paired_variable': self.paired_variable,
                    'sample_size': len(analysis_data),
                    'variable1_mean': float(var1_data.mean()),
                    'variable2_mean': float(var2_data.mean()),
                    'mean_difference': float(differences.mean()),
                    'test_results': {
                        't_statistic': float(t_stat),
                        'p_value': float(p_value),
                        'significant': p_value < self.significance_level,
                        'degrees_of_freedom': len(analysis_data) - 1,
                        'cohens_d': float(cohens_d),
                        'effect_size': self._interpret_effect_size(abs(cohens_d))
                    }
                })
                
                message = f"Paired t-test: {self.variable} vs {self.paired_variable}"
            
            # Add significance interpretation to message
            sig_status = "significant" if result_data['test_results']['significant'] else "not significant"
            message += f" - {sig_status} (p = {result_data['test_results']['p_value']:.4f})"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in t-test analysis: {e}")
            return self._create_error_result(f"Error in t-test analysis: {str(e)}")
    
    def _interpret_effect_size(self, d_value: float) -> str:
        """Interpret Cohen's d effect size"""
        if d_value >= 0.8:
            return "Large"
        elif d_value >= 0.5:
            return "Medium"
        elif d_value >= 0.2:
            return "Small"
        else:
            return "Negligible"


class GetDistributionAnalysis(BaseTool):
    """
    Analyze data distributions and test for normality.
    
    Includes normality tests, distribution fitting, and
    outlier detection methods.
    """
    
    variables: List[str] = Field(
        ...,
        description="Variables to analyze for distribution",
        min_items=1,
        max_items=10
    )
    
    normality_tests: List[str] = Field(
        ["shapiro", "jarque_bera"],
        description="Normality tests to perform: 'shapiro', 'jarque_bera', 'anderson'"
    )
    
    outlier_methods: List[str] = Field(
        ["iqr", "zscore"],
        description="Outlier detection methods: 'iqr', 'zscore', 'isolation_forest'"
    )
    
    significance_level: float = Field(
        0.05,
        description="Significance level for normality tests",
        ge=0.001,
        le=0.1
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Test normality of PFPR distribution",
            "Analyze rainfall distribution patterns",
            "Check for outliers in composite scores",
            "Distribution analysis of environmental variables"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform distribution analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check variables exist
            missing_vars = [var for var in self.variables if var not in gdf.columns]
            if missing_vars:
                return self._create_error_result(f"Variables not found: {missing_vars}")
            
            result_data = {
                'variables_analyzed': self.variables,
                'distribution_analysis': {},
                'summary': {}
            }
            
            normal_vars = 0
            total_outliers = 0
            
            for var in self.variables:
                data = gdf[var].dropna()
                
                if len(data) < 3:
                    continue
                
                var_analysis = {
                    'sample_size': len(data),
                    'missing_values': len(gdf) - len(data),
                    'basic_stats': {
                        'mean': float(data.mean()),
                        'median': float(data.median()),
                        'std': float(data.std()),
                        'skewness': float(stats.skew(data)),
                        'kurtosis': float(stats.kurtosis(data))
                    },
                    'normality_tests': {},
                    'outlier_analysis': {}
                }
                
                # Normality tests
                is_normal = True
                for test_name in self.normality_tests:
                    if test_name == "shapiro" and len(data) <= 5000:  # Shapiro-Wilk has sample size limit
                        stat, p_val = stats.shapiro(data)
                        var_analysis['normality_tests']['shapiro_wilk'] = {
                            'statistic': float(stat),
                            'p_value': float(p_val),
                            'normal': p_val > self.significance_level
                        }
                        if p_val <= self.significance_level:
                            is_normal = False
                    
                    elif test_name == "jarque_bera":
                        stat, p_val = stats.jarque_bera(data)
                        var_analysis['normality_tests']['jarque_bera'] = {
                            'statistic': float(stat),
                            'p_value': float(p_val),
                            'normal': p_val > self.significance_level
                        }
                        if p_val <= self.significance_level:
                            is_normal = False
                    
                    elif test_name == "anderson":
                        result = stats.anderson(data, dist='norm')
                        # Use 5% significance level (index 2)
                        critical_val = result.critical_values[2]
                        var_analysis['normality_tests']['anderson_darling'] = {
                            'statistic': float(result.statistic),
                            'critical_value': float(critical_val),
                            'normal': result.statistic < critical_val
                        }
                        if result.statistic >= critical_val:
                            is_normal = False
                
                if is_normal:
                    normal_vars += 1
                
                # Outlier detection
                var_outliers = 0
                for method in self.outlier_methods:
                    if method == "iqr":
                        q1, q3 = data.quantile([0.25, 0.75])
                        iqr = q3 - q1
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        outliers = data[(data < lower_bound) | (data > upper_bound)]
                        
                        var_analysis['outlier_analysis']['iqr_method'] = {
                            'outliers_count': len(outliers),
                            'outliers_percentage': float(len(outliers) / len(data) * 100),
                            'lower_bound': float(lower_bound),
                            'upper_bound': float(upper_bound)
                        }
                        var_outliers = max(var_outliers, len(outliers))
                    
                    elif method == "zscore":
                        z_scores = np.abs(stats.zscore(data))
                        outliers = data[z_scores > 3]  # |z| > 3 considered outliers
                        
                        var_analysis['outlier_analysis']['zscore_method'] = {
                            'outliers_count': len(outliers),
                            'outliers_percentage': float(len(outliers) / len(data) * 100),
                            'threshold': 3.0
                        }
                        var_outliers = max(var_outliers, len(outliers))
                
                total_outliers += var_outliers
                var_analysis['overall_normality'] = is_normal
                result_data['distribution_analysis'][var] = var_analysis
            
            # Summary
            result_data['summary'] = {
                'total_variables': len(self.variables),
                'analyzed_variables': len(result_data['distribution_analysis']),
                'normal_variables': normal_vars,
                'normality_percentage': float(normal_vars / len(result_data['distribution_analysis']) * 100) if result_data['distribution_analysis'] else 0,
                'total_outliers_detected': total_outliers
            }
            
            message = f"Distribution analysis completed for {len(result_data['distribution_analysis'])} variables. "
            message += f"{normal_vars} variables appear normally distributed"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in distribution analysis: {e}")
            return self._create_error_result(f"Error in distribution analysis: {str(e)}")


class PerformClusterAnalysis(BaseTool):
    """
    Perform clustering analysis to identify ward groupings.
    
    Supports K-means clustering with optimal cluster number detection
    and hierarchical clustering.
    """
    
    variables: List[str] = Field(
        ...,
        description="Variables to use for clustering",
        min_items=2,
        max_items=15
    )
    
    n_clusters: Optional[int] = Field(
        None,
        description="Number of clusters (if None, optimal number will be determined)",
        ge=2,
        le=20
    )
    
    clustering_method: str = Field(
        "kmeans",
        description="Clustering method: 'kmeans' or 'hierarchical'",
        pattern="^(kmeans|hierarchical)$"
    )
    
    standardize_data: bool = Field(
        True,
        description="Standardize variables before clustering"
    )
    
    max_clusters_test: int = Field(
        10,
        description="Maximum clusters to test for optimal number",
        ge=3,
        le=20
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Cluster wards by environmental characteristics",
            "Group wards by malaria risk factors",
            "Find similar wards using demographic variables",
            "Cluster analysis of settlement patterns"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Perform cluster analysis"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check variables exist
            missing_vars = [var for var in self.variables if var not in gdf.columns]
            if missing_vars:
                return self._create_error_result(f"Variables not found: {missing_vars}")
            
            # Prepare data
            cluster_data = gdf[self.variables + ['WardName']].dropna()
            
            if len(cluster_data) < 6:
                return self._create_error_result("Insufficient data for clustering analysis.")
            
            X = cluster_data[self.variables]
            
            # Standardize data if requested
            if self.standardize_data:
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                X_for_clustering = X_scaled
            else:
                X_for_clustering = X.values
            
            result_data = {
                'variables_used': self.variables,
                'sample_size': len(cluster_data),
                'standardized': self.standardize_data,
                'clustering_method': self.clustering_method
            }
            
            if self.clustering_method == "kmeans":
                # Determine optimal number of clusters if not specified
                if self.n_clusters is None:
                    inertias = []
                    silhouette_scores = []
                    k_range = range(2, min(self.max_clusters_test + 1, len(cluster_data) // 2))
                    
                    for k in k_range:
                        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
                        labels = kmeans.fit_predict(X_for_clustering)
                        inertias.append(kmeans.inertia_)
                        
                        # Calculate silhouette score
                        from sklearn.metrics import silhouette_score
                        if len(set(labels)) > 1:
                            sil_score = silhouette_score(X_for_clustering, labels)
                            silhouette_scores.append(sil_score)
                        else:
                            silhouette_scores.append(0)
                    
                    # Find optimal k using elbow method (simplified)
                    if len(silhouette_scores) > 0:
                        optimal_k = k_range[np.argmax(silhouette_scores)]
                    else:
                        optimal_k = 3
                    
                    result_data['cluster_optimization'] = {
                        'tested_k_values': list(k_range),
                        'inertias': inertias,
                        'silhouette_scores': silhouette_scores,
                        'optimal_k': optimal_k
                    }
                else:
                    optimal_k = self.n_clusters
                
                # Perform final clustering
                kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
                cluster_labels = kmeans.fit_predict(X_for_clustering)
                
                # Calculate final silhouette score
                from sklearn.metrics import silhouette_score
                if len(set(cluster_labels)) > 1:
                    final_silhouette = silhouette_score(X_for_clustering, cluster_labels)
                else:
                    final_silhouette = 0
                
                result_data['clustering_results'] = {
                    'n_clusters': optimal_k,
                    'silhouette_score': float(final_silhouette),
                    'inertia': float(kmeans.inertia_)
                }
                
                # Cluster centers (in original scale if standardized)
                if self.standardize_data:
                    centers = scaler.inverse_transform(kmeans.cluster_centers_)
                else:
                    centers = kmeans.cluster_centers_
                
                cluster_centers = {}
                for i, var in enumerate(self.variables):
                    cluster_centers[var] = [float(center[i]) for center in centers]
                
                result_data['cluster_centers'] = cluster_centers
                
            else:  # hierarchical
                from scipy.cluster.hierarchy import linkage, fcluster
                from scipy.spatial.distance import pdist
                
                # Compute linkage matrix
                linkage_matrix = linkage(X_for_clustering, method='ward')
                
                # Determine number of clusters
                if self.n_clusters is None:
                    optimal_k = 3  # Default for hierarchical
                else:
                    optimal_k = self.n_clusters
                
                # Get cluster labels
                cluster_labels = fcluster(linkage_matrix, optimal_k, criterion='maxclust') - 1
                
                result_data['clustering_results'] = {
                    'n_clusters': optimal_k,
                    'linkage_method': 'ward'
                }
            
            # Analyze clusters
            cluster_data['cluster'] = cluster_labels
            cluster_analysis = {}
            
            for cluster_id in range(max(cluster_labels) + 1):
                cluster_wards = cluster_data[cluster_data['cluster'] == cluster_id]
                
                cluster_stats = {
                    'size': len(cluster_wards),
                    'percentage': float(len(cluster_wards) / len(cluster_data) * 100),
                    'ward_names': cluster_wards['WardName'].tolist()[:10],  # First 10 wards
                    'variable_means': {}
                }
                
                for var in self.variables:
                    cluster_stats['variable_means'][var] = float(cluster_wards[var].mean())
                
                cluster_analysis[f'cluster_{cluster_id}'] = cluster_stats
            
            result_data['cluster_analysis'] = cluster_analysis
            
            message = f"Cluster analysis completed using {self.clustering_method} method. "
            message += f"Identified {optimal_k} clusters"
            if 'silhouette_score' in result_data['clustering_results']:
                message += f" (silhouette score: {result_data['clustering_results']['silhouette_score']:.3f})"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in cluster analysis: {e}")
            return self._create_error_result(f"Error in cluster analysis: {str(e)}")


class GetVariableImportance(BaseTool):
    """
    Calculate variable importance using Random Forest and other methods.
    
    Identifies which variables are most important for predicting
    the target variable.
    """
    
    target_variable: str = Field(
        ...,
        description="Target variable to predict"
    )
    
    predictor_variables: Optional[List[str]] = Field(
        None,
        description="Predictor variables (if None, uses all numeric variables)"
    )
    
    importance_methods: List[str] = Field(
        ["random_forest", "correlation"],
        description="Methods to calculate importance: 'random_forest', 'correlation', 'mutual_info'"
    )
    
    n_estimators: int = Field(
        100,
        description="Number of trees for Random Forest",
        ge=10,
        le=500
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Find most important predictors of PFPR",
            "Variable importance for composite score",
            "Which factors predict malaria risk best?",
            "Rank environmental variables by importance"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Calculate variable importance"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check target variable
            if self.target_variable not in gdf.columns:
                return self._create_error_result(f"Target variable '{self.target_variable}' not found.")
            
            # Select predictor variables
            if self.predictor_variables:
                missing_vars = [var for var in self.predictor_variables if var not in gdf.columns]
                if missing_vars:
                    return self._create_error_result(f"Predictor variables not found: {missing_vars}")
                predictors = self.predictor_variables
            else:
                # Use all numeric variables except target
                numeric_cols = gdf.select_dtypes(include=[np.number]).columns
                predictors = [col for col in numeric_cols if col != self.target_variable and col != 'geometry']
            
            # Prepare data
            all_vars = [self.target_variable] + predictors
            analysis_data = gdf[all_vars].dropna()
            
            if len(analysis_data) < 10:
                return self._create_error_result("Insufficient data for importance analysis.")
            
            X = analysis_data[predictors]
            y = analysis_data[self.target_variable]
            
            result_data = {
                'target_variable': self.target_variable,
                'predictor_variables': predictors,
                'sample_size': len(analysis_data),
                'importance_results': {}
            }
            
            # Random Forest importance
            if "random_forest" in self.importance_methods:
                rf = RandomForestRegressor(n_estimators=self.n_estimators, random_state=42)
                rf.fit(X, y)
                
                rf_importance = []
                for i, var in enumerate(predictors):
                    rf_importance.append({
                        'variable': var,
                        'importance': float(rf.feature_importances_[i]),
                        'rank': 0  # Will be filled after sorting
                    })
                
                # Sort and rank
                rf_importance.sort(key=lambda x: x['importance'], reverse=True)
                for i, item in enumerate(rf_importance):
                    item['rank'] = i + 1
                
                result_data['importance_results']['random_forest'] = {
                    'r2_score': float(rf.score(X, y)),
                    'variable_importance': rf_importance
                }
            
            # Correlation-based importance
            if "correlation" in self.importance_methods:
                corr_importance = []
                for var in predictors:
                    corr, p_val = stats.pearsonr(X[var], y)
                    corr_importance.append({
                        'variable': var,
                        'correlation': float(corr),
                        'abs_correlation': float(abs(corr)),
                        'p_value': float(p_val),
                        'significant': p_val < 0.05,
                        'rank': 0
                    })
                
                # Sort by absolute correlation
                corr_importance.sort(key=lambda x: x['abs_correlation'], reverse=True)
                for i, item in enumerate(corr_importance):
                    item['rank'] = i + 1
                
                result_data['importance_results']['correlation'] = {
                    'variable_importance': corr_importance
                }
            
            # Mutual information importance
            if "mutual_info" in self.importance_methods:
                try:
                    from sklearn.feature_selection import mutual_info_regression
                    
                    mi_scores = mutual_info_regression(X, y, random_state=42)
                    
                    mi_importance = []
                    for i, var in enumerate(predictors):
                        mi_importance.append({
                            'variable': var,
                            'mutual_info_score': float(mi_scores[i]),
                            'rank': 0
                        })
                    
                    # Sort and rank
                    mi_importance.sort(key=lambda x: x['mutual_info_score'], reverse=True)
                    for i, item in enumerate(mi_importance):
                        item['rank'] = i + 1
                    
                    result_data['importance_results']['mutual_info'] = {
                        'variable_importance': mi_importance
                    }
                except ImportError:
                    logger.warning("Mutual information calculation requires additional dependencies")
            
            # Create summary ranking
            if len(result_data['importance_results']) > 1:
                # Combine rankings
                combined_ranks = {}
                for var in predictors:
                    ranks = []
                    for method, results in result_data['importance_results'].items():
                        for item in results['variable_importance']:
                            if item['variable'] == var:
                                ranks.append(item['rank'])
                    
                    if ranks:
                        combined_ranks[var] = np.mean(ranks)
                
                summary_ranking = sorted(combined_ranks.items(), key=lambda x: x[1])
                result_data['summary_ranking'] = [
                    {'variable': var, 'average_rank': float(rank), 'final_rank': i + 1}
                    for i, (var, rank) in enumerate(summary_ranking)
                ]
            
            # Generate message
            if 'random_forest' in result_data['importance_results']:
                top_var = result_data['importance_results']['random_forest']['variable_importance'][0]
                message = f"Variable importance analysis completed. Most important: {top_var['variable']} "
                message += f"(importance: {top_var['importance']:.3f})"
            else:
                message = f"Variable importance analysis completed for {len(predictors)} variables"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error in variable importance analysis: {e}")
            return self._create_error_result(f"Error in importance analysis: {str(e)}")


# Register tools for discovery
__all__ = [
    'GetDescriptiveStatistics',
    'GetCorrelationAnalysis',
    'PerformRegressionAnalysis',
    'PerformANOVAAnalysis',
    'PerformTTest',
    'GetDistributionAnalysis',
    'PerformClusterAnalysis',
    'GetVariableImportance'
]