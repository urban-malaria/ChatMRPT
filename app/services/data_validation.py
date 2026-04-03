"""
Data Validation Module - Quality Checks and Variable Validation

This module handles data validation, quality checks, variable matching,
and ward name mismatch detection.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- DataValidator: Core validation functionality
- Variable matching and validation
- Ward name mismatch detection
- Data quality assessment
"""

import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List, Optional, Union

# Set up logging
from app.services.variable_resolver import variable_resolver
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Handles data validation, quality checks, and variable matching
    """
    
    def __init__(self, interaction_logger=None):
        """
        Initialize data validator
        
        Args:
            interaction_logger: Optional interaction logger
        """
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
    
    def check_wardname_mismatches(self, csv_data: pd.DataFrame, 
                                 shapefile_data: Optional[gpd.GeoDataFrame]) -> Optional[List[str]]:
        """
        Check for ward name mismatches between CSV and shapefile data
        
        Args:
            csv_data: CSV DataFrame with ward data
            shapefile_data: Shapefile GeoDataFrame with ward geometries
            
        Returns:
            List of mismatched ward names or None if no mismatches
        """
        if csv_data is None or shapefile_data is None:
            self.logger.info("Cannot check ward mismatches - missing data")
            return None
        
        if 'WardName' not in csv_data.columns or 'WardName' not in shapefile_data.columns:
            self.logger.warning("WardName column missing in one or both datasets")
            return None
        
        try:
            # Get unique ward names from both datasets
            csv_wards = set(csv_data['WardName'].dropna().unique())
            shp_wards = set(shapefile_data['WardName'].dropna().unique())
            
            # Find mismatches (wards in CSV but not in shapefile)
            csv_only = csv_wards - shp_wards
            shp_only = shp_wards - csv_wards
            
            mismatches = []
            
            if csv_only:
                mismatches.extend([f"CSV only: {ward}" for ward in sorted(csv_only)])
                
            if shp_only:
                mismatches.extend([f"Shapefile only: {ward}" for ward in sorted(shp_only)])
            
            if mismatches:
                self.logger.warning(f"Found {len(mismatches)} ward name mismatches")
                return mismatches
            else:
                self.logger.info("No ward name mismatches found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error checking ward mismatches: {str(e)}", exc_info=True)
            return None
    
    def get_available_variables(self, csv_data: pd.DataFrame, 
                              derived_variables: Optional[List[str]] = None) -> List[str]:
        """
        Get a list of available variable names suitable for analysis
        
        Args:
            csv_data: CSV DataFrame to analyze
            derived_variables: Optional list of derived variables
            
        Returns:
            List of suitable variable names
        """
        if csv_data is None:
            return []
        
        available_vars = []
        for col in csv_data.columns:
            # Exclude 'WardName' and common ID-like columns
            if col.lower() != 'wardname' and not self._is_id_column(col, csv_data):
                # Only include numeric columns for analysis
                if pd.api.types.is_numeric_dtype(csv_data[col]):
                    available_vars.append(col)
        
        # Add any derived variables if they exist
        if derived_variables:
            for var in derived_variables:
                if var not in available_vars:
                    available_vars.append(var)
        
        # Sort for easier browsing
        available_vars.sort()
        
        return available_vars
    
    def find_matching_variables(self, variables: List[str], 
                              available_variables: List[str]) -> Dict[str, Optional[str]]:
        """
        Find matching variables using case-insensitive and partial matching
        
        Args:
            variables: List of variable names to match
            available_variables: List of available variables to match against
            
        Returns:
            Dictionary mapping input variables to matched variables (None for unmatched)
        """
        available_vars_lookup = {var.lower(): var for var in available_variables}
        
        result = {}
        
        for var in variables:
            if not var:
                result[var] = None
                continue
                
            var_lower = var.lower()
            
            # Direct case-insensitive match
            if var_lower in available_vars_lookup:
                result[var] = available_vars_lookup[var_lower]
                continue
            
            # Try normalized version (replace spaces with underscores)
            normalized_var = var_lower.replace(' ', '_')
            if normalized_var in available_vars_lookup:
                result[var] = available_vars_lookup[normalized_var]
                continue
            
            # Try partial matching
            matched = False
            for av_lower, av in available_vars_lookup.items():
                # Check if variable name is contained in available variable or vice versa
                if var_lower in av_lower or av_lower in var_lower:
                    result[var] = av
                    matched = True
                    break
            
            # If no match found
            if not matched:
                result[var] = None
        
        return result
    
    def validate_variables(self, variables, data_columns=None):
        """
        Flexible variable validation with user-friendly name matching (LEGACY RESTORED)
        
        This function is more flexible than before - it tries to match user input
        to actual column names using various strategies:
        1. Exact matches
        2. Case-insensitive matches  
        3. Partial/fuzzy matches
        4. Common aliases and variations
        
        Args:
            variables: List of variable names from user input
            data_columns: Available columns in the dataset
            
        Returns:
            dict: {
                'valid': List of valid column names,
                'invalid': List of invalid user inputs,
                'mapping': Dict mapping user input to actual column names
            }
        """
        if not variables:
            return {'valid': [], 'invalid': [], 'mapping': {}}
        
        if data_columns is None:
            data_columns = []
        
        # Convert to lists if needed
        if isinstance(variables, str):
            variables = [variables]
        if isinstance(data_columns, str):
            data_columns = [data_columns]
        
        # Ensure all elements are strings
        variables = [str(v).strip() for v in variables if v]
        data_columns = [str(col) for col in data_columns if col]
        
        valid_variables = []
        invalid_variables = []
        mapping = {}
        
        # Create lookup mappings for flexible matching
        column_lower_map = {col.lower(): col for col in data_columns}
        
        # Create common aliases mapping (user-friendly names -> likely column patterns)
        aliases = {
            # Population variants
            'population': ['population', 'pop', 'total_pop', 'pop_total', 'people'],
            'pop': ['population', 'pop', 'total_pop', 'pop_total'],
            'people': ['population', 'pop', 'total_pop', 'pop_total', 'people'],
            
            # Rainfall/precipitation variants
            'rainfall': ['rainfall', 'rain', 'precipitation', 'precip', 'prec', 'rf'],
            'rain': ['rainfall', 'rain', 'precipitation', 'precip'],
            'precipitation': ['rainfall', 'rain', 'precipitation', 'precip', 'prec'],
            
            # Temperature variants
            'temperature': ['temperature', 'temp', 'avg_temp', 'mean_temp', 'temp_mean'],
            'temp': ['temperature', 'temp', 'avg_temp', 'mean_temp'],
            
            # Humidity variants
            'humidity': ['humidity', 'humid', 'rh', 'rh_mean', 'relative_humidity'],
            'humid': ['humidity', 'humid', 'rh', 'rh_mean'],
            
            # Elevation variants
            'elevation': ['elevation', 'elev', 'altitude', 'height', 'dem'],
            'altitude': ['elevation', 'elev', 'altitude', 'height'],
            
            # Building/infrastructure variants
            'buildings': ['buildings', 'building', 'built', 'infrastructure', 'structures'],
            'infrastructure': ['buildings', 'building', 'built', 'infrastructure', 'structures'],
            
            # Water/moisture variants
            'water': ['water', 'moisture', 'ndwi', 'water_bodies', 'waterbodies'],
            'moisture': ['water', 'moisture', 'ndwi', 'ndmi'],
            
            # Vegetation variants
            'vegetation': ['vegetation', 'veg', 'ndvi', 'green', 'plants'],
            'ndvi': ['ndvi', 'vegetation', 'veg', 'green'],
            
            # Urban/built-up variants
            'urban': ['urban', 'built', 'city', 'settlement', 'developed'],
            'built': ['urban', 'built', 'city', 'settlement', 'built_up'],
            
            # Economic/livelihood variants
            'livelihood': ['livelihood', 'economic', 'income', 'poverty', 'wealth'],
            'economic': ['livelihood', 'economic', 'income', 'poverty'],
            'poverty': ['poverty', 'poor', 'economic', 'wealth', 'livelihood'],
            
            # Health/medical variants
            'health': ['health', 'medical', 'clinic', 'hospital', 'healthcare'],
            'medical': ['health', 'medical', 'clinic', 'hospital'],
            
            # Access/distance variants
            'access': ['access', 'distance', 'proximity', 'travel', 'reach'],
            'distance': ['access', 'distance', 'proximity', 'travel'],
        }
        
        for var in variables:
            var_original = var
            var_lower = var.lower()
            matched_column = None
            
            # Strategy 1: Exact match
            if var in data_columns:
                matched_column = var
            
            # Strategy 2: Case-insensitive exact match  
            elif var_lower in column_lower_map:
                matched_column = column_lower_map[var_lower]
            
            # Strategy 3: Alias matching
            elif var_lower in aliases:
                alias_patterns = aliases[var_lower]
                for pattern in alias_patterns:
                    if pattern in column_lower_map:
                        matched_column = column_lower_map[pattern]
                        break
            
            # Strategy 4: Partial matching (contains)
            if not matched_column:
                for col in data_columns:
                    col_lower = col.lower()
                    # Check if user input is contained in column name
                    if var_lower in col_lower or col_lower in var_lower:
                        matched_column = col
                        break
            
            # Strategy 5: Pattern matching for complex names
            if not matched_column:
                import re
                var_clean = re.sub(r'[^a-zA-Z0-9]', '', var_lower)
                for col in data_columns:
                    col_clean = re.sub(r'[^a-zA-Z0-9]', '', col.lower())
                    # Check for substantial overlap
                    if len(var_clean) > 2 and len(col_clean) > 2:
                        # Simple similarity check
                        overlap = len(set(var_clean) & set(col_clean))
                        if overlap >= min(3, min(len(var_clean), len(col_clean)) * 0.6):
                            matched_column = col
                            break
            
            # Strategy 6: Fuzzy matching for typos/variations
            if not matched_column and len(var) > 3:
                best_match = None
                best_score = 0
                for col in data_columns:
                    # Simple Levenshtein-like scoring
                    score = self._calculate_similarity(var_lower, col.lower())
                    if score > best_score and score > 0.7:  # 70% similarity threshold
                        best_score = score
                        best_match = col
                
                if best_match:
                    matched_column = best_match
            
            # Record results
            if matched_column:
                valid_variables.append(matched_column)
                mapping[var_original] = matched_column
            else:
                invalid_variables.append(var_original)
        
        return {
            'valid': valid_variables,
            'invalid': invalid_variables, 
            'mapping': mapping
        }
    
    def _calculate_similarity(self, str1, str2):
        """Calculate similarity between two strings using a simple algorithm"""
        if not str1 or not str2:
            return 0.0
        
        # Simple character overlap similarity
        set1 = set(str1.lower())
        set2 = set(str2.lower())
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
        
        return intersection / union
    
    def check_missing_values(self, df: pd.DataFrame) -> List[str]:
        """
        Check for columns with missing values
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of column names with missing values
        """
        # Vectorized operation
        missing_counts = df.isna().sum()
        cols_with_missing = missing_counts[missing_counts > 0].index.tolist()
        
        if cols_with_missing:
            self.logger.info(f"Found missing values in {len(cols_with_missing)} columns")
        
        return cols_with_missing
    
    def run_data_quality_checks(self, csv_data: pd.DataFrame, 
                               shapefile_data: Optional[gpd.GeoDataFrame] = None) -> Dict[str, Any]:
        """
        Run comprehensive data quality checks
        
        Args:
            csv_data: CSV DataFrame to check
            shapefile_data: Optional shapefile data
            
        Returns:
            Dictionary with quality check results
        """
        quality_results = {
            'status': 'success',
            'checks_performed': [],
            'issues_found': [],
            'severe_issues': [],
            'recommendations': [],
            'quality_scores': {}  # Enhanced: Add quality scoring
        }
        
        try:
            # Enhanced: Calculate comprehensive quality scores
            quality_results['quality_scores'] = self.calculate_enhanced_quality_scores(csv_data)
            
            # Check 1: Missing values
            missing_columns = self.check_missing_values(csv_data)
            quality_results['checks_performed'].append('missing_values')
            
            if missing_columns:
                quality_results['issues_found'].append({
                    'type': 'missing_values',
                    'columns': missing_columns,
                    'count': len(missing_columns)
                })
                
                # Flag columns with >50% missing as severe
                for col in missing_columns:
                    missing_ratio = csv_data[col].isna().sum() / len(csv_data)
                    if missing_ratio > 0.5:
                        quality_results['severe_issues'].append({
                            'type': 'high_missing_values',
                            'column': col,
                            'missing_ratio': missing_ratio,
                            'recommendation': f'Consider excluding {col} or use advanced imputation'
                        })
            
            # Check 2: Duplicate ward names
            if 'WardName' in csv_data.columns:
                duplicates = csv_data['WardName'].duplicated().sum()
                quality_results['checks_performed'].append('duplicate_wards')
                
                if duplicates > 0:
                    quality_results['issues_found'].append({
                        'type': 'duplicate_wards',
                        'count': duplicates
                    })
            
            # Check 3: Ward name mismatches (if shapefile available)
            if shapefile_data is not None:
                mismatches = self.check_wardname_mismatches(csv_data, shapefile_data)
                quality_results['checks_performed'].append('ward_mismatches')
                
                if mismatches:
                    quality_results['issues_found'].append({
                        'type': 'ward_mismatches',
                        'mismatches': mismatches,
                        'count': len(mismatches)
                    })
            
            # Enhanced: Check 4 - Data consistency
            consistency_results = self.check_data_consistency(csv_data)
            quality_results['checks_performed'].append('data_consistency')
            if consistency_results['issues']:
                quality_results['issues_found'].extend(consistency_results['issues'])
            
            # Check 5: Data types and numeric ranges
            numeric_columns = csv_data.select_dtypes(include=['number']).columns
            quality_results['checks_performed'].append('data_types')
            
            for col in numeric_columns:
                if col.lower() != 'wardname':
                    # Check for extreme outliers (beyond 3 standard deviations)
                    mean_val = csv_data[col].mean()
                    std_val = csv_data[col].std()
                    
                    if std_val > 0:  # Avoid division by zero
                        outliers = csv_data[
                            (csv_data[col] - mean_val).abs() > 3 * std_val
                        ]
                        
                        if len(outliers) > len(csv_data) * 0.1:  # More than 10% outliers
                            quality_results['issues_found'].append({
                                'type': 'extreme_outliers',
                                'column': col,
                                'outlier_count': len(outliers),
                                'outlier_ratio': len(outliers) / len(csv_data)
                            })
            
            # Enhanced: Check 6 - Data validity ranges
            validity_results = self.validate_data_ranges(csv_data)
            quality_results['checks_performed'].append('data_validity')
            if validity_results['issues']:
                quality_results['issues_found'].extend(validity_results['issues'])
            
            # Check 7: Available variables for analysis
            available_vars = self.get_available_variables(csv_data)
            quality_results['checks_performed'].append('available_variables')
            quality_results['available_variables'] = available_vars
            
            if len(available_vars) < 2:
                quality_results['severe_issues'].append({
                    'type': 'insufficient_variables',
                    'count': len(available_vars),
                    'recommendation': 'Need at least 2 numeric variables for analysis'
                })
            
            # Enhanced: Generate intelligent recommendations based on quality scores
            quality_results['recommendations'] = self.generate_enhanced_recommendations(
                quality_results['quality_scores'], 
                quality_results['severe_issues'],
                quality_results['issues_found']
            )
            
            return quality_results
            
        except Exception as e:
            self.logger.error(f"Error during quality checks: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error during quality checks: {str(e)}',
                'checks_performed': quality_results['checks_performed']
            }
    
    def calculate_enhanced_quality_scores(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate comprehensive data quality scores
        
        Args:
            data: DataFrame to assess
            
        Returns:
            Dictionary with quality scores (0-1 scale, higher is better)
        """
        scores = {}
        
        try:
            # 1. Completeness Score (percentage of non-missing values)
            scores['completeness'] = self.calculate_completeness_score(data)
            
            # 2. Consistency Score (data format and type consistency)
            scores['consistency'] = self.calculate_consistency_score(data)
            
            # 3. Accuracy Score (estimated based on outliers and range validity)
            scores['accuracy'] = self.estimate_accuracy_score(data)
            
            # 4. Validity Score (values within expected ranges)
            scores['validity'] = self.calculate_validity_score(data)
            
            # 5. Overall Quality Score (weighted average)
            weights = {'completeness': 0.3, 'consistency': 0.2, 'accuracy': 0.3, 'validity': 0.2}
            scores['overall'] = sum(scores[metric] * weight for metric, weight in weights.items())
            
            # Add interpretation
            scores['interpretation'] = self.interpret_quality_score(scores['overall'])
            
        except Exception as e:
            self.logger.error(f"Error calculating quality scores: {str(e)}")
            scores = {'completeness': 0.0, 'consistency': 0.0, 'accuracy': 0.0, 'validity': 0.0, 'overall': 0.0}
        
        return scores

    def calculate_completeness_score(self, data: pd.DataFrame) -> float:
        """Calculate data completeness score (0-1, higher is better)"""
        if data.empty:
            return 0.0
        
        # Calculate overall completeness across all columns
        total_cells = data.shape[0] * data.shape[1]
        missing_cells = data.isna().sum().sum()
        
        return 1.0 - (missing_cells / total_cells)

    def calculate_consistency_score(self, data: pd.DataFrame) -> float:
        """Calculate data consistency score (0-1, higher is better)"""
        if data.empty:
            return 0.0
        
        consistency_factors = []
        
        # Check numeric columns for consistency
        numeric_cols = data.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if col.lower() != 'wardname':
                # Check for consistent data types (no mixed types that got coerced)
                non_null_data = data[col].dropna()
                if len(non_null_data) > 0:
                    # Check if all values are reasonable numbers (not infinity, not extreme)
                    reasonable_values = non_null_data[
                        (non_null_data != float('inf')) & 
                        (non_null_data != float('-inf')) & 
                        (abs(non_null_data) < 1e10)
                    ]
                    consistency_factors.append(len(reasonable_values) / len(non_null_data))
        
        return sum(consistency_factors) / len(consistency_factors) if consistency_factors else 1.0

    def estimate_accuracy_score(self, data: pd.DataFrame) -> float:
        """Estimate data accuracy based on outlier detection and distribution analysis"""
        if data.empty:
            return 0.0
        
        accuracy_factors = []
        numeric_cols = data.select_dtypes(include=['number']).columns
        
        for col in numeric_cols:
            if col.lower() != 'wardname':
                non_null_data = data[col].dropna()
                if len(non_null_data) > 3:  # Need minimum data for statistics
                    # Calculate outlier ratio (lower outlier ratio = higher accuracy)
                    q1, q3 = non_null_data.quantile([0.25, 0.75])
                    iqr = q3 - q1
                    if iqr > 0:
                        outlier_bounds = (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
                        outliers = non_null_data[
                            (non_null_data < outlier_bounds[0]) | 
                            (non_null_data > outlier_bounds[1])
                        ]
                        outlier_ratio = len(outliers) / len(non_null_data)
                        accuracy_factors.append(1.0 - min(outlier_ratio, 1.0))
                    else:
                        accuracy_factors.append(1.0)  # All values same = perfect accuracy
        
        return sum(accuracy_factors) / len(accuracy_factors) if accuracy_factors else 1.0

    def calculate_validity_score(self, data: pd.DataFrame) -> float:
        """Calculate validity score based on expected value ranges"""
        if data.empty:
            return 0.0
        
        validity_factors = []
        
        # Define expected ranges for common variables
        expected_ranges = {
            'temperature': (-50, 60),  # Celsius
            'temp': (-50, 60),
            'rainfall': (0, 5000),     # mm per year
            'precipitation': (0, 5000),
            'elevation': (-500, 9000), # meters
            'population': (0, float('inf')),
            'ndvi': (-1, 1),
            'evi': (-1, 1),
            'humidity': (0, 100),      # percentage
        }
        
        for col in data.columns:
            if col.lower() != 'wardname' and pd.api.types.is_numeric_dtype(data[col]):
                non_null_data = data[col].dropna()
                if len(non_null_data) > 0:
                    col_lower = col.lower()
                    
                    # Check against expected ranges
                    for var_name, (min_val, max_val) in expected_ranges.items():
                        if var_name in col_lower:
                            valid_values = non_null_data[
                                (non_null_data >= min_val) & 
                                (non_null_data <= max_val)
                            ]
                            validity_ratio = len(valid_values) / len(non_null_data)
                            validity_factors.append(validity_ratio)
                            break
                    else:
                        # For unknown variables, check for basic reasonableness
                        # (no negative values for likely positive variables)
                        if any(keyword in col_lower for keyword in ['count', 'population', 'distance', 'area']):
                            valid_values = non_null_data[non_null_data >= 0]
                            validity_ratio = len(valid_values) / len(non_null_data)
                            validity_factors.append(validity_ratio)
                        else:
                            validity_factors.append(1.0)  # Assume valid if unknown
        
        return sum(validity_factors) / len(validity_factors) if validity_factors else 1.0

    def interpret_quality_score(self, overall_score: float) -> str:
        """Interpret overall quality score"""
        if overall_score >= 0.9:
            return "Excellent"
        elif overall_score >= 0.8:
            return "Good" 
        elif overall_score >= 0.7:
            return "Fair"
        elif overall_score >= 0.6:
            return "Poor"
        else:
            return "Very Poor"

    def check_data_consistency(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Check for data consistency issues"""
        issues = []
        
        # Check for mixed data types in supposedly numeric columns
        for col in data.columns:
            if col.lower() != 'wardname':
                # Try to convert to numeric and see how many fail
                numeric_conversion = pd.to_numeric(data[col], errors='coerce')
                conversion_failures = numeric_conversion.isna().sum() - data[col].isna().sum()
                
                if conversion_failures > 0:
                    issues.append({
                        'type': 'mixed_data_types',
                        'column': col,
                        'conversion_failures': int(conversion_failures),
                        'recommendation': f'Column {col} contains non-numeric values that may need cleaning'
                    })
        
        return {'issues': issues}

    def validate_data_ranges(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate data ranges for known variable types"""
        issues = []
        
        range_checks = {
            'temperature': {'min': -50, 'max': 60, 'unit': '°C'},
            'temp': {'min': -50, 'max': 60, 'unit': '°C'},
            'rainfall': {'min': 0, 'max': 5000, 'unit': 'mm'},
            'precipitation': {'min': 0, 'max': 5000, 'unit': 'mm'},
            'humidity': {'min': 0, 'max': 100, 'unit': '%'},
            'ndvi': {'min': -1, 'max': 1, 'unit': 'index'},
            'evi': {'min': -1, 'max': 1, 'unit': 'index'},
        }
        
        for col in data.columns:
            if pd.api.types.is_numeric_dtype(data[col]):
                col_lower = col.lower()
                
                for var_name, ranges in range_checks.items():
                    if var_name in col_lower:
                        out_of_range = data[
                            (data[col] < ranges['min']) | 
                            (data[col] > ranges['max'])
                        ].dropna()
                        
                        if len(out_of_range) > 0:
                            issues.append({
                                'type': 'out_of_range_values',
                                'column': col,
                                'out_of_range_count': len(out_of_range),
                                'expected_range': f"{ranges['min']}-{ranges['max']} {ranges['unit']}",
                                'recommendation': f'Review {col} values outside expected range'
                            })
                        break
        
        return {'issues': issues}

    def generate_enhanced_recommendations(self, quality_scores: Dict[str, float], 
                                        severe_issues: List[Dict], 
                                        issues_found: List[Dict]) -> List[str]:
        """Generate intelligent recommendations based on quality analysis"""
        recommendations = []
        
        # Quality score based recommendations
        if quality_scores.get('overall', 0) >= 0.8:
            recommendations.append("✅ Data quality is good for analysis")
        elif quality_scores.get('overall', 0) >= 0.6:
            recommendations.append("⚠️ Data quality is fair - consider addressing identified issues")
        else:
            recommendations.append("❌ Data quality needs significant improvement before analysis")
        
        # Specific improvement recommendations
        if quality_scores.get('completeness', 1) < 0.8:
            recommendations.append("🔧 Improve data completeness by filling missing values or excluding incomplete variables")
        
        if quality_scores.get('consistency', 1) < 0.8:
            recommendations.append("🔧 Address data consistency issues (mixed types, formatting problems)")
        
        if quality_scores.get('accuracy', 1) < 0.8:
            recommendations.append("🔧 Review and clean outliers that may be data entry errors")
        
        if quality_scores.get('validity', 1) < 0.8:
            recommendations.append("🔧 Check for values outside expected ranges for your variables")
        
        # Issue-specific recommendations
        if severe_issues:
            recommendations.append("⚠️ Address severe issues before proceeding with analysis")
        
        # Add specific guidance for common issues
        missing_issues = [issue for issue in issues_found if issue['type'] == 'missing_values']
        if missing_issues:
            recommendations.append("💡 Consider using imputation methods for missing values")
        
        outlier_issues = [issue for issue in issues_found if issue['type'] == 'extreme_outliers']
        if outlier_issues:
            recommendations.append("💡 Review outliers - they may indicate data quality issues or legitimate extreme values")
        
        return recommendations
    
    def _is_id_column(self, column_name: str, df: pd.DataFrame) -> bool:
        """
        Check if a column appears to be an ID column
        
        Args:
            column_name: Column name to check
            df: DataFrame containing the column
            
        Returns:
            True if it seems to be an ID column
        """
        id_patterns = ['id', 'x.1', 'x', 'index', 'lga_code', 'wardid', 'ward_id', 'code']
        column_lower = column_name.lower()
        
        # Check if it matches common ID patterns
        for pattern in id_patterns:
            if pattern == column_lower or f"{pattern}_" in column_lower:
                return True
        
        # Also check if it's a non-numeric column with many unique values
        exists, resolved_col = variable_resolver.check_column_exists(column_name, list(df.columns))
        if exists:
            col = df[column_name]
            
            # If it's a string column with lots of unique values relative to row count
            if not pd.api.types.is_numeric_dtype(col):
                unique_ratio = col.nunique() / len(col) if len(col) > 0 else 0
                # If more than 90% of values are unique, likely an ID
                if unique_ratio > 0.9 and col.nunique() > 10:
                    return True
        
        return False


# Convenience functions for backward compatibility
def check_ward_mismatches(csv_data: pd.DataFrame, 
                         shapefile_data: Optional[gpd.GeoDataFrame],
                         interaction_logger=None) -> Optional[List[str]]:
    """
    Convenience function to check ward name mismatches
    
    Args:
        csv_data: CSV DataFrame
        shapefile_data: Shapefile GeoDataFrame  
        interaction_logger: Optional interaction logger
        
    Returns:
        List of mismatched ward names or None
    """
    validator = DataValidator(interaction_logger)
    return validator.check_wardname_mismatches(csv_data, shapefile_data)


def validate_variable_list(variables: List[str], csv_data: pd.DataFrame,
                          interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to validate variables against available data
    
    Args:
        variables: List of variables to validate
        csv_data: CSV DataFrame with available data
        interaction_logger: Optional interaction logger
        
    Returns:
        Validation results dictionary
    """
    validator = DataValidator(interaction_logger)
    available_vars = validator.get_available_variables(csv_data)
    return validator.validate_variables(variables, available_vars)


def run_quality_assessment(csv_data: pd.DataFrame, 
                          shapefile_data: Optional[gpd.GeoDataFrame] = None,
                          interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to run comprehensive data quality assessment
    
    Args:
        csv_data: CSV DataFrame to assess
        shapefile_data: Optional shapefile data
        interaction_logger: Optional interaction logger
        
    Returns:
        Quality assessment results
    """
    validator = DataValidator(interaction_logger)
    return validator.run_data_quality_checks(csv_data, shapefile_data)


# Package information
__version__ = "1.0.0"
__all__ = [
    'DataValidator',
    'check_ward_mismatches',
    'validate_variable_list',
    'run_quality_assessment'
] 