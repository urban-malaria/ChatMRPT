"""
Ward Data Tools for ChatMRPT - Phase 1 Implementation

This module provides tools for accessing and comparing ward-specific information.
All tools directly access the unified dataset for consistent, reliable results.

Tools included:
1. GetWardInformation - All data for a specific ward
2. GetWardVariable - Single variable value (e.g., TPR, elevation)
3. CompareWards - Compare multiple wards side-by-side
4. SearchWards - Find wards by any criteria
"""

import logging
from typing import Dict, Any, Optional, List
from pydantic import Field, validator
import pandas as pd
import numpy as np

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    ward_name_field, top_n_field, get_session_unified_dataset
)

logger = logging.getLogger(__name__)


def _smart_ward_match(gdf: pd.DataFrame, ward_name: str) -> pd.DataFrame:
    """
    Smart ward matching function - reusable across tools.
    Returns DataFrame with matching ward(s).
    """
    ward_matches = pd.DataFrame()
    
    # Strategy 1: Exact ward name match (case-insensitive)
    ward_matches = gdf[gdf['WardName'].str.lower() == ward_name.lower()]
    
    if ward_matches.empty:
        # Strategy 2: Partial ward name match
        ward_matches = gdf[gdf['WardName'].str.lower().str.startswith(ward_name.lower())]
    
    if ward_matches.empty:
        # Strategy 3: Check if input is a ward code
        for code_col in ['WardCode_x', 'WardCode_y', 'WardCode']:
            if code_col in gdf.columns:
                code_matches = gdf[gdf[code_col].str.lower() == ward_name.lower()]
                if not code_matches.empty:
                    ward_matches = code_matches
                    break
    
    if ward_matches.empty:
        # Strategy 4: Smart fuzzy matching
        fuzzy_matches = gdf[
            gdf['WardName'].str.lower().str.contains(ward_name.lower(), na=False, regex=False) |
            gdf['WardName'].str.lower().str.contains(ward_name.lower().split()[0], na=False, regex=False)
        ]
        if not fuzzy_matches.empty:
            ward_matches = fuzzy_matches
    
    # If multiple matches, prefer exact matches, then by data completeness
    if len(ward_matches) > 1:
        exact_matches = ward_matches[ward_matches['WardName'].str.lower() == ward_name.lower()]
        if not exact_matches.empty:
            ward_matches = exact_matches
        
        if len(ward_matches) > 1:
            ward_matches['completeness'] = ward_matches.count(axis=1)
            ward_matches = ward_matches.nlargest(1, 'completeness')
    
    return ward_matches


class GetWardInformation(BaseTool):
    """
    Get comprehensive information for a specific ward.
    
    Returns all available data for the specified ward including
    demographics, risk scores, environmental factors, and settlement data.
    """
    
    ward_name: str = ward_name_field(description="Name or code of the ward to get information for")
    
    include_raw_data: bool = Field(
        False,
        description="Include all raw variables in the output"
    )
    
    format_output: str = Field(
        "structured",
        description="Output format: 'structured' or 'detailed'",
        pattern="^(structured|detailed)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Tell me about Bagwai ward",
            "Get all information for Kano Municipal",
            "What data do we have for ward KN3807?",
            "Show me detailed information about Dala ward"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get comprehensive ward information"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Find ward using smart matching
            ward_matches = _smart_ward_match(gdf, self.ward_name)
            
            if ward_matches.empty:
                # Generate helpful error with similar ward names
                similar_wards = gdf[gdf['WardName'].str.lower().str.contains(
                    self.ward_name.lower()[:3], na=False, regex=False
                )]['WardName'].head(5).tolist()
                
                error_msg = f"Ward '{self.ward_name}' not found in dataset."
                if similar_wards:
                    error_msg += f" Similar wards: {', '.join(similar_wards)}"
                
                return self._create_error_result(error_msg)
            
            # Get ward data
            ward_data = ward_matches.iloc[0]
            ward_full_name = ward_data['WardName']
            
            # Organize data into categories
            result_data = {
                'ward_name': ward_full_name,
                'ward_code': ward_data.get('WardCode_x', ward_data.get('WardCode_y', ward_data.get('WardCode', 'N/A'))),
                'basic_info': {},
                'risk_assessment': {},
                'environmental_factors': {},
                'settlement_data': {},
                'health_indicators': {}
            }
            
            # Basic information
            basic_fields = ['StateCode_x', 'LGACode_x', 'Urban_x', 'UrbanPerce']
            for field in basic_fields:
                if field in ward_data and pd.notna(ward_data[field]):
                    clean_name = field.replace('_x', '').replace('_y', '')
                    result_data['basic_info'][clean_name] = ward_data[field]
            
            # Risk assessment data
            risk_fields = ['composite_score', 'composite_rank', 'vulnerability_category', 
                          'pca_score', 'pca_rank', 'overall_rank']
            for field in risk_fields:
                if field in ward_data and pd.notna(ward_data[field]):
                    if 'score' in field or 'rank' in field:
                        result_data['risk_assessment'][field] = float(ward_data[field])
                    else:
                        result_data['risk_assessment'][field] = str(ward_data[field])
            
            # Environmental factors
            env_fields = ['mean_rainfall', 'distance_to_water', 'temp_mean', 'elevation', 
                         'flood', 'mean_EVI', 'mean_NDVI', 'RH_mean']
            for field in env_fields:
                if field in ward_data and pd.notna(ward_data[field]):
                    result_data['environmental_factors'][field] = float(ward_data[field])
            
            # Settlement data
            settlement_fields = ['settlement_type', 'totalArea', 'urbanArea', 'urbanPercentage', 
                               'building_height', 'housing_quality']
            for field in settlement_fields:
                if field in ward_data and pd.notna(ward_data[field]):
                    if field in ['totalArea', 'urbanArea', 'urbanPercentage', 'building_height', 'housing_quality']:
                        result_data['settlement_data'][field] = float(ward_data[field])
                    else:
                        result_data['settlement_data'][field] = str(ward_data[field])
            
            # Health indicators
            health_fields = ['pfpr', 'u5_tpr_rdt']
            for field in health_fields:
                if field in ward_data and pd.notna(ward_data[field]):
                    result_data['health_indicators'][field] = float(ward_data[field])
            
            # Include raw data if requested
            if self.include_raw_data:
                raw_data = {}
                for col in gdf.columns:
                    if col != 'geometry' and pd.notna(ward_data[col]):
                        try:
                            if isinstance(ward_data[col], (int, float)):
                                raw_data[col] = float(ward_data[col])
                            else:
                                raw_data[col] = str(ward_data[col])
                        except:
                            raw_data[col] = str(ward_data[col])
                result_data['raw_data'] = raw_data
            
            # Generate summary message
            risk_info = ""
            if 'composite_score' in result_data['risk_assessment']:
                score = result_data['risk_assessment']['composite_score']
                rank = result_data['risk_assessment'].get('composite_rank', 'Unknown')
                risk_info = f"Risk score: {score:.3f} (Rank: {rank})"
            
            if 'vulnerability_category' in result_data['risk_assessment']:
                risk_info += f", {result_data['risk_assessment']['vulnerability_category']}"
            
            message = f"Ward information retrieved for {ward_full_name}"
            if risk_info:
                message += f" - {risk_info}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error getting ward information: {e}")
            return self._create_error_result(f"Error retrieving ward information: {str(e)}")


class GetWardVariable(BaseTool):
    """
    Get a specific variable value for a ward.
    
    Returns the value of a single variable (e.g., TPR, elevation, rainfall)
    for the specified ward.
    """
    
    ward_name: str = ward_name_field(description="Name or code of the ward")
    
    variable_name: str = Field(
        ...,
        description="Name of the variable to retrieve (e.g., 'pfpr', 'elevation', 'mean_rainfall')",
        min_length=1
    )
    
    include_context: bool = Field(
        True,
        description="Include contextual information about the variable"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What is the TPR in Bagwai ward?",
            "Get elevation for Kano Municipal",
            "What's the rainfall in Dala ward?",
            "Show me the flood risk for ward KN3807"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get specific variable value for ward"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Find ward using smart matching
            ward_matches = _smart_ward_match(gdf, self.ward_name)
            
            if ward_matches.empty:
                return self._create_error_result(f"Ward '{self.ward_name}' not found in dataset.")
            
            ward_data = ward_matches.iloc[0]
            ward_full_name = ward_data['WardName']
            
            # Find the variable (case-insensitive, flexible matching)
            variable_found = None
            variable_value = None
            
            # Direct match
            if self.variable_name in gdf.columns:
                variable_found = self.variable_name
                variable_value = ward_data[self.variable_name]
            else:
                # Case-insensitive match
                for col in gdf.columns:
                    if col.lower() == self.variable_name.lower():
                        variable_found = col
                        variable_value = ward_data[col]
                        break
                
                # Partial match if still not found
                if variable_found is None:
                    for col in gdf.columns:
                        if self.variable_name.lower() in col.lower() or col.lower() in self.variable_name.lower():
                            variable_found = col
                            variable_value = ward_data[col]
                            break
            
            if variable_found is None:
                # Suggest similar variables
                similar_vars = [col for col in gdf.columns if any(
                    term in col.lower() for term in self.variable_name.lower().split()
                )][:5]
                
                error_msg = f"Variable '{self.variable_name}' not found."
                if similar_vars:
                    error_msg += f" Similar variables: {', '.join(similar_vars)}"
                else:
                    error_msg += f" Available variables: {', '.join(list(gdf.columns)[:10])}..."
                
                return self._create_error_result(error_msg)
            
            # Prepare result
            result_data = {
                'ward_name': ward_full_name,
                'variable_name': variable_found,
                'value': variable_value,
                'data_type': str(type(variable_value).__name__)
            }
            
            # Format value appropriately
            if pd.isna(variable_value):
                result_data['value'] = None
                result_data['status'] = 'missing'
            elif isinstance(variable_value, (int, float)):
                result_data['value'] = float(variable_value)
                result_data['status'] = 'available'
            else:
                result_data['value'] = str(variable_value)
                result_data['status'] = 'available'
            
            # Add context if requested
            if self.include_context and result_data['status'] == 'available':
                # Get variable statistics across all wards
                var_series = gdf[variable_found].dropna()
                if len(var_series) > 0 and isinstance(result_data['value'], (int, float)):
                    result_data['context'] = {
                        'dataset_mean': float(var_series.mean()),
                        'dataset_median': float(var_series.median()),
                        'dataset_std': float(var_series.std()),
                        'dataset_min': float(var_series.min()),
                        'dataset_max': float(var_series.max()),
                        'ward_percentile': float((var_series <= result_data['value']).mean() * 100)
                    }
            
            # Generate message
            if result_data['status'] == 'missing':
                message = f"{variable_found} data not available for {ward_full_name}"
            else:
                if isinstance(result_data['value'], float):
                    message = f"{ward_full_name}: {variable_found} = {result_data['value']:.3f}"
                else:
                    message = f"{ward_full_name}: {variable_found} = {result_data['value']}"
                
                if 'context' in result_data:
                    percentile = result_data['context']['ward_percentile']
                    message += f" ({percentile:.1f}th percentile)"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error getting ward variable: {e}")
            return self._create_error_result(f"Error retrieving variable: {str(e)}")


class CompareWards(BaseTool):
    """
    Compare multiple wards side-by-side.
    
    Returns a comparison of key metrics across the specified wards,
    highlighting similarities and differences.
    """
    
    ward_names: List[str] = Field(
        ...,
        description="List of ward names or codes to compare",
        min_items=2,
        max_items=10
    )
    
    comparison_variables: Optional[List[str]] = Field(
        None,
        description="Specific variables to compare (if None, uses key indicators)"
    )
    
    include_rankings: bool = Field(
        True,
        description="Include risk rankings and scores in comparison"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Compare Bagwai and Kano Municipal wards",
            "Compare risk factors between Dala, Fagge, and Nassarawa wards",
            "Show differences between ward KN3807 and KN3912",
            "Compare settlement types across 3 wards"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Compare multiple wards"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Find all wards
            ward_data_list = []
            ward_names_found = []
            ward_names_not_found = []
            
            for ward_name in self.ward_names:
                ward_matches = _smart_ward_match(gdf, ward_name)
                if not ward_matches.empty:
                    ward_data_list.append(ward_matches.iloc[0])
                    ward_names_found.append(ward_matches.iloc[0]['WardName'])
                else:
                    ward_names_not_found.append(ward_name)
            
            if len(ward_data_list) < 2:
                error_msg = f"Need at least 2 wards for comparison. "
                if ward_names_not_found:
                    error_msg += f"Wards not found: {', '.join(ward_names_not_found)}"
                return self._create_error_result(error_msg)
            
            # Default comparison variables if none specified
            if self.comparison_variables is None:
                comparison_vars = [
                    'composite_score', 'vulnerability_category', 'pfpr', 'u5_tpr_rdt',
                    'mean_rainfall', 'distance_to_water', 'elevation', 'settlement_type',
                    'urbanPercentage', 'housing_quality'
                ]
            else:
                comparison_vars = self.comparison_variables
            
            # Build comparison data
            comparison_data = {
                'wards_compared': ward_names_found,
                'wards_not_found': ward_names_not_found,
                'comparison': {}
            }
            
            for var in comparison_vars:
                if var in gdf.columns:
                    var_data = {}
                    for i, ward_data in enumerate(ward_data_list):
                        ward_name = ward_names_found[i]
                        value = ward_data.get(var)
                        
                        if pd.notna(value):
                            if isinstance(value, (int, float)):
                                var_data[ward_name] = float(value)
                            else:
                                var_data[ward_name] = str(value)
                        else:
                            var_data[ward_name] = None
                    
                    if var_data:  # Only include if at least one ward has data
                        comparison_data['comparison'][var] = var_data
            
            # Add statistical summary for numeric variables
            comparison_data['summary'] = {}
            for var, ward_values in comparison_data['comparison'].items():
                numeric_values = [v for v in ward_values.values() if isinstance(v, (int, float))]
                if len(numeric_values) > 1:
                    comparison_data['summary'][var] = {
                        'min_value': min(numeric_values),
                        'max_value': max(numeric_values),
                        'range': max(numeric_values) - min(numeric_values),
                        'mean': sum(numeric_values) / len(numeric_values)
                    }
                    
                    # Identify which ward has min/max
                    for ward_name, value in ward_values.items():
                        if value == min(numeric_values):
                            comparison_data['summary'][var]['lowest_ward'] = ward_name
                        if value == max(numeric_values):
                            comparison_data['summary'][var]['highest_ward'] = ward_name
            
            # Generate summary message
            num_wards = len(ward_names_found)
            message = f"Comparison completed for {num_wards} wards: {', '.join(ward_names_found)}"
            
            if ward_names_not_found:
                message += f" (Note: {len(ward_names_not_found)} wards not found)"
            
            return self._create_success_result(
                message=message,
                data=comparison_data
            )
            
        except Exception as e:
            logger.error(f"Error comparing wards: {e}")
            return self._create_error_result(f"Error comparing wards: {str(e)}")


class SearchWards(BaseTool):
    """
    Search for wards based on specific criteria.
    
    Find wards that match specified conditions such as variable ranges,
    settlement types, or other characteristics.
    """
    
    search_criteria: Dict[str, Any] = Field(
        ...,
        description="Search criteria as key-value pairs (e.g., {'settlement_type': 'urban', 'pfpr': {'min': 0.2, 'max': 0.5}})"
    )
    
    max_results: int = Field(
        50,
        description="Maximum number of results to return",
        ge=1,
        le=200
    )
    
    sort_by: Optional[str] = Field(
        None,
        description="Variable to sort results by"
    )
    
    sort_ascending: bool = Field(
        True,
        description="Sort in ascending order (False for descending)"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Find urban wards with high rainfall",
            "Search for wards with TPR between 0.2 and 0.5",
            "Which wards have informal settlements?",
            "Find low-elevation wards near water"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Search wards by criteria"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Start with all wards
            filtered_gdf = gdf.copy()
            applied_filters = []
            
            # Apply each search criterion
            for field, criteria in self.search_criteria.items():
                if field not in gdf.columns:
                    return self._create_error_result(f"Field '{field}' not found in dataset.")
                
                if isinstance(criteria, dict):
                    # Range or complex criteria
                    if 'min' in criteria or 'max' in criteria:
                        min_val = criteria.get('min', float('-inf'))
                        max_val = criteria.get('max', float('inf'))
                        filtered_gdf = filtered_gdf[
                            (filtered_gdf[field] >= min_val) & 
                            (filtered_gdf[field] <= max_val)
                        ]
                        applied_filters.append(f"{field}: {min_val} to {max_val}")
                    elif 'contains' in criteria:
                        # String contains
                        filtered_gdf = filtered_gdf[
                            filtered_gdf[field].str.contains(criteria['contains'], case=False, na=False)
                        ]
                        applied_filters.append(f"{field} contains '{criteria['contains']}'")
                else:
                    # Direct value match
                    if isinstance(criteria, str):
                        # Case-insensitive string match
                        filtered_gdf = filtered_gdf[
                            filtered_gdf[field].str.lower() == criteria.lower()
                        ]
                    else:
                        filtered_gdf = filtered_gdf[filtered_gdf[field] == criteria]
                    applied_filters.append(f"{field} = {criteria}")
            
            # Sort results if requested
            if self.sort_by and self.sort_by in filtered_gdf.columns:
                filtered_gdf = filtered_gdf.sort_values(self.sort_by, ascending=self.sort_ascending)
                applied_filters.append(f"sorted by {self.sort_by} {'asc' if self.sort_ascending else 'desc'}")
            
            # Limit results
            result_wards = filtered_gdf.head(self.max_results)
            
            # Prepare results
            results = []
            for _, ward in result_wards.iterrows():
                ward_info = {
                    'ward_name': ward['WardName'],
                    'ward_code': ward.get('WardCode_x', ward.get('WardCode_y', 'N/A'))
                }
                
                # Add the searched fields to results
                for field in self.search_criteria.keys():
                    if field in ward and pd.notna(ward[field]):
                        if isinstance(ward[field], (int, float)):
                            ward_info[field] = float(ward[field])
                        else:
                            ward_info[field] = str(ward[field])
                
                # Add risk info if available
                if 'composite_score' in ward and pd.notna(ward['composite_score']):
                    ward_info['composite_score'] = float(ward['composite_score'])
                if 'vulnerability_category' in ward and pd.notna(ward['vulnerability_category']):
                    ward_info['risk_level'] = str(ward['vulnerability_category'])
                
                results.append(ward_info)
            
            # Generate message
            total_found = len(filtered_gdf)
            returned_count = len(results)
            
            message = f"Found {total_found} wards matching criteria"
            if returned_count < total_found:
                message += f" (showing first {returned_count})"
            
            if applied_filters:
                message += f". Filters: {'; '.join(applied_filters)}"
            
            return self._create_success_result(
                message=message,
                data={
                    'total_found': total_found,
                    'returned_count': returned_count,
                    'search_criteria': self.search_criteria,
                    'applied_filters': applied_filters,
                    'results': results
                }
            )
            
        except Exception as e:
            logger.error(f"Error searching wards: {e}")
            return self._create_error_result(f"Error searching wards: {str(e)}")


# Register tools for discovery
__all__ = [
    'GetWardInformation',
    'GetWardVariable',
    'CompareWards',
    'SearchWards'
]