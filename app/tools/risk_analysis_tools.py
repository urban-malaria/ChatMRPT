"""
Risk Analysis Tools for ChatMRPT - Phase 1 Implementation

This module provides tools for malaria risk assessment and burden stratification.
All tools directly access the unified dataset for consistent, reliable results.

Tools included:
1. GetWardRiskScore - Get composite/PCA score for any ward
2. GetTopRiskWards - Rank N wards by risk (composite or PCA)
3. FilterWardsByRiskLevel - Get high/medium/low risk wards
4. GetRiskStatistics - Distribution, percentiles, thresholds
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


class GetWardRiskScore(BaseTool):
    """
    Get risk score for a specific ward.
    
    Returns composite score, PCA score (if available), risk level,
    and ranking information for the specified ward.
    """
    
    ward_name: str = ward_name_field(description="Name of the ward to get risk score for")
    
    score_type: str = Field(
        "both",
        description="Type of score to return: 'composite', 'pca', or 'both'",
        pattern="^(composite|pca|both)$"
    )
    
    include_details: bool = Field(
        True,
        description="Include detailed breakdown of risk factors"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What is the risk level of Bagwai ward?",
            "Get risk score for Kano Municipal",
            "Show me Dala ward's malaria risk",
            "What's the composite score for Fagge ward?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get risk score for specified ward"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Smart ward matching strategy
            ward_matches = pd.DataFrame()
            
            # Strategy 1: Exact ward name match (case-insensitive)
            ward_matches = gdf[gdf['WardName'].str.lower() == self.ward_name.lower()]
            
            if ward_matches.empty:
                # Strategy 2: Partial ward name match (for names like "Tarauni" vs "Tarauni (KN3807)")
                ward_matches = gdf[gdf['WardName'].str.lower().str.startswith(self.ward_name.lower())]
            
            if ward_matches.empty:
                # Strategy 3: Check if input is a ward code
                for code_col in ['WardCode_x', 'WardCode_y', 'WardCode']:
                    if code_col in gdf.columns:
                        code_matches = gdf[gdf[code_col].str.lower() == self.ward_name.lower()]
                        if not code_matches.empty:
                            ward_matches = code_matches
                            break
            
            if ward_matches.empty:
                # Strategy 4: Fuzzy matching - ward name contains input or input contains ward name
                fuzzy_matches = gdf[
                    gdf['WardName'].str.lower().str.contains(self.ward_name.lower(), na=False, regex=False) |
                    gdf['WardName'].str.lower().str.contains(self.ward_name.lower().split()[0], na=False, regex=False)
                ]
                if not fuzzy_matches.empty:
                    ward_matches = fuzzy_matches
            
            if ward_matches.empty:
                # Generate helpful error with similar ward names
                similar_wards = gdf[gdf['WardName'].str.lower().str.contains(
                    self.ward_name.lower()[:3], na=False, regex=False
                )]['WardName'].head(5).tolist()
                
                error_msg = f"Ward '{self.ward_name}' not found in dataset."
                if similar_wards:
                    error_msg += f" Similar wards: {', '.join(similar_wards)}"
                else:
                    error_msg += f" Available wards: {', '.join(gdf['WardName'].head(10).tolist())}"
                
                return self._create_error_result(error_msg)
            
            # If multiple matches, prefer exact matches, then prioritize by completeness of data
            if len(ward_matches) > 1:
                # First try exact name match
                exact_matches = ward_matches[ward_matches['WardName'].str.lower() == self.ward_name.lower()]
                if not exact_matches.empty:
                    ward_matches = exact_matches
                
                # If still multiple, pick the one with most complete data (non-null values)
                if len(ward_matches) > 1:
                    ward_matches['completeness'] = ward_matches.count(axis=1)
                    ward_matches = ward_matches.nlargest(1, 'completeness')
            
            # Get first match
            ward_data = ward_matches.iloc[0]
            ward_full_name = ward_data['WardName']
            
            result_data = {
                'ward_name': ward_full_name,
                'ward_code': ward_data.get('WardCode_x', ward_data.get('WardCode_y', ward_data.get('WardCode', 'N/A')))
            }
            
            # Get composite score if available
            if 'composite_score' in gdf.columns and self.score_type in ['composite', 'both']:
                composite_score = ward_data.get('composite_score', None)
                if pd.notna(composite_score):
                    result_data['composite_score'] = float(composite_score)
                    
                    # Calculate rank
                    gdf_sorted = gdf.sort_values('composite_score', ascending=False)
                    rank = (gdf_sorted['WardName'] == ward_full_name).idxmax() + 1
                    result_data['composite_rank'] = int(rank)
                    result_data['total_wards'] = len(gdf)
            
            # Get PCA score if available
            if 'pca_score' in gdf.columns and self.score_type in ['pca', 'both']:
                pca_score = ward_data.get('pca_score', None)
                if pd.notna(pca_score):
                    result_data['pca_score'] = float(pca_score)
                    
                    # Calculate PCA rank
                    gdf_sorted = gdf.sort_values('pca_score', ascending=False)
                    rank = (gdf_sorted['WardName'] == ward_full_name).idxmax() + 1
                    result_data['pca_rank'] = int(rank)
            
            # Get risk level
            if 'vulnerability_category' in gdf.columns:
                result_data['risk_level'] = ward_data.get('vulnerability_category', 'Unknown')
            elif 'composite_category' in gdf.columns:
                result_data['risk_level'] = ward_data.get('composite_category', 'Unknown')
            elif 'pca_category' in gdf.columns:
                result_data['risk_level'] = ward_data.get('pca_category', 'Unknown')
            
            # Add details if requested
            if self.include_details:
                details = {}
                
                # Key risk indicators
                risk_indicators = ['tpr', 'pfpr', 'mean_rainfall', 'distance_to_water', 
                                 'elevation', 'temperature', 'flood_risk']
                
                for indicator in risk_indicators:
                    if indicator in gdf.columns:
                        value = ward_data.get(indicator)
                        if pd.notna(value):
                            details[indicator] = float(value) if isinstance(value, (int, float)) else str(value)
                
                if details:
                    result_data['risk_factors'] = details
            
            # Create appropriate message
            if 'composite_score' in result_data:
                message = f"{ward_full_name}: Risk score {result_data['composite_score']:.3f} "
                message += f"(Rank {result_data['composite_rank']}/{result_data['total_wards']})"
                if 'risk_level' in result_data:
                    message += f", {result_data['risk_level']} risk"
            else:
                message = f"Risk information retrieved for {ward_full_name}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error getting ward risk score: {e}")
            return self._create_error_result(f"Error retrieving risk score: {str(e)}")


class GetTopRiskWards(BaseTool):
    """
    Get the top N wards ranked by malaria risk.
    
    Returns a ranked list of wards with their risk scores,
    supporting both composite and PCA scoring methods.
    """
    
    top_n: int = top_n_field(
        default=10,
        description="Number of top risk wards to return"
    )
    
    ranking_method: str = Field(
        "composite",
        description="Ranking method: 'composite' or 'pca'",
        pattern="^(composite|pca)$"
    )
    
    include_scores: bool = Field(
        True,
        description="Include risk scores in the output"
    )
    
    location_filter: Optional[str] = Field(
        None,
        description="Optional location filter (e.g., LGA name)"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me the top 10 most vulnerable wards",
            "Which wards have the highest composite risk scores?",
            "Rank the top 5 wards by malaria risk",
            "List the 20 highest risk areas"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get top risk wards"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Check for required columns
            score_column = 'composite_score' if self.ranking_method == 'composite' else 'pca_score'
            if score_column not in gdf.columns:
                return self._create_error_result(
                    f"No {self.ranking_method} scores found. Please run {self.ranking_method} analysis first."
                )
            
            # Apply location filter if provided
            filtered_gdf = gdf
            if self.location_filter:
                # Try to filter by LGA or other location identifiers
                location_matches = gdf[
                    gdf['LGA'].str.contains(self.location_filter, case=False, na=False) |
                    gdf['WardName'].str.contains(self.location_filter, case=False, na=False)
                ]
                if not location_matches.empty:
                    filtered_gdf = location_matches
                else:
                    return self._create_error_result(
                        f"No wards found for location: {self.location_filter}"
                    )
            
            # Sort by score (higher is more vulnerable)
            sorted_gdf = filtered_gdf.sort_values(score_column, ascending=False)
            
            # Get top N
            top_wards = sorted_gdf.head(self.top_n)
            
            # Prepare results
            results = []
            for idx, (_, ward) in enumerate(top_wards.iterrows(), 1):
                ward_info = {
                    'rank': idx,
                    'ward_name': ward['WardName'],
                    'ward_code': ward.get('WardCode', 'N/A')
                }
                
                if self.include_scores:
                    ward_info[f'{self.ranking_method}_score'] = float(ward[score_column])
                    
                # Add risk level if available
                if 'risk_level' in ward:
                    ward_info['risk_level'] = ward['risk_level']
                elif 'vulnerability_category' in ward:
                    ward_info['risk_level'] = ward['vulnerability_category']
                
                # Add key indicators
                if 'tpr' in ward and pd.notna(ward['tpr']):
                    ward_info['tpr'] = float(ward['tpr'])
                if 'pfpr' in ward and pd.notna(ward['pfpr']):
                    ward_info['pfpr'] = float(ward['pfpr'])
                
                results.append(ward_info)
            
            # Create summary message
            if results:
                top_ward = results[0]
                message = f"Top {len(results)} high-risk wards identified. "
                message += f"Highest risk: {top_ward['ward_name']}"
                if self.include_scores:
                    message += f" (score: {top_ward[f'{self.ranking_method}_score']:.3f})"
            else:
                message = "No wards found matching criteria"
            
            return self._create_success_result(
                message=message,
                data={
                    'ranking_method': self.ranking_method,
                    'total_wards_analyzed': len(filtered_gdf),
                    'top_wards': results,
                    'location_filter': self.location_filter
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting top risk wards: {e}")
            return self._create_error_result(f"Error retrieving top wards: {str(e)}")


class FilterWardsByRiskLevel(BaseTool):
    """
    Filter wards by their risk level classification.
    
    Returns all wards that match the specified risk level
    (High Risk, Medium Risk, Low Risk).
    """
    
    risk_level: str = Field(
        ...,
        description="Risk level to filter by: 'high', 'medium', or 'low'",
        pattern="^(high|medium|low)$"
    )
    
    include_details: bool = Field(
        False,
        description="Include detailed information for each ward"
    )
    
    sort_by_score: bool = Field(
        True,
        description="Sort results by risk score within the category"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me all high risk wards",
            "Which wards are classified as low risk?",
            "List medium risk areas",
            "How many wards are high risk?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Filter wards by risk level"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            # Find risk level column
            risk_column = None
            if 'risk_level' in gdf.columns:
                risk_column = 'risk_level'
            elif 'vulnerability_category' in gdf.columns:
                risk_column = 'vulnerability_category'
            else:
                return self._create_error_result(
                    "No risk level classification found. Please run vulnerability analysis first."
                )
            
            # Normalize risk level for matching
            risk_level_map = {
                'high': ['High Risk', 'High', 'high', 'HIGH', 'High risk'],
                'medium': ['Medium Risk', 'Medium', 'medium', 'MEDIUM', 'Medium risk'],
                'low': ['Low Risk', 'Low', 'low', 'LOW', 'Low risk']
            }
            
            # Filter by risk level
            matching_terms = risk_level_map.get(self.risk_level.lower(), [])
            filtered_gdf = gdf[gdf[risk_column].isin(matching_terms)]
            
            if filtered_gdf.empty:
                return self._create_error_result(
                    f"No wards found with {self.risk_level} risk level. "
                    f"Available levels: {gdf[risk_column].unique().tolist()}"
                )
            
            # Sort by score if requested and available
            if self.sort_by_score and 'composite_score' in filtered_gdf.columns:
                filtered_gdf = filtered_gdf.sort_values('composite_score', ascending=False)
            
            # Prepare results
            results = []
            for _, ward in filtered_gdf.iterrows():
                ward_info = {
                    'ward_name': ward['WardName'],
                    'ward_code': ward.get('WardCode', 'N/A'),
                    'risk_level': ward[risk_column]
                }
                
                if self.include_details:
                    # Add scores if available
                    if 'composite_score' in ward and pd.notna(ward['composite_score']):
                        ward_info['composite_score'] = float(ward['composite_score'])
                    if 'pca_score' in ward and pd.notna(ward['pca_score']):
                        ward_info['pca_score'] = float(ward['pca_score'])
                    
                    # Add key indicators
                    if 'tpr' in ward and pd.notna(ward['tpr']):
                        ward_info['tpr'] = float(ward['tpr'])
                    if 'pfpr' in ward and pd.notna(ward['pfpr']):
                        ward_info['pfpr'] = float(ward['pfpr'])
                
                results.append(ward_info)
            
            # Create summary
            total_wards = len(gdf)
            filtered_count = len(results)
            percentage = (filtered_count / total_wards * 100) if total_wards > 0 else 0
            
            message = f"Found {filtered_count} {self.risk_level} risk wards "
            message += f"({percentage:.1f}% of {total_wards} total wards)"
            
            return self._create_success_result(
                message=message,
                data={
                    'risk_level': self.risk_level,
                    'ward_count': filtered_count,
                    'total_wards': total_wards,
                    'percentage': round(percentage, 2),
                    'wards': results[:50] if not self.include_details else results  # Limit if not detailed
                }
            )
            
        except Exception as e:
            logger.error(f"Error filtering wards by risk level: {e}")
            return self._create_error_result(f"Error filtering wards: {str(e)}")


class GetRiskStatistics(BaseTool):
    """
    Get statistical summary of risk distribution across all wards.
    
    Provides distribution of risk levels, percentiles, thresholds,
    and summary statistics for risk scores.
    """
    
    include_percentiles: bool = Field(
        True,
        description="Include percentile breakdown of risk scores"
    )
    
    include_distribution: bool = Field(
        True,
        description="Include risk level distribution counts"
    )
    
    score_type: str = Field(
        "composite",
        description="Score type to analyze: 'composite' or 'pca'",
        pattern="^(composite|pca)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What's the risk distribution across all wards?",
            "Show me risk statistics",
            "How many wards are in each risk category?",
            "What are the risk score percentiles?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get risk statistics"""
        try:
            # Load unified dataset
            gdf = get_session_unified_dataset(session_id)
            if gdf is None:
                return self._create_error_result("No data available. Please upload data first.")
            
            result_data = {
                'total_wards': len(gdf),
                'analysis_type': self.score_type
            }
            
            # Get score column
            score_column = f'{self.score_type}_score'
            if score_column in gdf.columns:
                scores = gdf[score_column].dropna()
                
                if len(scores) > 0:
                    # Basic statistics
                    result_data['score_statistics'] = {
                        'mean': float(scores.mean()),
                        'median': float(scores.median()),
                        'std_dev': float(scores.std()),
                        'min': float(scores.min()),
                        'max': float(scores.max()),
                        'range': float(scores.max() - scores.min())
                    }
                    
                    # Percentiles
                    if self.include_percentiles:
                        percentiles = [10, 25, 50, 75, 90, 95]
                        result_data['percentiles'] = {
                            f'p{p}': float(scores.quantile(p/100))
                            for p in percentiles
                        }
                        
                        # Risk thresholds (typically 33rd and 67th percentiles)
                        result_data['risk_thresholds'] = {
                            'low_medium': float(scores.quantile(0.33)),
                            'medium_high': float(scores.quantile(0.67))
                        }
            
            # Risk level distribution
            if self.include_distribution:
                risk_column = 'risk_level' if 'risk_level' in gdf.columns else 'vulnerability_category'
                
                if risk_column in gdf.columns:
                    distribution = gdf[risk_column].value_counts()
                    total = len(gdf)
                    
                    result_data['risk_distribution'] = {
                        'counts': distribution.to_dict(),
                        'percentages': {
                            level: round(count / total * 100, 2)
                            for level, count in distribution.items()
                        }
                    }
                    
                    # Simplified summary
                    high_risk = sum(count for level, count in distribution.items() 
                                  if 'high' in level.lower())
                    medium_risk = sum(count for level, count in distribution.items() 
                                    if 'medium' in level.lower())
                    low_risk = sum(count for level, count in distribution.items() 
                                 if 'low' in level.lower())
                    
                    result_data['risk_summary'] = {
                        'high_risk_wards': high_risk,
                        'medium_risk_wards': medium_risk,
                        'low_risk_wards': low_risk
                    }
            
            # Generate summary message
            if 'risk_summary' in result_data:
                summary = result_data['risk_summary']
                message = f"Risk analysis complete: "
                message += f"{summary['high_risk_wards']} high risk, "
                message += f"{summary['medium_risk_wards']} medium risk, "
                message += f"{summary['low_risk_wards']} low risk wards"
            else:
                message = f"Risk statistics calculated for {len(gdf)} wards"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error calculating risk statistics: {e}")
            return self._create_error_result(f"Error calculating statistics: {str(e)}")


# Register tools for discovery
__all__ = [
    'GetWardRiskScore',
    'GetTopRiskWards', 
    'FilterWardsByRiskLevel',
    'GetRiskStatistics'
]