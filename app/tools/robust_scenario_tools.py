"""
Robust Scenario Simulation Tools - Enhanced for Any Dataset

These enhanced tools adapt to different data structures, distributions, and quality levels:
- Dynamic threshold adaptation based on data distribution
- Flexible column detection and mapping  
- Smart fallbacks for missing data
- Graceful degradation with informative messages
- Data-driven parameter adjustment
"""

import logging
from typing import Dict, Any, Optional, List, Union, Tuple
from pydantic import Field, validator
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)

logger = logging.getLogger(__name__)


class DataAdaptiveUtils:
    """Utility class for making tools adaptive to different datasets."""
    
    @staticmethod
    def find_risk_column(df: pd.DataFrame) -> Optional[str]:
        """Find the best risk/score column in the dataset."""
        # Preference order for risk columns
        risk_candidates = [
            'composite_score', 'risk_score', 'malaria_risk', 'pca_score',
            'vulnerability_score', 'burden_score', 'transmission_risk',
            'pfpr', 'parasite_prevalence', 'incidence_rate'
        ]
        
        for candidate in risk_candidates:
            if candidate in df.columns:
                return candidate
        
        # Fuzzy matching for variations
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['risk', 'score', 'burden', 'vulnerability', 'pfpr']):
                if pd.api.types.is_numeric_dtype(df[col]):
                    return col
        
        return None
    
    @staticmethod
    def find_coverage_columns(df: pd.DataFrame) -> Dict[str, str]:
        """Find intervention coverage columns."""
        coverage_map = {}
        
        # ITN/Net coverage
        itn_candidates = ['itn_coverage', 'net_coverage', 'bednet_coverage', 'llin_coverage']
        for candidate in itn_candidates:
            if candidate in df.columns:
                coverage_map['itn'] = candidate
                break
        else:
            # Fuzzy search
            for col in df.columns:
                if any(term in col.lower() for term in ['itn', 'net', 'llin', 'bednet']):
                    if pd.api.types.is_numeric_dtype(df[col]):
                        coverage_map['itn'] = col
                        break
        
        # IRS coverage
        irs_candidates = ['irs_coverage', 'spray_coverage', 'indoor_spray']
        for candidate in irs_candidates:
            if candidate in df.columns:
                coverage_map['irs'] = candidate
                break
        else:
            # Fuzzy search
            for col in df.columns:
                if any(term in col.lower() for term in ['irs', 'spray', 'indoor']):
                    if pd.api.types.is_numeric_dtype(df[col]):
                        coverage_map['irs'] = col
                        break
        
        return coverage_map
    
    @staticmethod
    def find_population_column(df: pd.DataFrame) -> Optional[str]:
        """Find population column with flexible matching."""
        pop_candidates = [
            'population', 'total_population', 'pop', 'Pop', 'u5_population',
            'total_pop', 'population_total', 'people', 'inhabitants'
        ]
        
        for candidate in pop_candidates:
            if candidate in df.columns:
                return candidate
        
        # Fuzzy matching
        for col in df.columns:
            if any(term in col.lower() for term in ['pop', 'people', 'inhabit']):
                if pd.api.types.is_numeric_dtype(df[col]):
                    return col
        
        return None
    
    @staticmethod
    def adaptive_risk_threshold(df: pd.DataFrame, risk_col: str, 
                              percentile: float = 70.0) -> float:
        """Calculate adaptive risk threshold based on data distribution."""
        if risk_col not in df.columns:
            return 0.6  # Default fallback
        
        # Use percentile-based threshold
        threshold = df[risk_col].quantile(percentile / 100.0)
        
        # Ensure reasonable bounds
        min_val = df[risk_col].min()
        max_val = df[risk_col].max()
        
        # Adjust if threshold is too extreme
        if threshold < min_val + 0.1 * (max_val - min_val):
            threshold = min_val + 0.2 * (max_val - min_val)
        elif threshold > max_val - 0.1 * (max_val - min_val):
            threshold = max_val - 0.2 * (max_val - min_val)
        
        return float(threshold)
    
    @staticmethod
    def estimate_population(df: pd.DataFrame) -> pd.Series:
        """Estimate population when not available using multiple indicators."""
        # Try area-based estimation
        area_cols = [col for col in df.columns if 'area' in col.lower()]
        if area_cols:
            area_col = area_cols[0]
            # Typical population density ranges
            urban_density = 2000  # people per km²
            rural_density = 100   # people per km²
            
            # Check if we have urban/rural indicator
            urban_cols = [col for col in df.columns if 'urban' in col.lower()]
            if urban_cols:
                urban_col = urban_cols[0]
                is_urban = df[urban_col] if df[urban_col].dtype == bool else df[urban_col] >= 0.5
                density = np.where(is_urban, urban_density, rural_density)
            else:
                density = 500  # Mixed density
            
            return df[area_col] * density
        
        # Household-based estimation
        hh_cols = [col for col in df.columns if any(term in col.lower() for term in ['household', 'hh', 'homes'])]
        if hh_cols:
            hh_col = hh_cols[0]
            avg_hh_size = 5.5  # Typical for sub-Saharan Africa
            return df[hh_col] * avg_hh_size
        
        # Settlement type estimation
        settlement_cols = [col for col in df.columns if 'settlement' in col.lower()]
        if settlement_cols:
            settlement_col = settlement_cols[0]
            # Different estimates by settlement type
            if df[settlement_col].dtype == object:
                pop_map = {
                    'urban': 15000, 'rural': 3000, 'semi-urban': 8000,
                    'city': 25000, 'town': 12000, 'village': 2000
                }
                return df[settlement_col].map(pop_map).fillna(5000)
        
        # Default estimation based on dataset size and region
        n_wards = len(df)
        if n_wards < 100:  # Likely urban/metropolitan area
            base_pop = 8000
        elif n_wards < 500:  # State/province level
            base_pop = 5000
        else:  # National or large regional
            base_pop = 3000
        
        # Add some realistic variation
        return pd.Series(np.random.normal(base_pop, base_pop * 0.3, len(df)), index=df.index).abs()
    
    @staticmethod
    def estimate_coverage(df: pd.DataFrame, intervention: str) -> pd.Series:
        """Estimate coverage when data is missing."""
        n = len(df)
        
        # Different baseline coverage by intervention type
        if intervention == 'itn':
            # ITN coverage typically higher, varies by risk level
            if 'composite_score' in df.columns:
                # Lower coverage in higher risk areas (inverse relationship often seen)
                risk_scores = df['composite_score']
                base_coverage = 70 - (risk_scores * 30)  # 40-70% range
            else:
                base_coverage = 60  # Default ITN coverage
            
            variation = 15  # ±15% variation
        
        elif intervention == 'irs':
            # IRS coverage typically lower and more targeted
            if 'composite_score' in df.columns:
                risk_scores = df['composite_score']
                base_coverage = 20 + (risk_scores * 40)  # 20-60% range
            else:
                base_coverage = 35  # Default IRS coverage
            
            variation = 20  # ±20% variation
        
        else:
            base_coverage = 50
            variation = 15
        
        # Generate realistic coverage distribution
        coverage = np.random.normal(base_coverage, variation, n)
        coverage = np.clip(coverage, 0, 100)  # Bound between 0-100%
        
        return pd.Series(coverage, index=df.index)


class RobustSimulateCoverageIncrease(BaseTool):
    """
    Robust version of SimulateCoverageIncrease that adapts to any dataset.
    
    Automatically detects data structure, adjusts thresholds, and provides
    meaningful results even with missing or incomplete data.
    """
    
    intervention_type: str = Field(
        "itn",
        description="Intervention to increase: 'itn', 'irs', 'combined', or 'all'",
        pattern="^(itn|irs|combined|all)$"
    )
    
    coverage_increase: float = Field(
        20.0,
        description="Percentage point increase in coverage (e.g., 20 = +20%)",
        ge=0.0,
        le=100.0
    )
    
    target_areas: str = Field(
        "high_risk_auto",
        description="Areas to target: 'all', 'high_risk_auto', 'low_coverage_auto', 'top_tercile', 'bottom_tercile'",
        pattern="^(all|high_risk_auto|low_coverage_auto|top_tercile|bottom_tercile)$"
    )
    
    effectiveness_assumption: float = Field(
        0.6,
        description="Assumed intervention effectiveness (0-1, e.g., 0.6 = 60% reduction)",
        ge=0.0,
        le=1.0
    )
    
    adaptive_thresholds: bool = Field(
        True,
        description="Use data-adaptive thresholds instead of fixed values"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Increase ITN coverage by 25% in highest risk areas",
            "Simulate +30% IRS coverage in lowest coverage areas", 
            "What if we boost all interventions by 20% everywhere?",
            "Auto-adapt thresholds and increase coverage in top tercile wards"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute robust coverage increase simulation."""
        try:
            # Get data
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            scenario_df = df.copy()
            
            # Adaptive data discovery
            risk_col = DataAdaptiveUtils.find_risk_column(scenario_df)
            coverage_cols = DataAdaptiveUtils.find_coverage_columns(scenario_df)
            pop_col = DataAdaptiveUtils.find_population_column(scenario_df)
            
            # Create data quality report
            data_quality = {
                'risk_data_available': risk_col is not None,
                'risk_column_used': risk_col,
                'coverage_data_available': bool(coverage_cols),
                'coverage_columns_found': coverage_cols,
                'population_data_available': pop_col is not None,
                'population_column_used': pop_col
            }
            
            # Handle missing population data
            if pop_col is None:
                scenario_df['estimated_population'] = DataAdaptiveUtils.estimate_population(scenario_df)
                pop_col = 'estimated_population'
                data_quality['population_estimated'] = True
            else:
                data_quality['population_estimated'] = False
            
            # Adaptive target area selection
            target_mask = self._get_adaptive_target_mask(scenario_df, risk_col, coverage_cols)
            target_wards = scenario_df[target_mask].copy()
            
            if len(target_wards) == 0:
                # Fallback to top quartile if no wards selected
                if risk_col:
                    threshold = scenario_df[risk_col].quantile(0.75)
                    target_mask = scenario_df[risk_col] >= threshold
                    target_wards = scenario_df[target_mask].copy()
                    fallback_used = f"No wards met criteria. Used top 25% by {risk_col} (≥{threshold:.3f})"
                else:
                    target_mask = pd.Series(True, index=scenario_df.index)
                    target_wards = scenario_df.copy()
                    fallback_used = "No risk data available. Applied to all wards."
            else:
                fallback_used = None
            
            # Simulate coverage changes
            intervention_results = self._simulate_coverage_changes(target_wards, coverage_cols)
            
            # Calculate impact
            impact_results = self._calculate_adaptive_impact(
                target_wards, scenario_df, risk_col, pop_col, intervention_results
            )
            
            # Prepare results with full transparency
            total_wards_affected = len(target_wards)
            total_population_affected = target_wards[pop_col].sum()
            
            message = f"Simulated {self.coverage_increase}% increase in {self.intervention_type} coverage. "
            message += f"Affected {total_wards_affected} wards ({total_wards_affected/len(scenario_df)*100:.1f}% of total), "
            message += f"protecting {impact_results['additional_people_protected']:,.0f} additional people."
            
            return self._create_success_result(
                message=message,
                data={
                    'scenario_summary': {
                        'wards_affected': total_wards_affected,
                        'percentage_of_total_wards': round(total_wards_affected/len(scenario_df)*100, 1),
                        'population_affected': int(total_population_affected),
                        'additional_people_protected': int(impact_results['additional_people_protected']),
                        'intervention_type': self.intervention_type,
                        'coverage_increase': self.coverage_increase,
                        'target_strategy': self.target_areas
                    },
                    'intervention_changes': intervention_results,
                    'impact_analysis': impact_results,
                    'data_quality_report': data_quality,
                    'adaptive_features_used': {
                        'adaptive_thresholds': self.adaptive_thresholds,
                        'fallback_used': fallback_used,
                        'risk_column_detected': risk_col,
                        'target_selection_method': self._describe_target_method(target_mask, scenario_df, risk_col)
                    },
                    'recommendations': self._generate_recommendations(target_wards, scenario_df, data_quality)
                }
            )
            
        except Exception as e:
            logger.error(f"Error in robust coverage simulation: {e}")
            return self._create_error_result(f"Coverage simulation failed: {str(e)}")
    
    def _get_adaptive_target_mask(self, df: pd.DataFrame, risk_col: Optional[str], 
                                coverage_cols: Dict[str, str]) -> pd.Series:
        """Get target ward mask based on adaptive criteria."""
        
        if self.target_areas == "all":
            return pd.Series(True, index=df.index)
        
        elif self.target_areas == "high_risk_auto":
            if risk_col and self.adaptive_thresholds:
                threshold = DataAdaptiveUtils.adaptive_risk_threshold(df, risk_col, percentile=70)
                return df[risk_col] >= threshold
            elif risk_col:
                # Use top tercile as fallback
                threshold = df[risk_col].quantile(0.67)
                return df[risk_col] >= threshold
            else:
                # No risk data - use all wards
                return pd.Series(True, index=df.index)
        
        elif self.target_areas == "top_tercile":
            if risk_col:
                threshold = df[risk_col].quantile(0.67)
                return df[risk_col] >= threshold
            else:
                # Random top third if no risk data
                n_target = len(df) // 3
                return pd.Series(False, index=df.index).iloc[:n_target].fillna(False)
        
        elif self.target_areas == "bottom_tercile":
            if risk_col:
                threshold = df[risk_col].quantile(0.33)
                return df[risk_col] <= threshold
            else:
                # Random bottom third if no risk data
                n_target = len(df) // 3
                return pd.Series(False, index=df.index).iloc[-n_target:].fillna(False)
        
        elif self.target_areas == "low_coverage_auto":
            # Find wards with lowest coverage for the target intervention
            if self.intervention_type in coverage_cols:
                cov_col = coverage_cols[self.intervention_type]
                threshold = df[cov_col].quantile(0.33)  # Bottom tercile coverage
                return df[cov_col] <= threshold
            else:
                # Estimate coverage and target low areas
                estimated_cov = DataAdaptiveUtils.estimate_coverage(df, self.intervention_type)
                threshold = estimated_cov.quantile(0.33)
                return estimated_cov <= threshold
        
        else:
            return pd.Series(True, index=df.index)
    
    def _simulate_coverage_changes(self, target_wards: pd.DataFrame, 
                                 coverage_cols: Dict[str, str]) -> Dict[str, Any]:
        """Simulate coverage changes with robust data handling."""
        
        results = {
            'interventions_modified': [],
            'coverage_changes': {},
            'synthetic_data_used': {}
        }
        
        interventions = ['itn'] if self.intervention_type == 'itn' else \
                       ['irs'] if self.intervention_type == 'irs' else \
                       ['itn', 'irs'] if self.intervention_type in ['combined', 'all'] else \
                       ['itn']
        
        for intervention in interventions:
            if intervention in coverage_cols:
                # Use existing coverage data
                cov_col = coverage_cols[intervention]
                original_coverage = target_wards[cov_col].copy()
                new_coverage = np.minimum(100.0, original_coverage + self.coverage_increase)
                
                results['coverage_changes'][intervention] = {
                    'column_used': cov_col,
                    'baseline_mean': float(original_coverage.mean()),
                    'scenario_mean': float(new_coverage.mean()),
                    'increase_applied': self.coverage_increase,
                    'wards_at_max_coverage': int((new_coverage >= 100).sum())
                }
                results['synthetic_data_used'][intervention] = False
                
            else:
                # Estimate baseline and apply increase
                estimated_baseline = DataAdaptiveUtils.estimate_coverage(target_wards, intervention)
                new_coverage = np.minimum(100.0, estimated_baseline + self.coverage_increase)
                
                results['coverage_changes'][intervention] = {
                    'column_used': 'estimated',
                    'baseline_mean': float(estimated_baseline.mean()),
                    'scenario_mean': float(new_coverage.mean()),
                    'increase_applied': self.coverage_increase,
                    'wards_at_max_coverage': int((new_coverage >= 100).sum())
                }
                results['synthetic_data_used'][intervention] = True
            
            results['interventions_modified'].append(intervention)
        
        return results
    
    def _calculate_adaptive_impact(self, target_wards: pd.DataFrame, full_df: pd.DataFrame,
                                 risk_col: Optional[str], pop_col: str, 
                                 intervention_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate impact with adaptive modeling."""
        
        # Calculate population impact
        total_population = target_wards[pop_col].sum()
        
        # Estimate additional people protected
        # More sophisticated model could be used here
        avg_coverage_increase = self.coverage_increase
        additional_protected = total_population * (avg_coverage_increase / 100.0)
        
        # Risk reduction modeling (if risk data available)
        risk_impact = {}
        if risk_col:
            # Simple risk reduction model
            risk_reduction_factor = avg_coverage_increase / 100.0 * self.effectiveness_assumption
            baseline_risk = target_wards[risk_col].mean()
            scenario_risk = baseline_risk * (1 - risk_reduction_factor)
            
            risk_impact = {
                'baseline_avg_risk': float(baseline_risk),
                'scenario_avg_risk': float(scenario_risk),
                'relative_risk_reduction': float(risk_reduction_factor),
                'absolute_risk_reduction': float(baseline_risk - scenario_risk)
            }
        
        return {
            'additional_people_protected': additional_protected,
            'total_population_in_target_areas': total_population,
            'coverage_efficiency': avg_coverage_increase / 100.0,
            'risk_impact': risk_impact,
            'intervention_effectiveness_assumed': self.effectiveness_assumption
        }
    
    def _describe_target_method(self, target_mask: pd.Series, df: pd.DataFrame, 
                              risk_col: Optional[str]) -> str:
        """Describe how target areas were selected."""
        n_selected = target_mask.sum()
        total = len(df)
        percentage = n_selected / total * 100
        
        if self.target_areas == "high_risk_auto" and risk_col:
            threshold = df.loc[target_mask, risk_col].min() if n_selected > 0 else "N/A"
            return f"Selected {n_selected} wards ({percentage:.1f}%) with {risk_col} ≥ {threshold:.3f}"
        elif self.target_areas == "top_tercile":
            return f"Selected top tercile: {n_selected} wards ({percentage:.1f}%)"
        elif self.target_areas == "all":
            return f"Applied to all {total} wards"
        else:
            return f"Selected {n_selected} wards ({percentage:.1f}%) using {self.target_areas} criteria"
    
    def _generate_recommendations(self, target_wards: pd.DataFrame, full_df: pd.DataFrame, 
                                data_quality: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        # Data quality recommendations
        if not data_quality['risk_data_available']:
            recommendations.append("Consider uploading risk/burden data for more precise targeting")
        
        if not data_quality['coverage_data_available']:
            recommendations.append("Upload actual coverage data to improve scenario accuracy")
        
        if data_quality['population_estimated']:
            recommendations.append("Upload population data for more accurate impact estimates")
        
        # Targeting recommendations
        target_percentage = len(target_wards) / len(full_df) * 100
        if target_percentage < 10:
            recommendations.append("Very targeted approach - consider expanding scope if resources allow")
        elif target_percentage > 80:
            recommendations.append("Broad approach - consider focusing on highest priority areas first")
        
        # Coverage recommendations
        if self.coverage_increase > 50:
            recommendations.append("Large coverage increase - ensure implementation capacity exists")
        
        return recommendations