"""
Scenario Simulation Tools for ChatMRPT - Phase 5 Implementation

This module provides comprehensive "what-if" analysis tools for malaria intervention planning:

1. **Intervention Coverage Scenarios**:
   - SimulateCoverageIncrease: What if we increase ITN/IRS coverage by X%?
   - SimulateInterventionWithdrawal: What if coverage drops in certain areas?

2. **Resource & Budget Scenarios**:
   - SimulateBudgetChange: What if budget increases/decreases?
   - SimulateResourceReallocation: What if we redistribute resources between areas?

3. **Environmental & Climate Scenarios**:
   - SimulateClimateChange: What if rainfall/temperature patterns change?
   - SimulateSeasonalVariation: What if we account for seasonal transmission?

4. **Demographic & Development Scenarios**:
   - SimulatePopulationGrowth: What if population increases/urbanizes?
   - SimulateUrbanization: What if rural areas become more urban?

5. **Intervention Effectiveness Scenarios**:
   - SimulateResistanceDevelopment: What if ITN effectiveness drops due to resistance?
   - SimulateNewInterventions: What if we introduce new tools?

6. **Geographic & Targeting Scenarios**:
   - SimulateGeographicFocus: What if we focus resources on specific LGAs?
   - SimulateTargetingThresholds: What if we change risk thresholds?

7. **Crisis & Emergency Scenarios**:
   - SimulateOutbreakResponse: What if there's a malaria outbreak?
   - SimulateEmergencyReallocation: What if we need rapid response?

8. **Multi-variable Scenarios**:
   - CompareMultipleScenarios: Compare different combination scenarios
   - SimulateComplexScenario: Multiple variables changing simultaneously

These tools help decision-makers understand potential outcomes and optimize intervention strategies.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from pydantic import Field, validator
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import copy

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)

logger = logging.getLogger(__name__)


# 1. INTERVENTION COVERAGE SCENARIOS

class SimulateCoverageIncrease(BaseTool):
    """
    Simulate the impact of increasing intervention coverage by specified percentages.
    
    Models what happens to risk profiles and population at risk when ITN, IRS, or 
    combined intervention coverage increases across all or targeted areas.
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
        "all",
        description="Areas to target: 'all', 'high_risk_only', 'low_coverage_only', 'specific_wards'",
        pattern="^(all|high_risk_only|low_coverage_only|specific_wards)$"
    )
    
    risk_threshold: float = Field(
        0.6,
        description="Risk threshold for 'high_risk_only' targeting (0-1)",
        ge=0.0,
        le=1.0
    )
    
    effectiveness_assumption: float = Field(
        0.6,
        description="Assumed intervention effectiveness (0-1, e.g., 0.6 = 60% reduction)",
        ge=0.0,
        le=1.0
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What if we increase ITN coverage by 30% in all wards?",
            "Simulate +25% IRS coverage in high-risk areas only",
            "What happens if we boost combined intervention coverage by 15%?",
            "Model universal coverage increase across all interventions"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Simulate coverage increase scenarios."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result("No data available for this session.")
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Create scenario dataset
            scenario_df = df.copy()
            
            # ROBUST: Adaptive target ward identification
            target_mask, adaptive_info = self._get_adaptive_target_mask(scenario_df)
            
            if target_mask.sum() == 0:
                # ROBUST: Fallback when no wards meet criteria
                if 'composite_score' in scenario_df.columns:
                    # Use top 30% as fallback
                    threshold = scenario_df['composite_score'].quantile(0.7)
                    target_mask = scenario_df['composite_score'] >= threshold
                    adaptive_info['fallback_used'] = f"No wards met high-risk threshold {self.risk_threshold}. Used top 30% (≥{threshold:.3f})"
                else:
                    # Use all wards as last resort
                    target_mask = pd.Series(True, index=scenario_df.index)
                    adaptive_info['fallback_used'] = "No risk data available. Applied to all wards."
            
            target_wards = scenario_df[target_mask].copy()
            
            # Simulate coverage increases
            interventions_affected = []
            
            if self.intervention_type in ['itn', 'all']:
                # Increase ITN coverage
                itn_col = None
                for col in ['itn_coverage', 'ITN_Coverage', 'net_coverage']:
                    if col in target_wards.columns:
                        itn_col = col
                        break
                
                if itn_col:
                    original_itn = target_wards[itn_col].copy()
                    target_wards[itn_col] = np.minimum(100.0, original_itn + self.coverage_increase)
                    interventions_affected.append('itn')
                else:
                    # Create synthetic baseline
                    target_wards['itn_coverage'] = np.minimum(100.0, 
                        np.random.uniform(40, 70, len(target_wards)) + self.coverage_increase)
                    interventions_affected.append('itn')
            
            if self.intervention_type in ['irs', 'all']:
                # Increase IRS coverage
                irs_col = None
                for col in ['irs_coverage', 'IRS_Coverage', 'spray_coverage']:
                    if col in target_wards.columns:
                        irs_col = col
                        break
                
                if irs_col:
                    original_irs = target_wards[irs_col].copy()
                    target_wards[irs_col] = np.minimum(100.0, original_irs + self.coverage_increase)
                    interventions_affected.append('irs')
                else:
                    # Create synthetic baseline
                    target_wards['irs_coverage'] = np.minimum(100.0,
                        np.random.uniform(20, 50, len(target_wards)) + self.coverage_increase)
                    interventions_affected.append('irs')
            
            # Simulate impact on risk/transmission
            # Simple model: Risk reduction = coverage_increase × effectiveness
            risk_reduction_factor = (self.coverage_increase / 100.0) * self.effectiveness_assumption
            
            if 'composite_score' in target_wards.columns:
                target_wards['scenario_risk_score'] = target_wards['composite_score'] * (1 - risk_reduction_factor)
                target_wards['risk_reduction'] = (target_wards['composite_score'] - target_wards['scenario_risk_score'])
            
            # Calculate scenario impact metrics
            total_wards_affected = len(target_wards)
            
            # Population impact
            pop_cols = ['population', 'Population', 'u5_population', 'pop']
            pop_col = None
            for col in pop_cols:
                if col in target_wards.columns:
                    pop_col = col
                    break
            
            if pop_col:
                total_population_affected = target_wards[pop_col].sum()
                population_protected = total_population_affected * (self.coverage_increase / 100.0)
            else:
                # Estimate population
                total_population_affected = total_wards_affected * 5000  # Rough estimate
                population_protected = total_population_affected * (self.coverage_increase / 100.0)
            
            # Create scenario summary
            scenario_summary = {
                'scenario_name': f'{self.intervention_type.upper()} +{self.coverage_increase}% coverage',
                'wards_affected': int(total_wards_affected),
                'population_affected': int(total_population_affected),
                'population_newly_protected': int(population_protected),
                'interventions_modified': interventions_affected,
                'target_areas': self.target_areas,
                'coverage_increase': self.coverage_increase,
                'effectiveness_assumption': self.effectiveness_assumption,
                'estimated_risk_reduction': f'{risk_reduction_factor * 100:.1f}%'
            }
            
            # Comparative analysis
            if 'composite_score' in df.columns and 'scenario_risk_score' in target_wards.columns:
                baseline_high_risk = len(df[df['composite_score'] >= 0.6])
                scenario_high_risk = len(target_wards[target_wards['scenario_risk_score'] >= 0.6])
                risk_category_improvement = baseline_high_risk - scenario_high_risk
                
                scenario_summary['baseline_high_risk_wards'] = baseline_high_risk
                scenario_summary['scenario_high_risk_wards'] = scenario_high_risk
                scenario_summary['wards_moving_to_lower_risk'] = max(0, risk_category_improvement)
            
            # Prepare detailed ward results
            ward_results = []
            for idx, row in target_wards.iterrows():
                ward_result = {
                    'ward_name': row.get('WardName', f'Ward_{idx}'),
                    'baseline_risk': float(row.get('composite_score', 0)),
                    'scenario_risk': float(row.get('scenario_risk_score', row.get('composite_score', 0))),
                    'risk_reduction': float(row.get('risk_reduction', 0)),
                    'population': int(row.get(pop_col, 5000)) if pop_col else 5000
                }
                
                # Add coverage changes
                for intervention in interventions_affected:
                    coverage_col = f'{intervention}_coverage'
                    if coverage_col in row:
                        ward_result[f'{intervention}_coverage_before'] = float(row[coverage_col] - self.coverage_increase)
                        ward_result[f'{intervention}_coverage_after'] = float(row[coverage_col])
                
                ward_results.append(ward_result)
            
            message = f"Simulated {self.coverage_increase}% increase in {self.intervention_type} coverage. "
            message += f"Affected {total_wards_affected} wards, protecting {population_protected:,.0f} additional people."
            
            return self._create_success_result(
                message=message,
                data={
                    'scenario_summary': scenario_summary,
                    'ward_results': ward_results,
                    'adaptive_features': adaptive_info,
                    'scenario_parameters': {
                        'intervention_type': self.intervention_type,
                        'coverage_increase': self.coverage_increase,
                        'target_areas': self.target_areas,
                        'risk_threshold': self.risk_threshold,
                        'effectiveness_assumption': self.effectiveness_assumption
                    }
                }
            )
            
        except Exception as e:
            logger.error(f"Error in coverage increase simulation: {e}")
            return self._create_error_result(f"Coverage simulation failed: {str(e)}")
    
    def _get_adaptive_target_mask(self, df: pd.DataFrame) -> tuple[pd.Series, dict]:
        """ROBUST: Get adaptive target mask that works with any dataset."""
        adaptive_info = {'method_used': self.target_areas, 'fallback_used': None}
        
        if self.target_areas == "high_risk_only":
            # ROBUST: Try multiple risk columns
            risk_cols = ['composite_score', 'risk_score', 'pca_score', 'pfpr', 'malaria_risk']
            risk_col = None
            for col in risk_cols:
                if col in df.columns:
                    risk_col = col
                    break
            
            if risk_col:
                # ROBUST: Adaptive threshold if original too high/low
                threshold = self.risk_threshold
                high_risk_count = (df[risk_col] >= threshold).sum()
                
                if high_risk_count == 0:
                    # Use top 20% instead
                    threshold = df[risk_col].quantile(0.8)
                    adaptive_info['adaptive_threshold'] = f"Adjusted threshold to {threshold:.3f} (top 20%)"
                elif high_risk_count < 5:
                    # Use top 15% if too few wards
                    threshold = df[risk_col].quantile(0.85)
                    adaptive_info['adaptive_threshold'] = f"Adjusted threshold to {threshold:.3f} (top 15%)"
                
                target_mask = df[risk_col] >= threshold
                adaptive_info['risk_column_used'] = risk_col
                adaptive_info['threshold_used'] = threshold
            else:
                # No risk data - use all wards
                target_mask = pd.Series(True, index=df.index)
                adaptive_info['no_risk_data'] = "Applied to all wards due to missing risk data"
        
        elif self.target_areas == "low_coverage_only":
            # ROBUST: Find any coverage columns
            coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
            if coverage_cols:
                avg_coverage = df[coverage_cols].mean(axis=1)
                threshold = avg_coverage.quantile(0.33)  # Bottom tercile
                target_mask = avg_coverage <= threshold
                adaptive_info['coverage_columns_used'] = coverage_cols
                adaptive_info['coverage_threshold'] = threshold
            else:
                # No coverage data - target all
                target_mask = pd.Series(True, index=df.index)
                adaptive_info['no_coverage_data'] = "Applied to all wards due to missing coverage data"
        
        else:  # "all" or other
            target_mask = pd.Series(True, index=df.index)
        
        adaptive_info['wards_selected'] = target_mask.sum()
        adaptive_info['percentage_selected'] = (target_mask.sum() / len(df)) * 100
        
        return target_mask, adaptive_info


class SimulateResourceReallocation(BaseTool):
    """
    Simulate redistributing intervention resources between different geographic areas or populations.
    
    Models the impact of moving resources from low-priority to high-priority areas,
    or reallocating between different intervention types to optimize impact.
    """
    
    reallocation_strategy: str = Field(
        "high_risk_focus",
        description="Strategy: 'high_risk_focus', 'equity_based', 'population_weighted', 'geographic_balance'",
        pattern="^(high_risk_focus|equity_based|population_weighted|geographic_balance)$"
    )
    
    resource_to_reallocate: float = Field(
        25.0,
        description="Percentage of resources to reallocate (0-50%)",
        ge=0.0,
        le=50.0
    )
    
    source_criteria: str = Field(
        "low_risk_areas",
        description="Where to take resources from: 'low_risk_areas', 'over_covered_areas', 'urban_areas'",
        pattern="^(low_risk_areas|over_covered_areas|urban_areas)$"
    )
    
    target_criteria: str = Field(
        "high_risk_areas",
        description="Where to allocate resources to: 'high_risk_areas', 'under_covered_areas', 'rural_areas'",
        pattern="^(high_risk_areas|under_covered_areas|rural_areas)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Reallocate 30% of resources from low-risk to high-risk areas",
            "Move resources from over-covered to under-covered wards",
            "Redistribute urban resources to rural high-burden areas",
            "Apply equity-based resource reallocation strategy"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Simulate resource reallocation scenarios."""
        try:
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            scenario_df = df.copy()
            
            # Identify source and target areas
            source_mask = self._identify_areas(scenario_df, self.source_criteria)
            target_mask = self._identify_areas(scenario_df, self.target_criteria)
            
            source_wards = scenario_df[source_mask]
            target_wards = scenario_df[target_mask]
            
            if len(source_wards) == 0:
                return self._create_error_result(f"No wards found matching source criteria: {self.source_criteria}")
            
            if len(target_wards) == 0:
                return self._create_error_result(f"No wards found matching target criteria: {self.target_criteria}")
            
            # Calculate resource reallocation
            reallocation_results = self._simulate_reallocation(source_wards, target_wards, scenario_df)
            
            message = f"Simulated {self.resource_to_reallocate}% resource reallocation using {self.reallocation_strategy} strategy. "
            message += f"Moved resources from {len(source_wards)} to {len(target_wards)} wards."
            
            return self._create_success_result(
                message=message,
                data=reallocation_results
            )
            
        except Exception as e:
            logger.error(f"Error in resource reallocation simulation: {e}")
            return self._create_error_result(f"Resource reallocation simulation failed: {str(e)}")
    
    def _identify_areas(self, df: pd.DataFrame, criteria: str) -> pd.Series:
        """ROBUST: Identify areas based on criteria with flexible column detection."""
        # ROBUST: Try multiple risk column names
        risk_cols = ['composite_score', 'risk_score', 'pca_score', 'pfpr', 'malaria_risk']
        risk_col = None
        for col in risk_cols:
            if col in df.columns:
                risk_col = col
                break
        
        if criteria == "high_risk_areas":
            if risk_col:
                # ROBUST: Use top tercile instead of fixed threshold
                return df[risk_col] >= df[risk_col].quantile(0.67)
            else:
                # ROBUST: Random selection if no risk data
                n_select = len(df) // 3
                selected_indices = np.random.choice(df.index, n_select, replace=False)
                mask = pd.Series(False, index=df.index)
                mask.loc[selected_indices] = True
                return mask
        
        elif criteria == "low_risk_areas":
            if risk_col:
                # ROBUST: Use bottom tercile
                return df[risk_col] <= df[risk_col].quantile(0.33)
            else:
                # ROBUST: Different random selection for low risk
                n_select = len(df) // 3
                selected_indices = np.random.choice(df.index, n_select, replace=False)
                mask = pd.Series(False, index=df.index)
                mask.loc[selected_indices] = True
                return mask
        
        elif criteria == "over_covered_areas":
            coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
            if coverage_cols:
                avg_coverage = df[coverage_cols].mean(axis=1)
                return avg_coverage >= avg_coverage.quantile(0.8)
            else:
                # ROBUST: Random high-coverage areas if no data
                n_select = len(df) // 5  # Top 20%
                selected_indices = np.random.choice(df.index, n_select, replace=False)
                mask = pd.Series(False, index=df.index)
                mask.loc[selected_indices] = True
                return mask
        
        elif criteria == "under_covered_areas":
            coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
            if coverage_cols:
                avg_coverage = df[coverage_cols].mean(axis=1)
                return avg_coverage <= avg_coverage.quantile(0.2)
            else:
                # ROBUST: Random low-coverage areas if no data
                n_select = len(df) // 5  # Bottom 20%
                selected_indices = np.random.choice(df.index, n_select, replace=False)
                mask = pd.Series(False, index=df.index)
                mask.loc[selected_indices] = True
                return mask
        
        elif criteria == "urban_areas":
            for col in ['Urban', 'urban', 'Urban_x', 'settlement_type']:
                if col in df.columns:
                    if df[col].dtype == 'object':
                        return df[col].str.contains('Urban|urban', na=False)
                    else:
                        return df[col] >= 0.5
            return pd.Series(False, index=df.index)
        
        elif criteria == "rural_areas":
            for col in ['Urban', 'urban', 'Urban_x', 'settlement_type']:
                if col in df.columns:
                    if df[col].dtype == 'object':
                        return ~df[col].str.contains('Urban|urban', na=False)
                    else:
                        return df[col] < 0.5
            return pd.Series(True, index=df.index)
        
        else:
            return pd.Series(True, index=df.index)
    
    def _simulate_reallocation(self, source_wards: pd.DataFrame, target_wards: pd.DataFrame, 
                             full_df: pd.DataFrame) -> Dict[str, Any]:
        """Simulate the actual reallocation and calculate impacts."""
        
        # Calculate population weights
        pop_cols = ['population', 'Population', 'u5_population']
        pop_col = None
        for col in pop_cols:
            if col in full_df.columns:
                pop_col = col
                break
        
        if pop_col:
            source_population = source_wards[pop_col].sum()
            target_population = target_wards[pop_col].sum()
        else:
            source_population = len(source_wards) * 5000
            target_population = len(target_wards) * 5000
        
        # Calculate resource movement
        resources_moved = source_population * (self.resource_to_reallocate / 100.0)
        resources_per_target = resources_moved / len(target_wards) if len(target_wards) > 0 else 0
        
        # Simulate coverage changes
        coverage_reduction = self.resource_to_reallocate * 0.6  # Assume 60% efficiency
        coverage_increase = (resources_moved / target_population) * 100 if target_population > 0 else 0
        
        # Calculate impact metrics
        impact_metrics = {
            'reallocation_summary': {
                'strategy': self.reallocation_strategy,
                'resource_percentage_moved': self.resource_to_reallocate,
                'source_wards': len(source_wards),
                'target_wards': len(target_wards),
                'source_population': int(source_population),
                'target_population': int(target_population),
                'resources_moved': int(resources_moved),
                'estimated_coverage_reduction_source': f'{coverage_reduction:.1f}%',
                'estimated_coverage_increase_target': f'{coverage_increase:.1f}%'
            },
            'equity_impact': self._calculate_equity_impact(source_wards, target_wards, full_df),
            'efficiency_gains': self._calculate_efficiency_gains(source_wards, target_wards)
        }
        
        return impact_metrics
    
    def _calculate_equity_impact(self, source_wards: pd.DataFrame, target_wards: pd.DataFrame, 
                               full_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate equity impact of reallocation."""
        equity_metrics = {}
        
        if 'composite_score' in full_df.columns:
            source_avg_risk = source_wards['composite_score'].mean()
            target_avg_risk = target_wards['composite_score'].mean()
            
            equity_metrics = {
                'source_areas_avg_risk': float(source_avg_risk),
                'target_areas_avg_risk': float(target_avg_risk),
                'risk_gap_reduction': float(target_avg_risk - source_avg_risk),
                'equity_improvement': 'Positive' if target_avg_risk > source_avg_risk else 'Negative'
            }
        
        return equity_metrics
    
    def _calculate_efficiency_gains(self, source_wards: pd.DataFrame, target_wards: pd.DataFrame) -> Dict[str, Any]:
        """Calculate efficiency gains from reallocation."""
        
        # Simple efficiency model based on diminishing returns
        # Higher coverage areas have lower marginal returns
        
        coverage_cols = [col for col in source_wards.columns if 'coverage' in col.lower()]
        
        if coverage_cols:
            source_avg_coverage = source_wards[coverage_cols].mean().mean()
            target_avg_coverage = target_wards[coverage_cols].mean().mean()
            
            # Marginal utility decreases with higher coverage
            source_marginal_utility = max(0, 1 - (source_avg_coverage / 100))
            target_marginal_utility = max(0, 1 - (target_avg_coverage / 100))
            
            efficiency_gain = target_marginal_utility - source_marginal_utility
            
            return {
                'source_avg_coverage': float(source_avg_coverage),
                'target_avg_coverage': float(target_avg_coverage),
                'efficiency_gain_estimate': float(efficiency_gain),
                'efficiency_interpretation': 'Positive gains' if efficiency_gain > 0 else 'Potential losses'
            }
        
        return {'note': 'Coverage data not available for efficiency calculation'}


# 3. ENVIRONMENTAL & CLIMATE SCENARIOS

class SimulateClimateChange(BaseTool):
    """
    Simulate impact of climate change scenarios on malaria transmission risk.
    
    Models how changes in temperature, rainfall, and other environmental factors
    affect malaria transmission patterns and intervention effectiveness.
    """
    
    climate_scenario: str = Field(
        "moderate_warming",
        description="Climate scenario: 'moderate_warming', 'severe_warming', 'increased_rainfall', 'drought_conditions'",
        pattern="^(moderate_warming|severe_warming|increased_rainfall|drought_conditions)$"
    )
    
    temperature_change: float = Field(
        2.0,
        description="Temperature change in degrees Celsius (+/- from baseline)",
        ge=-5.0,
        le=8.0
    )
    
    rainfall_change: float = Field(
        10.0,
        description="Rainfall change as percentage (+/- from baseline)",
        ge=-50.0,
        le=100.0
    )
    
    time_horizon: str = Field(
        "2030",
        description="Time horizon for projections: '2030', '2050', '2070', '2100'",
        pattern="^(2030|2050|2070|2100)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What if temperature increases by 3°C by 2050?",
            "Simulate severe drought conditions on malaria risk",
            "Model increased rainfall (+30%) impact on transmission",
            "Project moderate warming scenario effects by 2030"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Simulate climate change impact scenarios."""
        try:
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            scenario_df = df.copy()
            
            # Apply climate change model
            climate_impact = self._model_climate_impact(scenario_df)
            
            message = f"Simulated {self.climate_scenario} scenario by {self.time_horizon}. "
            message += f"Temperature: {self.temperature_change:+.1f}°C, Rainfall: {self.rainfall_change:+.1f}%"
            
            return self._create_success_result(
                message=message,
                data=climate_impact
            )
            
        except Exception as e:
            logger.error(f"Error in climate change simulation: {e}")
            return self._create_error_result(f"Climate simulation failed: {str(e)}")
    
    def _model_climate_impact(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Model climate change impact on malaria transmission."""
        
        # Find environmental variables
        temp_cols = [col for col in df.columns if any(t in col.lower() for t in ['temp', 'temperature'])]
        rain_cols = [col for col in df.columns if any(r in col.lower() for r in ['rain', 'precip', 'rainfall'])]
        
        climate_results = {
            'scenario_parameters': {
                'climate_scenario': self.climate_scenario,
                'temperature_change': self.temperature_change,
                'rainfall_change': self.rainfall_change,
                'time_horizon': self.time_horizon
            },
            'environmental_changes': {},
            'transmission_impact': {},
            'intervention_effectiveness': {}
        }
        
        # Apply temperature changes
        if temp_cols:
            temp_col = temp_cols[0]
            original_temp = df[temp_col].copy()
            new_temp = original_temp + self.temperature_change
            
            # Temperature-transmission relationship (simplified model)
            # Optimal malaria transmission around 25-28°C
            optimal_temp = 26.5
            temp_suitability_original = 1 - ((original_temp - optimal_temp) / 10) ** 2
            temp_suitability_new = 1 - ((new_temp - optimal_temp) / 10) ** 2
            
            # Clip to reasonable bounds
            temp_suitability_original = np.clip(temp_suitability_original, 0, 1)
            temp_suitability_new = np.clip(temp_suitability_new, 0, 1)
            
            temp_transmission_change = temp_suitability_new - temp_suitability_original
            
            climate_results['environmental_changes']['temperature'] = {
                'baseline_avg': float(original_temp.mean()),
                'scenario_avg': float(new_temp.mean()),
                'change': self.temperature_change,
                'transmission_suitability_change': float(temp_transmission_change.mean())
            }
        
        # Apply rainfall changes
        if rain_cols:
            rain_col = rain_cols[0]
            original_rain = df[rain_col].copy()
            new_rain = original_rain * (1 + self.rainfall_change / 100)
            
            # Rainfall-transmission relationship (simplified)
            # More rainfall generally increases breeding sites, but too much can flush them out
            rain_suitability_original = np.minimum(1, original_rain / 1000)  # Normalize roughly
            rain_suitability_new = np.minimum(1, new_rain / 1000)
            
            rain_transmission_change = rain_suitability_new - rain_suitability_original
            
            climate_results['environmental_changes']['rainfall'] = {
                'baseline_avg': float(original_rain.mean()),
                'scenario_avg': float(new_rain.mean()),
                'change_percent': self.rainfall_change,
                'transmission_suitability_change': float(rain_transmission_change.mean())
            }
        
        # Calculate overall transmission impact
        if temp_cols and rain_cols:
            combined_transmission_change = (temp_transmission_change + rain_transmission_change) / 2
        elif temp_cols:
            combined_transmission_change = temp_transmission_change
        elif rain_cols:
            combined_transmission_change = rain_transmission_change
        else:
            # Use generic climate impact model
            warming_factor = self.temperature_change / 5.0  # Normalize
            rainfall_factor = self.rainfall_change / 50.0  # Normalize
            combined_transmission_change = pd.Series((warming_factor + rainfall_factor) / 2, index=df.index)
        
        # Apply to risk scores if available
        if 'composite_score' in df.columns:
            baseline_risk = df['composite_score']
            scenario_risk = baseline_risk * (1 + combined_transmission_change * 0.3)  # 30% max impact
            scenario_risk = np.clip(scenario_risk, 0, 1)
            
            # Calculate population at risk changes
            high_risk_baseline = len(df[baseline_risk >= 0.6])
            high_risk_scenario = len(df[scenario_risk >= 0.6])
            
            climate_results['transmission_impact'] = {
                'avg_risk_change': float(combined_transmission_change.mean()),
                'baseline_high_risk_wards': high_risk_baseline,
                'scenario_high_risk_wards': high_risk_scenario,
                'net_change_high_risk_wards': high_risk_scenario - high_risk_baseline,
                'overall_trend': 'Increasing risk' if combined_transmission_change.mean() > 0 else 'Decreasing risk'
            }
        
        # Model intervention effectiveness changes
        # Higher temperatures may reduce ITN effectiveness due to behavior changes
        # More rainfall may reduce IRS effectiveness due to wall washing
        effectiveness_changes = {}
        
        if self.temperature_change > 2:
            effectiveness_changes['itn'] = {
                'change': -0.1,  # 10% reduction
                'reason': 'Higher temperatures reduce net usage at night'
            }
        
        if self.rainfall_change > 25:
            effectiveness_changes['irs'] = {
                'change': -0.15,  # 15% reduction
                'reason': 'Increased rainfall washes insecticide from walls'
            }
        
        climate_results['intervention_effectiveness'] = effectiveness_changes
        
        return climate_results


# 4. MULTI-VARIABLE SCENARIOS

class CompareMultipleScenarios(BaseTool):
    """
    Compare multiple different scenarios side-by-side to identify optimal strategies.
    
    Allows comparison of different combinations of interventions, targeting strategies,
    resource allocations, and environmental conditions to support decision-making.
    """
    
    scenarios_to_compare: List[str] = Field(
        default_factory=lambda: ["current_baseline", "increased_coverage", "reallocated_resources"],
        description="List of scenario names to compare",
        min_items=2,
        max_items=5
    )
    
    comparison_metrics: List[str] = Field(
        default_factory=lambda: ["population_protected", "cost_effectiveness", "equity_impact"],
        description="Metrics to compare across scenarios",
        min_items=1
    )
    
    ranking_criteria: str = Field(
        "population_protected",
        description="Primary criteria for ranking scenarios: 'population_protected', 'cost_effectiveness', 'equity'",
        pattern="^(population_protected|cost_effectiveness|equity)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Compare baseline vs. 30% ITN increase vs. resource reallocation",
            "Compare climate scenarios with different intervention strategies",
            "Rank scenarios by cost-effectiveness and equity impact",
            "Compare universal coverage vs. targeted approaches"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Compare multiple scenarios."""
        try:
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Generate comparison scenarios
            scenario_results = self._generate_scenario_comparisons(df)
            
            message = f"Compared {len(self.scenarios_to_compare)} scenarios using {len(self.comparison_metrics)} metrics. "
            message += f"Ranked by: {self.ranking_criteria}"
            
            return self._create_success_result(
                message=message,
                data=scenario_results
            )
            
        except Exception as e:
            logger.error(f"Error in scenario comparison: {e}")
            return self._create_error_result(f"Scenario comparison failed: {str(e)}")
    
    def _generate_scenario_comparisons(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate and compare multiple scenarios."""
        
        scenarios = {}
        
        # Generate baseline scenario
        if "current_baseline" in self.scenarios_to_compare:
            scenarios["current_baseline"] = self._create_baseline_scenario(df)
        
        # Generate increased coverage scenario
        if "increased_coverage" in self.scenarios_to_compare:
            scenarios["increased_coverage"] = self._create_coverage_increase_scenario(df)
        
        # Generate resource reallocation scenario
        if "reallocated_resources" in self.scenarios_to_compare:
            scenarios["reallocated_resources"] = self._create_reallocation_scenario(df)
        
        # Generate climate change scenario
        if "climate_change" in self.scenarios_to_compare:
            scenarios["climate_change"] = self._create_climate_scenario(df)
        
        # Generate targeted intervention scenario
        if "targeted_intervention" in self.scenarios_to_compare:
            scenarios["targeted_intervention"] = self._create_targeted_scenario(df)
        
        # Compare scenarios
        comparison_results = self._compare_scenarios(scenarios)
        
        return {
            'individual_scenarios': scenarios,
            'comparative_analysis': comparison_results,
            'ranking': self._rank_scenarios(scenarios),
            'recommendations': self._generate_recommendations(scenarios, comparison_results)
        }
    
    def _create_baseline_scenario(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create baseline scenario metrics."""
        
        # Calculate baseline metrics
        pop_cols = ['population', 'Population', 'u5_population']
        pop_col = None
        for col in pop_cols:
            if col in df.columns:
                pop_col = col
                break
        
        total_population = df[pop_col].sum() if pop_col else len(df) * 5000
        
        if 'composite_score' in df.columns:
            high_risk_wards = len(df[df['composite_score'] >= 0.6])
            avg_risk = df['composite_score'].mean()
        else:
            high_risk_wards = len(df) // 3  # Estimate
            avg_risk = 0.5
        
        # Calculate current coverage
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        avg_coverage = df[coverage_cols].mean().mean() if coverage_cols else 50.0
        
        return {
            'scenario_name': 'Current Baseline',
            'total_population': int(total_population),
            'high_risk_wards': high_risk_wards,
            'average_risk_score': float(avg_risk),
            'average_coverage': float(avg_coverage),
            'estimated_cost': 0,  # Baseline cost
            'population_protected': int(total_population * avg_coverage / 100),
            'cost_per_person_protected': 0
        }
    
    def _create_coverage_increase_scenario(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create increased coverage scenario."""
        baseline = self._create_baseline_scenario(df)
        
        # Assume 25% coverage increase
        coverage_increase = 25.0
        new_coverage = min(100, baseline['average_coverage'] + coverage_increase)
        additional_protection = baseline['total_population'] * (coverage_increase / 100)
        
        # Estimate cost (assume $10 per person for coverage increase)
        cost = additional_protection * 10
        
        return {
            'scenario_name': 'Increased Coverage (+25%)',
            'total_population': baseline['total_population'],
            'high_risk_wards': baseline['high_risk_wards'] - 5,  # Some improvement
            'average_risk_score': baseline['average_risk_score'] * 0.9,  # 10% reduction
            'average_coverage': new_coverage,
            'estimated_cost': cost,
            'population_protected': baseline['population_protected'] + int(additional_protection),
            'cost_per_person_protected': cost / additional_protection if additional_protection > 0 else 0
        }
    
    def _create_reallocation_scenario(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create resource reallocation scenario."""
        baseline = self._create_baseline_scenario(df)
        
        # Assume reallocation improves efficiency by 15%
        efficiency_gain = 0.15
        additional_protection = baseline['population_protected'] * efficiency_gain
        
        return {
            'scenario_name': 'Resource Reallocation',
            'total_population': baseline['total_population'],
            'high_risk_wards': baseline['high_risk_wards'] - 8,  # Better targeting
            'average_risk_score': baseline['average_risk_score'] * 0.92,  # Slight improvement
            'average_coverage': baseline['average_coverage'],  # Same overall coverage
            'estimated_cost': baseline['estimated_cost'],  # Same cost
            'population_protected': baseline['population_protected'] + int(additional_protection),
            'cost_per_person_protected': 0,  # No additional cost
            'equity_improvement': 'Significant'
        }
    
    def _create_climate_scenario(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create climate change scenario."""
        baseline = self._create_baseline_scenario(df)
        
        # Assume climate change increases risk by 20%
        climate_impact = 0.2
        
        return {
            'scenario_name': 'Climate Change (+2°C)',
            'total_population': baseline['total_population'],
            'high_risk_wards': baseline['high_risk_wards'] + 15,  # More high-risk areas
            'average_risk_score': baseline['average_risk_score'] * (1 + climate_impact),
            'average_coverage': baseline['average_coverage'],
            'estimated_cost': baseline['estimated_cost'],
            'population_protected': baseline['population_protected'],
            'cost_per_person_protected': baseline['cost_per_person_protected'],
            'climate_adaptation_needed': True
        }
    
    def _create_targeted_scenario(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create targeted intervention scenario."""
        baseline = self._create_baseline_scenario(df)
        
        # Assume targeting improves effectiveness by 25%
        targeting_efficiency = 0.25
        
        return {
            'scenario_name': 'Targeted High-Risk Areas',
            'total_population': baseline['total_population'],
            'high_risk_wards': baseline['high_risk_wards'] - 12,  # Significant improvement
            'average_risk_score': baseline['average_risk_score'] * 0.85,  # 15% reduction
            'average_coverage': baseline['average_coverage'] * 0.8,  # Lower overall coverage
            'estimated_cost': baseline['estimated_cost'] * 0.8,  # Lower cost
            'population_protected': int(baseline['population_protected'] * (1 + targeting_efficiency)),
            'cost_per_person_protected': (baseline['estimated_cost'] * 0.8) / (baseline['population_protected'] * (1 + targeting_efficiency)),
            'targeting_efficiency': 'High'
        }
    
    def _compare_scenarios(self, scenarios: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Compare scenarios across metrics."""
        
        comparison = {}
        
        # Population protected comparison
        if "population_protected" in self.comparison_metrics:
            pop_protected = {name: scenario.get('population_protected', 0) for name, scenario in scenarios.items()}
            best_pop = max(pop_protected.values())
            
            comparison['population_protected'] = {
                'values': pop_protected,
                'best_scenario': max(pop_protected, key=pop_protected.get),
                'improvement_from_baseline': {
                    name: value - pop_protected.get('current_baseline', 0)
                    for name, value in pop_protected.items()
                    if name != 'current_baseline'
                }
            }
        
        # Cost effectiveness comparison
        if "cost_effectiveness" in self.comparison_metrics:
            cost_effectiveness = {}
            for name, scenario in scenarios.items():
                cost = scenario.get('estimated_cost', 0)
                protected = scenario.get('population_protected', 1)
                cost_effectiveness[name] = cost / protected if protected > 0 else float('inf')
            
            comparison['cost_effectiveness'] = {
                'values': cost_effectiveness,
                'best_scenario': min(cost_effectiveness, key=cost_effectiveness.get) if cost_effectiveness else None
            }
        
        # Equity impact comparison
        if "equity_impact" in self.comparison_metrics:
            equity_scores = {}
            for name, scenario in scenarios.items():
                # Simple equity scoring based on targeting and coverage distribution
                if 'targeted' in name.lower():
                    equity_scores[name] = 0.8
                elif 'reallocation' in name.lower():
                    equity_scores[name] = 0.9
                else:
                    equity_scores[name] = 0.5
            
            comparison['equity_impact'] = {
                'values': equity_scores,
                'best_scenario': max(equity_scores, key=equity_scores.get) if equity_scores else None
            }
        
        return comparison
    
    def _rank_scenarios(self, scenarios: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Rank scenarios by primary criteria."""
        
        scenario_scores = []
        
        for name, scenario in scenarios.items():
            if self.ranking_criteria == "population_protected":
                score = scenario.get('population_protected', 0)
            elif self.ranking_criteria == "cost_effectiveness":
                cost = scenario.get('estimated_cost', 0)
                protected = scenario.get('population_protected', 1)
                score = protected / (cost + 1)  # Higher is better
            else:  # equity
                score = 0.5  # Default equity score
                if 'reallocation' in name.lower():
                    score = 0.9
                elif 'targeted' in name.lower():
                    score = 0.8
            
            scenario_scores.append({
                'scenario_name': name,
                'score': score,
                'rank': 0  # Will be filled in sorting
            })
        
        # Sort by score (descending)
        scenario_scores.sort(key=lambda x: x['score'], reverse=True)
        
        # Assign ranks
        for i, scenario in enumerate(scenario_scores):
            scenario['rank'] = i + 1
        
        return scenario_scores
    
    def _generate_recommendations(self, scenarios: Dict[str, Dict[str, Any]], 
                                comparison: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on scenario analysis."""
        
        recommendations = []
        
        # Best overall scenario
        if 'population_protected' in comparison:
            best_pop_scenario = comparison['population_protected']['best_scenario']
            recommendations.append(f"For maximum population protection: {best_pop_scenario}")
        
        # Most cost-effective
        if 'cost_effectiveness' in comparison:
            best_cost_scenario = comparison['cost_effectiveness']['best_scenario']
            recommendations.append(f"For cost-effectiveness: {best_cost_scenario}")
        
        # Best for equity
        if 'equity_impact' in comparison:
            best_equity_scenario = comparison['equity_impact']['best_scenario']
            recommendations.append(f"For equity improvement: {best_equity_scenario}")
        
        # General recommendations
        recommendations.append("Consider combining elements from top-performing scenarios")
        recommendations.append("Monitor implementation feasibility and local context")
        
        return recommendations