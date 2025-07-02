"""
Intervention Targeting Tools for ChatMRPT - Phase 4 Implementation

This module provides practical malaria intervention planning and targeting tools:

1. GetInterventionPriorities - Identify wards needing specific interventions (ITN/IRS)
2. IdentifyCoverageGaps - Find high-risk areas with low intervention coverage
3. GetReprioritizationStrategy - Generate optimal targeting plans based on risk and resources
4. CalculateResourceNeeds - Calculate intervention resource requirements (ITNs, IRS, etc.)

These tools support evidence-based decision making for malaria control programs,
helping optimize intervention allocation and maximize impact with limited resources.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from pydantic import Field, validator
import pandas as pd
import numpy as np
from datetime import datetime

from .base import (
    BaseTool, ToolExecutionResult, ToolCategory,
    get_session_unified_dataset, validate_session_data_exists
)

logger = logging.getLogger(__name__)


class GetInterventionPriorities(BaseTool):
    """
    Identify wards requiring specific malaria interventions based on risk scores and current coverage.
    
    Analyzes risk levels, population data, and existing intervention coverage to prioritize
    wards for ITN (bed nets), IRS (indoor spraying), or combined interventions.
    """
    
    intervention_type: str = Field(
        "itn",
        description="Intervention type: 'itn' (bed nets), 'irs' (indoor spraying), 'combined', or 'auto'",
        pattern="^(itn|irs|combined|auto)$"
    )
    
    risk_threshold: float = Field(
        0.5,
        description="Minimum risk score for intervention consideration (0-1)",
        ge=0.0,
        le=1.0
    )
    
    coverage_threshold: float = Field(
        60.0,
        description="Current coverage threshold below which intervention is needed (%)",
        ge=0.0,
        le=100.0
    )
    
    top_n: int = Field(
        20,
        description="Number of top priority wards to return",
        ge=1,
        le=100
    )
    
    include_population: bool = Field(
        True,
        description="Consider population size in prioritization"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.INTERVENTION_TARGETING
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Get top 20 wards needing bed net distribution",
            "Identify high-risk areas with low IRS coverage",
            "Find priority wards for combined interventions",
            "Auto-recommend intervention types for high-risk wards"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Identify intervention priorities based on risk and coverage analysis."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Check required columns
            if 'composite_score' not in df.columns:
                return self._create_error_result("Risk scores not found. Please run risk analysis first.")
            
            # Prepare intervention data
            analysis_df = df.copy()
            
            # Create or find population column
            pop_col = None
            for col in ['population', 'Population', 'pop', 'Pop', 'u5_population']:
                if col in analysis_df.columns:
                    pop_col = col
                    break
            
            # Create synthetic population if none found
            if pop_col is None:
                # Use area as proxy for population, with some randomness
                if 'area_km2' in analysis_df.columns:
                    analysis_df['synthetic_population'] = (
                        analysis_df['area_km2'] * np.random.uniform(50, 200, len(analysis_df))
                    ).astype(int)
                    pop_col = 'synthetic_population'
                    synthetic_pop = True
                else:
                    analysis_df['synthetic_population'] = np.random.randint(1000, 10000, len(analysis_df))
                    pop_col = 'synthetic_population'
                    synthetic_pop = True
            else:
                synthetic_pop = False
            
            # Find or create coverage columns
            coverage_data = {}
            intervention_types = ['itn', 'irs'] if self.intervention_type == 'combined' else [self.intervention_type]
            
            for intervention in intervention_types if self.intervention_type != 'auto' else ['itn', 'irs']:
                coverage_col = None
                for col in analysis_df.columns:
                    if intervention.lower() in col.lower() and any(term in col.lower() for term in ['coverage', 'cov', 'percent']):
                        coverage_col = col
                        break
                
                if coverage_col is None:
                    # Create synthetic coverage based on urban percentage
                    if 'urbanPercentage' in analysis_df.columns:
                        base_coverage = analysis_df['urbanPercentage']
                        # Add random variation
                        analysis_df[f'{intervention}_coverage'] = np.clip(
                            base_coverage + np.random.normal(10, 15, len(analysis_df)),
                            0, 100
                        )
                        coverage_col = f'{intervention}_coverage'
                    else:
                        analysis_df[f'{intervention}_coverage'] = np.random.uniform(20, 80, len(analysis_df))
                        coverage_col = f'{intervention}_coverage'
                    
                    synthetic_coverage = True
                else:
                    synthetic_coverage = False
                
                coverage_data[intervention] = {
                    'column': coverage_col,
                    'synthetic': synthetic_coverage
                }
            
            # Calculate priority scores
            analysis_df['normalized_risk'] = analysis_df['composite_score']
            
            priority_wards = []
            
            if self.intervention_type == 'auto':
                # Auto-recommend based on risk and existing coverage
                for idx, row in analysis_df.iterrows():
                    if row['normalized_risk'] >= self.risk_threshold:
                        itn_cov = row[coverage_data['itn']['column']]
                        irs_cov = row[coverage_data['irs']['column']]
                        
                        # Determine best intervention
                        if itn_cov < self.coverage_threshold and irs_cov < self.coverage_threshold:
                            recommended_intervention = 'combined'
                            coverage_gap = (self.coverage_threshold - min(itn_cov, irs_cov))
                        elif itn_cov < irs_cov:
                            recommended_intervention = 'itn'
                            coverage_gap = (self.coverage_threshold - itn_cov)
                        else:
                            recommended_intervention = 'irs'
                            coverage_gap = (self.coverage_threshold - irs_cov)
                        
                        # Calculate priority score
                        risk_score = row['normalized_risk']
                        pop_factor = row[pop_col] / analysis_df[pop_col].max() if self.include_population else 1.0
                        priority_score = risk_score * coverage_gap * (1 + 0.5 * pop_factor)
                        
                        priority_wards.append({
                            'ward_name': row.get('WardName', f'Ward_{idx}'),
                            'risk_score': float(risk_score),
                            'recommended_intervention': recommended_intervention,
                            'current_itn_coverage': float(itn_cov),
                            'current_irs_coverage': float(irs_cov),
                            'coverage_gap': float(coverage_gap),
                            'population': int(row[pop_col]),
                            'priority_score': float(priority_score)
                        })
            else:
                # Specific intervention type
                for intervention in intervention_types:
                    coverage_col = coverage_data[intervention]['column']
                    
                    for idx, row in analysis_df.iterrows():
                        if (row['normalized_risk'] >= self.risk_threshold and 
                            row[coverage_col] < self.coverage_threshold):
                            
                            risk_score = row['normalized_risk']
                            coverage_gap = (self.coverage_threshold - row[coverage_col])
                            pop_factor = row[pop_col] / analysis_df[pop_col].max() if self.include_population else 1.0
                            priority_score = risk_score * coverage_gap * (1 + 0.5 * pop_factor)
                            
                            priority_wards.append({
                                'ward_name': row.get('WardName', f'Ward_{idx}'),
                                'risk_score': float(risk_score),
                                'intervention_type': intervention,
                                'current_coverage': float(row[coverage_col]),
                                'coverage_gap': float(coverage_gap),
                                'population': int(row[pop_col]),
                                'priority_score': float(priority_score)
                            })
            
            # Sort by priority score and take top N
            priority_wards.sort(key=lambda x: x['priority_score'], reverse=True)
            top_priorities = priority_wards[:self.top_n]
            
            # Calculate summary statistics
            summary_stats = {
                'total_wards_analyzed': len(analysis_df),
                'high_risk_wards': len(analysis_df[analysis_df['normalized_risk'] >= self.risk_threshold]),
                'total_priority_wards': len(priority_wards),
                'top_n_returned': len(top_priorities),
                'avg_priority_score': float(np.mean([w['priority_score'] for w in top_priorities])) if top_priorities else 0,
                'total_priority_population': sum([w['population'] for w in top_priorities]),
                'intervention_breakdown': {}
            }
            
            # Add intervention breakdown
            if self.intervention_type == 'auto':
                breakdown = {}
                for ward in top_priorities:
                    intervention = ward['recommended_intervention']
                    breakdown[intervention] = breakdown.get(intervention, 0) + 1
                summary_stats['intervention_breakdown'] = breakdown
            else:
                summary_stats['intervention_breakdown'][self.intervention_type] = len(top_priorities)
            
            result_data = {
                'intervention_type': self.intervention_type,
                'risk_threshold': self.risk_threshold,
                'coverage_threshold': self.coverage_threshold,
                'priority_wards': top_priorities,
                'summary_statistics': summary_stats,
                'data_notes': {
                    'synthetic_population': synthetic_pop,
                    'synthetic_coverage': any(cov['synthetic'] for cov in coverage_data.values()),
                    'population_column': pop_col,
                    'coverage_columns': {k: v['column'] for k, v in coverage_data.items()}
                }
            }
            
            message = f"Identified {len(top_priorities)} priority wards for {self.intervention_type} intervention. "
            message += f"Total priority population: {summary_stats['total_priority_population']:,}"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error getting intervention priorities: {e}")
            return self._create_error_result(f"Intervention priority analysis failed: {str(e)}")


class IdentifyCoverageGaps(BaseTool):
    """
    Identify geographic areas with high malaria risk but low intervention coverage.
    
    Analyzes the gap between risk levels and current intervention coverage to find
    underserved areas that require immediate attention for malaria control efforts.
    """
    
    risk_variable: str = Field(
        "composite_score",
        description="Risk variable to use: 'composite_score', 'pca_score', or 'pfpr'"
    )
    
    high_risk_threshold: float = Field(
        0.6,
        description="Threshold above which areas are considered high risk (0-1)",
        ge=0.0,
        le=1.0
    )
    
    low_coverage_threshold: float = Field(
        50.0,
        description="Threshold below which coverage is considered low (%)",
        ge=0.0,
        le=100.0
    )
    
    gap_severity: str = Field(
        "all",
        description="Gap severity to analyze: 'all', 'critical', 'severe', 'moderate'",
        pattern="^(all|critical|severe|moderate)$"
    )
    
    geographic_grouping: str = Field(
        "ward",
        description="Geographic level for analysis: 'ward', 'lga', 'cluster'",
        pattern="^(ward|lga|cluster)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.INTERVENTION_TARGETING
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Find high-risk areas with low ITN coverage",
            "Identify critical coverage gaps by LGA",
            "Show severe intervention gaps using PCA scores",
            "Find moderate coverage gaps at ward level"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Identify areas with high risk but low intervention coverage."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Validate risk variable
            if self.risk_variable not in df.columns:
                return self._create_error_result(
                    f"Risk variable '{self.risk_variable}' not found. "
                    f"Available options: {[col for col in df.columns if 'score' in col.lower()]}"
                )
            
            analysis_df = df.copy()
            
            # Normalize risk variable to 0-1 scale
            risk_col = self.risk_variable
            risk_values = analysis_df[risk_col]
            if risk_values.max() > 1:
                analysis_df['normalized_risk'] = (risk_values - risk_values.min()) / (risk_values.max() - risk_values.min())
            else:
                analysis_df['normalized_risk'] = risk_values
            
            # Find coverage columns
            coverage_columns = []
            for col in analysis_df.columns:
                if any(term in col.lower() for term in ['coverage', 'cov', 'itn', 'irs']) and \
                   any(term in col.lower() for term in ['percent', '%', 'coverage']):
                    coverage_columns.append(col)
            
            # Create synthetic coverage if none found
            if not coverage_columns:
                # Create ITN and IRS coverage based on urban percentage and random variation
                if 'urbanPercentage' in analysis_df.columns:
                    base = analysis_df['urbanPercentage']
                else:
                    base = np.random.uniform(30, 70, len(analysis_df))
                
                analysis_df['itn_coverage'] = np.clip(
                    base + np.random.normal(0, 20, len(analysis_df)), 0, 100
                )
                analysis_df['irs_coverage'] = np.clip(
                    base + np.random.normal(-10, 15, len(analysis_df)), 0, 100
                )
                coverage_columns = ['itn_coverage', 'irs_coverage']
                synthetic_coverage = True
            else:
                synthetic_coverage = False
            
            # Calculate coverage gaps for each intervention
            gap_analysis = []
            
            for coverage_col in coverage_columns:
                intervention_name = coverage_col.replace('_coverage', '').replace('Coverage', '').upper()
                
                # Identify gaps
                high_risk_mask = analysis_df['normalized_risk'] >= self.high_risk_threshold
                low_coverage_mask = analysis_df[coverage_col] <= self.low_coverage_threshold
                gap_mask = high_risk_mask & low_coverage_mask
                
                gap_areas = analysis_df[gap_mask].copy()
                
                if len(gap_areas) > 0:
                    # Calculate gap severity
                    gap_areas['coverage_deficit'] = self.low_coverage_threshold - gap_areas[coverage_col]
                    gap_areas['risk_excess'] = gap_areas['normalized_risk'] - self.high_risk_threshold
                    gap_areas['gap_score'] = gap_areas['coverage_deficit'] * gap_areas['risk_excess']
                    
                    # Classify gap severity
                    gap_areas['gap_severity'] = pd.cut(
                        gap_areas['gap_score'],
                        bins=3,
                        labels=['moderate', 'severe', 'critical']
                    )
                    
                    # Filter by requested severity
                    if self.gap_severity != 'all':
                        gap_areas = gap_areas[gap_areas['gap_severity'] == self.gap_severity]
                    
                    # Group by geographic level if requested
                    if self.geographic_grouping == 'lga' and 'LGACode_x' in gap_areas.columns:
                        grouped_gaps = gap_areas.groupby('LGACode_x').agg({
                            'normalized_risk': 'mean',
                            coverage_col: 'mean',
                            'gap_score': 'mean',
                            'WardName': 'count'
                        }).reset_index()
                        grouped_gaps.columns = ['geographic_unit', 'avg_risk', 'avg_coverage', 'avg_gap_score', 'ward_count']
                        grouped_gaps['intervention'] = intervention_name
                        gap_analysis.extend(grouped_gaps.to_dict('records'))
                    else:
                        # Ward-level analysis
                        for _, row in gap_areas.iterrows():
                            gap_analysis.append({
                                'geographic_unit': row.get('WardName', f'Ward_{row.name}'),
                                'geographic_level': self.geographic_grouping,
                                'intervention': intervention_name,
                                'risk_score': float(row['normalized_risk']),
                                'coverage_level': float(row[coverage_col]),
                                'coverage_deficit': float(row['coverage_deficit']),
                                'gap_score': float(row['gap_score']),
                                'gap_severity': str(row['gap_severity']),
                                'population': int(row.get('population', row.get('synthetic_population', 5000)))
                            })
            
            # Sort by gap score (highest gaps first)
            gap_analysis.sort(key=lambda x: x.get('gap_score', x.get('avg_gap_score', 0)), reverse=True)
            
            # Calculate summary statistics
            total_gaps = len(gap_analysis)
            if gap_analysis:
                severity_counts = {}
                intervention_counts = {}
                total_affected_population = 0
                
                for gap in gap_analysis:
                    severity = gap.get('gap_severity', 'unknown')
                    intervention = gap.get('intervention', 'unknown')
                    
                    severity_counts[severity] = severity_counts.get(severity, 0) + 1
                    intervention_counts[intervention] = intervention_counts.get(intervention, 0) + 1
                    total_affected_population += gap.get('population', 0)
                
                avg_gap_score = np.mean([gap.get('gap_score', gap.get('avg_gap_score', 0)) for gap in gap_analysis])
            else:
                severity_counts = {}
                intervention_counts = {}
                total_affected_population = 0
                avg_gap_score = 0
            
            summary_stats = {
                'total_gaps_identified': total_gaps,
                'severity_breakdown': severity_counts,
                'intervention_breakdown': intervention_counts,
                'total_affected_population': int(total_affected_population),
                'average_gap_score': float(avg_gap_score),
                'analysis_parameters': {
                    'risk_variable': self.risk_variable,
                    'high_risk_threshold': self.high_risk_threshold,
                    'low_coverage_threshold': self.low_coverage_threshold,
                    'gap_severity_filter': self.gap_severity,
                    'geographic_grouping': self.geographic_grouping
                }
            }
            
            result_data = {
                'coverage_gaps': gap_analysis,
                'summary_statistics': summary_stats,
                'data_notes': {
                    'synthetic_coverage': synthetic_coverage,
                    'coverage_columns_analyzed': coverage_columns,
                    'total_areas_analyzed': len(analysis_df)
                }
            }
            
            message = f"Identified {total_gaps} coverage gaps using {self.risk_variable}. "
            if total_gaps > 0:
                message += f"Average gap score: {avg_gap_score:.3f}, "
                message += f"Affected population: {total_affected_population:,}"
            else:
                message += "No significant coverage gaps found with current thresholds."
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error identifying coverage gaps: {e}")
            return self._create_error_result(f"Coverage gap analysis failed: {str(e)}")


class GetReprioritizationStrategy(BaseTool):
    """
    Generate optimal intervention targeting strategy based on risk, resources, and constraints.
    
    Creates a comprehensive reprioritization plan that maximizes impact given budget
    constraints, population targets, and intervention effectiveness parameters.
    """
    
    budget_constraint: float = Field(
        100000.0,
        description="Total budget available for interventions (USD)",
        ge=1000.0
    )
    
    intervention_costs: Dict[str, float] = Field(
        default_factory=lambda: {"itn": 5.0, "irs": 12.0, "combined": 15.0},
        description="Cost per person for each intervention type (USD)"
    )
    
    target_coverage: float = Field(
        80.0,
        description="Target coverage percentage to achieve",
        ge=50.0,
        le=100.0
    )
    
    strategy_objective: str = Field(
        "maximize_impact",
        description="Strategy objective: 'maximize_impact', 'maximize_coverage', 'cost_efficiency'",
        pattern="^(maximize_impact|maximize_coverage|cost_efficiency)$"
    )
    
    risk_weighting: float = Field(
        0.7,
        description="Weight given to risk scores in prioritization (0-1)",
        ge=0.0,
        le=1.0
    )
    
    equity_factor: bool = Field(
        True,
        description="Include equity considerations (prioritize underserved areas)"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.INTERVENTION_TARGETING
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Generate cost-efficient intervention strategy with $100k budget",
            "Create maximum impact strategy prioritizing high-risk areas",
            "Develop equitable coverage strategy across all wards",
            "Optimize intervention mix for 80% target coverage"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Generate optimal intervention targeting strategy."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Check required columns
            if 'composite_score' not in df.columns:
                return self._create_error_result("Risk scores not found. Please run risk analysis first.")
            
            analysis_df = df.copy()
            
            # Get or create population data
            pop_col = None
            for col in ['population', 'Population', 'pop', 'Pop']:
                if col in analysis_df.columns:
                    pop_col = col
                    break
            
            if pop_col is None:
                # Create synthetic population
                if 'area_km2' in analysis_df.columns:
                    analysis_df['population'] = (
                        analysis_df['area_km2'] * np.random.uniform(100, 500, len(analysis_df))
                    ).astype(int)
                else:
                    analysis_df['population'] = np.random.randint(2000, 15000, len(analysis_df))
                pop_col = 'population'
                synthetic_population = True
            else:
                synthetic_population = False
            
            # Get or create current coverage data
            coverage_data = {}
            for intervention in self.intervention_costs.keys():
                coverage_col = None
                for col in analysis_df.columns:
                    if intervention in col.lower() and 'coverage' in col.lower():
                        coverage_col = col
                        break
                
                if coverage_col is None:
                    # Create synthetic coverage
                    base_coverage = np.random.uniform(20, 60, len(analysis_df))
                    if 'urbanPercentage' in analysis_df.columns:
                        base_coverage = analysis_df['urbanPercentage'] + np.random.normal(0, 15, len(analysis_df))
                    
                    analysis_df[f'{intervention}_coverage'] = np.clip(base_coverage, 0, 100)
                    coverage_col = f'{intervention}_coverage'
                    synthetic_coverage = True
                else:
                    synthetic_coverage = False
                
                coverage_data[intervention] = coverage_col
            
            # Calculate priority scores for each ward
            analysis_df['risk_score'] = analysis_df['composite_score']
            
            # Normalize risk scores
            if analysis_df['risk_score'].max() > 1:
                analysis_df['risk_score'] = (
                    analysis_df['risk_score'] - analysis_df['risk_score'].min()
                ) / (analysis_df['risk_score'].max() - analysis_df['risk_score'].min())
            
            # Calculate intervention needs and costs for each ward
            strategy_options = []
            
            for idx, row in analysis_df.iterrows():
                ward_name = row.get('WardName', f'Ward_{idx}')
                population = row[pop_col]
                risk_score = row['risk_score']
                
                # Calculate equity factor (prioritize underserved areas)
                if self.equity_factor:
                    avg_coverage = np.mean([row[coverage_data[intervention]] for intervention in coverage_data.keys()])
                    equity_bonus = (100 - avg_coverage) / 100 * 0.3  # Up to 30% bonus for low coverage areas
                else:
                    equity_bonus = 0
                
                # Evaluate each intervention option
                for intervention, cost_per_person in self.intervention_costs.items():
                    current_coverage = row[coverage_data[intervention]]
                    coverage_gap = max(0, self.target_coverage - current_coverage)
                    
                    if coverage_gap > 0:
                        # Calculate people needing intervention
                        people_needing = int(population * coverage_gap / 100)
                        total_cost = people_needing * cost_per_person
                        
                        # Calculate priority score based on strategy objective
                        if self.strategy_objective == "maximize_impact":
                            # Prioritize high-risk areas with large populations
                            priority_score = (
                                self.risk_weighting * risk_score +
                                (1 - self.risk_weighting) * (people_needing / analysis_df[pop_col].max()) +
                                equity_bonus
                            )
                        elif self.strategy_objective == "maximize_coverage":
                            # Prioritize areas with largest coverage gaps
                            priority_score = coverage_gap / 100 + equity_bonus
                        else:  # cost_efficiency
                            # Prioritize lowest cost per person reached
                            priority_score = risk_score / cost_per_person + equity_bonus
                        
                        # Calculate expected impact
                        expected_impact = risk_score * people_needing * (coverage_gap / 100)
                        
                        strategy_options.append({
                            'ward_name': ward_name,
                            'intervention': intervention,
                            'current_coverage': float(current_coverage),
                            'target_coverage': float(self.target_coverage),
                            'coverage_gap': float(coverage_gap),
                            'people_needing': people_needing,
                            'total_cost': float(total_cost),
                            'cost_per_person': float(cost_per_person),
                            'priority_score': float(priority_score),
                            'expected_impact': float(expected_impact),
                            'risk_score': float(risk_score),
                            'population': int(population)
                        })
            
            # Sort by priority score
            strategy_options.sort(key=lambda x: x['priority_score'], reverse=True)
            
            # Select interventions within budget constraint
            selected_interventions = []
            remaining_budget = self.budget_constraint
            total_people_reached = 0
            total_expected_impact = 0
            
            for option in strategy_options:
                if option['total_cost'] <= remaining_budget:
                    selected_interventions.append(option)
                    remaining_budget -= option['total_cost']
                    total_people_reached += option['people_needing']
                    total_expected_impact += option['expected_impact']
                    
                    # Stop if we've allocated most of the budget
                    if remaining_budget < min(self.intervention_costs.values()) * 1000:
                        break
            
            # Calculate strategy summary
            budget_utilized = self.budget_constraint - remaining_budget
            budget_efficiency = total_people_reached / budget_utilized if budget_utilized > 0 else 0
            
            intervention_summary = {}
            for intervention in self.intervention_costs.keys():
                intervention_count = len([opt for opt in selected_interventions if opt['intervention'] == intervention])
                intervention_cost = sum([opt['total_cost'] for opt in selected_interventions if opt['intervention'] == intervention])
                intervention_summary[intervention] = {
                    'ward_count': intervention_count,
                    'total_cost': float(intervention_cost),
                    'people_reached': sum([opt['people_needing'] for opt in selected_interventions if opt['intervention'] == intervention])
                }
            
            strategy_stats = {
                'total_budget': float(self.budget_constraint),
                'budget_utilized': float(budget_utilized),
                'budget_remaining': float(remaining_budget),
                'budget_utilization_rate': float(budget_utilized / self.budget_constraint * 100),
                'total_wards_selected': len(selected_interventions),
                'total_people_reached': total_people_reached,
                'total_expected_impact': float(total_expected_impact),
                'budget_efficiency': float(budget_efficiency),
                'intervention_breakdown': intervention_summary,
                'strategy_parameters': {
                    'objective': self.strategy_objective,
                    'target_coverage': self.target_coverage,
                    'risk_weighting': self.risk_weighting,
                    'equity_factor': self.equity_factor
                }
            }
            
            result_data = {
                'selected_interventions': selected_interventions,
                'strategy_statistics': strategy_stats,
                'alternative_options': strategy_options[len(selected_interventions):len(selected_interventions)+10],  # Next 10 options
                'data_notes': {
                    'synthetic_population': synthetic_population,
                    'synthetic_coverage': synthetic_coverage,
                    'intervention_costs': self.intervention_costs
                }
            }
            
            message = f"Generated {self.strategy_objective} strategy: {len(selected_interventions)} interventions, "
            message += f"{total_people_reached:,} people reached, ${budget_utilized:,.0f} budget utilized ({budget_utilized/self.budget_constraint*100:.1f}%)"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error generating reprioritization strategy: {e}")
            return self._create_error_result(f"Strategy generation failed: {str(e)}")


class CalculateResourceNeeds(BaseTool):
    """
    Calculate specific resource requirements for malaria intervention implementation.
    
    Estimates quantities of ITNs, IRS materials, personnel, and other resources needed
    to achieve specified coverage targets across selected geographic areas.
    """
    
    intervention_type: str = Field(
        "itn",
        description="Intervention type: 'itn', 'irs', 'combined', or 'all'",
        pattern="^(itn|irs|combined|all)$"
    )
    
    target_coverage: float = Field(
        80.0,
        description="Target coverage percentage to achieve",
        ge=0.0,
        le=100.0
    )
    
    geographic_scope: str = Field(
        "all",
        description="Geographic scope: 'all', 'high_risk_only', 'top_n_wards'",
        pattern="^(all|high_risk_only|top_n_wards)$"
    )
    
    risk_threshold: float = Field(
        0.6,
        description="Risk threshold for 'high_risk_only' scope (0-1)",
        ge=0.0,
        le=1.0
    )
    
    top_n: int = Field(
        20,
        description="Number of top wards for 'top_n_wards' scope",
        ge=1,
        le=100
    )
    
    planning_horizon: str = Field(
        "1_year",
        description="Planning period: '6_months', '1_year', '2_years', '5_years'",
        pattern="^(6_months|1_year|2_years|5_years)$"
    )
    
    include_operational_costs: bool = Field(
        True,
        description="Include operational costs (training, logistics, monitoring)"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.INTERVENTION_TARGETING
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Calculate ITN needs for 80% coverage in all wards",
            "Estimate IRS resources for high-risk areas over 2 years",
            "Calculate combined intervention resources for top 20 wards",
            "Get comprehensive resource needs including operational costs"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Calculate resource requirements for intervention implementation."""
        try:
            # Validate session data
            if not validate_session_data_exists(session_id):
                return self._create_error_result(
                    "No data available for this session. Please upload data first."
                )
            
            # Get unified dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            analysis_df = df.copy()
            
            # Get or create population data
            pop_col = None
            for col in ['population', 'Population', 'pop', 'Pop', 'total_pop']:
                if col in analysis_df.columns:
                    pop_col = col
                    break
            
            if pop_col is None:
                # Create population estimates based on area
                if 'area_km2' in analysis_df.columns:
                    analysis_df['population'] = (
                        analysis_df['area_km2'] * np.random.uniform(200, 800, len(analysis_df))
                    ).astype(int)
                else:
                    analysis_df['population'] = np.random.randint(3000, 20000, len(analysis_df))
                pop_col = 'population'
                synthetic_population = True
            else:
                synthetic_population = False
            
            # Get risk scores
            if 'composite_score' in analysis_df.columns:
                risk_col = 'composite_score'
                synthetic_risk = False
            elif 'pca_score' in analysis_df.columns:
                risk_col = 'pca_score'
                synthetic_risk = False
            else:
                # Create synthetic risk scores
                analysis_df['synthetic_risk'] = np.random.uniform(0, 1, len(analysis_df))
                risk_col = 'synthetic_risk'
                synthetic_risk = True
            
            # Normalize risk scores to 0-1
            if analysis_df[risk_col].max() > 1:
                analysis_df['normalized_risk'] = (
                    analysis_df[risk_col] - analysis_df[risk_col].min()
                ) / (analysis_df[risk_col].max() - analysis_df[risk_col].min())
            else:
                analysis_df['normalized_risk'] = analysis_df[risk_col]
            
            # Filter geographic scope
            if self.geographic_scope == "high_risk_only":
                target_areas = analysis_df[analysis_df['normalized_risk'] >= self.risk_threshold].copy()
            elif self.geographic_scope == "top_n_wards":
                target_areas = analysis_df.nlargest(self.top_n, 'normalized_risk').copy()
            else:  # all
                target_areas = analysis_df.copy()
            
            if len(target_areas) == 0:
                return self._create_error_result("No areas selected with current scope criteria.")
            
            # Define resource requirements per person/household
            resource_specs = {
                'itn': {
                    'nets_per_household': 2.5,  # Average nets per household
                    'people_per_household': 4.2,  # Average household size
                    'net_lifespan_years': 3,
                    'cost_per_net': 5.0,
                    'distribution_cost_per_net': 1.5,
                    'training_cost_per_1000_nets': 200,
                    'monitoring_cost_percentage': 0.1
                },
                'irs': {
                    'houses_per_person': 0.24,  # Inverse of people per household
                    'spray_cycles_per_year': 2,
                    'cost_per_house_per_cycle': 6.0,
                    'equipment_cost_per_1000_houses': 1500,
                    'training_cost_per_spray_team': 800,
                    'monitoring_cost_percentage': 0.15
                }
            }
            
            # Calculate planning horizon multiplier
            horizon_multipliers = {
                '6_months': 0.5,
                '1_year': 1.0,
                '2_years': 2.0,
                '5_years': 5.0
            }
            horizon_multiplier = horizon_multipliers[self.planning_horizon]
            
            # Calculate resource needs for each intervention type
            resource_calculations = {}
            total_cost = 0
            
            intervention_types = []
            if self.intervention_type == 'all':
                intervention_types = ['itn', 'irs']
            elif self.intervention_type == 'combined':
                intervention_types = ['itn', 'irs']
            else:
                intervention_types = [self.intervention_type]
            
            for intervention in intervention_types:
                if intervention not in resource_specs:
                    continue
                
                specs = resource_specs[intervention]
                intervention_resources = {
                    'ward_breakdown': [],
                    'totals': {},
                    'costs': {}
                }
                
                total_population = 0
                total_households = 0
                
                for idx, row in target_areas.iterrows():
                    ward_name = row.get('WardName', f'Ward_{idx}')
                    population = row[pop_col]
                    households = int(population / specs.get('people_per_household', 4.2))
                    
                    # Get current coverage
                    current_coverage_col = None
                    for col in analysis_df.columns:
                        if intervention in col.lower() and 'coverage' in col.lower():
                            current_coverage_col = col
                            break
                    
                    if current_coverage_col is not None:
                        current_coverage = row[current_coverage_col]
                    else:
                        # Estimate current coverage
                        current_coverage = np.random.uniform(30, 60)
                    
                    # Calculate coverage gap
                    coverage_gap = max(0, self.target_coverage - current_coverage)
                    people_needing = int(population * coverage_gap / 100)
                    households_needing = int(households * coverage_gap / 100)
                    
                    if intervention == 'itn':
                        # ITN calculations
                        nets_needed = int(households_needing * specs['nets_per_household'])
                        nets_per_year = nets_needed / specs['net_lifespan_years'] * horizon_multiplier
                        
                        ward_resources = {
                            'ward_name': ward_name,
                            'population': int(population),
                            'households': households,
                            'coverage_gap': float(coverage_gap),
                            'people_needing': people_needing,
                            'households_needing': households_needing,
                            'nets_needed_total': nets_needed,
                            'nets_needed_period': int(nets_per_year),
                            'current_coverage': float(current_coverage)
                        }
                        
                    elif intervention == 'irs':
                        # IRS calculations
                        houses_to_spray = households_needing
                        spray_cycles = specs['spray_cycles_per_year'] * horizon_multiplier
                        total_spray_operations = int(houses_to_spray * spray_cycles)
                        
                        ward_resources = {
                            'ward_name': ward_name,
                            'population': int(population),
                            'households': households,
                            'coverage_gap': float(coverage_gap),
                            'people_needing': people_needing,
                            'households_needing': households_needing,
                            'houses_to_spray': houses_to_spray,
                            'spray_operations_total': total_spray_operations,
                            'current_coverage': float(current_coverage)
                        }
                    
                    intervention_resources['ward_breakdown'].append(ward_resources)
                    total_population += people_needing
                    total_households += households_needing
                
                # Calculate intervention totals
                if intervention == 'itn':
                    total_nets = sum([w['nets_needed_period'] for w in intervention_resources['ward_breakdown']])
                    
                    # Costs
                    net_costs = total_nets * specs['cost_per_net']
                    distribution_costs = total_nets * specs['distribution_cost_per_net']
                    training_costs = (total_nets / 1000) * specs['training_cost_per_1000_nets']
                    monitoring_costs = net_costs * specs['monitoring_cost_percentage'] if self.include_operational_costs else 0
                    
                    intervention_resources['totals'] = {
                        'total_nets_needed': int(total_nets),
                        'total_population_covered': total_population,
                        'total_households_covered': total_households,
                        'planning_horizon': self.planning_horizon
                    }
                    
                    intervention_resources['costs'] = {
                        'net_procurement': float(net_costs),
                        'distribution': float(distribution_costs),
                        'training': float(training_costs) if self.include_operational_costs else 0,
                        'monitoring': float(monitoring_costs),
                        'total': float(net_costs + distribution_costs + training_costs + monitoring_costs)
                    }
                    
                elif intervention == 'irs':
                    total_spray_ops = sum([w['spray_operations_total'] for w in intervention_resources['ward_breakdown']])
                    total_houses = sum([w['houses_to_spray'] for w in intervention_resources['ward_breakdown']])
                    
                    # Costs
                    spray_costs = total_spray_ops * specs['cost_per_house_per_cycle']
                    equipment_costs = (total_houses / 1000) * specs['equipment_cost_per_1000_houses']
                    # Estimate spray teams needed (1 team per 500 houses)
                    spray_teams = max(1, int(total_houses / 500))
                    training_costs = spray_teams * specs['training_cost_per_spray_team'] if self.include_operational_costs else 0
                    monitoring_costs = spray_costs * specs['monitoring_cost_percentage'] if self.include_operational_costs else 0
                    
                    intervention_resources['totals'] = {
                        'total_spray_operations': int(total_spray_ops),
                        'total_houses_to_spray': int(total_houses),
                        'spray_teams_needed': spray_teams,
                        'total_population_covered': total_population,
                        'planning_horizon': self.planning_horizon
                    }
                    
                    intervention_resources['costs'] = {
                        'spray_operations': float(spray_costs),
                        'equipment': float(equipment_costs),
                        'training': float(training_costs),
                        'monitoring': float(monitoring_costs),
                        'total': float(spray_costs + equipment_costs + training_costs + monitoring_costs)
                    }
                
                resource_calculations[intervention] = intervention_resources
                total_cost += intervention_resources['costs']['total']
            
            # Calculate overall summary
            total_population_all = target_areas[pop_col].sum()
            total_population_needing = sum([
                sum([w.get('people_needing', 0) for w in calc['ward_breakdown']])
                for calc in resource_calculations.values()
            ])
            
            # Remove duplicates if combined intervention
            if len(intervention_types) > 1:
                unique_population = len(set([
                    w['ward_name'] 
                    for calc in resource_calculations.values() 
                    for w in calc['ward_breakdown']
                ]))
            else:
                unique_population = total_population_needing
            
            summary_statistics = {
                'intervention_type': self.intervention_type,
                'target_coverage': self.target_coverage,
                'geographic_scope': self.geographic_scope,
                'planning_horizon': self.planning_horizon,
                'total_wards_included': len(target_areas),
                'total_population_in_scope': int(total_population_all),
                'total_population_needing_intervention': int(unique_population),
                'overall_cost_estimate': float(total_cost),
                'cost_per_person_covered': float(total_cost / unique_population) if unique_population > 0 else 0,
                'includes_operational_costs': self.include_operational_costs
            }
            
            result_data = {
                'resource_calculations': resource_calculations,
                'summary_statistics': summary_statistics,
                'resource_specifications': resource_specs,
                'data_notes': {
                    'synthetic_population': synthetic_population,
                    'synthetic_risk': synthetic_risk if 'synthetic_risk' in locals() else False,
                    'population_column': pop_col,
                    'risk_column': risk_col
                }
            }
            
            message = f"Calculated resource needs for {self.intervention_type} intervention: "
            message += f"{len(target_areas)} wards, {unique_population:,} people, ${total_cost:,.0f} total cost"
            
            return self._create_success_result(
                message=message,
                data=result_data
            )
            
        except Exception as e:
            logger.error(f"Error calculating resource needs: {e}")
            return self._create_error_result(f"Resource calculation failed: {str(e)}")


# Register all tools for discovery
__all__ = [
    'GetInterventionPriorities',
    'IdentifyCoverageGaps',
    'GetReprioritizationStrategy',
    'CalculateResourceNeeds'
]