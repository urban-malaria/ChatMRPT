"""
Scenario Simulation Tools for ChatMRPT

These tools provide "what-if" analysis capabilities, allowing users to simulate
changes to variables and see how they affect burden scores, rankings, and
intervention prioritization.
"""

import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[pd.DataFrame]:
    """Get the unified dataset from session."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            logger.debug(f"✅ Unified dataset loaded: {len(unified_gdf)} rows")
            return unified_gdf
        else:
            logger.error(f"❌ Unified dataset not found for session {session_id}")
            return None
    except Exception as e:
        logger.error(f"❌ Error accessing unified dataset: {e}")
        return None


def simulate_coverage_increase_impact(session_id: str, ward_name: str, 
                                    coverage_increase: float = 0.3,
                                    intervention_type: str = "ITN") -> Dict[str, Any]:
    """
    Simulate the impact of increasing intervention coverage in a specific ward.
    
    Handles requests like:
    - "If ITN coverage in Ward X increases by 30%, how does the reprioritization change?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Find the ward
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        ward_data = df[df[ward_col].str.contains(ward_name, case=False, na=False, regex=False)]
        
        if len(ward_data) == 0:
            return {"error": f"Ward '{ward_name}' not found in dataset"}
        
        if len(ward_data) > 1:
            ward_matches = ward_data[ward_col].tolist()
            return {"error": f"Multiple wards found: {ward_matches}. Please be more specific."}
        
        # Get original rankings
        original_rankings = df.copy()
        if 'composite_rank' in original_rankings.columns:
            original_rankings = original_rankings.sort_values('composite_rank')
        
        target_ward_idx = ward_data.index[0]
        original_rank = df.loc[target_ward_idx, 'composite_rank'] if 'composite_rank' in df.columns else None
        original_score = df.loc[target_ward_idx, 'composite_score'] if 'composite_score' in df.columns else None
        
        # Create scenario dataset
        scenario_df = df.copy()
        
        # Simulate impact of increased coverage on burden score
        # Assumption: Higher intervention coverage reduces malaria burden
        if 'composite_score' in scenario_df.columns:
            # Reduce burden score based on coverage increase (simple linear reduction)
            reduction_factor = coverage_increase * 0.5  # 30% coverage increase = 15% burden reduction
            scenario_df.loc[target_ward_idx, 'composite_score'] *= (1 - reduction_factor)
            
            # Recalculate rankings
            scenario_df['new_composite_rank'] = scenario_df['composite_score'].rank(ascending=False, method='min')
            scenario_df = scenario_df.sort_values('new_composite_rank')
        
        new_rank = scenario_df.loc[target_ward_idx, 'new_composite_rank'] if 'new_composite_rank' in scenario_df.columns else None
        new_score = scenario_df.loc[target_ward_idx, 'composite_score'] if 'composite_score' in scenario_df.columns else None
        
        # Calculate impact on other wards
        rank_changes = []
        if 'composite_rank' in df.columns and 'new_composite_rank' in scenario_df.columns:
            for idx, row in scenario_df.iterrows():
                if idx != target_ward_idx:  # Skip the target ward
                    old_rank = df.loc[idx, 'composite_rank']
                    new_rank_other = row['new_composite_rank']
                    if old_rank != new_rank_other:
                        rank_changes.append({
                            'ward_name': row[ward_col],
                            'old_rank': int(old_rank),
                            'new_rank': int(new_rank_other),
                            'rank_change': int(new_rank_other - old_rank)
                        })
        
        # Sort by impact magnitude
        rank_changes.sort(key=lambda x: abs(x['rank_change']), reverse=True)
        
        return {
            'status': 'success',
            'scenario': {
                'ward_name': ward_data.iloc[0][ward_col],
                'intervention_type': intervention_type,
                'coverage_increase': coverage_increase,
                'assumption': f"{coverage_increase*100}% coverage increase reduces burden by {coverage_increase*50}%"
            },
            'target_ward_impact': {
                'original_rank': int(original_rank) if original_rank else None,
                'new_rank': int(new_rank) if new_rank else None,
                'rank_improvement': int(original_rank - new_rank) if (original_rank and new_rank) else None,
                'original_score': float(original_score) if original_score else None,
                'new_score': float(new_score) if new_score else None,
                'score_improvement': float(original_score - new_score) if (original_score and new_score) else None
            },
            'ripple_effects': {
                'wards_affected': len(rank_changes),
                'significant_changes': rank_changes[:10],  # Top 10 most affected
                'summary': f"{len([c for c in rank_changes if c['rank_change'] > 0])} wards moved down in priority"
            }
        }
        
    except Exception as e:
        logger.error(f"Error simulating coverage increase: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def simulate_variable_exclusion(session_id: str, variable_to_exclude: str) -> Dict[str, Any]:
    """
    Simulate what happens if a specific variable is excluded from the analysis.
    
    Handles requests like:
    - "What happens if we exclude elevation as a covariate?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Find the variable to exclude
        variable_cols = [col for col in df.columns if variable_to_exclude.lower() in col.lower()]
        
        if not variable_cols:
            return {"error": f"Variable '{variable_to_exclude}' not found. Available variables: {list(df.columns)[:10]}..."}
        
        excluded_col = variable_cols[0]
        
        # Get original composite scores (if available)
        if 'composite_score' not in df.columns:
            return {"error": "No composite scores available to recalculate"}
        
        # Create a simplified recalculation by removing the excluded variable's influence
        # This is a basic simulation - in reality, this would require re-running the full analysis
        scenario_df = df.copy()
        
        # Identify variables that likely contributed to the composite score
        potential_factors = []
        for col in df.columns:
            if any(term in col.lower() for term in ['rainfall', 'temperature', 'elevation', 'flood', 'vegetation', 'tpr', 'distance']):
                potential_factors.append(col)
        
        if excluded_col not in potential_factors:
            return {"error": f"'{excluded_col}' does not appear to be a factor in the composite score"}
        
        # Simple simulation: reduce weight of excluded variable
        # Assumption: excluded variable contributed approximately 1/n of the total score
        n_factors = len(potential_factors)
        weight_reduction = 1.0 / n_factors if n_factors > 0 else 0.1
        
        # Normalize excluded variable and reduce its contribution
        if excluded_col in scenario_df.columns:
            excluded_var_normalized = (scenario_df[excluded_col] - scenario_df[excluded_col].min()) / (scenario_df[excluded_col].max() - scenario_df[excluded_col].min())
            excluded_var_normalized = excluded_var_normalized.fillna(0.5)  # Fill NaN with neutral value
            
            # Adjust composite score by removing estimated contribution of excluded variable
            scenario_df['adjusted_composite_score'] = scenario_df['composite_score'] - (excluded_var_normalized * weight_reduction * scenario_df['composite_score'].std())
        
        # Recalculate rankings
        scenario_df['new_composite_rank'] = scenario_df['adjusted_composite_score'].rank(ascending=False, method='min')
        
        # Compare rankings
        ranking_changes = []
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        for idx, row in scenario_df.iterrows():
            old_rank = df.loc[idx, 'composite_rank'] if 'composite_rank' in df.columns else None
            new_rank = row['new_composite_rank']
            
            if old_rank and old_rank != new_rank:
                ranking_changes.append({
                    'ward_name': row[ward_col],
                    'old_rank': int(old_rank),
                    'new_rank': int(new_rank),
                    'rank_change': int(new_rank - old_rank),
                    'old_score': float(df.loc[idx, 'composite_score']),
                    'new_score': float(row['adjusted_composite_score'])
                })
        
        # Sort by magnitude of change
        ranking_changes.sort(key=lambda x: abs(x['rank_change']), reverse=True)
        
        # Calculate summary statistics
        rank_changes_values = [c['rank_change'] for c in ranking_changes]
        
        return {
            'status': 'success',
            'simulation': {
                'excluded_variable': excluded_col,
                'assumption': f"Variable contributed ~{weight_reduction*100:.1f}% to composite score",
                'methodology': 'Simplified simulation - full reanalysis would be more accurate'
            },
            'impact_summary': {
                'wards_affected': len(ranking_changes),
                'average_rank_change': float(np.mean(rank_changes_values)) if rank_changes_values else 0,
                'max_rank_improvement': min(rank_changes_values) if rank_changes_values else 0,
                'max_rank_decline': max(rank_changes_values) if rank_changes_values else 0
            },
            'significant_changes': ranking_changes[:15],  # Top 15 most affected wards
            'interpretation': _interpret_variable_exclusion_impact(excluded_col, ranking_changes)
        }
        
    except Exception as e:
        logger.error(f"Error simulating variable exclusion: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def simulate_tpr_assumption_change(session_id: str, ward_name: str, new_tpr_value: float) -> Dict[str, Any]:
    """
    Simulate how risk classification changes with different TPR assumptions.
    
    Handles requests like:
    - "How would the risk classification change if TPR is assumed to be 20% in Ward X?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Find the ward
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        ward_data = df[df[ward_col].str.contains(ward_name, case=False, na=False, regex=False)]
        
        if len(ward_data) == 0:
            return {"error": f"Ward '{ward_name}' not found in dataset"}
        
        if len(ward_data) > 1:
            ward_matches = ward_data[ward_col].tolist()
            return {"error": f"Multiple wards found: {ward_matches}. Please be more specific."}
        
        # Find TPR column
        tpr_cols = [col for col in df.columns if any(term in col.lower() for term in ['tpr', 'test_positivity'])]
        
        if not tpr_cols:
            return {"error": "No TPR (Test Positivity Rate) data found in dataset"}
        
        tpr_col = tpr_cols[0]
        target_ward_idx = ward_data.index[0]
        
        # Get original values
        original_tpr = df.loc[target_ward_idx, tpr_col]
        original_score = df.loc[target_ward_idx, 'composite_score'] if 'composite_score' in df.columns else None
        original_rank = df.loc[target_ward_idx, 'composite_rank'] if 'composite_rank' in df.columns else None
        original_category = df.loc[target_ward_idx, 'composite_category'] if 'composite_category' in df.columns else None
        
        # Create scenario dataset
        scenario_df = df.copy()
        scenario_df.loc[target_ward_idx, tpr_col] = new_tpr_value
        
        # Estimate impact on composite score
        # Assumption: TPR change directly affects composite score proportionally
        if 'composite_score' in scenario_df.columns and pd.notna(original_tpr):
            tpr_change_ratio = new_tpr_value / original_tpr if original_tpr != 0 else 1
            # Assume TPR contributes ~30% to composite score
            tpr_contribution = 0.3
            score_adjustment = (tpr_change_ratio - 1) * tpr_contribution
            scenario_df.loc[target_ward_idx, 'composite_score'] *= (1 + score_adjustment)
        
        # Recalculate rankings and categories
        scenario_df['new_composite_rank'] = scenario_df['composite_score'].rank(ascending=False, method='min')
        
        # Recalculate risk categories based on new score percentiles
        score_percentiles = scenario_df['composite_score'].rank(pct=True)
        scenario_df['new_composite_category'] = pd.cut(score_percentiles, 
                                                     bins=[0, 0.33, 0.67, 1.0],
                                                     labels=['Low Risk', 'Medium Risk', 'High Risk'])
        
        new_score = scenario_df.loc[target_ward_idx, 'composite_score']
        new_rank = scenario_df.loc[target_ward_idx, 'new_composite_rank']
        new_category = scenario_df.loc[target_ward_idx, 'new_composite_category']
        
        return {
            'status': 'success',
            'scenario': {
                'ward_name': ward_data.iloc[0][ward_col],
                'tpr_change': {
                    'original_tpr': float(original_tpr) if pd.notna(original_tpr) else None,
                    'new_tpr': float(new_tpr_value),
                    'change_percentage': float((new_tpr_value - original_tpr) / original_tpr * 100) if pd.notna(original_tpr) and original_tpr != 0 else None
                }
            },
            'classification_changes': {
                'original': {
                    'rank': int(original_rank) if original_rank else None,
                    'score': float(original_score) if original_score else None,
                    'category': original_category
                },
                'new': {
                    'rank': int(new_rank) if new_rank else None,
                    'score': float(new_score) if new_score else None,
                    'category': str(new_category) if new_category else None
                },
                'changes': {
                    'rank_change': int(original_rank - new_rank) if (original_rank and new_rank) else None,
                    'score_change': float(new_score - original_score) if (original_score and new_score) else None,
                    'category_change': original_category != str(new_category) if (original_category and new_category) else False
                }
            },
            'interpretation': _interpret_tpr_change_impact(original_tpr, new_tpr_value, original_category, str(new_category) if new_category else None)
        }
        
    except Exception as e:
        logger.error(f"Error simulating TPR assumption change: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def simulate_compactness_threshold_scenario(session_id: str, compactness_threshold: float = 0.5,
                                          location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Simulate reprioritization with wards above compactness threshold flagged as high-risk.
    
    Handles requests like:
    - "Simulate reprioritization with all wards above 50% compactness flagged as high-risk"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply location filtering if specified
        if location_filter:
            location_filter = location_filter.upper()
            location_columns = [col for col in df.columns if any(term in col.lower() for term in ['state', 'lga', 'ward'])]
            
            mask = pd.Series([False] * len(df))
            for col in location_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            df = df[mask]
        
        # Find compactness-related columns
        compactness_cols = [col for col in df.columns if any(term in col.lower() for term in ['compact', 'urban', 'settlement'])]
        
        if not compactness_cols:
            return {"error": "No compactness or urban density indicators found in dataset"}
        
        # Use urbanPercentage as proxy for compactness
        compactness_col = 'urbanPercentage' if 'urbanPercentage' in compactness_cols else compactness_cols[0]
        
        # Create scenario dataset
        scenario_df = df.copy()
        
        # Identify wards above compactness threshold
        high_compactness_mask = scenario_df[compactness_col] >= (compactness_threshold * 100)  # Convert to percentage
        high_compactness_wards = scenario_df[high_compactness_mask]
        
        # Original risk distribution
        original_risk_dist = df['composite_category'].value_counts().to_dict() if 'composite_category' in df.columns else {}
        
        # Force high-compactness wards to high-risk category
        scenario_df.loc[high_compactness_mask, 'new_composite_category'] = 'High Risk'
        scenario_df.loc[~high_compactness_mask, 'new_composite_category'] = scenario_df.loc[~high_compactness_mask, 'composite_category']
        
        # Adjust composite scores for consistency (boost scores of newly high-risk wards)
        if 'composite_score' in scenario_df.columns:
            # Boost scores of high-compactness wards to ensure they rank as high-risk
            high_risk_min_score = scenario_df[scenario_df['composite_category'] == 'High Risk']['composite_score'].min() if 'composite_category' in df.columns else scenario_df['composite_score'].quantile(0.7)
            
            for idx in high_compactness_wards.index:
                if scenario_df.loc[idx, 'composite_score'] < high_risk_min_score:
                    scenario_df.loc[idx, 'composite_score'] = high_risk_min_score + 0.01
        
        # Recalculate rankings
        scenario_df['new_composite_rank'] = scenario_df['composite_score'].rank(ascending=False, method='min')
        
        # New risk distribution
        new_risk_dist = scenario_df['new_composite_category'].value_counts().to_dict()
        
        # Identify affected wards
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        affected_wards = []
        for idx, row in scenario_df.iterrows():
            old_category = df.loc[idx, 'composite_category'] if 'composite_category' in df.columns else None
            new_category = row['new_composite_category']
            
            if old_category != new_category:
                affected_wards.append({
                    'ward_name': row[ward_col],
                    'compactness_value': float(row[compactness_col]),
                    'old_category': old_category,
                    'new_category': new_category,
                    'old_rank': int(df.loc[idx, 'composite_rank']) if 'composite_rank' in df.columns else None,
                    'new_rank': int(row['new_composite_rank'])
                })
        
        return {
            'status': 'success',
            'scenario': {
                'compactness_threshold': compactness_threshold,
                'compactness_indicator': compactness_col,
                'wards_affected_by_threshold': len(high_compactness_wards),
                'location_filter': location_filter
            },
            'reprioritization_impact': {
                'original_risk_distribution': original_risk_dist,
                'new_risk_distribution': new_risk_dist,
                'wards_reclassified': len(affected_wards),
                'newly_high_risk': len([w for w in affected_wards if w['new_category'] == 'High Risk'])
            },
            'affected_wards': affected_wards,
            'summary': _summarize_compactness_scenario_impact(affected_wards, compactness_threshold)
        }
        
    except Exception as e:
        logger.error(f"Error simulating compactness threshold scenario: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def _interpret_variable_exclusion_impact(excluded_variable: str, ranking_changes: List[Dict]) -> str:
    """Interpret the impact of excluding a variable."""
    if not ranking_changes:
        return f"Excluding {excluded_variable} had minimal impact on ward rankings."
    
    avg_change = np.mean([abs(c['rank_change']) for c in ranking_changes])
    max_change = max([abs(c['rank_change']) for c in ranking_changes])
    
    if max_change > 50:
        return f"Excluding {excluded_variable} caused major ranking changes (up to {max_change} positions). This variable has significant influence on risk assessment."
    elif avg_change > 10:
        return f"Excluding {excluded_variable} caused moderate ranking changes (average {avg_change:.1f} positions). This variable has moderate influence."
    else:
        return f"Excluding {excluded_variable} caused minor ranking changes (average {avg_change:.1f} positions). This variable has limited influence."


def _interpret_tpr_change_impact(original_tpr: float, new_tpr: float, original_category: str, new_category: str) -> str:
    """Interpret the impact of changing TPR assumptions."""
    if pd.isna(original_tpr):
        return f"TPR changed to {new_tpr:.1f}%. Impact assessment limited due to missing original TPR data."
    
    tpr_change = new_tpr - original_tpr
    category_changed = original_category != new_category
    
    if category_changed:
        return f"TPR change from {original_tpr:.1f}% to {new_tpr:.1f}% caused risk category change from {original_category} to {new_category}."
    else:
        if abs(tpr_change) > 10:
            return f"Despite significant TPR change ({tpr_change:+.1f}%), risk category remained {original_category}."
        else:
            return f"Minor TPR change ({tpr_change:+.1f}%) maintained risk category as {original_category}."


def _summarize_compactness_scenario_impact(affected_wards: List[Dict], threshold: float) -> str:
    """Summarize the impact of the compactness threshold scenario."""
    if not affected_wards:
        return f"No wards were reclassified using {threshold*100}% compactness threshold."
    
    newly_high_risk = len([w for w in affected_wards if w['new_category'] == 'High Risk'])
    
    return f"Compactness threshold of {threshold*100}% reclassified {len(affected_wards)} wards, with {newly_high_risk} newly flagged as high-risk."