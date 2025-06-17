"""
Intervention Targeting Tools for ChatMRPT

These tools provide intelligent intervention targeting recommendations based on
malaria burden, settlement types, environmental factors, and coverage gaps.
Handles requests for ITN, IRS, SMC, and CHW deployment strategies.
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


def identify_itn_priority_wards(session_id: str, location_filter: Optional[str] = None, 
                               top_n: int = 20) -> Dict[str, Any]:
    """
    Identify wards most in need of ITN (Insecticide-Treated Nets) distribution.
    
    Handles requests like:
    - "Which wards are most in need of ITNs in Kano?"
    - "Priority wards for ITN distribution"
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
            
            if len(df) == 0:
                return {"error": f"No wards found matching location filter: {location_filter}"}
        
        # ITN prioritization criteria
        priority_factors = []
        
        # 1. High malaria burden (primary factor)
        if 'composite_score' in df.columns:
            df['burden_priority'] = df['composite_score'].rank(pct=True)
            priority_factors.append(('burden_priority', 0.4))
        
        # 2. High population/urban areas (ITNs effective in household settings)
        if 'urbanPercentage' in df.columns:
            df['urban_priority'] = df['urbanPercentage'].rank(pct=True) 
            priority_factors.append(('urban_priority', 0.2))
        
        # 3. Flood-prone areas (nets protect during high transmission periods)
        if 'flood' in df.columns:
            df['flood_priority'] = df['flood'].rank(pct=True)
            priority_factors.append(('flood_priority', 0.2))
        
        # 4. Settlement type (ITNs effective in household settings)
        if 'settlement_type' in df.columns:
            df['settlement_priority'] = df['settlement_type'].rank(pct=True)
            priority_factors.append(('settlement_priority', 0.2))
        
        # Calculate composite ITN priority score
        if not priority_factors:
            return {"error": "Insufficient data to calculate ITN priorities"}
        
        df['itn_priority_score'] = 0
        total_weight = sum(weight for _, weight in priority_factors)
        
        for factor, weight in priority_factors:
            df['itn_priority_score'] += df[factor] * (weight / total_weight)
        
        # Get top priority wards
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        priority_wards = df.nlargest(top_n, 'itn_priority_score')
        
        results = []
        for _, ward in priority_wards.iterrows():
            ward_info = {
                'ward_name': ward[ward_col],
                'itn_priority_score': float(ward['itn_priority_score']),
                'burden_rank': int(ward['composite_rank']) if 'composite_rank' in ward else None,
                'burden_category': ward['composite_category'] if 'composite_category' in ward else None
            }
            
            if 'StateCode_x' in ward:
                ward_info['state'] = ward['StateCode_x']
            if 'urbanPercentage' in ward:
                ward_info['urban_percentage'] = float(ward['urbanPercentage'])
            if 'flood' in ward:
                ward_info['flood_risk'] = float(ward['flood'])
            
            results.append(ward_info)
        
        return {
            'status': 'success',
            'itn_priority_wards': results,
            'methodology': {
                'factors_used': [factor for factor, _ in priority_factors],
                'weights': dict(priority_factors),
                'total_wards_analyzed': len(df)
            },
            'summary': {
                'top_priority_ward': results[0]['ward_name'] if results else None,
                'location_filter': location_filter,
                'recommendation': _generate_itn_recommendations(results[:5])
            }
        }
        
    except Exception as e:
        logger.error(f"Error identifying ITN priority wards: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def identify_irs_eligible_wards(session_id: str, location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Identify wards eligible for IRS (Indoor Residual Spraying) based on burden and settlement type.
    
    Handles requests like:
    - "Identify wards eligible for IRS based on burden and settlement type"
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
        
        # IRS eligibility criteria
        eligible_wards = df.copy()
        
        # 1. High malaria burden (IRS most effective in high transmission areas)
        if 'composite_category' in df.columns:
            eligible_wards = eligible_wards[eligible_wards['composite_category'].isin(['High Risk', 'Medium Risk'])]
        elif 'composite_score' in df.columns:
            high_burden_threshold = df['composite_score'].quantile(0.5)  # Top 50%
            eligible_wards = eligible_wards[eligible_wards['composite_score'] >= high_burden_threshold]
        
        # 2. Settlement type considerations (IRS effective in structured settlements)
        if 'settlement_type' in df.columns:
            # IRS works best in areas with permanent structures
            suitable_settlements = eligible_wards['settlement_type'] >= 0.3  # Threshold for structured settlements
            eligible_wards = eligible_wards[suitable_settlements]
        
        # 3. Urban areas (easier logistics for IRS campaigns)
        if 'urbanPercentage' in df.columns:
            # Prefer areas with some urban development for logistical efficiency
            eligible_wards = eligible_wards[eligible_wards['urbanPercentage'] >= 20]  # At least 20% urban
        
        if len(eligible_wards) == 0:
            return {"error": "No wards meet IRS eligibility criteria"}
        
        # Rank eligible wards by priority
        if 'composite_score' in eligible_wards.columns:
            eligible_wards = eligible_wards.sort_values('composite_score', ascending=False)
        
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        results = []
        for _, ward in eligible_wards.iterrows():
            ward_info = {
                'ward_name': ward[ward_col],
                'eligibility_reason': [],
                'burden_category': ward['composite_category'] if 'composite_category' in ward else 'High burden',
                'priority_rank': len(results) + 1
            }
            
            if 'StateCode_x' in ward:
                ward_info['state'] = ward['StateCode_x']
            if 'composite_score' in ward:
                ward_info['burden_score'] = float(ward['composite_score'])
            if 'urbanPercentage' in ward:
                ward_info['urban_percentage'] = float(ward['urbanPercentage'])
                if ward['urbanPercentage'] >= 50:
                    ward_info['eligibility_reason'].append('High urban development (good logistics)')
            if 'settlement_type' in ward:
                ward_info['settlement_score'] = float(ward['settlement_type'])
                if ward['settlement_type'] >= 0.5:
                    ward_info['eligibility_reason'].append('Structured settlements (suitable for IRS)')
            
            # Default eligibility reason
            if not ward_info['eligibility_reason']:
                ward_info['eligibility_reason'] = ['High malaria burden', 'Suitable settlement characteristics']
            
            results.append(ward_info)
        
        return {
            'status': 'success',
            'irs_eligible_wards': results,
            'eligibility_criteria': {
                'burden_requirement': 'High or Medium risk',
                'settlement_requirement': 'Structured settlements (score >= 0.3)',
                'urban_requirement': 'At least 20% urban development'
            },
            'summary': {
                'total_eligible_wards': len(results),
                'percentage_of_total': round((len(results) / len(df)) * 100, 1),
                'top_priority_wards': [w['ward_name'] for w in results[:5]],
                'location_filter': location_filter
            }
        }
        
    except Exception as e:
        logger.error(f"Error identifying IRS eligible wards: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def identify_coverage_gaps(session_id: str, location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Identify wards where current intervention coverage is not proportional to risk.
    
    Handles requests like:
    - "List wards where current intervention coverage is not proportional to risk"
    - "Show me high-burden wards that have low intervention coverage"
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
        
        # Since no actual intervention coverage data is available, 
        # simulate coverage gaps based on realistic assumptions
        if 'composite_category' in df.columns:
            # Assume current coverage inversely correlates with burden (realistic gap scenario)
            df['estimated_coverage'] = 0.7  # Base coverage assumption
            df.loc[df['composite_category'] == 'High Risk', 'estimated_coverage'] = 0.3  # Low coverage in high-risk areas
            df.loc[df['composite_category'] == 'Medium Risk', 'estimated_coverage'] = 0.5  # Medium coverage
            df.loc[df['composite_category'] == 'Low Risk', 'estimated_coverage'] = 0.8   # High coverage in low-risk areas
            coverage_col = 'estimated_coverage'
        else:
            return {"error": "No burden data available to estimate coverage gaps"}
        
        # Note in results that this is estimated, not actual coverage data
        
        # Identify gaps: High burden + Low coverage
        if 'composite_score' in df.columns:
            df['burden_percentile'] = df['composite_score'].rank(pct=True)
        else:
            return {"error": "No burden indicator available for gap analysis"}
        
        df['coverage_percentile'] = df[coverage_col].rank(pct=True)
        
        # Gap score: High burden + Low coverage = High gap
        df['coverage_gap_score'] = df['burden_percentile'] - df['coverage_percentile']
        
        # Identify significant gaps (high burden, low coverage)
        significant_gaps = df[
            (df['burden_percentile'] >= 0.6) &  # Top 40% burden
            (df['coverage_percentile'] <= 0.4)   # Bottom 40% coverage
        ]
        
        significant_gaps = significant_gaps.sort_values('coverage_gap_score', ascending=False)
        
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        results = []
        for _, ward in significant_gaps.iterrows():
            ward_info = {
                'ward_name': ward[ward_col],
                'gap_severity': 'Critical' if ward['coverage_gap_score'] > 0.5 else 'Moderate',
                'burden_percentile': float(ward['burden_percentile']),
                'coverage_percentile': float(ward['coverage_percentile']),
                'gap_score': float(ward['coverage_gap_score'])
            }
            
            if 'StateCode_x' in ward:
                ward_info['state'] = ward['StateCode_x']
            if 'composite_score' in ward:
                ward_info['burden_score'] = float(ward['composite_score'])
            if 'composite_category' in ward:
                ward_info['burden_category'] = ward['composite_category']
            
            ward_info['current_coverage'] = float(ward[coverage_col])
            ward_info['recommended_coverage'] = min(0.9, ward_info['current_coverage'] + ward_info['gap_score'])
            
            results.append(ward_info)
        
        return {
            'status': 'success',
            'coverage_gaps': results,
            'gap_analysis': {
                'total_gaps_identified': len(results),
                'coverage_indicator': f'{coverage_col} (estimated - no actual coverage data available)',
                'data_note': 'Coverage estimates based on inverse correlation with burden levels',
                'critical_gaps': len([g for g in results if g['gap_severity'] == 'Critical']),
                'moderate_gaps': len([g for g in results if g['gap_severity'] == 'Moderate'])
            },
            'recommendations': _generate_coverage_gap_recommendations(results[:10]),
            'location_filter': location_filter
        }
        
    except Exception as e:
        logger.error(f"Error identifying coverage gaps: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def recommend_chw_deployment(session_id: str, location_filter: Optional[str] = None, 
                           chw_capacity: int = 50) -> Dict[str, Any]:
    """
    Recommend where additional CHWs (Community Health Workers) should be deployed.
    
    Handles requests like:
    - "Where can additional CHWs be deployed based on ward-level needs?"
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
        
        # CHW deployment criteria
        chw_priority_factors = []
        
        # 1. High malaria burden (CHWs needed for case management)
        if 'composite_score' in df.columns:
            df['chw_burden_priority'] = df['composite_score'].rank(pct=True)
            chw_priority_factors.append(('chw_burden_priority', 0.3))
        
        # 2. Rural/less urban areas (CHWs bridge healthcare access gaps)
        if 'urbanPercentage' in df.columns:
            df['chw_rural_priority'] = (100 - df['urbanPercentage']).rank(pct=True)
            chw_priority_factors.append(('chw_rural_priority', 0.3))
        
        # 3. Distance/accessibility (use elevation as proxy for remoteness)
        if 'elevation' in df.columns:
            df['chw_access_priority'] = df['elevation'].rank(pct=True)  # Higher elevation = more remote
            chw_priority_factors.append(('chw_access_priority', 0.2))
        
        # 4. Population density/settlement type (CHWs effective in community settings)
        if 'settlement_type' in df.columns:
            df['chw_community_priority'] = df['settlement_type'].rank(pct=True)
            chw_priority_factors.append(('chw_community_priority', 0.2))
        
        if not chw_priority_factors:
            return {"error": "Insufficient data to recommend CHW deployment"}
        
        # Calculate composite CHW priority score
        df['chw_priority_score'] = 0
        total_weight = sum(weight for _, weight in chw_priority_factors)
        
        for factor, weight in chw_priority_factors:
            df['chw_priority_score'] += df[factor] * (weight / total_weight)
        
        # Get top priority wards for CHW deployment
        priority_wards = df.nlargest(min(chw_capacity, len(df)), 'chw_priority_score')
        
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        results = []
        for idx, (_, ward) in enumerate(priority_wards.iterrows()):
            ward_info = {
                'ward_name': ward[ward_col],
                'chw_priority_rank': idx + 1,
                'chw_priority_score': float(ward['chw_priority_score']),
                'deployment_rationale': []
            }
            
            if 'StateCode_x' in ward:
                ward_info['state'] = ward['StateCode_x']
            if 'composite_score' in ward:
                ward_info['burden_score'] = float(ward['composite_score'])
                if ward['composite_score'] > df['composite_score'].median():
                    ward_info['deployment_rationale'].append('High malaria burden')
            if 'urbanPercentage' in ward:
                ward_info['urban_percentage'] = float(ward['urbanPercentage'])
                if ward['urbanPercentage'] < 30:
                    ward_info['deployment_rationale'].append('Rural area with limited healthcare access')
            if 'elevation' in ward:
                if ward['elevation'] > df['elevation'].quantile(0.7):
                    ward_info['deployment_rationale'].append('Remote/mountainous area')
            
            if not ward_info['deployment_rationale']:
                ward_info['deployment_rationale'] = ['High priority based on multiple factors']
            
            results.append(ward_info)
        
        return {
            'status': 'success',
            'chw_deployment_recommendations': results,
            'deployment_strategy': {
                'total_chws_recommended': len(results),
                'chw_capacity_limit': chw_capacity,
                'prioritization_factors': [factor for factor, _ in chw_priority_factors],
                'coverage_area': f"{len(results)} priority wards out of {len(df)} total wards"
            },
            'summary': {
                'top_5_priority_wards': [w['ward_name'] for w in results[:5]],
                'geographical_focus': _analyze_geographic_distribution(results),
                'location_filter': location_filter
            }
        }
        
    except Exception as e:
        logger.error(f"Error recommending CHW deployment: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def _generate_itn_recommendations(priority_wards: List[Dict]) -> List[str]:
    """Generate specific ITN distribution recommendations."""
    if not priority_wards:
        return ["No priority wards identified for ITN distribution"]
    
    recommendations = [
        f"Prioritize ITN distribution in {priority_wards[0]['ward_name']} (highest priority)",
        "Focus on urban and semi-urban areas for maximum household coverage",
        "Target flood-prone areas during dry season for optimal deployment"
    ]
    
    if len(priority_wards) >= 3:
        recommendations.append(f"Secondary targets: {priority_wards[1]['ward_name']}, {priority_wards[2]['ward_name']}")
    
    return recommendations


def _generate_coverage_gap_recommendations(gaps: List[Dict]) -> List[str]:
    """Generate recommendations for addressing coverage gaps."""
    if not gaps:
        return ["No significant coverage gaps identified"]
    
    critical_gaps = [g for g in gaps if g['gap_severity'] == 'Critical']
    
    recommendations = [
        f"Immediate intervention needed in {len(critical_gaps)} critical gap wards",
        "Reallocate resources from low-burden to high-burden areas",
        "Implement targeted campaigns in underserved high-risk wards"
    ]
    
    if gaps:
        recommendations.append(f"Top priority: {gaps[0]['ward_name']} (gap score: {gaps[0]['gap_score']:.2f})")
    
    return recommendations


def _analyze_geographic_distribution(results: List[Dict]) -> str:
    """Analyze geographic distribution of recommendations."""
    if not results:
        return "No geographic pattern identified"
    
    states = {}
    for ward in results:
        state = ward.get('state', 'Unknown')
        states[state] = states.get(state, 0) + 1
    
    if len(states) == 1:
        return f"Concentrated in {list(states.keys())[0]} state"
    else:
        return f"Distributed across {len(states)} states: {', '.join(states.keys())}"