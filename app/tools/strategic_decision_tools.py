"""
Strategic Decision Support Tools for ChatMRPT

These tools provide high-level strategic recommendations by synthesizing
burden analysis, environmental factors, intervention needs, and resource
allocation to support policy and program decision-making.
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


def recommend_priority_targeting_strategy(session_id: str, location_filter: Optional[str] = None,
                                         budget_constraints: str = "medium") -> Dict[str, Any]:
    """
    Recommend which wards should be targeted first based on comprehensive risk assessment.
    
    Handles requests like:
    - "Based on current risk, which wards should be targeted first in Kano State?"
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
        
        # Create comprehensive priority scoring
        priority_factors = {}
        
        # 1. Malaria Burden (40% weight)
        if 'composite_score' in df.columns:
            priority_factors['burden_score'] = df['composite_score'].rank(pct=True) * 0.4
        
        # 2. Environmental Risk (20% weight)
        environmental_score = 0
        env_count = 0
        if 'flood' in df.columns:
            environmental_score += df['flood'].rank(pct=True)
            env_count += 1
        if 'distance_to_water' in df.columns:
            environmental_score += (1 - df['distance_to_water'].rank(pct=True))  # Closer = higher risk
            env_count += 1
        
        if env_count > 0:
            priority_factors['environmental_score'] = (environmental_score / env_count) * 0.2
        
        # 3. Population/Impact Potential (20% weight)
        if 'urbanPercentage' in df.columns:
            priority_factors['population_score'] = df['urbanPercentage'].rank(pct=True) * 0.2
        
        # 4. Intervention Feasibility (20% weight)
        feasibility_score = 0
        feas_count = 0
        if 'settlement_type' in df.columns:
            feasibility_score += df['settlement_type'].rank(pct=True)  # More structured = more feasible
            feas_count += 1
        if 'urbanPercentage' in df.columns:
            feasibility_score += df['urbanPercentage'].rank(pct=True) * 0.5  # Some urban helps logistics
            feas_count += 1
        
        if feas_count > 0:
            priority_factors['feasibility_score'] = (feasibility_score / feas_count) * 0.2
        
        # Calculate composite priority score
        df['strategic_priority_score'] = 0
        for factor, score in priority_factors.items():
            df['strategic_priority_score'] += score
        
        # Adjust for budget constraints
        budget_multipliers = {
            "low": {"high_priority_count": 5, "medium_priority_count": 10},
            "medium": {"high_priority_count": 15, "medium_priority_count": 25},
            "high": {"high_priority_count": 30, "medium_priority_count": 50}
        }
        
        budget_params = budget_multipliers.get(budget_constraints, budget_multipliers["medium"])
        
        # Rank and categorize wards
        df['priority_rank'] = df['strategic_priority_score'].rank(ascending=False, method='min')
        
        # Categorize by priority
        total_wards = len(df)
        high_priority_threshold = min(budget_params["high_priority_count"], total_wards * 0.1)
        medium_priority_threshold = min(budget_params["medium_priority_count"], total_wards * 0.3)
        
        df['priority_category'] = 'Low Priority'
        df.loc[df['priority_rank'] <= medium_priority_threshold, 'priority_category'] = 'Medium Priority'
        df.loc[df['priority_rank'] <= high_priority_threshold, 'priority_category'] = 'High Priority'
        
        # Get recommendations by priority
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        high_priority = df[df['priority_category'] == 'High Priority'].sort_values('priority_rank')
        medium_priority = df[df['priority_category'] == 'Medium Priority'].sort_values('priority_rank')
        
        high_priority_list = []
        for _, ward in high_priority.iterrows():
            ward_info = {
                'ward_name': ward[ward_col],
                'priority_rank': int(ward['priority_rank']),
                'priority_score': float(ward['strategic_priority_score']),
                'rationale': _generate_priority_rationale(ward, priority_factors.keys())
            }
            if 'StateCode_x' in ward:
                ward_info['state'] = ward['StateCode_x']
            if 'composite_category' in ward:
                ward_info['burden_category'] = ward['composite_category']
            high_priority_list.append(ward_info)
        
        return {
            'status': 'success',
            'targeting_strategy': {
                'high_priority_wards': high_priority_list,
                'medium_priority_count': len(medium_priority),
                'total_wards_assessed': total_wards,
                'budget_level': budget_constraints
            },
            'methodology': {
                'factors_considered': list(priority_factors.keys()),
                'weighting_scheme': 'Burden (40%), Environment (20%), Population (20%), Feasibility (20%)',
                'budget_constraints': budget_params
            },
            'strategic_recommendations': _generate_strategic_recommendations(high_priority_list, budget_constraints),
            'location_filter': location_filter
        }
        
    except Exception as e:
        logger.error(f"Error generating priority targeting strategy: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def analyze_lga_risk_distribution(session_id: str, state_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyze which LGAs have the most high-risk wards for resource allocation planning.
    
    Handles requests like:
    - "Which LGAs in Kano State have the most high-risk wards?"
    """
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {"error": "No data available for analysis"}
        
        # Apply state filtering if specified
        if state_filter:
            state_filter = state_filter.upper()
            state_columns = [col for col in df.columns if 'state' in col.lower()]
            
            if state_columns:
                state_col = state_columns[0]
                df = df[df[state_col].str.contains(state_filter, case=False, na=False)]
                
                if len(df) == 0:
                    return {"error": f"No wards found in state: {state_filter}"}
        
        # Find LGA column
        lga_columns = [col for col in df.columns if 'lga' in col.lower()]
        if not lga_columns:
            return {"error": "No LGA (Local Government Area) information found in dataset"}
        
        lga_col = lga_columns[0]
        
        # Analyze risk distribution by LGA
        if 'composite_category' not in df.columns:
            return {"error": "No risk categories available for LGA analysis"}
        
        lga_analysis = []
        for lga in df[lga_col].unique():
            if pd.isna(lga):
                continue
                
            lga_wards = df[df[lga_col] == lga]
            
            risk_distribution = lga_wards['composite_category'].value_counts()
            high_risk_count = risk_distribution.get('High Risk', 0)
            medium_risk_count = risk_distribution.get('Medium Risk', 0)
            low_risk_count = risk_distribution.get('Low Risk', 0)
            total_wards = len(lga_wards)
            
            # Calculate priority metrics
            high_risk_percentage = (high_risk_count / total_wards) * 100
            resource_need_score = high_risk_count + (medium_risk_count * 0.5)  # Weighted need
            
            # Get average burden score
            avg_burden = lga_wards['composite_score'].mean() if 'composite_score' in lga_wards.columns else 0
            
            lga_analysis.append({
                'lga_name': lga,
                'total_wards': total_wards,
                'high_risk_wards': high_risk_count,
                'medium_risk_wards': medium_risk_count,
                'low_risk_wards': low_risk_count,
                'high_risk_percentage': round(high_risk_percentage, 1),
                'resource_need_score': round(resource_need_score, 1),
                'average_burden_score': round(avg_burden, 3),
                'priority_tier': _classify_lga_priority(high_risk_count, high_risk_percentage, total_wards)
            })
        
        # Sort by resource need
        lga_analysis.sort(key=lambda x: x['resource_need_score'], reverse=True)
        
        # State-level summary
        total_high_risk = sum(lga['high_risk_wards'] for lga in lga_analysis)
        total_wards = sum(lga['total_wards'] for lga in lga_analysis)
        
        return {
            'status': 'success',
            'lga_risk_analysis': lga_analysis,
            'state_summary': {
                'total_lgas': len(lga_analysis),
                'total_wards': total_wards,
                'total_high_risk_wards': total_high_risk,
                'state_high_risk_percentage': round((total_high_risk / total_wards) * 100, 1) if total_wards > 0 else 0,
                'top_3_priority_lgas': [lga['lga_name'] for lga in lga_analysis[:3]]
            },
            'resource_allocation_recommendations': _generate_lga_recommendations(lga_analysis[:10]),
            'state_filter': state_filter
        }
        
    except Exception as e:
        logger.error(f"Error analyzing LGA risk distribution: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def generate_monitoring_priorities(session_id: str, location_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Identify wards that need additional monitoring or data collection.
    
    Handles requests like:
    - "Which wards should receive additional monitoring or data collection?"
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
        
        monitoring_priorities = []
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        
        # Criteria for additional monitoring
        for _, ward in df.iterrows():
            monitoring_reasons = []
            priority_score = 0
            
            # 1. High burden with uncertainty
            if 'composite_category' in ward and ward['composite_category'] == 'High Risk':
                if 'model_agreement' in ward and ward['model_agreement'] < 0.8:  # Low model consensus
                    monitoring_reasons.append('High risk with low model consensus')
                    priority_score += 3
                elif 'model_std_score' in ward and ward['model_std_score'] > df['model_std_score'].quantile(0.8):
                    monitoring_reasons.append('High risk with high model uncertainty')
                    priority_score += 3
            
            # 2. Border/transition areas (rank differences between methods)
            if 'rank_difference_abs' in ward and ward['rank_difference_abs'] > df['rank_difference_abs'].quantile(0.8):
                monitoring_reasons.append('Large disagreement between analysis methods')
                priority_score += 2
            
            # 3. Data quality concerns
            if 'data_completeness' in ward and ward['data_completeness'] < 90:
                monitoring_reasons.append('Incomplete data (< 90% complete)')
                priority_score += 2
            
            # 4. Environmental risk factors requiring monitoring
            if 'flood' in ward and ward['flood'] > df['flood'].quantile(0.9):
                monitoring_reasons.append('High flood risk requiring seasonal monitoring')
                priority_score += 1
            
            # 5. Rapid change potential (high urban areas)
            if 'urbanPercentage' in ward and ward['urbanPercentage'] > 80:
                monitoring_reasons.append('Rapid urbanization area requiring regular updates')
                priority_score += 1
            
            # 6. Strategic importance (near high-risk areas)
            if 'composite_category' in ward and ward['composite_category'] == 'Medium Risk':
                if 'composite_score' in ward and ward['composite_score'] > df[df['composite_category'] == 'Medium Risk']['composite_score'].quantile(0.8):
                    monitoring_reasons.append('Medium risk ward at threshold for high risk')
                    priority_score += 2
            
            if monitoring_reasons:
                monitoring_info = {
                    'ward_name': ward[ward_col],
                    'monitoring_priority_score': priority_score,
                    'monitoring_reasons': monitoring_reasons,
                    'current_risk_category': ward['composite_category'] if 'composite_category' in ward else 'Unknown'
                }
                
                if 'StateCode_x' in ward:
                    monitoring_info['state'] = ward['StateCode_x']
                if 'composite_score' in ward:
                    monitoring_info['burden_score'] = float(ward['composite_score'])
                if 'data_completeness' in ward:
                    monitoring_info['data_completeness'] = float(ward['data_completeness'])
                
                monitoring_priorities.append(monitoring_info)
        
        # Sort by priority score
        monitoring_priorities.sort(key=lambda x: x['monitoring_priority_score'], reverse=True)
        
        # Categorize monitoring needs
        high_priority_monitoring = [w for w in monitoring_priorities if w['monitoring_priority_score'] >= 4]
        medium_priority_monitoring = [w for w in monitoring_priorities if 2 <= w['monitoring_priority_score'] < 4]
        
        return {
            'status': 'success',
            'monitoring_priorities': {
                'high_priority': high_priority_monitoring,
                'medium_priority': medium_priority_monitoring,
                'total_identified': len(monitoring_priorities)
            },
            'monitoring_recommendations': {
                'immediate_monitoring': len(high_priority_monitoring),
                'regular_monitoring': len(medium_priority_monitoring),
                'monitoring_types': _categorize_monitoring_types(monitoring_priorities),
                'resource_requirements': _estimate_monitoring_resources(monitoring_priorities)
            },
            'location_filter': location_filter
        }
        
    except Exception as e:
        logger.error(f"Error generating monitoring priorities: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def identify_deprioritization_candidates(session_id: str, location_filter: Optional[str] = None,
                                        safety_threshold: float = 0.8) -> Dict[str, Any]:
    """
    Identify low-risk wards that might be safely deprioritized for resource reallocation.
    
    Handles requests like:
    - "Highlight low-risk wards that might be safely deprioritized"
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
        
        # Identify deprioritization candidates
        ward_col = next((col for col in df.columns if 'ward' in col.lower()), 'WardName')
        deprioritization_candidates = []
        
        for _, ward in df.iterrows():
            candidate = False
            safety_factors = []
            confidence_score = 0
            
            # Primary criterion: Low risk category
            if 'composite_category' in ward and ward['composite_category'] == 'Low Risk':
                candidate = True
                
                # Additional safety checks
                
                # 1. Model consensus (high agreement = safer to deprioritize)
                if 'model_agreement' in ward and ward['model_agreement'] >= safety_threshold:
                    safety_factors.append(f'High model consensus ({ward["model_agreement"]:.1%})')
                    confidence_score += 2
                
                # 2. Method agreement (both composite and PCA agree)
                if 'method_agreement' in ward and ward['method_agreement'] == 'Low Agreement':
                    pass  # Don't add confidence if methods disagree
                else:
                    safety_factors.append('Analysis methods agree on low risk')
                    confidence_score += 2
                
                # 3. Consistently low burden (bottom percentiles)
                if 'composite_score' in ward:
                    score_percentile = (df['composite_score'] < ward['composite_score']).mean()
                    if score_percentile <= 0.2:  # Bottom 20%
                        safety_factors.append('Consistently low burden (bottom 20%)')
                        confidence_score += 2
                
                # 4. Low environmental risk
                environmental_risks = []
                if 'flood' in ward and ward['flood'] <= df['flood'].quantile(0.3):
                    environmental_risks.append('low flood risk')
                if 'distance_to_water' in ward and ward['distance_to_water'] >= df['distance_to_water'].quantile(0.7):
                    environmental_risks.append('far from water bodies')
                
                if environmental_risks:
                    safety_factors.append(f'Low environmental risk ({", ".join(environmental_risks)})')
                    confidence_score += 1
                
                # 5. Stable/rural area (less likely to change rapidly)
                if 'urbanPercentage' in ward and ward['urbanPercentage'] <= 30:
                    safety_factors.append('Rural/stable area (low change risk)')
                    confidence_score += 1
                
                # Only include if meets minimum safety criteria
                if confidence_score >= 3:  # Require at least moderate confidence
                    candidate_info = {
                        'ward_name': ward[ward_col],
                        'confidence_score': confidence_score,
                        'safety_factors': safety_factors,
                        'deprioritization_recommendation': _classify_deprioritization_safety(confidence_score)
                    }
                    
                    if 'StateCode_x' in ward:
                        candidate_info['state'] = ward['StateCode_x']
                    if 'composite_score' in ward:
                        candidate_info['burden_score'] = float(ward['composite_score'])
                    if 'composite_rank' in ward:
                        candidate_info['current_rank'] = int(ward['composite_rank'])
                    
                    deprioritization_candidates.append(candidate_info)
        
        # Sort by confidence score
        deprioritization_candidates.sort(key=lambda x: x['confidence_score'], reverse=True)
        
        # Categorize by safety level
        high_confidence = [c for c in deprioritization_candidates if c['confidence_score'] >= 6]
        medium_confidence = [c for c in deprioritization_candidates if 4 <= c['confidence_score'] < 6]
        low_confidence = [c for c in deprioritization_candidates if 3 <= c['confidence_score'] < 4]
        
        return {
            'status': 'success',
            'deprioritization_candidates': {
                'high_confidence': high_confidence,
                'medium_confidence': medium_confidence,
                'low_confidence': low_confidence,
                'total_candidates': len(deprioritization_candidates)
            },
            'reallocation_potential': {
                'resources_from_high_confidence': len(high_confidence),
                'resources_from_medium_confidence': len(medium_confidence),
                'safety_threshold_used': safety_threshold,
                'total_reallocatable_resources': len(high_confidence) + len(medium_confidence)
            },
            'safety_recommendations': _generate_deprioritization_guidelines(high_confidence, medium_confidence),
            'location_filter': location_filter
        }
        
    except Exception as e:
        logger.error(f"Error identifying deprioritization candidates: {e}")
        return {"error": f"Analysis failed: {str(e)}"}


def _generate_priority_rationale(ward: pd.Series, factors: List[str]) -> List[str]:
    """Generate rationale for why a ward is high priority."""
    rationale = []
    
    if 'burden_score' in factors and 'composite_category' in ward:
        if ward['composite_category'] == 'High Risk':
            rationale.append('High malaria burden')
    
    if 'environmental_score' in factors:
        if 'flood' in ward and ward['flood'] > 0.5:
            rationale.append('High flood risk')
    
    if 'population_score' in factors:
        if 'urbanPercentage' in ward and ward['urbanPercentage'] > 50:
            rationale.append('High population impact potential')
    
    if 'feasibility_score' in factors:
        if 'settlement_type' in ward and ward['settlement_type'] > 0.5:
            rationale.append('Good intervention feasibility')
    
    return rationale if rationale else ['Multiple risk factors']


def _classify_lga_priority(high_risk_count: int, high_risk_percentage: float, total_wards: int) -> str:
    """Classify LGA priority level."""
    if high_risk_count >= 5 and high_risk_percentage >= 30:
        return 'Critical Priority'
    elif high_risk_count >= 3 or high_risk_percentage >= 20:
        return 'High Priority'
    elif high_risk_count >= 1 or high_risk_percentage >= 10:
        return 'Medium Priority'
    else:
        return 'Low Priority'


def _generate_strategic_recommendations(high_priority_wards: List[Dict], budget_level: str) -> List[str]:
    """Generate strategic recommendations based on priority wards."""
    if not high_priority_wards:
        return ["No high-priority wards identified for intervention"]
    
    recommendations = [
        f"Immediately deploy resources to {len(high_priority_wards)} highest-priority wards",
        f"Focus initial efforts on {high_priority_wards[0]['ward_name']} (top priority)"
    ]
    
    if budget_level == "high":
        recommendations.extend([
            "Implement comprehensive intervention package (ITN + IRS + CHW)",
            "Establish real-time monitoring systems in priority areas"
        ])
    elif budget_level == "medium":
        recommendations.extend([
            "Prioritize ITN distribution and CHW deployment",
            "Implement targeted IRS in highest-burden areas"
        ])
    else:
        recommendations.extend([
            "Focus on ITN distribution as primary intervention",
            "Deploy CHWs to most remote/underserved areas"
        ])
    
    return recommendations


def _generate_lga_recommendations(top_lgas: List[Dict]) -> List[str]:
    """Generate LGA-level resource allocation recommendations."""
    if not top_lgas:
        return ["No LGA prioritization recommendations available"]
    
    recommendations = [
        f"Prioritize {top_lgas[0]['lga_name']} with {top_lgas[0]['high_risk_wards']} high-risk wards",
        "Establish LGA-level coordination centers in top 3 priority areas"
    ]
    
    critical_lgas = [lga for lga in top_lgas if lga['priority_tier'] == 'Critical Priority']
    if critical_lgas:
        recommendations.append(f"Deploy emergency response teams to {len(critical_lgas)} critical priority LGAs")
    
    return recommendations


def _categorize_monitoring_types(monitoring_priorities: List[Dict]) -> Dict[str, int]:
    """Categorize types of monitoring needed."""
    types = {
        'data_quality_monitoring': 0,
        'seasonal_monitoring': 0,
        'method_validation': 0,
        'urbanization_tracking': 0
    }
    
    for ward in monitoring_priorities:
        reasons = ward['monitoring_reasons']
        if any('data' in reason.lower() for reason in reasons):
            types['data_quality_monitoring'] += 1
        if any('flood' in reason.lower() or 'seasonal' in reason.lower() for reason in reasons):
            types['seasonal_monitoring'] += 1
        if any('method' in reason.lower() or 'consensus' in reason.lower() for reason in reasons):
            types['method_validation'] += 1
        if any('urban' in reason.lower() for reason in reasons):
            types['urbanization_tracking'] += 1
    
    return types


def _estimate_monitoring_resources(monitoring_priorities: List[Dict]) -> Dict[str, Any]:
    """Estimate resources needed for monitoring activities."""
    high_priority = len([w for w in monitoring_priorities if w['monitoring_priority_score'] >= 4])
    medium_priority = len([w for w in monitoring_priorities if 2 <= w['monitoring_priority_score'] < 4])
    
    return {
        'immediate_resources_needed': high_priority,
        'ongoing_resources_needed': medium_priority,
        'estimated_timeline': '3-6 months for initial assessment',
        'monitoring_frequency': 'Quarterly for high priority, biannual for medium priority'
    }


def _classify_deprioritization_safety(confidence_score: int) -> str:
    """Classify safety level for deprioritization."""
    if confidence_score >= 6:
        return 'Safe to deprioritize (high confidence)'
    elif confidence_score >= 4:
        return 'Consider for deprioritization (medium confidence)'
    else:
        return 'Monitor before deprioritizing (low confidence)'


def _generate_deprioritization_guidelines(high_confidence: List[Dict], medium_confidence: List[Dict]) -> List[str]:
    """Generate safety guidelines for deprioritization."""
    guidelines = []
    
    if high_confidence:
        guidelines.extend([
            f"Safe to immediately reallocate resources from {len(high_confidence)} high-confidence low-risk wards",
            "Maintain minimal surveillance in deprioritized areas"
        ])
    
    if medium_confidence:
        guidelines.extend([
            f"Consider gradual resource reallocation from {len(medium_confidence)} medium-confidence wards",
            "Implement 6-month monitoring period before full deprioritization"
        ])
    
    guidelines.extend([
        "Establish rapid response capacity for any emerging risks in deprioritized areas",
        "Review deprioritization decisions annually based on new data"
    ])
    
    return guidelines