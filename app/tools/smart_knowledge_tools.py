"""
Smart Knowledge Tools for ChatMRPT - Data-Driven Insights

These tools provide contextual, personalized knowledge based on the user's actual data,
not generic responses. Each tool analyzes the uploaded dataset to give tailored insights:

1. **ExplainDataContext** - "What does my data tell me about malaria in my area?"
2. **GetPersonalizedRecommendations** - "What should I do based on MY specific situation?"  
3. **InterpretYourResults** - "What do my analysis results actually mean for action?"
4. **GetDataDrivenInsights** - "What patterns and insights are hidden in my data?"

These tools make knowledge actionable by grounding it in the user's real context.
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


class ExplainDataContext(BaseTool):
    """
    Universal data-driven assistant that answers ANY question about the user's malaria data.
    
    Handles questions like: "What is this?", "Why are we doing this?", "I don't understand X",
    "Explain Y to me", "What does this mean?", "How do I interpret this?", etc.
    
    Grounds all explanations in the user's actual dataset and analysis results.
    """
    
    user_question: str = Field(
        "What does my data tell me about my malaria situation?",
        description="The user's specific question about their data, analysis, or malaria program"
    )
    
    explanation_level: str = Field(
        "practical",
        description="Explanation complexity: 'basic', 'practical', 'detailed', 'technical'",
        pattern="^(basic|practical|detailed|technical)$"
    )
    
    include_examples: bool = Field(
        True,
        description="Include specific examples from user's actual data"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.GENERAL_KNOWLEDGE
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What is composite score and why is mine so low?",
            "I don't understand why Bagwai has higher risk than Tudun Wada",
            "Explain what ITN coverage means in my context",
            "Why are we doing malaria risk analysis?",
            "What does this PCA score actually tell me?",
            "I'm confused about these results - what do they mean?",
            "How do I explain these findings to my supervisor?",
            "What is the difference between risk and coverage in my data?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Answer user's specific question using their actual data."""
        try:
            # Get dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Analyze the user's question to understand what they're asking
            question_analysis = self._analyze_user_question(self.user_question, df)
            
            # Generate data-driven answer
            answer = self._generate_data_driven_answer(df, question_analysis)
            
            # Create practical examples from their data
            examples = self._create_data_examples(df, question_analysis) if self.include_examples else {}
            
            # Add follow-up suggestions
            follow_ups = self._suggest_follow_up_questions(question_analysis, df)
            
            message = f"Analyzed your question about your {len(df)}-ward dataset. "
            message += f"Provided {self.explanation_level} explanation grounded in your actual data."
            
            return self._create_success_result(
                message=message,
                data={
                    'user_question': self.user_question,
                    'question_type': question_analysis['question_type'],
                    'data_driven_answer': answer,
                    'practical_examples': examples,
                    'follow_up_suggestions': follow_ups,
                    'explanation_level': self.explanation_level,
                    'data_context': self._get_relevant_data_context(df, question_analysis)
                }
            )
            
        except Exception as e:
            logger.error(f"Error in data context explanation: {e}")
            return self._create_error_result(f"Context analysis failed: {str(e)}")
    
    def _analyze_user_question(self, question: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze what the user is asking to provide targeted answers."""
        question_lower = question.lower()
        
        # Identify question type
        question_type = "general"
        specific_topic = None
        confusion_indicators = ["don't understand", "confused", "what is", "explain", "why", "how"]
        
        if any(indicator in question_lower for indicator in confusion_indicators):
            question_type = "clarification"
        
        # Identify specific topics mentioned
        topics = {
            'composite_score': 'composite_score' in question_lower or 'composite score' in question_lower,
            'pca_score': 'pca' in question_lower or 'pca_score' in question_lower,
            'risk': 'risk' in question_lower,
            'coverage': 'coverage' in question_lower or 'itn' in question_lower or 'irs' in question_lower,
            'wards': any(word in question_lower for word in ['ward', 'area', 'location']),
            'results': 'result' in question_lower or 'finding' in question_lower,
            'analysis': 'analysis' in question_lower or 'study' in question_lower,
            'intervention': 'intervention' in question_lower or 'treatment' in question_lower
        }
        
        specific_topic = [topic for topic, mentioned in topics.items() if mentioned]
        
        # Check if specific ward names are mentioned
        ward_names = []
        if 'WardName' in df.columns:
            for ward in df['WardName'].dropna().unique()[:20]:  # Check top 20 ward names
                if str(ward).lower() in question_lower:
                    ward_names.append(ward)
        
        return {
            'question_type': question_type,
            'topics_mentioned': specific_topic,
            'ward_names_mentioned': ward_names,
            'needs_clarification': question_type == "clarification",
            'asks_about_purpose': any(word in question_lower for word in ['why', 'purpose', 'reason']),
            'asks_for_comparison': any(word in question_lower for word in ['difference', 'compare', 'versus', 'vs']),
            'asks_for_explanation': any(word in question_lower for word in ['explain', 'meaning', 'interpret'])
        }
    
    def _generate_data_driven_answer(self, df: pd.DataFrame, question_analysis: Dict[str, Any]) -> str:
        """Generate a data-driven answer based on the user's specific question."""
        topics = question_analysis['topics_mentioned']
        ward_names = question_analysis['ward_names_mentioned']
        
        # Start building the answer
        answer_parts = []
        
        # Handle specific ward questions
        if ward_names:
            for ward_name in ward_names[:2]:  # Limit to 2 wards
                ward_data = df[df['WardName'] == ward_name]
                if not ward_data.empty:
                    ward_info = self._get_ward_specific_info(ward_data.iloc[0], df)
                    answer_parts.append(f"For {ward_name}: {ward_info}")
        
        # Handle topic-specific questions
        if 'composite_score' in topics:
            answer_parts.append(self._explain_composite_score_in_context(df))
        
        if 'pca_score' in topics:
            answer_parts.append(self._explain_pca_score_in_context(df))
        
        if 'risk' in topics:
            answer_parts.append(self._explain_risk_in_context(df))
        
        if 'coverage' in topics:
            answer_parts.append(self._explain_coverage_in_context(df))
        
        # Handle purpose/why questions
        if question_analysis['asks_about_purpose']:
            answer_parts.append(self._explain_purpose_in_context(df, topics))
        
        # Handle comparison questions
        if question_analysis['asks_for_comparison']:
            answer_parts.append(self._provide_comparison_in_context(df, topics))
        
        # If no specific topics, provide general overview
        if not answer_parts:
            answer_parts.append(self._provide_general_overview(df))
        
        return " ".join(answer_parts)
    
    def _get_ward_specific_info(self, ward_row: pd.Series, full_df: pd.DataFrame) -> str:
        """Get specific information about a ward using actual data."""
        info_parts = []
        
        # Risk information
        risk_cols = [col for col in ward_row.index if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        if risk_cols:
            risk_col = risk_cols[0]
            risk_value = ward_row[risk_col]
            rank = (full_df[risk_col] > risk_value).sum() + 1
            total_wards = len(full_df)
            info_parts.append(f"ranks #{rank} out of {total_wards} wards in {risk_col} ({risk_value:.3f})")
        
        # Coverage information
        coverage_cols = [col for col in ward_row.index if 'coverage' in col.lower()]
        if coverage_cols:
            coverage_col = coverage_cols[0]
            coverage_value = ward_row[coverage_col]
            info_parts.append(f"has {coverage_value:.1f}% {coverage_col}")
        
        # Population if available
        pop_cols = [col for col in ward_row.index if 'population' in col.lower()]
        if pop_cols:
            pop_value = ward_row[pop_cols[0]]
            info_parts.append(f"population of {pop_value:,.0f}")
        
        return ", ".join(info_parts) if info_parts else "data available in dataset"
    
    def _explain_composite_score_in_context(self, df: pd.DataFrame) -> str:
        """Explain composite score using the user's actual data."""
        if 'composite_score' not in df.columns:
            return "Composite score is not available in your dataset."
        
        scores = df['composite_score'].dropna()
        mean_score = scores.mean()
        min_score = scores.min()
        max_score = scores.max()
        
        # Find highest and lowest scoring wards
        highest_ward = df.loc[df['composite_score'].idxmax(), 'WardName'] if 'WardName' in df.columns else "Unknown ward"
        lowest_ward = df.loc[df['composite_score'].idxmin(), 'WardName'] if 'WardName' in df.columns else "Unknown ward"
        
        explanation = f"""Composite score in your data combines multiple malaria risk factors into a single number. 
        In your {len(df)} wards, scores range from {min_score:.3f} ({lowest_ward}) to {max_score:.3f} ({highest_ward}), 
        with an average of {mean_score:.3f}. Higher scores indicate higher malaria risk. 
        This helps prioritize which wards need more intervention resources."""
        
        return explanation
    
    def _explain_pca_score_in_context(self, df: pd.DataFrame) -> str:
        """Explain PCA score using the user's actual data."""
        if 'pca_score' not in df.columns:
            return "PCA score is not available in your dataset."
        
        scores = df['pca_score'].dropna()
        return f"""PCA (Principal Component Analysis) score in your data captures the main patterns 
        of malaria risk across your {len(df)} wards. It ranges from {scores.min():.3f} to {scores.max():.3f} 
        in your dataset. PCA reduces complex multi-variable risk patterns into a single score that 
        explains the most variation in your data."""
    
    def _explain_risk_in_context(self, df: pd.DataFrame) -> str:
        """Explain risk concept using user's data patterns."""
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        
        if not risk_cols:
            return "No risk indicators found in your dataset."
        
        risk_col = risk_cols[0]
        risk_data = df[risk_col].dropna()
        high_risk_count = (risk_data >= risk_data.quantile(0.8)).sum()
        
        return f"""Risk in your dataset (measured by {risk_col}) indicates malaria transmission potential. 
        {high_risk_count} of your {len(df)} wards are high-risk (top 20%). Risk helps identify where 
        malaria interventions will have the biggest impact on reducing disease burden."""
    
    def _explain_coverage_in_context(self, df: pd.DataFrame) -> str:
        """Explain coverage using user's actual coverage data."""
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        
        if not coverage_cols:
            return "No intervention coverage data found in your dataset."
        
        coverage_explanations = []
        for col in coverage_cols[:2]:  # Limit to 2 coverage types
            coverage_data = df[col].dropna()
            mean_coverage = coverage_data.mean()
            low_coverage_count = (coverage_data < 60).sum()
            
            coverage_explanations.append(f"{col} averages {mean_coverage:.1f}% across your wards, with {low_coverage_count} wards below 60%")
        
        return f"Coverage measures intervention reach in your area. " + ". ".join(coverage_explanations) + ". Higher coverage means better protection from malaria."
    
    def _explain_purpose_in_context(self, df: pd.DataFrame, topics: List[str]) -> str:
        """Explain the purpose of analysis using user's context."""
        purpose_parts = []
        
        if 'risk' in topics or 'composite_score' in topics:
            purpose_parts.append(f"We analyze malaria risk in your {len(df)} wards to identify which areas need the most urgent intervention")
        
        if 'coverage' in topics:
            purpose_parts.append("Coverage analysis shows where intervention gaps exist and resources should be reallocated")
        
        if not purpose_parts:
            purpose_parts.append(f"Analysis of your {len(df)}-ward dataset helps make evidence-based decisions about malaria control priorities")
        
        return ". ".join(purpose_parts) + ". This ensures limited resources go where they'll save the most lives."
    
    def _provide_comparison_in_context(self, df: pd.DataFrame, topics: List[str]) -> str:
        """Provide comparisons using actual data."""
        if 'risk' in topics and 'coverage' in topics:
            risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
            coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
            
            if risk_cols and coverage_cols:
                return f"Risk shows malaria transmission potential while coverage shows intervention reach. In your data, they help identify high-risk, low-coverage wards that need immediate attention."
        
        return "Comparisons help identify patterns and prioritize interventions in your specific area."
    
    def _provide_general_overview(self, df: pd.DataFrame) -> str:
        """Provide general overview of user's malaria situation."""
        overview_parts = [f"Your dataset contains {len(df)} wards with malaria-related information."]
        
        # Risk overview
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        if risk_cols:
            overview_parts.append(f"Risk assessment available using {risk_cols[0]}.")
        
        # Coverage overview
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        if coverage_cols:
            overview_parts.append(f"Intervention coverage data available for {len(coverage_cols)} intervention types.")
        
        # Geographic scope
        if len(df) < 100:
            scope = "a focused geographic area"
        elif len(df) < 500:
            scope = "a regional area"
        else:
            scope = "a large geographic region"
        
        overview_parts.append(f"This represents {scope} suitable for targeted malaria control planning.")
        
        return " ".join(overview_parts)
    
    def _create_data_examples(self, df: pd.DataFrame, question_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Create specific examples from user's data."""
        examples = {}
        
        # Ward examples
        if 'WardName' in df.columns:
            examples['example_wards'] = df['WardName'].head(3).tolist()
        
        # Risk examples
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
        if risk_cols:
            risk_col = risk_cols[0]
            highest_risk_ward = df.loc[df[risk_col].idxmax()]
            lowest_risk_ward = df.loc[df[risk_col].idxmin()]
            
            examples['risk_examples'] = {
                'highest_risk': {
                    'ward': highest_risk_ward.get('WardName', 'Unknown'),
                    'score': float(highest_risk_ward[risk_col])
                },
                'lowest_risk': {
                    'ward': lowest_risk_ward.get('WardName', 'Unknown'),
                    'score': float(lowest_risk_ward[risk_col])
                }
            }
        
        return examples
    
    def _suggest_follow_up_questions(self, question_analysis: Dict[str, Any], df: pd.DataFrame) -> List[str]:
        """Suggest relevant follow-up questions based on user's question and data."""
        suggestions = []
        
        if 'risk' in question_analysis['topics_mentioned']:
            suggestions.append("Which wards have the highest malaria risk in my area?")
            suggestions.append("What factors are driving high risk in my dataset?")
        
        if 'coverage' in question_analysis['topics_mentioned']:
            suggestions.append("Where are the biggest coverage gaps in my area?")
            suggestions.append("How can I improve intervention coverage efficiently?")
        
        if question_analysis['ward_names_mentioned']:
            suggestions.append("How do these wards compare to others in my dataset?")
            suggestions.append("What interventions would work best for these specific wards?")
        
        # Always relevant questions
        suggestions.extend([
            "What should I prioritize based on my data?",
            "How do I explain these findings to my supervisor?"
        ])
        
        return suggestions[:5]  # Limit to 5 suggestions
    
    def _get_relevant_data_context(self, df: pd.DataFrame, question_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Get relevant data context for the user's question."""
        context = {
            'total_wards': len(df),
            'data_columns': len(df.columns),
            'geographic_scope': self._determine_geographic_scope(df)
        }
        
        # Add topic-specific context
        if 'risk' in question_analysis['topics_mentioned']:
            risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
            if risk_cols:
                context['risk_data_available'] = True
                context['risk_column'] = risk_cols[0]
        
        if 'coverage' in question_analysis['topics_mentioned']:
            coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
            context['coverage_types'] = coverage_cols
        
        return context
    
    def _analyze_data_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the dataset to understand context."""
        
        analysis = {
            'overview': {},
            'risk_analysis': {},
            'intervention_analysis': {},
            'geographic_analysis': {},
            'data_quality': {}
        }
        
        # Dataset overview
        analysis['overview'] = {
            'total_wards': len(df),
            'total_columns': len(df.columns),
            'geographic_scope': self._determine_geographic_scope(df),
            'data_period': self._estimate_data_period(df)
        }
        
        # Risk analysis
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr', 'burden'])]
        if risk_cols:
            primary_risk_col = risk_cols[0]
            risk_data = df[primary_risk_col].dropna()
            
            analysis['risk_analysis'] = {
                'risk_column_used': primary_risk_col,
                'risk_range': f"{risk_data.min():.3f} - {risk_data.max():.3f}",
                'mean_risk': risk_data.mean(),
                'risk_distribution': self._categorize_risk_distribution(risk_data),
                'high_risk_wards': len(df[risk_data >= risk_data.quantile(0.8)]),
                'low_risk_wards': len(df[risk_data <= risk_data.quantile(0.2)]),
                'risk_variation': 'High' if risk_data.std() > risk_data.mean() * 0.3 else 'Moderate' if risk_data.std() > risk_data.mean() * 0.15 else 'Low'
            }
            
            analysis['risk_characterization'] = self._characterize_risk_situation(analysis['risk_analysis'])
        else:
            analysis['risk_analysis'] = {'note': 'No risk/burden variables found in dataset'}
            analysis['risk_characterization'] = 'unknown risk patterns'
        
        # Intervention analysis
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        if coverage_cols:
            coverage_data = {}
            for col in coverage_cols:
                coverage_data[col] = {
                    'mean': df[col].mean(),
                    'range': f"{df[col].min():.1f}% - {df[col].max():.1f}%",
                    'below_60': len(df[df[col] < 60]),
                    'above_80': len(df[df[col] >= 80])
                }
            
            analysis['intervention_analysis'] = {
                'coverage_types_available': list(coverage_data.keys()),
                'coverage_details': coverage_data,
                'overall_coverage_level': self._assess_coverage_level(coverage_data)
            }
            
            analysis['coverage_characterization'] = self._characterize_coverage_situation(analysis['intervention_analysis'])
        else:
            analysis['intervention_analysis'] = {'note': 'No intervention coverage data found'}
            analysis['coverage_characterization'] = 'unknown intervention coverage'
        
        # Geographic analysis
        geo_indicators = self._identify_geographic_indicators(df)
        analysis['geographic_analysis'] = {
            'spatial_indicators_found': geo_indicators,
            'settlement_patterns': self._analyze_settlement_patterns(df),
            'environmental_factors': self._identify_environmental_factors(df)
        }
        
        # Data quality
        analysis['data_quality'] = {
            'completeness_score': self._calculate_completeness_score(df),
            'missing_data_summary': self._summarize_missing_data(df),
            'data_consistency_issues': self._identify_consistency_issues(df)
        }
        
        return analysis
    
    def _determine_geographic_scope(self, df: pd.DataFrame) -> str:
        """Determine the geographic scope of the dataset."""
        if len(df) < 50:
            return "City/Metropolitan area"
        elif len(df) < 200:
            return "State/Province level"
        elif len(df) < 1000:
            return "Multi-state/Regional"
        else:
            return "National level"
    
    def _categorize_risk_distribution(self, risk_data: pd.Series) -> str:
        """Categorize the risk distribution pattern."""
        q25, q50, q75 = risk_data.quantile([0.25, 0.5, 0.75])
        
        if q75 - q25 < 0.1:  # Tight distribution
            if q50 > 0.6:
                return "Uniformly high risk"
            elif q50 < 0.3:
                return "Uniformly low risk"
            else:
                return "Uniformly moderate risk"
        else:  # Spread distribution
            if q75 > 0.7:
                return "Mixed with high-risk hotspots"
            elif q25 < 0.2:
                return "Mixed with low-risk areas"
            else:
                return "Heterogeneous risk distribution"
    
    def _characterize_risk_situation(self, risk_analysis: Dict[str, Any]) -> str:
        """Create a human-readable characterization of risk situation."""
        distribution = risk_analysis.get('risk_distribution', '')
        variation = risk_analysis.get('risk_variation', 'Unknown')
        
        if 'uniformly high' in distribution.lower():
            return f"{distribution.lower()} across most areas"
        elif 'uniformly low' in distribution.lower():
            return f"{distribution.lower()} with limited variation"
        elif 'mixed' in distribution.lower():
            return f"{distribution.lower()} requiring targeted approaches"
        else:
            return f"{variation.lower()} variation in malaria burden"
    
    def _assess_coverage_level(self, coverage_data: Dict[str, Dict]) -> str:
        """Assess overall intervention coverage level."""
        if not coverage_data:
            return "Unknown"
        
        avg_coverage = np.mean([data['mean'] for data in coverage_data.values()])
        
        if avg_coverage >= 80:
            return "High coverage"
        elif avg_coverage >= 60:
            return "Moderate coverage"
        elif avg_coverage >= 40:
            return "Low coverage"
        else:
            return "Very low coverage"
    
    def _characterize_coverage_situation(self, intervention_analysis: Dict[str, Any]) -> str:
        """Create human-readable coverage characterization."""
        coverage_level = intervention_analysis.get('overall_coverage_level', 'unknown')
        
        if coverage_level == "High coverage":
            return "good intervention coverage levels"
        elif coverage_level == "Moderate coverage":
            return "moderate intervention coverage with room for improvement"
        elif coverage_level == "Low coverage":
            return "significant intervention coverage gaps"
        else:
            return "very low intervention coverage requiring immediate attention"
    
    def _identify_geographic_indicators(self, df: pd.DataFrame) -> List[str]:
        """Identify available geographic indicators."""
        indicators = []
        
        if any('urban' in col.lower() for col in df.columns):
            indicators.append('Urban/rural classification')
        if any('elevation' in col.lower() for col in df.columns):
            indicators.append('Elevation data')
        if any('water' in col.lower() for col in df.columns):
            indicators.append('Water proximity')
        if any('temp' in col.lower() for col in df.columns):
            indicators.append('Temperature data')
        if any('rain' in col.lower() for col in df.columns):
            indicators.append('Rainfall data')
        
        return indicators
    
    def _analyze_settlement_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze settlement patterns if data available."""
        patterns = {}
        
        # Urban/rural distribution
        urban_cols = [col for col in df.columns if 'urban' in col.lower()]
        if urban_cols:
            urban_col = urban_cols[0]
            if df[urban_col].dtype == 'bool' or df[urban_col].dtype == 'object':
                urban_count = df[urban_col].sum() if df[urban_col].dtype == 'bool' else len(df[df[urban_col].str.contains('urban', case=False, na=False)])
                patterns['urban_rural_split'] = f"{urban_count} urban, {len(df) - urban_count} rural wards"
            else:
                # Numeric urban indicator
                avg_urban = df[urban_col].mean()
                patterns['urbanization_level'] = f"{avg_urban:.1%} average urbanization"
        
        return patterns
    
    def _identify_environmental_factors(self, df: pd.DataFrame) -> List[str]:
        """Identify environmental factors in the dataset."""
        factors = []
        
        env_keywords = {
            'elevation': 'Elevation/altitude',
            'temp': 'Temperature',
            'rain': 'Rainfall/precipitation',
            'vegetation': 'Vegetation coverage',
            'water': 'Water bodies proximity',
            'humidity': 'Humidity levels'
        }
        
        for keyword, description in env_keywords.items():
            if any(keyword in col.lower() for col in df.columns):
                factors.append(description)
        
        return factors
    
    def _calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """Calculate data completeness score."""
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        return (total_cells - missing_cells) / total_cells
    
    def _summarize_missing_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Summarize missing data patterns."""
        missing_summary = {}
        
        missing_by_column = df.isnull().sum()
        columns_with_missing = missing_by_column[missing_by_column > 0]
        
        if len(columns_with_missing) > 0:
            missing_summary['columns_with_missing'] = len(columns_with_missing)
            missing_summary['worst_missing_column'] = missing_by_column.idxmax()
            missing_summary['worst_missing_percent'] = (missing_by_column.max() / len(df)) * 100
        else:
            missing_summary['status'] = 'No missing data detected'
        
        return missing_summary
    
    def _identify_consistency_issues(self, df: pd.DataFrame) -> List[str]:
        """Identify potential data consistency issues."""
        issues = []
        
        # Check for impossible values in coverage columns
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        for col in coverage_cols:
            if df[col].max() > 100:
                issues.append(f"{col} has values >100%")
            if df[col].min() < 0:
                issues.append(f"{col} has negative values")
        
        # Check for extreme outliers in risk scores
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
        for col in risk_cols:
            if df[col].dtype in ['float64', 'int64']:
                q99 = df[col].quantile(0.99)
                q01 = df[col].quantile(0.01)
                if (q99 - q01) > 10 * df[col].std():
                    issues.append(f"{col} has extreme outliers")
        
        return issues
    
    def _generate_contextual_explanations(self, df: pd.DataFrame, analysis: Dict[str, Any]) -> Dict[str, str]:
        """Generate contextual explanations based on data analysis."""
        explanations = {}
        
        # Risk explanation
        if 'risk_analysis' in analysis and 'risk_column_used' in analysis['risk_analysis']:
            risk_col = analysis['risk_analysis']['risk_column_used']
            risk_range = analysis['risk_analysis']['risk_range']
            mean_risk = analysis['risk_analysis']['mean_risk']
            
            explanations['risk_situation'] = f"""
Your dataset shows malaria risk scores (using {risk_col}) ranging from {risk_range}, 
with an average of {mean_risk:.3f}. This suggests {analysis['risk_characterization']}. 
{analysis['risk_analysis']['high_risk_wards']} wards ({analysis['risk_analysis']['high_risk_wards']/len(df)*100:.1f}%) 
are in the highest risk category, while {analysis['risk_analysis']['low_risk_wards']} wards 
({analysis['risk_analysis']['low_risk_wards']/len(df)*100:.1f}%) are relatively low risk.
"""
        
        # Coverage explanation
        if 'intervention_analysis' in analysis and 'coverage_details' in analysis['intervention_analysis']:
            coverage_details = analysis['intervention_analysis']['coverage_details']
            explanations['intervention_situation'] = f"""
Your intervention landscape shows {analysis['coverage_characterization']}. 
Based on available coverage data: {', '.join(coverage_details.keys())}.
Coverage levels vary significantly across wards, indicating opportunities for 
optimization and targeted resource allocation.
"""
        
        # Geographic explanation
        geo_indicators = analysis['geographic_analysis']['spatial_indicators_found']
        if geo_indicators:
            explanations['geographic_context'] = f"""
Your dataset includes geographic information: {', '.join(geo_indicators)}. 
This allows for spatial analysis of malaria patterns and environmentally-informed 
intervention targeting. The data covers a {analysis['overview']['geographic_scope'].lower()}.
"""
        
        return explanations
    
    def _extract_actionable_insights(self, df: pd.DataFrame, analysis: Dict[str, Any]) -> List[str]:
        """Extract actionable insights from the analysis."""
        insights = []
        
        # Risk-based insights
        if 'risk_analysis' in analysis and 'high_risk_wards' in analysis['risk_analysis']:
            high_risk_count = analysis['risk_analysis']['high_risk_wards']
            if high_risk_count > len(df) * 0.3:
                insights.append(f"High burden area: {high_risk_count} wards need intensive intervention focus")
            elif high_risk_count < len(df) * 0.1:
                insights.append(f"Targeted approach: Focus resources on {high_risk_count} highest-risk wards for maximum impact")
            else:
                insights.append(f"Balanced approach: {high_risk_count} high-risk wards require priority, but don't neglect moderate-risk areas")
        
        # Coverage insights
        if 'intervention_analysis' in analysis and 'coverage_details' in analysis['intervention_analysis']:
            for coverage_type, details in analysis['intervention_analysis']['coverage_details'].items():
                if details['below_60'] > len(df) * 0.3:
                    insights.append(f"Coverage gap priority: {details['below_60']} wards have {coverage_type} below 60%")
        
        # Data quality insights
        completeness = analysis['data_quality']['completeness_score']
        if completeness < 0.9:
            insights.append(f"Data enhancement opportunity: {(1-completeness)*100:.1f}% missing data could be collected for better targeting")
        
        # Geographic insights
        if len(analysis['geographic_analysis']['environmental_factors']) >= 3:
            insights.append("Environmental targeting possible: Rich environmental data allows for climate-informed intervention strategies")
        
        return insights
    
    def _estimate_data_period(self, df: pd.DataFrame) -> str:
        """Estimate the time period of the data."""
        # Look for date columns or year indicators
        date_cols = [col for col in df.columns if any(term in col.lower() for term in ['date', 'year', 'time', 'period'])]
        
        if date_cols:
            return f"Time-series data available ({', '.join(date_cols)})"
        else:
            return "Cross-sectional (single time point)"


class GetPersonalizedRecommendations(BaseTool):
    """
    Generate personalized intervention recommendations based on the user's specific data patterns.
    
    Analyzes risk distribution, coverage gaps, and resource constraints to provide
    tailored strategic recommendations for malaria control program optimization.
    """
    
    recommendation_type: str = Field(
        "strategic",
        description="Type of recommendations: 'strategic', 'operational', 'targeting', 'resource_allocation'",
        pattern="^(strategic|operational|targeting|resource_allocation)$"
    )
    
    resource_constraint: str = Field(
        "moderate",
        description="Resource constraint level: 'limited', 'moderate', 'adequate', 'unconstrained'",
        pattern="^(limited|moderate|adequate|unconstrained)$"
    )
    
    priority_objective: str = Field(
        "maximize_impact",
        description="Primary objective: 'maximize_impact', 'equity_focus', 'cost_efficiency', 'coverage_expansion'",
        pattern="^(maximize_impact|equity_focus|cost_efficiency|coverage_expansion)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.GENERAL_KNOWLEDGE
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What should I prioritize based on my specific risk patterns?",
            "Give me operational recommendations for my coverage gaps",  
            "How should I allocate limited resources across my wards?",
            "What's the best targeting strategy for my data?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Generate personalized recommendations based on user's data."""
        try:
            # Get dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Analyze data patterns
            data_patterns = self._analyze_patterns_for_recommendations(df)
            
            # Generate recommendations based on patterns
            recommendations = self._generate_personalized_recommendations(df, data_patterns)
            
            # Create implementation roadmap
            roadmap = self._create_implementation_roadmap(recommendations, data_patterns)
            
            message = f"Generated {len(recommendations['immediate_actions'])} immediate and {len(recommendations['medium_term_actions'])} medium-term recommendations based on your specific data patterns."
            
            return self._create_success_result(
                message=message,
                data={
                    'personalized_recommendations': recommendations,
                    'implementation_roadmap': roadmap,
                    'data_patterns_identified': data_patterns,
                    'success_metrics': self._define_success_metrics(data_patterns),
                    'risk_mitigation': self._identify_implementation_risks(data_patterns)
                }
            )
            
        except Exception as e:
            logger.error(f"Error generating personalized recommendations: {e}")
            return self._create_error_result(f"Recommendation generation failed: {str(e)}")
    
    def _analyze_patterns_for_recommendations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data patterns to inform recommendations."""
        patterns = {}
        
        # Risk patterns
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        if risk_cols:
            risk_col = risk_cols[0]
            risk_data = df[risk_col].dropna()
            
            patterns['risk_patterns'] = {
                'distribution_type': self._classify_distribution(risk_data),
                'concentration_level': self._assess_risk_concentration(risk_data),
                'spatial_clustering': self._assess_spatial_clustering(df, risk_col),
                'outlier_wards': self._identify_outlier_wards(df, risk_col)
            }
        
        # Coverage patterns
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        if coverage_cols:
            patterns['coverage_patterns'] = {}
            for col in coverage_cols:
                patterns['coverage_patterns'][col] = {
                    'gap_severity': self._assess_gap_severity(df[col]),
                    'equity_distribution': self._assess_coverage_equity(df[col]),
                    'improvement_potential': self._assess_improvement_potential(df[col])
                }
        
        # Resource efficiency patterns
        patterns['efficiency_patterns'] = self._analyze_efficiency_opportunities(df)
        
        # Geographic patterns
        patterns['geographic_patterns'] = self._analyze_geographic_patterns(df)
        
        return patterns
    
    def _generate_personalized_recommendations(self, df: pd.DataFrame, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Generate recommendations based on identified patterns."""
        recommendations = {
            'immediate_actions': [],
            'medium_term_actions': [],
            'long_term_strategies': [],
            'resource_optimization': [],
            'targeting_strategies': []
        }
        
        # Risk-based recommendations
        if 'risk_patterns' in patterns:
            risk_recs = self._generate_risk_based_recommendations(df, patterns['risk_patterns'])
            recommendations['immediate_actions'].extend(risk_recs['immediate'])
            recommendations['targeting_strategies'].extend(risk_recs['targeting'])
        
        # Coverage-based recommendations  
        if 'coverage_patterns' in patterns:
            coverage_recs = self._generate_coverage_recommendations(df, patterns['coverage_patterns'])
            recommendations['immediate_actions'].extend(coverage_recs['immediate'])
            recommendations['medium_term_actions'].extend(coverage_recs['medium_term'])
        
        # Efficiency recommendations
        if 'efficiency_patterns' in patterns:
            efficiency_recs = self._generate_efficiency_recommendations(patterns['efficiency_patterns'])
            recommendations['resource_optimization'].extend(efficiency_recs)
        
        return recommendations
    
    def _generate_risk_based_recommendations(self, df: pd.DataFrame, risk_patterns: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate recommendations based on risk patterns."""
        recs = {'immediate': [], 'targeting': []}
        
        if risk_patterns['concentration_level'] == 'highly_concentrated':
            recs['immediate'].append("Focus 70% of resources on identified high-risk hotspots for maximum impact")
            recs['targeting'].append("Use hotspot targeting strategy - intensive intervention in top 20% risk wards")
        elif risk_patterns['concentration_level'] == 'dispersed':
            recs['immediate'].append("Implement broad coverage strategy across all moderate-to-high risk areas")
            recs['targeting'].append("Use blanket coverage approach with risk-weighted resource allocation")
        
        if 'outlier_wards' in risk_patterns and len(risk_patterns['outlier_wards']) > 0:
            outlier_names = [ward.get('WardName', f'Ward_{i}') for i, ward in enumerate(risk_patterns['outlier_wards'][:3])]
            recs['immediate'].append(f"Investigate extreme risk wards: {', '.join(outlier_names)} - they may need special interventions")
        
        return recs
    
    def _generate_coverage_recommendations(self, df: pd.DataFrame, coverage_patterns: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate coverage-based recommendations."""
        recs = {'immediate': [], 'medium_term': []}
        
        for coverage_type, pattern in coverage_patterns.items():
            if pattern['gap_severity'] == 'severe':
                recs['immediate'].append(f"Priority: Address severe {coverage_type} gaps - expand coverage by 25-30% in lowest-covered wards")
            elif pattern['gap_severity'] == 'moderate':
                recs['medium_term'].append(f"Gradual {coverage_type} improvement - target 15-20% coverage increase over 6 months")
        
            if pattern['equity_distribution'] == 'inequitable':
                recs['medium_term'].append(f"Implement equity-focused {coverage_type} redistribution from over-covered to under-covered areas")
        
        return recs
    
    def _create_implementation_roadmap(self, recommendations: Dict[str, Any], patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Create implementation roadmap based on recommendations."""
        roadmap = {
            'phase_1_immediate': {
                'timeline': '0-3 months',
                'actions': recommendations['immediate_actions'][:3],  # Top 3 priorities
                'success_criteria': ['Improved targeting of highest-risk areas', 'Resource reallocation initiated']
            },
            'phase_2_optimization': {
                'timeline': '3-12 months',
                'actions': recommendations['medium_term_actions'],
                'success_criteria': ['Coverage gaps reduced by 20%', 'Intervention efficiency improved']
            },
            'phase_3_sustainability': {
                'timeline': '12+ months',
                'actions': recommendations['long_term_strategies'],
                'success_criteria': ['Sustainable coverage levels achieved', 'Data-driven decision making institutionalized']
            }
        }
        
        return roadmap
    
    def _define_success_metrics(self, patterns: Dict[str, Any]) -> Dict[str, str]:
        """Define success metrics based on data patterns."""
        metrics = {}
        
        if 'risk_patterns' in patterns:
            metrics['risk_reduction'] = "Monitor changes in high-risk ward count and average risk scores"
            
        if 'coverage_patterns' in patterns:
            metrics['coverage_improvement'] = "Track coverage increase in bottom-quartile wards"
            metrics['equity_improvement'] = "Measure reduction in coverage variance across wards"
        
        metrics['overall_impact'] = "Monitor population at risk reduction and intervention cost-effectiveness"
        
        return metrics
    
    # Helper methods for pattern analysis
    def _classify_distribution(self, data: pd.Series) -> str:
        """Classify the distribution pattern of data."""
        skewness = data.skew()
        if abs(skewness) < 0.5:
            return 'normal'
        elif skewness > 0.5:
            return 'right_skewed'
        else:
            return 'left_skewed'
    
    def _assess_risk_concentration(self, risk_data: pd.Series) -> str:
        """Assess how concentrated the risk is."""
        top_20_percent = risk_data.quantile(0.8)
        top_20_share = len(risk_data[risk_data >= top_20_percent]) / len(risk_data)
        
        if top_20_share > 0.25:  # More than 25% in top 20%
            return 'dispersed'
        elif top_20_share < 0.15:  # Less than 15% in top 20%
            return 'highly_concentrated'
        else:
            return 'moderately_concentrated'
    
    def _assess_spatial_clustering(self, df: pd.DataFrame, risk_col: str) -> str:
        """Assess spatial clustering of risk (simplified)."""
        # Simple proxy: check if there are location indicators
        if any(col in df.columns for col in ['lat', 'lon', 'latitude', 'longitude', 'x', 'y']):
            return 'spatial_data_available'
        else:
            return 'no_spatial_data'
    
    def _identify_outlier_wards(self, df: pd.DataFrame, risk_col: str) -> List[Dict]:
        """Identify outlier wards with extreme risk values."""
        risk_data = df[risk_col].dropna()
        q99 = risk_data.quantile(0.99)
        outliers = df[df[risk_col] >= q99]
        
        return outliers.head(5).to_dict('records')  # Top 5 outliers
    
    def _assess_gap_severity(self, coverage_data: pd.Series) -> str:
        """Assess severity of coverage gaps."""
        below_50 = (coverage_data < 50).sum() / len(coverage_data)
        
        if below_50 > 0.4:
            return 'severe'
        elif below_50 > 0.2:
            return 'moderate'
        else:
            return 'minor'
    
    def _assess_coverage_equity(self, coverage_data: pd.Series) -> str:
        """Assess equity of coverage distribution."""
        cv = coverage_data.std() / coverage_data.mean()  # Coefficient of variation
        
        if cv > 0.3:
            return 'inequitable'
        elif cv > 0.15:
            return 'moderately_equitable'
        else:
            return 'equitable'
    
    def _assess_improvement_potential(self, coverage_data: pd.Series) -> str:
        """Assess potential for coverage improvement."""
        room_for_improvement = (100 - coverage_data).mean()
        
        if room_for_improvement > 40:
            return 'high_potential'
        elif room_for_improvement > 20:
            return 'moderate_potential'
        else:
            return 'limited_potential'
    
    def _analyze_efficiency_opportunities(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze opportunities for efficiency improvements."""
        opportunities = {}
        
        # Resource reallocation opportunities
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
        
        if coverage_cols and risk_cols:
            opportunities['reallocation_potential'] = 'high'  # Can compare risk vs coverage
        else:
            opportunities['reallocation_potential'] = 'limited'
        
        return opportunities
    
    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze geographic patterns for recommendations."""
        patterns = {}
        
        # Urban/rural analysis
        urban_cols = [col for col in df.columns if 'urban' in col.lower()]
        if urban_cols:
            patterns['urban_rural_available'] = True
        
        # Environmental factors
        env_cols = [col for col in df.columns if any(term in col.lower() for term in ['temp', 'rain', 'elevation'])]
        if env_cols:
            patterns['environmental_targeting_possible'] = True
        
        return patterns
    
    def _generate_efficiency_recommendations(self, efficiency_patterns: Dict[str, Any]) -> List[str]:
        """Generate efficiency-focused recommendations."""
        recs = []
        
        if efficiency_patterns.get('reallocation_potential') == 'high':
            recs.append("Implement data-driven resource reallocation from low-risk/high-coverage to high-risk/low-coverage wards")
            recs.append("Use scenario simulation tools to model optimal resource distribution before implementation")
        
        return recs
    
    def _identify_implementation_risks(self, patterns: Dict[str, Any]) -> List[str]:
        """Identify potential implementation risks."""
        risks = []
        
        if 'risk_patterns' in patterns and patterns['risk_patterns']['concentration_level'] == 'highly_concentrated':
            risks.append("Risk: Focusing too narrowly on hotspots may neglect emerging risk areas")
        
        risks.append("Risk: Data quality changes over time may affect targeting accuracy")
        risks.append("Risk: Local capacity constraints may limit implementation speed")
        
        return risks


class InterpretYourResults(BaseTool):
    """
    Help users understand what their specific analysis results mean for action.
    
    Takes analysis outputs and translates them into clear, actionable interpretations
    tailored to the user's context and objectives.
    """
    
    result_type: str = Field(
        "general",
        description="Type of results to interpret: 'general', 'risk_analysis', 'targeting', 'scenarios', 'visualizations'",
        pattern="^(general|risk_analysis|targeting|scenarios|visualizations)$"
    )
    
    interpretation_level: str = Field(
        "actionable",
        description="Interpretation depth: 'basic', 'detailed', 'actionable', 'technical'",
        pattern="^(basic|detailed|actionable|technical)$"
    )
    
    decision_context: str = Field(
        "program_management",
        description="Decision context: 'program_management', 'policy_making', 'research', 'funding_proposal'",
        pattern="^(program_management|policy_making|research|funding_proposal)$"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.GENERAL_KNOWLEDGE
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What do my risk analysis results mean for program decisions?",
            "Help me interpret my targeting analysis outcomes",
            "Explain my scenario simulation results in practical terms",
            "What actions should I take based on my analysis results?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Interpret analysis results for actionable insights."""
        try:
            # Get dataset for context
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Analyze current analysis state
            analysis_state = self._assess_analysis_state(df)
            
            # Generate interpretations
            interpretations = self._generate_result_interpretations(df, analysis_state)
            
            # Create action recommendations
            actions = self._translate_to_actions(interpretations, analysis_state)
            
            message = f"Interpreted your analysis results for {len(df)} wards. Generated {len(actions['immediate_actions'])} immediate and {len(actions['strategic_actions'])} strategic actions."
            
            return self._create_success_result(
                message=message,
                data={
                    'result_interpretations': interpretations,
                    'actionable_insights': actions,
                    'decision_support': self._create_decision_support(interpretations, actions),
                    'communication_templates': self._create_communication_templates(interpretations),
                    'next_analysis_recommendations': self._recommend_next_analyses(analysis_state)
                }
            )
            
        except Exception as e:
            logger.error(f"Error interpreting results: {e}")
            return self._create_error_result(f"Result interpretation failed: {str(e)}")
    
    def _assess_analysis_state(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess what analyses have been completed."""
        state = {
            'data_available': True,
            'risk_analysis_possible': False,
            'coverage_analysis_possible': False,
            'spatial_analysis_possible': False,
            'temporal_analysis_possible': False
        }
        
        # Check for risk analysis capability
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        if risk_cols:
            state['risk_analysis_possible'] = True
            state['primary_risk_indicator'] = risk_cols[0]
        
        # Check for coverage analysis capability
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        if coverage_cols:
            state['coverage_analysis_possible'] = True
            state['coverage_indicators'] = coverage_cols
        
        # Check for spatial analysis capability
        spatial_cols = [col for col in df.columns if col.lower() in ['lat', 'lon', 'latitude', 'longitude', 'x', 'y']]
        if spatial_cols or any('geometry' in col.lower() for col in df.columns):
            state['spatial_analysis_possible'] = True
        
        return state
    
    def _generate_result_interpretations(self, df: pd.DataFrame, analysis_state: Dict[str, Any]) -> Dict[str, Any]:
        """Generate interpretations based on available results."""
        interpretations = {}
        
        # Risk analysis interpretation
        if analysis_state['risk_analysis_possible']:
            risk_col = analysis_state['primary_risk_indicator']
            interpretations['risk_analysis'] = self._interpret_risk_results(df, risk_col)
        
        # Coverage analysis interpretation
        if analysis_state['coverage_analysis_possible']:
            interpretations['coverage_analysis'] = self._interpret_coverage_results(df, analysis_state['coverage_indicators'])
        
        # Overall program implications
        interpretations['program_implications'] = self._interpret_program_implications(df, analysis_state)
        
        return interpretations
    
    def _interpret_risk_results(self, df: pd.DataFrame, risk_col: str) -> Dict[str, Any]:
        """Interpret risk analysis results."""
        risk_data = df[risk_col].dropna()
        
        interpretation = {
            'summary': f"Risk analysis using {risk_col} reveals patterns across {len(df)} wards",
            'key_findings': [],
            'implications': [],
            'priority_areas': []
        }
        
        # Key findings
        high_risk_threshold = risk_data.quantile(0.8)
        high_risk_count = (risk_data >= high_risk_threshold).sum()
        
        interpretation['key_findings'].append(f"{high_risk_count} wards ({high_risk_count/len(df)*100:.1f}%) are high-risk (>{high_risk_threshold:.3f})")
        
        risk_range = risk_data.max() - risk_data.min()
        if risk_range > risk_data.std() * 3:
            interpretation['key_findings'].append("Extreme variation in risk levels suggests need for differentiated strategies")
        else:
            interpretation['key_findings'].append("Moderate risk variation allows for standardized approaches with risk-weighting")
        
        # Implications
        if high_risk_count < len(df) * 0.2:
            interpretation['implications'].append("Concentrated risk pattern: Focus resources on identified hotspots for maximum impact")
        else:
            interpretation['implications'].append("Dispersed risk pattern: Broad intervention coverage needed with risk-based prioritization")
        
        # Priority areas (top 5 highest risk)
        top_risk_wards = df.nlargest(5, risk_col)
        for idx, row in top_risk_wards.iterrows():
            ward_name = row.get('WardName', f'Ward_{idx}')
            risk_value = row[risk_col]
            interpretation['priority_areas'].append(f"{ward_name}: {risk_value:.3f} risk score")
        
        return interpretation
    
    def _interpret_coverage_results(self, df: pd.DataFrame, coverage_cols: List[str]) -> Dict[str, Any]:
        """Interpret coverage analysis results."""
        interpretation = {
            'summary': f"Coverage analysis across {len(coverage_cols)} intervention types",
            'coverage_status': {},
            'gap_analysis': {},
            'optimization_opportunities': []
        }
        
        for col in coverage_cols:
            coverage_data = df[col].dropna()
            
            # Coverage status
            mean_coverage = coverage_data.mean()
            below_60 = (coverage_data < 60).sum()
            above_80 = (coverage_data >= 80).sum()
            
            interpretation['coverage_status'][col] = {
                'average': f"{mean_coverage:.1f}%",
                'gaps': f"{below_60} wards below 60%",
                'well_covered': f"{above_80} wards above 80%"
            }
            
            # Gap analysis
            if below_60 > len(df) * 0.3:
                interpretation['gap_analysis'][col] = "Significant gaps requiring immediate attention"
            elif below_60 > len(df) * 0.1:
                interpretation['gap_analysis'][col] = "Moderate gaps with clear improvement targets"
            else:
                interpretation['gap_analysis'][col] = "Minor gaps with maintenance focus"
        
        # Optimization opportunities
        if len(coverage_cols) > 1:
            interpretation['optimization_opportunities'].append("Compare intervention types to identify most cost-effective approaches")
        
        return interpretation
    
    def _interpret_program_implications(self, df: pd.DataFrame, analysis_state: Dict[str, Any]) -> Dict[str, str]:
        """Interpret overall program implications."""
        implications = {}
        
        # Resource allocation implications
        if analysis_state['risk_analysis_possible'] and analysis_state['coverage_analysis_possible']:
            implications['resource_allocation'] = "Data supports evidence-based resource allocation using risk-coverage gap analysis"
        elif analysis_state['risk_analysis_possible']:
            implications['resource_allocation'] = "Risk data enables burden-based prioritization, but coverage data needed for gap analysis"
        else:
            implications['resource_allocation'] = "Limited data for evidence-based allocation - consider collecting risk/burden indicators"
        
        # Targeting strategy implications
        dataset_size = len(df)
        if dataset_size < 100:
            implications['targeting_strategy'] = "Small-scale area: Intensive, ward-by-ward targeting feasible"
        elif dataset_size < 500:
            implications['targeting_strategy'] = "Medium-scale area: Cluster-based or risk-stratum targeting recommended"
        else:
            implications['targeting_strategy'] = "Large-scale area: Statistical targeting and sampling approaches needed"
        
        # Monitoring implications
        if analysis_state['spatial_analysis_possible']:
            implications['monitoring'] = "Spatial data enables geographic monitoring and transmission pathway analysis"
        else:
            implications['monitoring'] = "Ward-level monitoring possible, consider adding GPS coordinates for spatial analysis"
        
        return implications
    
    def _translate_to_actions(self, interpretations: Dict[str, Any], analysis_state: Dict[str, Any]) -> Dict[str, List[str]]:
        """Translate interpretations into concrete actions."""
        actions = {
            'immediate_actions': [],
            'strategic_actions': [],
            'data_collection_actions': [],
            'analysis_actions': []
        }
        
        # Risk-based actions
        if 'risk_analysis' in interpretations:
            risk_interp = interpretations['risk_analysis']
            if len(risk_interp['priority_areas']) > 0:
                actions['immediate_actions'].append(f"Deploy rapid response teams to {len(risk_interp['priority_areas'])} highest-risk wards")
            
            if "Concentrated risk pattern" in str(risk_interp['implications']):
                actions['strategic_actions'].append("Implement hotspot targeting strategy with 70-80% resource concentration")
            else:
                actions['strategic_actions'].append("Implement broad-coverage strategy with risk-weighted resource allocation")
        
        # Coverage-based actions
        if 'coverage_analysis' in interpretations:
            coverage_interp = interpretations['coverage_analysis']
            for intervention, status in coverage_interp['coverage_status'].items():
                if "below 60%" in status['gaps'] and int(status['gaps'].split()[0]) > 10:
                    actions['immediate_actions'].append(f"Launch {intervention} catch-up campaign in wards with <60% coverage")
        
        # Data improvement actions
        if not analysis_state['risk_analysis_possible']:
            actions['data_collection_actions'].append("Collect malaria burden/risk indicators for evidence-based targeting")
        
        if not analysis_state['coverage_analysis_possible']:
            actions['data_collection_actions'].append("Conduct intervention coverage surveys to identify gaps")
        
        # Additional analysis actions
        actions['analysis_actions'].append("Run scenario simulations to model intervention scale-up options")
        actions['analysis_actions'].append("Use targeting tools to identify optimal resource allocation")
        
        return actions
    
    def _create_decision_support(self, interpretations: Dict[str, Any], actions: Dict[str, List[str]]) -> Dict[str, Any]:
        """Create decision support materials."""
        support = {
            'executive_summary': self._create_executive_summary(interpretations),
            'decision_matrix': self._create_decision_matrix(actions),
            'risk_assessment': self._create_risk_assessment(interpretations)
        }
        
        return support
    
    def _create_executive_summary(self, interpretations: Dict[str, Any]) -> str:
        """Create executive summary of findings."""
        summary_points = []
        
        if 'risk_analysis' in interpretations:
            risk_findings = interpretations['risk_analysis']['key_findings']
            if risk_findings:
                summary_points.append(f"Risk Analysis: {risk_findings[0]}")
        
        if 'coverage_analysis' in interpretations:
            coverage_gaps = interpretations['coverage_analysis']['gap_analysis']
            significant_gaps = [k for k, v in coverage_gaps.items() if "Significant" in v]
            if significant_gaps:
                summary_points.append(f"Coverage Gaps: {len(significant_gaps)} intervention types need immediate attention")
        
        return " | ".join(summary_points) if summary_points else "Analysis complete - data patterns identified"
    
    def _create_decision_matrix(self, actions: Dict[str, List[str]]) -> Dict[str, str]:
        """Create decision matrix for prioritizing actions."""
        matrix = {}
        
        if actions['immediate_actions']:
            matrix['high_impact_immediate'] = actions['immediate_actions'][0] if actions['immediate_actions'] else "None identified"
        
        if actions['strategic_actions']:
            matrix['high_impact_strategic'] = actions['strategic_actions'][0] if actions['strategic_actions'] else "None identified"
        
        return matrix
    
    def _create_risk_assessment(self, interpretations: Dict[str, Any]) -> List[str]:
        """Create risk assessment for decision making."""
        risks = []
        
        if 'risk_analysis' in interpretations:
            priority_areas = interpretations['risk_analysis']['priority_areas']
            if len(priority_areas) > 5:
                risks.append("High-risk ward count may strain intervention capacity")
        
        risks.append("Data quality changes over time may affect analysis validity")
        risks.append("Local implementation capacity may limit intervention speed")
        
        return risks
    
    def _create_communication_templates(self, interpretations: Dict[str, Any]) -> Dict[str, str]:
        """Create communication templates for different audiences."""
        templates = {}
        
        # Policy maker template
        templates['policy_brief'] = """
Based on analysis of ward-level malaria data:
- Priority areas identified for immediate intervention
- Resource allocation gaps quantified  
- Evidence-based targeting strategy recommended
Next Steps: Implement data-driven resource reallocation
"""
        
        # Program manager template  
        templates['program_update'] = """
Malaria Program Analysis Update:
- Risk pattern analysis complete
- Coverage gap assessment finalized
- Targeting recommendations ready for implementation
Action Required: Review priority ward list and allocate resources
"""
        
        return templates
    
    def _recommend_next_analyses(self, analysis_state: Dict[str, Any]) -> List[str]:
        """Recommend next analyses to perform."""
        recommendations = []
        
        if analysis_state['risk_analysis_possible'] and analysis_state['coverage_analysis_possible']:
            recommendations.append("Run intervention targeting analysis to identify optimal resource allocation")
            recommendations.append("Use scenario simulation to model different coverage increase strategies")
        
        if analysis_state['spatial_analysis_possible']:
            recommendations.append("Perform spatial analysis to identify transmission corridors")
        
        recommendations.append("Create visualization maps for stakeholder communication")
        recommendations.append("Generate comprehensive analysis report for program planning")
        
        return recommendations


class GetDataDrivenInsights(BaseTool):
    """
    Extract hidden patterns and insights from the user's specific dataset.
    
    Uses advanced analytics to uncover relationships, trends, and patterns
    that may not be immediately obvious but are valuable for decision-making.
    """
    
    insight_focus: str = Field(
        "comprehensive",
        description="Insight focus: 'comprehensive', 'correlations', 'outliers', 'clusters', 'trends'",
        pattern="^(comprehensive|correlations|outliers|clusters|trends)$"
    )
    
    insight_depth: str = Field(
        "actionable",
        description="Depth of analysis: 'surface', 'detailed', 'actionable', 'research_grade'",
        pattern="^(surface|detailed|actionable|research_grade)$"
    )
    
    surprise_factor: bool = Field(
        True,
        description="Include unexpected/surprising findings that challenge assumptions"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.GENERAL_KNOWLEDGE
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "What hidden patterns exist in my malaria data?",
            "Find unexpected correlations and relationships in my dataset",
            "Identify outlier wards that don't fit normal patterns",
            "Discover insights I might have missed in my analysis"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Extract data-driven insights from user's dataset."""
        try:
            # Get dataset
            df = get_session_unified_dataset(session_id)
            if df is None:
                return self._create_error_result("No data available for analysis")
            
            # Extract insights based on focus
            insights = self._extract_comprehensive_insights(df)
            
            # Identify surprising patterns
            surprises = self._find_surprising_patterns(df) if self.surprise_factor else {}
            
            # Generate actionable intelligence
            intelligence = self._generate_actionable_intelligence(df, insights, surprises)
            
            message = f"Extracted {len(insights['key_insights'])} insights and {len(surprises.get('unexpected_patterns', []))} surprising patterns from your {len(df)}-ward dataset."
            
            return self._create_success_result(
                message=message,
                data={
                    'key_insights': insights,
                    'surprising_patterns': surprises,
                    'actionable_intelligence': intelligence,
                    'data_story': self._create_data_story(df, insights, surprises),
                    'investigation_opportunities': self._identify_investigation_opportunities(df, insights)
                }
            )
            
        except Exception as e:
            logger.error(f"Error extracting insights: {e}")
            return self._create_error_result(f"Insight extraction failed: {str(e)}")
    
    def _extract_comprehensive_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract comprehensive insights from the dataset."""
        insights = {
            'key_insights': [],
            'correlation_insights': [],
            'distribution_insights': [],
            'outlier_insights': [],
            'pattern_insights': []
        }
        
        # Correlation insights
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr()
            strong_correlations = self._find_strong_correlations(corr_matrix)
            insights['correlation_insights'] = strong_correlations
            
            for corr in strong_correlations[:3]:  # Top 3
                insights['key_insights'].append(f"Strong {corr['relationship']} between {corr['var1']} and {corr['var2']} (r={corr['correlation']:.2f})")
        
        # Distribution insights
        for col in numeric_cols[:5]:  # Top 5 numeric columns
            dist_insight = self._analyze_distribution(df[col], col)
            if dist_insight:
                insights['distribution_insights'].append(dist_insight)
                insights['key_insights'].append(dist_insight['summary'])
        
        # Outlier insights
        outlier_insights = self._find_outlier_insights(df)
        insights['outlier_insights'] = outlier_insights
        if outlier_insights:
            insights['key_insights'].append(f"Identified {len(outlier_insights)} wards with exceptional characteristics")
        
        return insights
    
    def _find_strong_correlations(self, corr_matrix: pd.DataFrame) -> List[Dict[str, Any]]:
        """Find strong correlations in the data."""
        strong_corrs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                if abs(corr_val) > 0.6 and not pd.isna(corr_val):  # Strong correlation
                    relationship = "positive correlation" if corr_val > 0 else "negative correlation"
                    strong_corrs.append({
                        'var1': corr_matrix.columns[i],
                        'var2': corr_matrix.columns[j],
                        'correlation': corr_val,
                        'relationship': relationship,
                        'strength': 'very strong' if abs(corr_val) > 0.8 else 'strong'
                    })
        
        # Sort by absolute correlation strength
        return sorted(strong_corrs, key=lambda x: abs(x['correlation']), reverse=True)
    
    def _analyze_distribution(self, data: pd.Series, col_name: str) -> Optional[Dict[str, Any]]:
        """Analyze distribution characteristics of a variable."""
        if data.dropna().empty:
            return None
        
        clean_data = data.dropna()
        skewness = clean_data.skew()
        kurtosis = clean_data.kurtosis()
        
        insight = {
            'variable': col_name,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'summary': ''
        }
        
        # Generate summary
        if abs(skewness) > 1:
            skew_desc = "highly right-skewed" if skewness > 1 else "highly left-skewed"
            insight['summary'] = f"{col_name} shows {skew_desc} distribution"
        elif abs(skewness) > 0.5:
            skew_desc = "moderately right-skewed" if skewness > 0.5 else "moderately left-skewed"
            insight['summary'] = f"{col_name} shows {skew_desc} distribution"
        else:
            insight['summary'] = f"{col_name} shows approximately normal distribution"
        
        # Add kurtosis info for extreme cases
        if kurtosis > 3:
            insight['summary'] += " with heavy tails (outlier-prone)"
        elif kurtosis < -1:
            insight['summary'] += " with light tails (uniform-like)"
        
        return insight
    
    def _find_outlier_insights(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Find and characterize outlier wards."""
        outliers = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols[:5]:  # Check top 5 numeric columns
            data = df[col].dropna()
            if len(data) < 10:  # Skip if too few data points
                continue
                
            # Use IQR method to find outliers
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_mask = (data < lower_bound) | (data > upper_bound)
            outlier_wards = df.loc[outlier_mask]
            
            for idx, row in outlier_wards.head(3).iterrows():  # Top 3 outliers per variable
                ward_name = row.get('WardName', f'Ward_{idx}')
                outlier_value = row[col]
                
                outliers.append({
                    'ward_name': ward_name,
                    'variable': col,
                    'value': outlier_value,
                    'type': 'high_outlier' if outlier_value > upper_bound else 'low_outlier',
                    'deviation': f"{abs(outlier_value - data.median()) / data.std():.1f} standard deviations from median"
                })
        
        return outliers[:10]  # Return top 10 most extreme outliers
    
    def _find_surprising_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Find surprising or counter-intuitive patterns."""
        surprises = {
            'unexpected_patterns': [],
            'assumption_challenges': [],
            'anomalies': []
        }
        
        # Look for counter-intuitive correlations
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr()
            
            # Find unexpected negative correlations
            unexpected_negs = self._find_unexpected_negative_correlations(corr_matrix)
            surprises['unexpected_patterns'].extend(unexpected_negs)
        
        # Look for geographic anomalies (if geographic data available)
        geo_anomalies = self._find_geographic_anomalies(df)
        surprises['anomalies'].extend(geo_anomalies)
        
        # Look for assumption challenges
        assumption_challenges = self._find_assumption_challenges(df)
        surprises['assumption_challenges'].extend(assumption_challenges)
        
        return surprises
    
    def _find_unexpected_negative_correlations(self, corr_matrix: pd.DataFrame) -> List[str]:
        """Find unexpected negative correlations that challenge assumptions."""
        unexpected = []
        
        # Define expected positive relationships
        expected_positive = [
            ('risk', 'pfpr'), ('risk', 'burden'), ('elevation', 'temp'),
            ('population', 'urban'), ('coverage', 'protection')
        ]
        
        for var1_pattern, var2_pattern in expected_positive:
            var1_cols = [col for col in corr_matrix.columns if var1_pattern in col.lower()]
            var2_cols = [col for col in corr_matrix.columns if var2_pattern in col.lower()]
            
            for var1 in var1_cols:
                for var2 in var2_cols:
                    if var1 != var2 and var1 in corr_matrix.columns and var2 in corr_matrix.columns:
                        corr_val = corr_matrix.loc[var1, var2]
                        if corr_val < -0.3:  # Unexpected negative correlation
                            unexpected.append(f"Surprising: {var1} and {var2} are negatively correlated (r={corr_val:.2f}) - investigate data quality or local factors")
        
        return unexpected
    
    def _find_geographic_anomalies(self, df: pd.DataFrame) -> List[str]:
        """Find geographic anomalies in the data."""
        anomalies = []
        
        # Check urban/rural patterns
        urban_cols = [col for col in df.columns if 'urban' in col.lower()]
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score', 'pfpr'])]
        
        if urban_cols and risk_cols:
            urban_col = urban_cols[0]
            risk_col = risk_cols[0]
            
            # Check if urban areas have higher or lower risk (surprising either way)
            if df[urban_col].dtype in ['bool', 'object']:
                urban_risk = df[df[urban_col] == True][risk_col].mean() if df[urban_col].dtype == 'bool' else df[df[urban_col].str.contains('urban', case=False, na=False)][risk_col].mean()
                rural_risk = df[df[urban_col] == False][risk_col].mean() if df[urban_col].dtype == 'bool' else df[~df[urban_col].str.contains('urban', case=False, na=False)][risk_col].mean()
                
                if urban_risk > rural_risk * 1.2:
                    anomalies.append(f"Unexpected: Urban areas show {(urban_risk/rural_risk-1)*100:.0f}% higher malaria risk than rural areas")
                elif rural_risk > urban_risk * 1.2:
                    anomalies.append(f"Expected: Rural areas show {(rural_risk/urban_risk-1)*100:.0f}% higher malaria risk than urban areas")
        
        return anomalies
    
    def _find_assumption_challenges(self, df: pd.DataFrame) -> List[str]:
        """Find patterns that challenge common assumptions."""
        challenges = []
        
        # Challenge: All high-risk areas have low coverage
        risk_cols = [col for col in df.columns if any(term in col.lower() for term in ['risk', 'score'])]
        coverage_cols = [col for col in df.columns if 'coverage' in col.lower()]
        
        if risk_cols and coverage_cols:
            risk_col = risk_cols[0]
            coverage_col = coverage_cols[0]
            
            high_risk_wards = df[df[risk_col] >= df[risk_col].quantile(0.8)]
            if len(high_risk_wards) > 0:
                high_risk_high_coverage = high_risk_wards[high_risk_wards[coverage_col] >= 70]
                if len(high_risk_high_coverage) > len(high_risk_wards) * 0.3:
                    challenges.append(f"Assumption challenge: {len(high_risk_high_coverage)} high-risk wards actually have good coverage (>70%) - investigate targeting efficiency")
        
        return challenges
    
    def _generate_actionable_intelligence(self, df: pd.DataFrame, insights: Dict[str, Any], surprises: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate actionable intelligence from insights."""
        intelligence = {
            'immediate_investigations': [],
            'strategic_opportunities': [],
            'data_quality_flags': [],
            'hypothesis_testing': []
        }
        
        # Investigations based on outliers
        if insights['outlier_insights']:
            outlier_wards = set(o['ward_name'] for o in insights['outlier_insights'])
            if len(outlier_wards) > 0:
                intelligence['immediate_investigations'].append(f"Investigate {len(outlier_wards)} outlier wards: {', '.join(list(outlier_wards)[:3])}...")
        
        # Strategic opportunities from correlations
        for corr in insights['correlation_insights'][:2]:
            if 'coverage' in corr['var1'].lower() or 'coverage' in corr['var2'].lower():
                intelligence['strategic_opportunities'].append(f"Leverage {corr['relationship']} between {corr['var1']} and {corr['var2']} for targeted coverage improvement")
        
        # Data quality flags from surprises
        unexpected_patterns = surprises.get('unexpected_patterns', [])
        if unexpected_patterns:
            intelligence['data_quality_flags'].extend(unexpected_patterns[:2])
        
        # Hypothesis testing opportunities
        intelligence['hypothesis_testing'].append("Test effectiveness of risk-based vs. coverage-based targeting strategies")
        if insights['correlation_insights']:
            strongest_corr = insights['correlation_insights'][0]
            intelligence['hypothesis_testing'].append(f"Test causal relationship between {strongest_corr['var1']} and {strongest_corr['var2']}")
        
        return intelligence
    
    def _create_data_story(self, df: pd.DataFrame, insights: Dict[str, Any], surprises: Dict[str, Any]) -> str:
        """Create a narrative data story from insights."""
        story_parts = []
        
        # Opening
        story_parts.append(f"Analysis of {len(df)} wards reveals a complex malaria landscape.")
        
        # Key findings
        if insights['key_insights']:
            story_parts.append(f"The data shows {insights['key_insights'][0].lower()}.")
            if len(insights['key_insights']) > 1:
                story_parts.append(f"Additionally, {insights['key_insights'][1].lower()}.")
        
        # Surprising findings
        unexpected = surprises.get('unexpected_patterns', [])
        if unexpected:
            story_parts.append(f"Surprisingly, the analysis uncovered {unexpected[0].lower()}.")
        
        # Conclusion
        outlier_count = len(insights.get('outlier_insights', []))
        if outlier_count > 0:
            story_parts.append(f"The presence of {outlier_count} outlier wards suggests unique local factors requiring individual attention.")
        
        story_parts.append("These insights provide a foundation for evidence-based intervention planning.")
        
        return " ".join(story_parts)
    
    def _identify_investigation_opportunities(self, df: pd.DataFrame, insights: Dict[str, Any]) -> List[str]:
        """Identify opportunities for deeper investigation."""
        opportunities = []
        
        # Strong correlations warrant investigation
        strong_corrs = [c for c in insights['correlation_insights'] if abs(c['correlation']) > 0.7]
        for corr in strong_corrs[:2]:
            opportunities.append(f"Investigate causal mechanism behind {corr['var1']}-{corr['var2']} correlation")
        
        # Outliers warrant field investigation
        outlier_wards = set(o['ward_name'] for o in insights.get('outlier_insights', []))
        if len(outlier_wards) >= 3:
            opportunities.append(f"Conduct field visits to outlier wards: {', '.join(list(outlier_wards)[:3])}")
        
        # Distribution patterns suggest mechanisms
        dist_insights = insights.get('distribution_insights', [])
        skewed_vars = [d for d in dist_insights if 'skewed' in d['summary']]
        if skewed_vars:
            opportunities.append(f"Investigate factors causing skewed distribution in {skewed_vars[0]['variable']}")
        
        return opportunities