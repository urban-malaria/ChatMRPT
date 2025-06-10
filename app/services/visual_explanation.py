"""
Visual Explanation Service for ChatMRPT - Malaria Epidemiology Focus

This service provides AI-powered explanations of visualizations without exposing raw data.
Explanations are tailored to malaria epidemiology with dynamic geographic context detection.
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from flask import current_app
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class VisualizationContext:
    """Context data for visualization explanation without exposing raw values."""
    chart_type: str
    variable: str
    group_by: Optional[str] = None
    summary_stats: Optional[Dict[str, Any]] = None
    data_insights: Optional[Dict[str, Any]] = None
    epidemiological_context: Optional[Dict[str, Any]] = None
    geographic_context: Optional[Dict[str, Any]] = None


class VisualExplanationService:
    """
    Service for generating epidemiologically-informed explanations of visualizations.
    
    Key Features:
    - No raw data sent to LLM
    - Malaria epidemiology expertise
    - Dynamic geographic context detection
    - Educational and actionable insights
    - Scalable to any malaria surveillance dataset
    """
    
    def __init__(self, llm_manager=None):
        self.llm_manager = llm_manager
        
        # Core malaria variables knowledge base (always present)
        self.core_variable_contexts = {
            'tpr': {
                'full_name': 'Test Positivity Rate',
                'meaning': 'Percentage of malaria tests that return positive',
                'epidemiological_significance': 'Direct indicator of malaria transmission intensity',
                'interpretation_thresholds': {
                    'low': 5, 'medium': 15, 'high': 25, 'very_high': 40
                },
                'intervention_implications': {
                    'low': 'Maintenance phase - focus on surveillance',
                    'medium': 'Control phase - targeted interventions',
                    'high': 'Attack phase - intensive interventions needed',
                    'very_high': 'Emergency response - immediate action required'
                }
            },
            'composite_score': {
                'full_name': 'Composite Risk Score',
                'meaning': 'Multi-factor malaria risk assessment combining health, environmental, and demographic factors',
                'epidemiological_significance': 'Comprehensive measure for intervention prioritization',
                'usage': 'Higher scores indicate areas requiring urgent attention'
            },
            'composite_vulnerability_score': {
                'full_name': 'Composite Vulnerability Score',
                'meaning': 'Overall vulnerability assessment combining multiple risk factors',
                'epidemiological_significance': 'Comprehensive vulnerability measure for resource allocation',
                'usage': 'Higher scores indicate more vulnerable populations'
            },
            'pca_vulnerability_score': {
                'full_name': 'PCA Vulnerability Score',
                'meaning': 'Principal Component Analysis-based vulnerability assessment',
                'epidemiological_significance': 'Advanced statistical measure identifying key risk patterns',
                'usage': 'Identifies areas with similar risk profiles for targeted interventions'
            },
            'vulnerability_ranking': {
                'full_name': 'Vulnerability Ranking',
                'meaning': 'Relative ranking of areas by vulnerability level',
                'epidemiological_significance': 'Prioritization tool for intervention planning',
                'usage': 'Lower ranks indicate higher priority for interventions'
            },
            'pca_vulnerability_ranking': {
                'full_name': 'PCA Vulnerability Ranking',
                'meaning': 'Statistical ranking based on PCA analysis',
                'epidemiological_significance': 'Data-driven prioritization based on complex patterns',
                'usage': 'Advanced ranking system for resource allocation'
            }
        }
        
        # Common additional variables (may vary by dataset)
        self.common_variable_contexts = {
            'population': {
                'full_name': 'Population Count',
                'meaning': 'Number of people residing in the area',
                'epidemiological_significance': 'Determines intervention scale and resource requirements'
            },
            'settlement_type': {
                'full_name': 'Settlement Type',
                'meaning': 'Classification of area as Urban or Rural',
                'epidemiological_significance': 'Urban and rural areas have different malaria risk profiles',
                'key_differences': {
                    'urban': 'Lower transmission, better healthcare access, more breeding site control',
                    'rural': 'Higher transmission, limited healthcare, natural breeding sites'
                }
            },
            'ndvi': {
                'full_name': 'Normalized Difference Vegetation Index',
                'meaning': 'Measure of vegetation density and health',
                'epidemiological_significance': 'Vegetation creates mosquito breeding habitats'
            },
            'elevation': {
                'full_name': 'Elevation',
                'meaning': 'Height above sea level',
                'epidemiological_significance': 'Affects temperature and humidity, influencing mosquito survival'
            },
            'temperature': {
                'full_name': 'Temperature',
                'meaning': 'Average temperature measurements',
                'epidemiological_significance': 'Critical factor in mosquito development and malaria transmission'
            },
            'rainfall': {
                'full_name': 'Rainfall',
                'meaning': 'Precipitation measurements',
                'epidemiological_significance': 'Creates breeding sites for mosquitoes'
            }
        }
        
        # Combine all variable contexts
        self.variable_contexts = {**self.core_variable_contexts, **self.common_variable_contexts}
    
    def explain_visualization(self, session_id: str, viz_result: Dict[str, Any], 
                            user_question: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate comprehensive explanation of a visualization.
        
        Args:
            session_id: Session identifier
            viz_result: Result from visualization tool
            user_question: Optional specific question about the visualization
            
        Returns:
            Dictionary with explanation and educational insights
        """
        try:
            # Extract context from visualization result
            context = self._build_visualization_context(session_id, viz_result)
            
            # Generate epidemiologically-informed explanation
            explanation = self._generate_explanation(context, user_question)
            
            return {
                'status': 'success',
                'explanation': explanation,
                'chart_type': context.chart_type,
                'variable': context.variable,
                'educational_insights': self._get_educational_insights(context),
                'intervention_recommendations': self._get_intervention_recommendations(context)
            }
            
        except Exception as e:
            logger.error(f"Error generating visual explanation: {e}")
            return {
                'status': 'error',
                'message': f'Error generating explanation: {str(e)}'
            }
    
    def _build_visualization_context(self, session_id: str, viz_result: Dict[str, Any]) -> VisualizationContext:
        """Build context for explanation without exposing raw data."""
        chart_type = viz_result.get('chart_type', 'unknown')
        variable = viz_result.get('variable', 'unknown')
        group_by = viz_result.get('group_by') or viz_result.get('grouping')
        
        # Get geographic context dynamically first (needed for other functions)
        geographic_context = self._detect_geographic_context(session_id)
        
        # Get summary statistics safely
        summary_stats = self._calculate_safe_summary_stats(session_id, variable, group_by)
        
        # Get data insights  
        data_insights = self._extract_data_insights(session_id, variable, group_by, geographic_context)
        
        # Get epidemiological context
        epidemiological_context = self._get_epidemiological_context(variable, group_by)
        
        return VisualizationContext(
            chart_type=chart_type,
            variable=variable,
            group_by=group_by,
            summary_stats=summary_stats,
            data_insights=data_insights,
            epidemiological_context=epidemiological_context,
            geographic_context=geographic_context
        )
    
    def _detect_geographic_context(self, session_id: str) -> Dict[str, Any]:
        """Dynamically detect geographic context from the data."""
        try:
            from ..data.unified_dataset_builder import load_unified_dataset
            df = load_unified_dataset(session_id)
            
            if df is None:
                return {'location': 'Unknown', 'type': 'malaria surveillance area'}
            
            context = {'type': 'malaria surveillance area'}
            
            # Try to detect location from common column names
            location_columns = ['state', 'statename', 'state_name', 'region', 'country', 'province']
            ward_columns = ['ward', 'wardname', 'ward_name', 'district', 'lga', 'local_government']
            
            # Check for state/region information
            for col in location_columns:
                if col.lower() in [c.lower() for c in df.columns]:
                    actual_col = next(c for c in df.columns if c.lower() == col.lower())
                    if not df[actual_col].isna().all():
                        unique_locations = df[actual_col].dropna().unique()
                        if len(unique_locations) > 0:
                            main_location = unique_locations[0]
                            context['location'] = str(main_location)
                            context['location_type'] = 'state' if 'state' in col.lower() else 'region'
                            break
            
            # Check for ward/district level information
            for col in ward_columns:
                if col.lower() in [c.lower() for c in df.columns]:
                    actual_col = next(c for c in df.columns if c.lower() == col.lower())
                    ward_count = df[actual_col].nunique()
                    context['administrative_units'] = ward_count
                    context['unit_type'] = 'wards' if 'ward' in col.lower() else 'districts'
                    break
            
            # Detect scale
            total_records = len(df)
            if total_records < 50:
                context['scale'] = 'local'
            elif total_records < 200:
                context['scale'] = 'regional'
            else:
                context['scale'] = 'state/provincial'
            
            # Set default location if not detected
            if 'location' not in context:
                context['location'] = 'the study area'
                context['location_type'] = 'region'
            
            return context
            
        except Exception as e:
            logger.error(f"Error detecting geographic context: {e}")
            return {
                'location': 'the study area',
                'type': 'malaria surveillance area',
                'scale': 'regional'
            }
    
    def _calculate_safe_summary_stats(self, session_id: str, variable: str, 
                                    group_by: Optional[str] = None) -> Dict[str, Any]:
        """Calculate summary statistics without exposing individual values."""
        try:
            from ..data.unified_dataset_builder import load_unified_dataset
            df = load_unified_dataset(session_id)
            
            if df is None or variable not in df.columns:
                return {}
            
            stats = {}
            
            if pd.api.types.is_numeric_dtype(df[variable]):
                # Numeric variable statistics
                stats = {
                    'data_type': 'numeric',
                    'count': int(df[variable].count()),
                    'mean': float(df[variable].mean()),
                    'median': float(df[variable].median()),
                    'std': float(df[variable].std()),
                    'min': float(df[variable].min()),
                    'max': float(df[variable].max()),
                    'quartiles': {
                        'q1': float(df[variable].quantile(0.25)),
                        'q3': float(df[variable].quantile(0.75))
                    }
                }
                
                # Group statistics if grouping variable provided
                if group_by and group_by in df.columns:
                    group_stats = df.groupby(group_by)[variable].agg(['count', 'mean', 'median', 'std']).to_dict()
                    stats['group_statistics'] = group_stats
                    
            else:
                # Categorical variable statistics
                value_counts = df[variable].value_counts().to_dict()
                stats = {
                    'data_type': 'categorical',
                    'count': int(df[variable].count()),
                    'unique_values': int(df[variable].nunique()),
                    'value_distribution': value_counts
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating summary stats: {e}")
            return {}
    
    def _extract_data_insights(self, session_id: str, variable: str, 
                             group_by: Optional[str] = None,
                             geographic_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract key insights about the data patterns."""
        try:
            from ..data.unified_dataset_builder import load_unified_dataset
            df = load_unified_dataset(session_id)
            
            if df is None or variable not in df.columns:
                return {}
            
            insights = {}
            
            if pd.api.types.is_numeric_dtype(df[variable]):
                # Identify patterns in numeric data
                insights['distribution_shape'] = self._assess_distribution_shape(df[variable])
                insights['outliers_present'] = self._detect_outliers(df[variable])
                
                if group_by and group_by in df.columns:
                    insights['group_differences'] = self._assess_group_differences(df, variable, group_by)
            
            # Geographic context insights (detected dynamically)
            if geographic_context:
                insights['geographic_context'] = self._get_geographic_context_description(
                    geographic_context, variable
                )
            
            return insights
            
        except Exception as e:
            logger.error(f"Error extracting data insights: {e}")
            return {}
    
    def _assess_distribution_shape(self, series: pd.Series) -> str:
        """Assess the shape of the distribution."""
        skewness = series.skew()
        
        if abs(skewness) < 0.5:
            return 'approximately_normal'
        elif skewness > 0.5:
            return 'right_skewed'
        else:
            return 'left_skewed'
    
    def _detect_outliers(self, series: pd.Series) -> Dict[str, Any]:
        """Detect presence of outliers using IQR method."""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        
        return {
            'has_outliers': len(outliers) > 0,
            'outlier_count': len(outliers),
            'percentage': (len(outliers) / len(series)) * 100
        }
    
    def _assess_group_differences(self, df: pd.DataFrame, variable: str, group_by: str) -> Dict[str, Any]:
        """Assess differences between groups."""
        group_means = df.groupby(group_by)[variable].mean()
        
        # Calculate coefficient of variation between groups
        group_std = group_means.std()
        group_mean = group_means.mean()
        coefficient_of_variation = (group_std / group_mean) * 100 if group_mean != 0 else 0
        
        return {
            'variation_between_groups': 'high' if coefficient_of_variation > 20 else 'low',
            'coefficient_of_variation': coefficient_of_variation,
            'highest_group': group_means.idxmax(),
            'lowest_group': group_means.idxmin()
        }
    
    def _get_epidemiological_context(self, variable: str, group_by: Optional[str] = None) -> Dict[str, Any]:
        """Get epidemiological context for the variables."""
        context = {}
        
        if variable in self.variable_contexts:
            context['primary_variable'] = self.variable_contexts[variable]
        
        if group_by and group_by in self.variable_contexts:
            context['grouping_variable'] = self.variable_contexts[group_by]
        
        # Add specific context for common combinations
        if variable == 'tpr' and group_by == 'settlement_type':
            context['analysis_significance'] = (
                "Comparing TPR between urban and rural areas reveals key epidemiological patterns. "
                "Rural areas typically show higher transmission due to environmental factors, "
                "while urban areas may have better control but potential for rapid outbreaks."
            )
        
        return context
    
    def _get_geographic_context_description(self, geographic_context: Dict[str, Any], variable: str) -> str:
        """Get context description for the specific geographic area."""
        location = geographic_context.get('location', 'the study area')
        scale = geographic_context.get('scale', 'regional')
        unit_type = geographic_context.get('unit_type', 'areas')
        
        # Generic contextual descriptions that work for any location
        generic_contexts = {
            'tpr': f"{location} has diverse ecological zones that may affect malaria transmission patterns",
            'population': f"{location} includes a mix of urban and rural communities with varying population densities",
            'settlement_type': f"{location} encompasses both urban centers and rural areas with different transmission profiles",
            'composite_score': f"Risk assessment across {location} incorporates multiple epidemiological factors",
            'elevation': f"Elevation varies across {location}, affecting temperature and humidity patterns",
            'ndvi': f"Vegetation patterns in {location} create varying mosquito habitat suitability"
        }
        
        context_desc = generic_contexts.get(variable, f"{location} shows spatial variation in malaria risk factors")
        
        # Add scale information
        if scale == 'state/provincial':
            context_desc += f" across multiple {unit_type}"
        elif scale == 'regional':
            context_desc += f" within the regional {unit_type}"
        
        return context_desc
    
    def _generate_explanation(self, context: VisualizationContext, 
                            user_question: Optional[str] = None) -> str:
        """Generate LLM-powered explanation using context only."""
        if not self.llm_manager:
            return self._generate_fallback_explanation(context)
        
        # Build comprehensive prompt
        explanation_prompt = self._build_explanation_prompt(context, user_question)
        
        try:
            explanation = self.llm_manager.generate_response(
                prompt=explanation_prompt,
                system_message=self._get_explanation_system_prompt(),
                temperature=0.3,  # Lower temperature for more consistent explanations
                max_tokens=800
            )
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error generating LLM explanation: {e}")
            return self._generate_fallback_explanation(context)
    
    def _build_explanation_prompt(self, context: VisualizationContext, 
                                user_question: Optional[str] = None) -> str:
        """Build comprehensive prompt for explanation generation."""
        # Get geographic context for dynamic location reference
        location = context.geographic_context.get('location', 'the study area') if context.geographic_context else 'the study area'
        
        prompt = f"""
Explain this {context.chart_type} visualization for malaria analysis in {location}.

VISUALIZATION DETAILS:
- Chart Type: {context.chart_type}
- Primary Variable: {context.variable}
- Grouping Variable: {context.group_by or 'None'}

STATISTICAL SUMMARY:
{self._format_summary_stats(context.summary_stats)}

DATA INSIGHTS:
{self._format_data_insights(context.data_insights)}

EPIDEMIOLOGICAL CONTEXT:
{self._format_epidemiological_context(context.epidemiological_context)}

GEOGRAPHIC CONTEXT:
{self._format_geographic_context(context.geographic_context)}
"""
        
        if user_question:
            prompt += f"\nUSER'S SPECIFIC QUESTION: {user_question}\n"
        
        prompt += f"""
EXPLANATION REQUIREMENTS:
1. Explain what the visualization shows in epidemiological terms
2. Interpret the patterns and their public health significance
3. Provide actionable insights for malaria control in {location}
4. Include educational context about the variables
5. Suggest follow-up analyses or interventions if appropriate

Focus on practical implications for malaria control and intervention planning.
"""
        
        return prompt
    
    def _get_explanation_system_prompt(self) -> str:
        """Get system prompt for explanation generation."""
        return """
You are a senior malaria epidemiologist with expertise in surveillance data analysis. You provide clear, 
educational explanations of data visualizations that help public health officials make informed 
decisions about malaria control interventions.

Your explanations should:
- Use accessible language suitable for public health practitioners
- Connect data patterns to epidemiological principles
- Provide actionable insights for intervention planning
- Include educational context about malaria transmission
- Consider the specific geographic and demographic context of the study area
- Suggest practical next steps based on the findings
- Adapt recommendations to the scale and type of surveillance area

Always maintain a helpful, educational tone while being scientifically accurate and contextually appropriate.
"""
    
    def _format_summary_stats(self, stats: Optional[Dict[str, Any]]) -> str:
        """Format summary statistics for prompt."""
        if not stats:
            return "No statistical summary available."
        
        if stats.get('data_type') == 'numeric':
            return f"""
- Data points: {stats.get('count', 'unknown')}
- Mean: {stats.get('mean', 'unknown'):.2f}
- Median: {stats.get('median', 'unknown'):.2f}
- Range: {stats.get('min', 'unknown'):.2f} to {stats.get('max', 'unknown'):.2f}
- Standard deviation: {stats.get('std', 'unknown'):.2f}
"""
        else:
            return f"""
- Data points: {stats.get('count', 'unknown')}
- Unique categories: {stats.get('unique_values', 'unknown')}
- Category distribution: {stats.get('value_distribution', 'Not available')}
"""
    
    def _format_data_insights(self, insights: Optional[Dict[str, Any]]) -> str:
        """Format data insights for prompt."""
        if not insights:
            return "No specific data insights available."
        
        formatted = []
        
        if 'distribution_shape' in insights:
            formatted.append(f"Distribution shape: {insights['distribution_shape']}")
        
        if 'outliers_present' in insights:
            outlier_info = insights['outliers_present']
            if outlier_info.get('has_outliers'):
                formatted.append(f"Outliers detected: {outlier_info.get('outlier_count')} points ({outlier_info.get('percentage', 0):.1f}%)")
        
        if 'group_differences' in insights:
            group_info = insights['group_differences']
            formatted.append(f"Group variation: {group_info.get('variation_between_groups')}")
        
        return '\n'.join(formatted) if formatted else "No specific patterns detected."
    
    def _format_epidemiological_context(self, context: Optional[Dict[str, Any]]) -> str:
        """Format epidemiological context for prompt."""
        if not context:
            return "No specific epidemiological context available."
        
        formatted = []
        
        if 'primary_variable' in context:
            var_info = context['primary_variable']
            formatted.append(f"Variable meaning: {var_info.get('meaning', 'Not specified')}")
            formatted.append(f"Epidemiological significance: {var_info.get('epidemiological_significance', 'Not specified')}")
        
        if 'analysis_significance' in context:
            formatted.append(f"Analysis context: {context['analysis_significance']}")
        
        return '\n'.join(formatted) if formatted else "General malaria epidemiology context applies."
    
    def _format_geographic_context(self, context: Optional[Dict[str, Any]]) -> str:
        """Format geographic context for prompt."""
        if not context:
            return "No specific geographic context available."
        
        formatted = []
        
        location = context.get('location', 'Study area')
        location_type = context.get('location_type', 'region')
        scale = context.get('scale', 'regional')
        
        formatted.append(f"Study area: {location} ({location_type} level)")
        formatted.append(f"Analysis scale: {scale}")
        
        if 'administrative_units' in context:
            unit_type = context.get('unit_type', 'areas')
            unit_count = context['administrative_units']
            formatted.append(f"Coverage: {unit_count} {unit_type}")
        
        return '\n'.join(formatted)
    
    def _generate_fallback_explanation(self, context: VisualizationContext) -> str:
        """Generate basic explanation when LLM is not available."""
        variable_info = self.variable_contexts.get(context.variable, {})
        variable_name = variable_info.get('full_name', context.variable)
        
        # Get location dynamically
        location = context.geographic_context.get('location', 'the study area') if context.geographic_context else 'the study area'
        
        explanation = f"""
This {context.chart_type} shows the distribution of {variable_name} in {location}.
"""
        
        if context.group_by:
            explanation += f" The data is grouped by {context.group_by}."
        
        if variable_info.get('epidemiological_significance'):
            explanation += f" {variable_info['epidemiological_significance']}"
        
        explanation += f" This visualization helps identify patterns relevant to malaria control and intervention planning in {location}."
        
        return explanation
    
    def _get_educational_insights(self, context: VisualizationContext) -> List[str]:
        """Get educational insights about the variables and analysis."""
        insights = []
        
        variable_info = self.variable_contexts.get(context.variable, {})
        
        if variable_info:
            insights.append(f"📊 {variable_info.get('full_name', context.variable)}: {variable_info.get('meaning', 'Key malaria indicator')}")
        
        if context.group_by and context.group_by in self.variable_contexts:
            group_info = self.variable_contexts[context.group_by]
            insights.append(f"🏘️ {group_info.get('full_name', context.group_by)}: {group_info.get('meaning', 'Grouping factor')}")
        
        # Add chart type specific insights
        chart_insights = {
            'boxplot': "Box plots show the distribution and help identify outliers and differences between groups.",
            'histogram': "Histograms reveal the shape of the data distribution and help identify patterns.",
            'scatter_plot': "Scatter plots show relationships between variables and help identify correlations.",
            'bar_chart': "Bar charts compare values across categories and highlight differences.",
            'heatmap': "Heatmaps show correlations between multiple variables simultaneously."
        }
        
        if context.chart_type in chart_insights:
            insights.append(f"📈 {chart_insights[context.chart_type]}")
        
        return insights
    
    def _get_intervention_recommendations(self, context: VisualizationContext) -> List[str]:
        """Get intervention recommendations based on the analysis."""
        recommendations = []
        
        # Variable-specific recommendations
        if context.variable == 'tpr':
            recommendations.extend([
                "🎯 High TPR areas need immediate intervention with ITNs and IRS",
                "🔍 Low TPR areas require surveillance to maintain gains",
                "📋 Consider seasonal patterns when planning interventions"
            ])
        
        elif context.variable == 'composite_score':
            recommendations.extend([
                "📊 Use composite scores to prioritize intervention wards",
                "💰 Allocate resources based on risk ranking",
                "🔄 Regular monitoring to track intervention impact"
            ])
        
        # Group-specific recommendations
        if context.group_by == 'settlement_type':
            recommendations.extend([
                "🏙️ Urban areas: Focus on breeding site elimination",
                "🌾 Rural areas: Emphasize community case management",
                "🚌 Consider population movement between urban and rural areas"
            ])
        
        return recommendations


def get_visual_explanation_service(llm_manager=None) -> VisualExplanationService:
    """Factory function to get visual explanation service."""
    return VisualExplanationService(llm_manager=llm_manager) 