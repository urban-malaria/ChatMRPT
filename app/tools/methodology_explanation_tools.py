"""
Methodology Explanation Tools - Dynamic, Tailored Analysis Method Explanations

This module provides tools for explaining analysis methodologies in a dynamic,
data-driven way rather than generic template responses.
"""

import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Literal, Tuple
from pydantic import BaseModel, Field, validator
from datetime import datetime
from dataclasses import dataclass

from .base import DataAnalysisTool, ToolExecutionResult
from app.data import DataHandler
from app.data.unified_dataset_builder import UnifiedDatasetBuilder
from .enhanced_methodology_helpers import (
    generate_pre_analysis_composite_explanation,
    generate_pre_analysis_pca_explanation,
    generate_pre_analysis_comparison_explanation,
    generate_pre_analysis_variable_explanation,
    generate_data_driven_explanations,
    generate_basic_post_analysis_explanations,
    generate_correlation_analysis,
    get_score_statistics,
    analyze_method_agreement,
    find_strong_correlations,
    get_score_correlations
)


@dataclass
class VariableInfo:
    """Information about a variable"""
    description: str
    data_type: str
    relevance_template: str  # Use {region} placeholder
    category: str = "risk_factor"
    
    def get_relevance(self, region: str) -> str:
        """Get region-specific relevance"""
        return self.relevance_template.format(region=region)


@dataclass
class MethodInfo:
    """Information about an analysis method"""
    display_name: str
    description: str
    icon: str
    strengths: List[str]
    limitations: List[str]
    mathematical_steps: Dict[str, str]
    
    
# Enhanced variable configuration with relationship information
@dataclass
class VariableRelationship:
    """Information about how a variable relates to malaria risk"""
    relationship_type: str  # "direct", "inverse", "complex"
    strength: str  # "strong", "moderate", "weak"
    explanation: str
    examples: List[str]

@dataclass
class AnalysisContext:
    """Context information about the current analysis state"""
    has_data: bool
    analysis_completed: bool
    methods_run: List[str]
    variables_used: List[str]
    ward_count: int
    region: str
    unified_dataset_available: bool

# Configuration data - externalized for scalability
VARIABLE_CONFIG = {
    'pfpr': VariableInfo(
        description='Plasmodium falciparum parasite rate - malaria infection prevalence',
        data_type='Continuous (0-1 proportion)',
        relevance_template='Core malaria indicator for {region} - directly measures infection rates',
        category='epidemiological'
    ),
    'housing_quality': VariableInfo(
        description='Housing quality index - structural and material conditions',
        data_type='Continuous index (0-1)',
        relevance_template='Critical in {region} - poor housing increases vector exposure',
        category='socioeconomic'
    ),
    'elevation': VariableInfo(
        description='Elevation above sea level - affects mosquito breeding',
        data_type='Continuous (meters)',
        relevance_template='Important for {region} - affects mosquito habitat suitability',
        category='environmental'
    ),
    'u5_tpr_rdt': VariableInfo(
        description='Under-5 test positivity rate via rapid diagnostic test',
        data_type='Continuous (0-1 proportion)',
        relevance_template='Key for {region} - children are most vulnerable population',
        category='epidemiological'
    ),
    'rainfall': VariableInfo(
        description='Annual rainfall - affects mosquito breeding cycles',
        data_type='Continuous (mm/year)',
        relevance_template='Critical for {region} - influences breeding patterns',
        category='environmental'
    ),
    'temperature': VariableInfo(
        description='Average temperature - influences parasite development',
        data_type='Continuous (°C)',
        relevance_template='Important for {region} - affects parasite lifecycle',
        category='environmental'
    ),
    'population_density': VariableInfo(
        description='Population per square kilometer',
        data_type='Continuous (people/km²)',
        relevance_template='Relevant for {region} - affects transmission dynamics',
        category='demographic'
    ),
    'healthcare_access': VariableInfo(
        description='Distance to nearest health facility',
        data_type='Continuous (km)',
        relevance_template='Critical for {region} - impacts treatment access',
        category='healthcare'
    ),
    'education_level': VariableInfo(
        description='Literacy rate and education indicators',
        data_type='Continuous index (0-1)',
        relevance_template='Important for {region} - affects prevention practices',
        category='socioeconomic'
    ),
    'poverty_index': VariableInfo(
        description='Socioeconomic poverty measurements',
        data_type='Continuous index (0-1)',
        relevance_template='Key for {region} - poverty increases vulnerability',
        category='socioeconomic'
    )
}

METHOD_CONFIG = {
    'composite': MethodInfo(
        display_name='Composite Scoring',
        description='Multi-factor risk scoring system using multiple model combinations',
        icon='🎯',
        strengths=[
            'Intuitive and interpretable',
            'Equal weighting of all variables',
            'Multiple model combinations for robustness',
            'Robust median aggregation approach'
        ],
        limitations=[
            'Assumes equal importance of variables',
            'Simple arithmetic mean approach',
            'May not capture complex variable interactions',
            'Rank-based risk categorization'
        ],
        mathematical_steps={
            'normalization': 'normalized = (value - min) / (max - min)',
            'inverse_handling': 'inv_normalized = (1/value - 1/min) / (1/max - 1/min)',
            'model_scoring': 'model_score = Σ(normalized_vars) / n_vars',
            'median_aggregation': 'final_score = median(all_model_scores)',
            'risk_categorization': 'thirds_division: top 33% = High Risk'
        }
    ),
    'pca': MethodInfo(
        display_name='Principal Component Analysis',
        description='Statistical technique for dimensionality reduction and pattern extraction',
        icon='🔬',
        strengths=[
            'Data-driven variable weighting',
            'Captures hidden patterns',
            'Reduces dimensionality',
            'Identifies key variance sources'
        ],
        limitations=[
            'Less interpretable results',
            'Assumes linear relationships',
            'Sensitive to variable scaling',
            'May lose important information'
        ],
        mathematical_steps={
            'standardization': 'X_standardized = (X - μ) / σ',
            'covariance_matrix': 'C = (1/n-1) × X^T × X',
            'eigendecomposition': 'C = P × Λ × P^T',
            'component_selection': 'Kaiser criterion (eigenvalue > 1)',
            'score_calculation': 'Y = X × P_selected'
        }
    )
}

# Scalable method list - automatically derived from config
SUPPORTED_METHODS = list(METHOD_CONFIG.keys()) + ['both']


class MethodologyExplanationInput(BaseModel):
    """Input for methodology explanation tool with proper validation"""
    
    session_id: str = Field(..., description="Session identifier for data access")
    
    methods: List[str] = Field(
        default=["both"], 
        description="Which analysis methods to explain"
    )
    
    explanation_depth: Literal["overview", "detailed", "technical"] = Field(
        default="detailed",
        description="Level of technical detail in explanations"
    )
    
    include_variables: bool = Field(
        default=True,
        description="Include detailed variable explanations specific to user's data"
    )
    
    include_examples: bool = Field(
        default=True,
        description="Include examples using user's actual data"
    )
    
    include_comparison: bool = Field(
        default=True,
        description="Include comparison between methods"
    )
    
    focus_area: Optional[str] = Field(
        default=None,
        description="Specific aspect to focus on (e.g., 'variable_selection', 'scoring', 'ranking')"
    )
    
    user_question: Optional[str] = Field(
        default=None,
        description="Specific user question about methodology (enables LLM-powered responses)"
    )
    
    target_ward: Optional[str] = Field(
        default=None,
        description="Specific ward name for ward-focused explanations"
    )
    
    use_llm: bool = Field(
        default=True,
        description="Whether to use LLM for dynamic, conversational responses"
    )
    
    @validator('methods')
    def validate_methods(cls, v):
        """Ensure methods list is valid and not empty"""
        if not v:
            return ["both"]
        
        for method in v:
            if method not in SUPPORTED_METHODS:
                raise ValueError(f"Invalid method: {method}. Must be one of: {SUPPORTED_METHODS}")
        return v


class ExplainAnalysisMethodology(DataAnalysisTool):
    """
    Dynamic Analysis Methodology Explanation Tool
    
    Provides tailored, data-driven explanations of analysis methodologies
    rather than generic template responses. Uses real user data for examples.
    """
    
    name: str = "explain_analysis_methodology"
    description: str = (
        "Explains analysis methodologies (composite scoring and PCA) using your actual data "
        "for tailored, specific explanations rather than generic descriptions."
    )
    explanation_depth: str = "detailed"
    include_variables: bool = True
    include_examples: bool = True
    include_comparison: bool = True
    focus_area: Optional[str] = None
    llm_manager: Optional[Any] = None  # LLM manager for dynamic responses
    
    def set_llm_manager(self, llm_manager):
        """Set LLM manager for dynamic response generation"""
        self.llm_manager = llm_manager
        
    def get_examples(self) -> List[str]:
        """Get example usage patterns"""
        examples = []
        
        # Method-specific examples
        for method_name, method_info in METHOD_CONFIG.items():
            examples.extend([
                f"Explain how {method_info.display_name.lower()} works with my data",
                f"What makes a ward high-risk in my {method_name} scores?",
                f"How reliable are my {method_name} results?"
            ])
        
        # General examples
        examples.extend([
            "Compare analysis methods for my region",
            "What variables were selected for my analysis and why?",
            "How are the risk scores calculated for my wards?",
            "Give me a technical explanation of both methods",
            "How do you handle missing data in my analysis?",
            "Why do the two methods give different results?"
        ])
        
        return examples
    
    def execute(self, session_id: str, methods: List[str] = None, 
                explanation_depth: str = "detailed", **kwargs) -> ToolExecutionResult:
        """
        Execute enhanced methodology explanation with context-aware approach and LLM integration
        
        Args:
            session_id: Session identifier
            methods: Methods to explain (defaults to ["both"])
            explanation_depth: Level of detail (overview/detailed/technical)
            **kwargs: Additional parameters including user_question for LLM responses
        """
        
        try:
            # Validate inputs using Pydantic
            input_data = MethodologyExplanationInput(
                session_id=session_id,
                methods=methods or ["both"],
                explanation_depth=explanation_depth,
                **kwargs
            )
            
            # Detect analysis context
            context = self._detect_analysis_context(session_id)
            
            if not context.has_data:
                return ToolExecutionResult(
                    success=False,
                    message="❌ No data found for this session. Please upload data first.",
                    data={"error": "no_data_found"}
                )
            
            # Check if this is a specific user question requiring LLM response
            if input_data.user_question and input_data.use_llm and self.llm_manager:
                response = self._generate_llm_powered_response(input_data, context)
            else:
                # Route to appropriate explanation approach
                if context.analysis_completed:
                    response = self._generate_post_analysis_explanation(input_data, context)
                else:
                    response = self._generate_pre_analysis_explanation(input_data, context)
            
            return ToolExecutionResult(
                success=True,
                message=response,
                data={
                    "session_id": session_id,
                    "context_type": "post_analysis" if context.analysis_completed else "pre_analysis",
                    "methods_explained": input_data.methods,
                    "explanation_depth": input_data.explanation_depth,
                    "region": context.region,
                    "wards_count": context.ward_count,
                    "variables_available": context.variables_used,
                    "analysis_completed": context.analysis_completed,
                    "methods_run": context.methods_run,
                    "llm_powered": bool(input_data.user_question and input_data.use_llm and self.llm_manager)
                }
            )
            
        except Exception as e:
            logging.error(f"Error in enhanced methodology explanation: {str(e)}")
            return ToolExecutionResult(
                success=False,
                message=f"❌ Error generating methodology explanation: {str(e)}",
                data={"error": str(e)}
            )
    
    def _get_analysis_results(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get existing analysis results if available"""
        try:
            # Try to get unified dataset which contains analysis results
            builder = UnifiedDatasetBuilder(session_id=session_id)
            unified_data = builder.create_settlement_free_unified_dataset()
            
            if unified_data is not None:
                # Extract analysis results
                results = {}
                
                # Check for composite scores
                if 'composite_score' in unified_data.columns:
                    results['composite'] = {
                        'scores': unified_data['composite_score'].values,
                        'min_score': unified_data['composite_score'].min(),
                        'max_score': unified_data['composite_score'].max(),
                        'mean_score': unified_data['composite_score'].mean(),
                        'std_score': unified_data['composite_score'].std(),
                        'wards_count': len(unified_data)
                    }
                
                # Check for PCA scores
                if 'pca_score' in unified_data.columns:
                    results['pca'] = {
                        'scores': unified_data['pca_score'].values,
                        'min_score': unified_data['pca_score'].min(),
                        'max_score': unified_data['pca_score'].max(),
                        'mean_score': unified_data['pca_score'].mean(),
                        'std_score': unified_data['pca_score'].std(),
                        'wards_count': len(unified_data)
                    }
                
                return results if results else None
                
        except Exception:
            return None
    
    def _generate_dynamic_explanations(
        self, 
        input_data: MethodologyExplanationInput,
        data_handler: DataHandler,
        region_info: Dict[str, Any],
        analysis_results: Optional[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Generate dynamic, tailored explanations based on user's actual data"""
        
        explanations = {}
        
        # Get user's specific data context
        ward_count = len(data_handler.csv_data)
        region_name = region_info.get('region', 'Unknown')
        selected_variables = region_info.get('variables', [])
        
        # Generate explanations based on requested methods
        if "composite" in input_data.methods or "both" in input_data.methods:
            explanations['composite'] = self._generate_composite_explanation(
                input_data, ward_count, region_name, selected_variables, analysis_results
            )
        
        if "pca" in input_data.methods or "both" in input_data.methods:
            explanations['pca'] = self._generate_pca_explanation(
                input_data, ward_count, region_name, selected_variables, analysis_results
            )
        
        if input_data.include_comparison and ("both" in input_data.methods or 
                                            (len(input_data.methods) > 1 and 
                                             "composite" in input_data.methods and 
                                             "pca" in input_data.methods)):
            explanations['comparison'] = self._generate_comparison_explanation(
                input_data, analysis_results
            )
        
        # Add variable explanations if requested
        if input_data.include_variables:
            explanations['variables'] = self._generate_variable_explanations(
                selected_variables, region_name, input_data.explanation_depth
            )
        
        return explanations
    
    def _generate_method_explanation(
        self,
        method_name: str,
        input_data: MethodologyExplanationInput,
        ward_count: int,
        region_name: str,
        selected_variables: List[str],
        analysis_results: Optional[Dict[str, Any]]
    ) -> str:
        """Generate tailored explanation for any analysis method"""
        
        method_info = METHOD_CONFIG.get(method_name)
        if not method_info:
            return f"## ❌ Unknown Method: {method_name}\n\n"
        
        icon = method_info.icon
        display_name = method_info.display_name
        
        # Build explanation based on depth
        if input_data.explanation_depth == "overview":
            explanation = f"## {icon} {display_name} Method (Your {region_name} Analysis)\n\n"
            explanation += f"**What it is:** {method_info.description} that analyzes {len(selected_variables)} key malaria indicators "
            explanation += f"across your {ward_count} {region_name} wards.\n\n"
            
        elif input_data.explanation_depth == "detailed":
            explanation = f"## {icon} {display_name} - Detailed Explanation\n\n"
            explanation += f"**Applied to your data:** {ward_count} {region_name} wards with {len(selected_variables)} variables\n\n"
            
            explanation += "### 📊 How It Works:\n"
            explanation += "1. **Variable Selection:** Region-aware selection identified these key indicators for your area:\n"
            for var in selected_variables:
                var_info = self._get_variable_info(var)
                explanation += f"   - `{var}`: {var_info.description}\n"
            
            # Add method-specific steps
            if method_name == 'composite':
                explanation += "\n2. **Standardization:** Each variable is standardized (z-score) to ensure fair weighting\n"
                explanation += "3. **Composite Calculation:** Variables are combined using equal weights\n"
                explanation += "4. **Risk Scoring:** Final scores are normalized to 0-1 range\n"
                explanation += "5. **Ranking:** Wards are ranked from highest to lowest risk\n\n"
            elif method_name == 'pca':
                explanation += "\n2. **Correlation Analysis:** Identifies relationships between variables\n"
                explanation += "3. **Component Extraction:** Creates new components that capture maximum variance\n"
                explanation += "4. **Dimensionality Reduction:** Keeps only components explaining significant variance\n"
                explanation += "5. **Score Calculation:** Projects your wards onto principal components\n"
                explanation += "6. **Risk Ranking:** Ranks wards based on component scores\n\n"
            
            # Add actual results if available
            if analysis_results and method_name in analysis_results:
                method_data = analysis_results[method_name]
                explanation += f"### 📈 Your Results:\n"
                explanation += f"- **Score Range:** {method_data['min_score']:.3f} to {method_data['max_score']:.3f}\n"
                explanation += f"- **Average Score:** {method_data['mean_score']:.3f} ± {method_data['std_score']:.3f}\n"
                explanation += f"- **Wards Analyzed:** {method_data['wards_count']}\n\n"
            
        else:  # technical
            explanation = f"## {icon} {display_name} - Technical Implementation\n\n"
            explanation += f"**Dataset:** {ward_count} × {len(selected_variables)} matrix from {region_name} region\n\n"
            
            explanation += "### 🔢 Mathematical Approach:\n"
            for i, (step_name, formula) in enumerate(method_info.mathematical_steps.items(), 1):
                explanation += f"{i}. **{step_name.replace('_', ' ').title()}:**\n"
                explanation += f"   ```\n   {formula}\n   ```\n"
            
            if method_name == 'composite':
                explanation += f"   Where n = {len(selected_variables)} variables\n\n"
            else:
                explanation += "\n"
            
            if analysis_results and method_name in analysis_results:
                method_data = analysis_results[method_name]
                explanation += f"### 📊 Statistical Summary:\n"
                explanation += f"- **N:** {method_data['wards_count']}\n"
                explanation += f"- **Mean (μ):** {method_data['mean_score']:.6f}\n"
                explanation += f"- **Std Dev (σ):** {method_data['std_score']:.6f}\n"
                explanation += f"- **Range:** [{method_data['min_score']:.6f}, {method_data['max_score']:.6f}]\n\n"
        
        return explanation
    
    def _generate_composite_explanation(
        self,
        input_data: MethodologyExplanationInput,
        ward_count: int,
        region_name: str,
        selected_variables: List[str],
        analysis_results: Optional[Dict[str, Any]]
    ) -> str:
        """Generate tailored composite scoring explanation"""
        return self._generate_method_explanation(
            'composite', input_data, ward_count, region_name, selected_variables, analysis_results
        )
    
    def _generate_pca_explanation(
        self,
        input_data: MethodologyExplanationInput,
        ward_count: int,
        region_name: str,
        selected_variables: List[str],
        analysis_results: Optional[Dict[str, Any]]
    ) -> str:
        """Generate tailored PCA explanation"""
        return self._generate_method_explanation(
            'pca', input_data, ward_count, region_name, selected_variables, analysis_results
        )
    
    def _generate_comparison_explanation(
        self,
        input_data: MethodologyExplanationInput,
        analysis_results: Optional[Dict[str, Any]]
    ) -> str:
        """Generate method comparison explanation"""
        
        explanation = "## ⚖️ Method Comparison\n\n"
        
        if input_data.explanation_depth == "overview":
            explanation += "**Key Differences:**\n"
            explanation += "- **Composite:** Equal-weight combination, intuitive scoring\n"
            explanation += "- **PCA:** Data-driven weighting, captures hidden patterns\n"
            explanation += "- **Results:** May differ significantly due to different approaches\n\n"
            
        else:  # detailed or technical
            explanation += "### 🔍 Methodological Differences:\n\n"
            explanation += "| Aspect | Composite Scoring | PCA |\n"
            explanation += "|--------|------------------|-----|\n"
            
            # Use configuration data for comparison
            comparison_aspects = [
                ("Weighting", "Equal weights", "Data-driven weights"),
                ("Approach", "Additive combination", "Dimensional reduction"),
                ("Interpretation", "Intuitive, direct", "Statistical, abstract"),
                ("Assumptions", "Linear relationships", "Linear combinations"),
                ("Robustness", "Sensitive to outliers", "Captures variance patterns")
            ]
            
            for aspect, comp_desc, pca_desc in comparison_aspects:
                explanation += f"| **{aspect}** | {comp_desc} | {pca_desc} |\n"
            explanation += "\n"
            
            # Add method strengths/limitations
            comp_info = METHOD_CONFIG.get('composite')
            pca_info = METHOD_CONFIG.get('pca')
            
            if comp_info and pca_info:
                explanation += "### 💪 Method Strengths:\n"
                explanation += "**Composite Scoring:**\n"
                for strength in comp_info.strengths:
                    explanation += f"- {strength}\n"
                explanation += "\n**PCA:**\n"
                for strength in pca_info.strengths:
                    explanation += f"- {strength}\n"
                explanation += "\n"
                
                explanation += "### ⚠️ Method Limitations:\n"
                explanation += "**Composite Scoring:**\n"
                for limitation in comp_info.limitations:
                    explanation += f"- {limitation}\n"
                explanation += "\n**PCA:**\n"
                for limitation in pca_info.limitations:
                    explanation += f"- {limitation}\n"
                explanation += "\n"
            
            if analysis_results and 'composite' in analysis_results and 'pca' in analysis_results:
                comp_data = analysis_results['composite']
                pca_data = analysis_results['pca']
                
                explanation += "### 📊 Your Results Comparison:\n"
                explanation += f"- **Composite Range:** {comp_data['min_score']:.3f} - {comp_data['max_score']:.3f}\n"
                explanation += f"- **PCA Range:** {pca_data['min_score']:.3f} - {pca_data['max_score']:.3f}\n"
                explanation += f"- **Composite Mean:** {comp_data['mean_score']:.3f}\n"
                explanation += f"- **PCA Mean:** {pca_data['mean_score']:.3f}\n\n"
                
                explanation += "### 🤔 Why Results Differ:\n"
                explanation += "1. **Different Weighting:** PCA uses data-driven weights vs equal weights\n"
                explanation += "2. **Different Math:** Additive vs matrix transformation approaches\n"
                explanation += "3. **Different Focus:** Direct scoring vs pattern extraction\n"
                explanation += "4. **This is Normal:** Both methods are valid but capture different aspects\n\n"
        
        return explanation
    
    def _generate_variable_explanations(
        self,
        selected_variables: List[str],
        region_name: str,
        explanation_depth: str
    ) -> str:
        """Generate explanations for selected variables"""
        
        explanation = f"## 📊 Variables Selected for {region_name} Region\n\n"
        
        if explanation_depth == "overview":
            explanation += f"**{len(selected_variables)} key variables were selected based on your region's characteristics:**\n\n"
        else:
            explanation += f"**Region-Aware Selection:** {len(selected_variables)} variables chosen specifically for {region_name} zone:\n\n"
        
        for var in selected_variables:
            var_info = self._get_variable_info(var)
            explanation += f"### 🔹 {var}\n"
            explanation += f"**Description:** {var_info.description}\n"
            explanation += f"**Relevance:** {var_info.get_relevance(region_name)}\n"
            explanation += f"**Data Type:** {var_info.data_type}\n\n"
        
        return explanation
    
    def _get_region_info(self, csv_data) -> Dict[str, Any]:
        """Get region information from data"""
        try:
            # Simple region detection based on state codes
            if 'StateCode' in csv_data.columns:
                state_codes = csv_data['StateCode'].unique()
                # Map common state codes to regions (simplified)
                region_mapping = {
                    'KN': 'North_West',
                    'KD': 'North_West', 
                    'JG': 'North_West',
                    'OS': 'South_West',
                    'OY': 'South_West',
                    'OG': 'South_West'
                }
                
                for code in state_codes:
                    if code in region_mapping:
                        region = region_mapping[code]
                        # Default variable selection for regions
                        variables = ['pfpr', 'housing_quality', 'elevation', 'u5_tpr_rdt']
                        return {
                            'region': region,
                            'variables': variables,
                            'state_codes': list(state_codes)
                        }
            
            # Default fallback
            return {
                'region': 'Unknown',
                'variables': ['pfpr', 'housing_quality', 'elevation', 'u5_tpr_rdt'],
                'state_codes': []
            }
            
        except Exception:
            return {
                'region': 'Unknown', 
                'variables': ['pfpr', 'housing_quality', 'elevation', 'u5_tpr_rdt'],
                'state_codes': []
            }
    
    def _get_variable_info(self, variable: str) -> VariableInfo:
        """Get variable information from configuration"""
        return VARIABLE_CONFIG.get(variable, VariableInfo(
            description=f"Malaria risk indicator: {variable}",
            data_type="Continuous numeric",
            relevance_template="Selected based on {region} regional characteristics",
            category="risk_factor"
        ))
    
    def _format_methodology_response(
        self,
        explanations: Dict[str, str],
        input_data: MethodologyExplanationInput,
        region_info: Dict[str, Any]
    ) -> str:
        """Format the comprehensive methodology response"""
        
        response = f"# 📚 Analysis Methodology Explanation\n\n"
        response += f"**Your Data Context:** {region_info.get('region', 'Unknown')} region analysis\n"
        response += f"**Explanation Level:** {input_data.explanation_depth.title()}\n"
        response += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        response += "---\n\n"
        
        # Add explanations in logical order
        if 'variables' in explanations:
            response += explanations['variables'] + "\n"
        
        if 'composite' in explanations:
            response += explanations['composite'] + "\n"
        
        if 'pca' in explanations:
            response += explanations['pca'] + "\n"
        
        if 'comparison' in explanations:
            response += explanations['comparison'] + "\n"
        
        # Add footer
        response += "---\n\n"
        response += "💡 **Need More Details?** You can ask for specific aspects like:\n"
        response += "- *'Explain the variable selection process'*\n"
        response += "- *'How do you handle missing data?'*\n"
        response += "- *'Show me technical implementation details'*\n"
        response += "- *'Compare methods for my specific results'*\n"
        
        return response
    
    def _generate_pre_analysis_explanation(
        self, 
        input_data: MethodologyExplanationInput,
        context: AnalysisContext
    ) -> str:
        """Generate pre-analysis explanations with theoretical focus"""
        
        explanations = []
        
        # Header
        explanations.append(f"# 📚 **Methodology Guide - Pre-Analysis**")
        explanations.append(f"**Your Data Context:** {context.region} region, {context.ward_count} wards")
        explanations.append(f"**Status:** Ready for analysis")
        explanations.append("")
        explanations.append("---")
        explanations.append("")
        
        # Method explanations based on request
        if "composite" in input_data.methods or "both" in input_data.methods:
            explanations.append(generate_pre_analysis_composite_explanation(input_data, context))
        
        if "pca" in input_data.methods or "both" in input_data.methods:
            explanations.append(generate_pre_analysis_pca_explanation(input_data, context))
        
        if input_data.include_comparison and ("both" in input_data.methods or len(input_data.methods) > 1):
            explanations.append(generate_pre_analysis_comparison_explanation(input_data, context))
        
        # Variable selection explanation
        if input_data.include_variables:
            explanations.append(generate_pre_analysis_variable_explanation(input_data, context))
        
        # Next steps
        explanations.append("## 🚀 **Ready to Proceed**")
        explanations.append("")
        explanations.append("You can now run the analysis with: *'Run complete analysis'* or *'Start composite analysis'*")
        explanations.append("")
        explanations.append("After analysis completion, ask for detailed explanations of your specific results!")
        
        return "\n".join(explanations)
    
    def _generate_post_analysis_explanation(
        self, 
        input_data: MethodologyExplanationInput,
        context: AnalysisContext
    ) -> str:
        """Generate post-analysis explanations with data-driven insights"""
        
        # Get actual analysis results
        analysis_results = self._get_analysis_results(input_data.session_id)
        
        explanations = []
        
        # Header
        explanations.append(f"# 📊 **Methodology Analysis - Your Results**")
        explanations.append(f"**Your Data:** {context.region} region, {context.ward_count} wards")
        explanations.append(f"**Methods Completed:** {', '.join(context.methods_run)}")
        explanations.append("")
        explanations.append("---")
        explanations.append("")
        
        # Generate data-driven explanations
        if context.unified_dataset_available:
            # Enhanced explanations with actual data
            explanations.append(generate_data_driven_explanations(input_data, context, analysis_results))
        else:
            # Basic explanations with available results
            explanations.append(generate_basic_post_analysis_explanations(input_data, context, analysis_results))
        
        # Correlation analysis if available
        if context.unified_dataset_available:
            explanations.append(generate_correlation_analysis(input_data.session_id))
        
        return "\n".join(explanations)
    
    def _generate_llm_powered_response(
        self, 
        input_data: MethodologyExplanationInput,
        context: AnalysisContext
    ) -> str:
        """Generate LLM-powered responses to specific user questions"""
        
        try:
            # Build context-aware prompt based on analysis state
            if context.analysis_completed:
                prompt = self._build_post_analysis_prompt(input_data, context)
            else:
                prompt = self._build_pre_analysis_prompt(input_data, context)
            
            # Get LLM response
            if self.llm_manager:
                llm_response = self.llm_manager.get_completion(
                    prompt=prompt,
                    temperature=0.3,  # Lower temperature for more focused responses
                    max_tokens=2000
                )
                
                # Add header and footer for context
                response_parts = []
                response_parts.append(f"# 🤖 **AI-Powered Methodology Explanation**")
                response_parts.append(f"**Your Question:** {input_data.user_question}")
                response_parts.append(f"**Context:** {context.region} region, {'Post-Analysis' if context.analysis_completed else 'Pre-Analysis'}")
                response_parts.append("")
                response_parts.append("---")
                response_parts.append("")
                response_parts.append(llm_response)
                response_parts.append("")
                response_parts.append("---")
                response_parts.append("")
                response_parts.append("💡 **Need more details?** Ask follow-up questions about specific aspects!")
                
                return "\n".join(response_parts)
            else:
                return "❌ LLM not available for dynamic responses. Using structured explanations instead."
                
        except Exception as e:
            logging.error(f"Error generating LLM response: {e}")
            return f"❌ Error generating AI response: {str(e)}. Please try a more specific question."
    
    def _build_pre_analysis_prompt(
        self, 
        input_data: MethodologyExplanationInput,
        context: AnalysisContext
    ) -> str:
        """Build context-aware prompt for pre-analysis questions"""
        
        prompt_parts = []
        prompt_parts.append("You are an expert malaria epidemiologist explaining analysis methodologies.")
        prompt_parts.append(f"The user has uploaded data for {context.ward_count} wards in {context.region} region but has not run analysis yet.")
        prompt_parts.append("")
        prompt_parts.append("CONTEXT - Analysis Methods Available:")
        prompt_parts.append("1. COMPOSITE SCORE METHOD:")
        prompt_parts.append("   - Uses min-max normalization (0-1 scaling)")
        prompt_parts.append("   - Creates multiple model combinations of variables")
        prompt_parts.append("   - Equal weighting through arithmetic means")
        prompt_parts.append("   - Final score = median across all models")
        prompt_parts.append("   - Risk categories: top 33% = High, middle 33% = Medium, bottom 33% = Low")
        prompt_parts.append("")
        prompt_parts.append("2. PCA METHOD:")
        prompt_parts.append("   - Uses z-score standardization")
        prompt_parts.append("   - Eigenvalue decomposition for component extraction")
        prompt_parts.append("   - Data-driven variable weighting")
        prompt_parts.append("   - Kaiser criterion (eigenvalue > 1) for component selection")
        prompt_parts.append("")
        prompt_parts.append("CONTEXT - Variable Selection Process:")
        prompt_parts.append("- Variables scored based on malaria-related keywords")
        prompt_parts.append("- Regional relevance considered for selection")
        prompt_parts.append("- Direct vs inverse relationships automatically detected")
        prompt_parts.append("- Expected 4-5 variables for composite, 8-10 for PCA")
        prompt_parts.append("")
        prompt_parts.append(f"USER QUESTION: {input_data.user_question}")
        prompt_parts.append("")
        prompt_parts.append("Provide a detailed, educational explanation that:")
        prompt_parts.append("1. Directly answers their question")
        prompt_parts.append("2. Uses the correct methodology details above")
        prompt_parts.append("3. Explains what they can expect when they run the analysis")
        prompt_parts.append("4. Uses simple, clear language suitable for public health practitioners")
        prompt_parts.append("5. Focuses on practical implications for malaria intervention planning")
        
        return "\n".join(prompt_parts)
    
    def _build_post_analysis_prompt(
        self, 
        input_data: MethodologyExplanationInput,
        context: AnalysisContext
    ) -> str:
        """Build context-aware prompt for post-analysis questions"""
        
        prompt_parts = []
        prompt_parts.append("You are an expert malaria epidemiologist explaining analysis results.")
        prompt_parts.append(f"The user has completed analysis on {context.ward_count} wards in {context.region} region.")
        prompt_parts.append(f"Methods completed: {', '.join(context.methods_run)}")
        prompt_parts.append("")
        
        # Add actual data context if available
        try:
            from app.data.unified_dataset_builder import load_unified_dataset
            unified_data = load_unified_dataset(input_data.session_id)
            
            if unified_data is not None:
                prompt_parts.append("ACTUAL RESULTS CONTEXT:")
                
                if 'composite_score' in unified_data.columns:
                    comp_stats = get_score_statistics(unified_data, 'composite_score')
                    prompt_parts.append(f"- Composite scores range: {comp_stats['min']:.3f} to {comp_stats['max']:.3f}")
                    prompt_parts.append(f"- Top ward: {comp_stats['top_ward']} ({comp_stats['top_score']:.3f})")
                    prompt_parts.append(f"- Bottom ward: {comp_stats['bottom_ward']} ({comp_stats['bottom_score']:.3f})")
                
                if 'pca_score' in unified_data.columns:
                    pca_stats = get_score_statistics(unified_data, 'pca_score')
                    prompt_parts.append(f"- PCA scores range: {pca_stats['min']:.3f} to {pca_stats['max']:.3f}")
                    prompt_parts.append(f"- PCA top ward: {pca_stats['top_ward']} ({pca_stats['top_score']:.3f})")
                    prompt_parts.append(f"- PCA bottom ward: {pca_stats['bottom_ward']} ({pca_stats['bottom_score']:.3f})")
                
                if 'composite_score' in unified_data.columns and 'pca_score' in unified_data.columns:
                    agreement_stats = analyze_method_agreement(unified_data)
                    prompt_parts.append(f"- Method correlation: {agreement_stats['correlation']:.3f}")
                    prompt_parts.append(f"- Top 10 consensus: {agreement_stats['top_consensus']:.0f}% agreement")
                
                # Add variable information
                numeric_cols = unified_data.select_dtypes(include=[np.number]).columns
                analysis_cols = [col for col in numeric_cols if col not in ['WardCode', 'LGACode', 'StateCode']]
                if len(analysis_cols) > 2:
                    prompt_parts.append(f"- Variables in dataset: {', '.join(analysis_cols[:10])}")
                
        except Exception as e:
            prompt_parts.append("- Could not load detailed results data")
        
        prompt_parts.append("")
        prompt_parts.append(f"USER QUESTION: {input_data.user_question}")
        prompt_parts.append("")
        prompt_parts.append("Provide a detailed, data-driven explanation that:")
        prompt_parts.append("1. Directly answers their question using their actual results")
        prompt_parts.append("2. References specific ward names and scores when relevant")
        prompt_parts.append("3. Explains correlations and patterns in their specific data")
        prompt_parts.append("4. Provides actionable insights for malaria intervention planning")
        prompt_parts.append("5. Uses the actual methodology details (equal weighting for composite, data-driven for PCA)")
        prompt_parts.append("6. Explains why certain wards ranked as they did based on variable values")
        
        return "\n".join(prompt_parts) 