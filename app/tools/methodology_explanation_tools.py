"""
Methodology Explanation Tools - Dynamic, Tailored Analysis Method Explanations

This module provides tools for explaining analysis methodologies in a dynamic,
data-driven way rather than generic template responses.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, validator
from datetime import datetime
from dataclasses import dataclass

from .base import DataAnalysisTool, ToolExecutionResult
from app.data import DataHandler
from app.data.unified_dataset_builder import UnifiedDatasetBuilder


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
        description='Multi-factor risk scoring system combining multiple indicators',
        icon='🎯',
        strengths=[
            'Intuitive and interpretable',
            'Equal weighting of all variables',
            'Direct risk scoring',
            'Easy to understand results'
        ],
        limitations=[
            'Assumes equal importance of variables',
            'Sensitive to outliers',
            'May not capture complex interactions',
            'Linear combination only'
        ],
        mathematical_steps={
            'standardization': 'z_i = (x_i - μ) / σ',
            'composite_score': 'CS = Σ(w_i × z_i) / n',
            'normalization': 'Risk_Score = (CS - CS_min) / (CS_max - CS_min)'
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
        Execute methodology explanation with dynamic, tailored content
        
        Args:
            session_id: Session identifier
            methods: Methods to explain (defaults to ["both"])
            explanation_depth: Level of detail (overview/detailed/technical)
            **kwargs: Additional parameters
        """
        
        try:
            # Validate inputs using Pydantic
            input_data = MethodologyExplanationInput(
                session_id=session_id,
                methods=methods or ["both"],
                explanation_depth=explanation_depth,
                **kwargs
            )
            
            # Get session data
            data_handler = DataHandler(session_id=session_id)
            if not data_handler.csv_data or not data_handler.shapefile_data:
                return ToolExecutionResult(
                    success=False,
                    message="❌ No data found for this session. Please upload data first.",
                    data={"error": "no_data_found"}
                )
            
            # Get region information from data
            region_info = self._get_region_info(data_handler.csv_data)
            
            # Get analysis results if available
            analysis_results = self._get_analysis_results(session_id)
            
            # Generate dynamic explanations
            explanations = self._generate_dynamic_explanations(
                input_data, data_handler, region_info, analysis_results
            )
            
            # Format comprehensive response
            formatted_response = self._format_methodology_response(
                explanations, input_data, region_info
            )
            
            return ToolExecutionResult(
                success=True,
                message=formatted_response,
                data={
                    "session_id": session_id,
                    "methods_explained": input_data.methods,
                    "explanation_depth": input_data.explanation_depth,
                    "region": region_info.get('region', 'Unknown'),
                    "wards_count": len(data_handler.csv_data),
                    "variables_selected": region_info.get('variables', []),
                    "has_analysis_results": analysis_results is not None,
                    "explanations_generated": list(explanations.keys())
                }
            )
            
        except Exception as e:
            logging.error(f"Error in methodology explanation: {str(e)}")
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