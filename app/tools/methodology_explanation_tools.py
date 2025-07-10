"""
Methodology Explanation Tools for ChatMRPT
==========================================

Tools to explain analysis methodologies and help users understand the different approaches.
"""

import logging
from typing import Dict, Any, Optional, ClassVar
from pydantic import BaseModel, Field

from .base import BaseTool, ToolCategory, ToolExecutionResult

logger = logging.getLogger(__name__)


class ExplainAnalysisMethodologyInput(BaseModel):
    """Input for methodology explanation"""
    session_id: str = Field(..., description="Session identifier")
    methodology: str = Field(..., description="Methodology to explain: 'composite', 'pca', or 'both'")
    include_examples: bool = Field(True, description="Whether to include practical examples")
    
    
class ExplainAnalysisMethodology(BaseTool):
    """
    Explain analysis methodologies used in ChatMRPT.
    
    Provides detailed explanations of:
    - Composite risk scoring
    - Principal Component Analysis (PCA)
    - When to use each method
    - Advantages and limitations
    """
    
    name: ClassVar[str] = "explain_analysis_methodology"
    description: ClassVar[str] = "Explain analysis methodologies (composite scoring, PCA) used in malaria risk assessment"
    category: ClassVar[ToolCategory] = ToolCategory.KNOWLEDGE
    input_model: ClassVar[type] = ExplainAnalysisMethodologyInput
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute methodology explanation"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute methodology explanation"""
        try:
            session_id = kwargs.get('session_id')
            methodology = kwargs.get('methodology', 'both').lower()
            include_examples = kwargs.get('include_examples', True)
            
            explanations = {}
            
            if methodology in ['composite', 'both']:
                explanations['composite'] = self._explain_composite_scoring(include_examples)
            
            if methodology in ['pca', 'both']:
                explanations['pca'] = self._explain_pca_methodology(include_examples)
            
            if methodology == 'both':
                explanations['comparison'] = self._compare_methodologies()
            
            return ToolExecutionResult(
                success=True,
                message=f"Generated methodology explanation for {methodology}",
                data={
                    'methodology': methodology,
                    'explanations': explanations,
                    'session_id': session_id
                }
            )
            
        except Exception as e:
            logger.error(f"Error explaining methodology: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"Failed to explain methodology: {str(e)}",
                error_details=str(e)
            )
    
    def _explain_composite_scoring(self, include_examples: bool) -> Dict[str, Any]:
        """Explain composite scoring methodology"""
        explanation = {
            'name': 'Composite Risk Scoring',
            'description': 'A weighted sum approach that combines multiple malaria risk factors into a single score',
            'methodology': {
                'step1': 'Normalize all variables to 0-1 scale using min-max normalization',
                'step2': 'Apply equal weights to all variables (or custom weights if specified)',
                'step3': 'Sum weighted normalized values to create composite score',
                'step4': 'Rank wards from highest to lowest composite score'
            },
            'advantages': [
                'Simple and intuitive to understand',
                'Transparent - easy to see contribution of each variable',
                'Allows for custom weighting of variables',
                'Direct interpretation: higher score = higher risk'
            ],
            'limitations': [
                'Assumes linear relationships between variables',
                'All variables weighted equally by default',
                'May be sensitive to outliers',
                'Does not account for variable correlations'
            ],
            'best_used_when': [
                'You want a straightforward, interpretable risk score',
                'Stakeholders need to understand how the score is calculated',
                'You have expert knowledge about variable importance',
                'Variables are relatively independent'
            ]
        }
        
        if include_examples:
            explanation['example'] = {
                'scenario': 'Ward with PfPR=0.3, elevation=200m, distance_to_water=500m',
                'calculation': {
                    'pfpr_normalized': '0.6 (moderate parasite rate)',
                    'elevation_normalized': '0.3 (relatively low elevation)',
                    'water_distance_normalized': '0.4 (moderate distance)',
                    'composite_score': '0.6 + 0.3 + 0.4 = 1.3',
                    'interpretation': 'Medium-high risk ward'
                }
            }
        
        return explanation
    
    def _explain_pca_methodology(self, include_examples: bool) -> Dict[str, Any]:
        """Explain PCA methodology"""
        explanation = {
            'name': 'Principal Component Analysis (PCA)',
            'description': 'A dimensionality reduction technique that identifies patterns of variation in the data',
            'methodology': {
                'step1': 'Standardize all variables (mean=0, std=1)',
                'step2': 'Calculate correlation matrix between all variables',
                'step3': 'Extract principal components (eigenvectors) that explain most variance',
                'step4': 'Use first principal component as risk score',
                'step5': 'Rank wards based on PC1 scores'
            },
            'advantages': [
                'Reduces dimensionality while preserving information',
                'Accounts for correlations between variables',
                'Less sensitive to outliers after standardization',
                'Identifies underlying patterns in the data',
                'Objective weighting based on data structure'
            ],
            'limitations': [
                'More complex and harder to interpret',
                'Variable contributions may not match expert knowledge',
                'First component may not always represent "risk"',
                'Requires sufficient sample size',
                'Black box approach - less transparent'
            ],
            'best_used_when': [
                'You have many correlated variables',
                'You want an objective, data-driven approach',
                'Sample size is adequate (typically >50 observations)',
                'Variables show clear patterns of correlation'
            ]
        }
        
        if include_examples:
            explanation['example'] = {
                'scenario': 'Dataset with correlated environmental variables',
                'process': {
                    'variables': 'PfPR, elevation, rainfall, temperature, humidity',
                    'correlation_found': 'Rainfall, temperature, humidity highly correlated',
                    'pc1_captures': '65% of total variance in the data',
                    'interpretation': 'PC1 represents environmental suitability for malaria'
                },
                'loadings': {
                    'pfpr': '0.8 (strong positive loading)',
                    'elevation': '-0.6 (negative loading - higher elevation = lower risk)',
                    'rainfall': '0.7 (positive loading)',
                    'temperature': '0.5 (moderate positive loading)'
                }
            }
        
        return explanation
    
    def _compare_methodologies(self) -> Dict[str, Any]:
        """Compare composite and PCA methodologies"""
        return {
            'when_to_use_composite': [
                'Need transparency and explainability',
                'Have expert knowledge about variable importance',
                'Stakeholders prefer simple, understandable scores',
                'Variables are relatively independent'
            ],
            'when_to_use_pca': [
                'Have many correlated variables',
                'Want objective, data-driven approach',
                'Interested in underlying data patterns',
                'Have sufficient sample size'
            ],
            'convergence': [
                'Both methods often identify similar high-risk areas',
                'Differences usually occur in medium-risk wards',
                'Correlation between methods typically 0.7-0.9'
            ],
            'recommendation': 'Use both methods and compare results. If they agree on high-risk areas, confidence is high. If they disagree, investigate why and consider local context.'
        }


# Export the tool
__all__ = ['ExplainAnalysisMethodology']