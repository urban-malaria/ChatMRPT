"""
Complete Analysis Tools - Coordinated Dual-Method Workflow

This module provides the missing coordination layer to run both composite score
and PCA analyses together as originally intended by the ChatMRPT architecture.
"""

import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

from .base import BaseTool, ToolCategory, ToolExecutionResult, DataAnalysisTool

logger = logging.getLogger(__name__)


class RunCompleteAnalysisInput(BaseModel):
    """Input for complete analysis workflow"""
    session_id: str = Field(..., description="Session identifier for data access")
    include_visualizations: bool = Field(True, description="Whether to generate visualizations")
    create_unified_dataset: bool = Field(True, description="Whether to create/update unified dataset")


class RunCompleteAnalysis(DataAnalysisTool):
    """
    Run complete dual-method malaria risk analysis (Composite + PCA).
    
    This is the primary analysis tool that coordinates both composite score analysis
    and PCA analysis, then integrates the results into a unified dataset as originally
    intended by the ChatMRPT architecture.
    """
    
    name: str = "run_complete_analysis"
    description: str = "Run complete dual-method malaria risk analysis (Composite Score + PCA)"
    include_visualizations: bool = Field(True, description="Whether to generate visualizations")
    create_unified_dataset: bool = Field(True, description="Whether to create/update unified dataset")
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow"""
        try:
            session_id = kwargs.get('session_id')
            include_visualizations = kwargs.get('include_visualizations', True)
            create_unified_dataset = kwargs.get('create_unified_dataset', True)
            
            logger.info(f"Starting complete dual-method analysis for session {session_id}")
            
            # Step 1: Run composite score analysis
            logger.info("Step 1: Running composite score analysis...")
            composite_result = self._run_composite_analysis(session_id)
            
            if not composite_result['success']:
                return ToolExecutionResult(
                    success=False,
                    message=f"Composite analysis failed: {composite_result['message']}",
                    error_details=composite_result.get('error_details')
                )
            
            # Step 2: Run PCA analysis
            logger.info("Step 2: Running PCA analysis...")
            pca_result = self._run_pca_analysis(session_id)
            
            if not pca_result['success']:
                return ToolExecutionResult(
                    success=False,
                    message=f"PCA analysis failed: {pca_result['message']}",
                    error_details=pca_result.get('error_details')
                )
            
            # Step 3: Create/update unified dataset with both results
            if create_unified_dataset:
                logger.info("Step 3: Creating unified dataset with dual-method results...")
                unified_result = self._create_unified_dataset(session_id)
                
                if not unified_result['success']:
                    logger.warning(f"Unified dataset creation failed: {unified_result['message']}")
                    # Continue anyway - analyses succeeded
            
            # Step 4: Generate comparison summary
            logger.info("Step 4: Generating dual-method comparison summary...")
            comparison_summary = self._generate_comparison_summary(
                composite_result['data'], 
                pca_result['data']
            )
            
            # Prepare comprehensive result
            result_data = {
                'composite_analysis': composite_result['data'],
                'pca_analysis': pca_result['data'],
                'comparison_summary': comparison_summary,
                'analyses_completed': ['composite_score', 'pca'],
                'unified_dataset_created': create_unified_dataset and unified_result.get('success', False)
            }
            
            success_message = (
                f"✅ Complete dual-method analysis completed successfully!\n\n"
                f"📊 **Composite Score Analysis**: {composite_result['data'].get('wards_analyzed', 'N/A')} wards analyzed for vulnerability assessment\n"
                f"📊 **PCA Analysis**: {pca_result['data'].get('components_found', 'N/A')} principal components identified\n"
                f"📋 **Method Agreement**: {comparison_summary.get('agreement_rate', 'N/A')}% consensus on top vulnerable wards\n"
                f"🗃️ **Unified Dataset**: {'✅ Created' if result_data['unified_dataset_created'] else '⚠️ Not created'}\n\n"
                f"Both analyses provide complementary insights into malaria vulnerability patterns across all wards analyzed."
            )
            
            return ToolExecutionResult(
                success=True,
                message=success_message,
                data=result_data,
                metadata={
                    'analyses_run': ['composite_score', 'pca'],
                    'execution_steps': 4,
                    'unified_dataset_status': 'created' if result_data['unified_dataset_created'] else 'failed'
                }
            )
            
        except Exception as e:
            logger.error(f"Complete analysis failed: {e}", exc_info=True)
            return ToolExecutionResult(
                success=False,
                message=f"Complete analysis failed: {str(e)}",
                error_details=str(e)
            )
    
    def _run_composite_analysis(self, session_id: str) -> Dict[str, Any]:
        """Run composite score analysis using existing engine"""
        try:
            from ..analysis.engine import AnalysisEngine
            from ..models.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            result = analysis_engine.run_composite_analysis(session_id)
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'Composite analysis completed'),
                'data': result.get('data', {}),
                'error_details': result.get('error_details')
            }
            
        except Exception as e:
            logger.error(f"Composite analysis error: {e}")
            return {
                'success': False,
                'message': f"Composite analysis failed: {str(e)}",
                'error_details': str(e)
            }
    
    def _run_pca_analysis(self, session_id: str) -> Dict[str, Any]:
        """Run PCA analysis using existing engine"""
        try:
            from ..analysis.engine import AnalysisEngine
            from ..models.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            result = analysis_engine.run_pca_analysis(session_id)
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'PCA analysis completed'),
                'data': result.get('data', {}),
                'error_details': result.get('error_details')
            }
            
        except Exception as e:
            logger.error(f"PCA analysis error: {e}")
            return {
                'success': False,
                'message': f"PCA analysis failed: {str(e)}",
                'error_details': str(e)
            }
    
    def _create_unified_dataset(self, session_id: str) -> Dict[str, Any]:
        """Create unified dataset with integrated results"""
        try:
            from ..data.unified_dataset_builder import UnifiedDatasetBuilder
            
            builder = UnifiedDatasetBuilder(session_id)
            result = builder.build_unified_dataset()
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'Unified dataset created'),
                'data': result.get('data', {})
            }
            
        except Exception as e:
            logger.error(f"Unified dataset creation error: {e}")
            return {
                'success': False,
                'message': f"Unified dataset creation failed: {str(e)}",
                'error_details': str(e)
            }
    
    def _generate_comparison_summary(self, composite_data: Dict, pca_data: Dict) -> Dict[str, Any]:
        """Generate summary comparing both methods"""
        try:
            summary = {
                'methods_compared': ['composite_score', 'pca'],
                'composite_top_wards': composite_data.get('top_vulnerable_wards', [])[:10],
                'pca_top_wards': pca_data.get('top_vulnerable_wards', [])[:10],
                'agreement_rate': 0,
                'consensus_wards': [],
                'method_differences': []
            }
            
            # Calculate agreement between methods
            composite_top = set([w.get('ward_name', '') for w in summary['composite_top_wards']])
            pca_top = set([w.get('ward_name', '') for w in summary['pca_top_wards']])
            
            if composite_top and pca_top:
                consensus = composite_top.intersection(pca_top)
                summary['consensus_wards'] = list(consensus)
                summary['agreement_rate'] = round((len(consensus) / max(len(composite_top), len(pca_top))) * 100, 1)
            
            return summary
            
        except Exception as e:
            logger.warning(f"Comparison summary generation failed: {e}")
            return {
                'methods_compared': ['composite_score', 'pca'],
                'agreement_rate': 'unavailable',
                'error': str(e)
            }


class RunCompositeAnalysisInput(BaseModel):
    """Input for composite analysis only"""
    session_id: str = Field(..., description="Session identifier for data access")


class RunCompositeAnalysis(DataAnalysisTool):
    """Run composite score analysis only"""
    
    name: str = "run_composite_analysis"
    description: str = "Run composite score malaria risk analysis"
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute composite score analysis"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute composite score analysis"""
        try:
            session_id = kwargs.get('session_id')
            
            from ..analysis.engine import AnalysisEngine
            from ..models.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            result = analysis_engine.run_composite_analysis(session_id)
            
            if result.get('status') == 'success':
                return ToolExecutionResult(
                    success=True,
                    message="Composite score analysis completed successfully",
                    data=result.get('data', {}),
                    metadata={'analysis_type': 'composite_score'}
                )
            else:
                return ToolExecutionResult(
                    success=False,
                    message=f"Composite analysis failed: {result.get('message')}",
                    error_details=result.get('error_details')
                )
                
        except Exception as e:
            logger.error(f"Composite analysis failed: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"Composite analysis failed: {str(e)}",
                error_details=str(e)
            )


class RunPCAAnalysisInput(BaseModel):
    """Input for PCA analysis only"""
    session_id: str = Field(..., description="Session identifier for data access")


class RunPCAAnalysis(DataAnalysisTool):
    """Run PCA analysis only"""
    
    name: str = "run_pca_analysis"
    description: str = "Run Principal Component Analysis (PCA) for malaria risk assessment"
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute PCA analysis"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute PCA analysis"""
        try:
            session_id = kwargs.get('session_id')
            
            from ..analysis.engine import AnalysisEngine
            from ..models.data_handler import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            result = analysis_engine.run_pca_analysis(session_id)
            
            if result.get('status') == 'success':
                return ToolExecutionResult(
                    success=True,
                    message="PCA analysis completed successfully",
                    data=result.get('data', {}),
                    metadata={'analysis_type': 'pca'}
                )
            else:
                return ToolExecutionResult(
                    success=False,
                    message=f"PCA analysis failed: {result.get('message')}",
                    error_details=result.get('error_details')
                )
                
        except Exception as e:
            logger.error(f"PCA analysis failed: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"PCA analysis failed: {str(e)}",
                error_details=str(e)
            )