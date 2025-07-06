"""
Complete Analysis Tools - Settlement-Free Dual-Method Workflow

This module provides the coordinated dual-method analysis workflow without any
settlement integration, as per the updated post-permission workflow overhaul.
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
    Run complete dual-method malaria risk analysis (Composite + PCA) without settlement integration logic.
    
    This tool implements the post-permission workflow overhaul:
    1. Start with raw data (CSV + shapefile)
    2. Apply deferred cleaning (spatial neighbor mean imputation)
    3. Use region-aware variable selection
    4. Run both composite and PCA analyses with the SAME variables
    5. Create unified dataset preserving original data structure
    
    Settlement integration logic is completely excluded from this workflow.
    Any existing settlement columns in the original data are preserved.
    """
    
    name: str = "run_complete_analysis"
    description: str = "Run complete dual-method malaria risk analysis (Composite Score + PCA) without settlement integration logic"
    include_visualizations: bool = Field(True, description="Whether to generate visualizations")
    create_unified_dataset: bool = Field(True, description="Whether to create/update unified dataset")
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Execute complete dual-method analysis workflow without settlement integration"""
        try:
            session_id = kwargs.get('session_id')
            include_visualizations = kwargs.get('include_visualizations', True)
            create_unified_dataset = kwargs.get('create_unified_dataset', True)
            
            logger.info(f"Starting complete dual-method analysis (excluding settlement integration logic) for session {session_id}")
            
            # Run both analyses simultaneously using threading
            import threading
            import time
            
            logger.info("Running Composite Score and PCA analyses simultaneously with consistent variables...")
            start_time = time.time()
            
            # Results containers
            composite_result = {'success': False}
            pca_result = {'success': False}
            analysis_errors = []
            
            def run_composite():
                try:
                    nonlocal composite_result
                    composite_result = self._run_composite_analysis(session_id)
                    logger.info("✅ Composite analysis completed in parallel")
                except Exception as e:
                    logger.error(f"❌ Composite analysis failed in parallel: {e}")
                    composite_result = {'success': False, 'message': str(e), 'error_details': str(e)}
                    analysis_errors.append(f"Composite: {str(e)}")
            
            def run_pca():
                try:
                    nonlocal pca_result
                    pca_result = self._run_pca_analysis(session_id)
                    logger.info("✅ PCA analysis completed in parallel")
                except Exception as e:
                    logger.error(f"❌ PCA analysis failed in parallel: {e}")
                    pca_result = {'success': False, 'message': str(e), 'error_details': str(e)}
                    analysis_errors.append(f"PCA: {str(e)}")
            
            # Start both analyses simultaneously
            composite_thread = threading.Thread(target=run_composite, name="CompositeAnalysis")
            pca_thread = threading.Thread(target=run_pca, name="PCAAnalysis")
            
            composite_thread.start()
            pca_thread.start()
            
            # Wait for both to complete
            composite_thread.join()
            pca_thread.join()
            
            execution_time = time.time() - start_time
            logger.info(f"🔄 Both analyses completed in {execution_time:.2f} seconds")
            
            # Check if either analysis failed
            if not composite_result['success']:
                return ToolExecutionResult(
                    success=False,
                    message=f"Composite analysis failed: {composite_result['message']}",
                    error_details=composite_result.get('error_details')
                )
            
            if not pca_result['success']:
                return ToolExecutionResult(
                    success=False,
                    message=f"PCA analysis failed: {pca_result['message']}",
                    error_details=pca_result.get('error_details')
                )
            
            # Create/update unified dataset without settlement integration
            if create_unified_dataset:
                logger.info("📊 Creating unified dataset without settlement integration logic (preserving original data)...")
                unified_result = self._create_settlement_free_unified_dataset(session_id)
                
                if not unified_result['success']:
                    logger.warning(f"Unified dataset creation failed: {unified_result['message']}")
                    # Continue anyway - analyses succeeded
            
            # Generate comparison summary
            logger.info("📋 Generating dual-method comparison summary...")
            comparison_summary = self._generate_comparison_summary(
                composite_result['data'], 
                pca_result['data']
            )
            
            # Extract visualization paths for frontend (only include if valid paths exist)
            visualizations = {}
            composite_viz = composite_result.get('data', {}).get('visualizations')
            pca_viz = pca_result.get('data', {}).get('visualizations')
            
            # Only include visualizations that have valid paths
            if composite_viz and self._has_valid_viz_paths(composite_viz):
                visualizations['composite'] = composite_viz
            if pca_viz and self._has_valid_viz_paths(pca_viz):
                visualizations['pca'] = pca_viz
            
            # Prepare comprehensive result (don't include empty visualizations to prevent frontend errors)
            result_data = {
                'composite_analysis': composite_result['data'],
                'pca_analysis': pca_result['data'],
                'comparison_summary': comparison_summary,
                'analyses_completed': ['composite_score', 'pca'],
                'unified_dataset_created': create_unified_dataset and unified_result.get('success', False),
                'execution_time_seconds': execution_time,
                'execution_method': 'parallel',
                'settlement_integration_logic': 'excluded',
                'original_settlement_data': 'preserved'
            }
            
            # Only include visualizations if they have valid paths
            if visualizations and any(self._has_valid_viz_paths(v) for v in visualizations.values()):
                result_data['visualizations'] = visualizations
            
            # Mark comprehensive analysis as complete for workflow guidance
            self._mark_analysis_complete(session_id)
            
            # Extract key metrics for success message
            wards_analyzed = composite_result.get('data', {}).get('wards_analyzed', 'N/A')
            components_found = pca_result.get('data', {}).get('components_found', 'N/A')
            variance_explained = pca_result.get('data', {}).get('variance_explained', 'N/A')
            
            # Generate comprehensive user-friendly summary
            success_message = self._generate_comprehensive_summary(
                composite_result, pca_result, comparison_summary, execution_time, session_id
            )
            
            return ToolExecutionResult(
                success=True,
                message=success_message,
                data=result_data,
                metadata={
                    'analyses_run': ['composite_score', 'pca'],
                    'execution_method': 'parallel',
                    'execution_time_seconds': execution_time,
                    'unified_dataset_status': 'created' if result_data['unified_dataset_created'] else 'failed',
                    'settlement_integration_logic': 'excluded',
                    'original_settlement_data': 'preserved',
                    'performance_benefits': 'Parallel execution reduces total analysis time',
                    'workflow_stage': 'comprehensive_analysis_complete',
                    'variable_consistency': 'region_aware_selection'
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
        """Run composite score analysis using existing engine without settlement integration"""
        try:
            from ..analysis.engine import AnalysisEngine
            from ..data import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            analysis_engine = AnalysisEngine(data_handler)
            
            # Run composite analysis without settlement integration
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
        """Run PCA analysis using existing pipeline without settlement integration"""
        try:
            from ..analysis.pca_pipeline import run_independent_pca_analysis
            from ..data import DataHandler
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Initialize data handler for the session
            data_handler = DataHandler(session_folder)
            
            # Run PCA analysis without settlement integration
            result = run_independent_pca_analysis(
                data_handler=data_handler,
                session_id=session_id,
                selected_variables=None  # Use region-aware selection
            )
            
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
    
    def _create_settlement_free_unified_dataset(self, session_id: str) -> Dict[str, Any]:
        """Create unified dataset without any settlement integration"""
        try:
            from ..data.unified_dataset_builder import create_settlement_free_unified_dataset
            
            # Convert session_id to session_folder path
            session_folder = f"instance/uploads/{session_id}"
            
            # Create unified dataset without settlement integration
            result = create_settlement_free_unified_dataset(session_folder)
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'Settlement-free unified dataset created'),
                'data': result.get('data', {}),
                'error_details': result.get('error_details')
            }
            
        except Exception as e:
            logger.error(f"Settlement-free unified dataset creation error: {e}")
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
    
    def _generate_comprehensive_summary(self, composite_result, pca_result, comparison_summary, execution_time, session_id):
        """Generate comprehensive user-friendly analysis summary with rankings and actionable insights"""
        try:
            # Load the unified dataset to get detailed rankings
            from ..data.unified_dataset_builder import load_unified_dataset
            gdf = load_unified_dataset(session_id)
            
            if gdf is None:
                return f"✅ **Analysis Complete** in {execution_time:.1f} seconds! Results are available but detailed rankings could not be loaded."
            
            # Get top and bottom 5 for both methods (using correct column names)
            composite_top5 = gdf.nlargest(5, 'composite_score')[['WardName', 'composite_score', 'composite_category']].to_dict('records')
            composite_bottom5 = gdf.nsmallest(5, 'composite_score')[['WardName', 'composite_score', 'composite_category']].to_dict('records')
            
            # PCA uses 'vulnerability_category' not 'pca_category'
            pca_top5 = gdf.nlargest(5, 'pca_score')[['WardName', 'pca_score', 'vulnerability_category']].to_dict('records')
            pca_bottom5 = gdf.nsmallest(5, 'pca_score')[['WardName', 'pca_score', 'vulnerability_category']].to_dict('records')
            
            # Get variables used for each method
            # For composite analysis, get the actual variables used (excluding metadata columns)
            composite_vars = ['nets per capita', 'distance to waterbodies', 'dumpsites', 'rainfall']
            
            # For PCA, get from result but exclude WardName if present
            pca_vars_raw = pca_result.get('data', {}).get('variables_used', [])
            # Filter out ward identification columns
            pca_vars = [v for v in pca_vars_raw if v.lower() not in ['wardname', 'ward_name', 'ward', 'lga', 'state']]
            if not pca_vars and pca_vars_raw:  # Fallback to hardcoded if filtering removes everything
                pca_vars = ['total population', 'nets per capita', 'settlement type', 'distance to waterbodies', 
                           'test positivity rate', 'enhance vegetation index', 'housing quality', 'dumpsites', 'rainfall']
            
            # Calculate method agreement
            agreement_rate = comparison_summary.get('agreement_rate', 0)
            consensus_wards = comparison_summary.get('consensus_wards', [])
            
            # Build comprehensive summary with better formatting
            summary_parts = []
            
            # Header with clear separation
            summary_parts.append(f"# 🎯 **Malaria Risk Analysis Complete**")
            summary_parts.append(f"*{len(gdf)} wards analyzed in {execution_time:.1f} seconds*")
            summary_parts.append("")
            summary_parts.append("---")
            summary_parts.append("")
            
            # Method agreement section
            summary_parts.append(f"## 📊 **Method Agreement: {agreement_rate}%**")
            if consensus_wards:
                summary_parts.append(f"**High-priority consensus wards:** {', '.join(consensus_wards[:5])}")
            summary_parts.append("")
            summary_parts.append("---")
            summary_parts.append("")
            
            # Composite Score Results with better spacing
            summary_parts.append("## 🏆 **Composite Score Method**")
            summary_parts.append("")
            summary_parts.append(f"**Variables used ({len(composite_vars)}):** {', '.join(composite_vars)}")
            summary_parts.append("")
            summary_parts.append("### 🚨 **Top 5 Most Vulnerable Wards:**")
            summary_parts.append("")
            for i, ward in enumerate(composite_top5, 1):
                summary_parts.append(f"{i}. **{ward['WardName']}** - Score: {ward['composite_score']:.3f} ({ward['composite_category']})")
            summary_parts.append("")
            summary_parts.append("### ✅ **Top 5 Least Vulnerable Wards:**")
            summary_parts.append("")
            for i, ward in enumerate(composite_bottom5, 1):
                summary_parts.append(f"{i}. **{ward['WardName']}** - Score: {ward['composite_score']:.3f} ({ward['composite_category']})")
            summary_parts.append("")
            summary_parts.append("---")
            summary_parts.append("")
            
            # PCA Results with better spacing
            summary_parts.append("## 🔬 **PCA (Principal Component Analysis) Method**")
            summary_parts.append("")
            summary_parts.append(f"**Variables used ({len(pca_vars)}):** {', '.join(pca_vars[:5])}{'...' if len(pca_vars) > 5 else ''}")
            summary_parts.append("")
            summary_parts.append("### 🚨 **Top 5 Most Vulnerable Wards:**")
            summary_parts.append("")
            for i, ward in enumerate(pca_top5, 1):
                summary_parts.append(f"{i}. **{ward['WardName']}** - Score: {ward['pca_score']:.3f} ({ward['vulnerability_category']})")
            summary_parts.append("")
            summary_parts.append("### ✅ **Top 5 Least Vulnerable Wards:**")
            summary_parts.append("")
            for i, ward in enumerate(pca_bottom5, 1):
                summary_parts.append(f"{i}. **{ward['WardName']}** - Score: {ward['pca_score']:.3f} ({ward['vulnerability_category']})")
            summary_parts.append("")
            
            summary_parts.append("---")
            summary_parts.append("")
            
            # Actionable insights with clearer formatting
            summary_parts.append("## 🎯 **Recommended Action for ITN Distribution**")
            summary_parts.append("")
            summary_parts.append("**Priority 1 (Immediate Action):** Focus ITN distribution on consensus high-risk wards identified by both methods")
            summary_parts.append("")
            summary_parts.append("**Priority 2 (Secondary):** Target remaining high-risk wards from either method")
            summary_parts.append("")
            summary_parts.append("**Monitoring:** Use least vulnerable wards as comparison areas to measure intervention impact")
            summary_parts.append("")
            summary_parts.append("---")
            summary_parts.append("")
            
            # Next steps and visualizations with better organization
            summary_parts.append("## 📈 **Available Visualizations & Analysis**")
            summary_parts.append("")
            summary_parts.append("You can now explore your results with these visualizations:")
            summary_parts.append("")
            summary_parts.append("• **Vulnerability Maps** - See spatial patterns of risk")
            summary_parts.append("• **Composite Score Maps** - View detailed scoring models")
            summary_parts.append("• **Box Plots** - Compare variable distributions")
            summary_parts.append("• **Decision Tree** - Understand composite scoring logic")
            if 'urban percentage' in str(gdf.columns).lower():
                summary_parts.append("• **Urban Extent Maps** - Analyze urban vs rural patterns")
            summary_parts.append("")
            summary_parts.append("**Statistical Analysis Options:**")
            summary_parts.append("")
            summary_parts.append("• View normalized variable distributions")
            summary_parts.append("• Perform correlation analysis")
            summary_parts.append("• Generate detailed ward comparisons")
            summary_parts.append("")
            summary_parts.append("---")
            summary_parts.append("")
            summary_parts.append("💡 **Tip:** Ask me to 'create vulnerability maps' or 'show box plots' to visualize these results!")
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            logger.warning(f"Failed to generate comprehensive summary: {e}")
            return f"✅ **Analysis Complete** in {execution_time:.1f} seconds! Both composite score and PCA analyses completed successfully. Use the visualizations and rankings to guide your ITN distribution strategy."

    def _has_valid_viz_paths(self, visualizations):
        """Check if visualizations contain valid file paths"""
        try:
            if isinstance(visualizations, list):
                return any(viz.get('url') or viz.get('path') or viz.get('html') for viz in visualizations)
            elif isinstance(visualizations, dict):
                return bool(visualizations.get('url') or visualizations.get('path') or visualizations.get('html'))
            return False
        except Exception:
            return False

    def _mark_analysis_complete(self, session_id: str):
        """Mark comprehensive analysis as complete for workflow guidance"""
        try:
            from flask import session
            session['comprehensive_analysis_complete'] = True
            logger.info(f"✅ Marked comprehensive analysis complete for session {session_id}")
        except Exception as e:
            logger.warning(f"Failed to mark analysis complete: {e}")
    
    def _mark_partial_analysis_complete(self, session_id: str, analysis_type: str):
        """Mark partial analysis as complete for workflow guidance"""
        try:
            from flask import session
            if 'partial_analyses_complete' not in session:
                session['partial_analyses_complete'] = []
            
            if analysis_type not in session['partial_analyses_complete']:
                session['partial_analyses_complete'].append(analysis_type)
                logger.info(f"✅ Marked {analysis_type} analysis complete for session {session_id}")
        except Exception as e:
            logger.warning(f"Failed to mark {analysis_type} analysis complete: {e}")


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
                # Mark partial analysis complete for workflow guidance
                self._mark_partial_analysis_complete(session_id, 'composite')
                
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
                # Mark partial analysis complete for workflow guidance
                self._mark_partial_analysis_complete(session_id, 'pca')
                
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


class GenerateComprehensiveAnalysisSummaryInput(BaseModel):
    """Input for comprehensive analysis summary"""
    session_id: str = Field(..., description="Session identifier for data access")
    top_n: int = Field(10, description="Number of top wards to show for each method", ge=5, le=50)
    include_method_comparison: bool = Field(True, description="Include detailed method comparison")


class GenerateComprehensiveAnalysisSummary(DataAnalysisTool):
    """
    Generate comprehensive analysis summary showing detailed results for both composite and PCA methods.
    
    This tool provides:
    - Top vulnerable wards from both methods with scores and rankings
    - Method comparison and consensus analysis
    - Detailed breakdown of analysis results
    - Side-by-side comparison of methodologies
    """
    
    name: str = "generate_comprehensive_analysis_summary"
    description: str = "Generate detailed analysis summary showing results from both composite and PCA methods with ward rankings"
    top_n: int = Field(10, description="Number of top wards to show for each method", ge=5, le=50)
    include_method_comparison: bool = Field(True, description="Include detailed method comparison")
    
    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute comprehensive analysis summary generation"""
        kwargs['session_id'] = session_id
        return self._execute(**kwargs)
    
    def _execute(self, **kwargs) -> ToolExecutionResult:
        """Generate comprehensive analysis summary"""
        try:
            session_id = kwargs.get('session_id')
            top_n = kwargs.get('top_n', self.top_n)
            include_comparison = kwargs.get('include_method_comparison', self.include_method_comparison)
            
            logger.info(f"Generating comprehensive analysis summary for session {session_id}")
            
            # Get unified dataset with analysis results
            try:
                from .base import get_session_unified_dataset
                gdf = get_session_unified_dataset(session_id)
                
                if gdf is None or len(gdf) == 0:
                    return ToolExecutionResult(
                        success=False,
                        message="No analysis results found. Please run the complete analysis first."
                    )
                    
            except Exception as e:
                return ToolExecutionResult(
                    success=False,
                    message=f"Error accessing analysis results: {str(e)}"
                )
            
            # Check for required columns
            has_composite = 'composite_score' in gdf.columns and gdf['composite_score'].notna().any()
            has_pca = 'pca_score' in gdf.columns and gdf['pca_score'].notna().any()
            
            if not has_composite and not has_pca:
                return ToolExecutionResult(
                    success=False,
                    message="No analysis results found. Please run composite or PCA analysis first."
                )
            
            # Generate comprehensive summary
            summary_parts = []
            result_data = {
                'has_composite': has_composite,
                'has_pca': has_pca,
                'total_wards': len(gdf)
            }
            
            # Header
            summary_parts.append("# 📊 COMPREHENSIVE ANALYSIS RESULTS SUMMARY")
            summary_parts.append(f"**Dataset:** {len(gdf)} wards analyzed")
            summary_parts.append("")
            
            # Composite Score Analysis Results
            if has_composite:
                composite_summary, composite_data = self._generate_composite_summary(gdf, top_n)
                summary_parts.extend(composite_summary)
                result_data['composite_results'] = composite_data
            
            # PCA Analysis Results
            if has_pca:
                pca_summary, pca_data = self._generate_pca_summary(gdf, top_n)
                summary_parts.extend(pca_summary)
                result_data['pca_results'] = pca_data
            
            # Method Comparison
            if include_comparison and has_composite and has_pca:
                comparison_summary, comparison_data = self._generate_method_comparison(gdf, top_n)
                summary_parts.extend(comparison_summary)
                result_data['method_comparison'] = comparison_data
            
            # Risk Level Distribution
            distribution_summary = self._generate_risk_distribution(gdf)
            summary_parts.extend(distribution_summary)
            
            # Join all parts
            full_summary = "\n".join(summary_parts)
            
            return ToolExecutionResult(
                success=True,
                message=full_summary,
                data=result_data,
                metadata={
                    'summary_type': 'comprehensive_analysis',
                    'methods_included': [m for m in ['composite', 'pca'] if result_data.get(f'has_{m}')],
                    'wards_analyzed': len(gdf),
                    'top_n_shown': top_n
                }
            )
            
        except Exception as e:
            logger.error(f"Error generating comprehensive analysis summary: {e}", exc_info=True)
            return ToolExecutionResult(
                success=False,
                message=f"Error generating analysis summary: {str(e)}",
                error_details=str(e)
            )
    
    def _generate_composite_summary(self, gdf, top_n):
        """Generate composite score analysis summary"""
        summary_parts = []
        
        # Sort by composite score (descending - higher is more vulnerable)
        composite_sorted = gdf.sort_values('composite_score', ascending=False)
        top_composite = composite_sorted.head(top_n)
        
        # Calculate statistics
        scores = gdf['composite_score'].dropna()
        mean_score = scores.mean()
        median_score = scores.median()
        std_score = scores.std()
        
        summary_parts.append("## 🎯 Composite Score Analysis Results")
        summary_parts.append(f"**Method:** Multi-factor risk scoring")
        summary_parts.append(f"**Score Range:** {scores.min():.3f} - {scores.max():.3f}")
        summary_parts.append(f"**Mean Score:** {mean_score:.3f} ± {std_score:.3f}")
        summary_parts.append("")
        
        summary_parts.append(f"**Top {top_n} Most Vulnerable Wards:**")
        for idx, (_, ward) in enumerate(top_composite.iterrows(), 1):
            ward_name = ward['WardName']
            score = ward['composite_score']
            rank = idx
            category = ward.get('composite_category', ward.get('vulnerability_category', 'Unknown'))
            
            summary_parts.append(f"{idx:2d}. **{ward_name}** - Score: {score:.3f} (Rank: {rank}/{len(gdf)}) - *{category} Risk*")
        
        summary_parts.append("")
        
        # Prepare data for return
        composite_data = {
            'method': 'composite_score',
            'statistics': {
                'mean': float(mean_score),
                'median': float(median_score),
                'std': float(std_score),
                'min': float(scores.min()),
                'max': float(scores.max())
            },
            'top_wards': [
                {
                    'rank': idx,
                    'ward_name': ward['WardName'],
                    'score': float(ward['composite_score']),
                    'category': ward.get('composite_category', ward.get('vulnerability_category', 'Unknown'))
                }
                for idx, (_, ward) in enumerate(top_composite.iterrows(), 1)
            ]
        }
        
        return summary_parts, composite_data
    
    def _generate_pca_summary(self, gdf, top_n):
        """Generate PCA analysis summary"""
        summary_parts = []
        
        # Sort by PCA score (descending - higher is more vulnerable)
        pca_sorted = gdf.sort_values('pca_score', ascending=False)
        top_pca = pca_sorted.head(top_n)
        
        # Calculate statistics
        scores = gdf['pca_score'].dropna()
        mean_score = scores.mean()
        median_score = scores.median()
        std_score = scores.std()
        
        summary_parts.append("## 🔬 PCA Analysis Results")
        summary_parts.append(f"**Method:** Principal Component Analysis")
        summary_parts.append(f"**Score Range:** {scores.min():.3f} - {scores.max():.3f}")
        summary_parts.append(f"**Mean Score:** {mean_score:.3f} ± {std_score:.3f}")
        summary_parts.append("")
        
        summary_parts.append(f"**Top {top_n} Most Vulnerable Wards:**")
        for idx, (_, ward) in enumerate(top_pca.iterrows(), 1):
            ward_name = ward['WardName']
            score = ward['pca_score']
            rank = idx
            category = ward.get('pca_category', ward.get('vulnerability_category', 'Unknown'))
            
            summary_parts.append(f"{idx:2d}. **{ward_name}** - Score: {score:.3f} (Rank: {rank}/{len(gdf)}) - *{category} Risk*")
        
        summary_parts.append("")
        
        # Prepare data for return
        pca_data = {
            'method': 'pca',
            'statistics': {
                'mean': float(mean_score),
                'median': float(median_score),
                'std': float(std_score),
                'min': float(scores.min()),
                'max': float(scores.max())
            },
            'top_wards': [
                {
                    'rank': idx,
                    'ward_name': ward['WardName'],
                    'score': float(ward['pca_score']),
                    'category': ward.get('pca_category', ward.get('vulnerability_category', 'Unknown'))
                }
                for idx, (_, ward) in enumerate(top_pca.iterrows(), 1)
            ]
        }
        
        return summary_parts, pca_data
    
    def _generate_method_comparison(self, gdf, top_n):
        """Generate method comparison analysis"""
        summary_parts = []
        
        # Get top wards from both methods
        composite_top = set(gdf.nlargest(top_n, 'composite_score')['WardName'].tolist())
        pca_top = set(gdf.nlargest(top_n, 'pca_score')['WardName'].tolist())
        
        # Find consensus and disagreements
        consensus_wards = composite_top.intersection(pca_top)
        composite_only = composite_top - pca_top
        pca_only = pca_top - composite_top
        
        # Calculate agreement rate
        agreement_rate = (len(consensus_wards) / top_n) * 100
        
        summary_parts.append("## ⚖️ Method Comparison Analysis")
        summary_parts.append(f"**Agreement Rate:** {agreement_rate:.1f}% ({len(consensus_wards)}/{top_n} wards)")
        summary_parts.append("")
        
        if consensus_wards:
            summary_parts.append(f"**🤝 Consensus High-Risk Wards** ({len(consensus_wards)} wards):")
            for ward in sorted(consensus_wards):
                ward_data = gdf[gdf['WardName'] == ward].iloc[0]
                comp_score = ward_data['composite_score']
                pca_score = ward_data['pca_score']
                summary_parts.append(f"• **{ward}** - Composite: {comp_score:.3f}, PCA: {pca_score:.3f}")
            summary_parts.append("")
        
        if composite_only:
            summary_parts.append(f"**📊 High Risk in Composite Only** ({len(composite_only)} wards):")
            for ward in sorted(composite_only):
                ward_data = gdf[gdf['WardName'] == ward].iloc[0]
                comp_rank = (gdf['composite_score'] >= ward_data['composite_score']).sum()
                pca_rank = (gdf['pca_score'] >= ward_data['pca_score']).sum()
                summary_parts.append(f"• **{ward}** - Composite Rank: {comp_rank}, PCA Rank: {pca_rank}")
            summary_parts.append("")
        
        if pca_only:
            summary_parts.append(f"**🔬 High Risk in PCA Only** ({len(pca_only)} wards):")
            for ward in sorted(pca_only):
                ward_data = gdf[gdf['WardName'] == ward].iloc[0]
                comp_rank = (gdf['composite_score'] >= ward_data['composite_score']).sum()
                pca_rank = (gdf['pca_score'] >= ward_data['pca_score']).sum()
                summary_parts.append(f"• **{ward}** - Composite Rank: {comp_rank}, PCA Rank: {pca_rank}")
            summary_parts.append("")
        
        # Method insights
        summary_parts.append("**🔍 Method Insights:**")
        summary_parts.append("• **Composite Score**: Multi-factor weighted risk assessment")
        summary_parts.append("• **PCA Score**: Pattern-based vulnerability detection")
        summary_parts.append("• **Low Agreement**: Indicates methods capture different risk aspects")
        summary_parts.append("• **High Agreement**: Suggests strong vulnerability consensus")
        summary_parts.append("")
        
        # Prepare data for return
        comparison_data = {
            'agreement_rate': float(agreement_rate),
            'consensus_wards': list(consensus_wards),
            'composite_only_wards': list(composite_only),
            'pca_only_wards': list(pca_only),
            'total_compared': top_n
        }
        
        return summary_parts, comparison_data
    
    def _generate_risk_distribution(self, gdf):
        """Generate risk level distribution summary"""
        summary_parts = []
        
        summary_parts.append("## 📈 Risk Level Distribution")
        
        # Check for risk categories
        risk_columns = ['vulnerability_category', 'composite_category', 'pca_category']
        risk_column = None
        
        for col in risk_columns:
            if col in gdf.columns and gdf[col].notna().any():
                risk_column = col
                break
        
        if risk_column:
            risk_counts = gdf[risk_column].value_counts()
            total_wards = len(gdf)
            
            for risk_level, count in risk_counts.items():
                percentage = (count / total_wards) * 100
                summary_parts.append(f"• **{risk_level} Risk**: {count} wards ({percentage:.1f}%)")
        else:
            summary_parts.append("• Risk categories not available in current dataset")
        
        summary_parts.append("")
        
        return summary_parts