"""
Analysis Engine for ChatMRPT

Simple wrapper class that provides a consistent interface for analysis operations.
Enhanced with DUAL-METHOD settlement integration for both composite scoring and PCA.
"""

import logging
from typing import Dict, Any, Optional, List
from .pipeline import run_full_analysis_pipeline
from .pca_pipeline import run_independent_pca_analysis
from .metadata import AnalysisMetadata

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """
    Main analysis engine that coordinates different analysis workflows.
    
    Enhanced with settlement integration for BOTH composite scoring AND PCA analysis.
    This ensures users get the best of both worlds with settlement-enhanced vulnerability rankings.
    """
    
    def __init__(self, data_handler=None):
        """Initialize the analysis engine."""
        self.logger = logger
        self.data_handler = data_handler
        
    def run_composite_analysis(self, variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run composite scoring analysis with SETTLEMENT INTEGRATION.
        
        This method enhances composite scoring with settlement data automatically.
        
        Args:
            variables: Optional list of variables to use (auto-selected if None)
            
        Returns:
            Dictionary with analysis results enhanced by settlement data
        """
        try:
            logger.info("🏘️ COMPOSITE + SETTLEMENT: Starting composite analysis with settlement integration")
            
            # STEP 1: Integrate settlement data before analysis
            settlement_result = self._integrate_settlement_data('composite')
            
            # STEP 2: Run composite analysis (settlement variables now included)
            result = run_full_analysis_pipeline(
                data_handler=self.data_handler,
                selected_variables=variables
            )
            
            if result.get('status') == 'error':
                return result
            
            # STEP 3: Enhance results with settlement context
            enhanced_result = self._enhance_composite_results_with_settlement(result, settlement_result)
            
            return {
                'status': 'success',
                'message': 'Composite analysis completed successfully with settlement enhancement.',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'composite_scoring_with_settlements',
                'results': enhanced_result,
                'settlement_integration': settlement_result
            }
            
        except Exception as e:
            logger.error(f"Composite analysis with settlements failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Composite analysis failed: {str(e)}',
                'variables_used': variables or [],
                'analysis_type': 'composite_scoring_with_settlements'
            }
    
    def run_pca_analysis(self, variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run PCA analysis with SETTLEMENT INTEGRATION.
        
        This method enhances PCA analysis with settlement data automatically.
        
        Args:
            variables: Optional list of variables to use
            
        Returns:
            Dictionary with PCA analysis results enhanced by settlement data
        """
        try:
            logger.info("🏘️ PCA + SETTLEMENT: Starting PCA analysis with settlement integration")
            
            # STEP 1: Integrate settlement data before analysis
            settlement_result = self._integrate_settlement_data('pca')
            
            # STEP 2: Run PCA analysis (settlement variables now included in variable space)
            result = run_independent_pca_analysis(
                data_handler=self.data_handler,
                selected_variables=variables
            )
            
            if result.get('status') == 'error':
                return result
            
            # STEP 3: Enhance results with settlement context
            enhanced_result = self._enhance_pca_results_with_settlement(result, settlement_result)
            
            return {
                'status': 'success',
                'message': 'PCA analysis completed successfully with settlement enhancement.',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'pca_analysis_with_settlements',
                'results': enhanced_result,
                'settlement_integration': settlement_result,
                'n_components': result.get('n_components', 0),
                'variance_explained': result.get('variance_explained', 0)
            }
            
        except Exception as e:
            logger.error(f"PCA analysis with settlements failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'PCA analysis failed: {str(e)}',
                'variables_used': variables or [],
                'analysis_type': 'pca_analysis_with_settlements'
            }
        
    def _integrate_settlement_data(self, analysis_type: str) -> Dict[str, Any]:
        """
        Integrate settlement data before running analysis.
        
        This method is called by both composite and PCA analysis methods.
        """
        try:
            # Import settlement tools
            try:
                from ..tools.settlement_tools import integrate_settlement_data_unified
            except ImportError:
                from app.tools.settlement_tools import integrate_settlement_data_unified
            
            # Get session ID from data handler
            session_id = getattr(self.data_handler, 'session_id', 'default_session')
            
            # Integrate settlement data
            settlement_result = integrate_settlement_data_unified(session_id, self.data_handler)
            
            if settlement_result['status'] == 'success':
                logger.info(f"✅ SETTLEMENT INTEGRATION: Added {len(settlement_result.get('settlement_variables_added', []))} settlement variables for {analysis_type} analysis")
            else:
                logger.warning(f"⚠️ SETTLEMENT INTEGRATION: {settlement_result.get('message', 'Failed')}")
            
            return settlement_result
            
        except Exception as e:
            logger.warning(f"Settlement integration failed: {e}")
            return {
                'status': 'warning',
                'message': f'Settlement integration failed: {str(e)}. Analysis will proceed without settlement enhancement.',
                'settlement_variables_added': []
            }
    
    def _enhance_composite_results_with_settlement(self, analysis_result: Dict[str, Any], 
                                                  settlement_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance composite analysis results with settlement context."""
        try:
            enhanced_result = analysis_result.copy()
            
            # Add settlement-enhanced insights
            settlement_variables = settlement_result.get('settlement_variables_added', [])
            
            if settlement_variables:
                enhanced_result['settlement_enhanced'] = True
                enhanced_result['settlement_variables_included'] = settlement_variables
                enhanced_result['key_insights'] = [
                    f'Composite scores enhanced with {len(settlement_variables)} settlement variables',
                    'Informal settlement percentages increase vulnerability rankings',
                    'Building density and settlement compactness affect malaria transmission risk',
                    'Settlement morphology patterns captured in vulnerability scoring'
                ]
                
                # Add settlement-specific analysis summary
                enhanced_result['settlement_analysis_summary'] = {
                    'total_settlement_footprints': settlement_result.get('settlement_footprints', 0),
                    'settlement_variables_added': len(settlement_variables),
                    'analysis_enhancement': 'Composite scores now reflect both environmental AND settlement-based risk factors'
                }
            else:
                enhanced_result['settlement_enhanced'] = False
                enhanced_result['key_insights'] = [
                    'Composite scores calculated using available environmental and demographic variables',
                    'Settlement data not available - consider uploading settlement shapefiles for enhanced analysis'
                ]
            
            return enhanced_result
            
        except Exception as e:
            logger.error(f"Error enhancing composite results: {e}")
            return analysis_result
    
    def _enhance_pca_results_with_settlement(self, analysis_result: Dict[str, Any], 
                                           settlement_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance PCA analysis results with settlement context."""
        try:
            enhanced_result = analysis_result.copy()
            
            # Add settlement-enhanced insights
            settlement_variables = settlement_result.get('settlement_variables_added', [])
            
            if settlement_variables:
                enhanced_result['settlement_enhanced'] = True
                enhanced_result['settlement_variables_included'] = settlement_variables
                enhanced_result['key_insights'] = [
                    f'PCA analysis enhanced with {len(settlement_variables)} settlement variables',
                    'Principal components may capture settlement-environment risk interactions',
                    'Settlement morphology patterns integrated into dimensionality reduction',
                    'PCA rankings incorporate building footprint analysis for vulnerability assessment'
                ]
                
                # Add settlement-specific analysis summary
                enhanced_result['settlement_analysis_summary'] = {
                    'total_settlement_footprints': settlement_result.get('settlement_footprints', 0),
                    'settlement_variables_added': len(settlement_variables),
                    'pca_enhancement': 'Settlement variables added to PCA variable space for comprehensive pattern detection'
                }
            else:
                enhanced_result['settlement_enhanced'] = False
                enhanced_result['key_insights'] = [
                    'PCA analysis applied to available environmental and demographic variables',
                    'Settlement data not available - consider uploading settlement shapefiles for enhanced analysis'
                ]
            
            return enhanced_result
            
        except Exception as e:
            logger.error(f"Error enhancing PCA results: {e}")
            return analysis_result

    # ================= EXISTING METHODS (unchanged) =================

    def run_standard_analysis(self, data_handler, session_id: str = None) -> Dict[str, Any]:
        """
        Run standard composite analysis workflow.
        
        Args:
            data_handler: DataHandler instance with loaded data
            session_id: Session identifier
            
        Returns:
            Dictionary with analysis results
        """
        try:
            result = run_full_analysis_pipeline(
                data_handler=data_handler,
                session_id=session_id
            )
            
            return {
                'status': 'success',
                'message': 'Standard analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'analysis_type': 'composite_scoring',
                'results': result
            }
            
        except Exception as e:
            logger.error(f"Standard analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': [],
                'analysis_type': 'composite_scoring'
            }
    
    def run_custom_analysis(self, data_handler, selected_variables: List[str], 
                          session_id: str = None) -> Dict[str, Any]:
        """
        Run analysis with custom variable selection.
        
        Args:
            data_handler: DataHandler instance with loaded data
            selected_variables: List of variables to use in analysis
            session_id: Session identifier
            
        Returns:
            Dictionary with analysis results
        """
        try:
            result = run_full_analysis_pipeline(
                data_handler=data_handler,
                selected_variables=selected_variables,
                session_id=session_id
            )
            
            return {
                'status': 'success',
                'message': 'Custom analysis completed successfully',
                'variables_used': selected_variables,
                'analysis_type': 'custom_composite_scoring',
                'results': result
            }
            
        except Exception as e:
            logger.error(f"Custom analysis failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Analysis failed: {str(e)}',
                'variables_used': selected_variables,
                'analysis_type': 'custom_composite_scoring'
            }
    
    def explain_variable_selection(self, variables: List[str], data_handler) -> Dict[str, Any]:
        """
        Generate explanation for variable selection.
        
        Args:
            variables: List of variables used in analysis
            data_handler: DataHandler instance
            
        Returns:
            Dictionary with explanation results
        """
        try:
            # Simple explanation based on variable types and data
            explanations = {}
            
            for var in variables:
                if hasattr(data_handler, 'df') and data_handler.df is not None:
                    if var in data_handler.df.columns:
                        explanations[var] = f"Variable '{var}' selected for malaria risk analysis"
                    else:
                        explanations[var] = f"Variable '{var}' not found in dataset"
                else:
                    explanations[var] = f"Variable '{var}' selected for analysis"
            
            return {
                'status': 'success',
                'explanations': explanations,
                'message': 'Variable explanations generated'
            }
            
        except Exception as e:
            logger.error(f"Variable explanation failed: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to generate explanations: {str(e)}',
                'explanations': {}
            } 