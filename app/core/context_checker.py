"""
Context Checker for ChatMRPT

This module validates actions against current context and generates helpful
guidance when actions are not possible.
"""

import logging
from typing import Dict, List, Tuple, Callable
from .session_state import SessionState, WorkflowStage, DataState, AnalysisState

logger = logging.getLogger(__name__)


class ContextChecker:
    """Validates actions against current context and provides helpful guidance"""
    
    def __init__(self):
        """Initialize context checker with action requirements"""
        self._initialize_action_requirements()
    
    def _initialize_action_requirements(self):
        """Define requirements for each action"""
        self.action_requirements: Dict[str, Dict] = {
            # File upload actions
            'upload_csv': {
                'check': lambda s: s.workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD, WorkflowStage.DATA_VALIDATION],
                'error_message': "CSV files can be uploaded at any time to start or replace your data.",
                'guidance': "Go ahead and upload your CSV file! I'll help you validate it once it's uploaded.",
                'prerequisites': [],
                'next_steps': ["Upload shapefile", "Review data summary"]
            },
            
            'upload_shapefile': {
                'check': lambda s: s.workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD, WorkflowStage.DATA_VALIDATION],
                'error_message': "Shapefile can be uploaded at any time to provide geographic boundaries.",
                'guidance': "Please upload your shapefile! This provides the geographic boundaries for the analysis.",
                'prerequisites': [],
                'next_steps': ["Upload CSV if not done", "Start analysis if both files uploaded"]
            },
            
            # Analysis actions
            'start_analysis': {
                'check': lambda s: (s.data_state == DataState.BOTH_LOADED and 
                                  s.analysis_state == AnalysisState.NOT_STARTED),
                'error_message': "Analysis requires both CSV and shapefile data to be uploaded.",
                'guidance': self._get_analysis_guidance,
                'prerequisites': ["Upload CSV file", "Upload shapefile"],
                'next_steps': ["Wait for analysis completion", "Review progress"]
            },
            
            'run_analysis': {  # Alternative name for start_analysis
                'check': lambda s: (s.data_state == DataState.BOTH_LOADED and 
                                  s.analysis_state == AnalysisState.NOT_STARTED),
                'error_message': "Analysis requires both CSV and shapefile data to be uploaded.",
                'guidance': self._get_analysis_guidance,
                'prerequisites': ["Upload CSV file", "Upload shapefile"],
                'next_steps': ["Wait for analysis completion", "Review progress"]
            },
            
            # Visualization actions
            'create_map': {
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Maps can only be created after analysis is complete.",
                'guidance': self._get_map_creation_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Choose map type", "Customize visualization"]
            },
            
            'show_map': {  # Alternative name
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Maps can only be displayed after analysis is complete.",
                'guidance': self._get_map_creation_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Choose map type", "Customize visualization"]
            },
            
            'vulnerability_map': {
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Vulnerability maps require completed analysis results.",
                'guidance': self._get_vulnerability_map_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["View vulnerability rankings", "Generate report"]
            },
            
            # Ranking and results actions
            'view_rankings': {
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Vulnerability rankings are only available after analysis completion.",
                'guidance': self._get_rankings_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Create vulnerability map", "Generate detailed report"]
            },
            
            'show_rankings': {  # Alternative name
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Rankings are only available after analysis completion.",
                'guidance': self._get_rankings_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Create vulnerability map", "Generate detailed report"]
            },
            
            # Report generation
            'generate_report': {
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Reports can only be generated after analysis completion.",
                'guidance': self._get_report_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Download report", "Create additional visualizations"]
            },
            
            'create_report': {  # Alternative name
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Reports require completed analysis results.",
                'guidance': self._get_report_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Download report", "Create additional visualizations"]
            },
            
            # Download actions
            'download_results': {
                'check': lambda s: s.analysis_state == AnalysisState.COMPLETE,
                'error_message': "Results are only available for download after analysis completion.",
                'guidance': self._get_download_guidance,
                'prerequisites': ["Complete malaria risk analysis"],
                'next_steps': ["Choose download format", "Select specific results"]
            },
            
            # Data review actions
            'show_data_summary': {
                'check': lambda s: s.data_state != DataState.NO_DATA,
                'error_message': "No data has been uploaded yet.",
                'guidance': "Please upload your CSV and/or shapefile first, then I can show you a data summary.",
                'prerequisites': ["Upload at least one data file"],
                'next_steps': ["Upload remaining files", "Review data quality"]
            },
            
            'check_data_quality': {
                'check': lambda s: s.data_state == DataState.BOTH_LOADED,
                'error_message': "Data quality check requires both CSV and shapefile to be uploaded.",
                'guidance': self._get_data_quality_guidance,
                'prerequisites': ["Upload CSV file", "Upload shapefile"],
                'next_steps': ["Fix any data issues", "Start analysis"]
            }
        }
    
    def check_prerequisites(self, action: str, state: SessionState) -> Tuple[bool, str]:
        """
        Check if action can be performed in current state
        
        Args:
            action: Action to check
            state: Current session state
            
        Returns:
            Tuple of (can_perform, reason_message)
        """
        # Normalize action name
        action = action.lower().replace('-', '_').replace(' ', '_')
        
        if action not in self.action_requirements:
            # For unknown actions, check for partial matches
            partial_matches = [req_action for req_action in self.action_requirements.keys() 
                             if action in req_action or req_action in action]
            if partial_matches:
                action = partial_matches[0]
            else:
                logger.debug(f"Unknown action: {action}, allowing by default")
                return True, "Action is allowed"
        
        requirement = self.action_requirements[action]
        can_perform = requirement['check'](state)
        
        if can_perform:
            return True, "Action is allowed"
        else:
            return False, requirement['error_message']
    
    def generate_helpful_message(self, action: str, state: SessionState) -> str:
        """
        Generate helpful guidance for blocked actions
        
        Args:
            action: Action that was blocked
            state: Current session state
            
        Returns:
            Helpful guidance message
        """
        action = action.lower().replace('-', '_').replace(' ', '_')
        
        if action not in self.action_requirements:
            # Try partial match again
            partial_matches = [req_action for req_action in self.action_requirements.keys() 
                             if action in req_action or req_action in action]
            if partial_matches:
                action = partial_matches[0]
            else:
                return self._get_generic_guidance(action, state)
        
        requirement = self.action_requirements[action]
        guidance = requirement['guidance']
        
        if callable(guidance):
            return guidance(state)
        else:
            return guidance
    
    def get_missing_prerequisites(self, action: str, state: SessionState) -> List[str]:
        """
        Get list of missing prerequisites for an action
        
        Args:
            action: Action to check
            state: Current session state
            
        Returns:
            List of missing prerequisites
        """
        action = action.lower().replace('-', '_').replace(' ', '_')
        
        if action not in self.action_requirements:
            return []
        
        requirement = self.action_requirements[action]
        can_perform = requirement['check'](state)
        
        if can_perform:
            return []
        else:
            # Return all prerequisites since we don't know which specific ones are missing
            return requirement['prerequisites']
    
    def get_next_steps(self, action: str, state: SessionState) -> List[str]:
        """
        Get suggested next steps for an action
        
        Args:
            action: Action that was checked
            state: Current session state
            
        Returns:
            List of suggested next steps
        """
        action = action.lower().replace('-', '_').replace(' ', '_')
        
        if action not in self.action_requirements:
            return []
        
        return self.action_requirements[action]['next_steps']
    
    # Guidance message generators
    def _get_analysis_guidance(self, state: SessionState) -> str:
        """Generate guidance for starting analysis"""
        if state.data_state == DataState.NO_DATA:
            return """I'd love to help you start the malaria risk analysis, but I don't see any data uploaded yet. 
                     Here's what you need to do:
                     
                     1. **Upload your CSV file** - This should contain your malaria risk variables
                     2. **Upload your shapefile** - This provides the geographic boundaries
                     
                     Once both files are uploaded, I can start the comprehensive risk analysis!"""
        
        elif state.data_state == DataState.CSV_ONLY:
            return """I see you've uploaded your CSV data - great start! To begin the analysis, I also need your shapefile for geographic boundaries. 
                     Please upload your shapefile, then I can start the malaria risk analysis."""
        
        elif state.data_state == DataState.SHAPEFILE_ONLY:
            return """I see you've uploaded your shapefile - that's good! To begin the analysis, I also need your CSV data file with the malaria risk variables. 
                     Please upload your CSV file, then I can start the comprehensive analysis."""
        
        elif state.analysis_state != AnalysisState.NOT_STARTED:
            return """It looks like analysis has already been started or completed. 
                     Would you like me to show you the current results or start a new analysis?"""
        
        else:
            return """Both your files are uploaded, but I'm having trouble starting the analysis. 
                     Let me check your data quality first, then we can proceed."""
    
    def _get_map_creation_guidance(self, state: SessionState) -> str:
        """Generate guidance for map creation"""
        if state.analysis_state == AnalysisState.NOT_STARTED:
            return """I'd love to create maps for you, but I need to analyze your data first! 
                     The malaria risk analysis calculates vulnerability scores that are used to create meaningful maps.
                     
                     Would you like me to start the analysis now?"""
        
        elif state.analysis_state in [AnalysisState.CLEANING, AnalysisState.NORMALIZING, 
                                     AnalysisState.CALCULATING_SCORES, AnalysisState.GENERATING_RANKINGS]:
            return f"""Your analysis is currently in progress ({state.analysis_state.value}). 
                      Maps will be available once the analysis is complete. 
                      
                      Please wait a few more minutes, then I can create various types of maps including vulnerability maps, 
                      variable distribution maps, and composite risk maps."""
        
        elif state.analysis_state == AnalysisState.ERROR:
            return """There was an error during analysis, so I can't create maps yet. 
                     Let me help you resolve the analysis issues first, then we can create beautiful visualizations."""
        
        else:
            return """I need to complete the malaria risk analysis before creating maps. 
                     The analysis generates the vulnerability scores and rankings that make the maps meaningful."""
    
    def _get_vulnerability_map_guidance(self, state: SessionState) -> str:
        """Generate guidance for vulnerability maps specifically"""
        base_guidance = self._get_map_creation_guidance(state)
        
        if state.analysis_state == AnalysisState.COMPLETE:
            return """Your analysis is complete! I can now create a vulnerability map showing which areas have the highest malaria risk. 
                     This map will color-code areas from low to high vulnerability based on the analysis results."""
        
        return base_guidance + "\n\nThe vulnerability map will show which wards have the highest malaria risk based on all analyzed variables."
    
    def _get_rankings_guidance(self, state: SessionState) -> str:
        """Generate guidance for viewing rankings"""
        if state.analysis_state == AnalysisState.NOT_STARTED:
            return """Vulnerability rankings are generated during the malaria risk analysis. 
                     I need to run the analysis first to calculate which areas have the highest risk.
                     
                     Shall I start the analysis now?"""
        
        elif state.analysis_state != AnalysisState.COMPLETE:
            return f"""The analysis is currently running ({state.analysis_state.value}). 
                      Vulnerability rankings will be available once it's complete. 
                      
                      The rankings will show which wards have the highest to lowest malaria risk."""
        
        else:
            return """I need to complete the analysis first to generate vulnerability rankings. 
                     These rankings will show which areas are most at risk for malaria transmission."""
    
    def _get_report_guidance(self, state: SessionState) -> str:
        """Generate guidance for report generation"""
        if state.analysis_state != AnalysisState.COMPLETE:
            return """I can generate comprehensive reports once the malaria risk analysis is complete. 
                     The report will include:
                     
                     - Vulnerability rankings for all areas
                     - Analysis methodology and results  
                     - Visual maps and charts
                     - Recommendations for intervention
                     
                     Would you like me to start the analysis so we can generate your report?"""
        
        else:
            return """Your analysis is complete! I can now generate a comprehensive report with all results, 
                     maps, and recommendations."""
    
    def _get_download_guidance(self, state: SessionState) -> str:
        """Generate guidance for downloading results"""
        if state.analysis_state != AnalysisState.COMPLETE:
            return """Results will be available for download once the analysis is complete. 
                     You'll be able to download:
                     
                     - Vulnerability rankings (CSV)
                     - Analysis results (JSON)
                     - Generated maps (HTML/PNG)
                     - Comprehensive report (PDF)
                     
                     Would you like me to start the analysis?"""
        
        else:
            return """Your analysis is complete! You can now download various result formats."""
    
    def _get_data_quality_guidance(self, state: SessionState) -> str:
        """Generate guidance for data quality checks"""
        if state.data_state == DataState.NO_DATA:
            return """I need data files to check quality. Please upload your CSV and shapefile first."""
        
        elif state.data_state == DataState.CSV_ONLY:
            return """I can check your CSV data quality, but I also need the shapefile to ensure 
                     the geographic areas match between files."""
        
        elif state.data_state == DataState.SHAPEFILE_ONLY:
            return """I can check your shapefile, but I also need the CSV data to ensure 
                     the ward names match and check for data quality issues."""
        
        else:
            return """Great! I have both files and can check data quality including ward name matching 
                     and variable validation."""
    
    def _get_generic_guidance(self, action: str, state: SessionState) -> str:
        """Generate generic guidance for unknown actions"""
        stage_guidance = {
            WorkflowStage.INITIAL: "I'm ready to help! Please start by uploading your CSV data and shapefile.",
            WorkflowStage.DATA_UPLOAD: "I see you're uploading data. Make sure to upload both CSV and shapefile files.",
            WorkflowStage.DATA_VALIDATION: "Your data is uploaded! You can start the analysis or check data quality.",
            WorkflowStage.DATA_READY: "Your data is ready for analysis. Shall I start the malaria risk analysis?",
            WorkflowStage.ANALYSIS_RUNNING: "Analysis is in progress. Please wait for it to complete.",
            WorkflowStage.ANALYSIS_COMPLETE: "Analysis is complete! You can now create maps, view rankings, or generate reports.",
            WorkflowStage.MAPS_AVAILABLE: "You have maps and results available. You can create more visualizations or download results."
        }
        
        base_message = stage_guidance.get(state.workflow_stage, 
                                        "I'm not sure about that specific action, but I'm here to help!")
        
        return f"""I'm not sure about "{action}" specifically, but here's what you can do now:
                
                {base_message}
                
                Feel free to ask me what you can do or how to proceed!""" 