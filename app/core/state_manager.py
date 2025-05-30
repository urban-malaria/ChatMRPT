"""
State Manager for ChatMRPT

This module manages session state transitions, validation, and updates
throughout the user workflow.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from .session_state import (
    SessionState, WorkflowStage, DataState, AnalysisState, UserCapabilities,
    FileInfo, DataSummary, AnalysisResults,
    is_data_ready_for_analysis, is_analysis_complete, can_create_visualizations
)

logger = logging.getLogger(__name__)


class StateManager:
    """Manages session state and workflow progression"""
    
    def __init__(self, session_dict: Dict[str, Any]):
        """
        Initialize state manager with Flask session dictionary
        
        Args:
            session_dict: Flask session dictionary
        """
        self.session = session_dict
        self._state_cache = None
    
    def get_current_state(self) -> SessionState:
        """
        Get current session state, creating default if none exists
        
        Returns:
            SessionState: Current session state
        """
        if self._state_cache is None:
            state_data = self.session.get('enhanced_state', {})
            if state_data:
                try:
                    self._state_cache = SessionState.from_dict(state_data)
                except Exception as e:
                    logger.warning(f"Error loading session state, creating new: {e}")
                    self._state_cache = self._create_default_state()
            else:
                self._state_cache = self._create_default_state()
                
        return self._state_cache
    
    def _create_default_state(self) -> SessionState:
        """Create default session state based on existing session data"""
        session_id = self.session.get('session_id')
        
        # Determine current workflow stage based on existing session data
        workflow_stage = WorkflowStage.INITIAL
        data_state = DataState.NO_DATA
        analysis_state = AnalysisState.NOT_STARTED
        
        # Check what's already loaded
        csv_loaded = self.session.get('csv_loaded', False)
        shapefile_loaded = self.session.get('shapefile_loaded', False)
        analysis_complete = self.session.get('analysis_complete', False)
        
        if csv_loaded and shapefile_loaded:
            data_state = DataState.BOTH_LOADED
            workflow_stage = WorkflowStage.DATA_READY
        elif csv_loaded:
            data_state = DataState.CSV_ONLY
            workflow_stage = WorkflowStage.DATA_UPLOAD
        elif shapefile_loaded:
            data_state = DataState.SHAPEFILE_ONLY
            workflow_stage = WorkflowStage.DATA_UPLOAD
        
        if analysis_complete:
            analysis_state = AnalysisState.COMPLETE
            workflow_stage = WorkflowStage.ANALYSIS_COMPLETE
        
        # Create data summary from existing session data
        data_summary = DataSummary(
            available_variables=self.session.get('available_variables', []),
            ward_count=self.session.get('csv_rows', 0)
        )
        
        # Set capabilities based on current state
        capabilities = self._calculate_capabilities(workflow_stage, data_state, analysis_state)
        
        state = SessionState(
            workflow_stage=workflow_stage,
            data_state=data_state,
            analysis_state=analysis_state,
            data_summary=data_summary,
            capabilities=capabilities,
            session_id=session_id,
            next_suggestions=self._get_initial_suggestions(workflow_stage),
            help_topics=self._get_help_topics(workflow_stage)
        )
        
        # Save the initial state
        self._save_state(state)
        return state
    
    def update_state(self, **updates) -> SessionState:
        """
        Update session state with validation
        
        Args:
            **updates: State updates to apply
            
        Returns:
            SessionState: Updated state
        """
        current_state = self.get_current_state()
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(current_state, key):
                setattr(current_state, key, value)
                logger.debug(f"Updated {key} to {value}")
            else:
                logger.warning(f"Attempted to update unknown state field: {key}")
        
        # Update capabilities based on new state
        current_state.capabilities = self._calculate_capabilities(
            current_state.workflow_stage,
            current_state.data_state,
            current_state.analysis_state
        )
        
        # Update suggestions and help topics
        current_state.next_suggestions = self._get_next_suggestions(current_state)
        current_state.help_topics = self._get_help_topics(current_state.workflow_stage)
        
        # Update timestamp
        current_state.update_timestamp()
        
        # Save updated state
        self._save_state(current_state)
        
        # Update cache
        self._state_cache = current_state
        
        logger.info(f"State updated: {current_state.workflow_stage.value} - {current_state.data_state.value}")
        return current_state
    
    def transition_workflow_stage(self, new_stage: WorkflowStage, **context) -> SessionState:
        """
        Transition to a new workflow stage with validation
        
        Args:
            new_stage: Target workflow stage
            **context: Additional context for the transition
            
        Returns:
            SessionState: Updated state
        """
        current_state = self.get_current_state()
        
        # Validate transition is allowed
        if self._is_valid_transition(current_state.workflow_stage, new_stage):
            return self.update_state(
                workflow_stage=new_stage,
                last_action=f"transition_to_{new_stage.value}",
                last_action_time=datetime.now(),
                **context
            )
        else:
            logger.warning(f"Invalid workflow transition: {current_state.workflow_stage.value} -> {new_stage.value}")
            return current_state
    
    def update_data_info(self, file_type: str, file_info: Dict[str, Any]) -> SessionState:
        """
        Update data information when files are uploaded
        
        Args:
            file_type: 'csv' or 'shapefile'
            file_info: File information dictionary
            
        Returns:
            SessionState: Updated state
        """
        current_state = self.get_current_state()
        
        # Create FileInfo object
        file_obj = FileInfo(
            filename=file_info.get('filename', ''),
            size=file_info.get('size', 0),
            upload_time=datetime.now(),
            rows=file_info.get('rows'),
            columns=file_info.get('columns'),
            features=file_info.get('features')
        )
        
        # Update data summary
        data_summary = current_state.data_summary
        if file_type == 'csv':
            data_summary.csv_info = file_obj
            data_summary.available_variables = file_info.get('variables', [])
            data_summary.ward_count = file_info.get('rows', 0)
        elif file_type == 'shapefile':
            data_summary.shapefile_info = file_obj
        
        # Determine new data state
        new_data_state = DataState.NO_DATA
        if data_summary.csv_info and data_summary.shapefile_info:
            new_data_state = DataState.BOTH_LOADED
        elif data_summary.csv_info:
            new_data_state = DataState.CSV_ONLY
        elif data_summary.shapefile_info:
            new_data_state = DataState.SHAPEFILE_ONLY
        
        # Determine new workflow stage
        new_workflow_stage = WorkflowStage.DATA_UPLOAD
        if new_data_state == DataState.BOTH_LOADED:
            new_workflow_stage = WorkflowStage.DATA_VALIDATION
        
        return self.update_state(
            data_state=new_data_state,
            workflow_stage=new_workflow_stage,
            data_summary=data_summary,
            last_action=f"upload_{file_type}",
            last_action_time=datetime.now()
        )
    
    def update_analysis_progress(self, analysis_state: AnalysisState, **context) -> SessionState:
        """
        Update analysis progress
        
        Args:
            analysis_state: New analysis state
            **context: Additional context (results, errors, etc.)
            
        Returns:
            SessionState: Updated state
        """
        updates = {
            'analysis_state': analysis_state,
            'last_action': f"analysis_{analysis_state.value}",
            'last_action_time': datetime.now()
        }
        
        # Update workflow stage based on analysis state
        if analysis_state == AnalysisState.COMPLETE:
            updates['workflow_stage'] = WorkflowStage.ANALYSIS_COMPLETE
        elif analysis_state in [AnalysisState.CLEANING, AnalysisState.NORMALIZING, 
                               AnalysisState.CALCULATING_SCORES, AnalysisState.GENERATING_RANKINGS]:
            updates['workflow_stage'] = WorkflowStage.ANALYSIS_RUNNING
        
        # Update analysis results if provided
        if 'results' in context:
            current_state = self.get_current_state()
            analysis_results = current_state.analysis_results
            analysis_results.composite_scores = context['results'].get('composite_scores')
            analysis_results.vulnerability_rankings = context['results'].get('vulnerability_rankings')
            analysis_results.normalized_data = context['results'].get('normalized_data')
            analysis_results.analysis_metadata = context['results'].get('metadata')
            analysis_results.completed_steps = context['results'].get('completed_steps', [])
            updates['analysis_results'] = analysis_results
        
        return self.update_state(**updates)
    
    def can_perform_action(self, action: str) -> Tuple[bool, str]:
        """
        Check if action is possible in current state
        
        Args:
            action: Action to check
            
        Returns:
            Tuple of (can_perform, reason_if_not)
        """
        current_state = self.get_current_state()
        
        action_requirements = {
            'upload_csv': lambda s: (s.workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD], 
                                   "CSV can be uploaded at any time"),
            'upload_shapefile': lambda s: (s.workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD], 
                                         "Shapefile can be uploaded at any time"),
            'start_analysis': lambda s: (s.data_state == DataState.BOTH_LOADED and s.analysis_state == AnalysisState.NOT_STARTED,
                                       "Both CSV and shapefile must be uploaded before starting analysis"),
            'create_map': lambda s: (s.analysis_state == AnalysisState.COMPLETE,
                                   "Analysis must be completed before creating maps"),
            'view_rankings': lambda s: (s.analysis_state == AnalysisState.COMPLETE,
                                      "Analysis must be completed to view vulnerability rankings"),
            'generate_report': lambda s: (s.analysis_state == AnalysisState.COMPLETE,
                                        "Analysis must be completed to generate reports"),
            'download_results': lambda s: (s.analysis_state == AnalysisState.COMPLETE,
                                         "Analysis must be completed to download results")
        }
        
        if action in action_requirements:
            can_do, reason = action_requirements[action](current_state)
            return can_do, reason if not can_do else "Action is allowed"
        
        return True, "Unknown action, assuming allowed"
    
    def get_next_suggestions(self) -> List[str]:
        """Get contextual next step suggestions"""
        current_state = self.get_current_state()
        return self._get_next_suggestions(current_state)
    
    def get_available_capabilities(self) -> UserCapabilities:
        """Get what user can currently do"""
        current_state = self.get_current_state()
        return current_state.capabilities
    
    def _calculate_capabilities(self, workflow_stage: WorkflowStage, 
                              data_state: DataState, analysis_state: AnalysisState) -> UserCapabilities:
        """Calculate user capabilities based on current state"""
        return UserCapabilities(
            can_upload_csv=workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD],
            can_upload_shapefile=workflow_stage in [WorkflowStage.INITIAL, WorkflowStage.DATA_UPLOAD],
            can_start_analysis=data_state == DataState.BOTH_LOADED and analysis_state == AnalysisState.NOT_STARTED,
            can_create_maps=analysis_state == AnalysisState.COMPLETE,
            can_view_rankings=analysis_state == AnalysisState.COMPLETE,
            can_generate_reports=analysis_state == AnalysisState.COMPLETE,
            can_download_results=analysis_state == AnalysisState.COMPLETE
        )
    
    def _get_next_suggestions(self, state: SessionState) -> List[str]:
        """Get contextual suggestions based on current state"""
        suggestions = []
        
        if state.workflow_stage == WorkflowStage.INITIAL:
            suggestions = [
                "Upload your CSV data file",
                "Upload your shapefile for geographic boundaries",
                "Ask me what file formats I accept"
            ]
        elif state.workflow_stage == WorkflowStage.DATA_UPLOAD:
            if state.data_state == DataState.CSV_ONLY:
                suggestions = [
                    "Upload your shapefile to complete data setup",
                    "Check your CSV data summary"
                ]
            elif state.data_state == DataState.SHAPEFILE_ONLY:
                suggestions = [
                    "Upload your CSV data file",
                    "Check your shapefile information"
                ]
        elif state.workflow_stage == WorkflowStage.DATA_VALIDATION:
            suggestions = [
                "Start the malaria risk analysis",
                "Review your data summary",
                "Check for any data quality issues"
            ]
        elif state.workflow_stage == WorkflowStage.ANALYSIS_COMPLETE:
            suggestions = [
                "Create a vulnerability map",
                "View the vulnerability rankings",
                "Generate a comprehensive report",
                "Create variable distribution maps"
            ]
        elif state.workflow_stage == WorkflowStage.MAPS_AVAILABLE:
            suggestions = [
                "Create additional maps",
                "Download your results",
                "Generate a final report",
                "Explore different visualization options"
            ]
        
        return suggestions
    
    def _get_help_topics(self, workflow_stage: WorkflowStage) -> List[str]:
        """Get relevant help topics for current stage"""
        topics = {
            WorkflowStage.INITIAL: [
                "What file formats do you accept?",
                "How does malaria risk analysis work?",
                "What data do I need?"
            ],
            WorkflowStage.DATA_UPLOAD: [
                "What should my CSV file contain?",
                "What is a shapefile?",
                "How do I check my data quality?"
            ],
            WorkflowStage.DATA_VALIDATION: [
                "How long does analysis take?",
                "What variables are used in analysis?",
                "Can I customize the analysis?"
            ],
            WorkflowStage.ANALYSIS_COMPLETE: [
                "How do I interpret the results?",
                "What do the vulnerability rankings mean?",
                "What types of maps can I create?"
            ]
        }
        return topics.get(workflow_stage, ["Ask me anything about malaria risk analysis"])
    
    def _get_initial_suggestions(self, workflow_stage: WorkflowStage) -> List[str]:
        """Get initial suggestions for a given workflow stage"""
        return self._get_next_suggestions(SessionState(workflow_stage=workflow_stage))
    
    def _is_valid_transition(self, current: WorkflowStage, target: WorkflowStage) -> bool:
        """Check if workflow stage transition is valid"""
        # Define allowed transitions
        allowed_transitions = {
            WorkflowStage.INITIAL: [WorkflowStage.DATA_UPLOAD],
            WorkflowStage.DATA_UPLOAD: [WorkflowStage.DATA_VALIDATION, WorkflowStage.DATA_READY],
            WorkflowStage.DATA_VALIDATION: [WorkflowStage.DATA_READY, WorkflowStage.DATA_UPLOAD],
            WorkflowStage.DATA_READY: [WorkflowStage.ANALYSIS_RUNNING],
            WorkflowStage.ANALYSIS_RUNNING: [WorkflowStage.ANALYSIS_COMPLETE, WorkflowStage.DATA_READY],
            WorkflowStage.ANALYSIS_COMPLETE: [WorkflowStage.MAPS_AVAILABLE],
            WorkflowStage.MAPS_AVAILABLE: [WorkflowStage.ANALYSIS_COMPLETE]  # Can regenerate
        }
        
        return target in allowed_transitions.get(current, [])
    
    def _save_state(self, state: SessionState):
        """Save state to session storage"""
        try:
            self.session['enhanced_state'] = state.to_dict()
            logger.debug("Session state saved successfully")
        except Exception as e:
            logger.error(f"Error saving session state: {e}")
    
    def clear_state(self):
        """Clear session state"""
        if 'enhanced_state' in self.session:
            del self.session['enhanced_state']
        self._state_cache = None
        logger.info("Session state cleared") 