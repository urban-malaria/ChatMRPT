"""
Centralized Workflow State Manager
Single source of truth for all workflow state across the application.
Handles state persistence, validation, and transitions.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
from flask import session
from enum import Enum

logger = logging.getLogger(__name__)


class WorkflowSource(Enum):
    """Workflow source types"""
    STANDARD = "standard"
    DATA_ANALYSIS_V3 = "data_analysis_v3"
    TPR = "tpr"
    UNKNOWN = "unknown"


class WorkflowStage(Enum):
    """Workflow stages"""
    INITIAL = "initial"
    UPLOADED = "uploaded"
    DATA_PREPARED = "data_prepared"
    ANALYZING = "analyzing"
    ANALYSIS_COMPLETE = "analysis_complete"
    TRANSITIONING = "transitioning"
    ERROR = "error"


class WorkflowStateManager:
    """
    Centralized state manager for workflow state.
    Ensures consistency across workers and sessions.
    """
    
    VERSION = "1.0.0"
    STATE_FILE = "workflow_state.json"
    
    def __init__(self, session_id: str):
        """Initialize state manager for a session."""
        self.session_id = session_id
        self.session_folder = Path("instance/uploads") / session_id
        self.state_file = self.session_folder / self.STATE_FILE
        self._ensure_folder_exists()
        
    def _ensure_folder_exists(self):
        """Ensure session folder exists."""
        self.session_folder.mkdir(parents=True, exist_ok=True)
        
    def get_state(self) -> Dict[str, Any]:
        """
        Get current workflow state from persistent storage.
        Returns empty state if no state file exists.
        """
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    # Validate version
                    if state.get('version') != self.VERSION:
                        logger.warning(f"State version mismatch for {self.session_id}")
                        return self._create_default_state()
                    return state
            else:
                return self._create_default_state()
        except Exception as e:
            logger.error(f"Error reading state for {self.session_id}: {e}")
            return self._create_default_state()
    
    def _create_default_state(self) -> Dict[str, Any]:
        """Create default state structure."""
        return {
            'version': self.VERSION,
            'session_id': self.session_id,
            'workflow_source': WorkflowSource.UNKNOWN.value,
            'workflow_stage': WorkflowStage.INITIAL.value,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'data_loaded': False,
            'csv_loaded': False,
            'shapefile_loaded': False,
            'analysis_complete': False,
            'tpr_completed': False,
            'workflow_transitioned': False,
            'previous_workflow': None,
            'transitions': [],  # Track state transitions for debugging
            'markers': {},  # Track which marker files exist
            'metadata': {}  # Additional workflow-specific metadata
        }
    
    def update_state(self, updates: Dict[str, Any], transition_reason: str = None) -> bool:
        """
        Update workflow state with validation.
        
        Args:
            updates: Dictionary of state updates
            transition_reason: Optional reason for state change
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            # Get current state
            state = self.get_state()
            
            # Record transition if stage or source changes
            old_source = state.get('workflow_source')
            old_stage = state.get('workflow_stage')
            new_source = updates.get('workflow_source', old_source)
            new_stage = updates.get('workflow_stage', old_stage)
            
            if old_source != new_source or old_stage != new_stage:
                transition = {
                    'timestamp': datetime.now().isoformat(),
                    'from_source': old_source,
                    'to_source': new_source,
                    'from_stage': old_stage,
                    'to_stage': new_stage,
                    'reason': transition_reason or 'Manual update'
                }
                state['transitions'].append(transition)
                logger.info(f"📊 State transition for {self.session_id}: {old_source}/{old_stage} → {new_source}/{new_stage}")
            
            # Apply updates
            for key, value in updates.items():
                state[key] = value
            
            # Update timestamp
            state['updated_at'] = datetime.now().isoformat()
            
            # Persist state
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            # Also update Flask session for immediate availability
            self._sync_to_flask_session(state)
            
            logger.debug(f"✅ State updated for {self.session_id}: {updates}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating state for {self.session_id}: {e}")
            return False
    
    def _sync_to_flask_session(self, state: Dict[str, Any]):
        """Sync state to Flask session for cross-worker consistency."""
        try:
            # Sync key fields to Flask session
            session['workflow_source'] = state.get('workflow_source')
            session['workflow_stage'] = state.get('workflow_stage')
            session['data_loaded'] = state.get('data_loaded')
            session['csv_loaded'] = state.get('csv_loaded')
            session['shapefile_loaded'] = state.get('shapefile_loaded')
            session['analysis_complete'] = state.get('analysis_complete')
            session['tpr_completed'] = state.get('tpr_completed')
            session['workflow_transitioned'] = state.get('workflow_transitioned')
            session.modified = True
        except Exception as e:
            logger.warning(f"Could not sync to Flask session: {e}")
    
    def transition_workflow(self, 
                          from_source: WorkflowSource,
                          to_source: WorkflowSource,
                          new_stage: WorkflowStage = WorkflowStage.INITIAL,
                          clear_markers: List[str] = None) -> bool:
        """
        Handle workflow transition with proper cleanup.
        
        Args:
            from_source: Source workflow transitioning from
            to_source: Target workflow transitioning to
            new_stage: Initial stage in new workflow
            clear_markers: List of marker files to clear
            
        Returns:
            True if transition successful
        """
        try:
            logger.info(f"🔄 Transitioning workflow for {self.session_id}: {from_source.value} → {to_source.value}")
            
            # CRITICAL FIX: Preserve critical flags during transition
            preserved_flags = {}
            if from_source == WorkflowSource.DATA_ANALYSIS_V3:
                # Check for analysis complete marker
                marker_path = self.session_folder / '.analysis_complete'
                if marker_path.exists():
                    preserved_flags['analysis_complete'] = True
                    logger.info(f"📌 Preserving analysis_complete flag during transition")
                
                # Also check current state
                current_state = self.get_state()
                if current_state.get('analysis_complete'):
                    preserved_flags['analysis_complete'] = True
            
            # Clear specified marker files
            if clear_markers:
                for marker in clear_markers:
                    marker_path = self.session_folder / marker
                    if marker_path.exists():
                        try:
                            marker_path.unlink()
                            logger.info(f"🧹 Cleared marker: {marker}")
                        except Exception as e:
                            logger.error(f"Failed to clear marker {marker}: {e}")
            
            # Update state
            updates = {
                'workflow_source': to_source.value,
                'workflow_stage': new_stage.value,
                'previous_workflow': from_source.value,
                'workflow_transitioned': True
            }
            
            # Special handling for specific transitions
            if from_source == WorkflowSource.DATA_ANALYSIS_V3 and to_source == WorkflowSource.STANDARD:
                # TPR to standard workflow transition
                updates['data_loaded'] = True
                updates['csv_loaded'] = True
                updates['tpr_completed'] = True
                # DON'T clear analysis_complete - it might already be done!
                # updates['analysis_complete'] = False  # REMOVED - preserve state!
                
            elif from_source == WorkflowSource.STANDARD and to_source == WorkflowSource.DATA_ANALYSIS_V3:
                # Standard to Data Analysis V3 (new upload)
                updates['analysis_complete'] = False
                updates['tpr_completed'] = False
                updates['workflow_transitioned'] = False
            
            # Apply preserved flags AFTER all other updates
            if preserved_flags:
                updates.update(preserved_flags)
                logger.info(f"✅ Applied preserved flags: {preserved_flags}")
            
            return self.update_state(updates, f"Workflow transition: {from_source.value} → {to_source.value}")
            
        except Exception as e:
            logger.error(f"Error during workflow transition: {e}")
            return False
    
    def check_markers(self) -> Dict[str, bool]:
        """
        Check which marker files exist in session folder.
        Returns dict of marker names and existence status.
        """
        markers = {
            '.analysis_complete': False,
            '.data_analysis_mode': False,
            'agent_state.json': False,
            'workflow_state.json': False
        }
        
        for marker in markers.keys():
            marker_path = self.session_folder / marker
            markers[marker] = marker_path.exists()
        
        return markers
    
    def is_analysis_complete(self) -> bool:
        """
        Check if analysis is truly complete for current workflow.
        Considers workflow context to avoid false positives from stale markers.
        """
        state = self.get_state()
        
        # Only trust analysis_complete flag if in standard workflow
        if state.get('workflow_source') != WorkflowSource.STANDARD.value:
            return False
        
        # Check both state flag and marker file
        analysis_complete = state.get('analysis_complete', False)
        marker_exists = (self.session_folder / '.analysis_complete').exists()
        
        # Both should agree for analysis to be truly complete
        if analysis_complete != marker_exists:
            logger.warning(f"State mismatch for {self.session_id}: flag={analysis_complete}, marker={marker_exists}")
            # If they disagree, trust the state flag (single source of truth)
            if not analysis_complete and marker_exists:
                # Stale marker - remove it
                try:
                    (self.session_folder / '.analysis_complete').unlink()
                    logger.info(f"🧹 Removed stale .analysis_complete marker for {self.session_id}")
                except:
                    pass
        
        return analysis_complete
    
    def is_data_loaded(self) -> bool:
        """Check if data is loaded in current workflow."""
        state = self.get_state()
        return state.get('data_loaded', False) or (
            state.get('csv_loaded', False) and 
            state.get('shapefile_loaded', False)
        )
    
    def get_workflow_info(self) -> Dict[str, Any]:
        """Get summary of current workflow state."""
        state = self.get_state()
        markers = self.check_markers()
        
        return {
            'session_id': self.session_id,
            'workflow_source': state.get('workflow_source'),
            'workflow_stage': state.get('workflow_stage'),
            'data_loaded': self.is_data_loaded(),
            'analysis_complete': self.is_analysis_complete(),
            'tpr_completed': state.get('tpr_completed', False),
            'markers': markers,
            'last_updated': state.get('updated_at'),
            'transitions': len(state.get('transitions', []))
        }
    
    def validate_state(self) -> List[str]:
        """
        Validate current state for inconsistencies.
        Returns list of validation errors/warnings.
        """
        issues = []
        state = self.get_state()
        markers = self.check_markers()
        
        # Check for inconsistent analysis markers
        if markers['.analysis_complete'] and not state.get('analysis_complete'):
            issues.append("Marker file exists but state flag is False")
        
        # Check for data analysis mode marker in wrong workflow
        if markers['.data_analysis_mode'] and state.get('workflow_source') != WorkflowSource.DATA_ANALYSIS_V3.value:
            issues.append("Data analysis marker exists but not in V3 workflow")
        
        # Check for impossible state combinations
        if state.get('analysis_complete') and not self.is_data_loaded():
            issues.append("Analysis marked complete but no data loaded")
        
        # Check for stale transitions
        if state.get('workflow_transitioned') and state.get('workflow_stage') == WorkflowStage.INITIAL.value:
            issues.append("Workflow transitioned but still in initial stage")
        
        return issues
    
    def reset(self):
        """
        Reset workflow state to initial conditions.
        Useful for testing or error recovery.
        """
        logger.warning(f"⚠️ Resetting workflow state for {self.session_id}")
        
        # Clear all marker files
        markers_to_clear = ['.analysis_complete', '.data_analysis_mode', 'agent_state.json']
        for marker in markers_to_clear:
            marker_path = self.session_folder / marker
            if marker_path.exists():
                try:
                    marker_path.unlink()
                    logger.info(f"🧹 Cleared {marker}")
                except:
                    pass
        
        # Reset state
        default_state = self._create_default_state()
        with open(self.state_file, 'w') as f:
            json.dump(default_state, f, indent=2)
        
        # Clear Flask session
        try:
            for key in ['workflow_source', 'workflow_stage', 'data_loaded', 
                       'csv_loaded', 'shapefile_loaded', 'analysis_complete',
                       'tpr_completed', 'workflow_transitioned']:
                session.pop(key, None)
            session.modified = True
        except:
            pass
        
        logger.info(f"✅ State reset complete for {self.session_id}")