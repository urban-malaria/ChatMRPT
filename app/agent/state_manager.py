"""
Data Analysis State Manager

Manages conversation state for Data Analysis V3 agent to fix workflow amnesia.
Uses file-based storage for cross-worker compatibility.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import threading
from enum import Enum

logger = logging.getLogger(__name__)


class ConversationStage(Enum):
    """Conversation stages for TPR and general analysis workflows."""
    INITIAL = "initial"

    # Conversational onboarding stages (NEW)
    DATA_OVERVIEW_SHOWN = "data_overview_shown"  # Showed data summary
    TPR_EXPLAINED = "tpr_explained"  # Explained TPR workflow
    AWAITING_WORKFLOW_CONFIRMATION = "awaiting_workflow_confirmation"  # Waiting for "let's start"

    # TPR workflow stages (3-step process)
    TPR_STATE_SELECTION = "tpr_state_selection"
    TPR_FACILITY_LEVEL = "tpr_facility_level"
    TPR_AGE_GROUP = "tpr_age_group"
    TPR_CALCULATING = "tpr_calculating"
    TPR_COMPLETE = "tpr_complete"
    TPR_COMPLETED_AWAITING_CONFIRMATION = "tpr_completed_awaiting_confirmation"  # Waiting for yes/continue

    # General data exploration
    DATA_EXPLORING = "data_exploring"
    COMPLETE = "complete"


class DataAnalysisStateManager:
    """
    Manages persistent state for Data Analysis V3 agent.
    
    Features:
    - File-based storage for multi-worker support
    - Thread-safe operations
    - Automatic state validation
    - Graceful error handling
    """
    
    def __init__(self, session_id: str):
        """
        Initialize state manager for a session.
        
        Args:
            session_id: Unique session identifier
        """
        self.session_id = session_id
        self.state_dir = Path(f"instance/uploads/{session_id}")
        self.state_file = self.state_dir / ".agent_state.json"
        self._lock = threading.Lock()
        
        # Ensure directory exists
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"StateManager initialized for session {session_id}")
    
    def save_state(self, state: Dict[str, Any]) -> bool:
        """
        Save state to file with thread safety.
        
        Args:
            state: State dictionary to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self._lock:
                # Add metadata
                state['_last_updated'] = datetime.now().isoformat()
                state['_session_id'] = self.session_id
                
                # Write to temporary file first (atomic operation)
                temp_file = self.state_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(state, f, indent=2, default=str)
                
                # Atomic rename
                temp_file.replace(self.state_file)
                
                logger.debug(f"State saved for session {self.session_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False
    
    def load_state(self) -> Dict[str, Any]:
        """
        Load state from file with validation.
        
        Returns:
            State dictionary or empty dict if not found/invalid
        """
        try:
            with self._lock:
                if not self.state_file.exists():
                    return {}
                
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                
                # Validate session ID
                if state.get('_session_id') != self.session_id:
                    logger.warning(f"Session ID mismatch in state file")
                    return {}
                
                return state
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in state file: {e}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return {}
    
    def update_state(self, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields in state.
        
        Args:
            updates: Dictionary of fields to update
            
        Returns:
            True if successful
        """
        try:
            # Load current state
            state = self.load_state()
            
            # Apply updates
            state.update(updates)
            
            # Save back
            return self.save_state(state)
            
        except Exception as e:
            logger.error(f"Failed to update state: {e}")
            return False

    # ---------------------- Conversational State (Global) ----------------------
    def get_conversation_state(self) -> Dict[str, Any]:
        """
        Retrieve conversation-level state used for mixed-initiative dialogue.

        Returns:
            Dict containing:
              - slots: dict of slot_name -> value
              - pending_slots: list of slot names waiting to be filled
              - last_options: dict of context_key -> [options]
              - last_question: last question asked to user (str)
              - side_turns_count: int (number of deviations handled)
              - reminder_count: int (number of reminders appended)
              - current_workflow: str or None
        """
        state = self.load_state()
        return state.get('conversation_state', {
            'slots': {},
            'pending_slots': [],
            'last_options': {},
            'last_question': None,
            'side_turns_count': 0,
            'reminder_count': 0,
            'current_workflow': None
        })

    def set_conversation_state(self, convo_state: Dict[str, Any]) -> bool:
        """Overwrite conversation-level state."""
        state = self.load_state()
        convo_state = convo_state or {}
        state['conversation_state'] = convo_state
        saved = self.save_state(state)
        # Mirror to Redis when available (best-effort)
        try:
            from app.services.redis_state import get_redis_state_manager
            rsm = get_redis_state_manager()
            rsm.set_conversation_state(self.session_id, convo_state)
        except Exception:
            pass
        return saved

    def update_conversation_state(self, updates: Dict[str, Any]) -> bool:
        """Update conversation-level fields incrementally."""
        state = self.load_state()
        convo = state.get('conversation_state', {})
        convo.update(updates or {})
        state['conversation_state'] = convo
        saved = self.save_state(state)
        try:
            from app.services.redis_state import get_redis_state_manager
            rsm = get_redis_state_manager()
            rsm.set_conversation_state(self.session_id, convo)
        except Exception:
            pass
        return saved

    def set_last_options(self, context_key: str, options: list) -> bool:
        """Store the last presented options for numeric/ordinal mapping."""
        state = self.load_state()
        convo = state.get('conversation_state', {})
        last_options = convo.get('last_options', {})
        last_options[context_key] = options or []
        convo['last_options'] = last_options
        state['conversation_state'] = convo
        return self.save_state(state)

    def get_last_options(self, context_key: str) -> list:
        """Get last presented options for a given context key."""
        # Prefer Redis if available
        try:
            from app.services.redis_state import get_redis_state_manager
            rsm = get_redis_state_manager()
            convo = rsm.get_conversation_state(self.session_id)
            if convo:
                return (convo.get('last_options') or {}).get(context_key, [])
        except Exception:
            pass
        convo_local = self.get_conversation_state()
        return (convo_local.get('last_options') or {}).get(context_key, [])

    def get_field(self, field: str, default: Any = None) -> Any:
        """
        Get a specific field from state.
        
        Args:
            field: Field name to retrieve
            default: Default value if field not found
            
        Returns:
            Field value or default
        """
        state = self.load_state()
        return state.get(field, default)
    
    def get_state(self) -> Dict[str, Any]:
        """
        Get the entire state dictionary.
        
        Returns:
            Complete state dictionary
        """
        return self.load_state()
    
    def update_workflow_stage(self, stage: ConversationStage) -> bool:
        """
        Update the workflow stage.

        Args:
            stage: New conversation stage (enum or string)

        Returns:
            True if successful
        """
        # Handle both enum and string inputs
        stage_value = stage.value if hasattr(stage, 'value') else stage
        return self.update_state({
            'current_stage': stage_value,
            'stage_updated_at': datetime.now().isoformat()
        })
    
    def get_workflow_stage(self) -> ConversationStage:
        """
        Get current workflow stage.
        
        Returns:
            Current conversation stage
        """
        stage_value = self.get_field('current_stage', ConversationStage.INITIAL.value)
        try:
            return ConversationStage(stage_value)
        except ValueError:
            logger.warning(f"Invalid stage value: {stage_value}, defaulting to INITIAL")
            return ConversationStage.INITIAL
    
    def save_tpr_selection(self, selection_type: str, value: Any) -> bool:
        """
        Save a TPR workflow selection.
        
        Args:
            selection_type: Type of selection (state, facility_level, age_group)
            value: Selected value
            
        Returns:
            True if successful
        """
        state = self.load_state()
        if 'tpr_selections' not in state:
            state['tpr_selections'] = {}
        
        state['tpr_selections'][selection_type] = value
        ts = datetime.now().isoformat()
        # Preserve both keys for backward/compatibility with tests
        state['tpr_selections'][f'{selection_type}_timestamp'] = ts
        state['tpr_selections'][f'{selection_type}_selected_at'] = ts
        
        return self.save_state(state)

    # ---------------------- Chat History helpers ----------------------
    def save_chat_message(self, role: str, content: str) -> bool:
        """Append a chat message to history and truncate to last 50 entries."""
        state = self.load_state()
        history = state.get('chat_history', [])
        history.append({
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat()
        })
        # Truncate to last 50
        if len(history) > 50:
            history = history[-50:]
        state['chat_history'] = history
        return self.save_state(state)

    def get_chat_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return the chat history, optionally limited to most recent N messages."""
        state = self.load_state()
        history = state.get('chat_history', [])
        if limit is not None:
            return history[-limit:]
        return history

    def get_state_summary(self) -> Dict[str, Any]:
        """Return a concise summary of the current state (for debugging/tests)."""
        state = self.load_state()
        return {
            'session_id': self.session_id,
            'current_stage': state.get('current_stage', ConversationStage.INITIAL.value),
            'tpr_selections': state.get('tpr_selections', {}),
            'chat_history_length': len(state.get('chat_history', [])),
            'last_updated': state.get('_last_updated')
        }

    def get_tpr_selections(self) -> Dict[str, Any]:
        """
        Get all TPR workflow selections.

        Returns:
            Dictionary of TPR selections
        """
        state = self.load_state()
        return state.get('tpr_selections', {})

    def get_tpr_selection(self, selection_type: str) -> Optional[Any]:
        """
        Get a specific TPR workflow selection.

        Args:
            selection_type: Type of selection (state, facility_level, age_group)

        Returns:
            The selected value or None if not found
        """
        state = self.load_state()
        tpr_selections = state.get('tpr_selections', {})
        return tpr_selections.get(selection_type)

    def mark_tpr_workflow_active(self) -> bool:
        """
        Mark TPR workflow as active.
        
        Returns:
            True if successful
        """
        return self.update_state({
            'tpr_workflow_active': True,
            'tpr_workflow_started_at': datetime.now().isoformat()
        })
    
    def mark_tpr_workflow_complete(self) -> bool:
        """
        Mark TPR workflow as complete.
        
        Returns:
            True if successful
        """
        return self.update_state({
            'tpr_workflow_active': False,
            'tpr_workflow_completed_at': datetime.now().isoformat(),
            'current_stage': ConversationStage.TPR_COMPLETE.value
        })
    
    def is_tpr_workflow_active(self) -> bool:
        """
        Check if TPR workflow is currently active.
        
        Returns:
            True if TPR workflow is active
        """
        # Prefer explicit flag if set
        if self.get_field('tpr_workflow_active', False):
            return True
        # Fallback to checking stage membership
        stage_value = self.get_field('current_stage', ConversationStage.INITIAL.value)
        return stage_value in {
            ConversationStage.TPR_STATE_SELECTION.value,
            ConversationStage.TPR_FACILITY_LEVEL.value,
            ConversationStage.TPR_AGE_GROUP.value,
            ConversationStage.TPR_CALCULATING.value
        }
    
    def clear_state(self) -> bool:
        """
        Clear all state for this session.
        
        Returns:
            True if successful
        """
        try:
            with self._lock:
                if self.state_file.exists():
                    self.state_file.unlink()
                logger.info(f"State cleared for session {self.session_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to clear state: {e}")
            return False
    
    def log_event(self, event_type: str, details: Dict[str, Any]) -> bool:
        """
        Log an event to state history.
        
        Args:
            event_type: Type of event
            details: Event details
            
        Returns:
            True if successful
        """
        state = self.load_state()
        
        if 'event_history' not in state:
            state['event_history'] = []
        
        event = {
            'type': event_type,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        
        state['event_history'].append(event)
        
        # Keep only last 50 events
        if len(state['event_history']) > 50:
            state['event_history'] = state['event_history'][-50:]
        
        return self.save_state(state)


def check_tpr_workflow_active(session_id: str) -> bool:
    """Cross-worker check: is a TPR stage active for this session?

    Looks at the saved state file and returns True if the stage value
    is within the TPR_* stages.
    """
    try:
        state_file = Path(f"instance/uploads/{session_id}/.agent_state.json")
        if not state_file.exists():
            return False
        with open(state_file, 'r') as f:
            state = json.load(f)
        current = state.get('current_stage', '')
        return current in {
            ConversationStage.TPR_STATE_SELECTION.value,
            ConversationStage.TPR_FACILITY_LEVEL.value,
            ConversationStage.TPR_AGE_GROUP.value,
            ConversationStage.TPR_CALCULATING.value
        }
    except Exception:
        return False
