"""
Analysis State Handler for ChatMRPT

Manages state transitions when analysis completes, ensuring all components
are aware of the new unified dataset availability.
"""

import logging
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path
import pandas as pd

from app.services.data_state import get_data_state_manager

logger = logging.getLogger(__name__)


class AnalysisStateHandler:
    """
    Handles state transitions when analysis completes.
    
    This ensures:
    1. Unified dataset is immediately loaded
    2. All components are notified
    3. Tool availability is updated
    4. User gets clear messaging
    """
    
    def __init__(self):
        self.state_listeners: List[Callable] = []
        self.completion_hooks: List[Callable] = []
    
    def register_state_listener(self, listener: Callable):
        """Register a function to be called when analysis state changes."""
        self.state_listeners.append(listener)
    
    def register_completion_hook(self, hook: Callable):
        """Register a function to be called when analysis completes."""
        self.completion_hooks.append(hook)
    
    def on_analysis_complete(self, session_id: str, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Called when analysis completes successfully.
        
        Args:
            session_id: Session identifier
            analysis_results: Results from the analysis
            
        Returns:
            Status information about the state transition
        """
        logger.info(f"Analysis complete for session {session_id}, updating state...")
        
        try:
            # 1. Update unified data state
            data_state_manager = get_data_state_manager()
            data_state_manager.on_analysis_complete(session_id)
            
            # 2. Verify unified dataset is available
            data_state = data_state_manager.get_state(session_id)
            if data_state.unified_data is None:
                logger.error("Failed to load unified dataset after analysis")
                return {
                    'status': 'error',
                    'message': 'Analysis completed but unified dataset not available'
                }
            
            # 3. Notify all listeners
            for listener in self.state_listeners:
                try:
                    listener(session_id, 'analysis_complete', analysis_results)
                except Exception as e:
                    logger.error(f"Error notifying state listener: {e}")
            
            # 4. Run completion hooks
            for hook in self.completion_hooks:
                try:
                    hook(session_id, data_state)
                except Exception as e:
                    logger.error(f"Error running completion hook: {e}")
            
            # 5. Log success
            unified_shape = data_state.unified_data.shape
            logger.info(f"State transition complete. Unified dataset ready: {unified_shape}")
            
            return {
                'status': 'success',
                'message': 'Analysis complete and data ready for exploration',
                'unified_shape': unified_shape,
                'available_tools': self._get_available_tools(data_state)
            }
            
        except Exception as e:
            logger.error(f"Error in analysis state transition: {e}")
            return {
                'status': 'error',
                'message': f'State transition failed: {str(e)}'
            }
    
    def _get_available_tools(self, data_state) -> List[str]:
        """Get list of tools available in current state."""
        if data_state.analysis_complete:
            return [
                'query_data',  # Layer 1: text-to-SQL queries
                'analyze_data',  # Layer 2: Python analysis & explicit visualizations
                'create_vulnerability_map',
                'create_decision_tree',
                'create_urban_extent_map',
                'generate_report'
            ]
        elif data_state.data_loaded:
            return [
                'query_data',
                'analyze_data',
                'run_analysis'
            ]
        else:
            return []
    
    def check_and_update_state(self, session_id: str) -> Optional[str]:
        """
        Check if state needs updating and update if necessary.
        
        This can be called periodically to ensure state consistency.
        """
        try:
            data_state = get_data_state_manager().get_state(session_id)
            
            # Check if unified dataset exists but state doesn't know
            session_folder = Path(f"instance/uploads/{session_id}")
            unified_file = session_folder / "unified_dataset.geoparquet"
            
            if unified_file.exists() and not data_state.analysis_complete:
                logger.info(f"Found unified dataset for {session_id}, updating state...")
                data_state.on_analysis_complete()
                return "State updated - analysis data now available"
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking state: {e}")
            return None


# Global instance
_analysis_state_handler = None

def get_analysis_state_handler() -> AnalysisStateHandler:
    """Get the global analysis state handler instance."""
    global _analysis_state_handler
    if _analysis_state_handler is None:
        _analysis_state_handler = AnalysisStateHandler()
        
        # Register default hooks
        _register_default_hooks(_analysis_state_handler)
        
    return _analysis_state_handler


def _register_default_hooks(handler: AnalysisStateHandler):
    """Register default completion hooks."""
    
    def log_completion(session_id: str, data_state):
        """Log analysis completion details."""
        logger.info(f"Analysis completion hook triggered for {session_id}")
        info = data_state.get_data_info()
        logger.info(f"Data info: {info}")
    
    def update_session_metadata(session_id: str, data_state):
        """Update session metadata file."""
        try:
            session_folder = Path(f"instance/uploads/{session_id}")
            metadata_file = session_folder / "session_metadata.json"
            
            if metadata_file.exists():
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                metadata['analysis_complete'] = True
                metadata['unified_dataset_available'] = True
                metadata['last_updated'] = pd.Timestamp.now().isoformat()
                
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                    
        except Exception as e:
            logger.error(f"Error updating session metadata: {e}")
    
    handler.register_completion_hook(log_completion)
    handler.register_completion_hook(update_session_metadata)