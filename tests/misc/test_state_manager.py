"""
Unit tests for Data Analysis V3 State Manager

Tests state persistence, workflow tracking, and cross-worker compatibility.
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime
import shutil

from app.agent.state_manager import (
    DataAnalysisStateManager,
    ConversationStage,
    check_tpr_workflow_active
)


@pytest.fixture
def temp_session_dir():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def state_manager(temp_session_dir, monkeypatch):
    """Create a state manager with temporary directory."""
    session_id = "test_session_123"
    # Monkeypatch the state directory to use temp dir
    monkeypatch.setattr(
        'app.agent.state_manager.Path',
        lambda x: Path(temp_session_dir) / session_id if 'instance/uploads' in x else Path(x)
    )
    manager = DataAnalysisStateManager(session_id)
    manager.state_dir = Path(temp_session_dir) / session_id
    manager.state_file = manager.state_dir / ".agent_state.json"
    manager.state_dir.mkdir(parents=True, exist_ok=True)
    return manager


class TestStateManager:
    """Test DataAnalysisStateManager functionality."""
    
    def test_initialization(self, state_manager):
        """Test state manager initializes correctly."""
        assert state_manager.session_id == "test_session_123"
        assert state_manager.state_dir.exists()
        assert state_manager.state_file.name == ".agent_state.json"
    
    def test_save_and_load_state(self, state_manager):
        """Test saving and loading state."""
        # Arrange
        test_state = {
            'current_stage': 'tpr_state_selection',
            'tpr_selections': {'state': 'Adamawa'},
            'custom_field': 'test_value'
        }
        
        # Act
        save_result = state_manager.save_state(test_state)
        loaded_state = state_manager.load_state()
        
        # Assert
        assert save_result is True
        assert loaded_state['current_stage'] == 'tpr_state_selection'
        assert loaded_state['tpr_selections']['state'] == 'Adamawa'
        assert loaded_state['custom_field'] == 'test_value'
        assert '_last_updated' in loaded_state
        assert '_session_id' in loaded_state
    
    def test_update_state(self, state_manager):
        """Test updating specific state fields."""
        # Arrange
        initial_state = {'field1': 'value1', 'field2': 'value2'}
        state_manager.save_state(initial_state)
        
        # Act
        update_result = state_manager.update_state({'field2': 'updated', 'field3': 'new'})
        final_state = state_manager.load_state()
        
        # Assert
        assert update_result is True
        assert final_state['field1'] == 'value1'  # Unchanged
        assert final_state['field2'] == 'updated'  # Updated
        assert final_state['field3'] == 'new'      # Added
    
    def test_workflow_stage_management(self, state_manager):
        """Test workflow stage updates and retrieval."""
        # Act & Assert - Initial stage
        initial_stage = state_manager.get_workflow_stage()
        assert initial_stage == ConversationStage.INITIAL
        
        # Update to TPR state selection
        state_manager.update_workflow_stage(ConversationStage.TPR_STATE_SELECTION)
        current_stage = state_manager.get_workflow_stage()
        assert current_stage == ConversationStage.TPR_STATE_SELECTION
        
        # Update to facility level
        state_manager.update_workflow_stage(ConversationStage.TPR_FACILITY_LEVEL)
        current_stage = state_manager.get_workflow_stage()
        assert current_stage == ConversationStage.TPR_FACILITY_LEVEL
    
    def test_tpr_selections(self, state_manager):
        """Test saving and retrieving TPR selections."""
        # Act
        state_manager.save_tpr_selection('state', 'Kwara')
        state_manager.save_tpr_selection('facility_level', 'primary')
        state_manager.save_tpr_selection('age_group', 'u5')
        
        selections = state_manager.get_tpr_selections()
        
        # Assert
        assert selections['state'] == 'Kwara'
        assert selections['facility_level'] == 'primary'
        assert selections['age_group'] == 'u5'
        assert 'state_selected_at' in selections
        assert 'facility_level_selected_at' in selections
        assert 'age_group_selected_at' in selections
    
    def test_chat_history(self, state_manager):
        """Test chat history management."""
        # Act
        state_manager.save_chat_message('user', 'Hello')
        state_manager.save_chat_message('assistant', 'Hi there!')
        state_manager.save_chat_message('user', 'Calculate TPR')
        
        history = state_manager.get_chat_history(limit=2)
        
        # Assert
        assert len(history) == 2
        assert history[0]['content'] == 'Hi there!'
        assert history[1]['content'] == 'Calculate TPR'
        assert history[0]['role'] == 'assistant'
        assert history[1]['role'] == 'user'
    
    def test_chat_history_truncation(self, state_manager):
        """Test that chat history is truncated to prevent bloat."""
        # Add 60 messages (exceeds limit of 50)
        for i in range(60):
            state_manager.save_chat_message('user', f'Message {i}')
        
        # Get full history
        state = state_manager.load_state()
        
        # Assert history was truncated
        assert len(state['chat_history']) <= 50
        # Should keep the most recent messages
        assert 'Message 59' in state['chat_history'][-1]['content']
    
    def test_is_tpr_workflow_active(self, state_manager):
        """Test TPR workflow detection."""
        # Initially not in TPR workflow
        assert state_manager.is_tpr_workflow_active() is False
        
        # Set TPR stage
        state_manager.update_workflow_stage(ConversationStage.TPR_STATE_SELECTION)
        assert state_manager.is_tpr_workflow_active() is True
        
        # Set to another TPR stage
        state_manager.update_workflow_stage(ConversationStage.TPR_AGE_GROUP)
        assert state_manager.is_tpr_workflow_active() is True
        
        # Set to non-TPR stage
        state_manager.update_workflow_stage(ConversationStage.DATA_EXPLORING)
        assert state_manager.is_tpr_workflow_active() is False
    
    def test_clear_state(self, state_manager):
        """Test clearing state."""
        # Arrange
        state_manager.save_state({'test': 'data'})
        assert state_manager.state_file.exists()
        
        # Act
        clear_result = state_manager.clear_state()
        
        # Assert
        assert clear_result is True
        assert not state_manager.state_file.exists()
        loaded_state = state_manager.load_state()
        assert loaded_state == {}
    
    def test_state_validation(self, state_manager):
        """Test state validation on load."""
        # Create invalid state file (wrong session ID)
        invalid_state = {
            '_session_id': 'wrong_session',
            'data': 'test'
        }
        with open(state_manager.state_file, 'w') as f:
            json.dump(invalid_state, f)
        
        # Should return empty dict for invalid state
        loaded = state_manager.load_state()
        assert loaded == {}
    
    def test_get_field_with_default(self, state_manager):
        """Test getting specific field with default value."""
        # Save some state
        state_manager.save_state({'existing_field': 'value'})
        
        # Get existing field
        value = state_manager.get_field('existing_field')
        assert value == 'value'
        
        # Get non-existing field with default
        value = state_manager.get_field('missing_field', 'default')
        assert value == 'default'
    
    def test_get_state_summary(self, state_manager):
        """Test getting state summary for debugging."""
        # Setup state
        state_manager.update_workflow_stage(ConversationStage.TPR_FACILITY_LEVEL)
        state_manager.save_tpr_selection('state', 'Osun')
        state_manager.save_chat_message('user', 'test message')
        
        # Get summary
        summary = state_manager.get_state_summary()
        
        # Assert
        assert summary['session_id'] == 'test_session_123'
        assert summary['current_stage'] == 'tpr_facility_level'
        assert summary['tpr_selections']['state'] == 'Osun'
        assert summary['chat_history_length'] == 1
        assert 'last_updated' in summary
    
    def test_thread_safety(self, state_manager):
        """Test thread-safe operations."""
        import threading
        results = []
        
        def save_state(value):
            result = state_manager.save_state({'value': value})
            results.append(result)
        
        # Create multiple threads
        threads = []
        for i in range(10):
            t = threading.Thread(target=save_state, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # All saves should succeed
        assert all(results)
        
        # Final state should be consistent
        final_state = state_manager.load_state()
        assert 'value' in final_state


class TestCrossWorkerCompatibility:
    """Test cross-worker state checking function."""
    
    def test_check_tpr_workflow_active(self, temp_session_dir):
        """Test the cross-worker TPR check function."""
        session_id = "test_session"
        session_dir = Path(temp_session_dir) / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        state_file = session_dir / ".agent_state.json"
        
        # Create state file with TPR stage
        state = {
            'current_stage': 'tpr_age_group',
            '_session_id': session_id
        }
        with open(state_file, 'w') as f:
            json.dump(state, f)
        
        # Mock the path construction
        import app.agent.state_manager as sm
        original_path = sm.Path
        
        def mock_path(x):
            if 'instance/uploads' in x:
                return Path(temp_session_dir) / session_id / ".agent_state.json"
            return original_path(x)
        
        sm.Path = mock_path
        
        # Test the function
        result = check_tpr_workflow_active(session_id)
        
        # Restore
        sm.Path = original_path
        
        assert result is True
    
    def test_check_tpr_workflow_inactive(self, temp_session_dir):
        """Test when TPR workflow is not active."""
        session_id = "test_session"
        session_dir = Path(temp_session_dir) / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        state_file = session_dir / ".agent_state.json"
        
        # Create state file with non-TPR stage
        state = {
            'current_stage': 'data_exploring',
            '_session_id': session_id
        }
        with open(state_file, 'w') as f:
            json.dump(state, f)
        
        # Mock the path construction
        import app.agent.state_manager as sm
        original_path = sm.Path
        
        def mock_path(x):
            if 'instance/uploads' in x:
                return Path(temp_session_dir) / session_id / ".agent_state.json"
            return original_path(x)
        
        sm.Path = mock_path
        
        # Test the function
        result = check_tpr_workflow_active(session_id)
        
        # Restore
        sm.Path = original_path
        
        assert result is False