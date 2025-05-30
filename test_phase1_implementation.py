#!/usr/bin/env python3
"""
Test script for Phase 1 Implementation - State Management & Context Awareness

This script tests the core components of the enhanced session state management system.
"""

import sys
import os
import json
from datetime import datetime

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

try:
    from app.core import (
        SessionState, WorkflowStage, DataState, AnalysisState,
        StateManager, ContextChecker, GuidanceGenerator,
        get_workflow_progress_percentage
    )
    print("✅ Successfully imported all Phase 1 modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def test_session_state():
    """Test SessionState creation and serialization"""
    print("\n🧪 Testing SessionState...")
    
    # Create a new session state
    state = SessionState(session_id="test123")
    
    # Test basic properties
    assert state.workflow_stage == WorkflowStage.INITIAL
    assert state.data_state == DataState.NO_DATA
    assert state.analysis_state == AnalysisState.NOT_STARTED
    
    # Test serialization
    state_dict = state.to_dict()
    assert isinstance(state_dict, dict)
    assert state_dict['session_id'] == "test123"
    
    # Test deserialization
    restored_state = SessionState.from_dict(state_dict)
    assert restored_state.session_id == "test123"
    assert restored_state.workflow_stage == WorkflowStage.INITIAL
    
    print("✅ SessionState tests passed")

def test_state_manager():
    """Test StateManager functionality"""
    print("\n🧪 Testing StateManager...")
    
    # Mock Flask session
    mock_session = {}
    
    # Create state manager
    manager = StateManager(mock_session)
    
    # Test getting current state (should create default)
    state = manager.get_current_state()
    assert state.workflow_stage == WorkflowStage.INITIAL
    
    # Test updating state
    updated_state = manager.update_state(
        workflow_stage=WorkflowStage.DATA_UPLOAD,
        data_state=DataState.CSV_ONLY
    )
    assert updated_state.workflow_stage == WorkflowStage.DATA_UPLOAD
    assert updated_state.data_state == DataState.CSV_ONLY
    
    # Test action checking
    can_do, reason = manager.can_perform_action('start_analysis')
    assert not can_do  # Should not be able to start analysis with only CSV
    
    print("✅ StateManager tests passed")

def test_context_checker():
    """Test ContextChecker functionality"""
    print("\n🧪 Testing ContextChecker...")
    
    checker = ContextChecker()
    
    # Test with initial state
    initial_state = SessionState()
    
    # Test upload actions (should be allowed)
    can_upload, reason = checker.check_prerequisites('upload_csv', initial_state)
    assert can_upload
    
    # Test analysis action (should not be allowed)
    can_analyze, reason = checker.check_prerequisites('start_analysis', initial_state)
    assert not can_analyze
    
    # Test guidance generation
    guidance = checker.generate_helpful_message('start_analysis', initial_state)
    assert isinstance(guidance, str)
    assert len(guidance) > 0
    
    print("✅ ContextChecker tests passed")

def test_guidance_generator():
    """Test GuidanceGenerator functionality"""
    print("\n🧪 Testing GuidanceGenerator...")
    
    generator = GuidanceGenerator()
    
    # Test welcome message
    initial_state = SessionState()
    welcome = generator.get_welcome_message(initial_state)
    assert isinstance(welcome, str)
    assert "Welcome" in welcome
    
    # Test next step suggestions
    suggestions = generator.get_next_step_suggestions(initial_state)
    assert isinstance(suggestions, list)
    assert len(suggestions) > 0
    
    # Test help topics
    help_topics = generator.get_help_topics(initial_state)
    assert isinstance(help_topics, list)
    assert len(help_topics) > 0
    
    # Test progress update
    progress_msg = generator.get_progress_update(initial_state)
    assert isinstance(progress_msg, str)
    assert "Progress" in progress_msg
    
    print("✅ GuidanceGenerator tests passed")

def test_workflow_integration():
    """Test integrated workflow progression"""
    print("\n🧪 Testing integrated workflow...")
    
    mock_session = {}
    manager = StateManager(mock_session)
    checker = ContextChecker()
    generator = GuidanceGenerator()
    
    # Start with initial state
    state = manager.get_current_state()
    progress = get_workflow_progress_percentage(state)
    assert progress == 0
    
    # Simulate CSV upload
    state = manager.update_data_info('csv', {
        'filename': 'test.csv',
        'size': 1024,
        'rows': 100,
        'columns': 5,
        'variables': ['rainfall', 'temperature']
    })
    
    assert state.data_state == DataState.CSV_ONLY
    assert state.workflow_stage == WorkflowStage.DATA_UPLOAD
    
    # Check what user can do now
    can_analyze, reason = checker.check_prerequisites('start_analysis', state)
    assert not can_analyze  # Still need shapefile
    
    # Simulate shapefile upload
    state = manager.update_data_info('shapefile', {
        'filename': 'boundaries.zip',
        'size': 2048,
        'features': 100
    })
    
    assert state.data_state == DataState.BOTH_LOADED
    assert state.workflow_stage == WorkflowStage.DATA_VALIDATION
    
    # Now analysis should be possible
    can_analyze, reason = checker.check_prerequisites('start_analysis', state)
    assert can_analyze
    
    # Simulate analysis completion
    state = manager.update_analysis_progress(AnalysisState.COMPLETE, results={
        'composite_scores': {'model_1': [0.8, 0.6, 0.9]},
        'vulnerability_rankings': [{'ward': 'Ward1', 'rank': 1}]
    })
    
    assert state.analysis_state == AnalysisState.COMPLETE
    assert state.workflow_stage == WorkflowStage.ANALYSIS_COMPLETE
    
    # Maps should now be possible
    can_map, reason = checker.check_prerequisites('create_map', state)
    assert can_map
    
    final_progress = get_workflow_progress_percentage(state)
    assert final_progress > 0
    
    print("✅ Integrated workflow tests passed")

def test_error_scenarios():
    """Test error handling and edge cases"""
    print("\n🧪 Testing error scenarios...")
    
    checker = ContextChecker()
    generator = GuidanceGenerator()
    
    # Test unknown action
    unknown_state = SessionState()
    can_do, reason = checker.check_prerequisites('unknown_action', unknown_state)
    assert can_do  # Unknown actions are allowed by default
    
    # Test error guidance
    error_guidance = generator.get_error_guidance('file_upload', unknown_state)
    assert isinstance(error_guidance, str)
    assert len(error_guidance) > 0
    
    # Test feature explanation
    explanation = generator.get_feature_explanation('malaria_analysis')
    assert isinstance(explanation, str)
    assert "malaria" in explanation.lower()
    
    print("✅ Error scenario tests passed")

def main():
    """Run all tests"""
    print("🚀 Starting Phase 1 Implementation Tests")
    print("=" * 50)
    
    try:
        test_session_state()
        test_state_manager()
        test_context_checker()
        test_guidance_generator()
        test_workflow_integration()
        test_error_scenarios()
        
        print("\n" + "=" * 50)
        print("🎉 All Phase 1 tests passed successfully!")
        print("\n📋 What's working:")
        print("  ✅ Session state tracking and serialization")
        print("  ✅ Workflow stage management")
        print("  ✅ Context-aware action validation")
        print("  ✅ Proactive user guidance")
        print("  ✅ Error handling and recovery")
        print("  ✅ Integrated workflow progression")
        
        print("\n🎯 Next steps:")
        print("  1. Integrate with message service")
        print("  2. Update file upload handlers")
        print("  3. Enhance UI with progress indicators")
        print("  4. Add contextual help system")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 