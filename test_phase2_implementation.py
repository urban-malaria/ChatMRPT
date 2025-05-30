#!/usr/bin/env python3
"""
Test script for Phase 2 Implementation - Tool Self-Knowledge & Help System

This script tests the context-aware message service and tool self-knowledge features.
"""

import sys
import os
from unittest.mock import Mock, MagicMock

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

try:
    from app.core import SessionState, WorkflowStage, DataState, AnalysisState
    from app.services.context_aware_message_service import ContextAwareMessageService
    print("✅ Successfully imported Phase 2 modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def create_mock_base_service():
    """Create a mock base message service"""
    mock_service = Mock()
    mock_service.process_message.return_value = {
        "status": "success",
        "response": "Base service response",
        "action": "base_action"
    }
    return mock_service

def test_meta_intent_detection():
    """Test detection of meta-questions about the tool"""
    print("\n🧪 Testing meta-intent detection...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test capability questions
    assert service._detect_meta_intent("What can you do?") == "tool_capabilities"
    assert service._detect_meta_intent("what are your features") == "tool_capabilities"
    assert service._detect_meta_intent("tell me about yourself") == "tool_capabilities"
    
    # Test file requirement questions
    assert service._detect_meta_intent("What files do I need?") == "file_requirements"
    assert service._detect_meta_intent("csv requirements") == "file_requirements"
    assert service._detect_meta_intent("what format") == "file_requirements"
    
    # Test workflow questions
    assert service._detect_meta_intent("how to start") == "workflow_help"
    assert service._detect_meta_intent("what are the steps") == "workflow_help"
    assert service._detect_meta_intent("guide me through") == "workflow_help"
    
    # Test status questions
    assert service._detect_meta_intent("where am i") == "current_status"
    assert service._detect_meta_intent("what's my progress") == "current_status"
    assert service._detect_meta_intent("what can i do now") == "current_status"
    
    # Test general help
    assert service._detect_meta_intent("help") == "general_help"
    assert service._detect_meta_intent("getting started") == "general_help"
    
    print("✅ Meta-intent detection tests passed")

def test_action_intent_detection():
    """Test detection of action intents"""
    print("\n🧪 Testing action intent detection...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test analysis actions
    assert service._detect_action_intent("start analysis") == "start_analysis"
    assert service._detect_action_intent("run analysis") == "start_analysis"
    assert service._detect_action_intent("analyze") == "start_analysis"
    
    # Test map actions
    assert service._detect_action_intent("create map") == "create_map"
    assert service._detect_action_intent("show map") == "create_map"
    assert service._detect_action_intent("visualize") == "create_map"
    
    # Test ranking actions
    assert service._detect_action_intent("show rankings") == "view_rankings"
    assert service._detect_action_intent("most vulnerable") == "view_rankings"
    
    # Test upload actions
    assert service._detect_action_intent("upload csv") == "upload_csv"
    assert service._detect_action_intent("upload shapefile") == "upload_shapefile"
    
    print("✅ Action intent detection tests passed")

def test_tool_capabilities_explanation():
    """Test tool capabilities explanation"""
    print("\n🧪 Testing tool capabilities explanation...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test with initial state
    state = SessionState()
    response = service._explain_tool_capabilities(state)
    
    assert response["status"] == "success"
    assert response["action"] == "tool_explanation"
    assert "ChatMRPT" in response["response"]
    assert "malaria risk" in response["response"].lower()
    assert "suggestions" in response
    
    # Check that response contains key capability areas
    response_text = response["response"]
    assert "Data Processing" in response_text
    assert "Analysis" in response_text
    assert "Visualization" in response_text
    assert "Reporting" in response_text
    
    print("✅ Tool capabilities explanation tests passed")

def test_file_requirements_explanation():
    """Test file requirements explanation"""
    print("\n🧪 Testing file requirements explanation...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test with different data states
    states = [
        (DataState.NO_DATA, "Ready to upload"),
        (DataState.CSV_ONLY, "CSV uploaded"),
        (DataState.SHAPEFILE_ONLY, "Shapefile uploaded"),
        (DataState.BOTH_LOADED, "Both files uploaded")
    ]
    
    for data_state, expected_text in states:
        state = SessionState(data_state=data_state)
        response = service._explain_file_requirements(state)
        
        assert response["status"] == "success"
        assert response["action"] == "file_requirements_explanation"
        assert "CSV Data File" in response["response"]
        assert "Shapefile" in response["response"]
        assert expected_text in response["response"]
    
    print("✅ File requirements explanation tests passed")

def test_workflow_explanation():
    """Test workflow explanation"""
    print("\n🧪 Testing workflow explanation...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    state = SessionState(workflow_stage=WorkflowStage.DATA_UPLOAD)
    response = service._explain_workflow(state)
    
    assert response["status"] == "success"
    assert response["action"] == "workflow_explanation"
    assert "Workflow" in response["response"]
    assert "Data Preparation" in response["response"]
    assert "Analysis" in response["response"]
    assert "Visualization" in response["response"]
    assert "Progress" in response["response"]
    
    print("✅ Workflow explanation tests passed")

def test_current_status_explanation():
    """Test current status explanation"""
    print("\n🧪 Testing current status explanation...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Mock state manager
    mock_state_manager = Mock()
    mock_capabilities = Mock()
    mock_capabilities.can_upload_csv = True
    mock_capabilities.can_upload_shapefile = True
    mock_capabilities.can_start_analysis = False
    mock_capabilities.__dict__ = {
        'can_upload_csv': True,
        'can_upload_shapefile': True,
        'can_start_analysis': False,
        'can_create_maps': False,
        'can_view_rankings': False,
        'can_generate_reports': False
    }
    mock_state_manager.get_available_capabilities.return_value = mock_capabilities
    
    state = SessionState()
    response = service._explain_current_status(state, mock_state_manager)
    
    assert response["status"] == "success"
    assert response["action"] == "status_explanation"
    assert "What you can do now" in response["response"]
    assert "Upload CSV" in response["response"]
    assert "capabilities" in response
    
    print("✅ Current status explanation tests passed")

def test_blocked_action_response():
    """Test response when action is blocked"""
    print("\n🧪 Testing blocked action responses...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Mock state manager
    mock_state_manager = Mock()
    
    # Test blocked analysis (no data uploaded)
    state = SessionState()  # Initial state with no data
    response = service._generate_blocked_action_response('start_analysis', state, mock_state_manager)
    
    assert response["status"] == "blocked_action"
    assert response["blocked_action"] == "start_analysis"
    assert "missing_prerequisites" in response
    assert "suggestions" in response
    assert "I'd love to help" in response["response"]
    
    print("✅ Blocked action response tests passed")

def test_context_aware_message_processing():
    """Test full context-aware message processing"""
    print("\n🧪 Testing context-aware message processing...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test meta-question processing
    session_dict = {'session_id': 'test123'}
    
    # Test capability question
    response = service.process_message_with_context(
        "What can you do?", 
        session_dict
    )
    
    assert response["status"] == "success"
    assert response["action"] == "tool_explanation"
    assert "ChatMRPT" in response["response"]
    
    # Test blocked action
    response = service.process_message_with_context(
        "start analysis", 
        session_dict
    )
    
    assert response["status"] == "blocked_action"
    assert response["blocked_action"] == "start_analysis"
    
    print("✅ Context-aware message processing tests passed")

def test_legacy_format_conversion():
    """Test conversion to legacy format"""
    print("\n🧪 Testing legacy format conversion...")
    
    mock_base = create_mock_base_service()
    service = ContextAwareMessageService(mock_base)
    
    # Test state with both files loaded
    state = SessionState(
        data_state=DataState.BOTH_LOADED,
        analysis_state=AnalysisState.COMPLETE
    )
    state.data_summary.available_variables = ['rainfall', 'temperature']
    state.data_summary.ward_count = 50
    
    legacy_format = service._convert_state_to_legacy_format(state)
    
    assert legacy_format['csv_loaded'] == True
    assert legacy_format['shapefile_loaded'] == True
    assert legacy_format['analysis_complete'] == True
    assert legacy_format['available_variables'] == ['rainfall', 'temperature']
    assert legacy_format['csv_rows'] == 50
    
    print("✅ Legacy format conversion tests passed")

def main():
    """Run all Phase 2 tests"""
    print("🚀 Starting Phase 2 Implementation Tests")
    print("=" * 50)
    
    try:
        test_meta_intent_detection()
        test_action_intent_detection()
        test_tool_capabilities_explanation()
        test_file_requirements_explanation()
        test_workflow_explanation()
        test_current_status_explanation()
        test_blocked_action_response()
        test_context_aware_message_processing()
        test_legacy_format_conversion()
        
        print("\n" + "=" * 50)
        print("🎉 All Phase 2 tests passed successfully!")
        print("\n📋 What's working:")
        print("  ✅ Meta-intent detection for tool questions")
        print("  ✅ Action intent recognition")
        print("  ✅ Comprehensive tool self-knowledge")
        print("  ✅ Context-aware explanations")
        print("  ✅ Blocked action handling with guidance")
        print("  ✅ Legacy system integration")
        
        print("\n🎯 Phase 2 Complete! Key Features Added:")
        print("  🧠 Tool self-awareness and knowledge base")
        print("  🤔 Intent recognition for meta-questions")
        print("  💬 Context-intelligent conversation flow")
        print("  🚫 Graceful blocking with helpful guidance")
        print("  🔄 Seamless integration with existing system")
        
        print("\n🔮 Ready for Phase 3:")
        print("  1. Integrate with main routes")
        print("  2. Add proactive welcome messages")
        print("  3. Enhance UI with progress indicators")
        print("  4. Add contextual help tooltips")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 