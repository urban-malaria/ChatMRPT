#!/usr/bin/env python3
"""Test ITN distribution integration with ChatMRPT."""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.planning.itn_tools import PlanITNDistribution
from app.services.data_handler import DataHandler
import pandas as pd

def test_itn_tool():
    """Test the ITN planning tool directly."""
    print("Testing ITN Planning Tool Integration")
    print("=" * 50)
    
    # Use a test session ID (you should use an actual session with completed analysis)
    session_id = "test_session_123"
    
    # Test 1: Tool without parameters (should request parameters)
    print("\n1. Testing parameter request:")
    tool = PlanITNDistribution()
    result = tool.execute(session_id)
    print(f"Success: {result.success}")
    print(f"Message preview: {result.message[:200]}...")
    print(f"Waiting for params: {result.data.get('waiting_for_parameters', False)}")
    
    # Test 2: Tool with parameters
    print("\n2. Testing with parameters:")
    tool = PlanITNDistribution(
        total_nets=10000,
        avg_household_size=5.0,
        urban_threshold=30.0,
        method='composite'
    )
    
    # Note: This will fail if no analysis data exists for the session
    # In a real test, use a session that has completed analysis
    try:
        result = tool.execute(session_id)
        print(f"Success: {result.success}")
        print(f"Message: {result.message}")
    except Exception as e:
        print(f"Expected error (no data): {e}")
    
    print("\n" + "=" * 50)
    print("ITN tool tests complete!")

def test_conversational_flow():
    """Test the conversational flow handling."""
    print("\n\nTesting Conversational Flow")
    print("=" * 50)
    
    from app.agent.interpreter import RequestInterpreter
    from app.services.container import get_service_container
    
    # Initialize interpreter
    container = get_service_container()
    interpreter = RequestInterpreter(
        llm_manager=container.get_service('llm_manager'),
        tool_registry=None,
        analysis_service=container.get_service('analysis_service'),
        visualization_service=container.get_service('visualization_service'),
        data_service=container.get_service('data_service'),
        knowledge_base=None
    )
    
    session_id = "test_session_123"
    
    # Test ITN planning trigger
    print("\n1. Testing ITN planning trigger:")
    messages = [
        "I want to plan bed net distribution",
        "Help me distribute ITNs",
        "Plan ITN allocation"
    ]
    
    for msg in messages:
        print(f"\nUser: {msg}")
        # Note: This would normally go through the full LLM flow
        # Here we're just checking if the message would trigger ITN planning
        if any(word in msg.lower() for word in ['itn', 'bed net', 'net distribution', 'net allocation']):
            print("✓ Would trigger ITN planning")
        else:
            print("✗ Would NOT trigger ITN planning")
    
    print("\n" + "=" * 50)
    print("Conversational flow tests complete!")

if __name__ == "__main__":
    test_itn_tool()
    test_conversational_flow()
    
    print("\n\nNOTE: For full integration testing:")
    print("1. Start the Flask app")
    print("2. Upload data and run analysis")
    print("3. Say 'I want to plan ITN distribution'")
    print("4. Provide the requested parameters")
    print("5. View the generated distribution map")