"""
Debug script to investigate vulnerability count issues.
"""
import sys
import os
import pandas as pd

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def debug_vulnerability_extraction():
    print("=== Debugging Vulnerability Count Extraction ===")
    
    # Simulate the exact conditions from the real analysis
    from app.services.message_service import MessageService
    
    # Create message service
    message_service = MessageService()
    
    # Test different scenarios for vulnerability_rankings
    print("\n1. Testing with properly formatted vulnerability_rankings:")
    
    class MockDataHandler1:
        def __init__(self):
            self.vulnerability_rankings = pd.DataFrame({
                'WardName': ['Ward A', 'Ward B', 'Ward C', 'Ward D', 'Ward E', 'Ward F'],
                'vulnerability_category': ['High', 'High', 'Medium', 'Medium', 'Low', 'Low']
            })
            print(f"   Mock data created with {len(self.vulnerability_rankings)} rows")
            print(f"   Columns: {list(self.vulnerability_rankings.columns)}")
            print(f"   Categories: {self.vulnerability_rankings['vulnerability_category'].value_counts().to_dict()}")
    
    data_handler1 = MockDataHandler1()
    
    result = {
        'status': 'success',
        'variables_used': ['pfpr', 'rainfall'],
        'selection_method': 'llm_selected',
        'summary': {}
    }
    
    response = message_service._generate_analysis_success_response(result, 'test_session', data_handler1)
    print(f"   Result: {response['status']}")
    
    # Check if vulnerability counts are extracted correctly
    if '2 wards classified as <strong>High</strong>' in response['response']:
        print("   ✓ High vulnerability count extracted correctly")
    else:
        print("   ✗ High vulnerability count NOT extracted correctly")
        print(f"   Response snippet: {response['response'][:500]}...")
    
    print("\n2. Testing with None vulnerability_rankings:")
    
    class MockDataHandler2:
        def __init__(self):
            self.vulnerability_rankings = None
            print("   Mock data created with None vulnerability_rankings")
    
    data_handler2 = MockDataHandler2()
    response2 = message_service._generate_analysis_success_response(result, 'test_session', data_handler2)
    
    if '0 wards classified' in response2['response']:
        print("   ✓ Correctly shows 0 when vulnerability_rankings is None")
    else:
        print("   ✗ Unexpected behavior when vulnerability_rankings is None")
    
    print("\n3. Testing with missing vulnerability_category column:")
    
    class MockDataHandler3:
        def __init__(self):
            self.vulnerability_rankings = pd.DataFrame({
                'WardName': ['Ward A', 'Ward B', 'Ward C'],
                'some_other_column': ['A', 'B', 'C']
            })
            print(f"   Mock data created with columns: {list(self.vulnerability_rankings.columns)}")
    
    data_handler3 = MockDataHandler3()
    response3 = message_service._generate_analysis_success_response(result, 'test_session', data_handler3)
    
    if '0 wards classified' in response3['response']:
        print("   ✓ Correctly shows 0 when vulnerability_category column is missing")
    else:
        print("   ✗ Unexpected behavior when vulnerability_category column is missing")
    
    print("\n4. Testing what happens when data_handler is None:")
    
    response4 = message_service._generate_analysis_success_response(result, 'test_session', None)
    
    if '0 wards classified' in response4['response']:
        print("   ✓ Correctly shows 0 when data_handler is None")
    else:
        print("   ✗ Unexpected behavior when data_handler is None")
    
    print("\n=== Summary ===")
    print("If the real analysis is showing 0 wards, it means one of:")
    print("1. data_handler is None")
    print("2. data_handler.vulnerability_rankings is None") 
    print("3. vulnerability_rankings DataFrame is missing 'vulnerability_category' column")
    print("4. vulnerability_rankings DataFrame is empty")
    
    print("\nTo fix this, we need to check:")
    print("- Is the analysis pipeline properly populating vulnerability_rankings?")
    print("- Is the data_handler being passed correctly to the message service?")
    print("- Are the column names in vulnerability_rankings correct?")

if __name__ == "__main__":
    debug_vulnerability_extraction() 