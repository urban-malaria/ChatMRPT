"""
Test script to verify the updated analysis success response formatting.
"""
from app.services.message_service import MessageService
from app.core.llm_manager import LLMManager
import pandas as pd

def test_analysis_response():
    # Test the analysis success response formatting
    message_service = MessageService()
    
    # Test 1: Standard Analysis (LLM-selected variables)
    result_standard = {
        'status': 'success',
        'variables_used': ['pfpr', 'rainfall', 'temperature', 'poverty_rate', 'population_density'],
        'selection_method': 'llm_selected',  # Standard analysis
        'summary': {}
    }
    
    # Test 2: Custom Analysis (User-specified variables)
    result_custom = {
        'status': 'success',
        'variables_used': ['pfpr', 'rainfall', 'temperature'],
        'selection_method': 'user_specified',  # Custom analysis
        'summary': {}
    }
    
    # Create mock data_handler
    class MockDataHandler:
        def __init__(self):
            self.vulnerability_rankings = pd.DataFrame({
                'WardName': ['Ward A', 'Ward B', 'Ward C', 'Ward D', 'Ward E', 'Ward F'],
                'vulnerability_category': ['High', 'High', 'Medium', 'Medium', 'Low', 'Low']
            })
    
    data_handler = MockDataHandler()
    
    # Test Standard Analysis Response
    print("=== Testing Standard Analysis ===")
    response_standard = message_service._generate_analysis_success_response(result_standard, 'test_session', data_handler)
    print('✓ Standard analysis response generated successfully')
    print(f'Response status: {response_standard["status"]}')
    print('\nStandard Analysis Response:')
    print(response_standard['response'])
    
    # Check standard analysis elements
    response_text = response_standard['response']
    assert 'Analysis completed successfully!' in response_text
    assert 'Custom analysis' not in response_text  # Should NOT say custom
    assert 'using AI-selected variables' in response_text
    assert '2 wards classified as <strong>High</strong> vulnerability' in response_text
    
    print('\n✓ Standard analysis format is correct!')
    
    # Test Custom Analysis Response
    print("\n=== Testing Custom Analysis ===")
    response_custom = message_service._generate_analysis_success_response(result_custom, 'test_session', data_handler)
    print('✓ Custom analysis response generated successfully')
    print(f'Response status: {response_custom["status"]}')
    print('\nCustom Analysis Response:')
    print(response_custom['response'])
    
    # Check custom analysis elements
    response_text = response_custom['response']
    assert 'Custom analysis completed successfully!' in response_text
    assert 'with the variables you specified' in response_text
    assert '2 wards classified as <strong>High</strong> vulnerability' in response_text
    
    print('\n✓ Custom analysis format is correct!')
    print('✓ Both standard and custom analysis messages work properly!')

if __name__ == "__main__":
    test_analysis_response() 