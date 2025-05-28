"""
Test script to check intent extraction for visualization requests.
"""
from app.services.message_service import MessageService
from app.core.llm_manager import LLMManager

def test_intent_extraction():
    print("=== Testing Intent Extraction for Visualization Requests ===")
    
    # Create message service
    message_service = MessageService()
    
    # Test messages
    test_messages = [
        "Show composite map",
        "show composite map", 
        "Display composite map",
        "I want to see the composite map",
        "Generate composite map",
        "Create composite map",
        "Show the composite map please",
        "Show map for rainfall",
        "Show vulnerability map",
        "Show decision tree"
    ]
    
    for message in test_messages:
        print(f"\n--- Testing: '{message}' ---")
        
        # Test intent extraction
        try:
            nlu_result = message_service._process_user_intent(message, {}, 'test_session')
            if nlu_result:
                print(f"   Intent: {nlu_result.get('intent', 'None')}")
                print(f"   Entities: {nlu_result.get('entities', {})}")
                
                # Check if it's a visualization request
                if nlu_result.get('intent') == 'request_visualization':
                    print("   ✓ Correctly identified as visualization request")
                else:
                    print(f"   ✗ NOT identified as visualization request (got: {nlu_result.get('intent')})")
            else:
                print("   ✗ No result from intent extraction")
        except Exception as e:
            print(f"   ✗ Error: {str(e)}")
    
    print("\n=== Summary ===")
    print("If visualization requests aren't being identified correctly,")
    print("the LLM intent extraction needs improvement or rule-based")
    print("fallbacks need to be added to the message service.")

if __name__ == "__main__":
    test_intent_extraction() 