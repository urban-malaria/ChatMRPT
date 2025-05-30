#!/usr/bin/env python3
"""
Integration Example: Advanced Intent Recognition

This example shows how the new advanced intent recognition system
works alongside the existing intent recognition infrastructure.
"""

from typing import Dict, Any, Optional


class IntegrationExample:
    """
    Example showing how advanced intent recognition integrates with existing system
    """
    
    def __init__(self):
        """Initialize both old and new systems"""
        
        # Existing system components (unchanged)
        self.nlu_service = self._create_mock_nlu_service()
        self.llm_manager = self._create_mock_llm_manager()
        
        # New advanced system (added on top)
        try:
            from app.services.advanced_intent_recognition import AdvancedIntentRecognizer
            self.advanced_recognizer = AdvancedIntentRecognizer(
                llm_manager=self.llm_manager,
                use_embeddings=True
            )
            self.has_advanced = True
            print("✅ Advanced intent recognition available")
        except ImportError:
            self.has_advanced = False
            print("⚠️ Advanced intent recognition not available - using existing system")
    
    def process_message_hybrid(self, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Process message using hybrid approach:
        1. Try advanced system first (if available)
        2. Fall back to existing system if needed
        """
        
        print(f"\n🔍 Processing: '{message}'")
        
        # METHOD 1: Try Advanced System First
        if self.has_advanced:
            try:
                advanced_result = self.advanced_recognizer.recognize_intent(message, context)
                
                # Use advanced result if confident
                if advanced_result.confidence > 0.5:
                    print(f"✨ Advanced System: {advanced_result.intent} "
                          f"(confidence: {advanced_result.confidence:.2f}, "
                          f"methods: {advanced_result.method_used})")
                    
                    return {
                        "system_used": "advanced",
                        "intent": advanced_result.intent,
                        "confidence": advanced_result.confidence,
                        "category": advanced_result.category.value,
                        "entities": advanced_result.entities,
                        "methods": advanced_result.method_used,
                        "alternatives": advanced_result.alternative_intents
                    }
                
            except Exception as e:
                print(f"⚠️ Advanced system error: {e}")
        
        # METHOD 2: Fall Back to Existing System
        print("🔄 Using existing system (fallback)")
        
        # Use existing NLU service
        nlu_result = self.nlu_service.extract_intent_and_entities(message, context)
        
        # Or use existing LLM manager
        llm_result = self.llm_manager.extract_intent_and_entities(message, context)
        
        # Choose best result from existing systems
        if nlu_result['confidence'] > llm_result.get('confidence', 0):
            chosen_result = nlu_result
            system_used = "nlu_service"
        else:
            chosen_result = llm_result
            system_used = "llm_manager"
        
        print(f"🔧 Existing System ({system_used}): {chosen_result['intent']} "
              f"(confidence: {chosen_result.get('confidence', 0):.2f})")
        
        return {
            "system_used": system_used,
            "intent": chosen_result['intent'],
            "confidence": chosen_result.get('confidence', 0),
            "entities": chosen_result.get('entities', {}),
            "fallback_reason": "low_confidence" if self.has_advanced else "advanced_unavailable"
        }
    
    def compare_systems(self, test_messages: list) -> None:
        """Compare how different systems handle the same messages"""
        
        print("\n" + "="*80)
        print("🆚 SYSTEM COMPARISON")
        print("="*80)
        
        for message in test_messages:
            print(f"\n📝 Message: '{message}'")
            print("-" * 60)
            
            # Test existing NLU service
            nlu_result = self.nlu_service.extract_intent_and_entities(message)
            print(f"🔧 NLU Service:     {nlu_result['intent']} (conf: {nlu_result['confidence']:.2f})")
            
            # Test existing LLM manager  
            llm_result = self.llm_manager.extract_intent_and_entities(message)
            print(f"🤖 LLM Manager:     {llm_result['intent']} (conf: {llm_result.get('confidence', 0):.2f})")
            
            # Test advanced system (if available)
            if self.has_advanced:
                advanced_result = self.advanced_recognizer.recognize_intent(message)
                print(f"✨ Advanced System: {advanced_result.intent} (conf: {advanced_result.confidence:.2f})")
                print(f"   Methods used: {advanced_result.method_used}")
                print(f"   Category: {advanced_result.category.value}")
            
            # Show hybrid decision
            hybrid_result = self.process_message_hybrid(message)
            print(f"🎯 Hybrid Choice:   {hybrid_result['intent']} ({hybrid_result['system_used']})")
    
    def _create_mock_nlu_service(self):
        """Create mock of existing NLU service"""
        class MockNLUService:
            def extract_intent_and_entities(self, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
                # Simplified version of existing NLU logic
                message_lower = message.lower()
                
                if 'analysis' in message_lower or 'analyze' in message_lower:
                    return {'intent': 'run_standard_analysis', 'confidence': 0.8, 'entities': {}}
                elif 'map' in message_lower or 'visualiz' in message_lower:
                    return {'intent': 'request_visualization', 'confidence': 0.7, 'entities': {}}
                elif 'help' in message_lower:
                    return {'intent': 'request_help', 'confidence': 0.9, 'entities': {}}
                elif any(word in message_lower for word in ['what can you', 'capabilities']):
                    return {'intent': 'request_help', 'confidence': 0.6, 'entities': {}}
                elif 'hello' in message_lower or 'hi' in message_lower:
                    return {'intent': 'greet', 'confidence': 0.9, 'entities': {}}
                else:
                    return {'intent': 'clarification_needed', 'confidence': 0.3, 'entities': {}}
        
        return MockNLUService()
    
    def _create_mock_llm_manager(self):
        """Create mock of existing LLM manager"""
        class MockLLMManager:
            def extract_intent_and_entities(self, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
                # Simplified version of existing LLM logic
                message_lower = message.lower()
                
                # Basic keyword matching (existing fallback logic)
                if any(word in message_lower for word in ['run', 'analysis', 'analyze']):
                    return {"intent": "run_standard_analysis", "entities": {}, "confidence": 0.7}
                elif any(word in message_lower for word in ['visualiz', 'chart', 'map', 'plot']):
                    return {"intent": "request_visualization", "entities": {}, "confidence": 0.7}
                elif any(word in message_lower for word in ['explain', 'why', 'how']):
                    return {"intent": "explain_methodology", "entities": {}, "confidence": 0.6}
                elif any(word in message_lower for word in ['hello', 'hi', 'hey']):
                    return {"intent": "greet", "entities": {}, "confidence": 0.8}
                elif any(word in message_lower for word in ['what can you', 'capabilities']):
                    return {"intent": "request_help", "entities": {}, "confidence": 0.5}
                else:
                    return {"intent": "clarification_needed", "entities": {}, "confidence": 0.2}
        
        return MockLLMManager()


def main():
    """Demonstrate the integration"""
    
    print("🚀 Advanced Intent Recognition Integration Demo")
    print("="*60)
    
    # Initialize the integrated system
    integration = IntegrationExample()
    
    # Test messages that show the differences
    test_messages = [
        "What can you do?",                    # Meta-tool question
        "What files do I need?",               # File requirements
        "Start the analysis",                  # Action request
        "Hello there!",                        # Greeting
        "Tell me about your capabilities",     # Paraphrased capability question
        "How should I prepare my data?",       # File preparation (paraphrased)
        "Can you help me understand this?",    # Help request
        "Run malaria risk assessment",         # Analysis request (specific)
        "I want to see vulnerability rankings" # View results request
    ]
    
    # Show hybrid processing
    print("\n🔀 HYBRID PROCESSING EXAMPLES")
    print("="*60)
    
    for message in test_messages[:3]:
        result = integration.process_message_hybrid(message)
        print(f"Result: {result}")
    
    # Compare all systems
    integration.compare_systems(test_messages)
    
    print("\n" + "="*80)
    print("📋 INTEGRATION SUMMARY")
    print("="*80)
    print("✅ Existing systems continue working unchanged")
    print("✅ Advanced system enhances recognition when available")
    print("✅ Graceful fallback to existing system when needed")
    print("✅ Zero breaking changes to current functionality")
    print("✅ Progressive enhancement as dependencies are added")
    
    print("\n🎯 DEPLOYMENT STRATEGY:")
    print("1. Deploy with basic dependencies (existing system works)")
    print("2. Add sentence-transformers for semantic similarity")  
    print("3. Gradually migrate components to use enhanced service")
    print("4. Monitor confidence scores to tune thresholds")


if __name__ == "__main__":
    main() 