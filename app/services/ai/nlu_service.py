"""
Natural Language Understanding service for ChatMRPT.

This service provides specialized NLU functionality for understanding
user intents and extracting entities in the malaria analysis context.
"""

import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class NLUService:
    """
    Service for natural language understanding operations.
    
    This service specializes in understanding user intents and extracting
    relevant entities for the ChatMRPT application.
    """
    
    def __init__(self, llm_manager=None):
        """
        Initialize the NLU service.
        
        Args:
            llm_manager: LLM manager instance for intent classification
        """
        self.llm_manager = llm_manager
        
        # Define intent patterns and keywords
        self.intent_patterns = {
            'run_standard_analysis': [
                'run analysis', 'start analysis', 'analyze', 'run the analysis',
                'begin analysis', 'perform analysis', 'execute analysis'
            ],
            'run_custom_analysis': [
                'analyze with', 'use variables', 'custom analysis', 'specific variables',
                'analyze using', 'with these variables'
            ],
            'request_visualization': [
                'show map', 'create map', 'visualize', 'plot', 'chart', 'graph',
                'show visualization', 'display', 'view map'
            ],
            'explain_variable': [
                'explain variable', 'what is', 'define', 'describe variable',
                'tell me about', 'variable explanation'
            ],
            'explain_methodology': [
                'how does', 'methodology', 'explain method', 'how it works',
                'explain approach', 'calculation method'
            ],
            'generate_report': [
                'generate report', 'create report', 'download report', 'export report',
                'report generation', 'produce report'
            ],
            'load_sample_data': [
                'sample data', 'demo data', 'example data', 'test data',
                'try sample', 'use example'
            ]
        }
        
        # Variable-related keywords
        self.variable_keywords = [
            'population', 'healthcare', 'climate', 'rainfall', 'temperature',
            'altitude', 'vegetation', 'water', 'poverty', 'education',
            'infrastructure', 'urban', 'rural', 'density', 'access'
        ]
        
        # Visualization keywords
        self.visualization_keywords = {
            'map_types': ['choropleth', 'heat map', 'composite', 'variable map'],
            'plot_types': ['scatter', 'box plot', 'histogram', 'bar chart'],
            'threshold_keywords': ['above', 'below', 'higher than', 'lower than', 'threshold']
        }
    
    def extract_intent_and_entities(self, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extract intent and entities from user message.
        
        Args:
            message: User message text
            context: Optional context information
            
        Returns:
            Dictionary with intent, entities, and confidence
        """
        if not message:
            return {
                'intent': 'clarification_needed',
                'entities': {},
                'confidence': 0.0
            }
        
        message_lower = message.lower().strip()
        
        # Try rule-based classification first
        rule_based_result = self._classify_intent_rules(message_lower)
        
        # If rule-based classification is confident, use it
        if rule_based_result['confidence'] > 0.7:
            entities = self._extract_entities(message_lower, rule_based_result['intent'])
            return {
                'intent': rule_based_result['intent'],
                'entities': entities,
                'confidence': rule_based_result['confidence']
            }
        
        # Otherwise, use LLM for classification
        if self.llm_manager:
            try:
                llm_result = self.llm_manager.extract_intent_and_entities(message, context)
                if llm_result:
                    return llm_result
            except Exception as e:
                logger.error(f"Error in LLM intent extraction: {str(e)}", exc_info=True)
        
        # Fallback to rule-based result
        entities = self._extract_entities(message_lower, rule_based_result['intent'])
        return {
            'intent': rule_based_result['intent'],
            'entities': entities,
            'confidence': rule_based_result['confidence']
        }
    
    def _classify_intent_rules(self, message_lower: str) -> Dict[str, Any]:
        """
        Classify intent using rule-based patterns.
        
        Args:
            message_lower: Lowercase message text
            
        Returns:
            Dictionary with intent and confidence
        """
        best_intent = 'clarification_needed'
        best_confidence = 0.0
        
        for intent, patterns in self.intent_patterns.items():
            confidence = 0.0
            
            for pattern in patterns:
                if pattern in message_lower:
                    # Higher confidence for exact matches
                    confidence = max(confidence, 0.8)
                elif any(word in message_lower for word in pattern.split()):
                    # Lower confidence for partial matches
                    confidence = max(confidence, 0.4)
            
            if confidence > best_confidence:
                best_confidence = confidence
                best_intent = intent
        
        # Special cases for common variations
        if any(word in message_lower for word in ['yes', 'confirm', 'ok', 'sure', 'proceed']):
            return {'intent': 'confirm_custom_analysis', 'confidence': 0.9}
        elif any(word in message_lower for word in ['no', 'cancel', 'stop', 'abort']):
            return {'intent': 'cancel_custom_analysis', 'confidence': 0.9}
        elif any(word in message_lower for word in ['hello', 'hi', 'hey', 'greetings']):
            return {'intent': 'greet', 'confidence': 0.9}
        elif any(word in message_lower for word in ['help', 'what can you do', 'capabilities']):
            return {'intent': 'request_help', 'confidence': 0.8}
        
        return {'intent': best_intent, 'confidence': best_confidence}
    
    def _extract_entities(self, message_lower: str, intent: str) -> Dict[str, Any]:
        """
        Extract entities based on intent and message content.
        
        Args:
            message_lower: Lowercase message text
            intent: Classified intent
            
        Returns:
            Dictionary of extracted entities
        """
        entities = {}
        
        # Extract variables for custom analysis
        if intent == 'run_custom_analysis':
            variables = self._extract_variables(message_lower)
            if variables:
                entities['variable_names'] = variables
        
        # Extract single variable for explanations
        elif intent == 'explain_variable':
            variable = self._extract_single_variable(message_lower)
            if variable:
                entities['variable_name'] = variable
        
        # Extract visualization parameters
        elif intent == 'request_visualization':
            viz_entities = self._extract_visualization_entities(message_lower)
            entities.update(viz_entities)
        
        # Extract methodology type
        elif intent == 'explain_methodology':
            method = self._extract_methodology_type(message_lower)
            if method:
                entities['methodology_type'] = method
        
        # Extract report format
        elif intent == 'generate_report':
            format_type = self._extract_report_format(message_lower)
            if format_type:
                entities['report_format'] = format_type
        
        return entities
    
    def _extract_variables(self, message_lower: str) -> List[str]:
        """Extract variable names from message."""
        variables = []
        
        for keyword in self.variable_keywords:
            if keyword in message_lower:
                variables.append(keyword)
        
        # Look for quoted variables
        import re
        quoted_vars = re.findall(r'"([^"]*)"', message_lower)
        variables.extend(quoted_vars)
        
        return variables
    
    def _extract_single_variable(self, message_lower: str) -> Optional[str]:
        """Extract a single variable name from message."""
        for keyword in self.variable_keywords:
            if keyword in message_lower:
                return keyword
        
        # Look for quoted variable
        import re
        quoted_match = re.search(r'"([^"]*)"', message_lower)
        if quoted_match:
            return quoted_match.group(1)
        
        return None
    
    def _extract_visualization_entities(self, message_lower: str) -> Dict[str, Any]:
        """Extract visualization-related entities."""
        entities = {}
        
        # Map type
        for map_type in self.visualization_keywords['map_types']:
            if map_type in message_lower:
                entities['map_type'] = map_type.replace(' ', '_')
                break
        
        # Plot type
        for plot_type in self.visualization_keywords['plot_types']:
            if plot_type in message_lower:
                entities['plot_type'] = plot_type.replace(' ', '_')
                break
        
        # Variable for visualization
        variable = self._extract_single_variable(message_lower)
        if variable:
            entities['variable_for_viz'] = variable
        
        # Threshold values
        import re
        threshold_match = re.search(r'(\d+(?:\.\d+)?)', message_lower)
        if threshold_match and any(kw in message_lower for kw in self.visualization_keywords['threshold_keywords']):
            entities['threshold_value'] = float(threshold_match.group(1))
        
        return entities
    
    def _extract_methodology_type(self, message_lower: str) -> Optional[str]:
        """Extract methodology type from message."""
        methodology_types = [
            'composite scoring', 'variable selection', 'risk assessment',
            'spatial analysis', 'clustering', 'normalization'
        ]
        
        for method in methodology_types:
            if method in message_lower:
                return method.replace(' ', '_')
        
        return None
    
    def _extract_report_format(self, message_lower: str) -> Optional[str]:
        """Extract report format from message."""
        formats = ['pdf', 'html', 'markdown', 'word', 'excel']
        
        for fmt in formats:
            if fmt in message_lower:
                return fmt
        
        return 'pdf'  # Default format
    
    def get_intent_suggestions(self, partial_message: str) -> List[str]:
        """
        Get intent suggestions for partial message.
        
        Args:
            partial_message: Partial user message
            
        Returns:
            List of suggested intents
        """
        suggestions = []
        partial_lower = partial_message.lower()
        
        for intent, patterns in self.intent_patterns.items():
            for pattern in patterns:
                if pattern.startswith(partial_lower) or partial_lower in pattern:
                    suggestions.append(intent)
                    break
        
        return list(set(suggestions))  # Remove duplicates
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check health of the NLU service.
        
        Returns:
            Health status information
        """
        return {
            'status': 'healthy',
            'llm_manager_available': self.llm_manager is not None,
            'intent_patterns_loaded': len(self.intent_patterns),
            'variable_keywords_loaded': len(self.variable_keywords)
        } 