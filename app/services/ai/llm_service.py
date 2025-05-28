"""
AI service for language model operations in ChatMRPT.

This service provides a clean interface for AI operations including
text generation, explanations, and analysis.
"""

import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class AIService:
    """
    Service for AI operations and language model interactions.
    
    This service wraps LLM functionality and provides specialized methods
    for ChatMRPT use cases.
    """
    
    def __init__(self, llm_manager=None, interaction_logger=None):
        """
        Initialize the AI service.
        
        Args:
            llm_manager: LLM manager instance
            interaction_logger: Logger for user interactions
        """
        self.llm_manager = llm_manager
        self.interaction_logger = interaction_logger
    
    def generate_explanation(self, session_id: str, entity_type: str, entity_name: str, 
                           question: Optional[str] = None, context: Optional[Dict] = None) -> str:
        """
        Generate an explanation for an entity (ward, variable, methodology, etc.).
        
        Args:
            session_id: User session ID
            entity_type: Type of entity (ward, variable, methodology, visualization)
            entity_name: Name of the entity
            question: Optional specific question
            context: Optional context information
            
        Returns:
            Generated explanation text
        """
        if not self.llm_manager:
            return "AI service is not available. Please check the configuration."
        
        try:
            # Route to appropriate explanation method
            if entity_type == 'ward':
                explanation = self.llm_manager.explain_ward(session_id, entity_name, question, context)
            elif entity_type == 'variable':
                explanation = self.llm_manager.explain_variable(session_id, entity_name, question, context)
            elif entity_type == 'methodology':
                explanation = self.llm_manager.explain_methodology(session_id, entity_name, question, context)
            elif entity_type == 'visualization':
                explanation = self.llm_manager.explain_visualization(session_id, entity_name, context, question)
            else:
                explanation = self.llm_manager.generate_general_response(session_id, 
                    f"Please explain {entity_name} in the context of {entity_type}", context)
            
            # Log the explanation
            if self.interaction_logger:
                self.interaction_logger.log_explanation(
                    session_id=session_id,
                    entity_type=entity_type,
                    entity_name=entity_name,
                    question_type='explanation_request',
                    question=question,
                    explanation=explanation
                )
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error generating explanation: {str(e)}", exc_info=True)
            return f"I'm sorry, I encountered an error while generating the explanation: {str(e)}"
    
    def generate_variable_selection_explanation(self, session_id: str, variables: List[str], 
                                              context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate explanation for why certain variables were selected.
        
        Args:
            session_id: User session ID
            variables: List of selected variables
            context: Optional context information
            
        Returns:
            Dictionary with explanations for each variable
        """
        if not self.llm_manager:
            return {
                'status': 'error',
                'message': 'AI service is not available'
            }
        
        try:
            if hasattr(self.llm_manager, 'explain_variable_selection'):
                result = self.llm_manager.explain_variable_selection(
                    variables, None, context, session_id
                )
                return {
                    'status': 'success',
                    'explanations': result
                }
            else:
                # Fallback implementation
                explanations = {}
                for var in variables:
                    explanations[var] = self.generate_explanation(
                        session_id, 'variable', var, 
                        "Why was this variable selected for malaria risk analysis?", 
                        context
                    )
                
                return {
                    'status': 'success',
                    'explanations': explanations
                }
            
        except Exception as e:
            logger.error(f"Error generating variable selection explanation: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating explanation: {str(e)}'
            }
    
    def generate_response(self, session_id: str, prompt: str, context: Optional[Dict] = None,
                         system_message: Optional[str] = None, temperature: float = 0.7) -> str:
        """
        Generate a general AI response.
        
        Args:
            session_id: User session ID
            prompt: User prompt
            context: Optional context information
            system_message: Optional system message
            temperature: Generation temperature
            
        Returns:
            Generated response text
        """
        if not self.llm_manager:
            return "AI service is not available. Please check the configuration."
        
        try:
            response = self.llm_manager.generate_response(
                prompt=prompt,
                context=context,
                system_message=system_message,
                temperature=temperature,
                session_id=session_id
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error generating AI response: {str(e)}", exc_info=True)
            return f"I'm sorry, I encountered an error: {str(e)}"
    
    def analyze_user_intent(self, session_id: str, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Analyze user intent from a message.
        
        Args:
            session_id: User session ID
            message: User message
            context: Optional context information
            
        Returns:
            Dictionary with intent and entities
        """
        if not self.llm_manager:
            return {
                'intent': 'clarification_needed',
                'entities': {},
                'confidence': 0.0
            }
        
        try:
            result = self.llm_manager.extract_intent_and_entities(message, context)
            return result or {
                'intent': 'clarification_needed',
                'entities': {},
                'confidence': 0.0
            }
            
        except Exception as e:
            logger.error(f"Error analyzing user intent: {str(e)}", exc_info=True)
            return {
                'intent': 'clarification_needed',
                'entities': {},
                'confidence': 0.0,
                'error': str(e)
            }
    
    def select_optimal_variables(self, session_id: str, available_vars: List[str], 
                                csv_data, relationships: Optional[Dict] = None,
                                min_vars: int = 3, max_vars: int = 5) -> Dict[str, Any]:
        """
        Select optimal variables for analysis using LLM.
        
        Args:
            session_id: User session ID
            available_vars: List of available variables
            csv_data: CSV data for analysis
            relationships: Optional variable relationships
            min_vars: Minimum number of variables
            max_vars: Maximum number of variables
            
        Returns:
            Dictionary with selected variables and explanations
        """
        if not self.llm_manager:
            # Fallback: select first few numeric variables
            numeric_vars = [var for var in available_vars if var not in ['WardName', 'geometry']]
            selected = numeric_vars[:min_vars] if len(numeric_vars) >= min_vars else numeric_vars
            return selected, {var: f"Selected {var} for analysis" for var in selected}
        
        try:
            # Use the LLM to select variables
            if hasattr(self, '_select_optimal_variables_with_llm'):
                return self._select_optimal_variables_with_llm(
                    self.llm_manager, available_vars, csv_data, relationships, min_vars, max_vars
                )
            else:
                # Fallback implementation
                numeric_vars = [var for var in available_vars if var not in ['WardName', 'geometry']]
                selected = numeric_vars[:max_vars] if len(numeric_vars) >= max_vars else numeric_vars
                explanations = {}
                for var in selected:
                    explanations[var] = self.generate_explanation(
                        session_id, 'variable', var, 
                        "Why is this variable important for malaria risk analysis?", 
                        {'available_vars': available_vars}
                    )
                return selected, explanations
            
        except Exception as e:
            logger.error(f"Error selecting optimal variables: {str(e)}", exc_info=True)
            # Fallback selection
            numeric_vars = [var for var in available_vars if var not in ['WardName', 'geometry']]
            selected = numeric_vars[:min_vars] if len(numeric_vars) >= min_vars else numeric_vars
            return selected, {var: f"Selected {var} for analysis (fallback)" for var in selected}

    def convert_markdown_to_html(self, markdown_text: str) -> str:
        """
        Convert markdown text to HTML.
        
        Args:
            markdown_text: Markdown text to convert
            
        Returns:
            HTML string
        """
        if not markdown_text:
            return ""
        
        try:
            import markdown
            
            # Configure markdown with extensions
            md = markdown.Markdown(
                extensions=[
                    'tables',
                    'fenced_code',
                    'codehilite',
                    'toc',
                    'nl2br'
                ],
                extension_configs={
                    'codehilite': {
                        'css_class': 'highlight'
                    }
                }
            )
            
            html = md.convert(markdown_text)
            return html
            
        except ImportError:
            # Fallback: basic HTML conversion
            html = markdown_text
            html = html.replace('\n\n', '</p><p>')
            html = html.replace('\n', '<br>')
            html = html.replace('**', '<strong>').replace('**', '</strong>')
            html = html.replace('*', '<em>').replace('*', '</em>')
            html = f'<p>{html}</p>'
            return html
        except Exception as e:
            logger.error(f"Error converting markdown to HTML: {str(e)}")
            return markdown_text
    
    def get_llm_manager(self):
        """
        Get the LLM manager instance.
        
        Returns:
            LLM manager instance
        """
        return self.llm_manager

    def health_check(self) -> Dict[str, Any]:
        """
        Check health of the AI service.
        
        Returns:
            Health status information
        """
        status = {
            'status': 'healthy',
            'llm_manager_available': self.llm_manager is not None,
            'interaction_logger_available': self.interaction_logger is not None
        }
        
        if self.llm_manager:
            try:
                # Test LLM connectivity
                test_response = self.llm_manager.generate_response(
                    "Test", max_tokens=10, temperature=0
                )
                status['llm_connectivity'] = 'working' if test_response else 'failed'
            except Exception as e:
                status['llm_connectivity'] = f'error: {str(e)}'
        else:
            status['llm_connectivity'] = 'unavailable'
        
        return status 