"""
Knowledge and Explanation Tools for ChatMRPT - Pydantic Models

Converted from legacy function-based tools to Pydantic models for improved
parameter validation, schema generation, and tool selection accuracy.

These tools provide expert malaria epidemiology knowledge and explanations
using LLM capabilities with domain expertise and session context.
"""

import logging
import time
from typing import Dict, Any, Optional, List
from pydantic import Field, validator
from flask import current_app

from .base import (
    KnowledgeTool, ToolExecutionResult, ToolCategory,
    validate_session_data_exists
)

logger = logging.getLogger(__name__)


def _get_session_context(session_id: str) -> Optional[Dict[str, Any]]:
    """Get session context for personalized explanations."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        
        if unified_gdf is not None:
            context = {
                'total_wards': len(unified_gdf),
                'available_variables': list(unified_gdf.columns)[:20],
                'has_composite_analysis': any('composite' in col.lower() for col in unified_gdf.columns),
                'has_pca_analysis': any('pca' in col.lower() for col in unified_gdf.columns),
                'numeric_variables': list(unified_gdf.select_dtypes(include=['number']).columns)[:10]
            }
            
            # Add health variable example
            health_vars = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['tpr', 'malaria', 'prevalence'])]
            if health_vars:
                var = health_vars[0]
                if unified_gdf[var].dtype in ['number']:
                    context['health_variable_example'] = {
                        'variable': var,
                        'mean': float(unified_gdf[var].mean()),
                        'range': [float(unified_gdf[var].min()), float(unified_gdf[var].max())]
                    }
            
            return context
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting session context: {e}")
        return None


# Pydantic Tool Classes
class SimpleGreeting(KnowledgeTool):
    """
    Provide friendly greetings and introductions for ChatMRPT.
    
    This tool generates appropriate greetings and introductions based on
    the type of interaction and user context.
    """
    
    greeting_type: str = Field(
        "hello",
        description="Type of greeting: 'hello', 'hi', 'who_are_you', 'what_can_you_do'",
        pattern="^(hello|hi|who_are_you|what_can_you_do)$"
    )
    
    include_capabilities: bool = Field(
        True,
        description="Include brief overview of ChatMRPT capabilities in greeting"
    )
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Hello",
            "Hi there",
            "Who are you?",
            "What can you do?",
            "Introduce yourself"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute greeting generation"""
        start_time = time.time()
        
        try:
            greetings = {
                "hello": "Hello there! I'm ChatMRPT, your malaria risk assessment assistant. How can I help you today?",
                "hi": "Hi! I'm ChatMRPT, here to help you with malaria risk analysis. What would you like to explore?",
                "who_are_you": "Hello! I'm ChatMRPT, a malaria risk prioritization tool designed to help with urban microstratification analysis. How can I assist you?",
                "what_can_you_do": "I can help you analyze malaria risk data, create vulnerability maps, rank wards by risk, and generate visualizations for targeted intervention planning. What would you like to start with?"
            }
            
            base_message = greetings.get(self.greeting_type, greetings["hello"])
            
            if self.include_capabilities and self.greeting_type in ["hello", "hi"]:
                capabilities_addon = " I specialize in vulnerability analysis, risk mapping, and intervention targeting for malaria control."
                base_message += capabilities_addon
            
            result_data = {
                'greeting': base_message,
                'greeting_type': self.greeting_type,
                'include_capabilities': self.include_capabilities
            }
            
            message = "Simple greeting generated"
            execution_time = time.time() - start_time
            
            return self._create_success_result(
                message=message,
                data=result_data,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error generating greeting: {e}")
            return self._create_error_result(
                f"Error generating greeting: {str(e)}",
                error_details=f"Exception: {type(e).__name__}",
                execution_time=execution_time
            )


class ExplainConcept(KnowledgeTool):
    """
    Explain malaria epidemiology concepts with domain expertise.
    
    This tool provides comprehensive explanations of malaria-related concepts
    using expert knowledge and session context for personalization.
    """
    
    concept: str = Field(
        ...,
        description="Concept or topic to explain",
        min_length=1,
        max_length=200
    )
    
    include_context: bool = Field(
        True,
        description="Include session data context in the explanation"
    )
    
    technical_level: str = Field(
        "intermediate",
        description="Technical level: 'basic', 'intermediate', 'advanced'",
        pattern="^(basic|intermediate|advanced)$"
    )
    
    focus_area: Optional[str] = Field(
        None,
        description="Specific focus area: 'practical', 'research', 'policy', 'intervention'",
        max_length=50
    )
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Explain malaria transmission",
            "What is urban microstratification?",
            "Explain vector ecology",
            "What are bed nets?",
            "How does ChatMRPT work?"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute concept explanation"""
        start_time = time.time()
        
        try:
            llm_manager = current_app.services.llm_manager
            if not llm_manager:
                return self._create_error_result("LLM service not available")
            
            # Get session context if requested
            session_context = None
            if self.include_context:
                session_context = _get_session_context(session_id)
            
            # Create adaptive system prompt based on concept and context
            if self.concept.lower() in ['chatmrpt', 'system capabilities', 'what can you do']:
                system_prompt = f"""
                You are a malaria epidemiologist embedded in ChatMRPT, an advanced malaria risk assessment system.
                
                Explain ChatMRPT's capabilities at a {self.technical_level} level, naturally and conversationally:
                • Urban microstratification for malaria control
                • Composite Risk Scoring and PCA analysis methods  
                • Ward-level vulnerability mapping and ranking
                • Data-driven intervention targeting
                
                Keep your ChatMRPT persona but be warm and helpful, not robotic.
                Reference the user's specific data when available.
                """
            elif any(keyword in self.concept.lower() for keyword in ['how to use', 'upload data', 'data accept', 'getting started', 'data format']):
                system_prompt = f"""
                You are a malaria epidemiologist embedded in ChatMRPT. The user is asking about practical usage - how to upload data and get started.
                
                Provide COMPREHENSIVE, step-by-step guidance at a {self.technical_level} level covering:
                
                1. DATA REQUIREMENTS and formats
                2. STEP-BY-STEP UPLOAD PROCESS
                3. WHAT HAPPENS AFTER UPLOAD
                4. EXAMPLE DATA STRUCTURE
                
                Be practical, specific, and maintain your epidemiologist persona.
                """
            else:
                system_prompt = f"""
                You are a malaria epidemiologist working with ChatMRPT, a malaria risk assessment system.
                
                Your expertise covers all aspects of malaria and public health at a {self.technical_level} level:
                • Malaria biology, transmission, and control
                • Vector ecology and environmental factors  
                • Epidemiology and disease surveillance
                • Public health interventions and policy
                • Urban microstratification and spatial analysis
                
                RESPONSE GUIDELINES:
                • Expert but conversational - like a knowledgeable colleague
                • Educational and engaging, not textbook-like
                • Use accessible language while maintaining scientific accuracy
                • Focus on practical applications and real-world relevance
                {f'• Emphasize {self.focus_area} applications' if self.focus_area else ''}
                
                Answer comprehensively but efficiently - provide maximum value in minimum words.
                """
            
            user_prompt = f'Please provide a comprehensive explanation of: "{self.concept}"'
            
            # Add context if available - let LLM weave it in naturally
            if session_context:
                context_details = []
                context_details.append(f"The user has uploaded data for {session_context['total_wards']} wards")
                
                if session_context['has_composite_analysis'] and session_context['has_pca_analysis']:
                    context_details.append("They have run both composite and PCA risk analyses")
                elif session_context['has_composite_analysis']:
                    context_details.append("They have run composite risk analysis")
                elif session_context['has_pca_analysis']:
                    context_details.append("They have run PCA risk analysis")
                
                context_details.append(f"Their dataset includes variables like: {', '.join(session_context['available_variables'][:5])}")
                
                user_prompt += f"\n\nUser Context: {' | '.join(context_details)}. Naturally reference their data when relevant to make your response personal to their situation."
            
            explanation = llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.7,
                max_tokens=1500,
                session_id=session_id
            )
            
            result_data = {
                'concept': self.concept,
                'explanation': explanation,
                'technical_level': self.technical_level,
                'focus_area': self.focus_area,
                'personalized': session_context is not None,
                'include_context': self.include_context
            }
            
            message = f"Explanation generated for concept: {self.concept}"
            execution_time = time.time() - start_time
            
            return self._create_success_result(
                message=message,
                data=result_data,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error explaining concept: {e}")
            return self._create_error_result(
                f"Error generating explanation: {str(e)}",
                error_details=f"Exception: {type(e).__name__}",
                execution_time=execution_time
            )


# Legacy function wrappers for backward compatibility during transition
def simple_greeting(session_id: str, greeting_type: str = "hello") -> Dict[str, Any]:
    """Legacy wrapper - calls Pydantic version"""
    tool = SimpleGreeting(greeting_type=greeting_type)
    result = tool.execute(session_id)
    return {
        'status': 'success' if result.success else 'error',
        'message': result.message,
        **(result.data or {})
    }

def explain_concept(session_id: str, concept: str, include_context: bool = True) -> Dict[str, Any]:
    """Legacy wrapper - calls Pydantic version"""
    tool = ExplainConcept(concept=concept, include_context=include_context)
    result = tool.execute(session_id)
    return {
        'status': 'success' if result.success else 'error',
        'message': result.message,
        **(result.data or {})
    }


# Import methodology explanation tool from its dedicated module
from .methodology_explanation_tools import ExplainAnalysisMethodology

# Add legacy wrapper for methodology explanations
def explain_analysis_methodology(session_id: str, methods: List[str] = None, 
                                explanation_depth: str = "detailed", **kwargs) -> Dict[str, Any]:
    """Legacy wrapper for methodology explanation tool"""
    tool = ExplainAnalysisMethodology(
        methods=methods or ["both"], 
        explanation_depth=explanation_depth,
        **kwargs
    )
    result = tool.execute(session_id)
    return {
        'status': 'success' if result.success else 'error',
        'message': result.message,
        **(result.data or {})
    }