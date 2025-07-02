"""
Conversation Router - Phase 6.5 Frontend Integration

This module provides intelligent routing between different conversation processing systems:
- Simple Request Interpreter for basic tool execution
- ReAct Agent for complex reasoning and multi-step analysis
- LangChain Integration for advanced conversation management
- Production monitoring and optimization

Routes user messages to the most appropriate processing system based on:
- Query complexity and intent
- Available data and session state
- User preferences and history
- System load and performance
"""

import logging
import time
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from enum import Enum

from .session_state import SessionState, ConversationMode, needs_agent_processing
# Lazy imports to avoid slow startup - these will be imported when needed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .request_interpreter import RequestInterpreter

logger = logging.getLogger(__name__)


class ProcessingMode(Enum):
    """Processing modes for different types of conversations."""
    SIMPLE_TOOL = "simple_tool"           # Basic tool execution via Request Interpreter
    REACT_AGENT = "react_agent"           # ReAct reasoning for complex analysis
    LANGCHAIN_CHAT = "langchain_chat"     # LangChain conversation management
    HYBRID = "hybrid"                     # Combination of multiple systems


class ConversationRouter:
    """
    Intelligent conversation router that selects the best processing system
    for each user message based on complexity, context, and system state.
    """
    
    def __init__(self):
        """Initialize the conversation router."""
        # Use lazy initialization to avoid startup issues
        self._deployment_manager = None
        self._reflection_engine = None
        self._agent = None
        self._langchain_chain = None
        self.request_interpreter = None  # Will be injected
        
        # Routing statistics
        self.routing_stats = {
            'simple_tool': 0,
            'react_agent': 0,
            'langchain_chat': 0,
            'hybrid': 0,
            'errors': 0
        }
        
        # Performance thresholds
        self.complexity_threshold = 0.7
        self.agent_response_time_limit = 30.0
        self.fallback_enabled = True
        
        logger.info("Conversation router initialized with lazy component loading")
    
    def set_request_interpreter(self, request_interpreter):
        """Set the request interpreter instance."""
        self.request_interpreter = request_interpreter
    
    @property
    def deployment_manager(self):
        """Lazy load deployment manager."""
        if self._deployment_manager is None:
            try:
                from .production_deployment import get_deployment_manager
                self._deployment_manager = get_deployment_manager()
            except Exception as e:
                logger.warning(f"Failed to load deployment manager: {e}")
                self._deployment_manager = None
        return self._deployment_manager
    
    @property
    def reflection_engine(self):
        """Lazy load reflection engine."""
        if self._reflection_engine is None:
            try:
                from .reflection_engine import get_reflection_engine
                self._reflection_engine = get_reflection_engine()
            except Exception as e:
                logger.warning(f"Failed to load reflection engine: {e}")
                self._reflection_engine = None
        return self._reflection_engine
    
    @property
    def agent(self):
        """Lazy load ChatMRPT agent."""
        if self._agent is None:
            try:
                from .chatmrpt_agent import get_chatmrpt_agent
                self._agent = get_chatmrpt_agent()
            except Exception as e:
                logger.warning(f"Failed to load ChatMRPT agent: {e}")
                self._agent = None
        return self._agent
    
    @property
    def langchain_chain(self):
        """Lazy load LangChain conversation chain."""
        if self._langchain_chain is None:
            try:
                from .langchain_integration import get_conversation_chain
                self._langchain_chain = get_conversation_chain()
            except Exception as e:
                logger.warning(f"Failed to load LangChain chain: {e}")
                self._langchain_chain = None
        return self._langchain_chain
    
    def route_conversation(self, user_message: str, session_id: str, 
                          context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Route conversation to the most appropriate processing system.
        
        Args:
            user_message: User's input message
            session_id: Session identifier
            context: Additional context from Flask session
            
        Returns:
            Dict containing response and metadata
        """
        start_time = time.time()
        context = context or {}
        
        try:
            # Create/update session state
            session_state = self._create_session_state(session_id, context)
            
            # Determine optimal processing mode
            processing_mode, reasoning = self._determine_processing_mode(
                user_message, session_state, context
            )
            
            logger.info(f"Routing to {processing_mode.value}: {reasoning}")
            
            # Process message using selected mode
            response = self._process_with_mode(
                processing_mode, user_message, session_id, session_state, context
            )
            
            # Record metrics and learn from interaction
            execution_time = time.time() - start_time
            self._record_interaction_metrics(
                processing_mode, user_message, response, execution_time, session_id
            )
            
            # Update routing statistics
            self.routing_stats[processing_mode.value] += 1
            
            # Add routing metadata to response
            response['routing_info'] = {
                'processing_mode': processing_mode.value,
                'reasoning': reasoning,
                'execution_time': execution_time,
                'session_id': session_id
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Conversation routing failed: {e}")
            self.routing_stats['errors'] += 1
            
            # Fallback to simple processing
            if self.fallback_enabled and self.request_interpreter:
                logger.info("Falling back to simple request interpreter")
                try:
                    fallback_response = self.request_interpreter.process_message(user_message, session_id)
                    fallback_response['routing_info'] = {
                        'processing_mode': 'fallback',
                        'reasoning': f'Error in main routing: {str(e)}',
                        'execution_time': time.time() - start_time
                    }
                    return fallback_response
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}")
            
            # Ultimate fallback
            return {
                'status': 'error',
                'response': 'I encountered an error processing your request. Please try rephrasing your question.',
                'routing_info': {
                    'processing_mode': 'error',
                    'reasoning': str(e),
                    'execution_time': time.time() - start_time
                }
            }
    
    def _create_session_state(self, session_id: str, context: Dict[str, Any]) -> SessionState:
        """Create or update session state from Flask session context."""
        from flask import session
        
        # Extract relevant information from Flask session and context
        csv_loaded = context.get('csv_loaded', session.get('csv_loaded', False))
        shapefile_loaded = context.get('shapefile_loaded', session.get('shapefile_loaded', False))
        analysis_complete = context.get('analysis_complete', session.get('analysis_complete', False))
        analysis_type = context.get('analysis_type', session.get('analysis_type', 'none'))
        
        # Create session state
        session_state = SessionState(session_id=session_id)
        
        # Import the state enums
        from .session_state import DataState, AnalysisState
        
        # Update data state properly
        if csv_loaded and shapefile_loaded:
            session_state.data_state = DataState.LOADED
            session_state.metadata['csv_loaded'] = True
            session_state.metadata['shapefile_loaded'] = True
        elif csv_loaded or shapefile_loaded:
            session_state.data_state = DataState.LOADING  # Use LOADING instead of PARTIAL
        else:
            session_state.data_state = DataState.NOT_LOADED
        
        # Update analysis state properly
        if analysis_complete:
            session_state.analysis_state = AnalysisState.COMPLETED
            session_state.metadata['analysis_type'] = analysis_type
            session_state.metadata['analysis_complete'] = True
        
        # Update conversation state (if method exists)
        if hasattr(session_state, 'start_conversation'):
            session_state.start_conversation(context.get('user_role', 'analyst'))
        
        # Store additional context for routing decisions
        session_state.metadata.update({
            'variables_used': context.get('variables_used', []),
            'last_processing_mode': session.get('last_processing_mode', 'none'),
            'conversation_turns': session_state.conversation_state.total_exchanges
        })
        
        return session_state
    
    def _determine_processing_mode(self, user_message: str, session_state: SessionState, 
                                 context: Dict[str, Any]) -> Tuple[ProcessingMode, str]:
        """
        Determine the optimal processing mode for the user message.
        
        Returns:
            Tuple of (ProcessingMode, reasoning_string)
        """
        message_lower = user_message.lower()
        
        # Check for simple greetings and system commands
        simple_patterns = ['hello', 'hi', 'help', 'thanks', 'thank you', 'goodbye', 'bye']
        if any(pattern in message_lower for pattern in simple_patterns) and len(user_message.split()) <= 3:
            return ProcessingMode.SIMPLE_TOOL, "Simple greeting or system command"
        
        # Check if user has data loaded - critical for routing decisions
        has_data = context.get('csv_loaded', False) and context.get('shapefile_loaded', False)
        analysis_complete = context.get('analysis_complete', False)
        
        # Enhanced complexity detection
        complexity_score = self._calculate_complexity_score(user_message)
        
        # Data analysis patterns that should use specialized routing
        data_analysis_patterns = [
            'top', 'highest', 'lowest', 'most vulnerable', 'ranking', 'compare', 'analysis',
            'ward', 'settlement', 'malaria', 'risk', 'score', 'pca', 'composite',
            'map', 'visualization', 'chart', 'spatial', 'cluster', 'correlation',
            'scenario', 'simulation', 'intervention', 'resource', 'allocation'
        ]
        
        has_data_analysis_intent = any(pattern in message_lower for pattern in data_analysis_patterns)
        
        # Check for complex reasoning needs
        complex_reasoning_patterns = [
            'why', 'explain why', 'reason', 'because', 'analyze', 'compare',
            'what if', 'scenario', 'simulation', 'optimization', 'strategy'
        ]
        
        needs_complex_reasoning = any(pattern in message_lower for pattern in complex_reasoning_patterns)
        
        # Check for conversational context
        conversational_patterns = [
            'tell me about', 'explain', 'describe', 'what is', 'how does',
            'can you help', 'i want to know', 'based on', 'following up'
        ]
        
        is_conversational = any(pattern in message_lower for pattern in conversational_patterns)
        
        # Enhanced routing logic with component availability checks
        if has_data and has_data_analysis_intent and complexity_score > 0.5:
            if needs_complex_reasoning and self.agent is not None:
                return ProcessingMode.REACT_AGENT, f"Complex data analysis requiring reasoning (complexity: {complexity_score:.2f}, has_data: {has_data})"
            elif is_conversational and self.langchain_chain is not None:
                return ProcessingMode.LANGCHAIN_CHAT, f"Data analysis with conversational context (complexity: {complexity_score:.2f})"
            else:
                return ProcessingMode.SIMPLE_TOOL, f"Direct data analysis task (complexity: {complexity_score:.2f})"
        
        elif has_data and has_data_analysis_intent:
            # For data analysis with available data, prefer enhanced modes when available
            if complexity_score > 0.3 and self.agent is not None:
                return ProcessingMode.REACT_AGENT, f"Data analysis with reasoning (complexity: {complexity_score:.2f}, has_data: {has_data})"
            else:
                return ProcessingMode.SIMPLE_TOOL, f"Basic data analysis with available data (has_data: {has_data})"
        
        elif complexity_score > self.complexity_threshold:
            if is_conversational and self.langchain_chain is not None:
                return ProcessingMode.LANGCHAIN_CHAT, f"Complex conversational query (complexity: {complexity_score:.2f})"
            elif self.agent is not None:
                return ProcessingMode.REACT_AGENT, f"Complex reasoning required (complexity: {complexity_score:.2f})"
            else:
                return ProcessingMode.SIMPLE_TOOL, f"Complex query - advanced components unavailable (complexity: {complexity_score:.2f})"
        
        elif is_conversational and self.langchain_chain is not None:
            return ProcessingMode.LANGCHAIN_CHAT, "Conversational query benefiting from context"
        
        else:
            return ProcessingMode.SIMPLE_TOOL, "Simple tool execution sufficient"
    
    def _calculate_complexity_score(self, user_message: str) -> float:
        """Calculate complexity score for the user message."""
        message_lower = user_message.lower()
        
        # Complexity indicators
        complexity_factors = {
            # Analysis complexity
            'multi_step': ['analyze and', 'first', 'then', 'after that', 'next', 'finally'],
            'conditional': ['if', 'when', 'what if', 'suppose', 'assuming'],
            'comparison': ['compare', 'versus', 'vs', 'difference', 'better', 'worse'],
            'causation': ['why', 'because', 'cause', 'reason', 'impact', 'effect'],
            'quantitative': ['how many', 'how much', 'percentage', 'ratio', 'correlation'],
            'temporal': ['trend', 'over time', 'historical', 'future', 'predict'],
            'spatial': ['geographic', 'spatial', 'location', 'area', 'region', 'cluster'],
            'advanced': ['optimization', 'modeling', 'simulation', 'scenario', 'intervention']
        }
        
        # Calculate base complexity
        complexity = 0.0
        word_count = len(user_message.split())
        
        # Length factor (longer messages tend to be more complex)
        if word_count > 20:
            complexity += 0.3
        elif word_count > 10:
            complexity += 0.1
        
        # Complexity pattern matching
        for category, patterns in complexity_factors.items():
            if any(pattern in message_lower for pattern in patterns):
                complexity += 0.15
        
        # Question complexity
        question_words = ['what', 'how', 'why', 'when', 'where', 'which', 'who']
        question_count = sum(1 for word in question_words if word in message_lower)
        if question_count > 1:
            complexity += 0.2
        
        # Multiple entities or concepts
        if message_lower.count(' and ') > 1 or message_lower.count(',') > 2:
            complexity += 0.1
        
        return min(complexity, 1.0)  # Cap at 1.0
    
    def _should_use_langchain(self, user_message: str, session_state: SessionState) -> bool:
        """Determine if LangChain conversation management would be beneficial."""
        # Use LangChain for conversational flows that benefit from history
        conversational_indicators = [
            'remember', 'earlier', 'previous', 'before', 'continue', 'follow up',
            'also', 'additionally', 'furthermore', 'what about', 'how about'
        ]
        
        message_lower = user_message.lower()
        has_conversational_context = any(indicator in message_lower for indicator in conversational_indicators)
        
        # Use LangChain if we have ongoing conversation
        has_conversation_history = session_state.conversation_state.total_exchanges > 2
        
        # Use LangChain for explanatory requests that benefit from context
        explanatory_patterns = ['explain', 'tell me about', 'describe', 'what is', 'how does']
        is_explanatory = any(pattern in message_lower for pattern in explanatory_patterns)
        
        # Check if LangChain is available and enabled
        langchain_enabled = self.langchain_chain is not None and getattr(self.langchain_chain, 'enabled', False)
        
        return (has_conversational_context or 
                (has_conversation_history and is_explanatory) or
                langchain_enabled)
    
    def _is_conversational_query(self, user_message: str) -> bool:
        """Check if the query is conversational in nature."""
        conversational_patterns = [
            'tell me', 'explain', 'describe', 'what is', 'how does', 'can you',
            'please', 'i want to know', 'i need to understand', 'help me understand'
        ]
        
        message_lower = user_message.lower()
        return any(pattern in message_lower for pattern in conversational_patterns)
    
    def _process_with_mode(self, processing_mode: ProcessingMode, user_message: str,
                          session_id: str, session_state: SessionState, 
                          context: Dict[str, Any]) -> Dict[str, Any]:
        """Process message using the selected processing mode."""
        
        if processing_mode == ProcessingMode.SIMPLE_TOOL:
            return self._process_simple_tool(user_message, session_id)
        
        elif processing_mode == ProcessingMode.REACT_AGENT:
            return self._process_react_agent(user_message, session_id, session_state, context)
        
        elif processing_mode == ProcessingMode.LANGCHAIN_CHAT:
            return self._process_langchain_chat(user_message, session_id, context)
        
        elif processing_mode == ProcessingMode.HYBRID:
            return self._process_hybrid(user_message, session_id, session_state, context)
        
        else:
            raise ValueError(f"Unknown processing mode: {processing_mode}")
    
    def _process_simple_tool(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Process using simple tool execution."""
        if not self.request_interpreter:
            raise ValueError("Request interpreter not available")
        
        return self.request_interpreter.process_message(user_message, session_id)
    
    def _process_react_agent(self, user_message: str, session_id: str, 
                           session_state: SessionState, context: Dict[str, Any]) -> Dict[str, Any]:
        """Process using ReAct agent for complex reasoning."""
        try:
            if self.agent is None:
                logger.warning("ReAct agent requested but not available, falling back to simple tool")
                return self._process_simple_tool(user_message, session_id)
            
            agent_context = {
                'session_state': session_state,
                'user_role': context.get('user_role', 'analyst'),
                'has_data': session_state.data_state.value == 'loaded'
            }
            
            agent_response = self.agent.process_conversational_query(
                query=user_message,
                session_id=session_id,
                context=agent_context
            )
            
            # Convert agent response to expected format
            return {
                'status': 'success' if agent_response.success else 'error',
                'response': agent_response.final_answer,
                'tools_used': agent_response.tools_used,
                'reasoning_steps': len(agent_response.reasoning_steps),
                'confidence_score': agent_response.confidence_score,
                'conversation_mode': agent_response.conversation_mode.value,
                'execution_time': agent_response.total_execution_time,
                'visualizations': [],  # Extract from reasoning steps if available
                'explanations': []
            }
            
        except Exception as e:
            logger.error(f"ReAct agent processing failed: {e}")
            # Fallback to simple tool
            return self._process_simple_tool(user_message, session_id)
    
    def _process_langchain_chat(self, user_message: str, session_id: str, 
                              context: Dict[str, Any]) -> Dict[str, Any]:
        """Process using LangChain conversation management."""
        try:
            if not self.langchain_chain or not getattr(self.langchain_chain, 'enabled', False):
                logger.warning("LangChain requested but not available, falling back to ReAct agent")
                # Fallback to ReAct agent
                session_state = self._create_session_state(session_id, context)
                return self._process_react_agent(user_message, session_id, session_state, context)
            
            chain_config = {
                'user_role': context.get('user_role', 'analyst'),
                'has_data': context.get('csv_loaded', False) and context.get('shapefile_loaded', False)
            }
            
            langchain_response = self.langchain_chain.invoke(
                user_input=user_message,
                session_id=session_id,
                config=chain_config
            )
            
            # Convert LangChain response to expected format
            return {
                'status': 'success' if langchain_response['success'] else 'error',
                'response': langchain_response['response'],
                'tools_used': langchain_response.get('tools_used', []),
                'conversation_mode': langchain_response['conversation_mode'],
                'execution_time': langchain_response['execution_time'],
                'langchain_enabled': langchain_response['langchain_enabled'],
                'visualizations': [],
                'explanations': []
            }
            
        except Exception as e:
            logger.error(f"LangChain processing failed: {e}")
            # Fallback to ReAct agent
            session_state = self._create_session_state(session_id, context)
            return self._process_react_agent(user_message, session_id, session_state, context)
    
    def _process_hybrid(self, user_message: str, session_id: str, 
                       session_state: SessionState, context: Dict[str, Any]) -> Dict[str, Any]:
        """Process using hybrid approach (combination of systems)."""
        # For now, use ReAct agent as primary with LangChain memory
        return self._process_react_agent(user_message, session_id, session_state, context)
    
    def _record_interaction_metrics(self, processing_mode: ProcessingMode, user_message: str,
                                  response: Dict[str, Any], execution_time: float, session_id: str):
        """Record interaction metrics for reflection and learning."""
        if not self.reflection_engine or not getattr(self.reflection_engine, 'enabled', False):
            return
        
        try:
            # Record conversation quality
            user_satisfaction = self._estimate_user_satisfaction(response)
            self.reflection_engine.record_conversation_quality(
                session_id=session_id,
                response_time=execution_time,
                user_satisfaction=user_satisfaction,
                context={
                    'processing_mode': processing_mode.value,
                    'tools_used': response.get('tools_used', []),
                    'message_length': len(user_message),
                    'response_length': len(response.get('response', ''))
                }
            )
            
            # Record tool executions if any
            tools_used = response.get('tools_used', [])
            for tool_name in tools_used:
                self.reflection_engine.record_tool_execution(
                    tool_name=tool_name,
                    execution_time=execution_time / len(tools_used) if tools_used else execution_time,
                    success=response.get('status') == 'success',
                    session_id=session_id,
                    context={'processing_mode': processing_mode.value}
                )
                
        except Exception as e:
            logger.warning(f"Failed to record interaction metrics: {e}")
    
    def _estimate_user_satisfaction(self, response: Dict[str, Any]) -> float:
        """Estimate user satisfaction based on response quality."""
        satisfaction = 0.5  # Base score
        
        # Successful responses get higher scores
        if response.get('status') == 'success':
            satisfaction += 0.3
        
        # Responses with visualizations get bonus
        if response.get('visualizations'):
            satisfaction += 0.1
        
        # Responses with explanations get bonus
        if response.get('explanations'):
            satisfaction += 0.1
        
        # High confidence scores from agent get bonus
        if response.get('confidence_score', 0) > 0.8:
            satisfaction += 0.1
        
        # Longer, more detailed responses tend to be more satisfying
        response_length = len(response.get('response', ''))
        if response_length > 200:
            satisfaction += 0.05
        
        return min(satisfaction, 1.0)
    
    def get_routing_statistics(self) -> Dict[str, Any]:
        """Get routing statistics and performance metrics."""
        total_requests = sum(self.routing_stats.values())
        
        stats = {
            'total_requests': total_requests,
            'routing_distribution': {},
            'performance_metrics': {
                'deployment_manager_available': self.deployment_manager is not None,
                'reflection_engine_enabled': self.reflection_engine is not None and getattr(self.reflection_engine, 'enabled', False),
                'agent_available': self.agent is not None,
                'langchain_enabled': self.langchain_chain is not None and getattr(self.langchain_chain, 'enabled', False)
            }
        }
        
        # Calculate routing distribution percentages
        if total_requests > 0:
            for mode, count in self.routing_stats.items():
                stats['routing_distribution'][mode] = {
                    'count': count,
                    'percentage': (count / total_requests) * 100
                }
        
        return stats
    
    def reset_statistics(self):
        """Reset routing statistics."""
        for key in self.routing_stats:
            self.routing_stats[key] = 0
        logger.info("Routing statistics reset")


# Global router instance
_conversation_router = None


def get_conversation_router() -> ConversationRouter:
    """Get the global conversation router instance."""
    global _conversation_router
    if _conversation_router is None:
        _conversation_router = ConversationRouter()
    return _conversation_router


def init_conversation_router() -> ConversationRouter:
    """Initialize conversation router."""
    global _conversation_router
    _conversation_router = ConversationRouter()
    return _conversation_router