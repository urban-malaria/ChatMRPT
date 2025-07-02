"""
LangChain Integration for ChatMRPT - Phase 4 Implementation

This module provides modern LangChain conversation chains with session persistence,
integrating with the existing ChatMRPT infrastructure while adding advanced
conversation management capabilities.

Features:
- RunnableWithMessageHistory for production-grade conversations
- LCEL (LangChain Expression Language) patterns
- Session-based conversation memory
- Integration with existing ChatMRPT tools and agent
"""

import logging
import os
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime

try:
    from langchain_core.runnables import RunnableWithMessageHistory, RunnableLambda
    from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
    from langchain_core.chat_history import BaseChatMessageHistory
    from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
    from langchain_core.output_parsers import StrOutputParser
    from langchain_openai import ChatOpenAI
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    # Mock classes for graceful fallback
    class RunnableWithMessageHistory:
        pass
    class BaseChatMessageHistory:
        pass
    class ChatPromptTemplate:
        pass
    class MessagesPlaceholder:
        pass
    class StrOutputParser:
        pass
    class ChatOpenAI:
        pass
    BaseMessage = object
    HumanMessage = object
    AIMessage = object

from .conversation_memory import get_conversation_memory, ConversationTurn
from .chatmrpt_agent import get_chatmrpt_agent
from .session_state import SessionState, ConversationMode

logger = logging.getLogger(__name__)


class ChatMRPTMessageHistory(BaseChatMessageHistory):
    """
    Custom message history implementation that integrates with ChatMRPT's
    conversation memory system.
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.memory_manager = get_conversation_memory()
        self._messages: List[BaseMessage] = []
        self._load_history()
    
    def _load_history(self):
        """Load conversation history from ChatMRPT memory system."""
        if not self.memory_manager.enabled:
            return
        
        try:
            # Get recent conversation context
            contexts = self.memory_manager.retrieve_relevant_context(
                query="",  # Empty query to get recent history
                session_id=self.session_id,
                limit=10
            )
            
            # Convert contexts to messages
            for context in reversed(contexts):  # Reverse to get chronological order
                content = context.get('content', '')
                if 'User:' in content and 'Assistant:' in content:
                    parts = content.split('\nAssistant:')
                    if len(parts) == 2:
                        user_part = parts[0].replace('User:', '').strip()
                        ai_part = parts[1].strip()
                        
                        if LANGCHAIN_AVAILABLE:
                            self._messages.append(HumanMessage(content=user_part))
                            self._messages.append(AIMessage(content=ai_part))
        
        except Exception as e:
            logger.warning(f"Failed to load message history: {e}")
    
    @property
    def messages(self) -> List[BaseMessage]:
        """Get message history."""
        return self._messages
    
    def add_message(self, message: BaseMessage) -> None:
        """Add a message to the history."""
        self._messages.append(message)
        
        # Keep only recent messages to avoid memory bloat
        if len(self._messages) > 20:
            self._messages = self._messages[-20:]
    
    def clear(self) -> None:
        """Clear message history."""
        self._messages.clear()


class ChatMRPTConversationChain:
    """
    Modern LangChain conversation chain with session persistence.
    
    Uses RunnableWithMessageHistory for production-grade conversations
    and integrates with existing ChatMRPT infrastructure.
    """
    
    def __init__(self, llm_config: Dict[str, Any] = None):
        """Initialize the conversation chain."""
        self.llm_config = llm_config or self._load_llm_config()
        self.enabled = LANGCHAIN_AVAILABLE and self._check_openai_availability()
        
        if self.enabled:
            self.llm = self._initialize_llm()
            self.chain = self._create_conversation_chain()
            self.chain_with_history = self._create_chain_with_history()
        else:
            logger.warning("LangChain integration running in fallback mode")
            self.llm = None
            self.chain = None
            self.chain_with_history = None
        
        # Store session histories
        self.session_histories: Dict[str, ChatMRPTMessageHistory] = {}
        
        # Integration with existing ChatMRPT components
        self.memory_manager = get_conversation_memory()
        self.agent = get_chatmrpt_agent()
    
    def _load_llm_config(self) -> Dict[str, Any]:
        """Load LLM configuration from environment."""
        return {
            'model_name': os.getenv('OPENAI_MODEL_NAME', 'gpt-4o'),
            'temperature': float(os.getenv('LANGCHAIN_TEMPERATURE', '0.7')),
            'max_tokens': int(os.getenv('LANGCHAIN_MAX_TOKENS', '2000')),
            'openai_api_key': os.getenv('OPENAI_API_KEY')
        }
    
    def _check_openai_availability(self) -> bool:
        """Check if OpenAI API is available."""
        return bool(self.llm_config.get('openai_api_key'))
    
    def _initialize_llm(self) -> Optional[ChatOpenAI]:
        """Initialize the LLM."""
        if not LANGCHAIN_AVAILABLE:
            return None
        
        try:
            return ChatOpenAI(
                model_name=self.llm_config['model_name'],
                temperature=self.llm_config['temperature'],
                max_tokens=self.llm_config['max_tokens'],
                openai_api_key=self.llm_config['openai_api_key']
            )
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {e}")
            return None
    
    def _create_conversation_chain(self):
        """Create LCEL-based conversation chain."""
        if not LANGCHAIN_AVAILABLE or not self.llm:
            return None
        
        try:
            # Create prompt template with system message and conversation history
            prompt = ChatPromptTemplate.from_messages([
                ("system", self._get_system_message()),
                MessagesPlaceholder(variable_name="history"),
                ("human", "{input}")
            ])
            
            # Create the chain using LCEL
            chain = prompt | self.llm | StrOutputParser()
            return chain
            
        except Exception as e:
            logger.error(f"Failed to create conversation chain: {e}")
            return None
    
    def _create_chain_with_history(self):
        """Create chain with message history."""
        if not self.chain:
            return None
        
        try:
            return RunnableWithMessageHistory(
                self.chain,
                self._get_session_history,
                input_messages_key="input",
                history_messages_key="history"
            )
        except Exception as e:
            logger.error(f"Failed to create chain with history: {e}")
            return None
    
    def _get_system_message(self) -> str:
        """Get system message for the conversation chain."""
        return """You are ChatMRPT, an expert malaria epidemiologist and data analyst.

You help users analyze malaria risk data, create visualizations, and make intervention recommendations.
You have access to various analytical tools and should use them systematically to provide comprehensive answers.

Key capabilities:
- Malaria risk analysis and prediction
- Ward-level demographic and health data analysis
- Intervention targeting and resource allocation
- Statistical analysis and visualization
- Geographic and spatial analysis

Guidelines:
1. Provide evidence-based recommendations
2. Use clear, accessible language
3. Consider epidemiological best practices
4. Be precise with data interpretation
5. Ask clarifying questions when needed

Always be helpful, accurate, and focused on supporting malaria prevention efforts."""
    
    def _get_session_history(self, session_id: str) -> ChatMRPTMessageHistory:
        """Get or create session history."""
        if session_id not in self.session_histories:
            self.session_histories[session_id] = ChatMRPTMessageHistory(session_id)
        return self.session_histories[session_id]
    
    def invoke(self, user_input: str, session_id: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process input with conversation history.
        
        Args:
            user_input: User's input message
            session_id: Session identifier
            config: Optional configuration for the chain
            
        Returns:
            Dictionary with response and metadata
        """
        start_time = datetime.utcnow()
        
        if not self.enabled:
            return self._fallback_processing(user_input, session_id, config)
        
        try:
            # Prepare config for chain execution
            chain_config = config or {}
            chain_config["configurable"] = {"session_id": session_id}
            
            # Invoke the chain with history
            response = self.chain_with_history.invoke(
                {"input": user_input},
                config=chain_config
            )
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            # Store conversation turn in memory
            self._store_conversation_turn(
                session_id=session_id,
                user_input=user_input,
                ai_response=response,
                execution_time=execution_time
            )
            
            return {
                'response': response,
                'session_id': session_id,
                'execution_time': execution_time,
                'conversation_mode': ConversationMode.SIMPLE_CHAT.value,
                'success': True,
                'langchain_enabled': True
            }
            
        except Exception as e:
            logger.error(f"Error in LangChain conversation: {e}")
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                'response': f"I encountered an error processing your request: {str(e)}",
                'session_id': session_id,
                'execution_time': execution_time,
                'conversation_mode': ConversationMode.SIMPLE_CHAT.value,
                'success': False,
                'error': str(e),
                'langchain_enabled': True
            }
    
    def _fallback_processing(self, user_input: str, session_id: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Fallback processing when LangChain unavailable."""
        start_time = datetime.utcnow()
        
        # Use existing ChatMRPT agent for processing
        try:
            agent_response = self.agent.process_conversational_query(
                query=user_input,
                session_id=session_id,
                context=config or {}
            )
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                'response': agent_response.final_answer,
                'session_id': session_id,
                'execution_time': execution_time,
                'conversation_mode': agent_response.conversation_mode.value,
                'success': agent_response.success,
                'tools_used': agent_response.tools_used,
                'reasoning_steps': len(agent_response.reasoning_steps),
                'langchain_enabled': False
            }
            
        except Exception as e:
            logger.error(f"Error in fallback processing: {e}")
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                'response': f"I encountered an error: {str(e)}",
                'session_id': session_id,
                'execution_time': execution_time,
                'conversation_mode': ConversationMode.SIMPLE_CHAT.value,
                'success': False,
                'error': str(e),
                'langchain_enabled': False
            }
    
    def _store_conversation_turn(self, session_id: str, user_input: str, 
                               ai_response: str, execution_time: float):
        """Store conversation turn in ChatMRPT memory system."""
        if not self.memory_manager.enabled:
            return
        
        try:
            turn = ConversationTurn(
                session_id=session_id,
                user_input=user_input,
                ai_response=ai_response,
                timestamp=datetime.utcnow().isoformat(),
                conversation_type="langchain_conversation",
                tools_used=[],
                user_role="user",
                quality_score=0.8,  # Default quality score for LangChain conversations
                response_time=execution_time
            )
            
            self.memory_manager.store_conversation_turn(turn)
            
        except Exception as e:
            logger.warning(f"Failed to store conversation turn: {e}")
    
    def get_conversation_summary(self, session_id: str, limit: int = 5) -> Dict[str, Any]:
        """Get conversation summary for a session."""
        if session_id in self.session_histories:
            history = self.session_histories[session_id]
            messages = history.messages[-limit:]  # Get recent messages
            
            summary = {
                'session_id': session_id,
                'message_count': len(history.messages),
                'recent_messages': []
            }
            
            for msg in messages:
                if hasattr(msg, 'content'):
                    msg_type = "human" if "Human" in str(type(msg)) else "ai"
                    summary['recent_messages'].append({
                        'type': msg_type,
                        'content': msg.content[:100] + "..." if len(msg.content) > 100 else msg.content
                    })
            
            return summary
        
        return {'session_id': session_id, 'message_count': 0, 'recent_messages': []}
    
    def clear_session_history(self, session_id: str) -> bool:
        """Clear conversation history for a session."""
        try:
            if session_id in self.session_histories:
                self.session_histories[session_id].clear()
                del self.session_histories[session_id]
            return True
        except Exception as e:
            logger.error(f"Failed to clear session history: {e}")
            return False
    
    def get_chain_stats(self) -> Dict[str, Any]:
        """Get statistics about the conversation chain."""
        return {
            'enabled': self.enabled,
            'langchain_available': LANGCHAIN_AVAILABLE,
            'openai_available': self._check_openai_availability(),
            'model_name': self.llm_config.get('model_name', 'unknown'),
            'active_sessions': len(self.session_histories),
            'memory_enabled': self.memory_manager.enabled,
            'agent_available': self.agent is not None
        }


class RedisSessionStorage:
    """
    Redis-based session storage for scalable conversation persistence.
    
    Optional component - falls back to in-memory if Redis unavailable.
    """
    
    def __init__(self, redis_url: str = None):
        """Initialize Redis session storage."""
        self.redis_url = redis_url or os.getenv('REDIS_URL')
        self.enabled = self.redis_url is not None
        self.redis_client = None
        
        if self.enabled:
            self.redis_client = self._initialize_redis()
        else:
            logger.info("Redis not configured - using in-memory session storage")
    
    def _initialize_redis(self):
        """Initialize Redis client with fallback."""
        try:
            import redis
            client = redis.from_url(self.redis_url)
            # Test connection
            client.ping()
            logger.info("Redis session storage initialized")
            return client
        except ImportError:
            logger.warning("Redis package not available - install with: pip install redis")
            self.enabled = False
            return None
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self.enabled = False
            return None
    
    def store_session_data(self, session_id: str, data: Dict[str, Any], ttl: int = 3600) -> bool:
        """Store session data with TTL."""
        if not self.enabled or not self.redis_client:
            return False
        
        try:
            import json
            data_json = json.dumps(data, default=str)
            self.redis_client.setex(f"session:{session_id}", ttl, data_json)
            return True
        except Exception as e:
            logger.error(f"Failed to store session data: {e}")
            return False
    
    def get_session_data(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve session data."""
        if not self.enabled or not self.redis_client:
            return None
        
        try:
            import json
            data_json = self.redis_client.get(f"session:{session_id}")
            if data_json:
                return json.loads(data_json)
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve session data: {e}")
            return None
    
    def delete_session_data(self, session_id: str) -> bool:
        """Delete session data."""
        if not self.enabled or not self.redis_client:
            return False
        
        try:
            self.redis_client.delete(f"session:{session_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete session data: {e}")
            return False
    
    def get_redis_stats(self) -> Dict[str, Any]:
        """Get Redis statistics."""
        stats = {
            'enabled': self.enabled,
            'redis_url': self.redis_url is not None,
            'connected': False,
            'session_count': 0
        }
        
        if self.enabled and self.redis_client:
            try:
                info = self.redis_client.info()
                stats['connected'] = True
                stats['redis_version'] = info.get('redis_version', 'unknown')
                
                # Count session keys
                session_keys = self.redis_client.keys("session:*")
                stats['session_count'] = len(session_keys)
                
            except Exception as e:
                logger.error(f"Failed to get Redis stats: {e}")
                stats['error'] = str(e)
        
        return stats


# Global instances
_conversation_chain = None
_redis_storage = None


def get_conversation_chain() -> ChatMRPTConversationChain:
    """Get the global conversation chain instance."""
    global _conversation_chain
    if _conversation_chain is None:
        _conversation_chain = ChatMRPTConversationChain()
    return _conversation_chain


def get_redis_storage() -> RedisSessionStorage:
    """Get the global Redis storage instance."""
    global _redis_storage
    if _redis_storage is None:
        _redis_storage = RedisSessionStorage()
    return _redis_storage


def init_langchain_integration(llm_config: Dict[str, Any] = None, 
                             redis_url: str = None) -> Dict[str, Any]:
    """
    Initialize LangChain integration with custom configuration.
    
    Args:
        llm_config: LLM configuration dictionary
        redis_url: Redis URL for session storage
        
    Returns:
        Initialization status and statistics
    """
    global _conversation_chain, _redis_storage
    
    # Initialize conversation chain
    _conversation_chain = ChatMRPTConversationChain(llm_config)
    
    # Initialize Redis storage
    _redis_storage = RedisSessionStorage(redis_url)
    
    # Return status
    return {
        'langchain_integration': {
            'enabled': _conversation_chain.enabled,
            'chain_stats': _conversation_chain.get_chain_stats()
        },
        'redis_storage': {
            'enabled': _redis_storage.enabled,
            'redis_stats': _redis_storage.get_redis_stats()
        }
    }


# Phase 4 Additional Components for Testing

class ChatMRPTMemory:
    """LangChain memory wrapper for ChatMRPT conversation memory."""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.memory_manager = get_conversation_memory()
    
    def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
        """Save conversation context to ChatMRPT memory."""
        try:
            user_input = inputs.get("user", str(inputs))
            ai_response = outputs.get("assistant", str(outputs))
            
            turn = ConversationTurn(
                session_id=self.session_id,
                user_input=user_input,
                ai_response=ai_response,
                timestamp=datetime.utcnow().isoformat(),
                conversation_type="langchain_conversation"
            )
            
            self.memory_manager.store_conversation_turn(turn)
        except Exception as e:
            logger.warning(f"Failed to save context to ChatMRPT memory: {e}")
    
    def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Load memory variables from ChatMRPT memory."""
        try:
            contexts = self.memory_manager.retrieve_relevant_context(
                query=str(inputs),
                session_id=self.session_id,
                limit=5
            )
            
            return {
                "history": [ctx.get('content', '') for ctx in contexts],
                "context": " ".join(ctx.get('content', '') for ctx in contexts)
            }
        except Exception as e:
            logger.warning(f"Failed to load memory variables: {e}")
            return {"history": [], "context": ""}


class LangChainToolWrapper:
    """Wrapper to convert ChatMRPT tools to LangChain format."""
    
    def __init__(self):
        from .tool_registry import get_tool_registry
        self.tool_registry = get_tool_registry()
    
    def get_langchain_tools(self) -> List[Any]:
        """Convert all ChatMRPT tools to LangChain format."""
        tools = []
        available_tools = self.tool_registry.list_tools()
        
        for tool_name in available_tools[:10]:  # Limit for testing
            wrapped_tool = self.wrap_tool(tool_name, "default_session")
            if wrapped_tool:
                tools.append(wrapped_tool)
        
        return tools
    
    def wrap_tool(self, tool_name: str, session_id: str) -> Optional[Any]:
        """Wrap a single ChatMRPT tool for LangChain."""
        try:
            metadata = self.tool_registry.get_tool_metadata(tool_name)
            if not metadata:
                return None
            
            # Create a simple tool wrapper
            class WrappedTool:
                def __init__(self, name, description, func):
                    self.name = name
                    self.description = description
                    self.func = func
                
                def run(self, input_str: str) -> str:
                    try:
                        result = self.func(input_str)
                        return str(result)
                    except Exception as e:
                        return f"Tool execution failed: {e}"
            
            def tool_func(input_str: str) -> str:
                try:
                    result = self.tool_registry.execute_tool(tool_name, session_id, query=input_str)
                    return str(result)
                except Exception as e:
                    return f"Error: {e}"
            
            return WrappedTool(
                name=tool_name,
                description=metadata.description,
                func=tool_func
            )
        except Exception as e:
            logger.warning(f"Failed to wrap tool {tool_name}: {e}")
            return None


class ConversationManager:
    """Manager for handling multiple conversation sessions."""
    
    def __init__(self):
        self.chains = {}
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration for conversation manager."""
        return {
            'max_sessions': int(os.getenv('MAX_CONVERSATION_SESSIONS', '100')),
            'session_timeout': int(os.getenv('SESSION_TIMEOUT_HOURS', '24')),
            'enable_persistence': os.getenv('ENABLE_SESSION_PERSISTENCE', 'true').lower() == 'true'
        }
    
    def get_or_create_chain(self, session_id: str) -> ChatMRPTConversationChain:
        """Get existing or create new conversation chain for session."""
        if session_id not in self.chains:
            # Create new chain for session
            self.chains[session_id] = ChatMRPTConversationChain()
            
            # Clean up old sessions if too many
            if len(self.chains) > self.config['max_sessions']:
                self._cleanup_old_sessions()
        
        return self.chains[session_id]
    
    def _cleanup_old_sessions(self):
        """Clean up oldest sessions to maintain session limit."""
        # Simple cleanup - remove first 10% of sessions
        cleanup_count = max(1, len(self.chains) // 10)
        session_ids = list(self.chains.keys())[:cleanup_count]
        
        for session_id in session_ids:
            del self.chains[session_id]
        
        logger.info(f"Cleaned up {cleanup_count} old conversation sessions")


# Update ChatMRPTConversationChain to expose required attributes
_original_init = ChatMRPTConversationChain.__init__

def _enhanced_init(self, llm_config: Dict[str, Any] = None, config: Dict[str, Any] = None):
    """Enhanced initialization with test attributes."""
    # Use config parameter if provided, otherwise use llm_config
    final_config = config or llm_config
    _original_init(self, final_config)
    
    # Add test attributes
    self.memory = ChatMRPTMemory("default_session") if self.enabled else None
    self.tool_wrapper = LangChainToolWrapper() if self.enabled else None

# Monkey patch the init method
ChatMRPTConversationChain.__init__ = _enhanced_init