"""
Conversation Memory Manager for ChatMRPT.

This module provides persistent conversation memory using ChromaDB for semantic storage
and retrieval of conversation history across sessions.
"""

import os
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

# Disable ChromaDB telemetry before import
os.environ["ANONYMIZED_TELEMETRY"] = "False"

try:
    import chromadb
    import chromadb.config
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    chromadb = None

try:
    from langchain_huggingface import HuggingFaceEmbeddings
    HUGGINGFACE_AVAILABLE = True
except ImportError:
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        HUGGINGFACE_AVAILABLE = True
    except ImportError:
        HUGGINGFACE_AVAILABLE = False
        HuggingFaceEmbeddings = None

from flask import current_app

logger = logging.getLogger(__name__)


@dataclass
class ConversationTurn:
    """Represents a single conversation turn with metadata."""
    session_id: str
    user_input: str
    ai_response: str
    timestamp: str
    conversation_type: str = "general"
    tools_used: List[str] = None
    entities_mentioned: List[str] = None
    user_role: str = "analyst"
    quality_score: float = 0.0
    response_time: float = 0.0

    def __post_init__(self):
        """Initialize default values for lists."""
        if self.tools_used is None:
            self.tools_used = []
        if self.entities_mentioned is None:
            self.entities_mentioned = []

    def to_text(self) -> str:
        """Convert conversation turn to text for embedding."""
        return f"User: {self.user_input}\nAssistant: {self.ai_response}"


class ConversationMemoryManager:
    """
    Manages persistent conversation memory using ChromaDB vector store.
    
    Features:
    - Semantic storage and retrieval of conversations
    - Session-based isolation
    - Automatic cleanup of old conversations
    - Metadata-rich storage for context
    - Graceful fallback when ChromaDB unavailable
    """

    def __init__(self, persist_directory: str = None, collection_name: str = None):
        """
        Initialize the conversation memory manager.
        
        Args:
            persist_directory: Directory for ChromaDB persistence
            collection_name: Name of the ChromaDB collection
        """
        self.persist_directory = persist_directory or self._get_persist_directory()
        self.collection_name = collection_name or self._get_collection_name()
        self.embedding_model_name = self._get_embedding_model()
        self.retrieval_k = self._get_retrieval_k()
        self.cleanup_days = self._get_cleanup_days()
        self.enabled = self._is_memory_enabled()
        
        # Initialize components
        self.embeddings = None
        self.chroma_client = None
        self.collection = None
        
        if self.enabled:
            self._initialize_components()
        else:
            logger.info("Conversation memory disabled - falling back to stateless mode")

    def _get_persist_directory(self) -> str:
        """Get persist directory from config or environment."""
        if current_app:
            return current_app.config.get('CHROMA_PERSIST_DIRECTORY', './memory')
        return os.getenv('CHROMA_PERSIST_DIRECTORY', './memory')

    def _get_collection_name(self) -> str:
        """Get collection name from config or environment."""
        if current_app:
            return current_app.config.get('CHROMA_COLLECTION_NAME', 'chatmrpt_conversations')
        return os.getenv('CHROMA_COLLECTION_NAME', 'chatmrpt_conversations')

    def _get_embedding_model(self) -> str:
        """Get embedding model from config or environment."""
        if current_app:
            return current_app.config.get('MEMORY_EMBEDDING_MODEL', 'BAAI/llm-embedder')
        return os.getenv('MEMORY_EMBEDDING_MODEL', 'BAAI/llm-embedder')

    def _get_retrieval_k(self) -> int:
        """Get retrieval K from config or environment."""
        if current_app:
            return current_app.config.get('MEMORY_RETRIEVAL_K', 5)
        return int(os.getenv('MEMORY_RETRIEVAL_K', '5'))

    def _get_cleanup_days(self) -> int:
        """Get cleanup days from config or environment."""
        if current_app:
            return current_app.config.get('MEMORY_CLEANUP_DAYS', 30)
        return int(os.getenv('MEMORY_CLEANUP_DAYS', '30'))

    def _is_memory_enabled(self) -> bool:
        """Check if conversation memory is enabled."""
        if current_app:
            return current_app.config.get('ENABLE_CONVERSATION_MEMORY', True)
        return os.getenv('ENABLE_CONVERSATION_MEMORY', 'true').lower() == 'true'

    def _initialize_components(self) -> None:
        """Initialize ChromaDB and embeddings components."""
        if not self._check_dependencies():
            logger.error("Required dependencies not available - disabling memory")
            self.enabled = False
            return

        try:
            # Initialize embeddings with fallback for invalid models
            try:
                self.embeddings = HuggingFaceEmbeddings(
                    model_name=self.embedding_model_name,
                    model_kwargs={'device': 'cpu'},
                    encode_kwargs={'normalize_embeddings': True}
                )
                logger.info(f"Initialized embeddings with model: {self.embedding_model_name}")
            except Exception as e:
                # Fallback to default model if specified model fails
                if "test-model" in self.embedding_model_name:
                    logger.warning(f"Test model detected, using default embeddings")
                    self.embedding_model_name = "sentence-transformers/all-MiniLM-L6-v2"
                    self.embeddings = HuggingFaceEmbeddings(
                        model_name=self.embedding_model_name,
                        model_kwargs={'device': 'cpu'},
                        encode_kwargs={'normalize_embeddings': True}
                    )
                else:
                    raise e

            # Ensure persist directory exists with proper permissions
            persist_path = Path(self.persist_directory)
            persist_path.mkdir(parents=True, exist_ok=True)
            
            # Set environment variable to disable telemetry
            os.environ["ANONYMIZED_TELEMETRY"] = "False"
            
            # Initialize ChromaDB client with telemetry disabled
            import chromadb.config
            self.chroma_client = chromadb.PersistentClient(
                path=str(persist_path.absolute()),
                settings=chromadb.config.Settings(
                    anonymized_telemetry=False,
                    allow_reset=True,
                    is_persistent=True
                )
            )

            # Get or create collection
            try:
                self.collection = self.chroma_client.get_collection(
                    name=self.collection_name
                )
                logger.info(f"Using existing collection: {self.collection_name}")
            except Exception:
                self.collection = self.chroma_client.create_collection(
                    name=self.collection_name,
                    metadata={"created_at": datetime.utcnow().isoformat()}
                )
                logger.info(f"Created new collection: {self.collection_name}")

        except Exception as e:
            logger.error(f"Failed to initialize conversation memory: {e}")
            self.enabled = False

    def _check_dependencies(self) -> bool:
        """Check if required dependencies are available."""
        if not CHROMADB_AVAILABLE:
            logger.warning("ChromaDB not available - install with: pip install chromadb")
            return False
        
        if not HUGGINGFACE_AVAILABLE:
            logger.warning("HuggingFace embeddings not available - install with: pip install langchain-huggingface")
            return False
        
        return True

    def store_conversation_turn(self, turn: ConversationTurn) -> bool:
        """
        Store a conversation turn in memory.
        
        Args:
            turn: ConversationTurn object to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled or not self.collection:
            logger.debug("Memory disabled or not initialized - skipping storage")
            return False

        try:
            # Convert turn to text for embedding
            conversation_text = turn.to_text()
            
            # Generate embedding
            embedding = self.embeddings.embed_query(conversation_text)
            
            # Prepare metadata - ChromaDB only accepts primitive types
            metadata = asdict(turn)
            # Remove fields that will be stored separately
            metadata.pop('user_input', None)
            metadata.pop('ai_response', None)
            
            # Convert lists to JSON strings for ChromaDB compatibility
            for key, value in metadata.items():
                if isinstance(value, list):
                    metadata[key] = json.dumps(value) if value else ""
                elif value is None:
                    metadata[key] = ""
            
            # Generate unique ID
            turn_id = f"{turn.session_id}_{turn.timestamp}_{hash(conversation_text) % 10000}"
            
            # Store in ChromaDB
            self.collection.add(
                documents=[conversation_text],
                embeddings=[embedding],
                metadatas=[metadata],
                ids=[turn_id]
            )
            
            logger.debug(f"Stored conversation turn: {turn_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store conversation turn: {e}")
            return False

    def store_conversation_turn_dict(self, session_id: str, user_input: str, 
                                   ai_response: str, metadata: Dict[str, Any] = None) -> bool:
        """
        Store a conversation turn from dictionary data.
        
        Args:
            session_id: Session identifier
            user_input: User's input text
            ai_response: AI's response text
            metadata: Additional metadata dictionary
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Create ConversationTurn object
        turn = ConversationTurn(
            session_id=session_id,
            user_input=user_input,
            ai_response=ai_response,
            timestamp=datetime.utcnow().isoformat(),
            conversation_type=metadata.get('conversation_type', 'general') if metadata else 'general',
            tools_used=metadata.get('tools_used', []) if metadata else [],
            entities_mentioned=metadata.get('entities_mentioned', []) if metadata else [],
            user_role=metadata.get('user_role', 'analyst') if metadata else 'analyst',
            quality_score=metadata.get('quality_score', 0.0) if metadata else 0.0,
            response_time=metadata.get('response_time', 0.0) if metadata else 0.0
        )
        
        return self.store_conversation_turn(turn)

    def retrieve_relevant_context(self, query: str, session_id: str = None, 
                                 limit: int = None) -> List[Dict[str, Any]]:
        """
        Retrieve semantically relevant conversation context.
        
        Args:
            query: Query text for semantic search
            session_id: Optional session ID to filter results
            limit: Maximum number of results (defaults to config value)
            
        Returns:
            List of conversation contexts with metadata
        """
        if not self.enabled or not self.collection:
            logger.debug("Memory disabled or not initialized - returning empty context")
            return []

        try:
            limit = limit or self.retrieval_k
            
            # Generate query embedding
            query_embedding = self.embeddings.embed_query(query)
            
            # Prepare where clause for session filtering
            where_clause = None
            if session_id:
                where_clause = {"session_id": session_id}
            
            # Query ChromaDB
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=limit,
                where=where_clause,
                include=['documents', 'metadatas', 'distances']
            )
            
            # Format results
            contexts = []
            if results and results['documents']:
                for i, (doc, metadata, distance) in enumerate(zip(
                    results['documents'][0],
                    results['metadatas'][0],
                    results['distances'][0]
                )):
                    context = {
                        'content': doc,
                        'metadata': metadata,
                        'similarity_score': 1 - distance,  # Convert distance to similarity
                        'timestamp': metadata.get('timestamp', ''),
                        'session_id': metadata.get('session_id', ''),
                        'tools_used': metadata.get('tools_used', []),
                        'user_role': metadata.get('user_role', 'analyst')
                    }
                    contexts.append(context)
            
            logger.debug(f"Retrieved {len(contexts)} relevant contexts for query")
            return contexts
            
        except Exception as e:
            logger.error(f"Failed to retrieve conversation context: {e}")
            return []

    def get_session_summary(self, session_id: str) -> str:
        """
        Get a summary of conversation for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            str: Conversation summary
        """
        if not self.enabled or not self.collection:
            return "Conversation memory not available"

        try:
            # Get all conversations for session
            results = self.collection.get(
                where={"session_id": session_id},
                include=['documents', 'metadatas']
            )
            
            if not results or not results['documents']:
                return "No conversation history found for this session"
            
            # Sort by timestamp
            conversations = list(zip(results['documents'], results['metadatas']))
            conversations.sort(key=lambda x: x[1].get('timestamp', ''))
            
            # Create summary
            summary_parts = []
            summary_parts.append(f"Session {session_id} - {len(conversations)} exchanges:")
            
            for i, (doc, metadata) in enumerate(conversations[-5:], 1):  # Last 5 exchanges
                timestamp = metadata.get('timestamp', '')[:19]  # Remove microseconds
                tools = metadata.get('tools_used', [])
                tools_str = f" (used: {', '.join(tools)})" if tools else ""
                summary_parts.append(f"{i}. [{timestamp}] {doc[:100]}...{tools_str}")
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Failed to get session summary: {e}")
            return f"Error retrieving session summary: {str(e)}"

    def cleanup_old_conversations(self, days: int = None) -> int:
        """
        Clean up conversations older than specified days.
        
        Args:
            days: Number of days to keep (defaults to config value)
            
        Returns:
            int: Number of conversations deleted
        """
        if not self.enabled or not self.collection:
            return 0

        try:
            days = days or self.cleanup_days
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            cutoff_iso = cutoff_date.isoformat()
            
            # Get conversations older than cutoff
            results = self.collection.get(
                where={"timestamp": {"$lt": cutoff_iso}},
                include=['metadatas']
            )
            
            if not results or not results['ids']:
                logger.info("No old conversations to clean up")
                return 0
            
            # Delete old conversations
            old_ids = results['ids']
            self.collection.delete(ids=old_ids)
            
            logger.info(f"Cleaned up {len(old_ids)} old conversations (older than {days} days)")
            return len(old_ids)
            
        except Exception as e:
            logger.error(f"Failed to cleanup old conversations: {e}")
            return 0

    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get memory system statistics.
        
        Returns:
            dict: Memory statistics
        """
        stats = {
            'enabled': self.enabled,
            'persist_directory': self.persist_directory,
            'collection_name': self.collection_name,
            'embedding_model': self.embedding_model_name,
            'total_conversations': 0,
            'unique_sessions': 0,
            'oldest_conversation': None,
            'newest_conversation': None
        }
        
        if not self.enabled or not self.collection:
            return stats
        
        try:
            # Get collection info
            collection_count = self.collection.count()
            stats['total_conversations'] = collection_count
            
            if collection_count > 0:
                # Get all metadata to analyze
                results = self.collection.get(include=['metadatas'])
                
                if results and results['metadatas']:
                    metadatas = results['metadatas']
                    
                    # Count unique sessions
                    sessions = set(m.get('session_id', '') for m in metadatas)
                    stats['unique_sessions'] = len(sessions)
                    
                    # Find oldest and newest
                    timestamps = [m.get('timestamp', '') for m in metadatas if m.get('timestamp')]
                    if timestamps:
                        stats['oldest_conversation'] = min(timestamps)
                        stats['newest_conversation'] = max(timestamps)
            
        except Exception as e:
            logger.error(f"Failed to get memory stats: {e}")
            stats['error'] = str(e)
        
        return stats

    def reset_collection(self) -> bool:
        """
        Reset the conversation collection (delete all data).
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled or not self.chroma_client:
            return False
        
        try:
            # Delete existing collection
            self.chroma_client.delete_collection(name=self.collection_name)
            
            # Create new collection
            self.collection = self.chroma_client.create_collection(
                name=self.collection_name,
                metadata={"created_at": datetime.utcnow().isoformat()}
            )
            
            logger.info(f"Reset conversation collection: {self.collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset collection: {e}")
            return False


# Global instance for easy access
conversation_memory = None


def get_conversation_memory() -> ConversationMemoryManager:
    """
    Get the global conversation memory instance.
    
    Returns:
        ConversationMemoryManager: Global memory manager instance
    """
    global conversation_memory
    if conversation_memory is None:
        conversation_memory = ConversationMemoryManager()
    return conversation_memory


def init_conversation_memory(app=None) -> ConversationMemoryManager:
    """
    Initialize conversation memory for Flask application.
    
    Args:
        app: Flask application instance
        
    Returns:
        ConversationMemoryManager: Initialized memory manager
    """
    global conversation_memory
    
    if app:
        with app.app_context():
            conversation_memory = ConversationMemoryManager()
    else:
        conversation_memory = ConversationMemoryManager()
    
    return conversation_memory