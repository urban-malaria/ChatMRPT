"""
Memory System Migration Plan for ChatMRPT

This module provides utilities and strategies for migrating from the fragmented
memory systems to the unified memory manager.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import os
from pathlib import Path

from app.core.unified_memory import (
    UnifiedMemoryManager, 
    MemoryType, 
    MemoryPriority,
    get_unified_memory
)
from app.core.conversation_memory import ConversationMemoryManager
from app.services.session_memory import SessionMemory
from app.core.session_state import SessionState

logger = logging.getLogger(__name__)


class MemoryMigrationManager:
    """
    Handles migration from old memory systems to unified memory.
    
    Migration strategy:
    1. Export data from old systems
    2. Transform to unified format
    3. Import into unified memory
    4. Verify migration
    5. Archive old data
    """
    
    def __init__(self):
        """Initialize migration manager."""
        self.unified_memory = get_unified_memory()
        self.migration_log = []
        self.migration_stats = {
            'conversations_migrated': 0,
            'session_states_migrated': 0,
            'analysis_contexts_migrated': 0,
            'errors': []
        }
    
    def migrate_conversation_memory(self, 
                                  old_memory: ConversationMemoryManager) -> bool:
        """
        Migrate data from ConversationMemoryManager to unified memory.
        
        Args:
            old_memory: Existing conversation memory manager
            
        Returns:
            bool: Success status
        """
        try:
            logger.info("Starting conversation memory migration...")
            
            # Get all conversations from ChromaDB
            if old_memory.collection:
                results = old_memory.collection.get(
                    include=['documents', 'metadatas', 'embeddings']
                )
                
                if results and results['documents']:
                    for i, (doc, metadata, embedding) in enumerate(zip(
                        results['documents'],
                        results['metadatas'],
                        results.get('embeddings', [None] * len(results['documents']))
                    )):
                        # Parse conversation from document
                        parts = doc.split('\n')
                        user_input = parts[0].replace('User: ', '') if parts else ''
                        ai_response = parts[1].replace('Assistant: ', '') if len(parts) > 1 else ''
                        
                        # Create conversation content
                        content = {
                            'message': {
                                'role': 'user',
                                'content': user_input,
                                'timestamp': metadata.get('timestamp', '')
                            },
                            'response': {
                                'role': 'assistant',
                                'content': ai_response,
                                'timestamp': metadata.get('timestamp', '')
                            }
                        }
                        
                        # Determine priority based on metadata
                        priority = MemoryPriority.MEDIUM
                        if metadata.get('tools_used'):
                            priority = MemoryPriority.HIGH
                        
                        # Store in unified memory
                        self.unified_memory.remember(
                            content=content,
                            memory_type=MemoryType.CONVERSATION,
                            priority=priority,
                            metadata={
                                'migrated_from': 'conversation_memory',
                                'original_id': results['ids'][i],
                                'tools_used': json.loads(metadata.get('tools_used', '[]')),
                                'user_role': metadata.get('user_role', 'analyst'),
                                'quality_score': float(metadata.get('quality_score', 0.0))
                            }
                        )
                        
                        self.migration_stats['conversations_migrated'] += 1
                
                logger.info(f"Migrated {self.migration_stats['conversations_migrated']} conversations")
                return True
                
        except Exception as e:
            error_msg = f"Failed to migrate conversation memory: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
            return False
    
    def migrate_session_memory(self, session_id: str, storage_path: str = "instance/memory") -> bool:
        """
        Migrate data from SessionMemory files to unified memory.
        
        Args:
            session_id: Session ID to migrate
            storage_path: Path to session memory files
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Migrating session memory for {session_id}...")
            
            memory_file = os.path.join(storage_path, f"{session_id}_memory.json")
            if not os.path.exists(memory_file):
                logger.warning(f"No session memory file found: {memory_file}")
                return False
            
            with open(memory_file, 'r') as f:
                data = json.load(f)
            
            # Migrate conversation history
            for msg_data in data.get('conversation_history', []):
                content = {
                    'message': {
                        'role': msg_data['type'],
                        'content': msg_data['content'],
                        'timestamp': msg_data['timestamp']
                    }
                }
                
                self.unified_memory.remember(
                    content=content,
                    memory_type=MemoryType.CONVERSATION,
                    priority=MemoryPriority.MEDIUM,
                    metadata={
                        'migrated_from': 'session_memory',
                        'session_id': session_id,
                        'original_metadata': msg_data.get('metadata', {})
                    }
                )
            
            # Migrate analysis context
            if data.get('analysis_context'):
                analysis = data['analysis_context']
                self.unified_memory.store_analysis_results(
                    analysis_type=analysis.get('composite_method', 'unknown'),
                    results={
                        'variables_used': analysis.get('variables_used', []),
                        'top_risk_wards': analysis.get('top_risk_wards', []),
                        'low_risk_wards': analysis.get('low_risk_wards', []),
                        'method_agreement': analysis.get('method_agreement', {}),
                        'score_ranges': analysis.get('score_ranges', {})
                    },
                    metadata={
                        'migrated_from': 'session_memory',
                        'session_id': session_id,
                        'analysis_timestamp': analysis.get('analysis_timestamp')
                    }
                )
                self.migration_stats['analysis_contexts_migrated'] += 1
            
            # Migrate user preferences and entities
            if data.get('user_preferences'):
                self.unified_memory.remember(
                    content=data['user_preferences'],
                    memory_type=MemoryType.USER_PREFERENCE,
                    priority=MemoryPriority.HIGH,
                    metadata={'migrated_from': 'session_memory', 'session_id': session_id}
                )
            
            self.migration_stats['session_states_migrated'] += 1
            logger.info(f"Successfully migrated session memory for {session_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to migrate session memory {session_id}: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
            return False
    
    def migrate_all_sessions(self, storage_path: str = "instance/memory") -> Dict[str, Any]:
        """
        Migrate all session memory files.
        
        Args:
            storage_path: Path to session memory files
            
        Returns:
            dict: Migration results
        """
        logger.info("Starting migration of all session memories...")
        
        # Find all session memory files
        memory_files = list(Path(storage_path).glob("*_memory.json"))
        
        for memory_file in memory_files:
            # Extract session ID from filename
            session_id = memory_file.stem.replace('_memory', '')
            self.migrate_session_memory(session_id, storage_path)
        
        return self.get_migration_summary()
    
    def verify_migration(self) -> Dict[str, Any]:
        """
        Verify that migration was successful.
        
        Returns:
            dict: Verification results
        """
        verification = {
            'unified_memory_stats': self.unified_memory.get_memory_insights(),
            'migration_stats': self.migration_stats,
            'verification_status': 'unknown'
        }
        
        # Check if unified memory has expected counts
        total_memories = verification['unified_memory_stats']['total_memories']
        expected_memories = (
            self.migration_stats['conversations_migrated'] +
            self.migration_stats['session_states_migrated'] +
            self.migration_stats['analysis_contexts_migrated']
        )
        
        if total_memories >= expected_memories * 0.9:  # Allow 10% variance
            verification['verification_status'] = 'success'
        else:
            verification['verification_status'] = 'partial'
            verification['missing_count'] = expected_memories - total_memories
        
        return verification
    
    def create_migration_backup(self, backup_path: str = "instance/memory_backup") -> bool:
        """
        Create backup of old memory systems before migration.
        
        Args:
            backup_path: Path for backup files
            
        Returns:
            bool: Success status
        """
        try:
            import shutil
            from datetime import datetime
            
            # Create timestamped backup directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = Path(backup_path) / f"backup_{timestamp}"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Backup memory files
            memory_path = Path("instance/memory")
            if memory_path.exists():
                shutil.copytree(memory_path, backup_dir / "session_memory")
            
            # Backup ChromaDB
            chroma_path = Path("./memory")
            if chroma_path.exists():
                shutil.copytree(chroma_path, backup_dir / "chromadb")
            
            # Save backup info
            backup_info = {
                'timestamp': timestamp,
                'migration_stats': self.migration_stats,
                'backup_location': str(backup_dir)
            }
            
            with open(backup_dir / "backup_info.json", 'w') as f:
                json.dump(backup_info, f, indent=2)
            
            logger.info(f"Created migration backup at: {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create migration backup: {e}")
            return False
    
    def get_migration_summary(self) -> Dict[str, Any]:
        """Get summary of migration process."""
        return {
            'migration_stats': self.migration_stats,
            'migration_log': self.migration_log,
            'timestamp': datetime.now().isoformat(),
            'success': len(self.migration_stats['errors']) == 0
        }


# Integration helpers for updating existing code

def update_request_interpreter_import():
    """Update request_interpreter.py to use unified memory."""
    return """
# Replace old imports:
# from app.core.conversation_memory import get_conversation_memory
# from app.services.session_memory import SessionMemory

# With new import:
from app.core.unified_memory import get_unified_memory

# Update memory usage:
memory = get_unified_memory()

# Store conversation:
memory.add_conversation_turn(
    user_message=query,
    ai_response=response,
    metadata={
        'tools_used': tools_used,
        'response_time': response_time,
        'success': success
    }
)

# Get context:
context = memory.get_context(session_id)

# Recall relevant memories:
relevant_memories = memory.recall(
    query=user_query,
    memory_types=[MemoryType.CONVERSATION, MemoryType.ANALYSIS_CONTEXT],
    limit=5
)
"""


def update_route_imports():
    """Update route files to use unified memory."""
    return """
# In web/routes/chat.py and other routes:

# Replace:
# from app.services.session_memory import SessionMemory
# memory = SessionMemory(session_id)

# With:
from app.core.unified_memory import get_unified_memory
memory = get_unified_memory()

# Update session state:
memory.update_session_state({
    'current_stage': 'analysis',
    'data_loaded': True
})

# Store analysis results:
memory.store_analysis_results(
    analysis_type='composite_scoring',
    results=analysis_results,
    metadata={'tool': 'composite_analysis'}
)
"""


def update_tool_imports():
    """Update analysis tools to use unified memory."""
    return """
# In tools that need memory:

from app.core.unified_memory import get_unified_memory

# Store important results:
memory = get_unified_memory()
memory.store_analysis_results(
    analysis_type=self.name,
    results={
        'summary': results_summary,
        'visualizations': viz_files,
        'key_findings': findings
    }
)
"""


# Deprecation warnings for old systems

def deprecation_warning():
    """Generate deprecation warnings for old memory systems."""
    return """
import warnings

# Add to old memory files:
warnings.warn(
    "This memory system is deprecated. Please use app.core.unified_memory instead.",
    DeprecationWarning,
    stacklevel=2
)
"""