#!/usr/bin/env python3

"""
Session Memory System for ChatMRPT

Tracks conversation history, analysis results, and context to enable
intelligent follow-up questions and method explanations.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

class MessageType(Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    ANALYSIS = "analysis"

@dataclass
class ConversationMessage:
    timestamp: str
    type: MessageType
    content: str
    metadata: Dict[str, Any] = None
    
    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'type': self.type.value,
            'content': self.content,
            'metadata': self.metadata or {}
        }

@dataclass
class AnalysisContext:
    """Store analysis-specific context for intelligent explanations"""
    session_id: str
    status: str
    variables_used: List[str] = None
    composite_method: str = "expert_weighted"
    pca_components: int = None
    top_risk_wards: List[str] = None
    low_risk_wards: List[str] = None
    method_agreement: Dict[str, Any] = None
    score_ranges: Dict[str, tuple] = None
    analysis_timestamp: str = None
    
    def to_dict(self):
        return asdict(self)

class SessionMemory:
    """
    Manages conversation history and analysis context for intelligent responses
    """
    
    def __init__(self, session_id: str, storage_path: str = "instance/memory"):
        self.session_id = session_id
        self.storage_path = storage_path
        self.memory_file = os.path.join(storage_path, f"{session_id}_memory.json")
        
        # Initialize storage
        os.makedirs(storage_path, exist_ok=True)
        
        # Load existing memory or initialize new
        self.conversation_history: List[ConversationMessage] = []
        self.analysis_context: Optional[AnalysisContext] = None
        self.user_preferences: Dict[str, Any] = {}
        self.key_entities: Dict[str, Any] = {}  # Track mentioned wards, variables, etc.
        
        self._load_memory()
    
    def add_message(self, message_type: MessageType, content: str, metadata: Dict[str, Any] = None):
        """Add a message to conversation history"""
        message = ConversationMessage(
            timestamp=datetime.utcnow().isoformat(),
            type=message_type,
            content=content,
            metadata=metadata or {}
        )
        
        self.conversation_history.append(message)
        self._extract_entities(message)
        self._save_memory()
    
    def update_analysis_context(self, analysis_data: Dict[str, Any]):
        """Update analysis context with results"""
        self.analysis_context = AnalysisContext(
            session_id=self.session_id,
            status=analysis_data.get('status', 'unknown'),
            variables_used=analysis_data.get('variables_used', []),
            composite_method=analysis_data.get('method', 'expert_weighted'),
            pca_components=analysis_data.get('pca_components', None),
            top_risk_wards=analysis_data.get('top_risk_wards', []),
            low_risk_wards=analysis_data.get('low_risk_wards', []),
            method_agreement=analysis_data.get('method_agreement', {}),
            score_ranges=analysis_data.get('score_ranges', {}),
            analysis_timestamp=datetime.utcnow().isoformat()
        )
        
        # Add analysis summary to conversation
        self.add_message(
            MessageType.ANALYSIS,
            f"Analysis completed: {len(self.analysis_context.variables_used)} variables, "
            f"top risk: {', '.join(self.analysis_context.top_risk_wards[:3])}...",
            metadata=self.analysis_context.to_dict()
        )
        
        self._save_memory()
    
    def get_conversation_context(self, last_n_messages: int = 5) -> str:
        """Get recent conversation context for LLM"""
        recent_messages = self.conversation_history[-last_n_messages:]
        
        context_parts = []
        
        # Add analysis context if available
        if self.analysis_context:
            context_parts.append(f"""
📊 **Current Analysis Context:**
- Variables used: {', '.join(self.analysis_context.variables_used)}
- Top risk wards: {', '.join(self.analysis_context.top_risk_wards[:5])}
- Low risk wards: {', '.join(self.analysis_context.low_risk_wards[:5])}
- Analysis method: {self.analysis_context.composite_method}
- Completed: {self.analysis_context.analysis_timestamp}
""")
        
        # Add recent conversation
        if recent_messages:
            context_parts.append("\n🗣️ **Recent Conversation:**")
            for msg in recent_messages:
                role = "User" if msg.type == MessageType.USER else "Assistant"
                context_parts.append(f"- {role}: {msg.content[:100]}...")
        
        # Add key entities mentioned
        if self.key_entities:
            context_parts.append(f"\n🎯 **Key Topics Discussed:** {', '.join(self.key_entities.keys())}")
        
        return '\n'.join(context_parts)
    
    def get_method_explanation_context(self) -> str:
        """Get context for explaining analysis methods"""
        if not self.analysis_context:
            return "No analysis has been completed yet."
        
        return f"""
🔬 **Analysis Method Details:**

**Composite Scoring Method:**
- Variables used: {', '.join(self.analysis_context.variables_used)}
- Weighting approach: Expert-based epidemiological weights
- Score range: {self.analysis_context.score_ranges.get('composite', 'N/A')}
- Purpose: Create interpretable risk scores for policy decisions

**Principal Component Analysis (PCA):**
- Components extracted: {self.analysis_context.pca_components or 'Multiple'}
- Purpose: Identify data-driven patterns and reduce dimensionality
- Score range: {self.analysis_context.score_ranges.get('pca', 'N/A')}
- Advantage: Unbiased statistical approach

**Method Comparison:**
- Agreement on high-risk wards: {self.analysis_context.method_agreement.get('high_risk_agreement', 'N/A')}
- Agreement on low-risk wards: {self.analysis_context.method_agreement.get('low_risk_agreement', 'N/A')}
"""
    
    def find_previous_mentions(self, query: str) -> List[ConversationMessage]:
        """Find previous messages mentioning specific topics"""
        query_lower = query.lower()
        matches = []
        
        for msg in self.conversation_history:
            if query_lower in msg.content.lower():
                matches.append(msg)
        
        return matches[-3:]  # Return last 3 matches
    
    def _extract_entities(self, message: ConversationMessage):
        """Extract key entities from messages for better context tracking"""
        content_lower = message.content.lower()
        
        # Track ward names mentioned
        if self.analysis_context:
            for ward in self.analysis_context.top_risk_wards + self.analysis_context.low_risk_wards:
                if ward.lower() in content_lower:
                    self.key_entities[f"ward:{ward}"] = {
                        'type': 'ward',
                        'name': ward,
                        'last_mentioned': message.timestamp
                    }
        
        # Track analysis methods mentioned
        methods = ['composite', 'pca', 'principal component', 'vulnerability', 'risk score']
        for method in methods:
            if method in content_lower:
                self.key_entities[f"method:{method}"] = {
                    'type': 'method',
                    'name': method,
                    'last_mentioned': message.timestamp
                }
        
        # Track variables mentioned
        if self.analysis_context:
            for var in self.analysis_context.variables_used:
                if var.lower() in content_lower:
                    self.key_entities[f"variable:{var}"] = {
                        'type': 'variable',
                        'name': var,
                        'last_mentioned': message.timestamp
                    }
    
    def _load_memory(self):
        """Load memory from disk"""
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, 'r') as f:
                    data = json.load(f)
                
                # Load conversation history
                for msg_data in data.get('conversation_history', []):
                    msg = ConversationMessage(
                        timestamp=msg_data['timestamp'],
                        type=MessageType(msg_data['type']),
                        content=msg_data['content'],
                        metadata=msg_data.get('metadata', {})
                    )
                    self.conversation_history.append(msg)
                
                # Load analysis context
                if 'analysis_context' in data and data['analysis_context']:
                    self.analysis_context = AnalysisContext(**data['analysis_context'])
                
                # Load other data
                self.user_preferences = data.get('user_preferences', {})
                self.key_entities = data.get('key_entities', {})
                
            except Exception as e:
                print(f"Warning: Could not load memory for session {self.session_id}: {e}")
    
    def _save_memory(self):
        """Save memory to disk"""
        try:
            data = {
                'session_id': self.session_id,
                'conversation_history': [msg.to_dict() for msg in self.conversation_history],
                'analysis_context': self.analysis_context.to_dict() if self.analysis_context else None,
                'user_preferences': self.user_preferences,
                'key_entities': self.key_entities,
                'last_updated': datetime.utcnow().isoformat()
            }
            
            with open(self.memory_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            print(f"Warning: Could not save memory for session {self.session_id}: {e}")
    
    def clear_memory(self):
        """Clear all session memory"""
        self.conversation_history = []
        self.analysis_context = None
        self.user_preferences = {}
        self.key_entities = {}
        
        if os.path.exists(self.memory_file):
            os.remove(self.memory_file)
    
    def get_memory_summary(self) -> Dict[str, Any]:
        """Get summary of current memory state"""
        return {
            'session_id': self.session_id,
            'conversation_length': len(self.conversation_history),
            'has_analysis': self.analysis_context is not None,
            'key_entities_count': len(self.key_entities),
            'last_activity': self.conversation_history[-1].timestamp if self.conversation_history else None
        } 