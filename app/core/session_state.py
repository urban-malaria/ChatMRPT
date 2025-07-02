"""
Session state management for ChatMRPT application.

This module provides unified session state management, workflow tracking,
and state validation utilities.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class WorkflowStage(Enum):
    """Enumeration of workflow stages."""
    INITIALIZATION = "initialization"
    DATA_LOADING = "data_loading"
    DATA_PROCESSING = "data_processing"
    ANALYSIS = "analysis"
    VISUALIZATION = "visualization"
    COMPLETED = "completed"


class DataState(Enum):
    """Enumeration of data states."""
    NOT_LOADED = "not_loaded"
    LOADING = "loading"
    LOADED = "loaded"
    PROCESSED = "processed"
    ERROR = "error"


class AnalysisState(Enum):
    """Enumeration of analysis states."""
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


class ConversationMode(Enum):
    """Enumeration of conversation modes."""
    SIMPLE_CHAT = "simple_chat"
    AGENT_ANALYSIS = "agent_analysis" 
    TOOL_EXECUTION = "tool_execution"
    MEMORY_RETRIEVAL = "memory_retrieval"


@dataclass
class FileInfo:
    """Information about uploaded files."""
    filename: str
    size: int
    upload_time: datetime
    file_type: str
    status: str = "uploaded"


@dataclass
class DataSummary:
    """Summary of loaded data."""
    total_records: int = 0
    columns: List[str] = field(default_factory=list)
    data_types: Dict[str, str] = field(default_factory=dict)
    missing_values: Dict[str, int] = field(default_factory=dict)
    geographic_columns: List[str] = field(default_factory=list)
    last_updated: Optional[datetime] = None


@dataclass
class AnalysisResults:
    """Results from analysis operations."""
    method: str
    status: str
    results: Dict[str, Any] = field(default_factory=dict)
    visualizations: List[str] = field(default_factory=list)
    execution_time: Optional[float] = None
    timestamp: Optional[datetime] = None


@dataclass
class ConversationState:
    """Track conversation state and context."""
    total_exchanges: int = 0
    conversation_mode: ConversationMode = ConversationMode.SIMPLE_CHAT
    last_tools_used: List[str] = field(default_factory=list)
    context_entities: List[str] = field(default_factory=list)
    user_role: str = "analyst"
    conversation_quality_avg: float = 0.0
    memory_enabled: bool = True
    last_query: str = ""
    last_response: str = ""
    conversation_started_at: Optional[datetime] = None


@dataclass
class UserCapabilities:
    """Track user capabilities and permissions."""
    can_upload: bool = True
    can_analyze: bool = True
    can_visualize: bool = True
    can_export: bool = True
    can_chat: bool = True
    can_use_memory: bool = True
    max_file_size: int = 100 * 1024 * 1024  # 100MB default


@dataclass
class SessionState:
    """Unified session state management."""
    session_id: str
    user_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    
    # Workflow tracking
    current_stage: WorkflowStage = WorkflowStage.INITIALIZATION
    completed_stages: List[WorkflowStage] = field(default_factory=list)
    
    # Data state
    data_state: DataState = DataState.NOT_LOADED
    uploaded_files: List[FileInfo] = field(default_factory=list)
    data_summary: Optional[DataSummary] = None
    
    # Analysis state
    analysis_state: AnalysisState = AnalysisState.NOT_STARTED
    analysis_results: List[AnalysisResults] = field(default_factory=list)
    
    # Conversation state (Phase 1 addition)
    conversation_state: ConversationState = field(default_factory=ConversationState)
    
    # User capabilities
    capabilities: UserCapabilities = field(default_factory=UserCapabilities)
    
    # Session metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_activity(self):
        """Update last activity timestamp."""
        self.last_activity = datetime.now()
    
    def add_file(self, file_info: FileInfo):
        """Add uploaded file information."""
        self.uploaded_files.append(file_info)
        self.update_activity()
    
    def set_data_summary(self, summary: DataSummary):
        """Set data summary information."""
        self.data_summary = summary
        self.data_state = DataState.LOADED
        self.update_activity()
    
    def add_analysis_result(self, result: AnalysisResults):
        """Add analysis result."""
        self.analysis_results.append(result)
        if result.status == "completed":
            self.analysis_state = AnalysisState.COMPLETED
        self.update_activity()
    
    def advance_stage(self, stage: WorkflowStage):
        """Advance to next workflow stage."""
        if self.current_stage not in self.completed_stages:
            self.completed_stages.append(self.current_stage)
        self.current_stage = stage
        self.update_activity()
    
    # Phase 1: Conversation state management methods
    
    def start_conversation(self, user_role: str = "analyst"):
        """Initialize conversation state."""
        if self.conversation_state.conversation_started_at is None:
            self.conversation_state.conversation_started_at = datetime.now()
            self.conversation_state.user_role = user_role
        self.update_activity()
    
    def update_conversation(self, query: str, response: str, tools_used: List[str] = None, 
                          conversation_mode: ConversationMode = None, quality_score: float = 0.0):
        """Update conversation state with new exchange."""
        self.conversation_state.total_exchanges += 1
        self.conversation_state.last_query = query
        self.conversation_state.last_response = response
        
        if tools_used:
            self.conversation_state.last_tools_used = tools_used
        
        if conversation_mode:
            self.conversation_state.conversation_mode = conversation_mode
        
        # Update rolling average quality score
        if quality_score > 0:
            current_avg = self.conversation_state.conversation_quality_avg
            total = self.conversation_state.total_exchanges
            self.conversation_state.conversation_quality_avg = (
                (current_avg * (total - 1) + quality_score) / total
            )
        
        self.update_activity()
    
    def add_context_entity(self, entity: str):
        """Add an entity to conversation context."""
        if entity not in self.conversation_state.context_entities:
            self.conversation_state.context_entities.append(entity)
            # Keep only last 20 entities to avoid memory bloat
            if len(self.conversation_state.context_entities) > 20:
                self.conversation_state.context_entities = self.conversation_state.context_entities[-20:]
    
    def set_user_role(self, role: str):
        """Set user role for conversation adaptation."""
        self.conversation_state.user_role = role
        self.update_activity()
    
    def get_conversation_duration(self) -> Optional[float]:
        """Get conversation duration in minutes."""
        if self.conversation_state.conversation_started_at:
            duration = datetime.now() - self.conversation_state.conversation_started_at
            return duration.total_seconds() / 60.0
        return None
    
    def can_use_memory(self) -> bool:
        """Check if user can use conversation memory."""
        return (
            self.capabilities.can_use_memory and 
            self.conversation_state.memory_enabled
        )
    
    def toggle_memory(self, enabled: bool = None):
        """Toggle conversation memory on/off."""
        if enabled is None:
            self.conversation_state.memory_enabled = not self.conversation_state.memory_enabled
        else:
            self.conversation_state.memory_enabled = enabled
        self.update_activity()


# Utility functions for state validation

def is_data_ready_for_analysis(session_state: SessionState) -> bool:
    """Check if data is ready for analysis."""
    return (
        session_state.data_state == DataState.LOADED and
        session_state.data_summary is not None and
        session_state.data_summary.total_records > 0
    )


def is_analysis_complete(session_state: SessionState) -> bool:
    """Check if analysis is complete."""
    return (
        session_state.analysis_state == AnalysisState.COMPLETED and
        len(session_state.analysis_results) > 0
    )


def can_create_visualizations(session_state: SessionState) -> bool:
    """Check if visualizations can be created."""
    return (
        is_analysis_complete(session_state) and
        session_state.capabilities.can_visualize
    )


def get_workflow_progress_percentage(session_state: SessionState) -> float:
    """Get workflow progress as percentage."""
    total_stages = len(WorkflowStage)
    completed_count = len(session_state.completed_stages)
    
    # Add current stage if not in completed
    if session_state.current_stage not in session_state.completed_stages:
        completed_count += 0.5  # Partial credit for current stage
    
    return min(100.0, (completed_count / total_stages) * 100)


# Phase 1: Conversation state utility functions

def is_conversation_active(session_state: SessionState) -> bool:
    """Check if conversation is active."""
    return (
        session_state.conversation_state.conversation_started_at is not None and
        session_state.conversation_state.total_exchanges > 0
    )


def needs_agent_processing(query: str, session_state: SessionState) -> bool:
    """Determine if query needs agent processing vs simple chat."""
    # Check for analysis indicators
    analysis_keywords = ['analyze', 'show', 'create', 'calculate', 'rank', 'compare', 
                        'highest risk', 'statistics', 'visualization', 'intervention']
    
    query_lower = query.lower()
    has_analysis_intent = any(keyword in query_lower for keyword in analysis_keywords)
    
    # Check if data is available for analysis
    has_data = is_data_ready_for_analysis(session_state)
    
    # Agent processing needed if analysis intent AND data available
    return has_analysis_intent and has_data


def get_conversation_summary(session_state: SessionState) -> Dict[str, Any]:
    """Get summary of conversation state."""
    conv_state = session_state.conversation_state
    
    summary = {
        'total_exchanges': conv_state.total_exchanges,
        'conversation_mode': conv_state.conversation_mode.value,
        'user_role': conv_state.user_role,
        'duration_minutes': session_state.get_conversation_duration(),
        'quality_avg': conv_state.conversation_quality_avg,
        'memory_enabled': conv_state.memory_enabled,
        'context_entities_count': len(conv_state.context_entities),
        'last_tools_used': conv_state.last_tools_used,
        'is_active': is_conversation_active(session_state)
    }
    
    return summary


def update_conversation_context(session_state: SessionState, entities: List[str]):
    """Update conversation context with entities from latest exchange."""
    for entity in entities:
        session_state.add_context_entity(entity)


def should_store_in_memory(session_state: SessionState, response_length: int = 0) -> bool:
    """Determine if conversation turn should be stored in memory."""
    if not session_state.can_use_memory():
        return False
    
    # Store if meaningful exchange (not just greetings)
    if session_state.conversation_state.total_exchanges == 0:
        return False  # Skip first exchange if it's just initialization
    
    # Store if response has substance (not too short)
    if response_length > 50:
        return True
    
    # Store if tools were used
    if session_state.conversation_state.last_tools_used:
        return True
    
    return False 