"""
Session state management for ChatMRPT application.

This module provides unified session state management, workflow tracking,
and state validation utilities.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from datetime import datetime


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
class UserCapabilities:
    """Track user capabilities and permissions."""
    can_upload: bool = True
    can_analyze: bool = True
    can_visualize: bool = True
    can_export: bool = True
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