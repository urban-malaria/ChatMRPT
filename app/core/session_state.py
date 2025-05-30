"""
Session State Management for ChatMRPT

This module defines the state models for tracking user workflow progression,
data availability, and contextual capabilities throughout the malaria risk analysis process.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import json
from datetime import datetime


class WorkflowStage(Enum):
    """User workflow stages in the malaria risk analysis process"""
    INITIAL = "initial"                    # User just started, no data uploaded
    DATA_UPLOAD = "data_upload"           # User is uploading files
    DATA_VALIDATION = "data_validation"    # Files uploaded, checking compatibility
    DATA_READY = "data_ready"             # Data validated and ready for analysis
    ANALYSIS_RUNNING = "analysis_running"  # Analysis in progress
    ANALYSIS_COMPLETE = "analysis_complete" # Analysis finished, results available
    MAPS_AVAILABLE = "maps_available"      # Visualizations have been generated


class DataState(Enum):
    """Data availability states"""
    NO_DATA = "no_data"                   # No files uploaded
    CSV_ONLY = "csv_only"                 # Only CSV file uploaded
    SHAPEFILE_ONLY = "shapefile_only"     # Only shapefile uploaded
    BOTH_LOADED = "both_loaded"           # Both CSV and shapefile uploaded
    VALIDATED = "validated"               # Data validated and compatible
    PROCESSED = "processed"               # Data cleaned and normalized


class AnalysisState(Enum):
    """Analysis completion states"""
    NOT_STARTED = "not_started"
    CLEANING = "cleaning"
    NORMALIZING = "normalizing"
    CALCULATING_SCORES = "calculating_scores"
    GENERATING_RANKINGS = "generating_rankings"
    COMPLETE = "complete"
    ERROR = "error"


@dataclass
class FileInfo:
    """Information about uploaded files"""
    filename: str
    size: int
    upload_time: datetime
    rows: Optional[int] = None
    columns: Optional[int] = None
    features: Optional[int] = None
    status: str = "uploaded"


@dataclass
class DataSummary:
    """Summary of loaded data"""
    csv_info: Optional[FileInfo] = None
    shapefile_info: Optional[FileInfo] = None
    available_variables: List[str] = field(default_factory=list)
    ward_count: int = 0
    ward_mismatches: List[str] = field(default_factory=list)
    quality_issues: List[str] = field(default_factory=list)


@dataclass
class AnalysisResults:
    """Analysis results and metadata"""
    composite_scores: Optional[Dict[str, Any]] = None
    vulnerability_rankings: Optional[Dict[str, Any]] = None
    normalized_data: Optional[Dict[str, Any]] = None
    analysis_metadata: Optional[Dict[str, Any]] = None
    completed_steps: List[str] = field(default_factory=list)
    error_messages: List[str] = field(default_factory=list)


@dataclass
class UserCapabilities:
    """What the user can currently do"""
    can_upload_csv: bool = True
    can_upload_shapefile: bool = True
    can_start_analysis: bool = False
    can_create_maps: bool = False
    can_view_rankings: bool = False
    can_generate_reports: bool = False
    can_download_results: bool = False


@dataclass
class SessionState:
    """Complete session state tracking"""
    # Core states
    workflow_stage: WorkflowStage = WorkflowStage.INITIAL
    data_state: DataState = DataState.NO_DATA
    analysis_state: AnalysisState = AnalysisState.NOT_STARTED
    
    # Data information
    data_summary: DataSummary = field(default_factory=DataSummary)
    
    # Analysis information
    analysis_results: AnalysisResults = field(default_factory=AnalysisResults)
    
    # User context
    capabilities: UserCapabilities = field(default_factory=UserCapabilities)
    
    # Interaction history
    last_action: Optional[str] = None
    last_action_time: Optional[datetime] = None
    conversation_context: Dict[str, Any] = field(default_factory=dict)
    
    # Guidance and suggestions
    next_suggestions: List[str] = field(default_factory=list)
    help_topics: List[str] = field(default_factory=list)
    
    # Session metadata
    session_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert session state to dictionary for storage"""
        def serialize_value(value):
            if isinstance(value, Enum):
                return value.value
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, (FileInfo, DataSummary, AnalysisResults, UserCapabilities)):
                return serialize_dataclass(value)
            elif isinstance(value, list):
                return [serialize_value(item) for item in value]
            elif isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            else:
                return value
        
        def serialize_dataclass(obj):
            result = {}
            for key, value in obj.__dict__.items():
                result[key] = serialize_value(value)
            return result
        
        return serialize_dataclass(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SessionState':
        """Create session state from dictionary"""
        def deserialize_datetime(value):
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value)
                except (ValueError, TypeError):
                    return datetime.now()
            return value or datetime.now()
        
        # Handle enums
        workflow_stage = WorkflowStage(data.get('workflow_stage', WorkflowStage.INITIAL.value))
        data_state = DataState(data.get('data_state', DataState.NO_DATA.value))
        analysis_state = AnalysisState(data.get('analysis_state', AnalysisState.NOT_STARTED.value))
        
        # Handle nested dataclasses
        data_summary_dict = data.get('data_summary', {})
        data_summary = DataSummary(
            csv_info=FileInfo(**data_summary_dict.get('csv_info', {})) if data_summary_dict.get('csv_info') else None,
            shapefile_info=FileInfo(**data_summary_dict.get('shapefile_info', {})) if data_summary_dict.get('shapefile_info') else None,
            available_variables=data_summary_dict.get('available_variables', []),
            ward_count=data_summary_dict.get('ward_count', 0),
            ward_mismatches=data_summary_dict.get('ward_mismatches', []),
            quality_issues=data_summary_dict.get('quality_issues', [])
        )
        
        analysis_results_dict = data.get('analysis_results', {})
        analysis_results = AnalysisResults(
            composite_scores=analysis_results_dict.get('composite_scores'),
            vulnerability_rankings=analysis_results_dict.get('vulnerability_rankings'),
            normalized_data=analysis_results_dict.get('normalized_data'),
            analysis_metadata=analysis_results_dict.get('analysis_metadata'),
            completed_steps=analysis_results_dict.get('completed_steps', []),
            error_messages=analysis_results_dict.get('error_messages', [])
        )
        
        capabilities_dict = data.get('capabilities', {})
        capabilities = UserCapabilities(
            can_upload_csv=capabilities_dict.get('can_upload_csv', True),
            can_upload_shapefile=capabilities_dict.get('can_upload_shapefile', True),
            can_start_analysis=capabilities_dict.get('can_start_analysis', False),
            can_create_maps=capabilities_dict.get('can_create_maps', False),
            can_view_rankings=capabilities_dict.get('can_view_rankings', False),
            can_generate_reports=capabilities_dict.get('can_generate_reports', False),
            can_download_results=capabilities_dict.get('can_download_results', False)
        )
        
        return cls(
            workflow_stage=workflow_stage,
            data_state=data_state,
            analysis_state=analysis_state,
            data_summary=data_summary,
            analysis_results=analysis_results,
            capabilities=capabilities,
            last_action=data.get('last_action'),
            last_action_time=deserialize_datetime(data.get('last_action_time')),
            conversation_context=data.get('conversation_context', {}),
            next_suggestions=data.get('next_suggestions', []),
            help_topics=data.get('help_topics', []),
            session_id=data.get('session_id'),
            created_at=deserialize_datetime(data.get('created_at')),
            last_updated=deserialize_datetime(data.get('last_updated'))
        )
    
    def update_timestamp(self):
        """Update the last_updated timestamp"""
        self.last_updated = datetime.now()


# Convenience functions for common state checks
def is_data_ready_for_analysis(state: SessionState) -> bool:
    """Check if data is ready for analysis"""
    return state.data_state in [DataState.VALIDATED, DataState.PROCESSED]


def is_analysis_complete(state: SessionState) -> bool:
    """Check if analysis is complete"""
    return state.analysis_state == AnalysisState.COMPLETE


def can_create_visualizations(state: SessionState) -> bool:
    """Check if visualizations can be created"""
    return is_analysis_complete(state) and state.capabilities.can_create_maps


def get_workflow_progress_percentage(state: SessionState) -> int:
    """Get workflow completion percentage"""
    stage_weights = {
        WorkflowStage.INITIAL: 0,
        WorkflowStage.DATA_UPLOAD: 20,
        WorkflowStage.DATA_VALIDATION: 30,
        WorkflowStage.DATA_READY: 40,
        WorkflowStage.ANALYSIS_RUNNING: 60,
        WorkflowStage.ANALYSIS_COMPLETE: 80,
        WorkflowStage.MAPS_AVAILABLE: 100
    }
    return stage_weights.get(state.workflow_stage, 0) 