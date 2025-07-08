"""
Base classes and interfaces for ChatMRPT Pydantic-based tool system.

This module provides the foundation for the new tool architecture that uses
Pydantic models for automatic parameter validation, schema generation, and
improved tool selection accuracy.
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel, Field, validator
from enum import Enum

logger = logging.getLogger(__name__)


class ToolCategory(str, Enum):
    """Categories for organizing tools"""
    DATA_ANALYSIS = "data_analysis"
    STATISTICAL = "statistical"
    VISUALIZATION = "visualization"
    KNOWLEDGE = "knowledge"
    GENERAL_KNOWLEDGE = "general_knowledge"  # Added for smart knowledge tools
    SYSTEM = "system"
    SPATIAL_ANALYSIS = "spatial_analysis"
    GROUP_ANALYSIS = "group_analysis"
    METHODOLOGY = "methodology"
    ENVIRONMENTAL_RISK = "environmental_risk"
    INTERVENTION_TARGETING = "intervention_targeting"
    SCENARIO_SIMULATION = "scenario_simulation"
    STRATEGIC_DECISION = "strategic_decision"
    MEMORY = "memory"
    SETTLEMENT_VALIDATION = "settlement_validation"
    SETTLEMENT_TOOLS = "settlement_tools"
    SETTLEMENT_VISUALIZATION = "settlement_visualization"
    VISUAL_EXPLANATION = "visual_explanation"


class ToolExecutionResult(BaseModel):
    """Standardized result format for all tool executions"""
    success: bool = Field(..., description="Whether the tool execution was successful")
    message: str = Field(..., description="Human-readable message about the result")
    data: Optional[Dict[str, Any]] = Field(None, description="Tool-specific result data")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Execution metadata and statistics")
    execution_time: Optional[float] = Field(None, description="Execution time in seconds")
    web_path: Optional[str] = Field(None, description="Path to generated web visualization")
    chart_type: Optional[str] = Field(None, description="Type of chart/visualization created")
    error_details: Optional[str] = Field(None, description="Detailed error information")

    class Config:
        extra = "allow"  # Allow additional fields for tool-specific data


class BaseTool(BaseModel, ABC):
    """
    Abstract base class for all ChatMRPT tools.
    
    All tools must inherit from this class and implement the execute method.
    This provides automatic parameter validation, schema generation, and
    consistent error handling.
    """
    
    class Config:
        # Ensure all fields are included in schema generation
        extra = "forbid"
        # Use enum values in schema
        use_enum_values = True
        # Validate assignment
        validate_assignment = True
        # Allow reuse of models
        allow_reuse = True
    
    @classmethod
    def get_tool_name(cls) -> str:
        """Get the tool name for registration"""
        return cls.__name__.lower()
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        """Get the tool category - override in subclasses"""
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_description(cls) -> str:
        """Get tool description from docstring"""
        return cls.__doc__ or f"Execute {cls.get_tool_name()}"
    
    @classmethod
    def get_examples(cls) -> List[str]:
        """Get usage examples - override in subclasses"""
        return []
    
    @abstractmethod
    def execute(self, session_id: str) -> ToolExecutionResult:
        """
        Execute the tool with validated parameters.
        
        Args:
            session_id: Session identifier for data access
            
        Returns:
            ToolExecutionResult with status, message, and data
        """
        pass
    
    def _create_success_result(self, message: str, data: Optional[Dict] = None, 
                             **kwargs) -> ToolExecutionResult:
        """Helper to create successful result"""
        return ToolExecutionResult(
            success=True,
            message=message,
            data=data or {},
            **kwargs
        )
    
    def _create_error_result(self, message: str, error_details: Optional[str] = None,
                           **kwargs) -> ToolExecutionResult:
        """Helper to create error result"""
        return ToolExecutionResult(
            success=False, 
            message=message,
            error_details=error_details,
            **kwargs
        )


class DataAnalysisTool(BaseTool):
    """Base class for data analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS


class StatisticalTool(BaseTool):
    """Base class for statistical analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STATISTICAL


class VisualizationTool(BaseTool):
    """Base class for visualization tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION


class KnowledgeTool(BaseTool):
    """Base class for knowledge and explanation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.KNOWLEDGE


class SystemTool(BaseTool):
    """Base class for system and utility tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SYSTEM


class ScenarioSimulationTool(BaseTool):
    """Base class for scenario simulation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION


class SpatialAnalysisTool(BaseTool):
    """Base class for spatial analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SPATIAL_ANALYSIS


class EnvironmentalRiskTool(BaseTool):
    """Base class for environmental risk analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.ENVIRONMENTAL_RISK


class SpatialTool(BaseTool):
    """Base class for spatial analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SPATIAL_ANALYSIS


class InterventionTargetingTool(BaseTool):
    """Base class for intervention targeting tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.INTERVENTION_TARGETING


class ScenarioTool(BaseTool):
    """Base class for scenario simulation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SCENARIO_SIMULATION


class StrategicDecisionTool(BaseTool):
    """Base class for strategic decision support tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.STRATEGIC_DECISION


class GroupAnalysisTool(BaseTool):
    """Base class for group analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.GROUP_ANALYSIS


class MethodologyTool(BaseTool):
    """Base class for methodology explanation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.METHODOLOGY


class SettlementValidationTool(BaseTool):
    """Base class for settlement validation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SETTLEMENT_VALIDATION


class SettlementTool(BaseTool):
    """Base class for settlement analysis tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SETTLEMENT_TOOLS


class SettlementVisualizationTool(BaseTool):
    """Base class for settlement visualization tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SETTLEMENT_VISUALIZATION


class VisualExplanationTool(BaseTool):
    """Base class for visual explanation tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUAL_EXPLANATION


class MemoryTool(BaseTool):
    """Base class for conversation memory tools"""
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.MEMORY


# Common field types and validators
class WardName(str):
    """Custom type for ward names with validation"""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('Ward name must be a string')
        if len(v.strip()) == 0:
            raise ValueError('Ward name cannot be empty')
        return v.strip()


class TopN(int):
    """Custom type for top_n parameters with validation"""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        if not isinstance(v, int):
            raise TypeError('top_n must be an integer')
        if v < 1:
            raise ValueError('top_n must be at least 1')
        if v > 1000:
            raise ValueError('top_n cannot exceed 1000')
        return v


# Common parameter definitions that can be reused
def top_n_field(default: int = 10, description: str = "Number of top items to return") -> Field:
    """Standard top_n field with validation"""
    return Field(
        default=default,
        description=description,
        ge=1,
        le=1000
    )


def ward_name_field(description: str = "Name of the ward") -> Field:
    """Standard ward_name field"""
    return Field(
        ...,
        description=description,
        min_length=1,
        max_length=200
    )


def session_context_field() -> Field:
    """Field for session context (usually not set by user)"""
    return Field(
        None,
        description="Session context (automatically populated)"
    )


# Tool metadata for discovery and documentation
class ToolMetadata(BaseModel):
    """Metadata about a tool for discovery and documentation"""
    name: str
    category: ToolCategory
    description: str
    parameters: Dict[str, Any]  # JSON schema
    examples: List[str]
    tags: List[str] = Field(default_factory=list)
    is_experimental: bool = False
    requires_data: bool = True
    estimated_execution_time: Optional[str] = None


def create_tool_metadata(tool_class: type) -> ToolMetadata:
    """Create metadata for a tool class"""
    return ToolMetadata(
        name=tool_class.get_tool_name(),
        category=tool_class.get_category(),
        description=tool_class.get_description(),
        parameters=tool_class.schema(),
        examples=tool_class.get_examples()
    )


# Validation utilities
def validate_session_data_exists(session_id: str) -> bool:
    """Check if session has uploaded data"""
    try:
        import os
        session_folder = f"instance/uploads/{session_id}"
        
        # Check if session folder exists
        if not os.path.exists(session_folder):
            return False
        
        # Check for unified dataset (preferred) or fallback to raw data
        unified_geoparquet = os.path.join(session_folder, "unified_dataset.geoparquet")
        unified_csv = os.path.join(session_folder, "unified_dataset.csv")
        raw_data = os.path.join(session_folder, "raw_data.csv")
        processed_data = os.path.join(session_folder, "processed_data.csv")  # Legacy support
        
        return (os.path.exists(unified_geoparquet) or 
                os.path.exists(unified_csv) or 
                os.path.exists(raw_data) or 
                os.path.exists(processed_data))
    except Exception:
        return False


def get_session_unified_dataset(session_id: str):
    """Get unified dataset for session with error handling and automatic creation"""
    try:
        # Lazy import to avoid 30-second startup delay from geopandas
        from ..data.unified_dataset_builder import load_unified_dataset, UnifiedDatasetBuilder
        
        # First try to load existing unified dataset
        gdf = load_unified_dataset(session_id)
        if gdf is not None:
            return gdf
        
        # If unified dataset doesn't exist, check if we can create it from raw or cleaned data
        session_folder = f"instance/uploads/{session_id}"
        csv_exists = (os.path.exists(os.path.join(session_folder, "processed_data.csv")) or
                     os.path.exists(os.path.join(session_folder, "analysis_cleaned_data.csv")) or
                     os.path.exists(os.path.join(session_folder, "raw_data.csv")))
        shapefile_exists = (os.path.exists(os.path.join(session_folder, "shapefile", "processed.shp")) or
                           os.path.exists(os.path.join(session_folder, "raw_shapefile.zip")))
        
        if csv_exists and shapefile_exists:
            logger.info(f"Unified dataset not found for session {session_id}, attempting to create it...")
            
            # Try to build unified dataset
            builder = UnifiedDatasetBuilder(session_id)
            result = builder.build_unified_dataset()
            
            if result['status'] == 'success':
                logger.info(f"Successfully created unified dataset for session {session_id}")
                # Try to load it again
                return load_unified_dataset(session_id)
            else:
                logger.error(f"Failed to build unified dataset: {result.get('message')}")
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to load unified dataset for session {session_id}: {e}")
        return None