"""
Data Preparation Tools for ChatMRPT

This module provides tools for preparing data for analysis, including
building the unified dataset that other tools depend on.
"""

import logging
import os
from typing import Dict, Any, Optional, List
from pydantic import Field

from .base import (
    SystemTool, ToolExecutionResult, ToolCategory
)

logger = logging.getLogger(__name__)


class CreateUnifiedDataset(SystemTool):
    """
    Create or rebuild the unified dataset from session data.
    
    This tool builds a comprehensive GeoParquet dataset that combines:
    - Original uploaded data (CSV + shapefile)
    - Analysis results (if available)
    - Spatial calculations
    - Metadata
    
    The unified dataset is required for most analysis tools to function.
    """
    
    force_rebuild: bool = Field(
        False,
        description="Force rebuild even if unified dataset already exists"
    )
    
    include_analysis: bool = Field(
        True,
        description="Include analysis results if available"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SYSTEM
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Create unified dataset",
            "Build analysis dataset",
            "Prepare data for analysis",
            "Rebuild unified dataset"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Build unified dataset for session"""
        try:
            from ..data.unified_dataset_builder import UnifiedDatasetBuilder
            
            # Check if unified dataset already exists
            unified_path = os.path.join(f"instance/uploads/{session_id}", "unified_dataset.geoparquet")
            
            if os.path.exists(unified_path) and not self.force_rebuild:
                return self._create_success_result(
                    message="Unified dataset already exists. Use force_rebuild=True to rebuild.",
                    data={
                        'file_path': unified_path,
                        'exists': True,
                        'action': 'skipped'
                    }
                )
            
            # Build unified dataset
            builder = UnifiedDatasetBuilder(session_id)
            result = builder.build_unified_dataset()
            
            if result['status'] == 'success':
                dataset = result.get('dataset')
                metadata = result.get('metadata', {})
                
                return self._create_success_result(
                    message=result.get('message', 'Unified dataset created successfully'),
                    data={
                        'file_paths': result.get('file_paths', {}),
                        'rows': len(dataset) if dataset is not None else 0,
                        'columns': len(dataset.columns) if dataset is not None else 0,
                        'has_geometry': 'geometry' in dataset.columns if dataset is not None else False,
                        'has_analysis': metadata.get('has_analysis_results', False),
                        'action': 'created' if not os.path.exists(unified_path) else 'rebuilt'
                    }
                )
            else:
                return self._create_error_result(
                    message=result.get('message', 'Failed to build unified dataset'),
                    error_details=str(result)
                )
                
        except Exception as e:
            logger.error(f"Error creating unified dataset: {e}")
            return self._create_error_result(
                message=f"Failed to create unified dataset: {str(e)}",
                error_details=str(e)
            )


class CheckDataReadiness(SystemTool):
    """
    Check if session data is ready for analysis.
    
    This tool verifies:
    - CSV data is uploaded and processed
    - Shapefile data is uploaded and processed
    - Unified dataset exists or can be created
    - Data quality is sufficient for analysis
    """
    
    create_if_missing: bool = Field(
        True,
        description="Create unified dataset if it doesn't exist"
    )
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Check if data is ready",
            "Is my data uploaded?",
            "Can I run analysis?",
            "Check data status"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Check data readiness"""
        try:
            session_folder = f"instance/uploads/{session_id}"
            
            # Check if session folder exists
            if not os.path.exists(session_folder):
                return self._create_error_result(
                    message="No data found. Please upload your CSV and shapefile data first.",
                    data={'session_exists': False}
                )
            
            # Check for processed data files
            csv_exists = os.path.exists(os.path.join(session_folder, "processed_data.csv"))
            shapefile_exists = os.path.exists(os.path.join(session_folder, "shapefile", "processed.shp"))
            unified_exists = os.path.exists(os.path.join(session_folder, "unified_dataset.geoparquet"))
            
            readiness = {
                'csv_uploaded': csv_exists,
                'shapefile_uploaded': shapefile_exists,
                'unified_dataset_exists': unified_exists,
                'ready_for_analysis': False,
                'missing_components': []
            }
            
            # Check what's missing
            if not csv_exists:
                readiness['missing_components'].append('CSV data')
            if not shapefile_exists:
                readiness['missing_components'].append('Shapefile data')
            
            # If basic data exists but unified dataset doesn't, try to create it
            if csv_exists and shapefile_exists:
                if not unified_exists and self.create_if_missing:
                    # Try to create unified dataset
                    from ..data.unified_dataset_builder import UnifiedDatasetBuilder
                    builder = UnifiedDatasetBuilder(session_id)
                    result = builder.build_unified_dataset()
                    
                    if result['status'] == 'success':
                        readiness['unified_dataset_exists'] = True
                        readiness['unified_dataset_created'] = True
                        readiness['ready_for_analysis'] = True
                        
                        return self._create_success_result(
                            message="Data is ready! Unified dataset was created successfully.",
                            data=readiness
                        )
                elif unified_exists:
                    readiness['ready_for_analysis'] = True
                    
                    return self._create_success_result(
                        message="Data is ready for analysis!",
                        data=readiness
                    )
            
            # Generate appropriate message
            if readiness['missing_components']:
                message = f"Missing required data: {', '.join(readiness['missing_components'])}"
            else:
                message = "Data uploaded but unified dataset is missing. Set create_if_missing=True to create it."
            
            return self._create_success_result(
                message=message,
                data=readiness
            )
            
        except Exception as e:
            logger.error(f"Error checking data readiness: {e}")
            return self._create_error_result(
                message=f"Error checking data status: {str(e)}",
                error_details=str(e)
            )


# Register tools for discovery
__all__ = [
    'CreateUnifiedDataset',
    'CheckDataReadiness'
]