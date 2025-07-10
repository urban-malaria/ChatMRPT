# app/data/utils.py
"""
Data Utilities Module - Helper Functions and Common Utilities

This module provides helper functions, file management utilities,
and common functionality used across the data package.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- File management utilities
- Data format conversion helpers
- Common validation functions
- Session management helpers
"""

import os
import tempfile
import shutil
import logging
import json
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime
from pathlib import Path

# Set up logging
from app.services.variable_resolution_service import variable_resolver
logger = logging.getLogger(__name__)


class FileManager:
    """
    Handles file operations and session management
    """
    
    def __init__(self, base_folder: str = 'sessions'):
        """
        Initialize file manager
        
        Args:
            base_folder: Base folder for session storage
        """
        self.base_folder = base_folder
        self.logger = logging.getLogger(__name__)
        
        # Ensure base folder exists
        os.makedirs(self.base_folder, exist_ok=True)
    
    def create_session_folder(self, session_id: Optional[str] = None) -> str:
        """
        Create a unique session folder
        
        Args:
            session_id: Optional session ID (auto-generated if None)
            
        Returns:
            Path to the created session folder
        """
        if session_id is None:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        session_folder = os.path.join(self.base_folder, session_id)
        os.makedirs(session_folder, exist_ok=True)
        
        self.logger.info(f"Created session folder: {session_folder}")
        return session_folder
    
    def list_session_folders(self) -> List[Dict[str, Any]]:
        """
        List all available session folders
        
        Returns:
            List of session folder information
        """
        sessions = []
        
        if not os.path.exists(self.base_folder):
            return sessions
        
        for folder_name in os.listdir(self.base_folder):
            folder_path = os.path.join(self.base_folder, folder_name)
            
            if os.path.isdir(folder_path):
                # Get folder statistics
                folder_stat = os.stat(folder_path)
                
                sessions.append({
                    'session_id': folder_name,
                    'path': folder_path,
                    'created': datetime.fromtimestamp(folder_stat.st_ctime).isoformat(),
                    'modified': datetime.fromtimestamp(folder_stat.st_mtime).isoformat(),
                    'files_count': len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
                })
        
        # Sort by creation time (newest first)
        sessions.sort(key=lambda x: x['created'], reverse=True)
        
        return sessions
    
    def cleanup_session_folder(self, session_folder: str, keep_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Clean up session folder, optionally keeping specific files
        
        Args:
            session_folder: Path to session folder
            keep_files: Optional list of files to keep
            
        Returns:
            Cleanup results
        """
        try:
            if not os.path.exists(session_folder):
                return {'status': 'error', 'message': 'Session folder does not exist'}
            
            keep_files = keep_files or []
            removed_files = []
            
            for file_name in os.listdir(session_folder):
                file_path = os.path.join(session_folder, file_name)
                
                if os.path.isfile(file_path) and file_name not in keep_files:
                    os.remove(file_path)
                    removed_files.append(file_name)
            
            return {
                'status': 'success',
                'removed_files': removed_files,
                'kept_files': keep_files
            }
            
        except Exception as e:
            self.logger.error(f"Error cleaning up session folder: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error cleaning up: {str(e)}'}
    
    def save_to_session(self, session_folder: str, filename: str, 
                       data: Union[pd.DataFrame, gpd.GeoDataFrame, dict, str],
                       format_type: str = 'auto') -> Dict[str, Any]:
        """
        Save data to session folder in appropriate format
        
        Args:
            session_folder: Session folder path
            filename: Target filename
            data: Data to save
            format_type: Format type ('csv', 'json', 'geojson', 'auto')
            
        Returns:
            Save operation result
        """
        try:
            os.makedirs(session_folder, exist_ok=True)
            file_path = os.path.join(session_folder, filename)
            
            # Auto-detect format if needed
            if format_type == 'auto':
                if isinstance(data, gpd.GeoDataFrame):
                    format_type = 'geojson'
                elif isinstance(data, pd.DataFrame):
                    format_type = 'csv'
                elif isinstance(data, (dict, list)):
                    format_type = 'json'
                else:
                    format_type = 'txt'
            
            # Save based on format
            if format_type == 'csv' and isinstance(data, pd.DataFrame):
                data.to_csv(file_path, index=False)
            elif format_type == 'geojson' and isinstance(data, gpd.GeoDataFrame):
                data.to_file(file_path, driver='GeoJSON')
            elif format_type == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            else:
                # Save as text
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(str(data))
            
            return {
                'status': 'success',
                'file_path': file_path,
                'format': format_type
            }
            
        except Exception as e:
            self.logger.error(f"Error saving to session: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error saving: {str(e)}'}


class DataConverter:
    """
    Handles data format conversions and transformations
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def dataframe_to_dict(self, df: pd.DataFrame, 
                         orient: str = 'records') -> Dict[str, Any]:
        """
        Convert DataFrame to dictionary with metadata
        
        Args:
            df: DataFrame to convert
            orient: Orientation for conversion
            
        Returns:
            Dictionary with data and metadata
        """
        try:
            return {
                'status': 'success',
                'data': df.to_dict(orient=orient),
                'metadata': {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns.tolist(),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
                }
            }
        except Exception as e:
            self.logger.error(f"Error converting DataFrame: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Conversion error: {str(e)}'}
    
    def geodataframe_to_dict(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Convert GeoDataFrame to dictionary with geographic metadata
        
        Args:
            gdf: GeoDataFrame to convert
            
        Returns:
            Dictionary with geographic data and metadata
        """
        try:
            # Convert to GeoJSON-like structure
            features = []
            for _, row in gdf.iterrows():
                properties = {k: v for k, v in row.items() if k != 'geometry'}
                
                # Handle geometry
                geom = row.get('geometry')
                if geom is not None:
                    try:
                        geometry = geom.__geo_interface__
                    except:
                        geometry = None
                else:
                    geometry = None
                
                features.append({
                    'type': 'Feature',
                    'properties': properties,
                    'geometry': geometry
                })
            
            return {
                'status': 'success',
                'data': {
                    'type': 'FeatureCollection',
                    'features': features
                },
                'metadata': {
                    'features': len(gdf),
                    'crs': str(gdf.crs) if gdf.crs else None,
                    'columns': gdf.columns.tolist(),
                    'bounds': gdf.total_bounds.tolist() if hasattr(gdf, 'total_bounds') else None
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error converting GeoDataFrame: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Conversion error: {str(e)}'}


class ValidationHelper:
    """
    Provides common validation functions
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_file_path(self, file_path: str, 
                          allowed_extensions: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Validate file path and extension
        
        Args:
            file_path: Path to validate
            allowed_extensions: List of allowed extensions (e.g., ['.csv', '.xlsx'])
            
        Returns:
            Validation result
        """
        try:
            if not file_path:
                return {'is_valid': False, 'message': 'File path is empty'}
            
            if not os.path.exists(file_path):
                return {'is_valid': False, 'message': 'File does not exist'}
            
            if not os.path.isfile(file_path):
                return {'is_valid': False, 'message': 'Path is not a file'}
            
            # Check file extension if specified
            if allowed_extensions:
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext not in [ext.lower() for ext in allowed_extensions]:
                    return {
                        'is_valid': False, 
                        'message': f'File extension must be one of: {allowed_extensions}'
                    }
            
            # Check file size (limit to 100MB)
            file_size = os.path.getsize(file_path)
            max_size = 100 * 1024 * 1024  # 100MB
            if file_size > max_size:
                return {
                    'is_valid': False,
                    'message': f'File too large ({file_size / 1024 / 1024:.1f}MB). Maximum size is 100MB.'
                }
            
            return {
                'is_valid': True,
                'message': 'File is valid',
                'file_size': file_size,
                'extension': os.path.splitext(file_path)[1]
            }
            
        except Exception as e:
            self.logger.error(f"Error validating file path: {str(e)}", exc_info=True)
            return {'is_valid': False, 'message': f'Validation error: {str(e)}'}
    
    def validate_dataframe_columns(self, df: pd.DataFrame, 
                                  required_columns: Optional[List[str]] = None,
                                  optional_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Validate DataFrame column structure
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            optional_columns: List of optional column names
            
        Returns:
            Validation result
        """
        try:
            if df is None or df.empty:
                return {'is_valid': False, 'message': 'DataFrame is empty or None'}
            
            validation_result = {
                'is_valid': True,
                'message': 'DataFrame structure is valid',
                'found_columns': df.columns.tolist(),
                'missing_required': [],
                'missing_optional': [],
                'extra_columns': []
            }
            
            # Check required columns
            if required_columns:
                missing_required = [col for col in required_columns if col not in df.columns]
                if missing_required:
                    validation_result['is_valid'] = False
                    validation_result['message'] = f'Missing required columns: {missing_required}'
                    validation_result['missing_required'] = missing_required
            
            # Check optional columns
            if optional_columns:
                missing_optional = [col for col in optional_columns if col not in df.columns]
                validation_result['missing_optional'] = missing_optional
            
            # Identify extra columns
            expected_columns = (required_columns or []) + (optional_columns or [])
            if expected_columns:
                extra_columns = [col for col in df.columns if col not in expected_columns]
                validation_result['extra_columns'] = extra_columns
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating DataFrame columns: {str(e)}", exc_info=True)
            return {'is_valid': False, 'message': f'Validation error: {str(e)}'}


class SessionMetadata:
    """
    Handles session metadata management
    """
    
    def __init__(self, session_folder: str):
        """
        Initialize session metadata manager
        
        Args:
            session_folder: Path to session folder
        """
        self.session_folder = session_folder
        self.metadata_file = os.path.join(session_folder, 'session_metadata.json')
        self.logger = logging.getLogger(__name__)
        
        # Ensure session folder exists
        os.makedirs(session_folder, exist_ok=True)
    
    def save_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Save session metadata
        
        Args:
            metadata: Metadata dictionary to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add timestamp
            metadata['last_updated'] = datetime.now().isoformat()
            
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving metadata: {str(e)}", exc_info=True)
            return False
    
    def load_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Load session metadata
        
        Returns:
            Metadata dictionary or None if not found
        """
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading metadata: {str(e)}", exc_info=True)
            return None
    
    def update_metadata(self, updates: Dict[str, Any]) -> bool:
        """
        Update existing metadata with new values
        
        Args:
            updates: Dictionary of updates to apply
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load existing metadata
            existing = self.load_metadata() or {}
            
            # Apply updates
            existing.update(updates)
            
            # Save updated metadata
            return self.save_metadata(existing)
            
        except Exception as e:
            self.logger.error(f"Error updating metadata: {str(e)}", exc_info=True)
            return False


# Convenience functions
def create_temp_directory() -> str:
    """
    Create a temporary directory for processing
    
    Returns:
        Path to temporary directory
    """
    return tempfile.mkdtemp()


def cleanup_temp_directory(temp_dir: str) -> bool:
    """
    Clean up temporary directory
    
    Args:
        temp_dir: Path to temporary directory
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return True
    except Exception as e:
        logger.error(f"Error cleaning up temp directory: {str(e)}", exc_info=True)
        return False


def safe_filename(filename: str) -> str:
    """
    Create a safe filename by removing/replacing problematic characters
    
    Args:
        filename: Original filename
        
    Returns:
        Safe filename
    """
    # Replace problematic characters
    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    safe_name = ''.join(c if c in safe_chars else '_' for c in filename)
    
    # Ensure it doesn't start with a dot or dash
    if safe_name.startswith(('.', '-')):
        safe_name = 'file_' + safe_name
    
    # Ensure it's not empty
    if not safe_name or safe_name.isspace():
        safe_name = 'unnamed_file'
    
    return safe_name


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get comprehensive file information
    
    Args:
        file_path: Path to file
        
    Returns:
        File information dictionary
    """
    try:
        if not os.path.exists(file_path):
            return {'exists': False, 'error': 'File does not exist'}
        
        stat = os.stat(file_path)
        
        return {
            'exists': True,
            'path': file_path,
            'name': os.path.basename(file_path),
            'extension': os.path.splitext(file_path)[1],
            'size_bytes': stat.st_size,
            'size_formatted': format_file_size(stat.st_size),
            'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'is_file': os.path.isfile(file_path),
            'is_directory': os.path.isdir(file_path)
        }
        
    except Exception as e:
        logger.error(f"Error getting file info: {str(e)}", exc_info=True)
        return {'exists': False, 'error': str(e)}


# Package information
__version__ = "1.0.0"
__all__ = [
    'FileManager',
    'DataConverter',
    'ValidationHelper',
    'SessionMetadata',
    'create_temp_directory',
    'cleanup_temp_directory',
    'safe_filename',
    'format_file_size',
    'get_file_info'
] 