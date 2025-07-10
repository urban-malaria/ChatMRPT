"""
Session Data Service for ChatMRPT
================================

Provides session-aware data handling that can properly load and manage
data for specific sessions.
"""

import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from . import DataHandler

logger = logging.getLogger(__name__)


class SessionDataService:
    """
    Session-aware data service that manages DataHandler instances per session.
    
    This service creates and manages DataHandler instances for each session,
    ensuring proper data isolation and access.
    """
    
    def __init__(self, base_upload_folder: str):
        """
        Initialize the session data service.
        
        Args:
            base_upload_folder: Base folder where session data is stored
        """
        self.base_upload_folder = base_upload_folder
        self.session_handlers: Dict[str, DataHandler] = {}
        
    def get_handler(self, session_id: str) -> Optional[DataHandler]:
        """
        Get or create a DataHandler for the specified session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            DataHandler instance for the session, or None if no data found
        """
        try:
            # Check if we already have a handler for this session
            if session_id in self.session_handlers:
                return self.session_handlers[session_id]
            
            # Create session folder path
            session_folder = os.path.join(self.base_upload_folder, session_id)
            
            # Check if session folder exists
            if not os.path.exists(session_folder):
                logger.warning(f"Session folder does not exist: {session_folder}")
                return None
            
            # Check if data files exist
            csv_path = os.path.join(session_folder, 'raw_data.csv')
            shp_path = os.path.join(session_folder, 'raw_shapefile.zip')
            
            if not os.path.exists(csv_path):
                logger.warning(f"No CSV data found for session {session_id}")
                return None
            
            # Create DataHandler for this session
            handler = DataHandler(
                session_folder=session_folder,
                interaction_logger=None  # Will be set by tools if needed
            )
            
            # Cache the handler
            self.session_handlers[session_id] = handler
            
            logger.info(f"Created DataHandler for session {session_id}")
            return handler
            
        except Exception as e:
            logger.error(f"Error getting handler for session {session_id}: {e}")
            return None
    
    def get_data_handler(self, session_id: str):
        """Alias for get_handler for backward compatibility."""
        return self.get_handler(session_id)
    
    def clear_session_data(self, session_id: str) -> bool:
        """
        Clear data for a specific session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove from cache
            if session_id in self.session_handlers:
                del self.session_handlers[session_id]
            
            # Session folder cleanup would go here if needed
            # For now, we just clear the cache
            
            logger.info(f"Cleared session data for {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing session data for {session_id}: {e}")
            return False
    
    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status information for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Status information dictionary or None
        """
        try:
            session_folder = os.path.join(self.base_upload_folder, session_id)
            
            if not os.path.exists(session_folder):
                return None
            
            # Check file existence
            csv_path = os.path.join(session_folder, 'raw_data.csv')
            shp_path = os.path.join(session_folder, 'raw_shapefile.zip')
            
            status = {
                'session_id': session_id,
                'session_folder': session_folder,
                'csv_loaded': os.path.exists(csv_path),
                'shapefile_loaded': os.path.exists(shp_path),
                'data_loaded': os.path.exists(csv_path) and os.path.exists(shp_path)
            }
            
            # Add file info if available
            if status['csv_loaded']:
                import pandas as pd
                try:
                    df = pd.read_csv(csv_path)
                    status['csv_info'] = {
                        'filename': 'raw_data.csv',
                        'rows': len(df),
                        'columns': len(df.columns)
                    }
                except Exception as e:
                    logger.warning(f"Could not read CSV info: {e}")
            
            if status['shapefile_loaded']:
                status['shapefile_info'] = {
                    'filename': 'raw_shapefile.zip',
                    'size': os.path.getsize(shp_path)
                }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting session status for {session_id}: {e}")
            return None
    
    def get_available_variables(self, session_id: str) -> list:
        """
        Get available variables for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of available variable names
        """
        try:
            handler = self.get_handler(session_id)
            if handler:
                return handler.get_available_variables()
            return []
            
        except Exception as e:
            logger.error(f"Error getting available variables for {session_id}: {e}")
            return []
    
    def has_data(self, session_id: str) -> bool:
        """
        Check if session has data available.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if data is available, False otherwise
        """
        try:
            handler = self.get_handler(session_id)
            return handler is not None
            
        except Exception as e:
            logger.error(f"Error checking data availability for {session_id}: {e}")
            return False