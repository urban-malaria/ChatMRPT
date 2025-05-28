"""
Data service for handling data operations in ChatMRPT.

This service provides a clean interface for data loading, validation,
and processing operations.
"""

import logging
import os
from typing import Dict, Any, List, Optional
from ...models.data_handler import DataHandler

logger = logging.getLogger(__name__)


class DataService:
    """
    Service for data operations including loading, validation, and processing.
    
    This service wraps the existing DataHandler and provides additional
    functionality for the modern service architecture.
    """
    
    def __init__(self, upload_folder: str, interaction_logger=None):
        """
        Initialize the data service.
        
        Args:
            upload_folder: Path to upload folder for session data
            interaction_logger: Logger for user interactions
        """
        self.upload_folder = upload_folder
        self.interaction_logger = interaction_logger
        self._handlers = {}  # Cache of session data handlers
        
    def get_handler(self, session_id: str) -> Optional[DataHandler]:
        """
        Get or create a data handler for a session.
        
        Args:
            session_id: User session ID
            
        Returns:
            DataHandler instance for the session
        """
        if session_id not in self._handlers:
            session_folder = os.path.join(self.upload_folder, session_id)
            os.makedirs(session_folder, exist_ok=True)
            self._handlers[session_id] = DataHandler(session_folder)
            
        return self._handlers[session_id]
    
    def load_csv_file(self, session_id: str, file_path: str) -> Dict[str, Any]:
        """
        Load a CSV file for a session.
        
        Args:
            session_id: User session ID
            file_path: Path to CSV file
            
        Returns:
            Result dictionary with status and metadata
        """
        try:
            handler = self.get_handler(session_id)
            result = handler.load_csv(file_path)
            
            # Log the operation
            if self.interaction_logger and result.get('status') == 'success':
                metadata = {
                    'rows': result.get('rows', 0),
                    'columns': result.get('columns', 0),
                    'missing_values': result.get('missing_values', 0)
                }
                self.interaction_logger.log_file_upload(
                    session_id, 'csv', os.path.basename(file_path),
                    os.path.getsize(file_path), metadata
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error loading CSV file: {str(e)}'
            }
    
    def load_shapefile(self, session_id: str, file_path: str) -> Dict[str, Any]:
        """
        Load a shapefile for a session.
        
        Args:
            session_id: User session ID
            file_path: Path to shapefile (ZIP)
            
        Returns:
            Result dictionary with status and metadata
        """
        try:
            handler = self.get_handler(session_id)
            result = handler.load_shapefile(file_path)
            
            # Log the operation
            if self.interaction_logger and result.get('status') in ['success', 'warning']:
                metadata = {
                    'features': result.get('features', 0),
                    'crs': result.get('crs', ''),
                    'has_mismatches': result.get('mismatches') is not None
                }
                self.interaction_logger.log_file_upload(
                    session_id, 'shapefile', os.path.basename(file_path),
                    os.path.getsize(file_path), metadata
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error loading shapefile: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error loading shapefile: {str(e)}'
            }
    
    def validate_variables(self, session_id: str, variables: List[str]) -> Dict[str, Any]:
        """
        Validate variables against loaded data.
        
        Args:
            session_id: User session ID
            variables: List of variable names to validate
            
        Returns:
            Validation result with valid/invalid variables
        """
        try:
            handler = self.get_handler(session_id)
            if not hasattr(handler, 'validate_variables'):
                return {
                    'is_valid': False,
                    'message': 'Data not loaded or validation not available'
                }
            
            return handler.validate_variables(variables)
            
        except Exception as e:
            logger.error(f"Error validating variables: {str(e)}", exc_info=True)
            return {
                'is_valid': False,
                'message': f'Error validating variables: {str(e)}'
            }
    
    def get_available_variables(self, session_id: str) -> List[str]:
        """
        Get list of available variables for a session.
        
        Args:
            session_id: User session ID
            
        Returns:
            List of available variable names
        """
        try:
            handler = self.get_handler(session_id)
            if hasattr(handler, 'get_available_variables'):
                return handler.get_available_variables()
            return []
            
        except Exception as e:
            logger.error(f"Error getting available variables: {str(e)}", exc_info=True)
            return []
    
    def check_ward_mismatches(self, session_id: str) -> Optional[List[str]]:
        """
        Check for ward name mismatches between CSV and shapefile.
        
        Args:
            session_id: User session ID
            
        Returns:
            List of mismatched ward names or None
        """
        try:
            handler = self.get_handler(session_id)
            if hasattr(handler, 'check_wardname_mismatches'):
                return handler.check_wardname_mismatches()
            return None
            
        except Exception as e:
            logger.error(f"Error checking ward mismatches: {str(e)}", exc_info=True)
            return None
    
    def get_data_summary(self, session_id: str) -> Dict[str, Any]:
        """
        Get summary information about loaded data.
        
        Args:
            session_id: User session ID
            
        Returns:
            Summary of loaded data
        """
        try:
            handler = self.get_handler(session_id)
            
            summary = {
                'csv_loaded': hasattr(handler, 'df') and handler.df is not None,
                'shapefile_loaded': hasattr(handler, 'gdf') and handler.gdf is not None,
                'variables_count': len(self.get_available_variables(session_id)),
                'csv_rows': len(handler.df) if hasattr(handler, 'df') and handler.df is not None else 0,
                'shapefile_features': len(handler.gdf) if hasattr(handler, 'gdf') and handler.gdf is not None else 0
            }
            
            # Check for analysis results
            summary['analysis_complete'] = (
                hasattr(handler, 'vulnerability_rankings') and 
                handler.vulnerability_rankings is not None
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}", exc_info=True)
            return {
                'csv_loaded': False,
                'shapefile_loaded': False,
                'variables_count': 0,
                'csv_rows': 0,
                'shapefile_features': 0,
                'analysis_complete': False
            }
    
    def cleanup_session(self, session_id: str):
        """
        Clean up data for a session.
        
        Args:
            session_id: User session ID
        """
        if session_id in self._handlers:
            del self._handlers[session_id]
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check health of the data service.
        
        Returns:
            Health status information
        """
        return {
            'status': 'healthy',
            'active_sessions': len(self._handlers),
            'upload_folder_exists': os.path.exists(self.upload_folder),
            'upload_folder_writable': os.access(self.upload_folder, os.W_OK) if os.path.exists(self.upload_folder) else False
        }

    def get_ward_information(self, session_id: str) -> Dict[str, Any]:
        """
        Get ward information from loaded data.
        
        Args:
            session_id: User session ID
            
        Returns:
            Dictionary with ward information
        """
        try:
            handler = self.get_handler(session_id)
            
            ward_names = []
            wards = []
            
            # Get ward names from CSV data if available
            if hasattr(handler, 'data') and handler.data is not None:
                if 'WardName' in handler.data.columns:
                    ward_names = handler.data['WardName'].unique().tolist()
                    
            # Get ward features from shapefile if available  
            if hasattr(handler, 'shapefile_data') and handler.shapefile_data is not None:
                if 'WardName' in handler.shapefile_data.columns:
                    wards = [
                        {
                            'name': row['WardName'],
                            'geometry': str(row.geometry) if hasattr(row, 'geometry') else None
                        }
                        for _, row in handler.shapefile_data.iterrows()
                    ]
            
            return {
                'status': 'success',
                'wards': wards,
                'ward_names': ward_names
            }
            
        except Exception as e:
            logger.error(f"Error getting ward information: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error retrieving ward information: {str(e)}'
            }

    def load_sample_data(self, session_id: str) -> Dict[str, Any]:
        """
        Load pre-packaged sample data into the user's session.
        
        Args:
            session_id: User session ID
            
        Returns:
            Result dictionary with status and metadata
        """
        try:
            import shutil
            
            # Define paths
            sample_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'sample_data')
            source_csv_path = os.path.join(sample_data_dir, 'sample_data_template.csv')
            source_zip_path = os.path.join(sample_data_dir, 'sample_boundary_template.zip')

            session_folder = os.path.join(self.upload_folder, session_id)
            os.makedirs(session_folder, exist_ok=True)

            target_csv_path = os.path.join(session_folder, 'sample_data.csv')
            target_zip_path = os.path.join(session_folder, 'sample_boundary.zip')

            # Check if sample files exist
            if not os.path.exists(source_csv_path) or not os.path.exists(source_zip_path):
                return {
                    'status': 'error',
                    'message': 'Sample data files are missing on the server.'
                }

            # Copy sample files to session folder
            shutil.copy(source_csv_path, target_csv_path)
            shutil.copy(source_zip_path, target_zip_path)
            logger.info("Sample files copied to session folder.")

            # Load the sample data using the service methods
            csv_result = self.load_csv_file(session_id, target_csv_path)
            if csv_result['status'] != 'success':
                return {
                    'status': 'error',
                    'message': f"Failed to process sample CSV: {csv_result.get('message')}"
                }

            shp_result = self.load_shapefile(session_id, target_zip_path)
            if shp_result['status'] not in ['success', 'warning']:
                return {
                    'status': 'error',
                    'message': f"Failed to process sample shapefile: {shp_result.get('message')}"
                }

            # Get variables and ward information
            variables = self.get_available_variables(session_id)
            ward_info = self.get_ward_information(session_id)
            
            return {
                'status': 'success',
                'message': 'Sample data loaded successfully',
                'csv_filename': 'sample_data.csv',
                'shapefile_filename': 'sample_boundary.zip',
                'variables': variables,
                'ward_count': len(ward_info.get('ward_names', [])),
                'rows': csv_result.get('rows', 0),
                'features': shp_result.get('features', 0)
            }
            
        except Exception as e:
            logger.error(f"Error loading sample data: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error loading sample data: {str(e)}'
            } 