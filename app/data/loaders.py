"""
Data Loading Module - File Loading and Initial Processing

This module handles loading various file formats including CSV, Excel, and Shapefiles.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- CSVLoader: CSV and Excel file loading
- ShapefileLoader: Shapefile loading from ZIP archives
- File validation and initial processing
"""

import os
import zipfile
import tempfile
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Dict, Any, Optional

# Set up logging
logger = logging.getLogger(__name__)


class CSVLoader:
    """
    Handles loading and initial processing of CSV and Excel files
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize CSV loader
        
        Args:
            session_folder: Path to session folder for saving files
            interaction_logger: Optional interaction logger
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Ensure session folder exists
        os.makedirs(self.session_folder, exist_ok=True)
    
    def load_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load and process CSV or Excel file
        
        Args:
            file_path: Path to the CSV or Excel file
            
        Returns:
            dict: Status and information about the loaded data
        """
        try:
            # Check file extension to determine loading method
            file_extension = os.path.splitext(file_path)[1].lower()
            
            if file_extension in ['.xlsx', '.xls']:
                # Load Excel file
                data = pd.read_excel(file_path)
                self.logger.info(f"Loaded Excel file: {file_path}")
            else:
                # Load CSV file with robust parameters
                data = pd.read_csv(
                    file_path,
                    na_values=['NA', '', 'N/A'],
                    keep_default_na=True
                )
                self.logger.info(f"Loaded CSV file: {file_path}")
            
            # Process the loaded data
            processed_data = self._process_csv_data(data)
            
            # Save processed CSV locally
            output_path = os.path.join(self.session_folder, 'processed_data.csv')
            processed_data.to_csv(output_path, index=False)
            
            # Log the operation if interaction logger is available
            if self.interaction_logger:
                try:
                    from app.analysis import check_data_quality, AnalysisMetadata
                    
                    # Initialize analysis metadata
                    session_id = os.path.basename(self.session_folder)
                    analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
                    
                    # Run data quality check
                    quality_issues = check_data_quality(processed_data, analysis_metadata)
                    
                    # Include severe quality issues in response
                    severe_issues = []
                    if 'severe_issues' in quality_issues:
                        for issue in quality_issues['severe_issues']:
                            severe_issues.append({
                                'type': issue['type'],
                                'column': issue.get('column', 'N/A'),
                                'message': issue.get('recommendation', 'Review this data')
                            })
                except ImportError:
                    severe_issues = None
                    self.logger.warning("Analysis module not available for quality checks")
            else:
                severe_issues = None
            
            return {
                'status': 'success',
                'message': f'File loaded successfully with {len(processed_data)} rows and {len(processed_data.columns)} columns',
                'data': processed_data,
                'rows': len(processed_data),
                'columns': len(processed_data.columns),
                'missing_values': self._count_missing_values(processed_data),
                'quality_issues': severe_issues,
                'file_path': output_path
            }
            
        except Exception as e:
            self.logger.error(f"Error loading file {file_path}: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error loading file: {str(e)}',
                'data': None
            }
    
    def _process_csv_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process loaded CSV data with standardization
        
        Args:
            data: Raw CSV data
            
        Returns:
            Processed DataFrame
        """
        # Ensure column names are valid
        data.columns = data.columns.str.strip()
        
        # Ensure WardName column exists - rename if needed
        if 'Ward' in data.columns and 'WardName' not in data.columns:
            data = data.rename(columns={'Ward': 'WardName'})
            self.logger.info("Renamed 'Ward' column to 'WardName'")
        
        # Handle duplicate ward names if WardCode exists
        if 'WardName' in data.columns and 'WardCode' in data.columns:
            data = self._handle_duplicate_wardnames(data)
        
        return data
    
    def _handle_duplicate_wardnames(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle duplicate ward names by appending ward codes
        
        Args:
            df: DataFrame with potential duplicate ward names
            
        Returns:
            DataFrame with unique ward names
        """
        if 'WardName' not in df.columns or 'WardCode' not in df.columns:
            return df
        
        # Check for duplicates
        duplicates = df['WardName'].duplicated(keep=False)
        
        if duplicates.any():
            self.logger.info(f"Found {duplicates.sum()} duplicate ward names, appending ward codes")
            
            # For duplicate ward names, append the ward code
            df.loc[duplicates, 'WardName'] = (
                df.loc[duplicates, 'WardName'].astype(str) + 
                ' (' + df.loc[duplicates, 'WardCode'].astype(str) + ')'
            )
        
        return df
    
    def _count_missing_values(self, df: pd.DataFrame) -> int:
        """
        Count total missing values in DataFrame
        
        Args:
            df: DataFrame to check
            
        Returns:
            Total number of missing values
        """
        return df.isna().sum().sum()


class ShapefileLoader:
    """
    Handles loading and processing of shapefiles from ZIP archives
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize shapefile loader
        
        Args:
            session_folder: Path to session folder for saving files
            interaction_logger: Optional interaction logger
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Ensure session folder exists
        os.makedirs(self.session_folder, exist_ok=True)
    
    def load_shapefile(self, zip_file_path: str) -> Dict[str, Any]:
        """
        Extract and load shapefile from ZIP
        
        Args:
            zip_file_path: Path to the ZIP file containing shapefile
            
        Returns:
            dict: Status and information about the loaded shapefile
        """
        try:
            # Create a temporary directory to extract the ZIP
            with tempfile.TemporaryDirectory() as temp_dir:
                # Extract the ZIP file
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                    self.logger.info(f"Extracted ZIP file to temporary directory")
                
                # Find shapefile(s) in the extracted directory
                shp_files = []
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.shp'):
                            shp_files.append(os.path.join(root, file))
                
                if not shp_files:
                    return {
                        'status': 'error',
                        'message': 'No shapefile (.shp) found in the ZIP file',
                        'data': None
                    }
                
                # Load the first shapefile found
                shapefile_data = gpd.read_file(shp_files[0])
                self.logger.info(f"Loaded shapefile with CRS: {shapefile_data.crs}")
                
                # Process the shapefile data
                processed_data = self._process_shapefile_data(shapefile_data)
                
                # Save shapefile locally for future use
                shp_output_dir = os.path.join(self.session_folder, 'shapefile')
                os.makedirs(shp_output_dir, exist_ok=True)
                output_path = os.path.join(shp_output_dir, 'processed.shp')
                processed_data.to_file(output_path)
                
                return {
                    'status': 'success',
                    'message': f'Shapefile loaded successfully with {len(processed_data)} features',
                    'data': processed_data,
                    'features': len(processed_data),
                    'crs': str(processed_data.crs),
                    'file_path': output_path
                }
        
        except Exception as e:
            self.logger.error(f"Error loading shapefile {zip_file_path}: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error loading shapefile: {str(e)}',
                'data': None
            }
    
    def _process_shapefile_data(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Process loaded shapefile data with standardization
        
        Args:
            data: Raw shapefile data
            
        Returns:
            Processed GeoDataFrame
        """
        # Ensure WardName column exists
        if 'WardName' not in data.columns:
            # Look for potential ward name columns
            potential_columns = [col for col in data.columns if 
                               any(name in col.lower() for name in ['ward', 'name', 'area'])]
            
            if potential_columns:
                # Use the first potential column
                data = data.rename(columns={potential_columns[0]: 'WardName'})
                self.logger.info(f"Renamed '{potential_columns[0]}' column to 'WardName'")
            else:
                # Create sequential ward names if no suitable column found
                data['WardName'] = [f'Ward_{i+1}' for i in range(len(data))]
                self.logger.info("Created sequential WardName column")
        
        # Handle duplicate ward names if WardCode exists
        if 'WardCode' in data.columns:
            data = self._handle_duplicate_wardnames(data)
        
        # Ensure UrbanPercent column exists (for urban extent analysis)
        if 'UrbanPercent' not in data.columns:
            # Create random UrbanPercent values for demonstration
            np.random.seed(42)  # For reproducible demo data
            data['UrbanPercent'] = np.random.uniform(10, 90, len(data))
            self.logger.info("Created demo UrbanPercent column with random values")
        
        # Standardize the CRS to WGS84 (EPSG:4326)
        if data.crs and data.crs != "EPSG:4326":
            self.logger.info(f"Converting shapefile from {data.crs} to EPSG:4326")
            try:
                data = data.to_crs(epsg=4326)
                self.logger.info("CRS conversion successful")
            except Exception as crs_error:
                self.logger.warning(f"CRS conversion error: {str(crs_error)}. Using original CRS.")
        
        return data
    
    def _handle_duplicate_wardnames(self, df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Handle duplicate ward names by appending ward codes
        
        Args:
            df: GeoDataFrame with potential duplicate ward names
            
        Returns:
            GeoDataFrame with unique ward names
        """
        if 'WardName' not in df.columns or 'WardCode' not in df.columns:
            return df
        
        # Check for duplicates
        duplicates = df['WardName'].duplicated(keep=False)
        
        if duplicates.any():
            self.logger.info(f"Found {duplicates.sum()} duplicate ward names, appending ward codes")
            
            # For duplicate ward names, append the ward code
            df.loc[duplicates, 'WardName'] = (
                df.loc[duplicates, 'WardName'].astype(str) + 
                ' (' + df.loc[duplicates, 'WardCode'].astype(str) + ')'
            )
        
        return df


# Convenience functions for backward compatibility
def load_csv_file(session_folder: str, file_path: str, interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to load CSV file
    
    Args:
        session_folder: Session folder path
        file_path: Path to CSV file
        interaction_logger: Optional interaction logger
        
    Returns:
        Loading result dictionary
    """
    loader = CSVLoader(session_folder, interaction_logger)
    return loader.load_file(file_path)


def load_shapefile_zip(session_folder: str, zip_path: str, interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to load shapefile from ZIP
    
    Args:
        session_folder: Session folder path
        zip_path: Path to ZIP file containing shapefile
        interaction_logger: Optional interaction logger
        
    Returns:
        Loading result dictionary
    """
    loader = ShapefileLoader(session_folder, interaction_logger)
    return loader.load_shapefile(zip_path)


# Package information
__version__ = "1.0.0"
__all__ = [
    'CSVLoader',
    'ShapefileLoader', 
    'load_csv_file',
    'load_shapefile_zip'
] 