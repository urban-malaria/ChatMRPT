"""
Data Package - Modular Data Handling System

This is the main interface for the data package, providing backward compatibility
with the original DataHandler class while implementing a modular architecture.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Main Components:
- DataHandler: Backward-compatible main interface  
- Individual modules: loaders, validation, processing, analysis, reporting, utils
- Enhanced functionality with professional architecture
"""

import os
import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import geopandas as gpd

# Import all modules
from .loaders import CSVLoader, ShapefileLoader, load_csv_file, load_shapefile_zip
from .validation import DataValidator, check_ward_mismatches, validate_variable_list, run_quality_assessment
from .processing import DataProcessor, clean_dataset, normalize_dataset, calculate_composite_scores, analyze_urban_thresholds
from .analysis import AnalysisCoordinator, run_complete_analysis
from .reporting import ReportGenerator, generate_result_summary
from .utils import (
    FileManager, DataConverter, ValidationHelper, SessionMetadata,
    create_temp_directory, cleanup_temp_directory, safe_filename, 
    format_file_size, get_file_info
)

# Set up logging
logger = logging.getLogger(__name__)


class DataHandler:
    """
    Main DataHandler class providing backward compatibility with enhanced modular architecture
    
    This class maintains the original interface while leveraging the new modular components
    for improved maintainability, testing, and functionality.
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize DataHandler with modular architecture
        
        Args:
            session_folder: Path to session folder for data storage
            interaction_logger: Optional interaction logger for tracking operations
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize modular components
        self.coordinator = AnalysisCoordinator(session_folder, interaction_logger)
        self.csv_loader = CSVLoader(session_folder, interaction_logger)
        self.shapefile_loader = ShapefileLoader(session_folder, interaction_logger)
        self.validator = DataValidator(interaction_logger)
        self.processor = DataProcessor(session_folder, interaction_logger)
        self.reporter = ReportGenerator(session_folder, interaction_logger)
        self.file_manager = FileManager()
        self.data_converter = DataConverter()
        self.validation_helper = ValidationHelper()
        self.session_metadata = SessionMetadata(session_folder)
        
        # Data storage - maintain original structure for compatibility
        self.csv_data = None
        self.shapefile_data = None
        self.cleaned_data = None
        self.normalized_data = None
        self.composite_scores = None
        self.vulnerability_rankings = None
        self.urban_extent_results = None
        self.variable_relationships = {}
        self.composite_variables = []
        self.na_handling_methods = []
        
        # Ensure session folder exists
        os.makedirs(session_folder, exist_ok=True)
        
        self.logger.info(f"DataHandler initialized with modular architecture - Session: {session_folder}")
    
    # ===========================================
    # ORIGINAL INTERFACE METHODS (Backward Compatibility)
    # ===========================================
    
    def load_csv_file(self, file_path: str) -> Dict[str, Any]:
        """Load CSV or Excel file - Original interface method"""
        result = self.csv_loader.load_file(file_path)
        if result['status'] == 'success':
            self.csv_data = result['data']
        return result
    
    def load_csv(self, file_path: str) -> Dict[str, Any]:
        """Load CSV or Excel file - Backward compatibility alias"""
        return self.load_csv_file(file_path)
    
    def load_shapefile_from_zip(self, zip_file_path: str) -> Dict[str, Any]:
        """Load shapefile from ZIP - Original interface method"""
        result = self.shapefile_loader.load_shapefile(zip_file_path)
        if result['status'] == 'success':
            self.shapefile_data = result['data']
        return result
    
    def load_shapefile(self, zip_file_path: str) -> Dict[str, Any]:
        """Load shapefile from ZIP - Backward compatibility alias"""
        return self.load_shapefile_from_zip(zip_file_path)
    
    def check_wardname_mismatches(self) -> Optional[List[str]]:
        """Check ward name mismatches - Original interface method"""
        return self.validator.check_wardname_mismatches(self.csv_data, self.shapefile_data)
    
    def get_available_variables(self) -> List[str]:
        """Get available variables - Original interface method"""
        if self.csv_data is None:
            return []
        return self.validator.get_available_variables(self.csv_data)
    
    def validate_variables(self, variables: List[str]) -> Dict[str, Any]:
        """Validate variables - Original interface method"""
        if self.csv_data is None:
            return {'is_valid': False, 'message': 'No CSV data loaded'}
        
        available_vars = self.validator.get_available_variables(self.csv_data)
        return self.validator.validate_variables(variables, available_vars)
    
    def run_full_analysis(self, selected_variables: Optional[List[str]] = None,
                         na_methods: Optional[Dict[str, str]] = None,
                         custom_relationships: Optional[Dict[str, str]] = None,
                         llm_manager=None) -> Dict[str, Any]:
        """Run full analysis pipeline - Original interface method"""
        # Ensure data is loaded in coordinator
        self.coordinator.csv_data = self.csv_data
        self.coordinator.shapefile_data = self.shapefile_data
        
        # Run analysis
        result = self.coordinator.run_full_analysis(
            selected_variables, na_methods, custom_relationships, llm_manager
        )
        
        # Update local state for backward compatibility
        if result['status'] == 'success' and 'results' in result:
            analysis_results = result['results']
            self.cleaned_data = analysis_results.get('cleaned_data')
            self.normalized_data = analysis_results.get('normalized_data')
            self.composite_scores = analysis_results.get('composite_scores')
            self.vulnerability_rankings = analysis_results.get('vulnerability_rankings')
            self.variable_relationships = analysis_results.get('variable_relationships', {})
            self.composite_variables = analysis_results.get('composite_variables', [])
        
        return result
    
    def generate_context_for_analysis(self, analysis_type: str = 'vulnerability') -> Dict[str, Any]:
        """Generate analysis context - Original interface method"""
        if self.csv_data is None:
            return {'status': 'error', 'message': 'No CSV data loaded'}
        
        return self.reporter.generate_context_for_analysis(
            self.csv_data, analysis_type, self.shapefile_data
        )
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get data summary - Original interface method"""
        return self.coordinator.get_data_summary()
    
    def has_geo_data(self) -> bool:
        """Check if geographic data is available - Original interface method"""
        return self.coordinator.has_geo_data()
    
    # ===========================================
    # ENHANCED METHODS (New Functionality)
    # ===========================================
    
    def run_data_quality_assessment(self) -> Dict[str, Any]:
        """
        Run comprehensive data quality assessment
        
        Returns:
            Quality assessment results with recommendations
        """
        if self.csv_data is None:
            return {'status': 'error', 'message': 'No CSV data loaded'}
        
        return self.validator.run_data_quality_checks(self.csv_data, self.shapefile_data)
    
    def clean_data(self, na_methods: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Clean data using specified methods
        
        Args:
            na_methods: Dictionary mapping columns to cleaning methods
            
        Returns:
            Cleaning results and cleaned data
        """
        if self.csv_data is None:
            return {'status': 'error', 'message': 'No CSV data loaded'}
        
        result = self.processor.clean_data(self.csv_data, self.shapefile_data, na_methods)
        if result['status'] == 'success':
            self.cleaned_data = result['data']
            self.na_handling_methods = result.get('na_handling_methods', [])
        
        return result
    
    def normalize_data(self, relationships: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Normalize cleaned data
        
        Args:
            relationships: Variable relationships (direct/inverse)
            
        Returns:
            Normalization results and normalized data
        """
        if self.cleaned_data is None:
            return {'status': 'error', 'message': 'No cleaned data available. Run clean_data() first.'}
        
        result = self.processor.normalize_data(self.cleaned_data, relationships)
        if result['status'] == 'success':
            self.normalized_data = result['data']
            self.variable_relationships = result.get('relationships', {})
        
        return result
    
    def compute_composite_scores(self, selected_variables: Optional[List[str]] = None,
                               method: str = 'mean') -> Dict[str, Any]:
        """
        Compute composite vulnerability scores
        
        Args:
            selected_variables: Variables to include in scoring
            method: Scoring method ('mean', 'weighted', etc.)
            
        Returns:
            Composite scoring results
        """
        if self.normalized_data is None:
            return {'status': 'error', 'message': 'No normalized data available. Run normalize_data() first.'}
        
        result = self.processor.compute_composite_scores(
            self.normalized_data, selected_variables, method
        )
        if result['status'] == 'success':
            self.composite_scores = result['scores']
            self.composite_variables = result.get('composite_variables', [])
        
        return result
    
    def calculate_vulnerability_rankings(self, n_categories: int = 3) -> Dict[str, Any]:
        """
        Calculate vulnerability rankings from composite scores
        
        Args:
            n_categories: Number of vulnerability categories
            
        Returns:
            Vulnerability ranking results
        """
        if self.composite_scores is None:
            return {'status': 'error', 'message': 'No composite scores available. Run compute_composite_scores() first.'}
        
        result = self.processor.calculate_vulnerability_rankings(self.composite_scores, n_categories)
        if result['status'] == 'success':
            self.vulnerability_rankings = result['rankings']
        
        return result
    
    def analyze_urban_extent(self, thresholds: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        Analyze urban extent at different thresholds
        
        Args:
            thresholds: List of thresholds to analyze
            
        Returns:
            Urban extent analysis results
        """
        if self.csv_data is None:
            return {'status': 'error', 'message': 'No CSV data loaded'}
        
        result = self.processor.process_urban_extent(self.csv_data, self.shapefile_data, thresholds)
        if result['status'] == 'success':
            self.urban_extent_results = result['results']
        
        return result
    
    def generate_analysis_summary(self) -> Dict[str, Any]:
        """
        Generate comprehensive analysis summary
        
        Returns:
            Analysis summary with recommendations
        """
        # Prepare analysis results
        analysis_results = {
            'cleaned_data': self.cleaned_data,
            'normalized_data': self.normalized_data,
            'composite_scores': self.composite_scores,
            'vulnerability_rankings': self.vulnerability_rankings,
            'composite_variables': self.composite_variables,
            'variable_relationships': self.variable_relationships,
            'na_handling_methods': self.na_handling_methods,
            'urban_extent_results': self.urban_extent_results
        }
        
        return self.reporter.generate_analysis_summary(
            analysis_results, self.csv_data, self.shapefile_data
        )
    
    def format_vulnerability_results(self, top_n: int = 10) -> Dict[str, Any]:
        """
        Format vulnerability results for presentation
        
        Args:
            top_n: Number of top/bottom wards to highlight
            
        Returns:
            Formatted vulnerability results
        """
        if self.vulnerability_rankings is None:
            return {'status': 'error', 'message': 'No vulnerability rankings available'}
        
        return self.reporter.format_vulnerability_results(
            self.vulnerability_rankings, self.composite_variables, top_n
        )
    
    def export_analysis_report(self, format_type: str = 'comprehensive') -> Dict[str, Any]:
        """
        Export comprehensive analysis report
        
        Args:
            format_type: Report format ('comprehensive', 'summary', 'technical')
            
        Returns:
            Export results with file paths
        """
        # Prepare analysis results
        analysis_results = {
            'cleaned_data': self.cleaned_data,
            'normalized_data': self.normalized_data,
            'composite_scores': self.composite_scores,
            'vulnerability_rankings': self.vulnerability_rankings,
            'composite_variables': self.composite_variables,
            'variable_relationships': self.variable_relationships,
            'na_handling_methods': self.na_handling_methods,
            'urban_extent_results': self.urban_extent_results
        }
        
        return self.reporter.export_analysis_report(analysis_results, format_type)
    
    def save_session_state(self) -> bool:
        """
        Save current session state to metadata
        
        Returns:
            True if successful, False otherwise
        """
        try:
            metadata = {
                'session_folder': self.session_folder,
                'csv_data_loaded': self.csv_data is not None,
                'shapefile_data_loaded': self.shapefile_data is not None,
                'analysis_complete': self.vulnerability_rankings is not None,
                'composite_variables': self.composite_variables,
                'variable_relationships': self.variable_relationships,
                'na_handling_methods': self.na_handling_methods
            }
            
            return self.session_metadata.save_metadata(metadata)
            
        except Exception as e:
            self.logger.error(f"Error saving session state: {str(e)}", exc_info=True)
            return False
    
    def load_session_state(self) -> bool:
        """
        Load session state from metadata
        
        Returns:
            True if successful, False otherwise
        """
        try:
            metadata = self.session_metadata.load_metadata()
            if metadata is None:
                return False
            
            # Restore state from metadata
            self.composite_variables = metadata.get('composite_variables', [])
            self.variable_relationships = metadata.get('variable_relationships', {})
            self.na_handling_methods = metadata.get('na_handling_methods', [])
            
            # Try to reload data files if they exist
            self._attempt_data_reload()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading session state: {str(e)}", exc_info=True)
            return False
    
    def get_package_info(self) -> Dict[str, Any]:
        """
        Get comprehensive package information
        
        Returns:
            Package information and statistics
        """
        return {
            'package_name': 'app.data',
            'version': __version__,
            'description': 'Modular data handling system for vulnerability analysis',
            'session_folder': self.session_folder,
            'components': {
                'loaders': 'File loading (CSV, Excel, Shapefile)',
                'validation': 'Data quality checks and variable validation',
                'processing': 'Data cleaning, normalization, and scoring',
                'analysis': 'Pipeline coordination and orchestration',
                'reporting': 'Result summarization and export',
                'utils': 'Utilities and helper functions'
            },
            'data_state': {
                'csv_loaded': self.csv_data is not None,
                'shapefile_loaded': self.shapefile_data is not None,
                'cleaned': self.cleaned_data is not None,
                'normalized': self.normalized_data is not None,
                'scored': self.composite_scores is not None,
                'ranked': self.vulnerability_rankings is not None
            },
            'available_methods': len([method for method in dir(self) if not method.startswith('_')])
        }
    
    def validate_package_integrity(self) -> Dict[str, Any]:
        """
        Validate package integrity and component availability
        
        Returns:
            Validation results for all components
        """
        validation_results = {
            'overall_status': 'success',
            'components': {},
            'issues': []
        }
        
        try:
            # Test each component
            components = {
                'csv_loader': self.csv_loader,
                'shapefile_loader': self.shapefile_loader,
                'validator': self.validator,
                'processor': self.processor,
                'coordinator': self.coordinator,
                'reporter': self.reporter,
                'file_manager': self.file_manager,
                'data_converter': self.data_converter,
                'validation_helper': self.validation_helper,
                'session_metadata': self.session_metadata
            }
            
            for name, component in components.items():
                try:
                    # Basic validation - check if component is accessible
                    if component is not None and hasattr(component, '__class__'):
                        validation_results['components'][name] = {
                            'status': 'available',
                            'class': component.__class__.__name__
                        }
                    else:
                        validation_results['components'][name] = {
                            'status': 'unavailable',
                            'issue': 'Component not initialized'
                        }
                        validation_results['issues'].append(f'{name} component not available')
                        
                except Exception as e:
                    validation_results['components'][name] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    validation_results['issues'].append(f'{name} component error: {str(e)}')
            
            # Check session folder
            if not os.path.exists(self.session_folder):
                validation_results['issues'].append('Session folder does not exist')
            
            # Overall status
            if validation_results['issues']:
                validation_results['overall_status'] = 'issues_found'
            
            return validation_results
            
        except Exception as e:
            return {
                'overall_status': 'error',
                'message': f'Package validation failed: {str(e)}',
                'issues': [str(e)]
            }
    
    def _attempt_data_reload(self):
        """Attempt to reload data from session files"""
        try:
            # Try to reload CSV data
            csv_path = os.path.join(self.session_folder, 'processed_data.csv')
            if os.path.exists(csv_path):
                self.csv_data = pd.read_csv(csv_path)
            
            # Try to reload other data files
            files_to_reload = [
                ('cleaned_data.csv', 'cleaned_data'),
                ('normalized_data.csv', 'normalized_data'),
                ('vulnerability_rankings.csv', 'vulnerability_rankings')
            ]
            
            for filename, attr_name in files_to_reload:
                file_path = os.path.join(self.session_folder, filename)
                if os.path.exists(file_path):
                    setattr(self, attr_name, pd.read_csv(file_path))
            
        except Exception as e:
            self.logger.warning(f"Could not reload some data files: {str(e)}")


# Package-level convenience functions for direct import
def create_data_handler(session_folder: str, interaction_logger=None) -> DataHandler:
    """
    Convenience function to create a DataHandler instance
    
    Args:
        session_folder: Session folder path
        interaction_logger: Optional interaction logger
        
    Returns:
        Configured DataHandler instance
    """
    return DataHandler(session_folder, interaction_logger)


def validate_data_file(file_path: str, file_type: str = 'auto') -> Dict[str, Any]:
    """
    Convenience function to validate a data file
    
    Args:
        file_path: Path to file to validate
        file_type: File type ('csv', 'excel', 'shapefile', 'auto')
        
    Returns:
        Validation results
    """
    validator = ValidationHelper()
    
    if file_type == 'auto':
        # Auto-detect based on extension
        ext = os.path.splitext(file_path)[1].lower()
        if ext in ['.csv']:
            allowed_exts = ['.csv']
        elif ext in ['.xlsx', '.xls']:
            allowed_exts = ['.xlsx', '.xls']
        elif ext in ['.zip']:
            allowed_exts = ['.zip']
        else:
            allowed_exts = None
    elif file_type == 'csv':
        allowed_exts = ['.csv']
    elif file_type == 'excel':
        allowed_exts = ['.xlsx', '.xls']
    elif file_type == 'shapefile':
        allowed_exts = ['.zip']
    else:
        allowed_exts = None
    
    return validator.validate_file_path(file_path, allowed_exts)


# Package metadata and exports
__version__ = "1.0.0"
__author__ = "ChatMRPT Development Team"
__description__ = "Modular data handling system for vulnerability analysis"

# Main exports - maintain backward compatibility while exposing new functionality
__all__ = [
    # Main class
    'DataHandler',
    
    # Module classes
    'CSVLoader',
    'ShapefileLoader', 
    'DataValidator',
    'DataProcessor',
    'AnalysisCoordinator',
    'ReportGenerator',
    'FileManager',
    'DataConverter',
    'ValidationHelper',
    'SessionMetadata',
    
    # Convenience functions
    'create_data_handler',
    'validate_data_file',
    'load_csv_file',
    'load_shapefile_zip',
    'check_ward_mismatches',
    'validate_variable_list',
    'run_quality_assessment',
    'clean_dataset',
    'normalize_dataset',
    'calculate_composite_scores',
    'analyze_urban_thresholds',
    'run_complete_analysis',
    'generate_result_summary',
    'create_temp_directory',
    'cleanup_temp_directory',
    'safe_filename',
    'format_file_size',
    'get_file_info',
    
    # Package metadata
    '__version__',
    '__author__',
    '__description__'
] 