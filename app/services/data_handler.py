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
from .unified_dataset_builder import UnifiedDatasetBuilder, build_unified_dataset
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
        self._csv_data = None
        self._shapefile_data = None
        self.cleaned_data = None
        self.normalized_data = None
        self._data_loaded = False  # Track if we've attempted to load data
        
        # Dual method composite scoring system
        self.composite_scores_mean = None      # Mean method results
        self.composite_scores_pca = None       # PCA method results
        
        # Dual method vulnerability rankings
        self.vulnerability_rankings = None     # Mean method rankings (default)
        self.vulnerability_rankings_pca = None # PCA method rankings
        
        # Urban extent analysis for both methods
        self.urban_extent_results = None       # Mean method urban analysis
        self.urban_extent_results_pca = None   # PCA method urban analysis
        
        # Analysis configuration and metadata
        self.variable_relationships = None
        self.composite_variables = None
        self.variable_selection_method = None
        self.variable_selection_explanations = {}
        self.na_handling_methods = {}
        
        # Current viewing method ('mean' or 'pca')
        self.current_method = 'mean'  # Default to mean method for backwards compatibility
        
        # Unified dataset for post-analysis operations
        self.unified_dataset = None
        
        # Don't auto-create session folder - only create when data is actually uploaded
        # This prevents confusion about whether data exists based on directory existence
        
        # Try to reload any existing data from the session folder
        self._attempt_data_reload()
        
        self.logger.info(f"DataHandler initialized with modular architecture - Session: {session_folder}")
    
    # ===========================================
    # BACKWARD COMPATIBILITY PROPERTIES
    # ===========================================
    
    @property
    def df(self):
        """Backward compatibility property for accessing CSV data as DataFrame"""
        return self.csv_data
    
    @df.setter
    def df(self, value):
        """Backward compatibility setter for CSV data"""
        self.csv_data = value
    
    @property
    def csv_data(self):
        """Property for accessing CSV data - returns cleaned data if available"""
        # 🔧 PHASE 1 FIX: Always prioritize cleaned data for analysis
        if self.cleaned_data is not None:
            return self.cleaned_data
        return self._csv_data
    
    @csv_data.setter
    def csv_data(self, value):
        """Property setter for CSV data"""
        self._csv_data = value
        if value is not None:
            self._data_loaded = True
    
    @property
    def raw_csv_data(self):
        """Property for accessing RAW CSV data (bypasses cleaned data priority)"""
        return self._csv_data
    
    @property
    def shapefile_data(self):
        """Property for accessing shapefile data"""
        return self._shapefile_data
    
    @shapefile_data.setter
    def shapefile_data(self, value):
        """Property setter for shapefile data"""
        self._shapefile_data = value
    
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
    
    def fix_wardname_mismatches(self, csv_data: pd.DataFrame) -> pd.DataFrame:
        """Fix ward name mismatches using fuzzy matching"""
        if self.shapefile_data is None:
            return csv_data
        
        from difflib import get_close_matches
        
        csv_wards = set(csv_data['WardName'].unique()) if 'WardName' in csv_data.columns else set()
        shp_wards = set(self.shapefile_data['WardName'].unique()) if 'WardName' in self.shapefile_data.columns else set()
        
        # Find mismatches
        csv_only = csv_wards - shp_wards
        
        if not csv_only:
            return csv_data
        
        # Create mapping for fixes
        ward_mapping = {}
        
        for ward in csv_only:
            # Find closest match in shapefile
            matches = get_close_matches(ward, shp_wards, n=1, cutoff=0.8)
            if matches:
                ward_mapping[ward] = matches[0]
                self.logger.info(f"Mapping '{ward}' -> '{matches[0]}'")
        
        # Apply mapping
        if ward_mapping:
            csv_data = csv_data.copy()
            csv_data['WardName'] = csv_data['WardName'].replace(ward_mapping)
            self.logger.info(f"Fixed {len(ward_mapping)} ward name mismatches")
        
        return csv_data
    
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
            self.composite_scores_mean = analysis_results.get('composite_scores_mean')
            self.composite_scores_pca = analysis_results.get('composite_scores_pca')
            self.vulnerability_rankings = analysis_results.get('vulnerability_rankings')
            self.vulnerability_rankings_pca = analysis_results.get('vulnerability_rankings_pca')
            self.variable_relationships = analysis_results.get('variable_relationships', {})
            self.composite_variables = analysis_results.get('composite_variables', [])
            
            # **CRITICAL FIX: Set backward compatibility attribute for visualization**
            # The visualization code expects 'composite_scores' (not 'composite_scores_mean')
            if self.composite_scores_mean is not None:
                self.composite_scores = self.composite_scores_mean
        
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
            method: Scoring method ('mean', 'pca')
                - 'mean': Simple average (default, fast)
                - 'pca': Principal Component Analysis (advanced, with feature importance)
            
        Returns:
            Composite scoring results
        """
        if self.normalized_data is None:
            return {'status': 'error', 'message': 'No normalized data available. Run normalize_data() first.'}
        
        result = self.processor.compute_composite_scores(
            self.normalized_data, selected_variables, method
        )
        if result['status'] == 'success':
            if method == 'mean':
                self.composite_scores_mean = result['scores']
            else:
                self.composite_scores_pca = result['scores']
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
        if self.composite_scores_mean is None and self.composite_scores_pca is None:
            return {'status': 'error', 'message': 'No composite scores available. Run compute_composite_scores() first.'}
        
        if self.composite_scores_mean is not None:
            result = self.processor.calculate_vulnerability_rankings(self.composite_scores_mean, n_categories)
            if result['status'] == 'success':
                self.vulnerability_rankings = result['rankings']
        if self.composite_scores_pca is not None:
            result = self.processor.calculate_vulnerability_rankings(self.composite_scores_pca, n_categories)
            if result['status'] == 'success':
                self.vulnerability_rankings_pca = result['rankings']
        
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
            self.urban_extent_results_pca = result['results_pca']
        
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
            'composite_scores_mean': self.composite_scores_mean,
            'composite_scores_pca': self.composite_scores_pca,
            'vulnerability_rankings': self.vulnerability_rankings,
            'vulnerability_rankings_pca': self.vulnerability_rankings_pca,
            'composite_variables': self.composite_variables,
            'variable_relationships': self.variable_relationships,
            'na_handling_methods': self.na_handling_methods,
            'urban_extent_results': self.urban_extent_results,
            'urban_extent_results_pca': self.urban_extent_results_pca
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
        if self.vulnerability_rankings is None and self.vulnerability_rankings_pca is None:
            return {'status': 'error', 'message': 'No vulnerability rankings available'}
        
        if self.vulnerability_rankings is not None:
            mean_results = self.reporter.format_vulnerability_results(
                self.vulnerability_rankings, self.composite_variables, top_n
            )
        else:
            mean_results = {'status': 'error', 'message': 'No mean method rankings available'}
        
        if self.vulnerability_rankings_pca is not None:
            pca_results = self.reporter.format_vulnerability_results(
                self.vulnerability_rankings_pca, self.composite_variables, top_n
            )
        else:
            pca_results = {'status': 'error', 'message': 'No PCA method rankings available'}
        
        return {
            'mean_method': mean_results,
            'pca_method': pca_results
        }
    
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
            'composite_scores_mean': self.composite_scores_mean,
            'composite_scores_pca': self.composite_scores_pca,
            'vulnerability_rankings': self.vulnerability_rankings,
            'vulnerability_rankings_pca': self.vulnerability_rankings_pca,
            'composite_variables': self.composite_variables,
            'variable_relationships': self.variable_relationships,
            'na_handling_methods': self.na_handling_methods,
            'urban_extent_results': self.urban_extent_results,
            'urban_extent_results_pca': self.urban_extent_results_pca
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
                'scored': self.composite_scores_mean is not None or self.composite_scores_pca is not None,
                'ranked': self.vulnerability_rankings is not None or self.vulnerability_rankings_pca is not None
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
        """Attempt to reload data from session files - PRIORITIZING CLEANED DATA"""
        try:
            # 🔧 PHASE 1 FIX: Check for cleaned data first (analysis-ready data)
            cleaned_paths = [
                'analysis_cleaned_data.csv',  # Analysis-specific cleaned data
                'cleaned_data.csv'            # General cleaned data
            ]
            
            cleaned_data_loaded = False
            for cleaned_path in cleaned_paths:
                full_path = os.path.join(self.session_folder, cleaned_path)
                if os.path.exists(full_path):
                    try:
                        self.csv_data = pd.read_csv(full_path)
                        self.cleaned_data = self.csv_data.copy()  # Store as both csv_data and cleaned_data
                        cleaned_data_loaded = True
                        self.logger.info(f"✅ PRIORITIZED: Loaded cleaned data from {cleaned_path} (analysis-ready)")
                        break
                    except Exception as e:
                        self.logger.warning(f"Could not load cleaned data from {cleaned_path}: {e}")
            
            # Only load raw data if no cleaned data exists
            if not cleaned_data_loaded:
                raw_csv_path = os.path.join(self.session_folder, 'raw_data.csv')
                if os.path.exists(raw_csv_path):
                    # Load raw data (will need cleaning)
                    self.csv_data = pd.read_csv(raw_csv_path)
                    self.logger.info("⚠️ FALLBACK: Loaded raw data from raw_data.csv (will need cleaning)")
                else:
                    # Fallback to processed data for backward compatibility
                    processed_csv_path = os.path.join(self.session_folder, 'processed_data.csv')
                    if os.path.exists(processed_csv_path):
                        self.csv_data = pd.read_csv(processed_csv_path)
                        self.logger.info("⚠️ LEGACY: Loaded CSV data from processed_data.csv")
                    else:
                        # Final fallback to any CSV file
                        original_csv_files = [f for f in os.listdir(self.session_folder) if f.endswith('.csv')]
                        if original_csv_files:
                            csv_path = os.path.join(self.session_folder, original_csv_files[0])
                            self.csv_data = pd.read_csv(csv_path)
                            self.logger.info(f"⚠️ ULTIMATE FALLBACK: Loaded CSV data from {original_csv_files[0]}")
            
            # Try to reload shapefile data - prioritize RAW shapefile for new workflow
            shapefile_folder = os.path.join(self.session_folder, 'shapefile')
            if os.path.exists(shapefile_folder):
                # Look for raw shapefile first
                raw_shp_path = os.path.join(shapefile_folder, 'raw.shp')
                if os.path.exists(raw_shp_path):
                    import geopandas as gpd
                    self.shapefile_data = gpd.read_file(raw_shp_path)
                    self.logger.info("Reloaded shapefile data from raw.shp (new workflow)")
                else:
                    # Fallback to any .shp file in folder
                    shp_files = [f for f in os.listdir(shapefile_folder) if f.endswith('.shp')]
                    if shp_files:
                        import geopandas as gpd
                        shp_path = os.path.join(shapefile_folder, shp_files[0])
                        self.shapefile_data = gpd.read_file(shp_path)
                        self.logger.info(f"Reloaded shapefile data from {shp_files[0]} (fallback)")
            
            # If no shapefile found but raw_shapefile.zip exists, extract it automatically
            if self.shapefile_data is None:
                raw_shapefile_zip = os.path.join(self.session_folder, 'raw_shapefile.zip')
                if os.path.exists(raw_shapefile_zip):
                    try:
                        self.logger.info("🔧 Auto-extracting shapefile from raw_shapefile.zip")
                        result = self.load_shapefile_from_zip(raw_shapefile_zip)
                        if result['status'] == 'success':
                            self.logger.info("✅ Auto-extraction successful - shapefile data loaded")
                        else:
                            self.logger.warning(f"❌ Auto-extraction failed: {result.get('message', 'Unknown error')}")
                    except Exception as e:
                        self.logger.warning(f"❌ Auto-extraction failed with exception: {e}")
            
            # **FIXED: Also reload composite_scores and analysis results**
            # Try to reload other data files
            files_to_reload = [
                ('cleaned_data.csv', 'cleaned_data'),
                ('normalized_data.csv', 'normalized_data'),
                ('vulnerability_rankings.csv', 'vulnerability_rankings'),
                ('vulnerability_rankings_pca.csv', 'vulnerability_rankings_pca'),
                ('urban_extent_results.csv', 'urban_extent_results'),
                ('urban_extent_results_pca.csv', 'urban_extent_results_pca')
            ]
            
            for filename, attribute_name in files_to_reload:
                file_path = os.path.join(self.session_folder, filename)
                if os.path.exists(file_path):
                    try:
                        data = pd.read_csv(file_path)
                        setattr(self, attribute_name, data)
                        self.logger.info(f"Reloaded {attribute_name} from {filename}")
                    except Exception as e:
                        self.logger.warning(f"Could not reload {attribute_name} from {filename}: {e}")

            # **CRITICAL FIX: Reload analysis-specific files**
            analysis_files_to_reload = [
                ('analysis_cleaned_data.csv', 'cleaned_data'),
                ('analysis_normalized_data.csv', 'normalized_data'),
                ('analysis_vulnerability_rankings.csv', 'vulnerability_rankings'),
                ('analysis_vulnerability_rankings_pca.csv', 'vulnerability_rankings_pca')
            ]
            
            for filename, attribute_name in analysis_files_to_reload:
                file_path = os.path.join(self.session_folder, filename)
                if os.path.exists(file_path):
                    try:
                        data = pd.read_csv(file_path)
                        setattr(self, attribute_name, data)
                        self.logger.info(f"Reloaded {attribute_name} from {filename}")
                    except Exception as e:
                        self.logger.warning(f"Could not reload {attribute_name} from {filename}: {e}")

            # **CRITICAL FIX: Reload composite_scores as dict from CSV files**
            # Try to reload composite_scores from saved CSV files
            composite_scores_files = [
                ('composite_scores.csv', 'model_formulas.csv', 'composite_scores_mean'),  # From processing module
                ('analysis_composite_scores.csv', 'model_formulas.csv', 'composite_scores')  # From analysis module
            ]
            
            for scores_file, formulas_file, attribute_name in composite_scores_files:
                scores_path = os.path.join(self.session_folder, scores_file)
                formulas_path = os.path.join(self.session_folder, formulas_file)
                
                if os.path.exists(scores_path):
                    try:
                        # Load the scores DataFrame
                        scores_df = pd.read_csv(scores_path)
                        
                        # Load the formulas if available
                        model_formulas = []
                        if os.path.exists(formulas_path):
                            formulas_df = pd.read_csv(formulas_path)
                            for _, row in formulas_df.iterrows():
                                # Handle variables column that might be comma-separated string
                                variables = row['variables']
                                if isinstance(variables, str):
                                    variables = variables.split(',')
                                elif pd.isna(variables):
                                    variables = []
                                
                                model_formulas.append({
                                    'model': row['model'],
                                    'variables': variables,
                                    'formula': row.get('formula', f"Mean of: {variables}")
                                })
                        else:
                            # Generate basic formulas from model columns
                            model_columns = [col for col in scores_df.columns if col.startswith('model_')]
                            for model_col in model_columns:
                                model_formulas.append({
                                    'model': model_col,
                                    'variables': [],
                                    'formula': f"Composite score {model_col}"
                                })
                        
                        # Reconstruct the composite_scores dict
                        composite_scores_dict = {
                            'scores': scores_df,
                            'formulas': model_formulas,
                            'model_formulas': model_formulas,  # Backward compatibility
                            'method': 'mean'  # Default method
                        }
                        
                        # Set the appropriate attribute
                        setattr(self, attribute_name, composite_scores_dict)
                        self.logger.info(f"Reloaded {attribute_name} from {scores_file}")
                        
                    except Exception as e:
                        self.logger.warning(f"Could not reload {attribute_name} from {scores_file}: {e}")
            
            # **CRITICAL FIX: Set backward compatibility attributes**
            # The visualization code expects composite_scores, not composite_scores_mean
            if hasattr(self, 'composite_scores_mean') and self.composite_scores_mean is not None:
                if not hasattr(self, 'composite_scores') or self.composite_scores is None:
                    self.composite_scores = self.composite_scores_mean
                    self.logger.info("Set composite_scores from composite_scores_mean for backward compatibility")
                    
            # Also ensure composite_scores_mean is set if we have composite_scores
            if hasattr(self, 'composite_scores') and self.composite_scores is not None:
                if not hasattr(self, 'composite_scores_mean') or self.composite_scores_mean is None:
                    self.composite_scores_mean = self.composite_scores
                    self.logger.info("Set composite_scores_mean from composite_scores for consistency")
            
            # **NEW: Load unified dataset if available**
            self._load_unified_dataset()
            
        except Exception as e:
            self.logger.warning(f"Could not reload some data files: {str(e)}")
    
    def _load_unified_dataset(self, require_geometry=False):
        """Attempt to load unified dataset from disk"""
        try:
            session_id = os.path.basename(self.session_folder)
            from .unified_dataset_builder import load_unified_dataset
            
            self.logger.info(f"Attempting to load unified dataset for session {session_id} (require_geometry={require_geometry})")
            unified_dataset = load_unified_dataset(session_id, require_geometry=require_geometry)
            
            if unified_dataset is not None:
                self.unified_dataset = unified_dataset
                self.logger.info(f"✅ Successfully loaded unified dataset: {len(unified_dataset)} rows, {len(unified_dataset.columns)} columns")
                # Force a sync to ensure it's really there
                import time
                time.sleep(0.1)
                return True
            else:
                self.logger.warning("load_unified_dataset returned None")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load unified dataset: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def get_unified_dataset(self, require_geometry=False):
        """Get unified dataset, creating it if necessary
        
        Args:
            require_geometry: If True, ensures the dataset includes geometry column for mapping
        """
        # Return existing unified dataset if available and has geometry when required
        if self.unified_dataset is not None:
            if not require_geometry or 'geometry' in self.unified_dataset.columns:
                return self.unified_dataset
            else:
                # Need to reload with geometry
                self.logger.info("Reloading unified dataset with geometry requirement")
        
        # Try to load from disk first
        self._load_unified_dataset(require_geometry=require_geometry)
        if self.unified_dataset is not None:
            return self.unified_dataset
        
        # If still not available, try to create it using the base tool function
        try:
            from ..tools.base import get_session_unified_dataset
            session_id = os.path.basename(self.session_folder)
            
            unified_dataset = get_session_unified_dataset(session_id, require_geometry=require_geometry)
            if unified_dataset is not None:
                self.unified_dataset = unified_dataset
                self.logger.info(f"Created and loaded unified dataset with {len(unified_dataset)} rows, {len(unified_dataset.columns)} columns")
                return self.unified_dataset
            else:
                self.logger.warning("Could not create unified dataset - no data available")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating unified dataset: {e}")
            return None
    
    def set_viewing_method(self, method: str) -> Dict[str, Any]:
        """
        Set the method for viewing results ('mean' or 'pca')
        
        Args:
            method: Method to use for viewing ('mean' or 'pca')
            
        Returns:
            Dict with status and current method info
        """
        if method not in ['mean', 'pca']:
            return {
                'status': 'error',
                'message': f"Invalid method '{method}'. Must be 'mean' or 'pca'."
            }
        
        self.current_method = method
        
        # Check if the selected method has results available
        method_available = False
        if method == 'mean':
            method_available = self.composite_scores_mean is not None and self.vulnerability_rankings is not None
        else:
            method_available = self.composite_scores_pca is not None and self.vulnerability_rankings_pca is not None
        
        return {
            'status': 'success',
            'current_method': self.current_method,
            'method_available': method_available,
            'message': f'Viewing method set to {method.upper()}{"" if method_available else " (results not available)"}'
        }
    
    def switch_analysis_method(self, method: str) -> bool:
        """
        Switch analysis method (backward compatibility alias for set_viewing_method)
        
        Args:
            method: Method to switch to ('mean' or 'pca')
            
        Returns:
            True if successful, False otherwise
        """
        result = self.set_viewing_method(method)
        return result['status'] == 'success'
    
    def get_current_composite_scores(self) -> Optional[Dict[str, Any]]:
        """
        Get composite scores for the currently selected viewing method
        
        Returns:
            Composite scores dict or None if not available
        """
        if self.current_method == 'mean':
            return self.composite_scores_mean
        else:
            return self.composite_scores_pca
    
    def get_current_vulnerability_rankings(self) -> Optional[Any]:
        """
        Get vulnerability rankings for the currently selected viewing method
        
        Returns:
            Vulnerability rankings DataFrame or None if not available
        """
        if self.current_method == 'mean':
            return self.vulnerability_rankings
        else:
            return self.vulnerability_rankings_pca
    
    def get_current_urban_extent_results(self) -> Optional[Dict[str, Any]]:
        """
        Get urban extent results for the currently selected viewing method
        
        Returns:
            Urban extent results dict or None if not available
        """
        if self.current_method == 'mean':
            return self.urban_extent_results
        else:
            return self.urban_extent_results_pca
    
    def get_method_comparison_summary(self) -> Dict[str, Any]:
        """
        Get a comparison summary of both methods' results with practical focus on top priorities
        
        Returns:
            Dict with comparison information focused on actionable priorities
        """
        summary = {
            'methods_available': [],
            'current_method': self.current_method,
            'mean_method': {
                'available': False,
                'composite_scores': None,
                'vulnerability_rankings': None,
                'top_vulnerable_wards': [],
                'bottom_vulnerable_wards': [],
                'immediate_action_count': 0,
                'top_risk_ward': 'N/A',
                'top_risk_score': 'N/A'
            },
            'pca_method': {
                'available': False,
                'composite_scores': None,
                'vulnerability_rankings': None,
                'top_vulnerable_wards': [],
                'bottom_vulnerable_wards': [],
                'immediate_action_count': 0,
                'top_risk_ward': 'N/A',
                'top_risk_score': 'N/A'
            }
        }
        
        # Check mean method availability
        if self.composite_scores_mean is not None and self.vulnerability_rankings is not None:
            summary['methods_available'].append('mean')
            summary['mean_method']['available'] = True
            summary['mean_method']['composite_scores'] = 'Available'
            summary['mean_method']['vulnerability_rankings'] = 'Available'
            
            # Get practical priority information for mean method
            if self.vulnerability_rankings is not None:
                # Top 5 most vulnerable wards (keep 5 for summary consistency)
                top_wards_df = self.vulnerability_rankings.sort_values('overall_rank').head(5)
                top_wards = []
                for _, row in top_wards_df.iterrows():
                    top_wards.append({
                        'ward_name': row['WardName'],
                        'rank': int(row['overall_rank']),
                        'score': round(float(row['median_score']), 3)
                    })
                summary['mean_method']['top_vulnerable_wards'] = top_wards
                
                # Bottom 5 least vulnerable wards (keep 5 for summary consistency)
                bottom_wards_df = self.vulnerability_rankings.sort_values('overall_rank').tail(5)
                bottom_wards = []
                for _, row in bottom_wards_df.iterrows():
                    bottom_wards.append({
                        'ward_name': row['WardName'],
                        'rank': int(row['overall_rank']),
                        'score': round(float(row['median_score']), 3)
                    })
                summary['mean_method']['bottom_vulnerable_wards'] = bottom_wards
                
                # High risk count (practical categorization)
                high_risk = self.vulnerability_rankings[
                    self.vulnerability_rankings['vulnerability_category'] == 'High Risk'
                ]
                summary['mean_method']['immediate_action_count'] = len(high_risk)
                
                # Get top risk ward with score
                top_ward_data = self.vulnerability_rankings.sort_values('overall_rank').iloc[0]
                summary['mean_method']['top_risk_ward'] = top_ward_data['WardName']
                summary['mean_method']['top_risk_score'] = top_ward_data['median_score']
        
        # Check PCA method availability
        if self.composite_scores_pca is not None and self.vulnerability_rankings_pca is not None:
            summary['methods_available'].append('pca')
            summary['pca_method']['available'] = True
            summary['pca_method']['composite_scores'] = 'Available'
            summary['pca_method']['vulnerability_rankings'] = 'Available'
            
            # Get practical priority information for PCA method
            if self.vulnerability_rankings_pca is not None:
                # Top 5 most vulnerable wards (keep 5 for summary consistency)
                top_wards_df = self.vulnerability_rankings_pca.sort_values('overall_rank').head(5)
                top_wards = []
                for _, row in top_wards_df.iterrows():
                    top_wards.append({
                        'ward_name': row['WardName'],
                        'rank': int(row['overall_rank']),
                        'score': round(float(row['median_score']), 3)
                    })
                summary['pca_method']['top_vulnerable_wards'] = top_wards
                
                # Bottom 5 least vulnerable wards (keep 5 for summary consistency)
                bottom_wards_df = self.vulnerability_rankings_pca.sort_values('overall_rank').tail(5)
                bottom_wards = []
                for _, row in bottom_wards_df.iterrows():
                    bottom_wards.append({
                        'ward_name': row['WardName'],
                        'rank': int(row['overall_rank']),
                        'score': round(float(row['median_score']), 3)
                    })
                summary['pca_method']['bottom_vulnerable_wards'] = bottom_wards
                
                # High risk count (practical categorization)
                high_risk = self.vulnerability_rankings_pca[
                    self.vulnerability_rankings_pca['vulnerability_category'] == 'High Risk'
                ]
                summary['pca_method']['immediate_action_count'] = len(high_risk)
                
                # Get top risk ward with score
                top_ward_data = self.vulnerability_rankings_pca.sort_values('overall_rank').iloc[0]
                summary['pca_method']['top_risk_ward'] = top_ward_data['WardName']
                summary['pca_method']['top_risk_score'] = top_ward_data['median_score']
        
        return summary
    
    def validate_data_consistency(self) -> Dict[str, Any]:
        """
        🔧 PHASE 1 FIX: Validate data consistency for debugging
        
        Returns:
            Dict with validation results and debugging info
        """
        validation = {
            'has_raw_data': self._csv_data is not None,
            'has_cleaned_data': self.cleaned_data is not None,
            'data_source_used': 'unknown',
            'ward_count_raw': 0,
            'ward_count_cleaned': 0,
            'ward_name_consistency': False,
            'issues': []
        }
        
        try:
            # Check what data source is actually being used
            if self.cleaned_data is not None:
                validation['data_source_used'] = 'cleaned_data'
                validation['ward_count_cleaned'] = len(self.cleaned_data)
                
                # Check if csv_data property returns cleaned data
                if self.csv_data is self.cleaned_data:
                    validation['csv_data_properly_redirected'] = True
                else:
                    validation['csv_data_properly_redirected'] = False
                    validation['issues'].append('csv_data property not returning cleaned_data')
            elif self._csv_data is not None:
                validation['data_source_used'] = 'raw_data'
                validation['ward_count_raw'] = len(self._csv_data)
                validation['issues'].append('Using raw data - cleaning needed')
            else:
                validation['data_source_used'] = 'no_data'
                validation['issues'].append('No data loaded')
            
            # Check ward name patterns if data exists
            current_data = self.csv_data
            if current_data is not None and 'WardName' in current_data.columns:
                ward_names = current_data['WardName'].tolist()
                coded_wards = [name for name in ward_names if '(' in str(name) and ')' in str(name)]
                validation['coded_wards_count'] = len(coded_wards)
                validation['total_wards'] = len(ward_names)
                validation['duplicates_handled'] = len(coded_wards) > 0
                
                if len(coded_wards) > 0:
                    validation['sample_coded_wards'] = coded_wards[:3]
                    validation['issues'].append(f'{len(coded_wards)} wards have coded names (duplicates handled)')
            
            self.logger.info(f"Data consistency validation: {validation['data_source_used']} with {validation.get('total_wards', 0)} wards")
            
        except Exception as e:
            validation['issues'].append(f'Validation error: {str(e)}')
            
        return validation


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