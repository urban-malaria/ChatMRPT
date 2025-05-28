"""
Analysis Coordination Module - Pipeline Management and Orchestration

This module handles the coordination of the full analysis pipeline,
orchestrating data loading, validation, processing, and result generation.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- AnalysisCoordinator: Main pipeline orchestration
- Full analysis pipeline management
- Variable selection and relationship management
- Analysis result coordination
"""

import os
import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List, Optional, Union

# Import other data modules
from .loaders import CSVLoader, ShapefileLoader
from .validation import DataValidator
from .processing import DataProcessor

# Set up logging
logger = logging.getLogger(__name__)


class AnalysisCoordinator:
    """
    Coordinates the full analysis pipeline from data loading to results
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize analysis coordinator
        
        Args:
            session_folder: Path to session folder for storing files
            interaction_logger: Optional interaction logger
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize component modules
        self.csv_loader = CSVLoader(session_folder, interaction_logger)
        self.shapefile_loader = ShapefileLoader(session_folder, interaction_logger)
        self.validator = DataValidator(interaction_logger)
        self.processor = DataProcessor(session_folder, interaction_logger)
        
        # Data storage
        self.csv_data = None
        self.shapefile_data = None
        self.cleaned_data = None
        self.normalized_data = None
        self.composite_scores = None
        self.vulnerability_rankings = None
        self.variable_relationships = {}
        self.composite_variables = None
        self.urban_extent_results = None
        self.na_handling_methods = []
        self.analysis_metadata = None
        
        # Ensure session folder exists
        os.makedirs(self.session_folder, exist_ok=True)
    
    def run_full_analysis(self, selected_variables: Optional[List[str]] = None,
                         na_methods: Optional[Dict[str, str]] = None,
                         custom_relationships: Optional[Dict[str, str]] = None,
                         llm_manager=None) -> Dict[str, Any]:
        """
        Run the complete analysis pipeline from loaded data to vulnerability rankings
        
        Args:
            selected_variables: Variables to include in analysis (None for auto-selection)
            na_methods: Custom methods for handling missing values
            custom_relationships: Custom variable relationships (direct/inverse)
            llm_manager: Optional LLM manager for explanations
            
        Returns:
            dict: Comprehensive analysis results
        """
        try:
            # Check if data is loaded
            if self.csv_data is None:
                return {
                    'status': 'error',
                    'message': 'No CSV data loaded. Please load data first.'
                }
            
            # Import analysis functions
            try:
                from app.analysis import run_full_analysis_pipeline, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata
            session_id = os.path.basename(self.session_folder)
            self.analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Run the full analysis pipeline
            analysis_results = run_full_analysis_pipeline(
                self,
                selected_variables,
                na_methods,
                custom_relationships,
                self.analysis_metadata,
                session_id,
                self.interaction_logger,
                llm_manager  # Pass the LLM manager for variable selection
            )
            
            # Store results
            self.cleaned_data = analysis_results.get('cleaned_data')
            self.normalized_data = analysis_results.get('normalized_data')
            self.composite_scores = analysis_results.get('composite_scores')
            self.vulnerability_rankings = analysis_results.get('vulnerability_rankings')
            self.variable_relationships = analysis_results.get('variable_relationships', {})
            self.composite_variables = analysis_results.get('composite_variables', [])
            
            # Save analysis results
            self._save_analysis_results(analysis_results)
            
            # Log success
            if self.interaction_logger:
                try:
                    self.interaction_logger.log_analysis_event(
                        session_id,
                        'full_analysis_complete',
                        {
                            'variables_used': len(self.composite_variables),
                            'rankings_generated': len(self.vulnerability_rankings) if self.vulnerability_rankings is not None else 0
                        }
                    )
                except Exception as log_error:
                    self.logger.warning(f"Failed to log analysis event: {log_error}")
            
            return {
                'status': 'success',
                'message': 'Full analysis completed successfully',
                'results': analysis_results,
                'variables_used': self.composite_variables,
                'rankings_count': len(self.vulnerability_rankings) if self.vulnerability_rankings is not None else 0
            }
            
        except Exception as e:
            self.logger.error(f"Error in full analysis: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error in full analysis: {str(e)}'
            }
    
    def explain_variable_selection(self, llm_manager=None) -> Dict[str, Any]:
        """
        Generate explanation for variable selection process
        
        Args:
            llm_manager: Optional LLM manager for generating explanations
            
        Returns:
            dict: Variable selection explanation
        """
        try:
            if self.csv_data is None:
                return {
                    'status': 'error',
                    'message': 'No data loaded for variable explanation'
                }
            
            # Get available variables
            available_vars = self.validator.get_available_variables(self.csv_data)
            
            # Basic explanation without LLM
            explanation = {
                'status': 'success',
                'total_variables_in_data': len(self.csv_data.columns),
                'available_for_analysis': len(available_vars),
                'excluded_variables': [],
                'inclusion_criteria': [
                    'Must be numeric data type',
                    'Cannot be WardName or identifier columns',
                    'Must have sufficient non-missing values'
                ]
            }
            
            # Identify excluded variables and reasons
            for col in self.csv_data.columns:
                if col not in available_vars:
                    reason = self._get_exclusion_reason(col)
                    explanation['excluded_variables'].append({
                        'variable': col,
                        'reason': reason
                    })
            
            # If LLM manager is available, enhance explanation
            if llm_manager and hasattr(llm_manager, 'generate_explanation'):
                try:
                    # Prepare context for LLM
                    context = {
                        'available_variables': available_vars,
                        'excluded_variables': explanation['excluded_variables'],
                        'data_summary': self.get_data_summary()
                    }
                    
                    # Generate enhanced explanation
                    llm_explanation = llm_manager.generate_explanation(
                        'variable_selection',
                        context,
                        'detailed'
                    )
                    
                    explanation['llm_explanation'] = llm_explanation
                    
                except Exception as llm_error:
                    self.logger.warning(f"LLM explanation failed: {llm_error}")
                    explanation['llm_explanation'] = None
            
            return explanation
            
        except Exception as e:
            self.logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error explaining variable selection: {str(e)}'
            }
    
    def load_csv_data(self, file_path: str) -> Dict[str, Any]:
        """
        Load CSV data using the CSV loader
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Loading result dictionary
        """
        result = self.csv_loader.load_file(file_path)
        if result['status'] == 'success':
            self.csv_data = result['data']
        return result
    
    def load_shapefile_data(self, zip_path: str) -> Dict[str, Any]:
        """
        Load shapefile data using the shapefile loader
        
        Args:
            zip_path: Path to ZIP file containing shapefile
            
        Returns:
            Loading result dictionary
        """
        result = self.shapefile_loader.load_shapefile(zip_path)
        if result['status'] == 'success':
            self.shapefile_data = result['data']
        return result
    
    def validate_analysis_variables(self, variables: List[str]) -> Dict[str, Any]:
        """
        Validate variables for analysis using the validator
        
        Args:
            variables: List of variables to validate
            
        Returns:
            Validation result dictionary
        """
        if self.csv_data is None:
            return {
                'status': 'error',
                'message': 'No CSV data loaded for validation'
            }
        
        available_vars = self.validator.get_available_variables(self.csv_data)
        return self.validator.validate_variables(variables, available_vars)
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive summary of all loaded data and analysis state
        
        Returns:
            Dictionary with complete data summary
        """
        summary = {
            'csv_loaded': self.csv_data is not None,
            'shapefile_loaded': self.shapefile_data is not None,
            'analysis_complete': (hasattr(self, 'vulnerability_rankings') and 
                                self.vulnerability_rankings is not None)
        }
        
        # Add CSV data summary if available
        if self.csv_data is not None:
            available_vars = self.validator.get_available_variables(self.csv_data)
            summary.update({
                'csv_rows': len(self.csv_data),
                'csv_columns': len(self.csv_data.columns),
                'available_variables': available_vars[:10],  # First 10 for brevity
                'variable_count': len(available_vars)
            })
        
        # Add shapefile summary if available
        if self.shapefile_data is not None:
            summary.update({
                'shapefile_features': len(self.shapefile_data),
                'shapefile_crs': str(self.shapefile_data.crs)
            })
        
        # Add analysis summary if available
        if hasattr(self, 'vulnerability_rankings') and self.vulnerability_rankings is not None:
            category_counts = {}
            if 'vulnerability_category' in self.vulnerability_rankings.columns:
                category_counts = self.vulnerability_rankings['vulnerability_category'].value_counts().to_dict()
            
            summary.update({
                'analysis_complete': True,
                'vulnerability_categories': category_counts,
                'variables_used': self.composite_variables if hasattr(self, 'composite_variables') else [],
                'rankings_count': len(self.vulnerability_rankings)
            })
        
        return summary
    
    def has_geo_data(self) -> bool:
        """
        Check if shapefile data is loaded
        
        Returns:
            True if shapefile data is available
        """
        return self.shapefile_data is not None and not self.shapefile_data.empty
    
    def _save_analysis_results(self, results: Dict[str, Any]):
        """
        Save analysis results to files
        
        Args:
            results: Analysis results dictionary
        """
        try:
            # Save cleaned data
            if 'cleaned_data' in results and results['cleaned_data'] is not None:
                cleaned_path = os.path.join(self.session_folder, 'analysis_cleaned_data.csv')
                results['cleaned_data'].to_csv(cleaned_path, index=False)
            
            # Save normalized data
            if 'normalized_data' in results and results['normalized_data'] is not None:
                normalized_path = os.path.join(self.session_folder, 'analysis_normalized_data.csv')
                results['normalized_data'].to_csv(normalized_path, index=False)
            
            # Save vulnerability rankings
            if 'vulnerability_rankings' in results and results['vulnerability_rankings'] is not None:
                rankings_path = os.path.join(self.session_folder, 'analysis_vulnerability_rankings.csv')
                results['vulnerability_rankings'].to_csv(rankings_path, index=False)
            
            # Save composite scores
            if 'composite_scores' in results and results['composite_scores'] is not None:
                scores_path = os.path.join(self.session_folder, 'analysis_composite_scores.csv')
                if isinstance(results['composite_scores'], dict) and 'scores' in results['composite_scores']:
                    results['composite_scores']['scores'].to_csv(scores_path, index=False)
            
            self.logger.info("Analysis results saved successfully")
            
        except Exception as e:
            self.logger.error(f"Error saving analysis results: {str(e)}", exc_info=True)
    
    def _get_exclusion_reason(self, column_name: str) -> str:
        """
        Get reason why a column was excluded from analysis
        
        Args:
            column_name: Name of the column
            
        Returns:
            Reason for exclusion
        """
        if column_name.lower() == 'wardname':
            return 'Identifier column (WardName)'
        
        if self.validator._is_id_column(column_name, self.csv_data):
            return 'Appears to be an ID column'
        
        if column_name in self.csv_data.columns:
            if not pd.api.types.is_numeric_dtype(self.csv_data[column_name]):
                return 'Non-numeric data type'
            
            # Check for too many missing values
            missing_ratio = self.csv_data[column_name].isna().sum() / len(self.csv_data)
            if missing_ratio > 0.8:
                return f'Too many missing values ({missing_ratio:.1%})'
        
        return 'Does not meet analysis criteria'


# Convenience functions for backward compatibility
def run_complete_analysis(csv_data: pd.DataFrame, session_folder: str,
                         shapefile_data: Optional[gpd.GeoDataFrame] = None,
                         selected_variables: Optional[List[str]] = None,
                         na_methods: Optional[Dict[str, str]] = None,
                         custom_relationships: Optional[Dict[str, str]] = None,
                         interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to run complete analysis pipeline
    
    Args:
        csv_data: CSV DataFrame to analyze
        session_folder: Session folder path
        shapefile_data: Optional shapefile data
        selected_variables: Optional variable selection
        na_methods: Optional cleaning methods
        custom_relationships: Optional variable relationships
        interaction_logger: Optional interaction logger
        
    Returns:
        Analysis results dictionary
    """
    coordinator = AnalysisCoordinator(session_folder, interaction_logger)
    coordinator.csv_data = csv_data
    if shapefile_data is not None:
        coordinator.shapefile_data = shapefile_data
    
    return coordinator.run_full_analysis(
        selected_variables, na_methods, custom_relationships
    )


# Package information
__version__ = "1.0.0"
__all__ = [
    'AnalysisCoordinator',
    'run_complete_analysis'
] 