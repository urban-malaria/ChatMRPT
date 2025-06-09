# app/data/processing.py
"""
Data Processing Module - Cleaning, Normalization, and Scoring

This module handles data cleaning, normalization, composite scoring,
and urban extent analysis functionality.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- DataProcessor: Core processing functionality
- Data cleaning and normalization
- Composite score computation
- Urban extent analysis
"""

import os
import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List, Optional, Union

# Set up logging
logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Handles data processing including cleaning, normalization, and scoring
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize data processor
        
        Args:
            session_folder: Path to session folder for saving files
            interaction_logger: Optional interaction logger
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Ensure session folder exists
        os.makedirs(self.session_folder, exist_ok=True)
    
    def clean_data(self, csv_data: pd.DataFrame, 
                   shapefile_data: Optional[gpd.GeoDataFrame] = None,
                   na_methods: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Clean data by handling missing values
        
        Args:
            csv_data: DataFrame to clean
            shapefile_data: Optional shapefile for spatial imputation
            na_methods: Dict mapping columns to cleaning methods
            
        Returns:
            dict: Status and information about the cleaning process
        """
        try:
            if csv_data is None:
                return {'status': 'error', 'message': 'No CSV data provided'}
            
            # Import analysis functions
            try:
                from app.analysis import handle_missing_values, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available for data cleaning")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata if needed
            session_id = os.path.basename(self.session_folder)
            analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Use the centralized cleaning function
            cleaned_data = handle_missing_values(
                csv_data,
                na_methods,
                shapefile_data,
                -1,  # Use all available cores
                analysis_metadata
            )
            
            # Store NA handling methods from metadata
            na_handling_calcs = [calc for calc in analysis_metadata.calculations 
                               if calc['operation'].startswith('imputation_')]
            
            na_handling_methods = []
            for calc in na_handling_calcs:
                na_handling_methods.append({
                    'column': calc['variable'],
                    'method': calc['operation'].replace('imputation_', ''),
                    'parameters': calc.get('input_values', {})
                })
            
            # Save cleaned data
            output_path = os.path.join(self.session_folder, 'cleaned_data.csv')
            cleaned_data.to_csv(output_path, index=False)
            
            return {
                'status': 'success',
                'message': f'Successfully cleaned data with {len(na_handling_methods)} methods',
                'data': cleaned_data,
                'methods_used': {method['column']: method['method'] for method in na_handling_methods},
                'na_handling_methods': na_handling_methods,
                'file_path': output_path
            }
            
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error cleaning data: {str(e)}'}
    
    def normalize_data(self, cleaned_data: pd.DataFrame, 
                      relationships: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Normalize data based on variable relationships
        
        Args:
            cleaned_data: Cleaned DataFrame to normalize
            relationships: Dict mapping variables to relationships (direct/inverse)
            
        Returns:
            dict: Status and information about normalization
        """
        try:
            if cleaned_data is None:
                return {'status': 'error', 'message': 'No cleaned data provided'}
            
            # Import analysis functions
            try:
                from app.analysis import normalize_data, determine_variable_relationships, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available for normalization")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata
            session_id = os.path.basename(self.session_folder)
            analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Determine relationships if not provided
            variable_relationships = relationships
            if relationships is None:
                # Get variables from cleaned data
                variables = [col for col in cleaned_data.columns 
                           if col != 'WardName' and pd.api.types.is_numeric_dtype(cleaned_data[col])]
                
                # Determine relationships
                variable_relationships = determine_variable_relationships(
                    variables, 
                    None,
                    analysis_metadata
                )
            
            # Use the centralized normalization function
            normalized_data = normalize_data(
                cleaned_data,
                variable_relationships,
                None,  # No columns to exclude
                -1,  # Use all available cores
                analysis_metadata
            )
            
            # Save normalized data
            output_path = os.path.join(self.session_folder, 'normalized_data.csv')
            normalized_data.to_csv(output_path, index=False)
            
            # Count normalized columns
            norm_cols = [col for col in normalized_data.columns if col.startswith('normalization_')]
            
            return {
                'status': 'success',
                'message': f'Successfully normalized {len(norm_cols)} variables',
                'data': normalized_data,
                'normalized_columns': norm_cols,
                'relationships': variable_relationships,
                'file_path': output_path
            }
            
        except Exception as e:
            self.logger.error(f"Error normalizing data: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error normalizing data: {str(e)}'}
    
    def compute_composite_scores(self, normalized_data: pd.DataFrame,
                               selected_variables: Optional[List[str]] = None,
                               method: str = 'mean') -> Dict[str, Any]:
        """
        Compute composite vulnerability scores from normalized data
        
        Args:
            normalized_data: Normalized DataFrame
            selected_variables: Variables to include (None for all)
            method: Scoring method ('mean', 'pca')
                - 'mean': Simple average (default, fast)
                - 'pca': Principal Component Analysis (advanced, with feature importance)
            
        Returns:
            dict: Status and information about scoring
        """
        try:
            if normalized_data is None:
                return {'status': 'error', 'message': 'No normalized data provided'}
            
            # Import analysis functions
            try:
                from app.analysis import compute_composite_scores, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available for composite scoring")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata
            session_id = os.path.basename(self.session_folder)
            analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Use the centralized composite score function
            composite_scores = compute_composite_scores(
                normalized_data,
                selected_variables,
                method,
                -1,  # Use all available cores
                analysis_metadata
            )
            
            # Store selected variables
            composite_variables = selected_variables or [
                col.replace('normalization_', '') for col in 
                normalized_data.columns if col.startswith('normalization_')
            ]
            
            # Save composite scores to CSV
            scores_path = os.path.join(self.session_folder, 'composite_scores.csv')
            composite_scores['scores'].to_csv(scores_path, index=False)
            
            # Save model formulas
            formulas_path = os.path.join(self.session_folder, 'model_formulas.csv')
            pd.DataFrame([
                {
                    'model': formula['model'],
                    'variables': ','.join(formula['variables'])
                } for formula in composite_scores['formulas']
            ]).to_csv(formulas_path, index=False)
            
            return {
                'status': 'success',
                'message': f'Successfully computed composite scores using {method} method',
                'scores': composite_scores,
                'composite_variables': composite_variables,
                'method': method,
                'scores_file': scores_path,
                'formulas_file': formulas_path
            }
            
        except Exception as e:
            self.logger.error(f"Error computing composite scores: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error computing composite scores: {str(e)}'}
    
    def process_urban_extent(self, csv_data: pd.DataFrame,
                           shapefile_data: Optional[gpd.GeoDataFrame] = None,
                           thresholds: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        Analyze urban extent at different thresholds
        
        Args:
            csv_data: CSV data with urban percentage information
            shapefile_data: Optional shapefile data
            thresholds: List of thresholds to analyze (default: [30, 50, 75, 100])
            
        Returns:
            Dict with results for each threshold
        """
        try:
            if csv_data is None:
                return {'status': 'error', 'message': 'No CSV data provided'}
            
            # Import analysis functions
            try:
                from app.analysis import analyze_urban_extent, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available for urban extent analysis")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata
            session_id = os.path.basename(self.session_folder)
            analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Use the centralized urban extent analysis function
            urban_extent_results = analyze_urban_extent(
                csv_data,
                shapefile_data,
                None,  # Auto-detect urban percentage column
                thresholds,
                analysis_metadata
            )
            
            # Save urban extent summary
            summary_rows = []
            for threshold, results in urban_extent_results.items():
                summary_rows.append({
                    'threshold': threshold,
                    'meets_threshold': results['meets_threshold'],
                    'below_threshold': results['below_threshold']
                })
            
            summary_path = os.path.join(self.session_folder, 'urban_extent_summary.csv')
            pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
            
            return {
                'status': 'success',
                'message': f'Successfully analyzed urban extent at {len(urban_extent_results)} thresholds',
                'thresholds': list(urban_extent_results.keys()),
                'results': urban_extent_results,
                'summary_file': summary_path
            }
            
        except Exception as e:
            self.logger.error(f"Error processing urban extent: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error processing urban extent: {str(e)}'
            }
    
    def calculate_vulnerability_rankings(self, composite_scores: Dict[str, Any],
                                       n_categories: int = 3) -> Dict[str, Any]:
        """
        Calculate vulnerability rankings from composite scores
        
        Args:
            composite_scores: Composite scores dictionary from compute_composite_scores
            n_categories: Number of vulnerability categories (default: 3)
            
        Returns:
            dict: Status and vulnerability rankings
        """
        try:
            if not composite_scores or 'scores' not in composite_scores:
                return {'status': 'error', 'message': 'No composite scores provided'}
            
            # Import analysis functions
            try:
                from app.analysis import analyze_vulnerability, AnalysisMetadata
            except ImportError:
                self.logger.error("Analysis module not available for vulnerability analysis")
                return {'status': 'error', 'message': 'Analysis module not available'}
            
            # Initialize analysis metadata
            session_id = os.path.basename(self.session_folder)
            analysis_metadata = AnalysisMetadata(session_id, self.interaction_logger)
            
            # Use the centralized vulnerability analysis function
            vulnerability_results = analyze_vulnerability(
                composite_scores['scores'],
                n_categories,
                analysis_metadata
            )
            
            # Save vulnerability rankings
            rankings_path = os.path.join(self.session_folder, 'vulnerability_rankings.csv')
            vulnerability_results['rankings'].to_csv(rankings_path, index=False)
            
            # Count categories
            category_counts = {}
            if 'vulnerability_category' in vulnerability_results['rankings'].columns:
                category_counts = vulnerability_results['rankings']['vulnerability_category'].value_counts().to_dict()
            
            return {
                'status': 'success',
                'message': f'Successfully calculated vulnerability rankings with {n_categories} categories',
                'rankings': vulnerability_results['rankings'],
                'category_counts': category_counts,
                'vulnerability_results': vulnerability_results,
                'rankings_file': rankings_path
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating vulnerability rankings: {str(e)}", exc_info=True)
            return {'status': 'error', 'message': f'Error calculating vulnerability rankings: {str(e)}'}


# Convenience functions for backward compatibility
def clean_dataset(csv_data: pd.DataFrame, session_folder: str,
                 shapefile_data: Optional[gpd.GeoDataFrame] = None,
                 na_methods: Optional[Dict[str, str]] = None,
                 interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to clean a dataset
    
    Args:
        csv_data: DataFrame to clean
        session_folder: Session folder path
        shapefile_data: Optional shapefile for spatial imputation
        na_methods: Optional cleaning methods
        interaction_logger: Optional interaction logger
        
    Returns:
        Cleaning result dictionary
    """
    processor = DataProcessor(session_folder, interaction_logger)
    return processor.clean_data(csv_data, shapefile_data, na_methods)


def normalize_dataset(cleaned_data: pd.DataFrame, session_folder: str,
                     relationships: Optional[Dict[str, str]] = None,
                     interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to normalize a dataset
    
    Args:
        cleaned_data: Cleaned DataFrame to normalize
        session_folder: Session folder path
        relationships: Optional variable relationships
        interaction_logger: Optional interaction logger
        
    Returns:
        Normalization result dictionary
    """
    processor = DataProcessor(session_folder, interaction_logger)
    return processor.normalize_data(cleaned_data, relationships)


def calculate_composite_scores(normalized_data: pd.DataFrame, session_folder: str,
                             selected_variables: Optional[List[str]] = None,
                             method: str = 'mean',
                             interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to calculate composite scores
    
    Args:
        normalized_data: Normalized DataFrame
        session_folder: Session folder path
        selected_variables: Optional variable selection
        method: Scoring method
        interaction_logger: Optional interaction logger
        
    Returns:
        Scoring result dictionary
    """
    processor = DataProcessor(session_folder, interaction_logger)
    return processor.compute_composite_scores(normalized_data, selected_variables, method)


def analyze_urban_thresholds(csv_data: pd.DataFrame, session_folder: str,
                           shapefile_data: Optional[gpd.GeoDataFrame] = None,
                           thresholds: Optional[List[int]] = None,
                           interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to analyze urban extent thresholds
    
    Args:
        csv_data: CSV data with urban information
        session_folder: Session folder path
        shapefile_data: Optional shapefile data
        thresholds: Analysis thresholds
        interaction_logger: Optional interaction logger
        
    Returns:
        Urban extent analysis results
    """
    processor = DataProcessor(session_folder, interaction_logger)
    return processor.process_urban_extent(csv_data, shapefile_data, thresholds)


# Package information
__version__ = "1.0.0"
__all__ = [
    'DataProcessor',
    'clean_dataset',
    'normalize_dataset', 
    'calculate_composite_scores',
    'analyze_urban_thresholds'
] 