# app/models/data_handler.py
import os
import zipfile
import tempfile
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import json
import requests
from typing import List, Dict, Tuple, Optional, Union, Any
import shutil

# Import analysis module
from app.models.analysis import (
    handle_missing_values,
    determine_variable_relationships,
    normalize_data,
    compute_composite_scores,
    analyze_vulnerability,
    analyze_urban_extent,
    check_data_quality,
    run_full_analysis_pipeline,
    AnalysisMetadata
)
# *** ADD THIS IMPORT ***
from app.utilities import is_id_column

# Set up logging
logger = logging.getLogger(__name__)

class DataHandler:
    """Class to handle data loading, storage, and coordination with analysis module"""
    
    def __init__(self, session_folder, interaction_logger=None):
        """
        Initialize with session folder path
        
        Args:
            session_folder: Path to folder for storing session files
            interaction_logger: Optional InteractionLogger instance
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.session_id = os.path.basename(session_folder)
        self.csv_data = None
        self.shapefile_data = None
        self.cleaned_data = None
        self.normalized_data = None
        self.composite_scores = None
        self.variable_relationships = {}
        self.missing_columns = []
        self.mismatched_wards = None
        self.composite_variables = None
        self.vulnerability_rankings = None
        self.boxwhisker_plot = None
        self.urban_extent_results = None
        self.na_handling_methods = []
        self.analysis_metadata = None
        
        # Create session folder if it doesn't exist
        os.makedirs(self.session_folder, exist_ok=True)
        
        # Set up logging
        self.logger = logging.getLogger(__name__)

    # *** ADD THIS METHOD ***
    def get_available_variables(self) -> List[str]:
        """
        Get a list of available variable names from the loaded CSV data
        that are suitable for analysis.
        
        Returns:
            List[str]: List of suitable variable names in their proper case and format.
        """
        if self.csv_data is None:
            return []
        
        available_vars = []
        for col in self.csv_data.columns:
            # Exclude 'WardName' and common ID-like columns
            if col.lower() != 'wardname' and not self._is_id_column(col):
                # Only include numeric columns for analysis
                if pd.api.types.is_numeric_dtype(self.csv_data[col]):
                    available_vars.append(col)
        
        # Add any derived variables if they exist
        if hasattr(self, 'derived_variables') and self.derived_variables:
            for var in self.derived_variables:
                if var not in available_vars:
                    available_vars.append(var)
        
        # Sort for easier browsing
        available_vars.sort()
        
        return available_vars
    # *** END OF ADDED METHOD ***
    
    def load_csv(self, file_path):
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
                self.csv_data = pd.read_excel(file_path)
            else:
                # Load CSV file with robust parameters
                self.csv_data = pd.read_csv(
                    file_path,
                    na_values=['NA', '', 'N/A'],
                    keep_default_na=True
                )
            
            # Ensure column names are valid
            self.csv_data.columns = self.csv_data.columns.str.strip()
            
            # Ensure WardName column exists - rename if needed
            if 'Ward' in self.csv_data.columns and 'WardName' not in self.csv_data.columns:
                self.csv_data = self.csv_data.rename(columns={'Ward': 'WardName'})
            
            # Handle duplicate ward names if WardCode exists
            if 'WardName' in self.csv_data.columns and 'WardCode' in self.csv_data.columns:
                self.csv_data = self._handle_duplicate_wardnames(self.csv_data)
            
            # Check for missing values
            self.missing_columns = self._check_missing_values(self.csv_data)
            
            # Save processed CSV locally
            self.csv_data.to_csv(os.path.join(self.session_folder, 'processed_data.csv'), index=False)
            
            # Run data quality check
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            quality_issues = check_data_quality(self.csv_data, self.analysis_metadata)
            
            # Include severe quality issues in response
            severe_issues = []
            if 'severe_issues' in quality_issues:
                for issue in quality_issues['severe_issues']:
                    severe_issues.append({
                        'type': issue['type'],
                        'column': issue.get('column', 'N/A'),
                        'message': issue.get('recommendation', 'Review this data')
                    })
            
            return {
                'status': 'success',
                'message': f'CSV file loaded successfully with {len(self.csv_data)} rows and {len(self.csv_data.columns)} columns',
                'rows': len(self.csv_data),
                'columns': len(self.csv_data.columns),
                'missing_values': len(self.missing_columns),
                'quality_issues': severe_issues if severe_issues else None
            }
            
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error loading CSV file: {str(e)}'
            }
    
    def load_shapefile(self, zip_file_path):
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
                
                # Find shapefile(s) in the extracted directory
                shp_files = []
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.shp'):
                            shp_files.append(os.path.join(root, file))
                
                if not shp_files:
                    return {
                        'status': 'error',
                        'message': 'No shapefile (.shp) found in the ZIP file'
                    }
                
                # Load the first shapefile found
                self.shapefile_data = gpd.read_file(shp_files[0])
                
                # Log the original CRS
                self.logger.info(f"Loaded shapefile with CRS: {self.shapefile_data.crs}")
                
                # Ensure WardName column exists
                if 'WardName' not in self.shapefile_data.columns:
                    # Look for potential ward name columns
                    potential_columns = [col for col in self.shapefile_data.columns if 
                                        any(name in col.lower() for name in ['ward', 'name', 'area'])]
                    
                    if potential_columns:
                        # Use the first potential column
                        self.shapefile_data = self.shapefile_data.rename(
                            columns={potential_columns[0]: 'WardName'}
                        )
                    else:
                        # Create sequential ward names if no suitable column found
                        self.shapefile_data['WardName'] = [f'Ward_{i+1}' for i in range(len(self.shapefile_data))]
                
                # Handle duplicate ward names if WardCode exists
                if 'WardCode' in self.shapefile_data.columns:
                    self.shapefile_data = self._handle_duplicate_wardnames(self.shapefile_data)
                
                # Ensure UrbanPercent column exists (for urban extent analysis)
                if 'UrbanPercent' not in self.shapefile_data.columns:
                    if 'Urban' in self.shapefile_data.columns:
                        # If Urban column exists, convert to percentage
                        self.shapefile_data['Urban'] = self.shapefile_data['Urban'].astype(str)
                        # Convert Yes/No to 100/0
                        self.shapefile_data['UrbanPercent'] = self.shapefile_data['Urban'].apply(
                            lambda x: 100 if x.lower() in ['yes', 'true', '1'] else 0
                        )
                    else:
                        # Do not generate random values - leave column absent
                        self.logger.warning("No urban data found in shapefile")
                
                # Standardize the CRS to WGS84 (EPSG:4326)
                # Check if CRS needs conversion
                if self.shapefile_data.crs and self.shapefile_data.crs != "EPSG:4326":
                    self.logger.info(f"Converting shapefile from {self.shapefile_data.crs} to EPSG:4326")
                    try:
                        self.shapefile_data = self.shapefile_data.to_crs(epsg=4326)
                        self.logger.info("CRS conversion successful")
                    except Exception as crs_error:
                        self.logger.warning(f"CRS conversion error: {str(crs_error)}. Using original CRS.")
                
                # Save shapefile locally for future use
                shp_output_dir = os.path.join(self.session_folder, 'shapefile')
                os.makedirs(shp_output_dir, exist_ok=True)
                self.shapefile_data.to_file(os.path.join(shp_output_dir, 'processed.shp'))
                
                # Check for ward name mismatches if CSV is already loaded
                if self.csv_data is not None:
                    self.mismatched_wards = self.check_wardname_mismatches()
                
                return {
                    'status': 'success',
                    'message': f'Shapefile loaded successfully with {len(self.shapefile_data)} features',
                    'features': len(self.shapefile_data),
                    'crs': str(self.shapefile_data.crs),
                    'mismatches': self.mismatched_wards
                }
        
        except Exception as e:
            self.logger.error(f"Error loading shapefile: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error loading shapefile: {str(e)}'
            }
    
    def check_wardname_mismatches(self):
        """
        Check for ward name mismatches between CSV and shapefile
        
        Returns:
            list: List of mismatched ward names, or None if no mismatches or data not loaded
        """
        if self.csv_data is None or self.shapefile_data is None:
            return None
        
        if 'WardName' not in self.csv_data.columns or 'WardName' not in self.shapefile_data.columns:
            return None
        
        # Get ward names from both datasets
        csv_wardnames = set(self.csv_data['WardName'].unique())
        shp_wardnames = set(self.shapefile_data['WardName'].unique())
        
        # Find ward names in CSV that don't exist in shapefile
        mismatched_wards = csv_wardnames - shp_wardnames
        
        if mismatched_wards:
            # Create a list of mismatches with potential matches from shapefile
            mismatches = []
            for ward in mismatched_wards:
                mismatches.append({
                    'csv_wardname': ward,
                    'potential_matches': list(shp_wardnames)[:10]  # Limit to 10 potential matches
                })
            
            return mismatches
        
        return None
    
    def run_full_analysis(self, selected_variables=None, na_methods=None, custom_relationships=None, llm_manager=None):
        """
        Run the complete analysis pipeline
        
        Args:
            selected_variables: List of variables to use for composite scores
            na_methods: Dict mapping columns to methods for handling missing values
            custom_relationships: Dict mapping variables to relationships (direct/inverse)
            llm_manager: Optional LLM manager for AI-driven variable selection
            
        Returns:
            Dict with analysis results
        """
        try:
            # Initialize or reset analysis metadata
            self.analysis_metadata = AnalysisMetadata(
                self.session_id, 
                self.interaction_logger
            )
            
            # Call the centralized analysis pipeline
            result = run_full_analysis_pipeline(
                self,  # Pass self as data_handler
                selected_variables,
                na_methods,
                custom_relationships,
                self.analysis_metadata,
                self.session_id,
                self.interaction_logger,
                llm_manager  # Pass the LLM manager for variable selection
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in full analysis pipeline: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error in full analysis pipeline: {str(e)}'
            }

    # Add a method to explain variable selection
    def explain_variable_selection(self, llm_manager=None):
        """
        Generate an explanation for why the current variables were selected
        
        Args:
            llm_manager: Optional LLM manager for generating the explanation
            
        Returns:
            dict: Explanation result
        """
        try:
            # Check if we have composite variables and a selection method
            if not hasattr(self, 'composite_variables') or not self.composite_variables:
                return {
                    'status': 'error',
                    'message': 'No variables have been selected yet. Run the analysis first.'
                }
            
            # Get selection method and variables
            selection_method = getattr(self, 'variable_selection_method', 'default')
            variables = self.composite_variables
            explanations = getattr(self, 'variable_selection_explanations', {})
            
            # If there's no LLM manager or no stored explanations, return basic info
            if not llm_manager or not explanations:
                # Create a simple explanation based on selection method
                if selection_method == 'user_specified':
                    explanation = f"The variables {', '.join(variables)} were specified by the user."
                elif selection_method == 'llm_selected':
                    explanation = f"The variables {', '.join(variables)} were automatically selected for optimal malaria risk prediction."
                else:
                    explanation = f"All available variables were used for the analysis: {', '.join(variables)}."
                
                return {
                    'status': 'success',
                    'message': 'Variable selection explanation',
                    'explanation': explanation,
                    'selection_method': selection_method,
                    'variables': variables
                }
            
            # Use LLM to generate a detailed explanation
            explanation = llm_manager.explain_variable_selection(
                variables=variables,
                explanations=explanations,
                context={
                    'selection_method': selection_method,
                    'total_variables': len(variables)
                },
                session_id=self.session_id
            )
            
            return {
                'status': 'success',
                'message': 'Generated variable selection explanation',
                'explanation': explanation,
                'selection_method': selection_method,
                'variables': variables
            }
            
        except Exception as e:
            self.logger.error(f"Error explaining variable selection: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error explaining variable selection: {str(e)}'
            }

    # Add a method to get available variables
    def get_available_variables(self):
        """
        Get all available variables from the loaded data
        
        Returns:
            list: Available variable names
        """
        try:
            available_vars = []
            
            # Check if CSV data is loaded
            if self.csv_data is not None:
                # Identify variables (not identifiers or WardName)
                available_vars = [col for col in self.csv_data.columns 
                                if col != 'WardName' and not self._is_id_column(col)]
            
            return available_vars
        except Exception as e:
            self.logger.error(f"Error getting available variables: {str(e)}")
            return []

    
    def clean_data(self, na_methods=None):
        """
        Clean data by handling missing values
        
        Args:
            na_methods: Dict mapping columns to cleaning methods
            
        Returns:
            dict: Status and information about the cleaning process
        """
        try:
            if self.csv_data is None:
                return {'status': 'error', 'message': 'No CSV data loaded'}
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Use the centralized cleaning function
            self.cleaned_data = handle_missing_values(
                self.csv_data,
                na_methods,
                self.shapefile_data,
                -1,  # Use all available cores
                self.analysis_metadata
            )
            
            # Store NA handling methods from metadata
            na_handling_calcs = [calc for calc in self.analysis_metadata.calculations 
                               if calc['operation'].startswith('imputation_')]
            
            self.na_handling_methods = []
            for calc in na_handling_calcs:
                self.na_handling_methods.append({
                    'column': calc['variable'],
                    'method': calc['operation'].replace('imputation_', ''),
                    'parameters': calc.get('input_values', {})
                })
            
            # Save cleaned data
            self.cleaned_data.to_csv(os.path.join(self.session_folder, 'cleaned_data.csv'), index=False)
            
            return {
                'status': 'success',
                'message': f'Successfully cleaned data with {len(self.na_handling_methods)} methods',
                'methods_used': {method['column']: method['method'] for method in self.na_handling_methods}
            }
            
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}")
            return {'status': 'error', 'message': f'Error cleaning data: {str(e)}'}
    
    def normalize_data(self, relationships=None):
        """
        Normalize data based on variable relationships
        
        Args:
            relationships: Dict mapping variables to relationships (direct/inverse)
            
        Returns:
            dict: Status and information about normalization
        """
        try:
            if self.cleaned_data is None:
                # Try to clean data first
                clean_result = self.clean_data()
                if clean_result['status'] != 'success':
                    return clean_result
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Determine relationships if not provided
            if relationships is None:
                if not self.variable_relationships:
                    # Get variables from cleaned data
                    variables = [col for col in self.cleaned_data.columns 
                               if col != 'WardName' and pd.api.types.is_numeric_dtype(self.cleaned_data[col])]
                    
                    # Determine relationships
                    self.variable_relationships = determine_variable_relationships(
                        variables, 
                        None,
                        self.analysis_metadata
                    )
                
                relationships = self.variable_relationships
            else:
                # Store provided relationships
                self.variable_relationships = relationships
            
            # Use the centralized normalization function
            self.normalized_data = normalize_data(
                self.cleaned_data,
                relationships,
                None,  # No columns to exclude
                -1,  # Use all available cores
                self.analysis_metadata
            )
            
            # Save normalized data
            self.normalized_data.to_csv(os.path.join(self.session_folder, 'normalized_data.csv'), index=False)
            
            norm_cols = [col for col in self.normalized_data.columns if col.startswith('normalization_')]
            
            return {
                'status': 'success',
                'message': f'Successfully normalized {len(norm_cols)} variables',
                'normalized_columns': norm_cols
            }
            
        except Exception as e:
            self.logger.error(f"Error normalizing data: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error normalizing data: {str(e)}'
            }
    
    def compute_composite_scores(self, selected_variables=None, method='mean'):
        """
        Calculate composite scores using selected normalized variables
        
        Args:
            selected_variables: List of variables to use (if None, selects using available variables)
            method: Aggregation method ('mean')
            
        Returns:
            dict: Status and information about the composite scores
        """
        try:
            if self.normalized_data is None:
                # Try to normalize data first
                norm_result = self.normalize_data()
                if norm_result['status'] != 'success':
                    return norm_result
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Use the centralized composite score function
            self.composite_scores = compute_composite_scores(
                self.normalized_data,
                selected_variables,
                method,
                -1,  # Use all available cores
                self.analysis_metadata
            )
            
            # Store selected variables
            self.composite_variables = selected_variables or [
                col.replace('normalization_', '') for col in 
                self.normalized_data.columns if col.startswith('normalization_')
            ]
            
            # Save composite scores to CSV
            self.composite_scores['scores'].to_csv(
                os.path.join(self.session_folder, 'composite_scores.csv'), 
                index=False
            )
            
            # Save model formulas
            pd.DataFrame([
                {
                    'model': formula['model'],
                    'variables': ','.join(formula['variables'])
                } for formula in self.composite_scores['model_formulas']
            ]).to_csv(
                os.path.join(self.session_folder, 'model_formulas.csv'),
                index=False
            )
            
            return {
                'status': 'success',
                'message': f'Successfully calculated {len(self.composite_scores["model_formulas"])} composite score models',
                'models': len(self.composite_scores['model_formulas']),
                'variables_used': self.composite_variables
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating composite scores: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error calculating composite scores: {str(e)}'
            }
    
    def calculate_vulnerability_rankings(self, n_categories=3):
        """
        Calculate vulnerability rankings based on composite scores
        
        Args:
            n_categories: Number of vulnerability categories
            
        Returns:
            dict: Status and vulnerability ranking information
        """
        try:
            if not hasattr(self, 'composite_scores') or self.composite_scores is None:
                # Try to calculate composite scores first
                composite_result = self.compute_composite_scores()
                if composite_result['status'] != 'success':
                    return composite_result
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Use the centralized vulnerability analysis function
            self.vulnerability_rankings = analyze_vulnerability(
                self.composite_scores,
                n_categories,
                self.analysis_metadata
            )
            
            # Save vulnerability rankings
            self.vulnerability_rankings.to_csv(
                os.path.join(self.session_folder, 'vulnerability_rankings.csv'),
                index=False
            )
            
            # Get most vulnerable wards
            vulnerable_wards = self.vulnerability_rankings.sort_values('overall_rank')['WardName'].tolist()
            
            return {
                'status': 'success',
                'message': f'Successfully ranked {len(self.vulnerability_rankings)} wards by vulnerability',
                'vulnerable_wards': vulnerable_wards
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating vulnerability rankings: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error calculating vulnerability rankings: {str(e)}'
            }
    
    def process_urban_extent(self, thresholds=None):
        """
        Analyze urban extent at different thresholds
        
        Args:
            thresholds: List of thresholds to analyze (default: [30, 50, 75, 100])
            
        Returns:
            Dict with results for each threshold
        """
        try:
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Use the centralized urban extent analysis function
            self.urban_extent_results = analyze_urban_extent(
                self.csv_data,
                self.shapefile_data,
                None,  # Auto-detect urban percentage column
                thresholds,
                self.analysis_metadata
            )
            
            # Save urban extent summary
            summary_rows = []
            for threshold, results in self.urban_extent_results.items():
                summary_rows.append({
                    'threshold': threshold,
                    'meets_threshold': results['meets_threshold'],
                    'below_threshold': results['below_threshold']
                })
            
            pd.DataFrame(summary_rows).to_csv(
                os.path.join(self.session_folder, 'urban_extent_summary.csv'),
                index=False
            )
            
            return {
                'status': 'success',
                'message': f'Successfully analyzed urban extent at {len(self.urban_extent_results)} thresholds',
                'thresholds': list(self.urban_extent_results.keys()),
                'results': self.urban_extent_results
            }
            
        except Exception as e:
            self.logger.error(f"Error processing urban extent: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error processing urban extent: {str(e)}'
            }
    
    def generate_report(self, format="markdown"):
        """
        Generate analysis report
        
        Args:
            format: Report format ("markdown", "html", "dict")
            
        Returns:
            Report content in the specified format
        """
        try:
            # Import here to avoid circular imports
            from app.models.analysis import generate_analysis_report
            
            # Check if analysis is complete
            if not hasattr(self, 'vulnerability_rankings') or self.vulnerability_rankings is None:
                return {
                    'status': 'error',
                    'message': 'Analysis is not complete. Run the full analysis first.'
                }
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            # Generate report using the centralized function
            report_content = generate_analysis_report(
                self,
                self.analysis_metadata,
                format
            )
            
            # Save report to file
            report_folder = os.path.join(self.session_folder, 'reports')
            os.makedirs(report_folder, exist_ok=True)
            
            if format == "html":
                report_path = os.path.join(report_folder, 'analysis_report.html')
                with open(report_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
            else:
                report_path = os.path.join(report_folder, 'analysis_report.md')
                with open(report_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
            
            return {
                'status': 'success',
                'message': f'Successfully generated {format} report',
                'report_path': report_path,
                'report_content': report_content
            }
            
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error generating report: {str(e)}'
            }
    
    def get_explanation_context(self, entity_type, entity_name, question=None):
        """
        Get explanation context for LLM processing
        
        Args:
            entity_type: Type of entity to explain ('ward', 'variable', 'visualization')
            entity_name: Name of the entity
            question: Optional specific question being asked
            
        Returns:
            Dict with context for LLM explanation
        """
        try:
            # Import relevant functions from analysis module
            from app.models.analysis import (
                get_explanation_for_ward,
                get_explanation_for_visualization,
                get_explanation_for_analysis_result
            )
            
            # Initialize analysis metadata if needed
            if self.analysis_metadata is None:
                self.analysis_metadata = AnalysisMetadata(
                    self.session_id, 
                    self.interaction_logger
                )
            
            if entity_type == 'ward':
                context = get_explanation_for_ward(
                    entity_name,
                    question,
                    self.analysis_metadata,
                    self
                )
            elif entity_type == 'visualization':
                context = get_explanation_for_visualization(
                    entity_name,  # Visualization type
                    {},  # Visualization data (would need to be populated)
                    question,
                    self.analysis_metadata
                )
            elif entity_type == 'analysis_result':
                # Get the last analysis result
                last_result = {
                    'variables_used': self.composite_variables,
                    'vulnerable_wards': self.vulnerability_rankings['WardName'].tolist() if self.vulnerability_rankings is not None else [],
                }
                
                context = get_explanation_for_analysis_result(
                    last_result,
                    question,
                    self.analysis_metadata
                )
            else:
                context = {
                    'error': f'Unknown entity type: {entity_type}',
                    'available_types': ['ward', 'visualization', 'analysis_result']
                }
            
            return context
            
        except Exception as e:
            self.logger.error(f"Error getting explanation context: {str(e)}")
            return {
                'error': str(e),
                'entity_type': entity_type,
                'entity_name': entity_name
            }
    
    def _handle_duplicate_wardnames(self, df):
        """
        Handle duplicate ward names in a dataframe
        
        Args:
            df: Dataframe with potentially duplicate ward names
            
        Returns:
            Dataframe with unique ward names
        """
        # Check if WardName column exists
        if 'WardName' not in df.columns:
            self.logger.warning("WardName column not found, cannot handle duplicates")
            return df
        
        # Check if WardCode column exists
        if 'WardCode' not in df.columns:
            self.logger.warning("WardCode column not found, cannot disambiguate duplicates")
            return df
        
        # Find duplicate ward names
        duplicate_mask = df['WardName'].duplicated(keep=False)
        if not duplicate_mask.any():
            return df  # No duplicates found
        
        # Get duplicate ward names
        duplicate_wards = df.loc[duplicate_mask, 'WardName'].unique()
        
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # For each duplicate ward name, create unique names using WardCode_WardName
        for ward in duplicate_wards:
            # Find rows with this ward name
            ward_mask = df_copy['WardName'] == ward
            
            # Rename only if there's more than one occurrence
            if ward_mask.sum() > 1:
                # Vectorized operation instead of row-by-row
                df_copy.loc[ward_mask, 'WardName'] = df_copy.loc[ward_mask, 'WardCode'] + '_' + df_copy.loc[ward_mask, 'WardName']
        
        return df_copy
    
    def _check_missing_values(self, df):
        """
        Check for columns with missing values
        
        Args:
            df: Dataframe to check
            
        Returns:
            list: Names of columns with missing values
        """
        # Vectorized operation
        missing_counts = df.isna().sum()
        cols_with_missing = missing_counts[missing_counts > 0].index.tolist()
        
        return cols_with_missing
    
    # Helper method to identify ID columns
    def _is_id_column(self, column_name):
        """
        Check if a column appears to be an ID column
        
        Args:
            column_name: Column name to check
            
        Returns:
            bool: True if it seems to be an ID column
        """
        id_patterns = ['id', 'x.1', 'x', 'index', 'lga_code', 'wardid', 'ward_id', 'code']
        column_lower = column_name.lower()
        
        # Check if it matches common ID patterns
        for pattern in id_patterns:
            if pattern == column_lower or f"{pattern}_" in column_lower:
                return True
        
        # Also check if it's a non-numeric column with many unique values
        if self.csv_data is not None and column_name in self.csv_data.columns:
            col = self.csv_data[column_name]
            
            # If it's a string column with lots of unique values relative to row count
            if not pd.api.types.is_numeric_dtype(col):
                unique_ratio = col.nunique() / len(col) if len(col) > 0 else 0
                # If more than 90% of values are unique, likely an ID
                if unique_ratio > 0.9 and col.nunique() > 10:
                    return True
        
        return False

    def has_geo_data(self):
        """
        Check if the data handler has shapefile data loaded
        
        Returns:
            bool: True if shapefile data is loaded
        """
        return self.shapefile_data is not None and not self.shapefile_data.empty
        
    def get_data_summary(self):
        """
        Get a summary of the loaded data for visualizations and explanations
        
        Returns:
            dict: Summary information about the loaded data
        """
        summary = {
            'csv_loaded': self.csv_data is not None,
            'shapefile_loaded': self.shapefile_data is not None,
            'analysis_complete': hasattr(self, 'vulnerability_rankings') and self.vulnerability_rankings is not None
        }
        
        # Add CSV data summary if available
        if self.csv_data is not None:
            summary.update({
                'rows': len(self.csv_data),
                'columns': len(self.csv_data.columns),
                'available_variables': self.get_available_variables()[:10],  # First 10 for brevity
                'variable_count': len(self.get_available_variables())
            })
        
        # Add shapefile summary if available
        if self.shapefile_data is not None:
            summary.update({
                'features': len(self.shapefile_data),
                'crs': str(self.shapefile_data.crs)
            })
        
        # Add analysis summary if available
        if hasattr(self, 'vulnerability_rankings') and self.vulnerability_rankings is not None:
            high_risk_count = 0
            medium_risk_count = 0
            low_risk_count = 0
            
            if 'vulnerability_category' in self.vulnerability_rankings.columns:
                high_risk_count = len(self.vulnerability_rankings[self.vulnerability_rankings['vulnerability_category'] == 'High'])
                medium_risk_count = len(self.vulnerability_rankings[self.vulnerability_rankings['vulnerability_category'] == 'Medium'])
                low_risk_count = len(self.vulnerability_rankings[self.vulnerability_rankings['vulnerability_category'] == 'Low'])
            
            summary.update({
                'high_risk_count': high_risk_count,
                'medium_risk_count': medium_risk_count,
                'low_risk_count': low_risk_count,
                'variables_used': self.composite_variables if hasattr(self, 'composite_variables') else []
            })
        
        return summary
        
    def find_matching_variables(self, variables):
        """
        Find matching variables in the dataset using case-insensitive and partial matching.
        
        Args:
            variables: List of variable names to match
            
        Returns:
            dict: Mapping of input variables to matched variables, with None for unmatched
        """
        available_vars = self.get_available_variables()
        available_vars_lookup = {var.lower(): var for var in available_vars}
        
        result = {}
        
        for var in variables:
            if not var:
                result[var] = None
                continue
                
            var_lower = var.lower()
            
            # Direct case-insensitive match
            if var_lower in available_vars_lookup:
                result[var] = available_vars_lookup[var_lower]
                continue
            
            # Try normalized version (replace spaces with underscores)
            normalized_var = var_lower.replace(' ', '_')
            if normalized_var in available_vars_lookup:
                result[var] = available_vars_lookup[normalized_var]
                continue
            
            # Try partial matching
            matched = False
            for av_lower, av in available_vars_lookup.items():
                # Check if variable name is contained in available variable or vice versa
                if var_lower in av_lower or av_lower in var_lower:
                    result[var] = av
                    matched = True
                    break
            
            # If no match found
            if not matched:
                result[var] = None
        
        return result
    
    def validate_variables(self, variables):
        """
        Validate a list of variable names against available variables.
        
        Args:
            variables: List of variable names to validate
            
        Returns:
            dict: Validation result with matched variables and status
        """
        if not variables:
            return {
                'is_valid': False,
                'message': 'No variables provided',
                'valid_variables': [],
                'invalid_variables': []
            }
        
        # Get matches using flexible matching
        matches = self.find_matching_variables(variables)
        
        valid_variables = []
        invalid_variables = []
        match_details = {}
        
        for var, match in matches.items():
            if match:
                valid_variables.append(match)
                if var != match:
                    match_details[var] = match
            else:
                invalid_variables.append(var)
        
        # Remove duplicates while preserving order
        seen = set()
        valid_variables = [x for x in valid_variables if not (x in seen or seen.add(x))]
        
        is_valid = len(valid_variables) >= 2  # Need at least 2 variables for analysis
        
        return {
            'is_valid': is_valid,
            'message': 'Variables validated successfully' if is_valid else 'Not enough valid variables',
            'valid_variables': valid_variables,
            'invalid_variables': invalid_variables,
            'match_details': match_details
        }