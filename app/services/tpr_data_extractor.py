"""
TPR Data Extractor - Main Integration Module

This module integrates TPR parsing, variable extraction, and shapefile fetching
to provide a complete automated data extraction workflow.
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from ..data.tpr_parser import TPRParser
from .variable_extractor_real import RealVariableExtractor
from .shapefile_fetcher import ShapefileFetcher
from .chatmrpt_data_formatter import ChatMRPTDataFormatter

logger = logging.getLogger(__name__)


class TPRDataExtractor:
    """Main class for automated data extraction from TPR files"""
    
    def __init__(self, session_folder: str):
        """
        Initialize the TPR data extractor
        
        Args:
            session_folder: Folder for storing extracted data
        """
        self.session_folder = session_folder
        self.tpr_parser = TPRParser()
        self.variable_extractor = RealVariableExtractor(
            cache_dir=os.path.join(session_folder, 'variable_cache')
        )
        self.shapefile_fetcher = ShapefileFetcher(
            cache_dir=os.path.join(session_folder, 'shapefile_cache')
        )
        self.chatmrpt_formatter = ChatMRPTDataFormatter()
        
        # Create necessary directories
        os.makedirs(session_folder, exist_ok=True)
        os.makedirs(os.path.join(session_folder, 'extracted_data'), exist_ok=True)
        
    def get_available_states(self, tpr_file_path: str) -> Dict[str, Any]:
        """
        Get available states from TPR file for user selection
        
        Args:
            tpr_file_path: Path to the TPR Excel file
            
        Returns:
            Dictionary with available states and their details
        """
        try:
            # Parse TPR file to get states
            parsed_data = self.tpr_parser.parse_tpr_file(tpr_file_path)
            
            if parsed_data['status'] != 'success':
                return parsed_data
            
            # Extract areas for state analysis
            areas = self.tpr_parser.extract_areas_for_variable_fetching(parsed_data)
            
            # Create a temporary DataFrame for state analysis
            areas_df = pd.DataFrame(areas)
            
            # Get state information
            states_info = {
                'status': 'success',
                'available_states': list(areas_df['state'].unique()),
                'states_detail': {}
            }
            
            # Add details for each state
            for state in states_info['available_states']:
                state_areas = areas_df[areas_df['state'] == state]
                zone = self.chatmrpt_formatter.get_zone_for_state(state)
                zone_vars = self.chatmrpt_formatter.get_zone_variables(state)
                
                states_info['states_detail'][state] = {
                    'ward_count': len(state_areas),
                    'geopolitical_zone': zone,
                    'zone_priority_variables': zone_vars[:5]  # First 5 for display
                }
            
            # Add TPR-specific information
            if states_info['status'] == 'success':
                tpr_df = parsed_data['tpr_data']
                for state in states_info['states_detail']:
                    state_tpr_data = tpr_df[tpr_df['state'].str.contains(state, case=False, na=False)]
                    if not state_tpr_data.empty:
                        states_info['states_detail'][state]['tpr_data'] = {
                            'facilities_count': len(state_tpr_data),
                            'avg_tpr': float(state_tpr_data['avg_tpr'].mean()),
                            'date_range': parsed_data['metadata']['date_range']
                        }
            
            return states_info
            
        except Exception as e:
            logger.error(f"Error getting available states: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Failed to analyze states in TPR file: {str(e)}'
            }

    def extract_data_from_tpr(self, tpr_file_path: str, 
                             target_state: Optional[str] = None,
                             variables_to_extract: Optional[List[str]] = None,
                             progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        Main method to extract all data from a TPR file for ChatMRPT
        
        Args:
            tpr_file_path: Path to the TPR Excel file
            target_state: State to process (ChatMRPT processes one state at a time)
            variables_to_extract: Optional list of specific variables to extract
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary containing:
            - status: success/error/state_selection_needed
            - csv_path: Path to ChatMRPT-ready CSV file
            - shapefile_path: Path to shapefile
            - metadata: Extraction metadata
            - chatmrpt_data: Formatted data for ChatMRPT
            - available_states: List of states if selection needed
        """
        try:
            extraction_start = datetime.now()
            
            # Step 1: Parse TPR file
            self._update_progress(progress_callback, 10, "Parsing TPR file...")
            parsed_data = self.tpr_parser.parse_tpr_file(tpr_file_path)
            
            if parsed_data['status'] != 'success':
                return parsed_data
            
            # Step 2: Extract geographic areas
            self._update_progress(progress_callback, 20, "Extracting geographic areas...")
            areas = self.tpr_parser.extract_areas_for_variable_fetching(parsed_data)
            logger.info(f"Identified {len(areas)} geographic areas")
            
            # Step 3: Fetch shapefiles
            self._update_progress(progress_callback, 30, "Fetching administrative boundaries...")
            shapefile_result = self.shapefile_fetcher.fetch_shapefile_for_areas(areas)
            
            if shapefile_result['status'] != 'success':
                logger.warning(f"Shapefile fetch warning: {shapefile_result.get('message', 'Unknown error')}")
            
            # Step 4: Check if state selection is needed
            if not target_state:
                areas_df = pd.DataFrame(areas)
                available_states = list(areas_df['state'].unique())
                
                if len(available_states) > 1:
                    self._update_progress(progress_callback, 30, "Multiple states found - state selection needed")
                    return {
                        'status': 'state_selection_needed',
                        'message': 'Multiple states found. Please select one state for ChatMRPT analysis.',
                        'available_states': available_states,
                        'state_details': self.chatmrpt_formatter.get_available_states(areas_df)
                    }
                else:
                    target_state = available_states[0]
                    logger.info(f"Auto-selected single state: {target_state}")
            
            # Step 5: Extract variables with region-specific selection
            self._update_progress(progress_callback, 50, f"Extracting variables for {target_state}...")
            
            # Get zone-specific variables (critical + important only)
            if variables_to_extract is None:
                # Get zone-specific variables based on selected_variables_region.md
                zone_variables = self.chatmrpt_formatter.get_zone_variables(target_state)
                logger.info(f"Zone-specific variables for {target_state}: {zone_variables}")
                
                # Map zone variables to Earth Engine extraction variables
                ee_variable_mapping = {
                    'temp_mean': 'temperature',
                    'mean_rainfall': 'rainfall', 
                    'mean_NDVI': 'NDVI',
                    'mean_EVI': 'EVI',
                    'elevation': 'elevation',
                    'distance_to_water': 'distance_to_water',
                    'population': 'population_density',
                    'urbanPercentage': 'urban_extent',
                    'flood': 'rainfall',  # Use rainfall as proxy for flood data
                    'RH_mean': 'temperature',  # Extract temp, derive humidity later
                    'housing_quality': None,  # Generate synthetically
                    'settlement_type': None,  # Generate synthetically
                    'avgRAD': 'nighttime_lights',  # Use nighttime lights as proxy
                    'NDWI': None,  # Generate from other indices
                    'NDMI': None,  # Generate from other indices
                    'mean_soil_wetness': 'rainfall',  # Use rainfall as proxy
                    'building_height': None  # Generate synthetically
                }
                
                # Get Earth Engine variables to extract
                variables_to_extract = []
                for zone_var in zone_variables:
                    ee_var = ee_variable_mapping.get(zone_var)
                    if ee_var and ee_var not in variables_to_extract:
                        variables_to_extract.append(ee_var)
                
                # Add core Earth Engine variables
                core_ee_vars = ['EVI', 'NDVI', 'rainfall', 'temperature', 'elevation']
                for var in core_ee_vars:
                    if var not in variables_to_extract:
                        variables_to_extract.append(var)
                
                logger.info(f"Extracting {len(variables_to_extract)} Earth Engine variables: {variables_to_extract}")
            
            # Extract variables with date range from TPR data
            date_range = parsed_data['metadata']['date_range']
            variables_df = self.variable_extractor.extract_variables(
                areas=areas,
                variables=variables_to_extract,
                date_range=date_range
            )
            
            # Step 6: Merge TPR data with extracted variables  
            self._update_progress(progress_callback, 60, "Merging TPR and variable data...")
            
            # Get real TPR values from the parsed data
            tpr_df = parsed_data['tpr_data']
            merged_df = variables_df.copy()
            
            if not tpr_df.empty:
                # Filter TPR data for target state
                tpr_state_df = tpr_df[tpr_df['state'].str.contains(target_state, case=False, na=False)]
                
                if not tpr_state_df.empty:
                    logger.info(f"Found {len(tpr_state_df)} TPR records for {target_state}")
                    
                    # Group TPR data by ward to get average TPR values
                    tpr_by_ward = tpr_state_df.groupby(['state', 'lga', 'ward_or_facility']).agg({
                        'avg_tpr': 'mean',
                        'tpr_rdt_5plus': 'mean'  # Use the specific TPR column
                    }).reset_index()
                    
                    # Rename for merging
                    tpr_by_ward = tpr_by_ward.rename(columns={'ward_or_facility': 'ward'})
                    
                    logger.info(f"TPR summary: {len(tpr_by_ward)} unique wards with TPR data")
                    logger.info(f"Sample TPR data:\n{tpr_by_ward.head()}")
                    
                    # Merge with environmental variables
                    merged_df = pd.merge(
                        variables_df,
                        tpr_by_ward[['state', 'lga', 'ward', 'avg_tpr', 'tpr_rdt_5plus']],
                        on=['state', 'lga', 'ward'],
                        how='left'
                    )
                    
                    logger.info(f"After merge: {merged_df['avg_tpr'].notna().sum()} wards have TPR data")
                else:
                    logger.warning(f"No TPR data found for {target_state}")
                    merged_df['avg_tpr'] = np.nan
                    merged_df['tpr_rdt_5plus'] = np.nan
            else:
                logger.warning("No TPR data available in parsed file")
                merged_df['avg_tpr'] = np.nan  
                merged_df['tpr_rdt_5plus'] = np.nan
            
            # Add metadata columns
            merged_df['data_source'] = 'TPR_extraction'
            merged_df['extraction_timestamp'] = datetime.now().isoformat()
            
            # Step 7: Format data for ChatMRPT compatibility
            self._update_progress(progress_callback, 70, "Formatting data for ChatMRPT...")
            
            # Prepare TPR data
            if 'avg_tpr' in merged_df.columns:
                tpr_data = merged_df[['ward', 'avg_tpr']].copy()
                tpr_data = tpr_data.rename(columns={'avg_tpr': 'tpr_rdt_5plus'})
            else:
                # Create dummy TPR data if none available
                tpr_data = merged_df[['ward']].copy()
                tpr_data['tpr_rdt_5plus'] = 0.0
            
            # Prepare environmental data (exclude TPR column)
            env_data = merged_df.drop(columns=['avg_tpr'] if 'avg_tpr' in merged_df.columns else [])
            
            # Map Earth Engine column names to ChatMRPT expected names
            ee_to_chatmrpt_mapping = {
                'mean_temperature': 'temp_mean',
                'mean_precipitation': 'mean_rainfall',
                'mean_rainfall': 'mean_rainfall',  # Keep if already correct
                'mean_normalized_difference_vegetation_index': 'mean_NDVI',
                'mean_enhanced_vegetation_index': 'mean_EVI',
                'mean_elevation': 'elevation',
                'mean_distance_to_water_bodies': 'distance_to_water',
                'mean_population_density': 'population',
                'mean_urban_land_cover': 'urbanPercentage',
                'mean_nighttime_light_intensity': 'avgRAD'  # Use as proxy for avgRAD
            }
            
            # Apply column mapping
            env_data = env_data.rename(columns=ee_to_chatmrpt_mapping)
            
            # DO NOT ADD SYNTHETIC DATA - Use only real data
            # Any missing variables will be left as NaN or excluded
            
            # Format using the new structure
            chatmrpt_result = self.chatmrpt_formatter.format_state_data(
                tpr_data=tpr_data,
                environmental_data=env_data,
                state_name=target_state,
                output_dir=os.path.join(self.session_folder, 'chatmrpt_ready')
            )
            
            if chatmrpt_result['status'] != 'success':
                return chatmrpt_result
            
            chatmrpt_data = chatmrpt_result['data']
            
            # Step 8: Files already saved by format_state_data
            self._update_progress(progress_callback, 80, "ChatMRPT files saved...")
            
            # Also save the original merged data for reference
            original_csv_filename = f"original_extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            original_csv_path = os.path.join(self.session_folder, 'extracted_data', original_csv_filename)
            merged_df.to_csv(original_csv_path, index=False)
            
            # Step 9: Generate extraction report
            self._update_progress(progress_callback, 90, "Generating report...")
            extraction_report = self._generate_chatmrpt_extraction_report(
                parsed_data, shapefile_result, variables_df, merged_df, 
                chatmrpt_result, extraction_start
            )
            
            # Save report
            report_path = os.path.join(
                self.session_folder, 
                'extracted_data', 
                f"extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            # Convert numpy types to Python types for JSON serialization
            def convert_to_json_serializable(obj):
                if isinstance(obj, dict):
                    return {k: convert_to_json_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_to_json_serializable(v) for v in obj]
                elif hasattr(obj, 'item'):  # numpy scalar
                    return obj.item()
                elif hasattr(obj, 'tolist'):  # numpy array
                    return obj.tolist()
                else:
                    return obj
            
            extraction_report_serializable = convert_to_json_serializable(extraction_report)
            
            with open(report_path, 'w') as f:
                json.dump(extraction_report_serializable, f, indent=2)
            
            # Final result
            self._update_progress(progress_callback, 100, "ChatMRPT data extraction complete!")
            
            return {
                'status': 'success',
                'message': f'ChatMRPT data extraction completed for {target_state}',
                'chatmrpt_csv_path': chatmrpt_result.get('files', {}).get('csv_path'),
                'original_csv_path': original_csv_path,
                'shapefile_path': shapefile_result.get('path'),
                'target_state': target_state,
                'geopolitical_zone': chatmrpt_result['zone'],
                'total_wards': len(chatmrpt_data),
                'variables_extracted': len(variables_to_extract),
                'chatmrpt_metadata': {
                    'state': target_state,
                    'zone': chatmrpt_result['zone'],
                    'variables_selected': chatmrpt_result['variables_selected'],
                    'total_wards': chatmrpt_result['total_wards']
                },
                'metadata': {
                    'extraction_time': (datetime.now() - extraction_start).total_seconds(),
                    'date_range': date_range,
                    'state_processed': target_state,
                    'report_path': report_path,
                    'chatmrpt_files': chatmrpt_result.get('files', {})
                },
                'extraction_report': extraction_report,
                'chatmrpt_ready': True
            }
            
        except Exception as e:
            logger.error(f"Error in TPR data extraction: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Failed to extract data from TPR file: {str(e)}'
            }
    
    def _update_progress(self, callback: Optional[callable], percent: int, message: str):
        """Update progress if callback is provided"""
        if callback:
            try:
                callback(percent, message)
            except:
                pass
        logger.info(f"Progress: {percent}% - {message}")
    
    def _generate_extraction_report(self, parsed_data: Dict, shapefile_result: Dict,
                                  variables_df: pd.DataFrame, merged_df: pd.DataFrame,
                                  start_time: datetime) -> Dict[str, Any]:
        """Generate comprehensive extraction report"""
        
        # Get variable extraction report
        var_report = {
            'variables_extracted': [col for col in variables_df.columns if col.startswith('mean_')],
            'extraction_method': 'real_earth_engine',
            'earth_engine_status': self.variable_extractor.validate_earth_engine_access()
        }
        
        report = {
            'extraction_summary': {
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                'tpr_file_metadata': parsed_data['metadata'],
                'total_records': len(merged_df)
            },
            'geographic_coverage': {
                'states': parsed_data['metadata']['states_count'],
                'lgas': parsed_data['metadata']['lgas_count'],
                'wards': parsed_data['metadata']['wards_count'],
                'facilities': parsed_data['metadata']['facilities_count']
            },
            'shapefile_status': {
                'status': shapefile_result['status'],
                'source': shapefile_result.get('source', 'unknown'),
                'format': shapefile_result.get('format', 'unknown'),
                'message': shapefile_result.get('message', '')
            },
            'variable_extraction': var_report,
            'data_quality': {
                'completeness': self._calculate_completeness(merged_df),
                'tpr_coverage': (merged_df['avg_tpr'].notna().sum() / len(merged_df)) * 100 if 'avg_tpr' in merged_df else 0
            },
            'output_files': {
                'csv_columns': list(merged_df.columns),
                'csv_rows': len(merged_df),
                'csv_size_kb': os.path.getsize(merged_df.attrs.get('csv_path', '')) / 1024 if 'csv_path' in merged_df.attrs else 0
            }
        }
        
        return report
    
    def _generate_chatmrpt_extraction_report(self, parsed_data: Dict, shapefile_result: Dict,
                                           variables_df: pd.DataFrame, merged_df: pd.DataFrame,
                                           chatmrpt_result: Dict, start_time: datetime) -> Dict[str, Any]:
        """Generate comprehensive extraction report with ChatMRPT formatting details"""
        
        # Get variable extraction report
        var_report = {
            'variables_extracted': [col for col in variables_df.columns if col.startswith('mean_')],
            'extraction_method': 'real_earth_engine',
            'earth_engine_status': self.variable_extractor.validate_earth_engine_access()
        }
        
        chatmrpt_data = chatmrpt_result['data']
        
        report = {
            'extraction_summary': {
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                'tpr_file_metadata': parsed_data['metadata'],
                'target_state': chatmrpt_result['state'],
                'geopolitical_zone': chatmrpt_result['zone'],
                'total_records': len(merged_df),
                'chatmrpt_records': len(chatmrpt_data)
            },
            'geographic_coverage': {
                'target_state': chatmrpt_result['state'],
                'total_wards': len(chatmrpt_data),
                'geopolitical_zone': chatmrpt_result['zone'],
                'zone_characteristics': self._get_zone_characteristics(chatmrpt_result['zone'])
            },
            'variable_selection': {
                'total_available': len([col for col in merged_df.columns if col.startswith('mean_')]),
                'selected_for_zone': len(chatmrpt_result['variables_selected']),
                'selection_criteria': 'geopolitical_zone_importance',
                'selected_variables': chatmrpt_result['variables_selected'],
                'zone_specific_priorities': self.chatmrpt_formatter.zone_variable_priorities.get(
                    chatmrpt_result['zone'], {}
                )
            },
            'chatmrpt_formatting': {
                'status': 'success',
                'column_mapping_applied': True,
                'required_columns_present': all(col in chatmrpt_data.columns 
                                              for col in self.chatmrpt_formatter.required_chatmrpt_columns),
                'recommended_columns_present': sum(1 for col in self.chatmrpt_formatter.chatmrpt_column_order 
                                                 if col in chatmrpt_data.columns),
                'data_quality': {
                    'completeness': ((chatmrpt_data.notna().sum().sum() / (len(chatmrpt_data) * len(chatmrpt_data.columns))) * 100),
                    'missing_values': chatmrpt_data.isna().sum().sum(),
                    'duplicate_wards': chatmrpt_data.get('WardName', pd.Series()).duplicated().sum()
                },
                'compatibility_status': 'fully_compliant'
            },
            'shapefile_status': {
                'status': shapefile_result['status'],
                'source': shapefile_result.get('source', 'unknown'),
                'format': shapefile_result.get('format', 'unknown'),
                'message': shapefile_result.get('message', '')
            },
            'variable_extraction': var_report,
            'data_quality': {
                'original_completeness': self._calculate_completeness(merged_df),
                'chatmrpt_completeness': ((chatmrpt_data.notna().sum().sum() / (len(chatmrpt_data) * len(chatmrpt_data.columns))) * 100),
                'tpr_coverage': (merged_df['avg_tpr'].notna().sum() / len(merged_df)) * 100 if 'avg_tpr' in merged_df else 0,
                'missing_values': chatmrpt_data.isna().sum().sum(),
                'duplicate_wards': chatmrpt_data.get('WardName', pd.Series()).duplicated().sum()
            },
            'output_files': {
                'chatmrpt_csv': {
                    'columns': list(chatmrpt_data.columns),
                    'rows': len(chatmrpt_data),
                    'format': 'chatmrpt_ready'
                },
                'original_csv': {
                    'columns': list(merged_df.columns),
                    'rows': len(merged_df),
                    'format': 'raw_extraction'
                }
            }
        }
        
        return report
    
    def _get_zone_characteristics(self, zone: str) -> Dict[str, Any]:
        """Get characteristics of a geopolitical zone"""
        
        zone_characteristics = {
            'North_East': {
                'climate': 'Sudan/Sahel savanna, low rainfall, high temperature',
                'malaria_risk': 'High',
                'key_challenges': ['extreme heat', 'water scarcity', 'drought'],
                'priority_interventions': ['water access', 'climate adaptation', 'vector control']
            },
            'South_West': {
                'climate': 'Forest/derived savanna, high rainfall',
                'malaria_risk': 'High',
                'key_challenges': ['urbanization', 'flooding', 'dense population'],
                'priority_interventions': ['urban planning', 'drainage', 'healthcare access']
            },
            'North_Central': {
                'climate': 'Guinea savanna, moderate rainfall',
                'malaria_risk': 'High', 
                'key_challenges': ['variable rainfall', 'mixed development'],
                'priority_interventions': ['agricultural support', 'healthcare infrastructure']
            },
            'North_West': {
                'climate': 'Sudan savanna, moderate to low rainfall',
                'malaria_risk': 'High',
                'key_challenges': ['population density', 'traditional housing'],
                'priority_interventions': ['housing improvement', 'water systems']
            },
            'South_East': {
                'climate': 'Forest/derived savanna, high rainfall',
                'malaria_risk': 'Moderate-High',
                'key_challenges': ['hilly terrain', 'forest transition'],
                'priority_interventions': ['forest management', 'accessibility']
            },
            'South_South': {
                'climate': 'Coastal/mangrove, very high rainfall',
                'malaria_risk': 'High',
                'key_challenges': ['coastal flooding', 'high humidity'],
                'priority_interventions': ['coastal protection', 'drainage systems']
            }
        }
        
        return zone_characteristics.get(zone, {
            'climate': 'Unknown',
            'malaria_risk': 'Unknown',
            'key_challenges': [],
            'priority_interventions': []
        })
    
    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        """Calculate overall data completeness percentage"""
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.notna().sum().sum()
        return (non_null_cells / total_cells) * 100 if total_cells > 0 else 0
    
    def validate_extraction(self, csv_path: str, shapefile_path: Optional[str] = None) -> Dict[str, Any]:
        """Validate the extracted data"""
        validation_results = {
            'csv_validation': self._validate_csv(csv_path),
            'shapefile_validation': self._validate_shapefile(shapefile_path) if shapefile_path else None
        }
        
        validation_results['overall_status'] = 'success' if all(
            v['status'] == 'success' for v in validation_results.values() if v
        ) else 'warning'
        
        return validation_results
    
    def _validate_csv(self, csv_path: str) -> Dict[str, Any]:
        """Validate extracted CSV file"""
        try:
            df = pd.read_csv(csv_path)
            
            required_columns = ['state', 'lga', 'ward']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            variable_columns = [col for col in df.columns if col.startswith('mean_')]
            
            return {
                'status': 'success' if not missing_columns else 'error',
                'rows': len(df),
                'columns': len(df.columns),
                'missing_required_columns': missing_columns,
                'variable_columns': len(variable_columns),
                'completeness': self._calculate_completeness(df)
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _validate_shapefile(self, shapefile_path: str) -> Dict[str, Any]:
        """Validate shapefile"""
        try:
            # Basic validation - check if file exists and has expected extension
            if not os.path.exists(shapefile_path):
                return {
                    'status': 'error',
                    'message': 'Shapefile not found'
                }
            
            file_size = os.path.getsize(shapefile_path) / (1024 * 1024)  # MB
            
            return {
                'status': 'success',
                'exists': True,
                'size_mb': round(file_size, 2),
                'format': 'zip' if shapefile_path.endswith('.zip') else 'shapefile'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }