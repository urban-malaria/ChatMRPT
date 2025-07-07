"""
TPR Data Extractor - Clean Version for Convergence Workflow

This module processes TPR files and creates only the convergence format files:
- {statename}_plus.csv 
- {statename}_state.zip
"""

import logging
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..data.tpr_parser import TPRParser
from .variable_extractor_real import RealVariableExtractor
from .shapefile_fetcher import ShapefileFetcher

logger = logging.getLogger(__name__)


class TPRDataExtractor:
    """Clean TPR data extractor for convergence workflow only"""
    
    def __init__(self, session_folder: str):
        """Initialize the TPR data extractor"""
        self.session_folder = session_folder
        self.tpr_parser = TPRParser()
        self.variable_extractor = RealVariableExtractor(
            cache_dir=os.path.join(session_folder, 'variable_cache')
        )
        self.shapefile_fetcher = ShapefileFetcher(
            cache_dir=os.path.join(session_folder, 'shapefile_cache')
        )
        
        # Add caching for parsed TPR data to prevent triple parsing
        self._parsed_data_cache = {}
        self._areas_cache = {}
        
        # Create necessary directories
        os.makedirs(session_folder, exist_ok=True)
        
    def get_available_states(self, tpr_file_path: str) -> Dict[str, Any]:
        """Get available states from TPR file for user selection - WITH CACHING"""
        try:
            # Check cache first to prevent re-parsing
            cache_key = f"{tpr_file_path}_{os.path.getmtime(tpr_file_path)}"
            
            if cache_key in self._parsed_data_cache:
                logger.info("✅ Using cached TPR data (avoiding re-parse)")
                parsed_data = self._parsed_data_cache[cache_key]
                areas = self._areas_cache[cache_key]
            else:
                logger.info("🔄 Parsing TPR file for first time")
                parsed_data = self.tpr_parser.parse_tpr_file(tpr_file_path)
                if parsed_data['status'] != 'success':
                    return parsed_data
                
                areas = self.tpr_parser.extract_areas_for_variable_fetching(parsed_data)
                
                # Cache the results to prevent triple parsing
                self._parsed_data_cache[cache_key] = parsed_data
                self._areas_cache[cache_key] = areas
                logger.info(f"💾 Cached TPR data for {len(areas)} areas")
            
            areas_df = pd.DataFrame(areas)
            available_states = sorted(areas_df['state'].unique().tolist())
            
            states_info = {
                'status': 'success',
                'available_states': available_states,
                'total_states': len(available_states),
                'message': f'Found {len(available_states)} states in TPR file'
            }
            
            return states_info
            
        except Exception as e:
            logger.error(f"Error getting available states: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Failed to analyze states in TPR file: {str(e)}'
            }

    def extract_raw_data_for_convergence(self, tpr_file_path: str, selected_state: str = None, 
                                       progress_callback=None) -> Dict[str, Any]:
        """
        Main method: Process TPR file and create convergence files only.
        
        Creates ONLY:
        - {statename}_plus.csv
        - {statename}_state.zip
        """
        try:
            extraction_start = datetime.now()
            
            if progress_callback:
                progress_callback(5, "Starting TPR convergence processing")
            
            # Step 1: Get available states for user selection
            available_states = self.get_available_states(tpr_file_path)
            if available_states['status'] != 'success':
                return available_states
            
            if progress_callback:
                progress_callback(10, f"Found {len(available_states['available_states'])} states")
            
            # Step 2: Handle state selection
            if not selected_state:
                if len(available_states['available_states']) == 1:
                    selected_state = available_states['available_states'][0]
                else:
                    return {
                        'status': 'requires_state_selection',
                        'available_states': available_states['available_states'],
                        'message': 'Multiple states found. Please select one for analysis.'
                    }
            
            if progress_callback:
                progress_callback(15, f"Processing {selected_state}")
            
            # Step 3: Use cached TPR data (already parsed in get_available_states)
            self._update_progress(progress_callback, 20, "Using cached TPR data...")
            cache_key = f"{tpr_file_path}_{os.path.getmtime(tpr_file_path)}"
            
            if cache_key not in self._parsed_data_cache:
                logger.error("Cache miss - this should not happen")
                return {'status': 'error', 'message': 'Internal caching error'}
            
            parsed_data = self._parsed_data_cache[cache_key]
            areas = self._areas_cache[cache_key]
            
            # Step 4: Use cached geographic areas  
            self._update_progress(progress_callback, 30, "Using cached geographic areas...")
            logger.info(f"✅ Using cached {len(areas)} geographic areas (avoiding re-extraction)")
            
            # Step 5: Fetch shapefiles
            self._update_progress(progress_callback, 40, "Fetching administrative boundaries...")
            shapefile_result = self.shapefile_fetcher.fetch_shapefile_for_areas(areas)
            
            # Step 6: Extract ALL environmental variables
            self._update_progress(progress_callback, 50, f"Extracting variables for {selected_state}...")
            
            variables_to_extract = self._get_all_convergence_variables()
            date_range = parsed_data['metadata']['date_range']
            
            variables_df = self.variable_extractor.extract_variables(
                areas=areas,
                variables=variables_to_extract,
                date_range=date_range
            )
            
            # Step 7: Merge TPR data with extracted variables  
            self._update_progress(progress_callback, 70, "Merging TPR and variable data...")
            
            tpr_df = parsed_data['tpr_data']
            merged_df = variables_df.copy()
            
            if not tpr_df.empty:
                # Filter TPR data for target state
                tpr_state_df = tpr_df[tpr_df['state'].str.contains(selected_state, case=False, na=False)]
                
                if not tpr_state_df.empty:
                    logger.info(f"Found {len(tpr_state_df)} TPR records for {selected_state}")
                    
                    # Group TPR data by ward
                    tpr_by_ward = tpr_state_df.groupby(['state', 'lga', 'ward_or_facility']).agg({
                        'avg_tpr': 'mean'
                    }).reset_index()
                    
                    # Rename for merging
                    tpr_by_ward = tpr_by_ward.rename(columns={'ward_or_facility': 'ward'})
                    
                    # Merge with environmental variables
                    merged_df = pd.merge(
                        variables_df,
                        tpr_by_ward[['state', 'lga', 'ward', 'avg_tpr']],
                        on=['state', 'lga', 'ward'],
                        how='left'
                    )
                    
                    logger.info(f"After merge: {merged_df['avg_tpr'].notna().sum()} wards have TPR data")
                else:
                    logger.warning(f"No TPR data found for {selected_state}")
                    merged_df['avg_tpr'] = np.nan
            
            # Step 8: Extract Nigeria shapefile for selected state
            self._update_progress(progress_callback, 80, "Extracting state boundaries...")
            nigeria_shapefile_result = self._extract_nigeria_shapefile_for_state(selected_state)
            
            # Step 9: Create convergence files
            self._update_progress(progress_callback, 90, "Creating convergence files...")
            
            convergence_files = self._create_convergence_files(
                extracted_data=merged_df,
                shapefile_result=nigeria_shapefile_result,
                selected_state=selected_state
            )
            
            if progress_callback:
                progress_callback(100, "TPR convergence complete!")
            
            # Filter merged data for selected state to get accurate ward count
            state_merged_df = merged_df[merged_df['state'].str.contains(selected_state, case=False, na=False)]
            actual_state_wards = len(state_merged_df) if not state_merged_df.empty else len(merged_df)
            
            # Return result
            return {
                'status': 'success',
                'convergence_ready': True,
                'convergence_csv_path': convergence_files['convergence_csv_path'],
                'convergence_shapefile_path': convergence_files.get('convergence_shapefile_path'),
                'state_name': convergence_files['state_name'],
                'selected_state': selected_state,
                'extracted_wards': actual_state_wards,
                'variables_included': len(variables_to_extract),
                'has_shapefile': 'convergence_shapefile_path' in convergence_files,
                'extraction_time': (datetime.now() - extraction_start).total_seconds(),
                'message': f'Convergence complete: {convergence_files["state_name"]}_plus.csv and {convergence_files["state_name"]}_state.zip ready'
            }
            
        except Exception as e:
            logger.error(f"Error in TPR convergence processing: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Failed to process TPR for convergence: {str(e)}'
            }

    def _get_all_convergence_variables(self) -> List[str]:
        """Get complete list of variables for convergence (excluding problematic variables)"""
        return [
            'EVI', 'NDVI', 'rainfall', 'temperature', 'elevation',
            'distance_to_water', 'mean_NDMI', 'mean_NDWI', 'nighttime_lights'
            # Explicitly excluded per user request: 'population_density', 'urban_extent', 'soil_wetness'
        ]
    
    def _extract_nigeria_shapefile_for_state(self, state_name: str) -> Dict[str, Any]:
        """Extract specific state boundaries from Nigeria shapefile"""
        try:
            import zipfile
            import tempfile
            import geopandas as gpd
            import shutil
            
            nigeria_shapefile_path = '/mnt/c/Users/bbofo/OneDrive/Desktop/ChatMRPT/Nigeria_shapefile.zip'
            
            if not os.path.exists(nigeria_shapefile_path):
                return {
                    'status': 'error',
                    'message': f'Nigeria shapefile not found: {nigeria_shapefile_path}'
                }
            
            logger.info(f"Extracting {state_name} boundaries from Nigeria shapefile")
            
            # Extract Nigeria shapefile to temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(nigeria_shapefile_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find the shapefile
                shp_file = None
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.shp') and 'wards' in file.lower():
                            shp_file = os.path.join(root, file)
                            break
                    if shp_file:
                        break
                
                if not shp_file:
                    return {
                        'status': 'error',
                        'message': 'Could not find ward shapefile in Nigeria_shapefile.zip'
                    }
                
                # Load full Nigeria shapefile
                nigeria_gdf = gpd.read_file(shp_file)
                logger.info(f"Loaded Nigeria shapefile: {len(nigeria_gdf)} total wards")
                
                # Filter for selected state
                state_variations = [state_name, state_name.title(), state_name.upper()]
                state_filter = nigeria_gdf['StateName'].isin(state_variations)
                
                if not state_filter.any():
                    # Try partial matching
                    state_filter = nigeria_gdf['StateName'].str.contains(state_name, case=False, na=False)
                
                if not state_filter.any():
                    return {
                        'status': 'error',
                        'message': f'State "{state_name}" not found in Nigeria shapefile',
                        'available_states': sorted(nigeria_gdf['StateName'].unique().tolist())
                    }
                
                # Extract state-specific wards
                state_gdf = nigeria_gdf[state_filter].copy()
                logger.info(f"Extracted {len(state_gdf)} wards for {state_name}")
                
                # Create state shapefile in session folder  
                state_name_clean = state_name.replace(' ', '_').lower()
                state_shapefile_path = os.path.join(self.session_folder, f'{state_name_clean}_state.zip')
                
                # Use temporary directory for shapefile creation
                with tempfile.TemporaryDirectory() as temp_shapefile_dir:
                    logger.info(f"Using temporary directory: {temp_shapefile_dir}")
                    
                    # Save state shapefile to temp directory
                    temp_shp_path = os.path.join(temp_shapefile_dir, f'{state_name_clean}_wards')
                    state_gdf.to_file(temp_shp_path + '.shp')
                    
                    # Create ZIP file
                    import zipfile
                    with zipfile.ZipFile(state_shapefile_path, 'w') as zipf:
                        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                            file_path = temp_shp_path + ext
                            if os.path.exists(file_path):
                                zipf.write(file_path, f'{state_name_clean}_wards{ext}')
                    
                    logger.info(f"✅ Created {state_shapefile_path}")
                
                return {
                    'status': 'success',
                    'path': state_shapefile_path,
                    'extracted_wards': len(state_gdf),
                    'message': f'Successfully extracted {len(state_gdf)} wards for {state_name}'
                }
                
        except Exception as e:
            logger.error(f"Error extracting Nigeria shapefile: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Failed to extract shapefile: {str(e)}'
            }
    
    def _create_convergence_files(self, extracted_data: pd.DataFrame, shapefile_result: Dict[str, Any], 
                                selected_state: str) -> Dict[str, str]:
        """Create convergence files with ALL variables and proper identifiers"""
        try:
            logger.info(f"🔄 Creating convergence files for {selected_state}")
            
            # Step 1: Load Nigeria shapefile to get proper identifiers
            nigeria_shapefile_path = '/mnt/c/Users/bbofo/OneDrive/Desktop/ChatMRPT/Nigeria_shapefile.zip'
            
            if not os.path.exists(nigeria_shapefile_path):
                raise ValueError(f"Nigeria shapefile not found: {nigeria_shapefile_path}")
            
            import zipfile
            import tempfile
            import geopandas as gpd
            
            # Extract shapefile identifiers for the selected state
            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(nigeria_shapefile_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find the shapefile
                shp_file = None
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.shp') and 'wards' in file.lower():
                            shp_file = os.path.join(root, file)
                            break
                    if shp_file:
                        break
                
                if not shp_file:
                    raise ValueError("Could not find ward shapefile in Nigeria_shapefile.zip")
                
                # Load shapefile and filter for selected state
                nigeria_gdf = gpd.read_file(shp_file)
                state_variations = [selected_state, selected_state.title(), selected_state.upper()]
                state_filter = nigeria_gdf['StateName'].isin(state_variations)
                
                if not state_filter.any():
                    state_filter = nigeria_gdf['StateName'].str.contains(selected_state, case=False, na=False)
                
                if not state_filter.any():
                    raise ValueError(f"State '{selected_state}' not found in Nigeria shapefile")
                
                # Get state-specific ward identifiers
                state_gdf = nigeria_gdf[state_filter].copy()
                logger.info(f"📍 Found {len(state_gdf)} wards with identifiers for {selected_state}")
                
                # Step 2: Create base dataframe with shapefile identifiers
                base_columns = ['WardName', 'StateCode', 'WardCode', 'LGACode', 'Urban', 'Source', 'Timestamp', 'GlobalID', 'AMAPCODE']
                
                convergence_data = pd.DataFrame()
                for col in base_columns:
                    if col in state_gdf.columns:
                        convergence_data[col] = state_gdf[col]
                    else:
                        # Fill missing columns with defaults
                        if col == 'Urban':
                            convergence_data[col] = 'No'
                        elif col == 'Source':
                            convergence_data[col] = 'TPR_Convergence'
                        elif col == 'Timestamp':
                            convergence_data[col] = datetime.now().strftime('%Y-%m-%d')
                        else:
                            convergence_data[col] = ''
                
                # Step 3: Merge with extracted data
                convergence_data['WardName_clean'] = convergence_data['WardName'].str.strip().str.title()
                extracted_data_clean = extracted_data.copy()
                
                if 'ward' in extracted_data_clean.columns:
                    extracted_data_clean['WardName_clean'] = extracted_data_clean['ward'].str.strip().str.title()
                elif 'WardName' in extracted_data_clean.columns:
                    extracted_data_clean['WardName_clean'] = extracted_data_clean['WardName'].str.strip().str.title()
                
                # Merge on cleaned ward names
                merged_data = pd.merge(
                    convergence_data, 
                    extracted_data_clean, 
                    on='WardName_clean', 
                    how='left'
                )
                merged_data = merged_data.drop('WardName_clean', axis=1)
                
                # Step 4: Add TPR data
                if 'avg_tpr' not in merged_data.columns:
                    tpr_columns = [col for col in merged_data.columns if 'tpr' in col.lower()]
                    if tpr_columns:
                        merged_data['avg_tpr'] = merged_data[tpr_columns[0]]
                        logger.info(f"💡 Created avg_tpr from {tpr_columns[0]}")
                    else:
                        merged_data['avg_tpr'] = pd.NA
                        logger.warning(f"⚠️ No TPR data found")
                
                # Step 5: Include ALL environmental variables
                identifier_columns = ['WardName', 'StateCode', 'WardCode', 'LGACode']
                base_info_columns = ['Urban', 'Source', 'Timestamp', 'GlobalID', 'AMAPCODE']
                tpr_columns = ['avg_tpr']
                
                # Get ALL environmental variables (excluding problematic ones)
                all_env_columns = [col for col in merged_data.columns if (
                    col.startswith('mean_') or 
                    col.startswith('distance_') or
                    col in ['EVI', 'NDVI', 'rainfall', 'temperature', 'elevation', 'relative_humidity', 
                           'soil_wetness', 'NDMI', 'NDWI', 'nighttime_lights', 'pfpr', 'temp_mean', 
                           'mean_rainfall', 'mean_temperature', 'mean_elevation', 'mean_EVI', 'mean_NDVI', 
                           'mean_NDWI', 'mean_NDMI', 'mean_nighttime_lights', 'mean_soil_wetness']
                ) and col not in identifier_columns + base_info_columns + tpr_columns 
                and col not in ['urban_extent', 'population_density']]  # Exclude problematic variables
                
                # Build final column order: identifiers -> base info -> tpr -> ALL environmental variables
                final_columns = []
                for col in identifier_columns + base_info_columns + tpr_columns:
                    if col in merged_data.columns:
                        final_columns.append(col)
                final_columns.extend(sorted(all_env_columns))
                
                convergence_final = merged_data[final_columns].copy()
                
                logger.info(f"📊 Convergence file includes {len(final_columns)} columns:")
                logger.info(f"   - Identifiers: {[col for col in identifier_columns if col in final_columns]}")
                logger.info(f"   - Environmental vars: {len(all_env_columns)}")
                
                # Step 6: Save convergence files
                state_name_clean = selected_state.replace(' ', '_').lower()
                convergence_csv_path = os.path.join(self.session_folder, f'{state_name_clean}_plus.csv')
                convergence_final.to_csv(convergence_csv_path, index=False)
                
                logger.info(f"✅ Created {state_name_clean}_plus.csv: {len(convergence_final)} wards, {len(convergence_final.columns)} columns")
                
                result = {'convergence_csv_path': convergence_csv_path, 'state_name': state_name_clean}
                
                # Step 7: Copy shapefile if available
                if shapefile_result.get('status') == 'success':
                    result['convergence_shapefile_path'] = shapefile_result['path']
                    logger.info(f"✅ Using {state_name_clean}_state.zip with {shapefile_result['extracted_wards']} wards")
                
                return result
                
        except Exception as e:
            logger.error(f"❌ Error creating convergence files: {e}")
            raise

    def _update_progress(self, callback: Optional[callable], percent: int, message: str):
        """Update progress if callback is provided"""
        if callback:
            try:
                callback(percent, message)
            except:
                pass
        logger.info(f"Progress: {percent}% - {message}")