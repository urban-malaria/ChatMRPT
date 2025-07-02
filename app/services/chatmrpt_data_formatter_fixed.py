"""
ChatMRPT Data Formatter - Exact Column Structure Mapping

This service formats TPR and Earth Engine data to match the exact structure expected by ChatMRPT,
with zone-specific variable selection based on geopolitical zones and actual CSV column names.

Updated based on analysis of Kano_plus.csv and Niger_plus.csv to ensure exact compatibility.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import os
import json

logger = logging.getLogger(__name__)


class ChatMRPTDataFormatter:
    """Formats TPR and environmental data for ChatMRPT analysis with exact column structure"""
    
    def __init__(self):
        # Map states to geopolitical zones
        self.geopolitical_zones = {
            'North_Central': ['Benue', 'Kogi', 'Kwara', 'Nasarawa', 'Niger', 'Plateau', 'FCT'],
            'North_East': ['Adamawa', 'Bauchi', 'Borno', 'Gombe', 'Taraba', 'Yobe'],
            'North_West': ['Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Sokoto', 'Zamfara'],
            'South_East': ['Abia', 'Anambra', 'Ebonyi', 'Enugu', 'Imo'],
            'South_South': ['Akwa Ibom', 'Bayelsa', 'Cross River', 'Delta', 'Edo', 'Rivers'],
            'South_West': ['Ekiti', 'Lagos', 'Ogun', 'Ondo', 'Osun', 'Oyo']
        }
        
        # Critical mapping: conceptual variable names from selected_variables_region.md 
        # to actual ChatMRPT CSV column names (based on Kano_plus.csv/Niger_plus.csv)
        self.variable_name_mapping = {
            # Conceptual name -> ChatMRPT column name
            'temperature': 'temp_mean',
            'temp_mean': 'temp_mean',
            'mean_rainfall': 'mean_rainfall', 
            'rainfall': 'mean_rainfall',
            'precipitation': 'mean_rainfall',
            'pfpr': 'pfpr',
            'drought_index': 'flood',  # Inverse relationship - low flood = high drought
            'mean_NDVI': 'mean_NDVI',
            'NDVI': 'mean_NDVI',
            'mean_EVI': 'mean_EVI',
            'EVI': 'mean_EVI',
            'distance_to_water': 'distance_to_water',
            'settlement_type': 'settlement_type',
            'housing_quality': 'housing_quality',
            'elevation': 'elevation',
            'mean_soil_wetness': 'mean_soil_wetness',
            'RH_mean': 'RH_mean',
            'humidity': 'RH_mean',
            'flood': 'flood',
            'urban_extent': 'urbanPercentage',  # Key mapping!
            'population_density': 'population',  # Available in Niger data
            'avgRAD': 'avgRAD',
            'NDWI': 'NDWI',
            'NDMI': 'NDMI',
            'livestock_density': None,  # Not available in ChatMRPT format
            'coastal_proximity': None,  # Not available in ChatMRPT format
            'building_height': 'building_height'
        }
        
        # Zone-specific variable priorities (Critical and Important only, as requested)
        self.zone_variable_priorities = {
            'North_Central': {
                'critical': ['mean_rainfall', 'pfpr', 'temp_mean', 'flood', 'mean_NDVI'],
                'important': ['housing_quality', 'distance_to_water', 'settlement_type']
            },
            'North_East': {
                'critical': ['temp_mean', 'mean_rainfall', 'pfpr', 'flood', 'mean_NDVI'],  # drought_index mapped to flood
                'important': ['distance_to_water', 'settlement_type']
            },
            'North_West': {
                'critical': ['mean_rainfall', 'temp_mean', 'pfpr', 'mean_EVI', 'mean_NDVI'],
                'important': ['housing_quality', 'settlement_type', 'population']
            },
            'South_East': {
                'critical': ['flood', 'mean_rainfall', 'pfpr', 'RH_mean', 'mean_EVI'],
                'important': ['housing_quality', 'urbanPercentage', 'population']
            },
            'South_South': {
                'critical': ['flood', 'RH_mean', 'pfpr', 'mean_rainfall'],
                'important': ['urbanPercentage', 'housing_quality', 'NDWI', 'mean_soil_wetness']
            },
            'South_West': {
                'critical': ['urbanPercentage', 'flood', 'pfpr', 'avgRAD', 'housing_quality'],
                'important': ['mean_rainfall', 'population', 'RH_mean', 'mean_EVI']
            }
        }
        
        # Required ChatMRPT columns for proper functionality
        self.required_chatmrpt_columns = [
            'WardName',  # Primary identifier
            'u5_tpr_rdt',  # TPR data
            'pfpr'  # Always needed for malaria analysis
        ]
        
        # EXACT ChatMRPT column structure (from Kano_plus.csv) - MUST maintain this order
        self.chatmrpt_column_order = [
            'X.1', 'X', 'WardName', 'StateCode', 'WardCode', 'LGACode',
            'Urban', 'Source', 'Timestamp', 'GlobalID', 'AMAPCODE',
            'mean_EVI', 'mean_NDVI', 'mean_rainfall', 'distance_to_water',
            'RH_mean', 'temp_mean', 'housing_quality', 'pfpr', 'avgRAD',
            'flood', 'NDWI', 'NDMI', 'elevation', 'mean_soil_wetness',
            'settlement_type', 'u5_tpr_rdt', 'totalArea', 'urbanArea',
            'urbanPercentage', 'building_height'
        ]

    def get_zone_for_state(self, state_name: str) -> str:
        """Get geopolitical zone for a state"""
        for zone, states in self.geopolitical_zones.items():
            if state_name in states:
                return zone
        return 'Unknown'
    
    def get_zone_variables(self, state_name: str) -> List[str]:
        """Get critical and important variables for a state's zone"""
        zone = self.get_zone_for_state(state_name)
        if zone == 'Unknown':
            # Default to a comprehensive set if zone unknown
            return ['temp_mean', 'mean_rainfall', 'pfpr', 'mean_NDVI', 'flood']
        
        zone_priorities = self.zone_variable_priorities.get(zone, {})
        critical_vars = zone_priorities.get('critical', [])
        important_vars = zone_priorities.get('important', [])
        
        # Combine critical and important, remove duplicates while preserving order
        selected_vars = critical_vars + [var for var in important_vars if var not in critical_vars]
        
        # Map conceptual names to actual column names and filter out unavailable ones
        mapped_vars = []
        for var in selected_vars:
            mapped_var = self.variable_name_mapping.get(var, var)
            if mapped_var and mapped_var not in mapped_vars:
                mapped_vars.append(mapped_var)
        
        # Do NOT add structural columns (WardName, u5_tpr_rdt) to variables list
        # These are handled separately as identifiers and data columns, not analysis variables
        
        return mapped_vars

    def format_for_chatmrpt(self, tpr_data: pd.DataFrame, 
                           environmental_data: pd.DataFrame,
                           state_name: str) -> Dict[str, Any]:
        """Format combined data for ChatMRPT analysis with exact column structure"""
        try:
            # Get zone-specific variables
            zone_variables = self.get_zone_variables(state_name)
            zone = self.get_zone_for_state(state_name)
            
            logger.info(f"Formatting data for {state_name} ({zone} zone)")
            logger.info(f"Zone variables: {zone_variables}")
            
            # Merge TPR and environmental data on ward identifiers
            # Try different possible ward column names
            ward_columns = ['ward', 'WardName', 'ward_name']
            tpr_ward_col = None
            env_ward_col = None
            
            for col in ward_columns:
                if col in tpr_data.columns:
                    tpr_ward_col = col
                    break
            
            for col in ward_columns:
                if col in environmental_data.columns:
                    env_ward_col = col
                    break
            
            if not tpr_ward_col or not env_ward_col:
                return {
                    'status': 'error',
                    'message': 'Could not find ward identifier columns for merging'
                }
            
            # Merge data
            merged_data = pd.merge(
                tpr_data, 
                environmental_data, 
                left_on=tpr_ward_col, 
                right_on=env_ward_col, 
                how='inner'
            )
            
            logger.info(f"Merged data shape: {merged_data.shape}")
            
            # Standardize column names
            if tpr_ward_col != 'WardName':
                merged_data = merged_data.rename(columns={tpr_ward_col: 'WardName'})
            if env_ward_col != 'WardName' and env_ward_col in merged_data.columns:
                merged_data = merged_data.drop(columns=[env_ward_col])
            
            # Add TPR data as u5_tpr_rdt column (ChatMRPT standard)
            tpr_columns = ['tpr_rdt_5plus', 'tpr_u5', 'tpr']
            for tpr_col in tpr_columns:
                if tpr_col in merged_data.columns:
                    merged_data['u5_tpr_rdt'] = merged_data[tpr_col]
                    break
            
            # Select only the zone-specific variables that are available
            available_zone_vars = []
            for var in zone_variables:
                if var in merged_data.columns:
                    available_zone_vars.append(var)
                else:
                    logger.warning(f"Zone variable '{var}' not found in data")
            
            # Add required ChatMRPT columns if available and not already included
            for col in self.required_chatmrpt_columns:
                if col in merged_data.columns and col not in available_zone_vars:
                    available_zone_vars.append(col)
            
            # Ensure we have required columns
            missing_required = [col for col in self.required_chatmrpt_columns if col not in available_zone_vars]
            if missing_required:
                logger.warning(f"Missing required columns: {missing_required}")
            
            # Create final dataset with only selected variables
            final_columns = [col for col in available_zone_vars if col in merged_data.columns]
            formatted_data = merged_data[final_columns].copy()
            
            # Convert numpy types to native Python types for JSON serialization
            formatted_data = self._convert_numpy_types(formatted_data)
            
            return {
                'status': 'success',
                'data': formatted_data,
                'zone': zone,
                'variables_selected': final_columns,
                'zone_variables_requested': zone_variables,
                'variables_available': len(final_columns),
                'total_wards': len(formatted_data),
                'state': state_name,
                'message': f'Formatted {len(formatted_data)} wards for {state_name} ({zone} zone) with {len(final_columns)} variables'
            }
            
        except Exception as e:
            logger.error(f"Error formatting data for ChatMRPT: {e}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error formatting data: {str(e)}'
            }

    def format_state_data(self, tpr_data: pd.DataFrame, 
                         environmental_data: pd.DataFrame, 
                         state_name: str,
                         output_dir: str = None) -> Dict[str, Any]:
        """
        Format and save state data for ChatMRPT analysis
        
        Args:
            tpr_data: DataFrame with TPR data by ward
            environmental_data: DataFrame with environmental variables by ward
            state_name: Name of the state to process
            output_dir: Directory to save formatted files
            
        Returns:
            Dictionary with results and file paths
        """
        try:
            # Format the data
            result = self.format_for_chatmrpt(tpr_data, environmental_data, state_name)
            
            if result['status'] != 'success':
                return result
            
            # Generate StateCode and other identifiers
            result['data'] = self._add_chatmrpt_identifiers(result['data'], state_name)
            
            # Save files if output directory provided
            if output_dir:
                file_paths = self._save_chatmrpt_files(result, output_dir, state_name)
                result['files'] = file_paths
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing state data: {e}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error processing state data: {str(e)}'
            }

    def _add_chatmrpt_identifiers(self, df: pd.DataFrame, state_name: str) -> pd.DataFrame:
        """Add ChatMRPT-required identifier columns using REAL data only"""
        
        # DO NOT generate fake administrative codes
        # Use real StateCode if available, otherwise derive from state name
        if 'StateCode' not in df.columns:
            state_abbrev = {
                'Kano': 'KN', 'Niger': 'NI', 'Lagos': 'LA', 'Osun': 'OS',
                'Adamawa': 'AD', 'Kwara': 'KW', 'Borno': 'BO', 'Kaduna': 'KD', 'Oyo': 'OY'
            }
            df['StateCode'] = state_abbrev.get(state_name, state_name[:2].upper())
        
        # DO NOT generate fake WardCode or LGACode - leave blank if not available
        # These should come from real administrative boundary data
        if 'WardCode' not in df.columns:
            df['WardCode'] = ''
        if 'LGACode' not in df.columns:
            df['LGACode'] = ''
        
        # Add basic metadata columns only if missing
        if 'Urban' not in df.columns:
            if 'urbanPercentage' in df.columns:
                df['Urban'] = df['urbanPercentage'].apply(lambda x: 'Yes' if x > 50 else 'No')
            else:
                df['Urban'] = 'No'  # Default to No if no urban data
        
        if 'Source' not in df.columns:
            df['Source'] = 'TPR_Extraction'
        
        # Add required index columns
        if 'X.1' not in df.columns:
            df['X.1'] = range(1, len(df) + 1)
        if 'X' not in df.columns:
            df['X'] = range(1, len(df) + 1)
        
        return df

    def _save_chatmrpt_files(self, result: Dict[str, Any], output_dir: str, state_name: str) -> Dict[str, str]:
        """Save formatted data in ChatMRPT-ready format with EXACT column ordering"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Enforce EXACT ChatMRPT column ordering
        df = result['data'].copy()
        
        # Create ordered dataframe with only available columns in correct order
        ordered_columns = []
        for col in self.chatmrpt_column_order:
            if col in df.columns:
                ordered_columns.append(col)
        
        # Reorder dataframe to match ChatMRPT structure exactly
        df_ordered = df[ordered_columns]
        
        # Save main CSV file in ChatMRPT format with correct column order
        csv_filename = f"{state_name}_plus.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        df_ordered.to_csv(csv_path, index=False)
        
        logger.info(f"Saved {state_name}_plus.csv with {len(ordered_columns)} columns in correct order")
        logger.info(f"Column order: {ordered_columns[:5]}... (showing first 5)")
        
        # Save metadata
        metadata_filename = f"{state_name}_chatmrpt_metadata.json"
        metadata_path = os.path.join(output_dir, metadata_filename)
        
        metadata = {
            'state': state_name,
            'zone': result['zone'],
            'total_wards': result['total_wards'],
            'variables_selected': result['variables_selected'],
            'zone_variables_requested': result['zone_variables_requested'],
            'extraction_date': pd.Timestamp.now().isoformat(),
            'data_source': 'TPR_Earth_Engine_Extraction',
            'chatmrpt_ready': True
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        # Save variable selection report
        report_filename = f"{state_name}_variable_report.txt"
        report_path = os.path.join(output_dir, report_filename)
        
        with open(report_path, 'w') as f:
            f.write(f"ChatMRPT Variable Selection Report\n")
            f.write(f"==================================\n\n")
            f.write(f"State: {state_name}\n")
            f.write(f"Geopolitical Zone: {result['zone']}\n")
            f.write(f"Total Wards: {result['total_wards']}\n")
            f.write(f"Variables Available: {result['variables_available']}\n\n")
            
            f.write(f"Zone-Specific Variables (Critical + Important):\n")
            for var in result['zone_variables_requested']:
                status = "✅ Available" if var in result['variables_selected'] else "❌ Missing"
                f.write(f"  {var}: {status}\n")
            
            f.write(f"\nFinal Dataset Columns:\n")
            for col in result['variables_selected']:
                f.write(f"  - {col}\n")
        
        logger.info(f"Saved ChatMRPT files for {state_name} to {output_dir}")
        
        return {
            'csv_path': csv_path,
            'metadata_path': metadata_path,
            'report_path': report_path
        }

    def _convert_numpy_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert numpy types to native Python types for JSON serialization"""
        for col in df.columns:
            if df[col].dtype == 'object':
                continue
            df[col] = df[col].apply(
                lambda x: x.item() if hasattr(x, 'item') else x
            )
        return df

    def validate_chatmrpt_compatibility(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate that data is compatible with ChatMRPT requirements"""
        
        validation_results = {
            'compatible': True,
            'issues': [],
            'warnings': [],
            'requirements_met': {}
        }
        
        # Check required columns
        for req_col in self.required_chatmrpt_columns:
            if req_col in df.columns:
                validation_results['requirements_met'][req_col] = True
            else:
                validation_results['compatible'] = False
                validation_results['issues'].append(f"Missing required column: {req_col}")
                validation_results['requirements_met'][req_col] = False
        
        # Check data types
        if 'WardName' in df.columns:
            if not df['WardName'].dtype == 'object':
                validation_results['warnings'].append("WardName should be string/object type")
        
        # Check for duplicate wards
        if 'WardName' in df.columns:
            duplicates = df['WardName'].duplicated().sum()
            if duplicates > 0:
                validation_results['issues'].append(f"Found {duplicates} duplicate ward names")
                validation_results['compatible'] = False
        
        # Check data completeness
        missing_data = df.isnull().sum().sum()
        total_cells = len(df) * len(df.columns)
        completeness = ((total_cells - missing_data) / total_cells) * 100
        
        if completeness < 80:
            validation_results['warnings'].append(f"Data completeness is {completeness:.1f}% (below 80%)")
        
        validation_results['data_quality'] = {
            'completeness_percent': completeness,
            'total_missing_values': int(missing_data),
            'total_wards': len(df),
            'total_variables': len(df.columns)
        }
        
        return validation_results


def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization"""
    if hasattr(obj, 'item'):  # numpy scalar
        return obj.item()
    elif hasattr(obj, 'tolist'):  # numpy array
        return obj.tolist()
    return obj