"""
System Tools for ChatMRPT - Data and Session Management

These tools provide information about data availability, session status,
available variables, and ward information for the current session.
"""

import logging
from typing import Dict, Any, Optional, List
from flask import current_app
import pandas as pd

logger = logging.getLogger(__name__)


def check_data_availability(session_id: str) -> Dict[str, Any]:
    """Check what data is available in the current session."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        
        # Try to load unified dataset
        unified_gdf = load_unified_dataset(session_id)
        
        if unified_gdf is not None:
            # Analyze unified dataset
            total_records = len(unified_gdf)
            total_variables = len(unified_gdf.columns)
            
            # Categorize variables
            numeric_vars = list(unified_gdf.select_dtypes(include=['number']).columns)
            categorical_vars = list(unified_gdf.select_dtypes(include=['object', 'category']).columns)
            
            # Check for key analysis columns
            has_composite_analysis = any('composite' in col.lower() for col in unified_gdf.columns)
            has_pca_analysis = any('pca' in col.lower() for col in unified_gdf.columns)
            has_geographic_data = 'geometry' in unified_gdf.columns or any('lat' in col.lower() for col in unified_gdf.columns)
            
            # Identify health indicators
            health_indicators = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['tpr', 'malaria', 'prevalence', 'case', 'incidence'])]
            
            # Identify environmental variables
            environmental_vars = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['ndvi', 'temperature', 'rainfall', 'elevation', 'precipitation'])]
            
            # Identify demographic variables
            demographic_vars = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['population', 'density', 'urban', 'rural', 'literacy'])]
            
            # Check data quality
            missing_data_summary = {}
            for col in unified_gdf.columns[:20]:  # Limit to first 20 columns for performance
                missing_count = unified_gdf[col].isnull().sum()
                if missing_count > 0:
                    missing_data_summary[col] = {
                        'missing_count': int(missing_count),
                        'missing_percentage': float(missing_count / total_records * 100)
                    }
            
            return {
                'status': 'success',
                'message': 'Data availability check completed',
                'data_available': True,
                'data_source': 'unified_dataset',
                'summary': {
                    'total_records': total_records,
                    'total_variables': total_variables,
                    'numeric_variables': len(numeric_vars),
                    'categorical_variables': len(categorical_vars)
                },
                'variable_categories': {
                    'health_indicators': health_indicators[:5],  # Top 5
                    'environmental_variables': environmental_vars[:5],
                    'demographic_variables': demographic_vars[:5],
                    'numeric_variables': numeric_vars[:10],
                    'categorical_variables': categorical_vars[:10]
                },
                'analysis_status': {
                    'has_composite_analysis': has_composite_analysis,
                    'has_pca_analysis': has_pca_analysis,
                    'has_geographic_data': has_geographic_data
                },
                'data_quality': {
                    'variables_with_missing_data': len(missing_data_summary),
                    'missing_data_summary': missing_data_summary
                }
            }
        
        # Fallback to DataHandler (data_service IS the DataHandler)
        data_service = current_app.services.data_service
        
        if data_service and hasattr(data_service, 'csv_data') and data_service.csv_data is not None:
            df = data_service.csv_data
            return {
                'status': 'success',
                'message': 'Data availability check completed (using DataHandler)',
                'data_available': True,
                'data_source': 'data_handler',
                'summary': {
                    'total_records': len(df),
                    'total_variables': len(df.columns),
                    'numeric_variables': len(df.select_dtypes(include=['number']).columns),
                    'categorical_variables': len(df.select_dtypes(include=['object', 'category']).columns)
                },
                'analysis_status': {
                    'has_composite_analysis': False,
                    'has_pca_analysis': False,
                    'has_geographic_data': False
                }
            }
        
        return {
            'status': 'error',
            'message': 'No data available in current session',
            'data_available': False,
            'data_source': None
        }
        
    except Exception as e:
        logger.error(f"Error checking data availability: {e}")
        return {
            'status': 'error',
            'message': f'Error checking data availability: {str(e)}',
            'data_available': False
        }


def get_session_status(session_id: str) -> Dict[str, Any]:
    """Get comprehensive status of the current session."""
    try:
        # Check session folder existence
        import os
        session_folder = f"instance/uploads/{session_id}"
        session_exists = os.path.exists(session_folder)
        
        if not session_exists:
            return {
                'status': 'success',
                'session_status': 'new_session',
                'message': 'New session - ready to receive data. Please upload CSV and shapefile data.',
                'session_exists': False,
                'session_id': session_id,
                'csv_loaded': False,
                'shapefile_loaded': False,
                'analysis_complete': False,
                'can_run_analysis': False,
                'available_actions': ['upload_data', 'explain_concept']
            }
        
        # Get data availability
        data_status = check_data_availability(session_id)
        
        # Check for uploaded files
        uploaded_files = []
        if session_exists:
            try:
                for file in os.listdir(session_folder):
                    if file.endswith(('.csv', '.xlsx', '.shp', '.geojson')):
                        file_path = os.path.join(session_folder, file)
                        file_size = os.path.getsize(file_path)
                        uploaded_files.append({
                            'filename': file,
                            'size_bytes': file_size,
                            'size_mb': round(file_size / (1024 * 1024), 2)
                        })
            except Exception as e:
                logger.warning(f"Error reading session folder: {e}")
        
        # Check for visualizations
        viz_folder = os.path.join(session_folder, 'visualizations')
        visualizations = []
        if os.path.exists(viz_folder):
            try:
                for file in os.listdir(viz_folder):
                    if file.endswith('.html'):
                        visualizations.append({
                            'filename': file,
                            'web_path': f"/static/visualizations/{session_id}/{file}"
                        })
            except Exception as e:
                logger.warning(f"Error reading visualizations folder: {e}")
        
        # Session metadata
        session_metadata = {
            'session_id': session_id,
            'session_exists': session_exists,
            'uploaded_files_count': len(uploaded_files),
            'visualizations_count': len(visualizations),
            'data_available': data_status.get('data_available', False)
        }
        
        return {
            'status': 'success',
            'message': 'Session status retrieved successfully',
            'session_metadata': session_metadata,
            'uploaded_files': uploaded_files,
            'visualizations': visualizations,
            'data_status': data_status
        }
        
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        return {
            'status': 'error',
            'message': f'Error getting session status: {str(e)}',
            'session_exists': False
        }


def get_available_variables(session_id: str, category: str = 'all') -> Dict[str, Any]:
    """Get list of available variables in the dataset, optionally filtered by category."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available',
                'variables': []
            }
        
        all_variables = list(unified_gdf.columns)
        
        # Categorize variables
        variable_categories = {
            'numeric': list(unified_gdf.select_dtypes(include=['number']).columns),
            'categorical': list(unified_gdf.select_dtypes(include=['object', 'category']).columns),
            'health': [col for col in unified_gdf.columns if any(term in col.lower() for term in ['tpr', 'malaria', 'prevalence', 'case', 'incidence'])],
            'environmental': [col for col in unified_gdf.columns if any(term in col.lower() for term in ['ndvi', 'temperature', 'rainfall', 'elevation', 'precipitation'])],
            'demographic': [col for col in unified_gdf.columns if any(term in col.lower() for term in ['population', 'density', 'urban', 'rural', 'literacy'])],
            'infrastructure': [col for col in unified_gdf.columns if any(term in col.lower() for term in ['road', 'health_facility', 'school', 'market'])],
            'analysis_result': [col for col in unified_gdf.columns if any(term in col.lower() for term in ['composite', 'pca', 'score', 'rank', 'vulnerability'])]
        }
        
        # Filter by category if specified
        if category.lower() != 'all' and category.lower() in variable_categories:
            selected_variables = variable_categories[category.lower()]
            message = f'Retrieved {len(selected_variables)} {category} variables'
        else:
            selected_variables = all_variables
            message = f'Retrieved all {len(selected_variables)} variables'
        
        # Add variable details
        variable_details = []
        for var in selected_variables[:50]:  # Limit to 50 for performance
            var_info = {
                'name': var,
                'type': str(unified_gdf[var].dtype),
                'non_null_count': int(unified_gdf[var].count()),
                'null_count': int(unified_gdf[var].isnull().sum())
            }
            
            # Add summary stats for numeric variables
            if unified_gdf[var].dtype in ['number']:
                var_info.update({
                    'mean': float(unified_gdf[var].mean()),
                    'min': float(unified_gdf[var].min()),
                    'max': float(unified_gdf[var].max()),
                    'std': float(unified_gdf[var].std())
                })
            else:
                # For categorical variables, add unique values info
                unique_count = unified_gdf[var].nunique()
                var_info.update({
                    'unique_values': int(unique_count),
                    'top_values': unified_gdf[var].value_counts().head(3).to_dict() if unique_count <= 20 else {}
                })
            
            variable_details.append(var_info)
        
        return {
            'status': 'success',
            'message': message,
            'category': category,
            'total_variables': len(selected_variables),
            'variables': selected_variables,
            'variable_details': variable_details,
            'variable_categories': {k: len(v) for k, v in variable_categories.items()}
        }
        
    except Exception as e:
        logger.error(f"Error getting available variables: {e}")
        return {
            'status': 'error',
            'message': f'Error getting variables: {str(e)}',
            'variables': []
        }


def get_ward_information(session_id: str, ward_name: str = None, limit: int = 10) -> Dict[str, Any]:
    """Get information about specific wards or list wards in the dataset."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available',
                'wards': []
            }
        
        # Try to identify ward column
        ward_columns = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['ward', 'lga', 'district', 'area', 'locality'])]
        
        if not ward_columns:
            return {
                'status': 'error',
                'message': 'No ward/area identifier column found in dataset',
                'available_columns': list(unified_gdf.columns)[:10]
            }
        
        ward_col = ward_columns[0]
        
        if ward_name:
            # Get specific ward information with detailed analysis
            ward_data = unified_gdf[unified_gdf[ward_col].str.contains(ward_name, case=False, na=False)]
            
            if len(ward_data) == 0:
                return {
                    'status': 'error',
                    'message': f'Ward "{ward_name}" not found in dataset',
                    'available_wards': list(unified_gdf[ward_col].unique())[:20]
                }
            
            # Get ward details
            ward_info = ward_data.iloc[0].to_dict()
            
            # Extract specific risk factors and rankings
            risk_factors = {}
            
            # Environmental factors
            env_vars = ['distance_to_water', 'flood', 'mean_rainfall', 'temp_mean', 'elevation', 'NDMI', 'avgRAD', 'RH_mean']
            for var in env_vars:
                if var in ward_info and pd.notna(ward_info[var]):
                    risk_factors[f'environmental_{var}'] = float(ward_info[var])
            
            # Health indicators
            health_vars = ['pfpr', 'u5_tpr_rdt', 'housing_quality']
            for var in health_vars:
                if var in ward_info and pd.notna(ward_info[var]):
                    risk_factors[f'health_{var}'] = float(ward_info[var])
            
            # Demographic factors
            demo_vars = ['population_density', 'urbanArea', 'totalArea']
            for var in demo_vars:
                if var in ward_info and pd.notna(ward_info[var]):
                    risk_factors[f'demographic_{var}'] = float(ward_info[var])
            
            # Ranking information
            ranking_info = {}
            
            # Composite ranking
            if 'composite_rank' in ward_info and pd.notna(ward_info['composite_rank']):
                ranking_info['composite_rank'] = int(ward_info['composite_rank'])
                total_wards = len(unified_gdf)
                ranking_info['composite_percentile'] = round((1 - (ranking_info['composite_rank'] / total_wards)) * 100, 1)
            
            if 'composite_score' in ward_info and pd.notna(ward_info['composite_score']):
                ranking_info['composite_score'] = round(float(ward_info['composite_score']), 3)
            
            # PCA ranking
            if 'pca_rank' in ward_info and pd.notna(ward_info['pca_rank']):
                ranking_info['pca_rank'] = int(ward_info['pca_rank'])
                ranking_info['pca_percentile'] = round((1 - (ranking_info['pca_rank'] / total_wards)) * 100, 1)
            
            if 'pca_score' in ward_info and pd.notna(ward_info['pca_score']):
                ranking_info['pca_score'] = round(float(ward_info['pca_score']), 3)
            
            # Risk categorization
            vulnerability_category = 'Unknown'
            if 'vulnerability_category' in ward_info and pd.notna(ward_info['vulnerability_category']):
                vulnerability_category = str(ward_info['vulnerability_category'])
            
            # Clean up the full info - convert numpy types to Python types
            cleaned_info = {}
            for key, value in ward_info.items():
                if pd.isna(value):
                    cleaned_info[key] = None
                elif hasattr(value, 'item'):  # numpy scalar
                    cleaned_info[key] = value.item()
                else:
                    cleaned_info[key] = value
            
            return {
                'status': 'success',
                'message': f'Detailed analysis retrieved for: {ward_name}',
                'ward_found': True,
                'ward_name': ward_name,
                'ward_column': ward_col,
                'risk_factors': risk_factors,
                'ranking_info': ranking_info,
                'vulnerability_category': vulnerability_category,
                'total_wards_in_dataset': len(unified_gdf),
                'ward_data': cleaned_info,
                'total_matches': len(ward_data)
            }
        
        else:
            # List all wards
            unique_wards = unified_gdf[ward_col].unique()
            ward_list = [ward for ward in unique_wards if pd.notna(ward)][:limit]
            
            # Get summary stats for wards
            ward_summary = []
            for ward in ward_list:
                ward_data = unified_gdf[unified_gdf[ward_col] == ward]
                
                # Calculate summary for key indicators
                health_vars = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['tpr', 'malaria', 'prevalence'])]
                
                summary = {
                    'ward_name': ward,
                    'record_count': len(ward_data)
                }
                
                # Add health indicator if available
                if health_vars:
                    health_var = health_vars[0]
                    if unified_gdf[health_var].dtype in ['number']:
                        health_values = ward_data[health_var].dropna()
                        if len(health_values) > 0:
                            summary[f'{health_var}_mean'] = float(health_values.mean())
                
                # Add composite score if available
                composite_vars = [col for col in unified_gdf.columns if 'composite' in col.lower() and 'score' in col.lower()]
                if composite_vars:
                    comp_var = composite_vars[0]
                    if unified_gdf[comp_var].dtype in ['number']:
                        comp_values = ward_data[comp_var].dropna()
                        if len(comp_values) > 0:
                            summary[f'{comp_var}_mean'] = float(comp_values.mean())
                
                ward_summary.append(summary)
            
            return {
                'status': 'success',
                'message': f'Listed {len(ward_list)} wards (limit: {limit})',
                'ward_column': ward_col,
                'total_wards': len(unique_wards),
                'wards_returned': len(ward_list),
                'ward_list': ward_list,
                'ward_summary': ward_summary
            }
        
    except Exception as e:
        logger.error(f"Error getting ward information: {e}")
        return {
            'status': 'error',
            'message': f'Error getting ward information: {str(e)}',
            'wards': []
        }


def get_ward_variable_value(session_id: str, ward_name: str, variable_name: str) -> Dict[str, Any]:
    """
    Get the value of a specific variable for a specific ward.
    
    Args:
        session_id: The session ID
        ward_name: Name of the ward to query
        variable_name: Name of the variable to retrieve
        
    Returns:
        Dictionary containing the variable value and metadata
    """
    try:
        # Get data service
        from .. import get_app
        app = get_app()
        data_service = app.services.data
        
        # Get unified dataset
        unified_gdf = data_service.get_unified_dataset(session_id)
        
        if unified_gdf is None or unified_gdf.empty:
            return {
                'status': 'error',
                'message': 'No data available for analysis. Please upload data first.'
            }
        
        # Get ward column
        ward_col = None
        for col in ['ward', 'Ward', 'WARD', 'ward_name', 'Ward_Name', 'WARD_NAME']:
            if col in unified_gdf.columns:
                ward_col = col
                break
        
        if not ward_col:
            return {
                'status': 'error',
                'message': 'No ward column found in the dataset'
            }
        
        # Check if variable exists
        if variable_name not in unified_gdf.columns:
            return {
                'status': 'error',
                'message': f'Variable "{variable_name}" not found in dataset',
                'available_variables': list(unified_gdf.columns)
            }
        
        # Find the ward
        ward_data = unified_gdf[unified_gdf[ward_col].str.lower() == ward_name.lower()]
        
        if ward_data.empty:
            # Try partial match
            ward_data = unified_gdf[unified_gdf[ward_col].str.contains(ward_name, case=False, na=False)]
        
        if ward_data.empty:
            return {
                'status': 'error',
                'message': f'Ward "{ward_name}" not found in dataset',
                'available_wards': unified_gdf[ward_col].unique().tolist()[:10]  # Show first 10
            }
        
        # Get the value
        if len(ward_data) > 1:
            # Multiple matches, use first and warn
            ward_info = ward_data.iloc[0]
            warning = f'Multiple wards matched "{ward_name}", using first match: {ward_info[ward_col]}'
        else:
            ward_info = ward_data.iloc[0]
            warning = None
        
        value = ward_info[variable_name]
        
        # Clean up the value - convert numpy types to Python types
        if pd.isna(value):
            clean_value = None
        elif hasattr(value, 'item'):  # numpy scalar
            clean_value = value.item()
        else:
            clean_value = value
        
        result = {
            'status': 'success',
            'ward_name': ward_info[ward_col],
            'variable_name': variable_name,
            'value': clean_value,
            'data_type': str(type(clean_value).__name__)
        }
        
        if warning:
            result['warning'] = warning
            
        return result
        
    except Exception as e:
        logger.error(f"Error getting ward variable value: {e}")
        return {
            'status': 'error',
            'message': f'Error getting ward variable value: {str(e)}'
        } 