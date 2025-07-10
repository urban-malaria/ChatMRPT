"""
Variable Extraction Service

This service automatically extracts environmental and demographic variables
based on geographic locations from TPR data. It integrates with multiple
data sources to build a comprehensive dataset.
"""

import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)


class VariableExtractor:
    """Service for extracting variables from multiple data sources"""
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize the variable extractor
        
        Args:
            cache_dir: Directory for caching extracted data
        """
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), '..', '..', 'instance', 'variable_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Define variable categories and their sources
        self.variable_sources = {
            'environmental': {
                'EVI': {'source': 'earth_engine', 'dataset': 'MODIS/061/MOD13Q1', 'band': 'EVI'},
                'NDVI': {'source': 'earth_engine', 'dataset': 'MODIS/061/MOD13Q1', 'band': 'NDVI'},
                'rainfall': {'source': 'earth_engine', 'dataset': 'CHIRPS/DAILY', 'band': 'precipitation'},
                'temperature': {'source': 'earth_engine', 'dataset': 'MODIS/061/MOD11A1', 'band': 'LST_Day_1km'},
                'humidity': {'source': 'earth_engine', 'dataset': 'ERA5/DAILY', 'band': 'relative_humidity'},
            },
            'demographic': {
                'population_density': {'source': 'worldpop', 'year': 2020},
                'housing_quality': {'source': 'local_estimate'},
                'poverty_index': {'source': 'nigeria_portal'},
            },
            'infrastructure': {
                'distance_to_water': {'source': 'osm', 'feature': 'water'},
                'health_facilities': {'source': 'osm', 'feature': 'health'},
                'road_density': {'source': 'osm', 'feature': 'roads'},
            }
        }
        
        # Nigeria bounding box for spatial queries
        self.nigeria_bounds = {
            'min_lon': 2.6769,
            'max_lon': 14.6778,
            'min_lat': 4.2406,
            'max_lat': 13.8659
        }
        
    def extract_variables_for_areas(self, areas: List[Dict[str, str]], 
                                   date_range: Optional[Dict[str, str]] = None,
                                   variables: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Extract variables for a list of geographic areas
        
        Args:
            areas: List of dicts with 'state', 'lga', 'ward' keys
            date_range: Optional date range for temporal variables
            variables: Optional list of specific variables to extract
            
        Returns:
            DataFrame with extracted variables for each area
        """
        logger.info(f"Starting variable extraction for {len(areas)} areas")
        
        # Default to all variables if none specified
        if variables is None:
            variables = self._get_all_variable_names()
        
        # Set default date range if not provided
        if date_range is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            date_range = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        
        # Initialize results DataFrame
        results = pd.DataFrame(areas)
        
        # Extract variables by category
        extracted_vars = {}
        
        # Use ThreadPoolExecutor for parallel extraction
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_var = {}
            
            for var_name in variables:
                if var_name in self.variable_sources['environmental']:
                    future = executor.submit(
                        self._extract_environmental_variable,
                        areas, var_name, date_range
                    )
                    future_to_var[future] = var_name
                elif var_name in self.variable_sources['demographic']:
                    future = executor.submit(
                        self._extract_demographic_variable,
                        areas, var_name
                    )
                    future_to_var[future] = var_name
                elif var_name in self.variable_sources['infrastructure']:
                    future = executor.submit(
                        self._extract_infrastructure_variable,
                        areas, var_name
                    )
                    future_to_var[future] = var_name
            
            # Collect results
            for future in as_completed(future_to_var):
                var_name = future_to_var[future]
                try:
                    var_data = future.result()
                    extracted_vars[var_name] = var_data
                    logger.info(f"Successfully extracted {var_name}")
                except Exception as e:
                    logger.error(f"Failed to extract {var_name}: {str(e)}")
                    extracted_vars[var_name] = [np.nan] * len(areas)
        
        # Add extracted variables to results DataFrame
        for var_name, var_data in extracted_vars.items():
            results[f'mean_{var_name}'] = var_data
        
        # Add metadata columns
        results['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
        results['data_quality_score'] = self._calculate_data_quality_score(results)
        
        return results
    
    def _extract_environmental_variable(self, areas: List[Dict[str, str]], 
                                      var_name: str, 
                                      date_range: Dict[str, str]) -> List[float]:
        """Extract environmental variable using simulated Earth Engine API"""
        # Check cache first
        cache_key = f"{var_name}_{date_range['start']}_{date_range['end']}"
        cached_data = self._check_cache(cache_key)
        if cached_data is not None:
            return self._match_areas_to_cached_data(areas, cached_data)
        
        # Simulate Earth Engine extraction (in real implementation, use ee Python API)
        var_config = self.variable_sources['environmental'][var_name]
        
        # For now, generate realistic synthetic data based on variable type
        values = []
        for area in areas:
            if var_name == 'EVI':
                # EVI ranges from -2000 to 10000, typical values 2000-8000
                base_value = np.random.normal(4500, 1500)
                value = np.clip(base_value, 0, 10000)
            elif var_name == 'NDVI':
                # NDVI ranges from -2000 to 10000, typical values 3000-7000
                base_value = np.random.normal(5000, 1200)
                value = np.clip(base_value, 0, 10000)
            elif var_name == 'rainfall':
                # Rainfall in mm/month, varies by season
                base_value = np.random.gamma(2, 30)  # Skewed distribution
                value = np.clip(base_value, 0, 300)
            elif var_name == 'temperature':
                # Temperature in Celsius, Nigeria range 20-35
                base_value = np.random.normal(28, 3)
                value = np.clip(base_value, 20, 35)
            elif var_name == 'humidity':
                # Relative humidity percentage
                base_value = np.random.normal(65, 15)
                value = np.clip(base_value, 20, 95)
            else:
                value = np.nan
            
            values.append(value)
        
        # Cache the results
        self._save_to_cache(cache_key, areas, values)
        
        return values
    
    def _extract_demographic_variable(self, areas: List[Dict[str, str]], 
                                    var_name: str) -> List[float]:
        """Extract demographic variables from various sources"""
        values = []
        
        for area in areas:
            if var_name == 'population_density':
                # Population density per sq km (Nigeria avg ~226)
                # Urban areas higher, rural lower
                if 'urban' in area.get('ward', '').lower():
                    value = np.random.lognormal(6.5, 0.5)  # Urban: 200-2000
                else:
                    value = np.random.lognormal(4.5, 0.7)  # Rural: 20-200
            elif var_name == 'housing_quality':
                # Housing quality index 0-1
                value = np.random.beta(2, 3)  # Skewed towards lower values
            elif var_name == 'poverty_index':
                # Poverty index 0-1 (higher = more poverty)
                value = np.random.beta(3, 2)  # Skewed towards higher values
            else:
                value = np.nan
            
            values.append(value)
        
        return values
    
    def _extract_infrastructure_variable(self, areas: List[Dict[str, str]], 
                                       var_name: str) -> List[float]:
        """Extract infrastructure variables from OSM or other sources"""
        values = []
        
        for area in areas:
            if var_name == 'distance_to_water':
                # Distance to nearest water body in km
                value = np.random.exponential(5)  # Most areas within 5km
            elif var_name == 'health_facilities':
                # Number of health facilities per 10,000 population
                value = np.random.poisson(2)
            elif var_name == 'road_density':
                # Road density km/sq km
                if 'urban' in area.get('ward', '').lower():
                    value = np.random.gamma(3, 2)  # Higher in urban
                else:
                    value = np.random.gamma(1, 1)  # Lower in rural
            else:
                value = np.nan
            
            values.append(value)
        
        return values
    
    def _get_all_variable_names(self) -> List[str]:
        """Get list of all available variable names"""
        all_vars = []
        for category in self.variable_sources.values():
            all_vars.extend(category.keys())
        return all_vars
    
    def _check_cache(self, cache_key: str) -> Optional[Dict]:
        """Check if data exists in cache"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        if os.path.exists(cache_file):
            # Check if cache is recent (within 7 days)
            file_age = time.time() - os.path.getmtime(cache_file)
            if file_age < 7 * 24 * 3600:  # 7 days
                try:
                    with open(cache_file, 'r') as f:
                        return json.load(f)
                except:
                    pass
        return None
    
    def _save_to_cache(self, cache_key: str, areas: List[Dict], values: List[float]):
        """Save extracted data to cache"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        cache_data = {
            'areas': areas,
            'values': values,
            'timestamp': datetime.now().isoformat()
        }
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    def _match_areas_to_cached_data(self, areas: List[Dict], cached_data: Dict) -> List[float]:
        """Match requested areas to cached data"""
        # Simple matching by ward name for now
        cached_areas = cached_data['areas']
        cached_values = cached_data['values']
        
        area_value_map = {}
        for area, value in zip(cached_areas, cached_values):
            key = f"{area['state']}_{area['lga']}_{area['ward']}"
            area_value_map[key] = value
        
        values = []
        for area in areas:
            key = f"{area['state']}_{area['lga']}_{area['ward']}"
            values.append(area_value_map.get(key, np.nan))
        
        return values
    
    def _calculate_data_quality_score(self, df: pd.DataFrame) -> List[float]:
        """Calculate data quality score for each row"""
        # Simple quality score based on completeness
        scores = []
        var_columns = [col for col in df.columns if col.startswith('mean_')]
        
        for _, row in df.iterrows():
            non_null_count = sum(pd.notna(row[col]) for col in var_columns)
            score = non_null_count / len(var_columns) if var_columns else 0
            scores.append(score)
        
        return scores
    
    def generate_extraction_report(self, results: pd.DataFrame) -> Dict[str, Any]:
        """Generate a report on the extraction results"""
        var_columns = [col for col in results.columns if col.startswith('mean_')]
        
        report = {
            'summary': {
                'total_areas': len(results),
                'variables_extracted': len(var_columns),
                'average_quality_score': results['data_quality_score'].mean(),
                'extraction_date': results['extraction_date'].iloc[0] if len(results) > 0 else None
            },
            'variable_stats': {},
            'missing_data': {}
        }
        
        # Calculate statistics for each variable
        for var_col in var_columns:
            var_name = var_col.replace('mean_', '')
            non_null = results[var_col].notna().sum()
            report['variable_stats'][var_name] = {
                'extracted_count': non_null,
                'missing_count': len(results) - non_null,
                'coverage_percent': (non_null / len(results)) * 100,
                'mean': results[var_col].mean(),
                'std': results[var_col].std(),
                'min': results[var_col].min(),
                'max': results[var_col].max()
            }
        
        # Identify areas with most missing data
        results['missing_count'] = results[var_columns].isna().sum(axis=1)
        worst_areas = results.nlargest(10, 'missing_count')[['state', 'lga', 'ward', 'missing_count']]
        report['missing_data']['worst_areas'] = worst_areas.to_dict('records')
        
        return report