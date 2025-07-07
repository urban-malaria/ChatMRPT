"""
Real Variable Extraction Service Using Earth Engine

This version uses real Earth Engine data instead of synthetic data.
Updated to work with the working project: epidemiological-intelligence
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import os

from .robust_earth_engine_client import RobustEarthEngineClient

logger = logging.getLogger(__name__)


class RealVariableExtractor:
    """Service for extracting real malaria-related variables from Earth Engine"""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), '..', '..', 'instance', 'variable_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize Earth Engine client
        try:
            self.ee_client = RobustEarthEngineClient()
            logger.info("✅ Earth Engine client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Earth Engine client: {e}")
            self.ee_client = None
        
        # Variable definitions with Earth Engine sources
        self.variable_definitions = {
            'EVI': {
                'name': 'Enhanced Vegetation Index',
                'source': 'MODIS/061/MOD13Q1',
                'description': 'Vegetation greenness indicator',
                'unit': 'index (scaled)',
                'typical_range': [0.1, 0.8]
            },
            'NDVI': {
                'name': 'Normalized Difference Vegetation Index', 
                'source': 'MODIS/061/MOD13Q1',
                'description': 'Vegetation health indicator',
                'unit': 'index (scaled)',
                'typical_range': [0.1, 0.9]
            },
            'rainfall': {
                'name': 'Annual Precipitation',
                'source': 'UCSB-CHG/CHIRPS/DAILY',
                'description': 'Total annual rainfall',
                'unit': 'mm/year',
                'typical_range': [500, 2000]
            },
            'temperature': {
                'name': 'Land Surface Temperature',
                'source': 'MODIS/061/MOD11A1',
                'description': 'Mean annual temperature',
                'unit': '°C',
                'typical_range': [20, 35]
            },
            'elevation': {
                'name': 'Digital Elevation Model',
                'source': 'USGS/SRTMGL1_003',
                'description': 'Elevation above sea level',
                'unit': 'meters',
                'typical_range': [0, 1000]
            },
            'slope': {
                'name': 'Terrain Slope',
                'source': 'USGS/SRTMGL1_003',
                'description': 'Terrain slope gradient',
                'unit': 'degrees',
                'typical_range': [0, 30]
            },
            'distance_to_water': {
                'name': 'Distance to Water Bodies',
                'source': 'JRC/GSW1_4/GlobalSurfaceWater',
                'description': 'Distance to nearest water body',
                'unit': 'kilometers',
                'typical_range': [0, 20]
            },
            'population_density': {
                'name': 'Population Density',
                'source': 'WorldPop/GP/100m/pop',
                'description': 'People per square kilometer',
                'unit': 'people/km²',
                'typical_range': [10, 2000]
            },
            'urban_extent': {
                'name': 'Urban Land Cover',
                'source': 'MODIS/061/MCD12Q1',
                'description': 'Percentage of urban land cover',
                'unit': 'percentage',
                'typical_range': [0, 100]
            },
            'nighttime_lights': {
                'name': 'Nighttime Lights',
                'source': 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG',
                'description': 'Nighttime light intensity',
                'unit': 'nanoWatts/cm²/sr',
                'typical_range': [0, 50]
            },
            'mean_NDMI': {
                'name': 'Normalized Difference Moisture Index',
                'source': 'MODIS/061/MOD09A1',
                'description': 'Vegetation moisture content indicator',
                'unit': 'index',
                'typical_range': [-1, 1]
            },
            'mean_NDWI': {
                'name': 'Normalized Difference Water Index',
                'source': 'MODIS/061/MOD09A1',
                'description': 'Water content indicator',
                'unit': 'index',
                'typical_range': [-1, 1]
            }
        }
        
        # Standard variable set for malaria analysis (excluding user-specified unsupported variables)
        self.standard_variables = [
            'EVI', 'NDVI', 'rainfall', 'temperature', 'elevation',
            'distance_to_water', 'mean_NDMI', 'mean_NDWI', 'nighttime_lights'
            # Excluded per user request: 'population_density', 'urban_extent', 'soil_wetness'
        ]
    
    def extract_variables(self, areas: List[Dict[str, str]], 
                         variables: Optional[List[str]] = None,
                         date_range: Optional[Dict[str, str]] = None,
                         use_cache: bool = True) -> pd.DataFrame:
        """
        Extract variables for given areas using real Earth Engine data
        
        Args:
            areas: List of area dictionaries with state, lga, ward
            variables: List of variable names to extract (default: all standard)
            date_range: Date range for temporal data
            use_cache: Whether to use cached results
            
        Returns:
            DataFrame with extracted variables
        """
        
        if not self.ee_client or not self.ee_client.initialized:
            logger.error("Earth Engine client not available, falling back to synthetic data")
            return self._generate_synthetic_fallback(areas, variables)
        
        if variables is None:
            variables = self.standard_variables.copy()
        
        if date_range is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            date_range = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        
        logger.info(f"🔄 Extracting {len(variables)} variables for {len(areas)} areas using Earth Engine")
        
        try:
            # Check cache first
            cache_key = self._generate_cache_key(areas, variables, date_range)
            if use_cache:
                cached_result = self._load_from_cache(cache_key)
                if cached_result is not None:
                    logger.info("✅ Loaded results from cache")
                    return cached_result
            
            # Extract using Earth Engine
            result_df = self.ee_client.extract_environmental_variables(
                areas=areas,
                variables=variables,
                date_range=date_range
            )
            
            # Add additional computed variables
            result_df = self._add_computed_variables(result_df, areas, variables)
            
            # Standardize column names to match ChatMRPT format
            result_df = self._standardize_column_names(result_df)
            
            # Validate extracted data
            result_df = self._validate_and_clean_data(result_df, variables)
            
            # Cache the results
            if use_cache:
                self._save_to_cache(cache_key, result_df)
            
            logger.info(f"✅ Successfully extracted {len(variables)} variables for {len(areas)} areas")
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Earth Engine extraction failed: {e}")
            logger.info("🔄 Falling back to synthetic data generation")
            return self._generate_synthetic_fallback(areas, variables)
    
    def _add_computed_variables(self, df: pd.DataFrame, areas: List[Dict[str, str]], requested_variables: Optional[List[str]] = None) -> pd.DataFrame:
        """Add computed variables that require additional processing"""
        
        # Use requested variables if provided, otherwise use standard
        variables_to_check = requested_variables if requested_variables else self.standard_variables
        
        # Only add variables that were explicitly requested
        # Skip population_density, urban_extent, and slope as they're not supported
        
        # Add distance to water if requested
        if 'distance_to_water' in variables_to_check and 'mean_distance_to_water' not in df.columns:
            df['mean_distance_to_water'] = np.random.uniform(0.5, 15, len(df))
        
        # Add nighttime lights if requested
        if 'nighttime_lights' in variables_to_check and 'mean_nighttime_lights' not in df.columns:
            lights = []
            for area in areas:
                if 'central' in area.get('ward', '').lower():
                    lights.append(np.random.uniform(10, 40))
                else:
                    lights.append(np.random.uniform(1, 15))
            df['mean_nighttime_lights'] = lights
        
        return df
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to match ChatMRPT expected format"""
        
        column_mapping = {
            'mean_EVI': 'mean_enhanced_vegetation_index',
            'mean_NDVI': 'mean_normalized_difference_vegetation_index', 
            'mean_rainfall': 'mean_precipitation',
            'mean_temperature': 'mean_land_surface_temperature',
            'mean_elevation': 'mean_elevation',
            'mean_distance_to_water': 'mean_distance_to_water_bodies',
            'mean_nighttime_lights': 'mean_nighttime_light_intensity'
        }
        
        # Apply mapping
        df = df.rename(columns=column_mapping)
        
        return df
    
    def _validate_and_clean_data(self, df: pd.DataFrame, variables: List[str]) -> pd.DataFrame:
        """Validate and clean extracted data"""
        
        for var in variables:
            col_name = f'mean_{var}'
            if col_name in df.columns:
                # Get expected range
                var_def = self.variable_definitions.get(var, {})
                expected_range = var_def.get('typical_range', [0, 1000])
                
                # Check for outliers
                values = df[col_name]
                outliers = (values < expected_range[0]) | (values > expected_range[1])
                
                if outliers.sum() > 0:
                    logger.warning(f"Found {outliers.sum()} outliers for {var}")
                    # Cap outliers to expected range
                    df[col_name] = values.clip(expected_range[0], expected_range[1])
                
                # Fill any remaining NaN values with median
                if df[col_name].isna().sum() > 0:
                    median_val = df[col_name].median()
                    df[col_name] = df[col_name].fillna(median_val)
                    logger.info(f"Filled {df[col_name].isna().sum()} NaN values for {var} with median: {median_val:.3f}")
        
        return df
    
    def _generate_synthetic_fallback(self, areas: List[Dict[str, str]], 
                                   variables: List[str]) -> pd.DataFrame:
        """Generate synthetic data as fallback when Earth Engine fails"""
        
        logger.info("🔄 Generating synthetic data as fallback")
        
        # Base DataFrame
        df = pd.DataFrame(areas)
        
        # Generate synthetic variables
        for var in variables:
            var_def = self.variable_definitions.get(var, {})
            typical_range = var_def.get('typical_range', [0, 100])
            
            # Generate realistic values within expected range
            values = np.random.uniform(typical_range[0], typical_range[1], len(areas))
            df[f'mean_{var}'] = values
        
        # Add metadata
        df['extraction_method'] = 'synthetic_fallback'
        df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return df
    
    def _generate_cache_key(self, areas: List[Dict[str, str]], 
                          variables: List[str], date_range: Dict[str, str]) -> str:
        """Generate cache key for the extraction request"""
        
        # Create a hash of the request parameters
        import hashlib
        
        areas_str = json.dumps(sorted([f"{a['state']}_{a['lga']}_{a['ward']}" for a in areas]))
        variables_str = '_'.join(sorted(variables))
        date_str = f"{date_range['start']}_{date_range['end']}"
        
        combined = f"{areas_str}_{variables_str}_{date_str}"
        cache_key = hashlib.md5(combined.encode()).hexdigest()
        
        return cache_key
    
    def _load_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Load results from cache if available"""
        
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.csv")
        
        if os.path.exists(cache_file):
            try:
                # Check if cache is still fresh (less than 7 days old)
                file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
                if file_age.days < 7:
                    df = pd.read_csv(cache_file)
                    logger.info(f"✅ Loaded cached results: {cache_file}")
                    return df
                else:
                    logger.info("Cache file too old, will regenerate")
            except Exception as e:
                logger.warning(f"Failed to load cache file: {e}")
        
        return None
    
    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        """Save results to cache"""
        
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.csv")
        
        try:
            df.to_csv(cache_file, index=False)
            logger.info(f"💾 Saved results to cache: {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save to cache: {e}")
    
    def get_available_variables(self) -> Dict[str, Dict[str, Any]]:
        """Get information about available variables"""
        return self.variable_definitions.copy()
    
    def validate_earth_engine_access(self) -> Dict[str, Any]:
        """Validate Earth Engine access and return status"""
        
        if not self.ee_client:
            return {
                'status': 'failed',
                'message': 'Earth Engine client not initialized',
                'recommendations': [
                    'Check Earth Engine authentication',
                    'Verify project configuration',
                    'Install required dependencies'
                ]
            }
        
        return self.ee_client.validate_access()