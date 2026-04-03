"""
Google Earth Engine Client for Real-Time Variable Extraction

This module provides production-ready access to Google Earth Engine
for extracting environmental and geographic variables.
"""

import ee
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logger = logging.getLogger(__name__)


class EarthEngineClient:
    """Production Earth Engine client for variable extraction"""
    
    def __init__(self, service_account_file: Optional[str] = None):
        """
        Initialize Earth Engine client
        
        Args:
            service_account_file: Path to service account JSON file (optional)
        """
        self.initialized = False
        self.service_account_file = service_account_file
        self.nigeria_bounds = ee.Geometry.Rectangle([2.6769, 4.2406, 14.6778, 13.8659])
        
        # Dataset configurations
        self.datasets = {
            'EVI': {
                'collection': 'MODIS/061/MOD13Q1',
                'band': 'EVI',
                'scale': 250,
                'scale_factor': 0.0001,
                'valid_range': [0, 10000]
            },
            'NDVI': {
                'collection': 'MODIS/061/MOD13Q1', 
                'band': 'NDVI',
                'scale': 250,
                'scale_factor': 0.0001,
                'valid_range': [0, 10000]
            },
            'rainfall': {
                'collection': 'UCSB-CHG/CHIRPS/DAILY',
                'band': 'precipitation',
                'scale': 5566,
                'scale_factor': 1.0,
                'valid_range': [0, 500]
            },
            'temperature': {
                'collection': 'MODIS/061/MOD11A1',
                'band': 'LST_Day_1km',
                'scale': 1000,
                'scale_factor': 0.02,
                'offset': -273.15,
                'valid_range': [15, 50]
            },
            'elevation': {
                'collection': 'USGS/SRTMGL1_003',
                'band': 'elevation',
                'scale': 30,
                'scale_factor': 1.0,
                'valid_range': [0, 2000]
            },
            'urban_extent': {
                'collection': 'MODIS/061/MCD12Q1',
                'band': 'LC_Type1',
                'scale': 500,
                'urban_class': 13,
                'valid_range': [0, 100]
            }
        }
        
        self._initialize()
    
    def _initialize(self):
        """Initialize Earth Engine authentication"""
        try:
            if self.service_account_file and os.path.exists(self.service_account_file):
                # Service account authentication
                credentials = ee.ServiceAccountCredentials(None, self.service_account_file)
                ee.Initialize(credentials)
                logger.info("Earth Engine initialized with service account")
            else:
                # Default authentication (requires prior setup)
                ee.Initialize()
                logger.info("Earth Engine initialized with default credentials")
            
            # Test connection
            test_image = ee.Image('MODIS/061/MOD13Q1/2024_01_01')
            test_info = test_image.getInfo()
            
            self.initialized = True
            logger.info("Earth Engine connection verified successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {str(e)}")
            self.initialized = False
            raise Exception(f"Earth Engine initialization failed: {str(e)}")
    
    def get_most_recent_data(self, variable: str, geometry: ee.Geometry, 
                           max_age_days: int = 90) -> ee.Image:
        """
        Get the most recent available data for a variable
        
        Args:
            variable: Variable name (EVI, NDVI, etc.)
            geometry: Area of interest
            max_age_days: Maximum age of data in days
            
        Returns:
            Most recent Earth Engine image
        """
        if not self.initialized:
            raise Exception("Earth Engine not initialized")
        
        if variable not in self.datasets:
            raise ValueError(f"Variable {variable} not supported")
        
        config = self.datasets[variable]
        collection = ee.ImageCollection(config['collection'])
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=max_age_days)
        
        # Filter collection
        filtered = collection.filterDate(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ).filterBounds(geometry)
        
        if config['band'] != 'elevation':  # Elevation is static
            # Get most recent composite
            if variable in ['EVI', 'NDVI']:
                # For vegetation indices, use median to reduce cloud effects
                image = filtered.select(config['band']).median()
            elif variable == 'rainfall':
                # For rainfall, sum over period
                image = filtered.select(config['band']).sum()
            elif variable == 'temperature':
                # For temperature, use mean
                image = filtered.select(config['band']).mean()
            else:
                image = filtered.select(config['band']).mean()
        else:
            # Elevation is static
            image = ee.Image(config['collection']).select(config['band'])
        
        # Apply scale factor and offset if needed
        if 'scale_factor' in config:
            image = image.multiply(config['scale_factor'])
        if 'offset' in config:
            image = image.add(config['offset'])
        
        return image
    
    def extract_urban_extent(self, geometry: ee.Geometry, year: int = None) -> ee.Image:
        """Extract urban extent percentage"""
        if year is None:
            year = datetime.now().year - 1  # Use previous year for complete data
        
        config = self.datasets['urban_extent']
        
        # Get land cover for specific year
        land_cover = ee.ImageCollection(config['collection']).filterDate(
            f'{year}-01-01', f'{year}-12-31'
        ).first().select(config['band'])
        
        # Create urban mask (class 13 = urban)
        urban_mask = land_cover.eq(config['urban_class'])
        
        # Calculate urban percentage
        urban_percentage = urban_mask.multiply(100)
        
        return urban_percentage
    
    def extract_variables_for_areas(self, areas: List[Dict[str, str]], 
                                  variables: List[str],
                                  max_age_days: int = 90) -> pd.DataFrame:
        """
        Extract multiple variables for multiple areas using Earth Engine
        
        Args:
            areas: List of geographic areas with coordinates
            variables: List of variable names to extract
            max_age_days: Maximum age of data in days
            
        Returns:
            DataFrame with extracted values
        """
        if not self.initialized:
            raise Exception("Earth Engine not initialized")
        
        logger.info(f"Extracting {len(variables)} variables for {len(areas)} areas using Earth Engine")
        
        # Create feature collection from areas
        features = []
        for i, area in enumerate(areas):
            # Create point geometry (you can modify this to use actual ward boundaries)
            point = self._get_area_centroid(area)
            feature = ee.Feature(point, {
                'area_id': i,
                'state': area['state'],
                'lga': area['lga'], 
                'ward': area['ward']
            })
            features.append(feature)
        
        area_collection = ee.FeatureCollection(features)
        
        # Extract each variable
        results = {}
        for variable in variables:
            if variable in self.datasets:
                logger.info(f"Extracting {variable} from Earth Engine...")
                
                try:
                    if variable == 'urban_extent':
                        image = self.extract_urban_extent(self.nigeria_bounds)
                    else:
                        image = self.get_most_recent_data(variable, self.nigeria_bounds, max_age_days)
                    
                    # Extract values for all areas
                    config = self.datasets[variable]
                    extracted = image.reduceRegions(
                        collection=area_collection,
                        reducer=ee.Reducer.mean(),
                        scale=config['scale']
                    )
                    
                    # Get results
                    extraction_results = extracted.getInfo()
                    
                    # Process results
                    values = []
                    for feature in extraction_results['features']:
                        props = feature['properties']
                        value = props.get('mean', np.nan)
                        
                        # Apply validation
                        if variable in self.datasets:
                            valid_range = self.datasets[variable].get('valid_range', [0, 1000000])
                            if value is not None and (value < valid_range[0] or value > valid_range[1]):
                                logger.warning(f"Value {value} for {variable} outside valid range {valid_range}")
                                value = np.nan
                        
                        values.append(value)
                    
                    results[f'mean_{variable}'] = values
                    logger.info(f"Successfully extracted {variable}: {len([v for v in values if not pd.isna(v)])} valid values")
                    
                except Exception as e:
                    logger.error(f"Failed to extract {variable}: {str(e)}")
                    results[f'mean_{variable}'] = [np.nan] * len(areas)
            else:
                logger.warning(f"Variable {variable} not available in Earth Engine datasets")
                results[f'mean_{variable}'] = [np.nan] * len(areas)
        
        # Create DataFrame
        df = pd.DataFrame(areas)
        for var_name, values in results.items():
            df[var_name] = values
        
        # Add extraction metadata
        df['ee_extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['ee_max_age_days'] = max_age_days
        
        return df
    
    def _get_area_centroid(self, area: Dict[str, str]) -> ee.Geometry:
        """
        Get centroid point for an area (simplified version)
        In production, you would use actual ward boundaries
        """
        # Approximate centroids for Nigerian states
        state_centroids = {
            'Osun': [4.5200, 7.7500],
            'Adamawa': [12.3985, 9.3265], 
            'Kwara': [4.5418, 8.4894],
            'Kano': [8.5064, 12.0022]
        }
        
        state = area['state']
        if state in state_centroids:
            lon, lat = state_centroids[state]
            # Add small random offset for different wards
            lon += np.random.uniform(-0.1, 0.1)
            lat += np.random.uniform(-0.1, 0.1)
            return ee.Geometry.Point([lon, lat])
        else:
            # Default to Nigeria center
            return ee.Geometry.Point([8.6753, 9.0820])
    
    def get_available_variables(self) -> List[str]:
        """Get list of variables available through Earth Engine"""
        return list(self.datasets.keys())
    
    def validate_connection(self) -> bool:
        """Validate Earth Engine connection"""
        try:
            if not self.initialized:
                return False
            
            # Test basic operation
            test_point = ee.Geometry.Point([8.6753, 9.0820])
            test_image = ee.Image('MODIS/061/MOD13Q1/2024_01_01')
            sample = test_image.sample(test_point, 250).first()
            result = sample.getInfo()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Earth Engine validation failed: {str(e)}")
            return False
    
    def get_data_availability(self, variable: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """Check data availability for a variable in a date range"""
        if variable not in self.datasets:
            return {'available': False, 'reason': 'Variable not supported'}
        
        try:
            config = self.datasets[variable]
            collection = ee.ImageCollection(config['collection'])
            
            filtered = collection.filterDate(start_date, end_date).filterBounds(self.nigeria_bounds)
            count = filtered.size().getInfo()
            
            if count > 0:
                # Get date range of available data
                dates = filtered.aggregate_array('system:time_start').getInfo()
                if dates:
                    first_date = datetime.fromtimestamp(min(dates) / 1000).strftime('%Y-%m-%d')
                    last_date = datetime.fromtimestamp(max(dates) / 1000).strftime('%Y-%m-%d')
                    
                    return {
                        'available': True,
                        'count': count,
                        'first_date': first_date,
                        'last_date': last_date
                    }
            
            return {'available': False, 'reason': 'No data in date range'}
            
        except Exception as e:
            return {'available': False, 'reason': str(e)}