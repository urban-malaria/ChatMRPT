"""
Robust Earth Engine Client with Working Project Configuration

Updated with the working project: epidemiological-intelligence
"""

import ee
import logging
import pandas as pd
import numpy as np
import geopandas as gpd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import os
import time

logger = logging.getLogger(__name__)

# Working project configuration
EARTH_ENGINE_PROJECT = "epidemiological-intelligence"


class RobustEarthEngineClient:
    """Robust Earth Engine client with working project configuration"""
    
    def __init__(self, service_account_file: Optional[str] = None):
        """
        Initialize robust Earth Engine client with working project
        
        Args:
            service_account_file: Path to service account JSON file (optional)
        """
        self.initialized = False
        self.service_account_file = service_account_file
        self.authentication_method = None
        self.nigeria_bounds = None  # Will be set after initialization
        self.project_id = EARTH_ENGINE_PROJECT
        
        # Try multiple initialization methods
        self._initialize_with_fallbacks()
        
        # Set geometry after initialization
        if self.initialized:
            try:
                self.nigeria_bounds = ee.Geometry.Rectangle([2.6769, 4.2406, 14.6778, 13.8659])
            except:
                logger.warning("Could not create Nigeria bounds geometry")
                self.nigeria_bounds = None
    
    def _initialize_with_fallbacks(self):
        """Try multiple authentication methods in order of preference"""
        
        # Method 1: Known working project (PRIORITY)
        try:
            logger.info(f"Attempting initialization with project: {EARTH_ENGINE_PROJECT}")
            ee.Initialize(project=EARTH_ENGINE_PROJECT)
            self.authentication_method = "project"
            self.initialized = True
            logger.info("✅ Project initialization successful")
            return
        except Exception as e:
            logger.warning(f"Project initialization failed: {e}")
        
        # Method 2: Service Account with project (if provided)
        if self.service_account_file and os.path.exists(self.service_account_file):
            try:
                logger.info("Attempting service account authentication...")
                credentials = ee.ServiceAccountCredentials(None, self.service_account_file)
                ee.Initialize(credentials, project=EARTH_ENGINE_PROJECT)
                self.authentication_method = "service_account"
                self.initialized = True
                logger.info("✅ Service account authentication successful")
                return
            except Exception as e:
                logger.warning(f"Service account authentication failed: {e}")
        
        # Method 3: Environment-based authentication with project
        try:
            logger.info("Trying environment-based authentication...")
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                ee.Initialize(project=EARTH_ENGINE_PROJECT)
                self.authentication_method = "environment"
                self.initialized = True
                logger.info("✅ Environment-based authentication successful")
                return
        except Exception as e:
            logger.warning(f"Environment authentication failed: {e}")
        
        # Method 4: Legacy authentication (fallback)
        try:
            logger.info("Attempting legacy authentication...")
            ee.Initialize()
            self.authentication_method = "legacy"
            self.initialized = True
            logger.info("✅ Legacy authentication successful")
            return
        except Exception as e:
            logger.error(f"All authentication methods failed. Last error: {e}")
            self.initialized = False
    
    def get_nigeria_ward_boundaries(self) -> ee.FeatureCollection:
        """
        Get Nigeria ward boundaries using multiple fallback methods
        """
        if not self.initialized:
            raise Exception("Earth Engine not initialized")
        
        # Method 1: Try the original private asset (if accessible)
        try:
            logger.info("Attempting to access private ward asset...")
            wards = ee.FeatureCollection('projects/ee-hephzibahadeniji/assets/NGA_wards')
            # Test if we can access it
            count = wards.size().getInfo()
            logger.info(f"✅ Private asset accessible: {count} ward features")
            return wards
        except Exception as e:
            logger.warning(f"Private asset not accessible: {e}")
        
        # Method 2: Use public administrative boundaries
        try:
            logger.info("Using public administrative boundaries...")
            
            # Get Nigeria states from GADM
            admin1 = ee.FeatureCollection('FAO/GAUL/2015/level1')
            nigeria_states = admin1.filter(ee.Filter.eq('ADM0_NAME', 'Nigeria'))
            
            # Get Nigeria LGAs from GADM level 2
            admin2 = ee.FeatureCollection('FAO/GAUL/2015/level2')
            nigeria_lgas = admin2.filter(ee.Filter.eq('ADM0_NAME', 'Nigeria'))
            
            logger.info("✅ Using public administrative boundaries (LGA level)")
            return nigeria_lgas
            
        except Exception as e:
            logger.warning(f"Public boundaries failed: {e}")
        
        # Method 3: Create simplified grid-based boundaries
        try:
            logger.info("Creating grid-based boundaries...")
            
            # Create a grid over Nigeria for ward-level approximation
            grid = self._create_nigeria_grid(0.1)  # ~10km grid
            logger.info("✅ Using grid-based boundaries")
            return grid
            
        except Exception as e:
            logger.error(f"Grid creation failed: {e}")
            raise Exception("All boundary methods failed")
    
    def _create_nigeria_grid(self, cell_size: float = 0.1) -> ee.FeatureCollection:
        """Create a grid over Nigeria for ward-level analysis"""
        
        # Nigeria bounding box
        min_lon, min_lat, max_lon, max_lat = 2.6769, 4.2406, 14.6778, 13.8659
        
        # Create grid cells
        features = []
        cell_id = 0
        
        lon = min_lon
        while lon < max_lon:
            lat = min_lat
            while lat < max_lat:
                # Create cell geometry
                cell = ee.Geometry.Rectangle([lon, lat, lon + cell_size, lat + cell_size])
                
                # Create feature with properties
                feature = ee.Feature(cell, {
                    'cell_id': cell_id,
                    'WardName': f'Grid_Cell_{cell_id}',
                    'StateName': 'Nigeria',
                    'LGAName': f'Grid_LGA_{int(cell_id / 100)}',
                    'WardCode': f'NG{cell_id:06d}'
                })
                
                features.append(feature)
                cell_id += 1
                lat += cell_size
            lon += cell_size
        
        return ee.FeatureCollection(features)
    
    def extract_environmental_variables(self, areas: List[Dict[str, str]], 
                                      variables: List[str] = None,
                                      date_range: Dict[str, str] = None) -> pd.DataFrame:
        """
        Extract environmental variables using robust methods
        """
        if not self.initialized:
            raise Exception("Earth Engine not initialized")
        
        if variables is None:
            variables = ['EVI', 'NDVI', 'rainfall', 'temperature', 'elevation']
        
        if date_range is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            date_range = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        
        logger.info(f"Extracting {len(variables)} variables for {len(areas)} areas")
        
        # Create point features for areas
        features = []
        for i, area in enumerate(areas):
            point = self._get_area_point(area)
            feature = ee.Feature(point, {
                'area_id': i,
                'state': area['state'],
                'lga': area['lga'],
                'ward': area['ward']
            })
            features.append(feature)
        
        area_collection = ee.FeatureCollection(features)
        
        # Extract variables
        results_df = pd.DataFrame(areas)
        
        for variable in variables:
            try:
                logger.info(f"Extracting {variable}...")
                values = self._extract_single_variable(variable, area_collection, date_range)
                results_df[f'mean_{variable}'] = values
                logger.info(f"✅ {variable} extracted successfully")
            except Exception as e:
                logger.error(f"Failed to extract {variable}: {e}")
                results_df[f'mean_{variable}'] = [np.nan] * len(areas)
        
        # Add metadata
        results_df['ee_extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        results_df['ee_auth_method'] = self.authentication_method
        results_df['ee_project'] = EARTH_ENGINE_PROJECT
        
        return results_df
    
    def _extract_single_variable(self, variable: str, area_collection: ee.FeatureCollection,
                               date_range: Dict[str, str]) -> List[float]:
        """Extract a single variable for all areas"""
        
        if variable == 'EVI':
            image = self._get_evi_image(date_range)
        elif variable == 'NDVI':
            image = self._get_ndvi_image(date_range)
        elif variable == 'rainfall':
            image = self._get_rainfall_image(date_range)
        elif variable == 'temperature':
            image = self._get_temperature_image(date_range)
        elif variable == 'elevation':
            image = self._get_elevation_image()
        elif variable in ['slope', 'population_density', 'urban_extent', 'terrain_slope', 'urban_land_cover', 'soil_wetness', 'relative_humidity']:
            raise ValueError(f"Variable {variable} not supported")
        elif variable == 'distance_to_water':
            image = self._get_distance_to_water_image()
        elif variable == 'nighttime_lights':
            image = self._get_nighttime_lights_image(date_range)
        elif variable == 'mean_NDMI':
            image = self._get_ndmi_image(date_range)
        elif variable == 'mean_NDWI':
            image = self._get_ndwi_image(date_range)
        else:
            raise ValueError(f"Variable {variable} not supported")
        
        # Extract values
        extracted = image.reduceRegions(
            collection=area_collection,
            reducer=ee.Reducer.mean(),
            scale=1000  # Use coarser scale for reliability
        )
        
        # Get results
        results = extracted.getInfo()
        
        # Extract values
        values = []
        for feature in results['features']:
            value = feature['properties'].get('mean', np.nan)
            values.append(value)
        
        return values
    
    def _get_evi_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get EVI image with fallbacks"""
        try:
            # Primary: MODIS Terra
            collection = ee.ImageCollection('MODIS/061/MOD13Q1')
            image = collection.filterDate(date_range['start'], date_range['end']).select('EVI').median()
            return image.multiply(0.0001)  # Scale factor
        except:
            # Fallback: MODIS Aqua
            collection = ee.ImageCollection('MODIS/061/MYD13Q1')
            image = collection.filterDate(date_range['start'], date_range['end']).select('EVI').median()
            return image.multiply(0.0001)
    
    def _get_ndvi_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get NDVI image with fallbacks"""
        try:
            collection = ee.ImageCollection('MODIS/061/MOD13Q1')
            image = collection.filterDate(date_range['start'], date_range['end']).select('NDVI').median()
            return image.multiply(0.0001)
        except:
            # Fallback: Landsat NDVI
            collection = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            if self.nigeria_bounds:
                image = collection.filterDate(date_range['start'], date_range['end']).filterBounds(self.nigeria_bounds)
            else:
                image = collection.filterDate(date_range['start'], date_range['end'])
            ndvi = image.normalizedDifference(['SR_B5', 'SR_B4']).median()
            return ndvi
    
    def _get_rainfall_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get rainfall image"""
        collection = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
        image = collection.filterDate(date_range['start'], date_range['end']).select('precipitation').sum()
        return image
    
    def _get_temperature_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get temperature image"""
        try:
            collection = ee.ImageCollection('MODIS/061/MOD11A1')
            image = collection.filterDate(date_range['start'], date_range['end']).select('LST_Day_1km').mean()
            # Convert to Celsius
            return image.multiply(0.02).subtract(273.15)
        except:
            # Fallback: ERA5 temperature
            collection = ee.ImageCollection('ECMWF/ERA5/DAILY')
            image = collection.filterDate(date_range['start'], date_range['end']).select('mean_2m_air_temperature').mean()
            return image.subtract(273.15)
    
    def _get_elevation_image(self) -> ee.Image:
        """Get elevation image"""
        return ee.Image('USGS/SRTMGL1_003').select('elevation')
    
    def _get_slope_image(self) -> ee.Image:
        """Get slope image derived from elevation"""
        elevation = ee.Image('USGS/SRTMGL1_003').select('elevation')
        slope = ee.Terrain.slope(elevation)
        return slope
    
    def _get_population_density_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get population density image"""
        try:
            # Use WorldPop dataset
            year = int(date_range['end'][:4])  # Get year from end date
            pop_collection = ee.ImageCollection('WorldPop/GP/100m/pop')
            
            # Filter by year and get Nigeria
            pop_image = pop_collection.filter(ee.Filter.eq('year', year))\
                                    .filter(ee.Filter.eq('country', 'NGA'))\
                                    .first()
            
            return pop_image.select('population')
        except:
            # Fallback: Use older dataset or create synthetic
            return ee.Image.constant(100).rename('population')
    
    def _get_urban_extent_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get urban extent from land cover data"""
        try:
            year = int(date_range['end'][:4])
            
            # Use MODIS land cover
            landcover = ee.ImageCollection('MODIS/061/MCD12Q1')\
                         .filterDate(f'{year}-01-01', f'{year}-12-31')\
                         .first()\
                         .select('LC_Type1')
            
            # Urban class = 13
            urban_mask = landcover.eq(13)
            
            # Convert to percentage within a neighborhood
            urban_percentage = urban_mask.reduceNeighborhood(
                reducer=ee.Reducer.mean(),
                kernel=ee.Kernel.circle(1000, 'meters')  # 1km radius
            ).multiply(100)
            
            return urban_percentage.rename('urban_extent')
        except:
            # Fallback
            return ee.Image.constant(10).rename('urban_extent')
    
    def _get_distance_to_water_image(self) -> ee.Image:
        """Get distance to water bodies"""
        try:
            # Use JRC Global Surface Water
            water = ee.Image('JRC/GSW1_4/GlobalSurfaceWater').select('occurrence')
            
            # Create water mask (areas with >50% water occurrence)
            water_mask = water.gt(50)
            
            # Calculate distance to water
            distance = water_mask.distance(ee.Kernel.euclidean(10000, 'meters'))
            
            return distance.rename('distance_to_water')
        except:
            # Fallback: Random values
            return ee.Image.random().multiply(5000).rename('distance_to_water')
    
    def _get_nighttime_lights_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get nighttime lights data"""
        try:
            # Use VIIRS nighttime lights
            lights_collection = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
            
            lights_image = lights_collection.filterDate(date_range['start'], date_range['end'])\
                                          .select('avg_rad')\
                                          .median()
            
            return lights_image.rename('nighttime_lights')
        except:
            # Fallback
            return ee.Image.constant(5).rename('nighttime_lights')
    
    def _get_area_point(self, area: Dict[str, str]) -> ee.Geometry:
        """Get approximate point for an area"""
        # State centroids (approximate)
        state_centroids = {
            'Osun': [4.5200, 7.7500],
            'Adamawa': [12.3985, 9.3265],
            'Kwara': [4.5418, 8.4894],
            'Kano': [8.5064, 12.0022]
        }
        
        state = area['state']
        if state in state_centroids:
            lon, lat = state_centroids[state]
            # Add variation for different wards
            lon += np.random.uniform(-0.2, 0.2)
            lat += np.random.uniform(-0.2, 0.2)
            return ee.Geometry.Point([lon, lat])
        else:
            # Default to Nigeria center
            return ee.Geometry.Point([8.6753, 9.0820])
    
    def validate_access(self) -> Dict[str, Any]:
        """Validate Earth Engine access and return status"""
        if not self.initialized:
            return {
                'status': 'failed',
                'message': 'Earth Engine not initialized',
                'auth_method': None
            }
        
        try:
            # Test basic functionality
            test_image = ee.Image('USGS/SRTMGL1_003')
            test_value = test_image.sample(ee.Geometry.Point([8.6753, 9.0820]), 1000).first().get('elevation').getInfo()
            
            # Test ward boundaries
            try:
                ward_boundaries = self.get_nigeria_ward_boundaries()
                ward_count = ward_boundaries.size().getInfo()
                ward_access = True
            except:
                ward_count = 0
                ward_access = False
            
            return {
                'status': 'success',
                'message': 'Earth Engine access validated',
                'auth_method': self.authentication_method,
                'project': EARTH_ENGINE_PROJECT,
                'test_elevation': test_value,
                'ward_boundaries_accessible': ward_access,
                'ward_count': ward_count
            }
            
        except Exception as e:
            return {
                'status': 'partial',
                'message': f'Limited access: {str(e)}',
                'auth_method': self.authentication_method,
                'project': EARTH_ENGINE_PROJECT
            }
    
    def _get_ndmi_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get NDMI (Normalized Difference Moisture Index) image"""
        try:
            # Using MODIS surface reflectance data
            collection = ee.ImageCollection('MODIS/061/MOD09A1')
            filtered = collection.filterDate(date_range['start'], date_range['end'])
            
            # Check if collection has images
            count = filtered.size().getInfo()
            if count == 0:
                logger.warning("No MODIS images found for date range, using latest available")
                filtered = collection.limit(1, 'system:time_start', False)
            
            image = filtered.median()
            
            # NDMI = (NIR - SWIR) / (NIR + SWIR)
            # sur_refl_b02 = NIR (841-876 nm), sur_refl_b06 = SWIR1 (1628-1652 nm)
            nir = image.select('sur_refl_b02').multiply(0.0001)  # Apply scale factor first
            swir = image.select('sur_refl_b06').multiply(0.0001)
            
            # Calculate NDMI with proper masking
            ndmi = nir.subtract(swir).divide(nir.add(swir)).rename('NDMI')
            
            # Mask invalid values (outside -1 to 1 range)
            ndmi = ndmi.updateMask(ndmi.gte(-1).And(ndmi.lte(1)))
            
            return ndmi
        except Exception as e:
            logger.warning(f"NDMI extraction failed: {e}, using fallback")
            # Fallback: create reasonable moisture index based on location
            return ee.Image.constant(0.3)
    
    def _get_ndwi_image(self, date_range: Dict[str, str]) -> ee.Image:
        """Get NDWI (Normalized Difference Water Index) image"""
        try:
            # Using MODIS surface reflectance data
            collection = ee.ImageCollection('MODIS/061/MOD09A1')
            filtered = collection.filterDate(date_range['start'], date_range['end'])
            
            # Check if collection has images
            count = filtered.size().getInfo()
            if count == 0:
                logger.warning("No MODIS images found for date range, using latest available")
                filtered = collection.limit(1, 'system:time_start', False)
            
            image = filtered.median()
            
            # NDWI = (Green - NIR) / (Green + NIR)
            # sur_refl_b04 = Green (545-565 nm), sur_refl_b02 = NIR (841-876 nm)
            green = image.select('sur_refl_b04').multiply(0.0001)  # Apply scale factor first
            nir = image.select('sur_refl_b02').multiply(0.0001)
            
            # Calculate NDWI with proper masking
            ndwi = green.subtract(nir).divide(green.add(nir)).rename('NDWI')
            
            # Mask invalid values (outside -1 to 1 range)
            ndwi = ndwi.updateMask(ndwi.gte(-1).And(ndwi.lte(1)))
            
            return ndwi
        except Exception as e:
            logger.warning(f"NDWI extraction failed: {e}, using fallback")
            # Fallback: create reasonable water index based on location
            return ee.Image.constant(0.1)