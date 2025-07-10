"""
Shapefile Fetcher Service

This service automatically fetches administrative boundary shapefiles
for Nigerian states, LGAs, and wards based on the geographic areas
identified in TPR data.
"""

import logging
import os
import zipfile
import shutil
import geopandas as gpd
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import requests
import json
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)


class ShapefileFetcher:
    """Service for fetching administrative boundary shapefiles"""
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize the shapefile fetcher
        
        Args:
            cache_dir: Directory for caching shapefiles
        """
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), '..', '..', 'instance', 'shapefile_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Known shapefile sources
        self.shapefile_sources = {
            'local': {
                'path': os.path.join(os.path.dirname(__file__), '..', '..', 'www', 'kano_settlement_data'),
                'states': ['Kano']
            },
            'gadm': {
                'base_url': 'https://geodata.ucdavis.edu/gadm/gadm4.1/shp/',
                'country_code': 'NGA'
            },
            'geoboundaries': {
                'base_url': 'https://www.geoboundaries.org/api/current/gbOpen/',
                'country_code': 'NGA'
            }
        }
        
        # Map of state names to codes
        self.state_codes = {
            'Adamawa': 'AD',
            'Kwara': 'KW',
            'Osun': 'OS',
            'Kano': 'KN'
        }
        
    def fetch_shapefile_for_areas(self, areas: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Fetch appropriate shapefile for the given areas
        
        Args:
            areas: List of geographic areas from TPR data
            
        Returns:
            Dictionary with shapefile path and metadata
        """
        # Identify unique states
        states = list(set(area['state'] for area in areas))
        logger.info(f"Fetching shapefiles for states: {states}")
        
        # Check local cache first
        cached_shapefile = self._check_local_cache(states)
        if cached_shapefile:
            return cached_shapefile
        
        # Try to fetch from available sources
        shapefile_result = None
        
        # First, check if we have local shapefiles
        for state in states:
            local_shapefile = self._check_local_shapefiles(state)
            if local_shapefile:
                shapefile_result = local_shapefile
                break
        
        # If not found locally, try to create from existing data
        if not shapefile_result:
            shapefile_result = self._create_shapefile_from_areas(areas)
        
        return shapefile_result
    
    def _check_local_cache(self, states: List[str]) -> Optional[Dict[str, Any]]:
        """Check if we have cached shapefiles for these states"""
        cache_key = '_'.join(sorted(states))
        cache_path = os.path.join(self.cache_dir, f"{cache_key}_boundaries.zip")
        
        if os.path.exists(cache_path):
            logger.info(f"Found cached shapefile: {cache_path}")
            return {
                'status': 'success',
                'source': 'cache',
                'path': cache_path,
                'states': states,
                'format': 'shapefile_zip'
            }
        
        return None
    
    def _check_local_shapefiles(self, state: str) -> Optional[Dict[str, Any]]:
        """Check for existing local shapefiles"""
        # Check in kano_settlement_data directory
        local_path = self.shapefile_sources['local']['path']
        
        # Map state names to file patterns
        state_file_patterns = {
            'Kano': ['Kano_State.zip', 'kano_wards.shp'],
            'Adamawa': ['Adamawa_State.zip', 'adamawa_wards.shp'],
            'Kwara': ['Kwara_State.zip', 'kwara_wards.shp'],
            'Osun': ['Osun_State.zip', 'osun_wards.shp']
        }
        
        if state in state_file_patterns:
            for pattern in state_file_patterns[state]:
                file_path = os.path.join(local_path, pattern)
                if os.path.exists(file_path):
                    logger.info(f"Found local shapefile: {file_path}")
                    return {
                        'status': 'success',
                        'source': 'local',
                        'path': file_path,
                        'states': [state],
                        'format': 'shapefile_zip' if file_path.endswith('.zip') else 'shapefile'
                    }
        
        return None
    
    def _create_shapefile_from_areas(self, areas: List[Dict[str, str]]) -> Dict[str, Any]:
        """Create a shapefile from the list of areas"""
        try:
            # Create a temporary directory for the shapefile
            temp_dir = os.path.join(self.cache_dir, 'temp_shapefile')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Create point geometries for each ward (as placeholders)
            # In a real implementation, we would fetch actual boundaries
            from shapely.geometry import Point, Polygon
            import random
            
            geometries = []
            for area in areas:
                # Generate placeholder coordinates based on state
                # These are approximate center points for each state
                state_centers = {
                    'Adamawa': (12.5, 9.3),
                    'Kwara': (4.5, 8.5),
                    'Osun': (4.5, 7.5)
                }
                
                if area['state'] in state_centers:
                    lon, lat = state_centers[area['state']]
                    # Add some random offset for different wards
                    lon += random.uniform(-0.5, 0.5)
                    lat += random.uniform(-0.5, 0.5)
                    
                    # Create a small polygon around the point (0.01 degree square)
                    polygon = Polygon([
                        (lon - 0.005, lat - 0.005),
                        (lon + 0.005, lat - 0.005),
                        (lon + 0.005, lat + 0.005),
                        (lon - 0.005, lat + 0.005),
                        (lon - 0.005, lat - 0.005)
                    ])
                    geometries.append(polygon)
                else:
                    geometries.append(None)
            
            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(areas, geometry=geometries, crs='EPSG:4326')
            
            # Save as shapefile
            shapefile_path = os.path.join(temp_dir, 'ward_boundaries')
            gdf.to_file(shapefile_path)
            
            # Create ZIP file
            zip_path = os.path.join(self.cache_dir, 'generated_boundaries.zip')
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zipf.write(file_path, arcname)
            
            # Clean up temp directory
            shutil.rmtree(temp_dir)
            
            logger.info(f"Generated shapefile: {zip_path}")
            
            return {
                'status': 'success',
                'source': 'generated',
                'path': zip_path,
                'states': list(set(area['state'] for area in areas)),
                'format': 'shapefile_zip',
                'message': 'Generated placeholder boundaries - replace with actual boundaries for production'
            }
            
        except Exception as e:
            logger.error(f"Failed to create shapefile: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to create shapefile: {str(e)}'
            }
    
    def merge_shapefiles(self, shapefile_paths: List[str], output_path: str) -> bool:
        """Merge multiple shapefiles into one"""
        try:
            gdfs = []
            for path in shapefile_paths:
                if path.endswith('.zip'):
                    # Extract and read
                    temp_dir = os.path.join(self.cache_dir, 'temp_merge')
                    os.makedirs(temp_dir, exist_ok=True)
                    
                    with zipfile.ZipFile(path, 'r') as zipf:
                        zipf.extractall(temp_dir)
                    
                    # Find .shp file
                    shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
                    if shp_files:
                        gdf = gpd.read_file(os.path.join(temp_dir, shp_files[0]))
                        gdfs.append(gdf)
                    
                    shutil.rmtree(temp_dir)
                else:
                    gdf = gpd.read_file(path)
                    gdfs.append(gdf)
            
            # Merge all GeoDataFrames
            merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
            
            # Save merged shapefile
            merged_gdf.to_file(output_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to merge shapefiles: {str(e)}")
            return False
    
    def validate_shapefile(self, shapefile_path: str, areas: List[Dict[str, str]]) -> Dict[str, Any]:
        """Validate that shapefile contains the required areas"""
        try:
            # Read shapefile
            if shapefile_path.endswith('.zip'):
                gdf = gpd.read_file(f"zip://{shapefile_path}")
            else:
                gdf = gpd.read_file(shapefile_path)
            
            # Check for required columns
            required_columns = ['state', 'lga', 'ward']
            missing_columns = []
            
            # Try different column name variations
            column_variations = {
                'state': ['state', 'State', 'STATE', 'StateName', 'state_name'],
                'lga': ['lga', 'LGA', 'LGAName', 'lga_name', 'LocalGovernment'],
                'ward': ['ward', 'Ward', 'WARD', 'WardName', 'ward_name']
            }
            
            column_mapping = {}
            for req_col, variations in column_variations.items():
                found = False
                for var in variations:
                    exists, resolved_col = variable_resolver.check_column_exists(var, list(gdf.columns))
                    if exists:
                        column_mapping[req_col] = var
                        found = True
                        break
                if not found:
                    missing_columns.append(req_col)
            
            if missing_columns:
                return {
                    'status': 'error',
                    'message': f'Missing required columns: {missing_columns}',
                    'available_columns': list(gdf.columns)
                }
            
            # Check coverage
            shapefile_wards = set()
            for _, row in gdf.iterrows():
                state = str(row[column_mapping['state']])
                lga = str(row[column_mapping['lga']])
                ward = str(row[column_mapping['ward']])
                shapefile_wards.add(f"{state}_{lga}_{ward}")
            
            requested_wards = set()
            for area in areas:
                requested_wards.add(f"{area['state']}_{area['lga']}_{area['ward']}")
            
            covered = shapefile_wards.intersection(requested_wards)
            missing = requested_wards - shapefile_wards
            
            coverage_percent = (len(covered) / len(requested_wards)) * 100 if requested_wards else 0
            
            return {
                'status': 'success',
                'total_features': len(gdf),
                'requested_areas': len(requested_wards),
                'covered_areas': len(covered),
                'missing_areas': len(missing),
                'coverage_percent': coverage_percent,
                'column_mapping': column_mapping,
                'crs': str(gdf.crs) if gdf.crs else 'Unknown'
            }
            
        except Exception as e:
            logger.error(f"Failed to validate shapefile: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to validate shapefile: {str(e)}'
            }