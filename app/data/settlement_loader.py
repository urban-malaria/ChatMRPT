"""
Settlement Data Loader for ChatMRPT
Handles settlement shapefile integration for ANY Nigerian state/region
DESIGNED FOR EASY SCALING: Kano → All Nigeria

Based on actual Kano data structure:
- 786,104 building footprints
- clssfct: classification numbers 
- sttlmn_: settlement type labels
- Rich building morphology data
"""

import geopandas as gpd
import pandas as pd
import os
import json
from typing import Dict, Any, Optional, List, Union
import logging

logger = logging.getLogger(__name__)

class SettlementLoader:
    """
    SCALABLE settlement loader - works for single state or entire Nigeria
    Optimized for actual Kano data structure with 786K+ building footprints
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.session_folder = f"instance/uploads/{session_id}"
        self.config = self._load_settlement_config()
        
    def load_settlements(self, regions: Union[str, List[str]] = "auto") -> Optional[gpd.GeoDataFrame]:
        """
        Load settlement data for specified region(s)
        
        Args:
            regions: "auto" (detect), "kano", ["kano", "lagos"], "nigeria" (all states)
        
        Returns:
            GeoDataFrame with settlements for specified region(s)
        """
        try:
            if regions == "auto":
                regions = self._detect_available_regions()
            elif regions == "nigeria":
                regions = self._get_all_nigerian_states()
            elif isinstance(regions, str):
                regions = [regions]
            
            # Load settlements for all specified regions
            all_settlements = []
            
            for region in regions:
                region_gdf = self._load_region_settlements(region)
                if region_gdf is not None:
                    region_gdf['region'] = region  # Track source region
                    all_settlements.append(region_gdf)
            
            if not all_settlements:
                logger.warning(f"No settlement data found for regions: {regions}")
                return None
            
            # Combine all regions
            combined_gdf = gpd.GeoDataFrame(pd.concat(all_settlements, ignore_index=True))
            
            # Standardize and classify based on actual data structure
            combined_gdf = self._standardize_kano_data_structure(combined_gdf)
            
            logger.info(f"Settlement data loaded: {len(combined_gdf):,} settlements across {len(regions)} region(s)")
            return combined_gdf
            
        except Exception as e:
            logger.error(f"Error loading settlements: {e}")
            return None
    
    def _load_region_settlements(self, region: str) -> Optional[gpd.GeoDataFrame]:
        """Load settlements for a specific region with dynamic path resolution"""
        try:
            # Define search locations in priority order
            search_locations = [
                # 1. Session-specific uploads
                self.session_folder,
                # 2. Project settlement data directories
                "kano_settlement_data/Kano_clustered_footprint",
                "settlement_data",
                "data/settlements",
                # 3. Instance uploads (for other sessions)
                "instance/uploads",
                # 4. Root data directory
                "data"
            ]
            
            # Define file patterns to search for (flexible naming)
            possible_patterns = [
                f"{region.lower()}_grids_clustered.shp",
                f"{region.lower()}_clustered_footprint.shp",
                f"{region.lower()}_buildings.shp",
                f"{region.lower()}_settlements.shp",
                f"settlements_{region.lower()}.shp",
                f"{region.lower()}.shp",
                "kano_grids_clustered.shp",  # Legacy Kano file
                "settlements.shp",  # Generic fallback
                "buildings.shp"  # Generic buildings
            ]
            
            shp_path = None
            
            # Search through all locations and patterns
            for base_path in search_locations:
                if not os.path.exists(base_path):
                    continue
                    
                for pattern in possible_patterns:
                    candidate_path = os.path.join(base_path, pattern)
                    if os.path.exists(candidate_path):
                        # Additional validation: check if file is for the right region
                        if self._validate_settlement_file(candidate_path, region):
                            shp_path = candidate_path
                            break
                            
                if shp_path:
                    break
            
            # If no specific file found, try loading any settlement file and filter
            if shp_path is None:
                shp_path = self._find_any_settlement_file(search_locations)
            
            if shp_path:
                logger.info(f"Loading settlement data from: {shp_path}")
                gdf = gpd.read_file(shp_path)
                
                # Try to filter by region if the file contains multiple regions
                filtered_gdf = self._filter_by_region(gdf, region)
                if filtered_gdf is not None and not filtered_gdf.empty:
                    logger.info(f"Loaded {len(filtered_gdf):,} settlement records for {region}")
                    return filtered_gdf
                else:
                    # If no region filtering possible or region matches, return all data
                    logger.info(f"Loaded {len(gdf):,} settlement records (assuming {region} region)")
                    return gdf
            
            logger.warning(f"No settlement data found for region: {region}")
            return None
            
        except Exception as e:
            logger.error(f"Error loading settlements for {region}: {e}")
            return None
    
    def _validate_settlement_file(self, file_path: str, region: str) -> bool:
        """Validate if a settlement file is appropriate for the region"""
        try:
            # Quick validation by checking if file can be read and has expected columns
            import geopandas as gpd
            sample = gpd.read_file(file_path, rows=5)  # Read just a few rows
            
            # Check for expected settlement columns
            expected_cols = ['geometry']
            settlement_indicators = ['settlement', 'sttlmn_', 'building', 'cluster', 'class']
            
            has_geometry = 'geometry' in sample.columns
            has_settlement_data = any(
                any(indicator in col.lower() for indicator in settlement_indicators)
                for col in sample.columns
            )
            
            return has_geometry and has_settlement_data
            
        except Exception as e:
            logger.warning(f"Could not validate settlement file {file_path}: {e}")
            return False
    
    def _find_any_settlement_file(self, search_locations: List[str]) -> Optional[str]:
        """Find any settlement shapefile in the search locations"""
        for location in search_locations:
            if not os.path.exists(location):
                continue
                
            try:
                for filename in os.listdir(location):
                    if filename.endswith('.shp'):
                        file_path = os.path.join(location, filename)
                        if self._validate_settlement_file(file_path, 'any'):
                            return file_path
            except Exception as e:
                logger.warning(f"Could not search in {location}: {e}")
                continue
                
        return None
    
    def _filter_by_region(self, gdf: gpd.GeoDataFrame, region: str) -> Optional[gpd.GeoDataFrame]:
        """Try to filter GeoDataFrame by region if region columns exist"""
        try:
            # Look for region/state columns
            region_cols = [col for col in gdf.columns 
                          if any(term in col.lower() for term in ['state', 'region', 'area', 'lga'])]
            
            if region_cols:
                region_col = region_cols[0]
                # Try case-insensitive matching
                mask = gdf[region_col].astype(str).str.lower().str.contains(region.lower(), na=False)
                filtered = gdf[mask]
                
                if not filtered.empty:
                    return filtered
            
            # If no region filtering possible, return None (use full dataset)
            return None
            
        except Exception as e:
            logger.warning(f"Could not filter by region {region}: {e}")
            return None
    
    def _detect_available_regions(self) -> List[str]:
        """Auto-detect which regions have settlement data available"""
        detected_regions = []
        nigerian_states = self._get_all_nigerian_states()
        
        # Search locations for settlement data
        search_locations = [
            "kano_settlement_data/Kano_clustered_footprint",
            "settlement_data",
            "data/settlements",
            self.session_folder,
            "instance/uploads",
            "data"
        ]
        
        for location in search_locations:
            if not os.path.exists(location):
                continue
                
            try:
                for filename in os.listdir(location):
                    if not filename.endswith('.shp'):
                        continue
                        
                    file_path = os.path.join(location, filename)
                    
                    # Validate it's a settlement file
                    if not self._validate_settlement_file(file_path, 'any'):
                        continue
                    
                    # Extract region name from filename
                    name_parts = filename.replace('.shp', '').lower().split('_')
                    
                    # Check against Nigerian state names
                    region_found = False
                    for part in name_parts:
                        if part in [state.lower() for state in nigerian_states]:
                            region_name = part.title()
                            if region_name not in detected_regions:
                                detected_regions.append(region_name)
                            region_found = True
                            break
                    
                    # If no specific state found, try to detect from file content or assume default
                    if not region_found:
                        # Try to read and detect region from data
                        detected_region = self._detect_region_from_content(file_path)
                        if detected_region and detected_region not in detected_regions:
                            detected_regions.append(detected_region)
                        elif location == "kano_settlement_data/Kano_clustered_footprint":
                            # Kano data directory
                            if 'Kano' not in detected_regions:
                                detected_regions.append('Kano')
                        
            except Exception as e:
                logger.warning(f"Could not scan {location}: {e}")
                continue
        
        return detected_regions if detected_regions else ['Nigeria']  # Generic fallback
    
    def _get_all_nigerian_states(self) -> List[str]:
        """Get list of all Nigerian states for future scaling"""
        return [
            'Abia', 'Adamawa', 'AkwaIbom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue', 
            'Borno', 'CrossRiver', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 
            'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 
            'Kogi', 'Kwara', 'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo', 
            'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 
            'Zamfara', 'FCT'  # Federal Capital Territory
        ]
    
    def _standardize_kano_data_structure(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Standardize Kano data structure based on actual discovered columns:
        - clssfct: classification numbers (0-6)
        - sttlmn_: settlement type labels ('informal', 'formal', 'non-residential')
        - latitud, longitd: coordinates
        - Cluster mapping: 0,1,4=informal; 2,5,6=formal; 3=non-residential
        """
        # Standardize column names for our system
        column_mappings = {
            'clssfct': 'cluster',           # Classification numbers
            'sttlmn_': 'settlement_type',   # Settlement type labels  
            'latitud': 'latitude',          # Latitude
            'longitd': 'longitude',         # Longitude
            'ar_n_mt': 'area_sqm',          # Area in square meters
            'area_mn': 'mean_area',         # Mean area
            'prmtr_m': 'perimeter_m',       # Perimeter in meters
            'nndst_m': 'nearest_distance',  # Nearest neighbor distance
            'cmpct_m': 'compactness',       # Compactness metric
            'angl_mn': 'mean_angle',        # Mean angle
            'shap_mn': 'shape_metric'       # Shape metric
        }
        
        # Apply mappings only if columns exist
        for old_name, new_name in column_mappings.items():
            if old_name in gdf.columns and new_name not in gdf.columns:
                gdf = gdf.rename(columns={old_name: new_name})
        
        # Ensure we have required columns for our system
        if 'cluster' not in gdf.columns:
            if 'settlement_type' in gdf.columns:
                # Create cluster numbers from settlement types if available
                gdf['cluster'] = gdf['settlement_type'].map(
                    {'informal': 0, 'formal': 3, 'mixed': 6}
                ).fillna(0).astype(int)
            else:
                # Create dummy cluster numbers
                gdf['cluster'] = 0
        
        # Standardize settlement_type column
        if 'settlement_type' not in gdf.columns:
            # Create from cluster numbers using our classification rules
            gdf = self._classify_settlements(gdf)
        else:
            # Log original settlement types for debugging
            logger.info(f"🏘️ Original settlement types in data: {gdf['settlement_type'].unique()}")
            
            # Clean existing settlement_type values
            gdf['settlement_type'] = gdf['settlement_type'].str.lower().str.strip()
            
            # Log settlement type distribution before mapping
            logger.info(f"📊 Settlement type distribution before mapping:")
            type_counts = gdf['settlement_type'].value_counts()
            for stype, count in type_counts.items():
                logger.info(f"   '{stype}': {count:,} buildings")
            
            # Map variations to standard names - use space format to match original data
            settlement_mapping = {
                'formal': 'formal',
                'informal': 'informal', 
                'slum': 'informal',  # Map slum to informal
                'non residential': 'non residential',  # Keep original format with space
                'non-residential': 'non residential',  # Map dash to space format
                'nonresidential': 'non residential',
                'non_residential': 'non residential',
                'mixed': 'non residential',  # Map mixed to non residential
                'commercial': 'non residential',  # Add commercial mapping
                'industrial': 'non residential',  # Add industrial mapping
                'institutional': 'non residential'  # Add institutional mapping
            }
            gdf['settlement_type'] = gdf['settlement_type'].map(settlement_mapping).fillna('informal')
        
        # Add settlement risk factors
        gdf = self._add_settlement_risk_factors(gdf)
        
        return gdf
    
    def _classify_settlements(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Convert cluster numbers to settlement types
        Updated to use 3 settlement types: formal, informal, non-residential
        """
        # Load classification rules from config
        # Updated based on actual Kano data structure:
        # Cluster 3 = Non-residential
        # Clusters 0, 1, 4 = Informal
        # Clusters 2, 5, 6 = Formal
        classification_rules = self.config.get('classification_rules', {
            'informal': [0, 1, 4],
            'formal': [2, 5, 6], 
            'non residential': [3]
        })
        
        def get_settlement_type(cluster):
            for settlement_type, cluster_list in classification_rules.items():
                if cluster in cluster_list:
                    return settlement_type.lower()  # Use lowercase for consistency
            return 'informal'  # Default to informal
        
        # Add settlement classification
        if 'cluster' in gdf.columns:
            # Log unique cluster values for debugging
            unique_clusters = sorted(gdf['cluster'].unique())
            logger.info(f"🔢 Unique cluster values in data: {unique_clusters}")
            logger.info(f"📊 Cluster value counts:")
            cluster_counts = gdf['cluster'].value_counts().sort_index()
            for cluster, count in cluster_counts.items():
                logger.info(f"   Cluster {cluster}: {count:,} buildings")
            
            gdf['settlement_type'] = gdf['cluster'].apply(get_settlement_type)
        
        return gdf
    
    def _add_settlement_risk_factors(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Add risk multipliers based on settlement type"""
        risk_factors = self.config.get('risk_factors', {
            'informal': 1.5,  # 50% higher risk
            'formal': 1.0,    # baseline risk
            'non-residential': 0.5,  # Lower risk (non-residential)
            'unknown': 1.1    # slight increase for unknown
        })
        
        def get_risk_factor(settlement_type):
            return risk_factors.get(settlement_type.lower(), 1.0)
        
        if 'settlement_type' in gdf.columns:
            gdf['settlement_risk_factor'] = gdf['settlement_type'].apply(get_risk_factor)
        else:
            gdf['settlement_risk_factor'] = 1.0
        
        return gdf
    
    def _load_settlement_config(self) -> Dict[str, Any]:
        """Load configuration for settlement classification (future: from file)"""
        # Future: load from JSON config file for easy modification
        default_config = {
            'classification_rules': {
                'informal': [0, 1, 4],
                'formal': [2, 5, 6],
                'non-residential': [3]
            },
            'risk_factors': {
                'informal': 1.5,  # 50% higher risk
                'formal': 1.0,    # baseline risk
                'non-residential': 0.5,  # Lower risk (non-residential)
                'unknown': 1.1    # slight increase for unknown
            }
        }
        
        # Try to load from config file if it exists
        config_path = os.path.join(self.session_folder, 'settlement_config.json')
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load settlement config: {e}")
        
        return default_config
    
    def _detect_region_from_content(self, file_path: str) -> Optional[str]:
        """Try to detect region from file content or bounds"""
        try:
            import geopandas as gpd
            
            # Read a small sample to check bounds or content
            sample = gpd.read_file(file_path, rows=10)
            
            # Check if there are region/state columns with data
            region_cols = [col for col in sample.columns 
                          if any(term in col.lower() for term in ['state', 'region', 'area', 'lga'])]
            
            if region_cols:
                region_col = region_cols[0]
                unique_values = sample[region_col].dropna().unique()
                if len(unique_values) > 0:
                    # Try to match against Nigerian states
                    nigerian_states = self._get_all_nigerian_states()
                    for value in unique_values:
                        value_str = str(value).lower()
                        for state in nigerian_states:
                            if state.lower() in value_str or value_str in state.lower():
                                return state
            
            # If no explicit region found, try geographic bounds matching
            bounds = sample.total_bounds
            if len(bounds) == 4:
                # Rough bounds check for known regions
                lon_min, lat_min, lon_max, lat_max = bounds
                
                # Kano rough bounds: 8.4-8.6 longitude, 11.9-12.1 latitude
                if 8.3 <= lon_min <= 8.7 and 8.3 <= lon_max <= 8.7 and 11.8 <= lat_min <= 12.2 and 11.8 <= lat_max <= 12.2:
                    return 'Kano'
                
                # Lagos rough bounds: 3.0-3.7 longitude, 6.4-6.7 latitude
                if 3.0 <= lon_min <= 3.8 and 3.0 <= lon_max <= 3.8 and 6.3 <= lat_min <= 6.8 and 6.3 <= lat_max <= 6.8:
                    return 'Lagos'
                
                # Abuja rough bounds: 7.0-7.8 longitude, 8.8-9.3 latitude
                if 7.0 <= lon_min <= 7.8 and 7.0 <= lon_max <= 7.8 and 8.8 <= lat_min <= 9.3 and 8.8 <= lat_max <= 9.3:
                    return 'FCT'
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not detect region from content in {file_path}: {e}")
            return None
    
    def get_settlement_summary(self, regions: Union[str, List[str]] = "auto") -> Dict[str, Any]:
        """Get summary statistics of settlement data"""
        try:
            gdf = self.load_settlements(regions)
            if gdf is None:
                return {'error': 'No settlement data available'}
            
            summary = {
                'total_settlements': len(gdf),
                'regions': gdf['region'].unique().tolist() if 'region' in gdf.columns else [],
                'settlement_types': gdf['settlement_type'].value_counts().to_dict() if 'settlement_type' in gdf.columns else {},
                'cluster_distribution': gdf['cluster'].value_counts().sort_index().to_dict() if 'cluster' in gdf.columns else {},
                'geographic_bounds': gdf.total_bounds.tolist(),
                'coordinate_system': str(gdf.crs),
                'available_metrics': [col for col in gdf.columns if col not in ['geometry', 'region']]
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating settlement summary: {e}")
            return {'error': str(e)}

    # BACKWARD COMPATIBILITY: Keep old method name for existing code
    def load_kano_settlements(self) -> Optional[gpd.GeoDataFrame]:
        """Backward compatibility - delegates to new scalable method"""
        return self.load_settlements("kano") 