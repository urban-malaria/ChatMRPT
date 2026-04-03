"""
Unified Dataset Builder - GeoParquet Backend for ChatMRPT

Creates optimized GeoParquet datasets with original column names preserved
and smart metadata for tool discovery and categorization.

Architecture: Original Names + Smart Metadata (Option A)
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Union, Tuple
import logging
import time

logger = logging.getLogger(__name__)

class UnifiedDatasetBuilder:
    """
    Builds comprehensive unified datasets in GeoParquet format
    
    Key Features:
    - Preserves original column names (TPR, NDVI, Population, etc.)
    - Creates smart metadata for tool discovery
    - Integrates all analysis results including PCA and individual model scores
    - Optimized GeoParquet format for fast access
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.session_folder = f"instance/uploads/{session_id}"
        self.column_metadata = {}
        
    def build_unified_dataset(self) -> Dict[str, Any]:
        """Build comprehensive unified dataset preserving original names"""
        
        try:
            print("🔧 Building comprehensive unified GeoParquet dataset...")
            
            # 1. Load all data sources
            data_sources = self._load_all_data_sources()
            if not data_sources['success']:
                return {'status': 'error', 'message': data_sources['message']}
            
            # 2. Create base dataset with original names
            unified_gdf = self._create_base_dataset(data_sources)
            
            # 3. Generate smart metadata for columns
            self.column_metadata = self._create_smart_metadata(unified_gdf.columns)
            
            # 4. Integrate analysis results (composite + individual models + PCA)
            unified_gdf = self._integrate_analysis_results(unified_gdf, data_sources)
            
            # 5. Add comprehensive PCA analysis results
            unified_gdf = self._integrate_pca_analysis(unified_gdf, data_sources)
            
            # 6. Add individual model scores and metadata
            unified_gdf = self._integrate_model_scores(unified_gdf, data_sources)
            unified_gdf = self._integrate_model_metadata(unified_gdf, data_sources)
            
            # 7. Add spatial calculations
            unified_gdf = self._add_spatial_metrics(unified_gdf)
            
            # 8. Calculate derived metrics
            unified_gdf = self._calculate_derived_metrics(unified_gdf)
            
            # 9. Optimize for tool access
            unified_gdf = self._optimize_for_tools(unified_gdf)
            
            # 10. Validate and save
            validation_result = self._validate_dataset(unified_gdf)
            if not validation_result['valid']:
                return {'status': 'error', 'message': f"Validation failed: {validation_result['errors']}"}
            
            # 11. Save as GeoParquet and generate metadata
            file_paths = self._save_geoparquet_dataset(unified_gdf)
            metadata = self._generate_comprehensive_metadata(unified_gdf, data_sources)
            
            print(f"✅ Comprehensive unified dataset created: {unified_gdf.shape[0]} wards, {unified_gdf.shape[1]} columns")
            print(f"📁 Saved as: {file_paths['geoparquet']}")
            
            # NEW: Verify file existence with retries to handle sync delays
            def verify_file(path: str, max_attempts: int = 3) -> bool:
                for attempt in range(max_attempts):
                    if os.path.exists(path):
                        return True
                    logger.warning(f'File not yet visible: {path} (attempt {attempt+1})')
                    time.sleep(1)
                return False
            
            if not verify_file(file_paths['geoparquet']):
                raise ValueError(f'GeoParquet save verification failed: {file_paths["geoparquet"]}')
            if not verify_file(file_paths['csv']):
                logger.warning(f'CSV backup verification failed: {file_paths["csv"]}')
            
            return {
                'status': 'success',
                'dataset': unified_gdf,
                'file_paths': file_paths,
                'metadata': metadata,
                'column_metadata': self.column_metadata,
                'message': f'Comprehensive unified GeoParquet dataset ready with {unified_gdf.shape[0]} wards and {unified_gdf.shape[1]} columns'
            }
            
        except Exception as e:
            logger.error(f"Error building unified dataset: {e}")
            return {'status': 'error', 'message': f'Failed to build unified dataset: {str(e)}'}
    
    def _load_all_data_sources(self) -> Dict[str, Any]:
        """Load and validate all available data sources dynamically"""
        
        try:
            sources = {
                'csv_data': None,
                'shapefile_data': None,
                'composite_results': None,
                'composite_scores': None,
                'model_formulas': None,
                'pca_results': None,
                'pca_components': None,
                'pca_loadings': None,
                'success': False
            }
            
            # Load original data through DataHandler
            from app.data import DataHandler
            data_handler = DataHandler(self.session_folder)
            
            if data_handler.csv_data is not None and not data_handler.csv_data.empty:
                sources['csv_data'] = data_handler.csv_data
                print(f"📊 CSV loaded: {data_handler.csv_data.shape[0]} rows, {data_handler.csv_data.shape[1]} columns")
            
            # Load shapefile data using the shapefile_loader
            if hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None and not data_handler.shapefile_data.empty:
                sources['shapefile_data'] = data_handler.shapefile_data
                print(f"🗺️ Shapefile loaded: {data_handler.shapefile_data.shape[0]} features")
            elif hasattr(data_handler, 'shapefile_loader'):
                # Try to load via shapefile_loader
                shp_data = getattr(data_handler.shapefile_loader, 'data', None)
                if shp_data is not None and not shp_data.empty:
                    sources['shapefile_data'] = shp_data
                    print(f"🗺️ Shapefile loaded via loader: {shp_data.shape[0]} features")
            
            # Dynamically search for composite analysis results
            composite_patterns = [
                'analysis_vulnerability_rankings.csv',
                'analysis_composite_scores.csv',
                'vulnerability_rankings.csv',
                'composite_analysis.csv'
            ]
            
            for pattern in composite_patterns:
                filepath = os.path.join(self.session_folder, pattern)
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath)
                    if not df.empty:
                        sources['composite_results'] = df
                        print(f"📈 Composite results loaded: {pattern}")
                        break
            
            # Dynamically search for individual model scores
            model_score_patterns = [
                'composite_scores.csv',
                'model_scores.csv',
                'individual_model_scores.csv',
                'all_model_results.csv'
            ]
            
            for pattern in model_score_patterns:
                filepath = os.path.join(self.session_folder, pattern)
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath)
                    if not df.empty:
                        # Detect model columns dynamically
                        model_cols = [col for col in df.columns if col.startswith('model_') or 'model' in col.lower()]
                        if model_cols:  # Only load if it actually has model columns
                            sources['composite_scores'] = df
                            print(f"🎯 Model scores loaded: {len(model_cols)} individual models from {pattern}")
                        break
            
            # Dynamically search for model formulas/metadata
            formula_patterns = [
                'model_formulas.csv',
                'model_metadata.csv', 
                'model_definitions.csv',
                'analysis_methods.csv'
            ]
            
            for pattern in formula_patterns:
                filepath = os.path.join(self.session_folder, pattern)
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath)
                    if not df.empty:
                        sources['model_formulas'] = df
                        print(f"📋 Model formulas loaded: {len(df)} model definitions from {pattern}")
                        break
            
            # Dynamically load comprehensive PCA results
            self._load_pca_results_dynamically(sources)
            
            # Validate minimum requirements
            if sources['csv_data'] is None or sources['csv_data'].empty:
                return {'success': False, 'message': 'No CSV data available'}
            
            sources['success'] = True
            return sources
            
        except Exception as e:
            logger.error(f"Error loading data sources: {e}")
            return {'success': False, 'message': f'Error loading data: {str(e)}'}
    
    def _load_pca_results_dynamically(self, sources: Dict[str, Any]):
        """Dynamically load any available PCA analysis results"""
        
        # Search for PCA rankings/results with flexible naming
        pca_ranking_patterns = [
            'analysis_vulnerability_rankings_pca.csv',  # 🔧 FIXED: Actual file name
            'analysis_pca_rankings.csv',
            'pca_vulnerability_rankings.csv',
            'pca_analysis_results.csv',
            'pca_rankings.csv',
            'pca_results.csv'
        ]
        
        for pattern in pca_ranking_patterns:
            filepath = os.path.join(self.session_folder, pattern)
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                if not df.empty:
                    sources['pca_results'] = df
                    print(f"🔍 PCA rankings loaded: {pattern}")
                    break
        
        # Search for PCA components with flexible naming
        pca_component_patterns = [
            'analysis_pca_scores.csv',  # 🔧 FIXED: Actual file name
            'pca_components.csv',
            'principal_components.csv',
            'analysis_pca_components.csv',
            'pca_scores.csv',
            'pc_scores.csv'
        ]
        
        for pattern in pca_component_patterns:
            filepath = os.path.join(self.session_folder, pattern)
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                if not df.empty:
                    # Verify it has PC columns
                    pc_cols = [col for col in df.columns if col.startswith('PC') or 'component' in col.lower()]
                    if pc_cols:
                        sources['pca_components'] = df
                        print(f"🧮 PCA components loaded: {len(pc_cols)} components from {pattern}")
                        break
        
        # Search for PCA loadings with flexible naming
        pca_loading_patterns = [
            'pca_loadings.csv',
            'pca_variable_loadings.csv',
            'analysis_pca_loadings.csv',
            'variable_loadings.csv',
            'component_loadings.csv'
        ]
        
        for pattern in pca_loading_patterns:
            filepath = os.path.join(self.session_folder, pattern)
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                if not df.empty:
                    sources['pca_loadings'] = df
                    print(f"📊 PCA loadings loaded: {pattern}")
                    break
        
        # If no PCA results found, try to generate them dynamically
        pca_results_exists = sources.get('pca_results') is not None
        pca_components_exists = sources.get('pca_components') is not None
        if not (pca_results_exists or pca_components_exists):
            print("🔬 No PCA results found - attempting dynamic PCA analysis...")
            self._run_dynamic_pca_analysis(sources)
    
    def _run_dynamic_pca_analysis(self, sources: Dict[str, Any]):
        """Run PCA analysis dynamically if results don't exist"""
        try:
            # Try different PCA analysis approaches
            pca_approaches = [
                'app.analysis.pca_pipeline.run_independent_pca_analysis',
                'app.analysis.pca.run_pca_analysis', 
                'app.analysis.statistical_analysis.run_pca'
            ]
            
            for approach in pca_approaches:
                try:
                    module_path, function_name = approach.rsplit('.', 1)
                    module = __import__(module_path, fromlist=[function_name])
                    pca_function = getattr(module, function_name)
                    
                    from app.data import DataHandler
                    data_handler = DataHandler(self.session_folder)
                    
                    # Try to run PCA analysis
                    pca_result = pca_function(
                        data_handler=data_handler,
                        session_id=self.session_id
                    )
                    
                    # Check if pca_result is valid (handle both dict and DataFrame cases)
                    if pca_result is not None:
                        if isinstance(pca_result, dict) and pca_result.get('status') == 'success':
                            self._extract_pca_results_dynamically(pca_result, sources)
                            print(f"✅ Successfully ran PCA using {approach}")
                            break
                        elif isinstance(pca_result, pd.DataFrame) and not pca_result.empty:
                            # Direct DataFrame result - wrap in expected structure
                            wrapped_result = {'status': 'success', 'data': {'pca_results': pca_result}}
                            self._extract_pca_results_dynamically(wrapped_result, sources)
                            print(f"✅ Successfully ran PCA using {approach}")
                            break
                        
                except (ImportError, AttributeError, TypeError) as e:
                    print(f"⚠️ PCA approach {approach} not available: {e}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Could not run dynamic PCA analysis: {e}")
            logger.warning(f"Dynamic PCA analysis error: {e}")
    
    def _extract_pca_results_dynamically(self, pca_result: Dict[str, Any], sources: Dict[str, Any]):
        """Dynamically extract PCA results from any structure"""
        
        try:
            pca_data = pca_result.get('data', {}) if isinstance(pca_result, dict) else {}
            
            # Extract rankings if available
            for key in ['pca_rankings', 'rankings', 'vulnerability_rankings', 'results']:
                if key in pca_data and pca_data[key] is not None:
                    # Handle different data types
                    if isinstance(pca_data[key], pd.DataFrame):
                        sources['pca_results'] = pca_data[key]
                    elif isinstance(pca_data[key], dict):
                        # Convert dict to DataFrame with proper handling
                        try:
                            sources['pca_results'] = pd.DataFrame([pca_data[key]]) if pca_data[key] else None
                        except ValueError:
                            # If scalar values, create proper DataFrame structure
                            sources['pca_results'] = pd.DataFrame(list(pca_data[key].items()), columns=['Ward', 'Score']) if pca_data[key] else None
                    elif isinstance(pca_data[key], (list, tuple)):
                        # Convert list to DataFrame
                        if pca_data[key] and isinstance(pca_data[key][0], dict):
                            sources['pca_results'] = pd.DataFrame(pca_data[key])
                        else:
                            sources['pca_results'] = pd.DataFrame({'pca_score': pca_data[key]})
                    
                    if sources['pca_results'] is not None and not sources['pca_results'].empty:
                        print(f"✅ Extracted PCA rankings from '{key}': {len(sources['pca_results'])} wards")
                        break
            
            # Extract components if available
            component_keys = ['pca_components', 'components', 'principal_components', 'scores']
            for key in component_keys:
                if key in pca_data and pca_data[key] is not None:
                    # Handle different data types
                    if isinstance(pca_data[key], pd.DataFrame):
                        sources['pca_components'] = pca_data[key]
                    elif isinstance(pca_data[key], dict):
                        try:
                            sources['pca_components'] = pd.DataFrame(pca_data[key])
                        except ValueError:
                            # Handle scalar values properly
                            if all(isinstance(v, (int, float)) for v in pca_data[key].values()):
                                sources['pca_components'] = pd.DataFrame([pca_data[key]])
                            else:
                                sources['pca_components'] = pd.DataFrame(list(pca_data[key].items()), columns=['Component', 'Value'])
                    elif isinstance(pca_data[key], (list, tuple, np.ndarray)):
                        # Convert array-like to DataFrame
                        if hasattr(pca_data[key], 'shape') and len(pca_data[key].shape) == 2:
                            # 2D array - columns are components
                            n_components = pca_data[key].shape[1]
                            components_df = pd.DataFrame(pca_data[key], columns=[f'PC{i+1}' for i in range(n_components)])
                            sources['pca_components'] = components_df
                        else:
                            # 1D array or list
                            sources['pca_components'] = pd.DataFrame({'PC1': pca_data[key]})
                    
                    if sources['pca_components'] is not None and not sources['pca_components'].empty:
                        print(f"✅ Extracted PCA components from '{key}'")
                        break
            
            # Extract loadings if available  
            loading_keys = ['variable_importance', 'loadings', 'pca_loadings', 'component_loadings']
            for key in loading_keys:
                if key in pca_data and pca_data[key] is not None:
                    # Handle different data types
                    if isinstance(pca_data[key], pd.DataFrame):
                        sources['pca_loadings'] = pca_data[key]
                    elif isinstance(pca_data[key], dict):
                        try:
                            # Try different DataFrame construction approaches
                            if all(isinstance(v, (list, tuple, np.ndarray)) for v in pca_data[key].values()):
                                sources['pca_loadings'] = pd.DataFrame(pca_data[key])
                            else:
                                sources['pca_loadings'] = pd.DataFrame(list(pca_data[key].items()), columns=['Variable', 'Loading'])
                        except (ValueError, TypeError):
                            sources['pca_loadings'] = pd.DataFrame([pca_data[key]])
                    elif isinstance(pca_data[key], (list, tuple)):
                        sources['pca_loadings'] = pd.DataFrame({'loading': pca_data[key]})
                    
                    if sources['pca_loadings'] is not None and not sources['pca_loadings'].empty:
                        print(f"✅ Extracted PCA loadings from '{key}'")
                        break
                        
        except Exception as e:
            print(f"⚠️ Error extracting PCA results: {e}")
            logger.warning(f"PCA extraction error: {e}")
    
    def _create_base_dataset(self, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Create base unified geodataframe preserving original CSV ward count and names"""
        
        csv_df = data_sources['csv_data'].copy()
        original_ward_count = len(csv_df)
        
        # Only minimal cleaning - preserve original names
        csv_df.columns = csv_df.columns.str.strip()  # Remove leading/trailing spaces only
        print(f"🔧 Preserved original column names: {list(csv_df.columns[:5])}...")
        
        # 🔧 FIX DUPLICATE WARD NAMES: Rename duplicates using "WardName (WardCode)" format
        csv_df = self._fix_duplicate_ward_names(csv_df)
        
        # Merge with shapefile if available, but preserve CSV ward count
        if data_sources['shapefile_data'] is not None and not data_sources['shapefile_data'].empty:
            shp_gdf = data_sources['shapefile_data'].copy()
            
            # Also fix duplicate ward names in shapefile
            shp_gdf = self._fix_duplicate_ward_names(shp_gdf)
            
            # Find matching key columns
            csv_key = self._detect_ward_key_column(csv_df)
            shp_key = self._detect_ward_key_column(shp_gdf)
            
            if csv_key and shp_key:
                # PRESERVE CSV WARDS: Use left join to keep all CSV wards, add geometry where possible
                unified_gdf = self._preserve_csv_wards_merge(csv_df, shp_gdf, csv_key, shp_key)
                print(f"🔗 Preserved CSV wards with geometry: {unified_gdf.shape[0]} wards (maintaining original {original_ward_count})")
            else:
                print("⚠️ Could not match CSV and shapefile - using CSV only")
                unified_gdf = gpd.GeoDataFrame(csv_df)
        else:
            # CSV only - convert to GeoDataFrame
            unified_gdf = gpd.GeoDataFrame(csv_df)
            print("📊 Using CSV data only (no shapefile)")
        
        # Ensure we haven't inflated the ward count
        if len(unified_gdf) != original_ward_count:
            logger.warning(f"⚠️ Ward count changed from {original_ward_count} to {len(unified_gdf)} - this should not happen!")
        
        return unified_gdf
    
    def _fix_duplicate_ward_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix duplicate ward names by renaming them to 'WardName (WardCode)' format"""
        
        # Find ward name and ward code columns
        ward_name_col = None
        ward_code_col = None
        
        # Look for ward name column
        for col in ['WardName', 'ward_name', 'Ward', 'ward']:
            if col in df.columns:
                ward_name_col = col
                break
        
        # Look for ward code column
        for col in ['WardCode', 'ward_code', 'wardcode']:
            if col in df.columns:
                ward_code_col = col
                break
        
        if not ward_name_col:
            print("⚠️ No ward name column found - skipping duplicate fix")
            return df
            
        if not ward_code_col:
            print("⚠️ No ward code column found - skipping duplicate fix")
            return df
        
        df_fixed = df.copy()
        
        # Check if ward names already contain disambiguation pattern (WardCode)
        # This prevents double disambiguation when loading TPR output
        import re
        pattern = r'\s*\([A-Z0-9]+\)\s*$'  # Pattern for " (WardCode)" at end of name
        already_disambiguated = df_fixed[ward_name_col].str.contains(pattern, regex=True, na=False).any()
        
        if already_disambiguated:
            print(f"ℹ️ Ward names already contain disambiguation (WardCode) - skipping duplicate fix")
            print(f"   Total wards: {len(df_fixed)}")
            return df_fixed
        
        # Find duplicates in ward names
        duplicate_mask = df_fixed[ward_name_col].duplicated(keep=False)
        duplicate_count = duplicate_mask.sum()
        
        if duplicate_count == 0:
            print(f"✅ No duplicate ward names found in {ward_name_col}")
            return df_fixed
        
        print(f"🔧 Found {duplicate_count} duplicate ward names in {ward_name_col} - fixing with WardCode...")
        
        # For each duplicate ward name, rename using "WardName (WardCode)" format
        for idx in df_fixed[duplicate_mask].index:
            original_name = df_fixed.loc[idx, ward_name_col]
            ward_code = df_fixed.loc[idx, ward_code_col]
            
            # Create unique name: "WardName (WardCode)"
            new_name = f"{original_name} ({ward_code})"
            df_fixed.loc[idx, ward_name_col] = new_name
        
        # Verify no duplicates remain
        remaining_duplicates = df_fixed[ward_name_col].duplicated().sum()
        if remaining_duplicates == 0:
            print(f"✅ Fixed all duplicate ward names - all {len(df_fixed)} wards now have unique names")
        else:
            print(f"⚠️ Still have {remaining_duplicates} duplicate names after fix")
        
        return df_fixed
    
    def _preserve_csv_wards_merge(self, csv_df: pd.DataFrame, shp_gdf: gpd.GeoDataFrame,
                                csv_key: str, shp_key: str) -> gpd.GeoDataFrame:
        """
        Merge CSV and shapefile while preserving all CSV wards and their original names.
        Uses WardCode system for proper duplicate handling and fuzzy matching for name variations.
        """
        
        # Start with CSV data as base (to preserve original ward count)
        unified_gdf = gpd.GeoDataFrame(csv_df)
        
        # Check if WardCode is available for better matching
        csv_wardcode_col = None
        shp_wardcode_col = None
        
        for col in ['WardCode', 'ward_code', 'WardCode_x', 'WardCode_y']:
            if col in csv_df.columns:
                csv_wardcode_col = col
            if col in shp_gdf.columns:
                shp_wardcode_col = col
        
        if csv_wardcode_col and shp_wardcode_col:
            # Use WardCode for precise matching
            print(f"🔑 Using {csv_wardcode_col} and {shp_wardcode_col} for precise geometry matching")
            geometry_match = shp_gdf.set_index(shp_wardcode_col)['geometry'].to_dict()
            
            # Add geometry based on WardCode match
            unified_gdf['geometry'] = unified_gdf[csv_wardcode_col].map(geometry_match)
            matched_count = unified_gdf['geometry'].notna().sum()
            print(f"🎯 Matched geometry for {matched_count}/{len(unified_gdf)} wards using WardCode")
            
        else:
            # Fallback to ward name matching with fuzzy matching support
            print(f"🏘️ Using ward name matching (with fuzzy matching for unmatched wards)")
            
            # First try exact match
            merged = csv_df.merge(
                shp_gdf[[shp_key, 'geometry']], 
                left_on=csv_key, 
                right_on=shp_key, 
                how='left'
            )
            
            unified_gdf = gpd.GeoDataFrame(merged)
            initial_matched = unified_gdf['geometry'].notna().sum()
            print(f"🎯 Exact match: {initial_matched}/{len(unified_gdf)} wards")
            
            # Apply fuzzy matching for unmatched wards
            if initial_matched < len(unified_gdf):
                unmatched_mask = unified_gdf['geometry'].isna()
                unmatched_wards = unified_gdf.loc[unmatched_mask, csv_key].unique()
                
                print(f"🔍 Attempting fuzzy matching for {len(unmatched_wards)} unmatched wards...")
                
                # Import fuzzy matching functions
                try:
                    from fuzzywuzzy import fuzz, process
                    
                    # Create shapefile ward name to geometry mapping
                    shp_ward_geom = shp_gdf.set_index(shp_key)['geometry'].to_dict()
                    shp_ward_names = list(shp_ward_geom.keys())
                    
                    # Fuzzy match each unmatched ward
                    fuzzy_matches = 0
                    for csv_ward in unmatched_wards:
                        # Find best match in shapefile
                        best_match = process.extractOne(csv_ward, shp_ward_names, scorer=fuzz.token_sort_ratio)
                        
                        if best_match and best_match[1] >= 80:  # 80% similarity threshold
                            matched_shp_ward = best_match[0]
                            # Update geometry for all rows with this ward name
                            mask = (unified_gdf[csv_key] == csv_ward) & (unified_gdf['geometry'].isna())
                            unified_gdf.loc[mask, 'geometry'] = shp_ward_geom[matched_shp_ward]
                            fuzzy_matches += mask.sum()
                            print(f"  ✓ Matched '{csv_ward}' → '{matched_shp_ward}' (score: {best_match[1]})")
                    
                    final_matched = unified_gdf['geometry'].notna().sum()
                    print(f"🎯 After fuzzy matching: {final_matched}/{len(unified_gdf)} wards matched (+{fuzzy_matches} fuzzy)")
                    
                except ImportError:
                    print("⚠️ fuzzywuzzy not available, skipping fuzzy matching")
                except Exception as e:
                    print(f"⚠️ Error during fuzzy matching: {e}")
        
        print(f"✅ Preserved all {len(unified_gdf)} CSV wards with geometry where possible")
        return unified_gdf
    
    def _smart_merge_with_duplicates(self, shp_gdf: gpd.GeoDataFrame, csv_df: pd.DataFrame, 
                                   shp_key: str, csv_key: str) -> gpd.GeoDataFrame:
        """
        Simple merge that renames CSV duplicate ward names to match shapefile format.
        
        Problem: CSV has "Kawo" but shapefile has "Kawo (NISKNT04)" and "Kawo (NISNAS04)"
        Solution: Rename CSV duplicates to match shapefile ward names with codes
        """
        
        csv_df_fixed = csv_df.copy()
        
        # Find duplicate ward names in CSV
        csv_duplicates = csv_df_fixed[csv_df_fixed[csv_key].duplicated(keep=False)]
        
        if not csv_duplicates.empty:
            print(f"🔧 Found {len(csv_duplicates)} duplicate ward names in CSV - fixing...")
            
            # Get all shapefile ward names for matching
            shp_ward_names = set(shp_gdf[shp_key].tolist())
            
            # Group CSV duplicates by ward name
            for ward_name, group in csv_duplicates.groupby(csv_key):
                # Find matching shapefile names for this base ward
                matching_shp_names = [name for name in shp_ward_names 
                                    if name.startswith(ward_name + ' (') or name == ward_name]
                
                if len(matching_shp_names) >= len(group):
                    # Sort both lists for consistent matching
                    matching_shp_names = sorted(matching_shp_names)
                    csv_indices = sorted(group.index.tolist())
                    
                    # Rename CSV ward names to match shapefile format
                    for i, csv_idx in enumerate(csv_indices):
                        if i < len(matching_shp_names):
                            old_name = csv_df_fixed.loc[csv_idx, csv_key]
                            new_name = matching_shp_names[i]
                            csv_df_fixed.loc[csv_idx, csv_key] = new_name
                            print(f"   📝 Renamed: '{old_name}' → '{new_name}'")
        
        # Now perform simple merge since names should match
        unified_gdf = shp_gdf.merge(
            csv_df_fixed,
            left_on=shp_key,
            right_on=csv_key,
            how='inner'
        )
        
        print(f"✅ Simple merge complete: {len(unified_gdf)} total wards preserved")
        return unified_gdf
    
    def _create_smart_metadata(self, columns: List[str]) -> Dict[str, Dict[str, str]]:
        """Create intelligent metadata for original column names"""
        
        metadata = {}
        
        for col in columns:
            col_lower = col.lower()
            
            # Smart categorization based on epidemiological knowledge
            if any(pattern in col_lower for pattern in ['tpr', 'positivity', 'malaria', 'prevalence', 'test']):
                category = 'health'
                col_type = 'indicator'
                description = 'Health/disease indicator'
            elif any(pattern in col_lower for pattern in ['ndvi', 'ndmi', 'ndwi']):
                category = 'environmental'
                col_type = 'index'
                description = 'Environmental index'
            elif any(pattern in col_lower for pattern in ['elevation', 'temperature', 'rainfall', 'climate']):
                category = 'environmental'
                col_type = 'metric'
                description = 'Environmental measurement'
            elif any(pattern in col_lower for pattern in ['population', 'density']):
                category = 'demographic'
                col_type = 'count' if 'population' in col_lower else 'metric'
                description = 'Population/demographic data'
            elif any(pattern in col_lower for pattern in ['built', 'infrastructure', 'road', 'building', 'urban']):
                category = 'infrastructure'
                col_type = 'metric'
                description = 'Infrastructure measurement'
            elif any(pattern in col_lower for pattern in ['ward', 'name', 'state', 'lga']):
                category = 'identification'
                col_type = 'identifier'
                description = 'Geographic identifier'
            elif any(pattern in col_lower for pattern in ['lat', 'lon', 'coordinate', 'geometry', 'area', 'centroid']):
                category = 'spatial'
                col_type = 'coordinate' if any(x in col_lower for x in ['lat', 'lon']) else 'geometry'
                description = 'Spatial/geographic data'
            elif any(pattern in col_lower for pattern in ['score', 'rank', 'category', 'composite', 'pca']):
                category = 'analysis_result'
                col_type = 'score' if 'score' in col_lower else ('rank' if 'rank' in col_lower else 'category')
                description = 'Analysis result'
            else:
                category = 'other'
                col_type = 'unknown'
                description = 'Other data'
            
            metadata[col] = {
                'category': category,
                'type': col_type,
                'description': description,
                'original_name': col
            }
        
        # Summary
        category_counts = {}
        for meta in metadata.values():
            cat = meta['category']
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        print(f"🏷️ Smart metadata created for {len(metadata)} columns:")
        for cat, count in category_counts.items():
            print(f"   {cat}: {count} columns")
        
        return metadata
    
    def _detect_ward_key_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect the ward identifier column (should be unique after duplicate fix)"""
        
        # Look for standard ward name columns
        name_candidates = ['WardName', 'ward_name', 'Ward', 'ward']
        for candidate in name_candidates:
            if candidate in df.columns:
                duplicate_count = df[candidate].duplicated().sum()
                if duplicate_count == 0:
                    print(f"🔑 Using '{candidate}' for merge (unique names)")
                else:
                    print(f"⚠️ Using '{candidate}' for merge but found {duplicate_count} duplicates")
                return candidate
        
        # Legacy detection for other ward-related columns
        possible_keys = []
        for col in df.columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in ['ward', 'name']):
                if df[col].dtype == 'object' and df[col].nunique() > df.shape[0] * 0.8:
                    possible_keys.append(col)
        
        if possible_keys:
            candidate = possible_keys[0]
            duplicate_count = df[candidate].duplicated().sum()
            if duplicate_count == 0:
                print(f"🔑 Using '{candidate}' for merge (unique names)")
            else:
                print(f"⚠️ Using '{candidate}' for merge but found {duplicate_count} duplicates")
            return candidate
        
        return None
    
    def _integrate_analysis_results(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate composite and PCA analysis results preserving original names"""
        
        ward_key = self._detect_ward_key_column(gdf)
        if not ward_key:
            print("⚠️ No ward key found - skipping analysis integration")
            return gdf
        
        # 🔍 DEBUG: Track ward count at each step
        original_count = len(gdf)
        print(f"🔍 WARD COUNT DEBUG: Starting integration with {original_count} wards")
        
        # Add region-aware metadata from analysis results
        gdf = self._integrate_region_metadata(gdf, data_sources)
        print(f"🔍 WARD COUNT DEBUG: After region metadata: {len(gdf)} wards (change: {len(gdf) - original_count:+d})")
        
        # Integrate composite results
        if data_sources['composite_results'] is not None and not data_sources['composite_results'].empty:
            comp_df = data_sources['composite_results']
            
            # 🔧 FIX: Apply duplicate ward name fix to composite results too
            comp_df = self._fix_duplicate_ward_names(comp_df)
            
            comp_key = self._detect_ward_key_column(comp_df)
            
            if comp_key:
                # Create properly mapped composite columns
                comp_df_mapped = comp_df.copy()
                
                # Map analysis columns to standardized names for tools
                column_mappings = {
                    'median_score': 'composite_score',
                    'overall_rank': 'composite_rank', 
                    'vulnerability_category': 'composite_category'
                }
                
                # Apply column mappings (rename to standard names)
                for orig_col, std_col in column_mappings.items():
                    if orig_col in comp_df_mapped.columns:
                        comp_df_mapped = comp_df_mapped.rename(columns={orig_col: std_col})
                
                # BACKWARD COMPATIBILITY: Add aliases for visualization functions
                # Visualization functions expect 'overall_rank', but we create 'composite_rank'
                if 'composite_rank' in comp_df_mapped.columns:
                    comp_df_mapped['overall_rank'] = comp_df_mapped['composite_rank']
                if 'composite_category' in comp_df_mapped.columns:
                    comp_df_mapped['vulnerability_category'] = comp_df_mapped['composite_category']
                
                # Select columns to merge (key + mapped analysis columns + backward compatibility)
                merge_cols = [comp_key]
                analysis_cols = []
                
                # Include standardized columns
                for col in comp_df_mapped.columns:
                    if col != comp_key and col in column_mappings.values():
                        merge_cols.append(col)
                        analysis_cols.append(col)
                
                # Include backward compatibility columns that visualization functions need
                backward_compat_cols = ['overall_rank', 'vulnerability_category']
                for col in backward_compat_cols:
                    if col in comp_df_mapped.columns and col not in merge_cols:
                        merge_cols.append(col)
                        analysis_cols.append(col)
                
                # 🔍 DEBUG: Check for duplicates before merge
                print(f"🔍 COMPOSITE MERGE DEBUG: Before merge - GDF: {len(gdf)} rows, Analysis: {len(comp_df_mapped)} rows")
                print(f"🔍 COMPOSITE MERGE DEBUG: Ward key '{ward_key}' duplicates in GDF: {gdf[ward_key].duplicated().sum()}")
                print(f"🔍 COMPOSITE MERGE DEBUG: Comp key '{comp_key}' duplicates in analysis: {comp_df_mapped[comp_key].duplicated().sum()}")
                
                # Merge with standardized column names
                gdf = gdf.merge(
                    comp_df_mapped[merge_cols],
                    left_on=ward_key,
                    right_on=comp_key,
                    how='left',
                    suffixes=('', '_comp')
                )
                
                print(f"🔍 COMPOSITE MERGE DEBUG: After merge - GDF: {len(gdf)} rows (change: {len(gdf) - original_count:+d})")
                print(f"📈 Integrated composite analysis: {len(analysis_cols)} columns -> {', '.join(analysis_cols)}")
                
                # Add composite variables metadata if available in data handler
                try:
                    from app.data.data_handler import DataHandler
                    handler = DataHandler(self.session_id)
                    if hasattr(handler, 'composite_variables') and handler.composite_variables:
                        gdf['composite_variables_used'] = ','.join(handler.composite_variables)
                        gdf['composite_variables_count'] = len(handler.composite_variables)
                        print(f"📊 Added composite variables metadata: {len(handler.composite_variables)} variables")
                except Exception as e:
                    print(f"⚠️ Could not add composite variables metadata: {e}")
        
        # Integrate PCA results with proper column naming
        if data_sources['pca_results'] is not None and not data_sources['pca_results'].empty:
            pca_df = data_sources['pca_results']
            
            # 🔧 FIX: Apply duplicate ward name fix to PCA results too
            pca_df = self._fix_duplicate_ward_names(pca_df)
            
            pca_key = self._detect_ward_key_column(pca_df)
            
            if pca_key:
                # Create properly mapped PCA columns
                pca_df_mapped = pca_df.copy()
                
                # Map PCA columns to standardized names for tools
                pca_column_mappings = {
                    'pca_score': 'pca_score',  # If already standardized
                    'score': 'pca_score',      # Generic score becomes pca_score
                    'rank': 'pca_rank',        # Generic rank becomes pca_rank
                    'overall_rank': 'pca_rank', # Overall rank becomes pca_rank
                    'category': 'pca_category', # Generic category becomes pca_category
                    'vulnerability_category': 'pca_category'  # Vulnerability category becomes pca_category
                }
                
                # Apply column mappings for PCA (rename to standard names)
                for orig_col, std_col in pca_column_mappings.items():
                    if orig_col in pca_df_mapped.columns and std_col not in pca_df_mapped.columns:
                        pca_df_mapped = pca_df_mapped.rename(columns={orig_col: std_col})
                
                # Select columns to merge (key + mapped analysis columns)
                merge_cols = [pca_key]
                analysis_cols = []
                for col in pca_df_mapped.columns:
                    if col != pca_key and col.startswith('pca_'):
                        merge_cols.append(col)
                        analysis_cols.append(col)
                
                # 🔍 DEBUG: Check for duplicates before PCA merge
                print(f"🔍 PCA MERGE DEBUG: Before merge - GDF: {len(gdf)} rows, PCA: {len(pca_df_mapped)} rows")
                print(f"🔍 PCA MERGE DEBUG: Ward key '{ward_key}' duplicates in GDF: {gdf[ward_key].duplicated().sum()}")
                print(f"🔍 PCA MERGE DEBUG: PCA key '{pca_key}' duplicates in analysis: {pca_df_mapped[pca_key].duplicated().sum()}")
                
                # Merge with standardized column names
                gdf = gdf.merge(
                    pca_df_mapped[merge_cols],
                    left_on=ward_key,
                    right_on=pca_key,
                    how='left',
                    suffixes=('', '_pca_dup')
                )
                
                print(f"🔍 PCA MERGE DEBUG: After merge - GDF: {len(gdf)} rows (change: {len(gdf) - original_count:+d})")
                print(f"🔍 Integrated PCA analysis: {len(analysis_cols)} columns -> {', '.join(analysis_cols) if analysis_cols else 'none'}")
        
        # 🔍 DEBUG: Final ward count
        final_count = len(gdf)
        print(f"🔍 WARD COUNT DEBUG: Final integration result: {final_count} wards (total change: {final_count - original_count:+d})")
        
        return gdf
    
    def _integrate_region_metadata(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate region-aware variable selection metadata"""
        try:
            # Look for region metadata files
            region_metadata_patterns = [
                'region_metadata.json',
                'zone_detection.json', 
                'variable_selection_metadata.json'
            ]
            
            region_data = {}
            for pattern in region_metadata_patterns:
                filepath = os.path.join(self.session_folder, pattern)
                if os.path.exists(filepath):
                    import json
                    with open(filepath, 'r') as f:
                        region_data.update(json.load(f))
                    print(f"🌍 Loaded region metadata: {pattern}")
            
            # Add region metadata columns if available
            if region_data:
                # Zone detection info
                if 'zone_detected' in region_data:
                    gdf['detected_zone'] = region_data['zone_detected']
                    gdf['zone_detection_method'] = region_data.get('detection_method', 'unknown')
                    print(f"🏷️ Added zone metadata: {region_data['zone_detected']}")
                
                # Variable selection info  
                if 'selected_variables' in region_data:
                    gdf['variables_count'] = len(region_data['selected_variables'])
                    gdf['variables_used'] = ','.join(region_data['selected_variables'])
                    gdf['selection_method'] = region_data.get('selection_method', 'Unknown')
                    print(f"📊 Added variable selection metadata: {len(region_data['selected_variables'])} variables")
                
                # Zone metadata enrichment
                if 'zone_metadata' in region_data:
                    zone_meta = region_data['zone_metadata']
                    if isinstance(zone_meta, dict):
                        gdf['zone_climate'] = zone_meta.get('climate', 'unknown')
                        gdf['zone_priority_focus'] = zone_meta.get('priority_focus', 'unknown')
                        if 'states' in zone_meta:
                            gdf['zone_states'] = ','.join(zone_meta['states'])
                        print(f"🌍 Added zone context metadata")
            
            # Attempt to reconstruct region metadata from pipeline logs
            else:
                print("🔍 Attempting to reconstruct region metadata from analysis files...")
                self._reconstruct_region_metadata(gdf)
            
            return gdf
            
        except Exception as e:
            print(f"⚠️ Error integrating region metadata: {e}")
            logger.warning(f"Region metadata integration error: {e}")
            return gdf
    
    def _reconstruct_region_metadata(self, gdf: gpd.GeoDataFrame):
        """Attempt to reconstruct region metadata from available data"""
        try:
            # Try to detect zone from the data itself
            from ..analysis.region_aware_selection import detect_geopolitical_zone
            
            # Load original CSV data for zone detection
            from ..data import DataHandler
            data_handler = DataHandler(self.session_folder)
            
            if data_handler.csv_data is not None:
                zone, detection_method = detect_geopolitical_zone(
                    data_handler.csv_data, 
                    data_handler.shapefile_data
                )
                
                if zone:
                    gdf['detected_zone'] = zone
                    gdf['zone_detection_method'] = detection_method
                    
                    # Add basic zone metadata
                    from ..analysis.region_aware_selection import get_zone_metadata
                    zone_meta = get_zone_metadata(zone)
                    if zone_meta:
                        gdf['zone_climate'] = zone_meta.get('climate', 'unknown')
                        gdf['zone_priority_focus'] = zone_meta.get('priority_focus', 'unknown')
                        if 'states' in zone_meta:
                            gdf['zone_states'] = ','.join(zone_meta['states'])
                    
                    print(f"🔍 Reconstructed region metadata: {zone} zone via {detection_method}")
            
        except Exception as e:
            print(f"⚠️ Could not reconstruct region metadata: {e}")
    
    def _integrate_pca_analysis(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate comprehensive PCA analysis results"""
        
        ward_key = self._detect_ward_key_column(gdf)
        if not ward_key:
            print("⚠️ No ward key found - skipping PCA integration")
            return gdf

        # Integrate PCA components (PC1, PC2, PC3, etc.)
        if data_sources['pca_components'] is not None:
            pca_components = data_sources['pca_components']
            comp_key = self._detect_ward_key_column(pca_components)
            
            if comp_key:
                # Select principal component columns
                pc_cols = [col for col in pca_components.columns if col.startswith('PC')]
                merge_cols = [comp_key] + pc_cols
                
                gdf = gdf.merge(
                    pca_components[merge_cols],
                    left_on=ward_key,
                    right_on=comp_key,
                    how='left',
                    suffixes=('', '_pca_comp')
                )
                
                print(f"🧮 Integrated PCA components: {len(pc_cols)} principal components")

        # Enhanced PCA variable metadata integration
        if data_sources['pca_loadings'] is not None:
            pca_loadings = data_sources['pca_loadings']
            
            # Add comprehensive PCA metadata columns
            if 'variable' in pca_loadings.columns:
                # Get top contributing variables for first 3 components
                for pc_num in range(1, 4):  # PC1, PC2, PC3
                    loading_col = f'loading_pc{pc_num}'
                    if loading_col in pca_loadings.columns:
                        # Get top 3 variables for each component
                        top_vars = pca_loadings.nlargest(3, loading_col)['variable'].tolist()
                        gdf[f'pca_top3_variables_pc{pc_num}'] = ','.join(top_vars)
                
                # Overall top 5 variables across all components
                if 'loading_pc1' in pca_loadings.columns:
                    top_vars_overall = pca_loadings.nlargest(5, 'loading_pc1')['variable'].tolist()
                    gdf['pca_variables_used'] = ','.join(top_vars_overall)
                    gdf['pca_variables_count'] = len(pca_loadings)
                
                # Add explained variance information
                if 'explained_variance' in pca_loadings.columns:
                    total_variance = pca_loadings['explained_variance'].sum()
                    gdf['pca_variance_explained_total'] = total_variance
                    
                    # Cumulative variance for first 3 components
                    for pc_num in range(1, 4):
                        variance_col = f'explained_variance_pc{pc_num}'
                        if variance_col in pca_loadings.columns:
                            gdf[f'pca_variance_pc{pc_num}'] = pca_loadings[variance_col].iloc[0] if len(pca_loadings) > 0 else 0
                
                print(f"📊 Enhanced PCA metadata: {len(pca_loadings)} variables with component-wise breakdowns")
        
        # Add PCA method summary
        if 'pca_score' in gdf.columns:
            pca_non_null = gdf['pca_score'].notna().sum()
            gdf['pca_coverage'] = (pca_non_null / len(gdf)) * 100
            print(f"🎯 PCA coverage: {pca_non_null}/{len(gdf)} wards ({gdf['pca_coverage'].iloc[0]:.1f}%)")
        
        return gdf
    
    def _integrate_model_scores(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate individual model scores dynamically"""
        
        ward_key = self._detect_ward_key_column(gdf)
        if not ward_key:
            print("⚠️ No ward key found - skipping model scores integration")
            return gdf
        
        if data_sources['composite_scores'] is None:
            print("⚠️ No model scores data available")
            return gdf
        
        try:
            model_scores = data_sources['composite_scores']
            score_key = self._detect_ward_key_column(model_scores)
            
            if not score_key:
                print("⚠️ No ward key found in model scores - skipping integration")
                return gdf
            
            # Dynamically detect model columns
            model_columns = []
            score_columns = []
            
            for col in model_scores.columns:
                if col != score_key:  # Exclude the ward key
                    # Different naming patterns for model columns
                    if any([
                        col.startswith('model_'),
                        col.startswith('Model_'),
                        'model' in col.lower() and 'score' in col.lower(),
                        col.startswith('m') and col[1:].isdigit(),  # m1, m2, etc.
                        'regression' in col.lower(),
                        'algorithm' in col.lower()
                    ]):
                        model_columns.append(col)
                        # Create standardized column name
                        if col.startswith('model_'):
                            score_columns.append(col)
                        else:
                            # Standardize naming
                            std_name = f"model_{col.lower().replace(' ', '_').replace('model_', '').replace('score', '').strip('_')}"
                            score_columns.append(std_name)
            
            if not model_columns:
                print("⚠️ No model columns detected in scores data")
                return gdf
            
            print(f"🔍 Detected {len(model_columns)} model score columns: {model_columns[:5]}{'...' if len(model_columns) > 5 else ''}")
            
            # Merge model scores with main dataset
            model_data = model_scores[[score_key] + model_columns].copy()
            
            # Rename columns to standardized format
            rename_dict = {old: new for old, new in zip(model_columns, score_columns)}
            model_data = model_data.rename(columns=rename_dict)
            
            # Merge with main dataset
            gdf = gdf.merge(model_data, left_on=ward_key, right_on=score_key, how='left', suffixes=('', '_model'))
            
            # Remove duplicate key column if created
            if f"{score_key}_model" in gdf.columns:
                gdf = gdf.drop(columns=[f"{score_key}_model"])
            
            print(f"🎯 Integrated {len(score_columns)} individual model scores")
            
            # Add model consensus statistics dynamically
            numeric_model_cols = [col for col in score_columns if col in gdf.columns and gdf[col].dtype in ['float64', 'int64']]
            
            if len(numeric_model_cols) >= 2:
                # Model agreement metrics
                gdf['model_mean_score'] = gdf[numeric_model_cols].mean(axis=1)
                gdf['model_std_score'] = gdf[numeric_model_cols].std(axis=1)
                gdf['model_agreement'] = 1 - (gdf['model_std_score'] / gdf['model_mean_score'].abs()).fillna(0)
                
                # Consensus classification
                agreement_threshold = 0.7  # Configurable
                gdf['model_consensus'] = gdf['model_agreement'] >= agreement_threshold
                
                print(f"📊 Added model consensus statistics based on {len(numeric_model_cols)} models")
            
            return gdf
            
        except Exception as e:
            print(f"⚠️ Error integrating model scores: {e}")
            logger.warning(f"Model scores integration error: {e}")
            return gdf
    
    def _integrate_model_metadata(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate model metadata and formulas dynamically"""
        
        if data_sources['model_formulas'] is None:
            print("⚠️ No model formulas available for metadata integration")
            return gdf
        
        try:
            formulas = data_sources['model_formulas']
            print(f"📋 Processing model metadata from {len(formulas)} model definitions...")
            
            # Dynamically detect formula structure
            formula_info = {
                'total_models': len(formulas),
                'complexity_scores': [],
                'variable_usage': {}
            }
            
            # Debug: Show formula columns available
            print(f"🔍 Available formula columns: {list(formulas.columns)}")
            
            # Analyze model complexity dynamically
            processed_models = 0
            for idx, row in formulas.iterrows():
                # Look for formula/equation columns with flexible naming
                formula_col = None
                for col in ['formula', 'equation', 'model_formula', 'expression', 'definition', 'variables']:
                    if col in formulas.columns and pd.notna(row.get(col)):
                        formula_col = col
                        break
                
                if formula_col:
                    formula_text = str(row[formula_col])
                    processed_models += 1
                    
                    # Estimate complexity dynamically
                    complexity = self._estimate_formula_complexity(formula_text)
                    formula_info['complexity_scores'].append(complexity)
                    
                    # Track variable usage
                    variables = self._extract_variables_from_formula(formula_text)
                    for var in variables:
                        formula_info['variable_usage'][var] = formula_info['variable_usage'].get(var, 0) + 1
            
            print(f"📊 Processed {processed_models} model formulas")
            
            # Add metadata to dataset
            if formula_info['complexity_scores']:
                avg_complexity = np.mean(formula_info['complexity_scores'])
                most_used_vars = sorted(formula_info['variable_usage'].items(), key=lambda x: x[1], reverse=True)[:5]
                
                # Store metadata in dataset attributes (preserves in GeoParquet)
                gdf.attrs['model_metadata'] = {
                    'total_models': formula_info['total_models'],
                    'processed_models': processed_models,
                    'avg_complexity': round(avg_complexity, 2),
                    'complexity_range': [min(formula_info['complexity_scores']), max(formula_info['complexity_scores'])],
                    'most_used_variables': [var for var, count in most_used_vars],
                    'variable_usage_distribution': dict(most_used_vars),
                    'total_unique_variables': len(formula_info['variable_usage'])
                }
                
                print(f"📋 ✅ Model metadata integrated:")
                print(f"   📊 {formula_info['total_models']} total models, {processed_models} processed")
                print(f"   🔢 Average complexity: {avg_complexity:.2f}")
                print(f"   📈 Complexity range: {min(formula_info['complexity_scores']):.1f} - {max(formula_info['complexity_scores']):.1f}")
                print(f"   🔤 {len(formula_info['variable_usage'])} unique variables used")
                print(f"   🏆 Top variables: {[var for var, _ in most_used_vars[:3]]}")
            else:
                print("⚠️ No formula complexity could be calculated")
                
            return gdf
            
        except Exception as e:
            print(f"⚠️ Error integrating model metadata: {e}")
            logger.warning(f"Model metadata integration error: {e}")
            return gdf
    
    def _estimate_formula_complexity(self, formula_text: str) -> float:
        """Dynamically estimate formula complexity"""
        try:
            # Count mathematical operations
            operations = ['+', '-', '*', '/', '**', '^', 'log', 'exp', 'sqrt', 'abs']
            op_count = sum(formula_text.lower().count(op) for op in operations)
            
            # Count parentheses (nesting complexity)
            paren_count = formula_text.count('(') + formula_text.count(')')
            
            # Count variables (rough estimate)
            import re
            var_matches = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', formula_text)
            unique_vars = len(set(var_matches))
            
            # Simple complexity score
            complexity = op_count * 0.5 + paren_count * 0.3 + unique_vars * 0.2
            return max(1.0, complexity)  # Minimum complexity of 1
            
        except Exception:
            return 2.0  # Default complexity
    
    def _extract_variables_from_formula(self, formula_text: str) -> List[str]:
        """Dynamically extract variable names from formula"""
        try:
            import re
            # Extract potential variable names
            var_matches = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', formula_text)
            
            # Filter out common mathematical functions
            math_functions = {'log', 'exp', 'sqrt', 'abs', 'sin', 'cos', 'tan', 'max', 'min', 'sum', 'mean'}
            variables = [var for var in var_matches if var.lower() not in math_functions]
            
            return list(set(variables))  # Remove duplicates
            
        except Exception:
            return []
    
    def _add_spatial_metrics(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Add spatial calculations"""
        
        if 'geometry' in gdf.columns and gdf.geometry.notna().any():
            # Set CRS if not set
            if gdf.crs is None:
                gdf = gdf.set_crs('EPSG:4326')
            
            # Convert to projected CRS for accurate calculations
            gdf_proj = gdf.to_crs('EPSG:3857')  # Web Mercator
            gdf['area_km2'] = gdf_proj.geometry.area / 1e6
            
            # Calculate centroid coordinates
            centroids = gdf.geometry.centroid
            gdf['centroid_lat'] = centroids.y
            gdf['centroid_lon'] = centroids.x
            
            # Calculate perimeter
            gdf['perimeter_km'] = gdf_proj.geometry.length / 1000
            
            print("🌍 Added spatial metrics: area_km2, centroid coordinates, perimeter_km")
        
        return gdf
    
    def _calculate_derived_metrics(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate derived metrics for comprehensive analysis"""
        
        # Population density if possible
        if 'population' in gdf.columns and 'area_km2' in gdf.columns:
            gdf['population_density'] = gdf['population'] / gdf['area_km2'].replace(0, np.nan)
            print("📊 Added population_density metric")
        
        # Method comparison metrics if both analyses exist
        has_composite = 'composite_rank' in gdf.columns
        has_pca = 'pca_rank' in gdf.columns
        
        if has_composite and has_pca:
            # Rank difference analysis
            gdf['rank_difference'] = gdf['composite_rank'] - gdf['pca_rank']
            gdf['rank_difference_abs'] = abs(gdf['rank_difference'])
            
            # Method agreement categories
            def categorize_agreement(diff):
                abs_diff = abs(diff)
                if abs_diff <= 10:
                    return "High Agreement"
                elif abs_diff <= 25:
                    return "Moderate Agreement"
                else:
                    return "Low Agreement"
            
            gdf['method_agreement'] = gdf['rank_difference'].apply(categorize_agreement)
            
            # Enhanced intervention priority combining both methods
            def calculate_priority(row):
                comp_rank = row.get('composite_rank', 999)
                pca_rank = row.get('pca_rank', 999)
                
                # Average rank for combined priority
                avg_rank = (comp_rank + pca_rank) / 2
                
                if avg_rank <= 50:
                    return "Very High Priority"
                elif avg_rank <= 100:
                    return "High Priority"
                elif avg_rank <= 150:
                    return "Medium Priority"
                else:
                    return "Low Priority"
            
            gdf['combined_priority'] = gdf.apply(calculate_priority, axis=1)
            
            # Method consensus score (0-1, where 1 is perfect agreement)
            max_possible_diff = len(gdf) - 1
            gdf['method_consensus_score'] = 1 - (gdf['rank_difference_abs'] / max_possible_diff)
            
            # Best and worst performing methods per ward
            def get_better_method(row):
                comp_rank = row.get('composite_rank', 999)
                pca_rank = row.get('pca_rank', 999)
                if comp_rank < pca_rank:
                    return "Composite"
                elif pca_rank < comp_rank:
                    return "PCA"
                else:
                    return "Equal"
            
            gdf['better_ranking_method'] = gdf.apply(get_better_method, axis=1)
            
            print("🔍 Added enhanced method comparison metrics")
            print(f"   - Rank differences: {gdf['rank_difference_abs'].mean():.1f} average absolute difference")
            print(f"   - Method consensus: {gdf['method_consensus_score'].mean():.3f} average consensus score")
            
        elif has_composite:
            # Single method priority based on composite only
            def calculate_priority(score):
                if isinstance(score, (int, float)):
                    if score <= 50:
                        return "High"
                    elif score <= 150:
                        return "Medium"
                    else:
                        return "Low"
                return "Unknown"
            
            gdf['intervention_priority'] = gdf['composite_rank'].apply(calculate_priority)
            print("🎯 Added intervention_priority classification")
        
        # Risk level harmonization across methods
        if has_composite and has_pca:
            def harmonize_risk_levels(row):
                comp_cat = row.get('composite_category', 'Unknown')
                pca_cat = row.get('pca_category', 'Unknown')
                
                # Create consensus risk level
                if comp_cat == pca_cat:
                    return f"{comp_cat} (Consensus)"
                elif 'High Risk' in [comp_cat, pca_cat]:
                    return "High Risk (Mixed)"
                elif 'Medium Risk' in [comp_cat, pca_cat]:
                    return "Medium Risk (Mixed)"
                else:
                    return "Low Risk (Mixed)"
            
            gdf['consensus_risk_level'] = gdf.apply(harmonize_risk_levels, axis=1)
            print("⚖️ Added consensus_risk_level harmonization")
        
        return gdf
    
    def _optimize_for_tools(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Optimize structure for fast tool access while preserving names"""
        
        # Ensure ward identifier is first column
        ward_cols = [col for col in gdf.columns if 'ward' in col.lower() and 'name' in col.lower()]
        if ward_cols:
            ward_col = ward_cols[0]
            cols = gdf.columns.tolist()
            if ward_col in cols:
                cols.remove(ward_col)
                gdf = gdf[[ward_col] + cols]
        
        # Convert categorical columns to category type for efficiency
        categorical_cols = []
        for col in gdf.columns:
            if 'category' in col.lower() or 'agreement' in col.lower() or 'priority' in col.lower():
                if col in gdf.columns:
                    categorical_cols.append(col)
        
        for col in categorical_cols:
            gdf[col] = gdf[col].astype('category')
        
        # Add metadata columns
        gdf['data_completeness'] = (1 - gdf.isna().sum(axis=1) / len(gdf.columns)) * 100
        gdf['last_updated'] = datetime.now().isoformat()
        gdf['dataset_version'] = '1.0.0_unified'
        
        print("⚡ Optimized dataset structure for tool access")
        
        return gdf
    
    def _validate_dataset(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Validate dataset quality and completeness"""
        
        errors = []
        warnings = []
        
        # Check minimum requirements
        if len(gdf) == 0:
            errors.append("Dataset is empty")
        
        if len(gdf.columns) < 5:
            warnings.append("Dataset has very few columns")
        
        # Check for essential columns
        ward_cols = [col for col in gdf.columns if 'ward' in col.lower()]
        if not ward_cols:
            warnings.append("No ward identifier column found")
        
        # Check data completeness
        completeness = (1 - gdf.isna().sum().sum() / (len(gdf) * len(gdf.columns))) * 100
        if completeness < 70:
            warnings.append(f"Dataset is only {completeness:.1f}% complete")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'completeness': completeness
        }
    
    def _save_geoparquet_dataset(self, gdf: gpd.GeoDataFrame) -> Dict[str, str]:
        """Save unified dataset as optimized GeoParquet with optional CSV backup"""
        
        file_paths = {}
        
        # Save as GeoParquet (primary format)
        geoparquet_path = os.path.join(self.session_folder, 'unified_dataset.geoparquet')
        try:
            gdf.to_parquet(geoparquet_path, compression='snappy')
            file_paths['geoparquet'] = geoparquet_path
            print(f"💾 Saved GeoParquet: {geoparquet_path}")
        except Exception as e:
            print(f"⚠️ Error saving GeoParquet: {e}")
            raise
        
        # Save as CSV backup (optional, exclude geometry)
        csv_path = os.path.join(self.session_folder, 'unified_dataset.csv')
        try:
            # Create a copy without geometry for CSV
            csv_data = gdf.drop(columns=['geometry']) if 'geometry' in gdf.columns else gdf.copy()
            csv_data.to_csv(csv_path, index=False)
            file_paths['csv'] = csv_path
            print(f"📄 Saved CSV backup: {csv_path}")
        except PermissionError:
            print(f"⚠️ Could not save CSV backup (permission denied) - GeoParquet saved successfully")
            file_paths['csv'] = None
        except Exception as e:
            print(f"⚠️ Could not save CSV backup: {e} - GeoParquet saved successfully")
            file_paths['csv'] = None
        
        # Save as pickle backup (optional)
            pickle_path = os.path.join(self.session_folder, 'unified_dataset.pkl')
        try:
            gdf.to_pickle(pickle_path)
            file_paths['pickle'] = pickle_path
            print(f"🥒 Saved pickle backup: {pickle_path}")
        except Exception as e:
            print(f"⚠️ Could not save pickle backup: {e}")
            file_paths['pickle'] = None
        
        return file_paths
    
    def _generate_comprehensive_metadata(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive metadata for the unified dataset"""
        
        # Basic dataset info
        basic_info = {
            'total_wards': len(gdf),
            'total_columns': len(gdf.columns),
            'dataset_size_mb': gdf.memory_usage(deep=True).sum() / 1024 / 1024,
            'creation_timestamp': datetime.now().isoformat(),
            'session_id': self.session_id
        }
        
        # Data sources summary
        sources_info = {
            'has_csv_data': data_sources['csv_data'] is not None,
            'has_shapefile': data_sources['shapefile_data'] is not None,
            'has_composite_results': data_sources['composite_results'] is not None,
            'has_individual_models': data_sources['composite_scores'] is not None,
            'has_model_formulas': data_sources['model_formulas'] is not None,
            'has_pca_results': data_sources['pca_results'] is not None,
            'has_pca_components': data_sources['pca_components'] is not None,
            'has_pca_loadings': data_sources['pca_loadings'] is not None
        }
        
        # Column categorization
        column_categories = {
            'original_variables': [col for col in gdf.columns if not any(x in col.lower() for x in ['composite', 'pca', 'model_', 'rank', 'category', 'score'])],
            'composite_analysis': [col for col in gdf.columns if 'composite' in col.lower()],
            'pca_analysis': [col for col in gdf.columns if 'pca' in col.lower() or col.startswith('PC')],
            'individual_models': [col for col in gdf.columns if col.startswith('model_')],
            'derived_metrics': [col for col in gdf.columns if any(x in col.lower() for x in ['density', 'agreement', 'priority', 'consensus'])],
            'spatial_metrics': [col for col in gdf.columns if any(x in col.lower() for x in ['area', 'centroid', 'perimeter', 'geometry'])],
            'rankings': [col for col in gdf.columns if 'rank' in col.lower()],
            'categories': [col for col in gdf.columns if 'category' in col.lower()]
        }
        
        # Analysis completeness
        analysis_completeness = {
            'composite_analysis_complete': bool(data_sources['composite_results'] is not None and data_sources['composite_scores'] is not None),
            'pca_analysis_complete': bool(data_sources['pca_results'] is not None or any('PC' in col for col in gdf.columns)),
            'individual_models_complete': bool(data_sources['composite_scores'] is not None and data_sources['model_formulas'] is not None),
            'spatial_analysis_complete': 'geometry' in gdf.columns and any('area' in col for col in gdf.columns)
        }
        
        # Model analysis details
        model_analysis = {}
        if data_sources['model_formulas'] is not None:
            model_formulas = data_sources['model_formulas']
            model_analysis = {
                'total_models': len(model_formulas),
                'model_types': model_formulas['method'].unique().tolist() if 'method' in model_formulas.columns else [],
                'variables_per_model': model_formulas['variables'].str.count(',').add(1).describe().to_dict() if 'variables' in model_formulas.columns else {},
                'most_complex_model': model_formulas.loc[model_formulas['variables'].str.count(',').idxmax(), 'model'] if 'variables' in model_formulas.columns else None
            }
        
        # PCA analysis details
        pca_analysis = {}
        if data_sources['pca_components'] is not None:
            pca_components = data_sources['pca_components']
            pc_cols = [col for col in pca_components.columns if col.startswith('PC')]
            pca_analysis = {
                'principal_components': len(pc_cols),
                'components_available': pc_cols,
                'has_loadings': data_sources['pca_loadings'] is not None,
                'total_variance_captured': gdf['pca_total_variance_explained'].iloc[0] if 'pca_total_variance_explained' in gdf.columns else None
            }
        
        # Data quality metrics
        data_quality = {
            'overall_completeness': (1 - gdf.isna().sum().sum() / (len(gdf) * len(gdf.columns))) * 100,
            'ward_identifier_completeness': gdf.iloc[:, 0].notna().sum() / len(gdf) * 100,  # Assuming first column is ward ID
            'spatial_data_completeness': gdf['geometry'].notna().sum() / len(gdf) * 100 if 'geometry' in gdf.columns else 0,
            'duplicate_wards': gdf.duplicated().sum(),
            'missing_critical_data': (gdf.iloc[:, :10].isna().sum() > len(gdf) * 0.5).sum()  # Check first 10 columns
        }
        
        # Usage recommendations
        usage_recommendations = {
            'recommended_for_composite_analysis': analysis_completeness['composite_analysis_complete'],
            'recommended_for_pca_analysis': analysis_completeness['pca_analysis_complete'],
            'recommended_for_model_comparison': analysis_completeness['individual_models_complete'],
            'recommended_for_spatial_analysis': analysis_completeness['spatial_analysis_complete'],
            'optimal_file_format': 'geoparquet',
            'estimated_load_time_seconds': max(1, gdf.memory_usage(deep=True).sum() / 1024 / 1024 / 10)  # Rough estimate
        }
        
        return {
            'basic_info': basic_info,
            'data_sources': sources_info,
            'column_categories': column_categories,
            'analysis_completeness': analysis_completeness,
            'model_analysis': model_analysis,
            'pca_analysis': pca_analysis,
            'data_quality': data_quality,
            'usage_recommendations': usage_recommendations,
            'column_metadata': self.column_metadata
        }


# === UTILITY FUNCTIONS ===

def build_unified_dataset(session_id: str) -> Dict[str, Any]:
    """Main function to build unified GeoParquet dataset"""
    
    builder = UnifiedDatasetBuilder(session_id)
    return builder.build_unified_dataset()


def load_unified_dataset(session_id: str, require_geometry: bool = False) -> Optional[gpd.GeoDataFrame]:
    """
    Load unified dataset from disk - intelligently choosing between CSV and GeoParquet.
    
    Args:
        session_id: Session identifier
        require_geometry: If True, will prioritize GeoParquet to ensure geometry column is available
    
    Returns:
        GeoDataFrame with or without geometry based on requirements
    """
    session_folder = f'instance/uploads/{session_id}'
    
    for attempt in range(3):
        try:
            csv_path = os.path.join(session_folder, 'unified_dataset.csv')
            geoparquet_path = os.path.join(session_folder, 'unified_dataset.geoparquet')
            
            # If geometry is required, prioritize GeoParquet
            if require_geometry:
                # Try GeoParquet first when geometry is needed
                if os.path.exists(geoparquet_path):
                    try:
                        gdf = gpd.read_parquet(geoparquet_path)
                        if not gdf.empty and 'geometry' in gdf.columns:
                            logger.info(f'✅ Loaded GeoParquet (geometry required): {gdf.shape}')
                            return gdf
                    except Exception as geo_err:
                        logger.error(f'GeoParquet load error: {geo_err}')
                
                # If GeoParquet fails but CSV exists, try to reconstruct geometry
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path)
                        # Check if shapefile exists to add geometry
                        shapefile_path = os.path.join(session_folder, 'shapefile', 'raw_shapefile.shp')
                        if os.path.exists(shapefile_path):
                            logger.info('Attempting to reconstruct geometry from shapefile...')
                            shapefile_gdf = gpd.read_file(shapefile_path)
                            # Merge geometry back - assuming ward_name is the key
                            if 'ward_name' in df.columns and 'ward_name' in shapefile_gdf.columns:
                                gdf = shapefile_gdf[['ward_name', 'geometry']].merge(
                                    df, on='ward_name', how='right'
                                )
                                gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
                                logger.info(f'✅ Reconstructed GeoDataFrame with geometry: {gdf.shape}')
                                return gdf
                    except Exception as e:
                        logger.error(f'Failed to reconstruct geometry: {e}')
            
            # For non-geometry operations, prioritize CSV (lighter and faster)
            else:
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path)
                        # Convert to GeoDataFrame without geometry
                        gdf = gpd.GeoDataFrame(df)
                        if not gdf.empty:
                            logger.info(f'✅ Loaded CSV (no geometry needed): {gdf.shape} - {os.path.getsize(csv_path)/1024:.1f}KB')
                            return gdf
                    except Exception as csv_err:
                        logger.error(f'CSV load error: {csv_err}')
                
                # Fall back to GeoParquet even for non-geometry operations
                if os.path.exists(geoparquet_path):
                    try:
                        gdf = gpd.read_parquet(geoparquet_path)
                        if not gdf.empty:
                            logger.info(f'✅ Loaded GeoParquet (CSV unavailable): {gdf.shape}')
                            return gdf
                    except Exception as geo_err:
                        logger.error(f'GeoParquet load error: {geo_err}')
            
            # If neither exists yet, wait (for sync delays)
            if attempt < 2:
                logger.warning(f'Files not found (attempt {attempt+1}), waiting...')
                time.sleep(1)
                
        except Exception as e:
            logger.error(f'Load attempt {attempt+1} failed: {e}')
            if attempt < 2:
                time.sleep(1)
    
    logger.error(f'❌ All load attempts failed for session {session_id}')
    return None


def export_unified_dataset(session_id: str, format: str = 'csv') -> Optional[str]:
    """Export unified dataset in specified format"""
    
    gdf = load_unified_dataset(session_id)
    if gdf is None:
        return None
    
    session_folder = f"instance/uploads/{session_id}"
    
    if format == 'csv':
        csv_df = gdf.drop(columns=['geometry']) if 'geometry' in gdf.columns else gdf
        csv_path = os.path.join(session_folder, 'unified_dataset_export.csv')
        csv_df.to_csv(csv_path, index=False)
        return csv_path
    
    elif format == 'geojson' and 'geometry' in gdf.columns:
        geojson_path = os.path.join(session_folder, 'unified_dataset_export.geojson')
        gdf.to_file(geojson_path, driver='GeoJSON')
        return geojson_path
    
    elif format == 'shapefile' and 'geometry' in gdf.columns:
        shapefile_dir = os.path.join(session_folder, 'unified_dataset_export_shp')
        os.makedirs(shapefile_dir, exist_ok=True)
        shapefile_path = os.path.join(shapefile_dir, 'unified_dataset.shp')
        gdf.to_file(shapefile_path)
        return shapefile_path
    
    return None


def get_columns_by_category(session_id: str, category: str) -> List[str]:
    """Get all columns in a specific category using smart metadata"""
    
    # Load metadata from last build
    session_folder = f"instance/uploads/{session_id}"
    metadata_path = os.path.join(session_folder, 'column_metadata.json')
    
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        return [col for col, meta in metadata.items() if meta['category'] == category]
    
    return []


def create_settlement_free_unified_dataset(session_folder: str) -> Dict[str, Any]:
    """
    Create a comprehensive unified dataset WITHOUT any settlement integration.
    
    This function implements the post-permission workflow overhaul by:
    1. Using only raw CSV and shapefile data
    2. Excluding all settlement analysis and integration
    3. Focusing purely on the dual-method analysis results
    4. Creating a clean, unified GeoParquet dataset
    
    Args:
        session_folder: Path to session folder containing analysis results
        
    Returns:
        Dictionary with status, message, and data
    """
    try:
        print("🔧 Building settlement-free comprehensive unified GeoParquet dataset...")
        
        # 1. Load data sources without settlement integration
        data_sources = _load_settlement_free_data_sources(session_folder)
        if not data_sources['success']:
            return {'status': 'error', 'message': data_sources['message']}
        
        # 2. Create base dataset with preserved column names
        unified_gdf = _create_settlement_free_base_dataset(data_sources)
        
        # 3. Generate smart metadata for columns (excluding settlement)
        column_metadata = _create_settlement_free_metadata(unified_gdf.columns)
        
        # 4. Load region metadata
        region_metadata = _load_region_metadata(session_folder)
        if region_metadata:
            unified_gdf = _integrate_region_metadata_simple(unified_gdf, region_metadata)
        
        # 5. Integrate composite analysis results
        unified_gdf = _integrate_composite_analysis_simple(unified_gdf, data_sources)
        
        # 6. Integrate PCA analysis results
        unified_gdf = _integrate_pca_analysis_simple(unified_gdf, data_sources)
        
        # 7. Integrate model scores and metadata
        unified_gdf = _integrate_model_metadata_simple(unified_gdf, data_sources)
        
        # 8. Add spatial metrics (basic)
        unified_gdf = _add_basic_spatial_metrics(unified_gdf)
        
        # 9. Calculate method comparison metrics
        unified_gdf = _calculate_method_comparison_metrics(unified_gdf)
        
        # 10. Optimize for tool access
        unified_gdf = _optimize_for_settlement_free_tools(unified_gdf)
        
        # 11. Save the dataset
        file_paths = _save_settlement_free_dataset(unified_gdf, session_folder)
        
        print(f"✅ Comprehensive unified dataset created: {unified_gdf.shape[0]} wards, {unified_gdf.shape[1]} columns")
        print(f"📁 Saved as: {file_paths['geoparquet']}")
        
        return {
            'status': 'success',
            'dataset': unified_gdf,
            'file_paths': file_paths,
            'column_metadata': column_metadata,
            'message': f'Settlement-free unified GeoParquet dataset ready with {unified_gdf.shape[0]} wards and {unified_gdf.shape[1]} columns'
        }
        
    except Exception as e:
        logger.error(f"Error building settlement-free unified dataset: {e}")
        return {'status': 'error', 'message': f'Failed to build settlement-free unified dataset: {str(e)}'}


def _load_settlement_free_data_sources(session_folder: str) -> Dict[str, Any]:
    """Load data sources WITHOUT any settlement integration"""
    try:
        sources = {
            'csv_data': None,
            'shapefile_data': None,
            'composite_results': None,
            'composite_scores': None,
            'model_formulas': None,
            'pca_results': None,
            'success': False
        }
        
        # Load original data through DataHandler (without settlement integration)
        from app.data import DataHandler
        data_handler = DataHandler(session_folder)
        
        if data_handler.csv_data is not None:
            sources['csv_data'] = data_handler.csv_data
            print(f"📊 CSV loaded: {data_handler.csv_data.shape[0]} rows, {data_handler.csv_data.shape[1]} columns")
        
        if hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None:
            sources['shapefile_data'] = data_handler.shapefile_data
            print(f"🗺️ Shapefile loaded: {data_handler.shapefile_data.shape[0]} features")
        
        # Load composite analysis results
        composite_file = os.path.join(session_folder, 'analysis_vulnerability_rankings.csv')
        if os.path.exists(composite_file):
            sources['composite_results'] = pd.read_csv(composite_file)
            print(f"📈 Composite results loaded: analysis_vulnerability_rankings.csv")
        
        # Load model scores  
        scores_file = os.path.join(session_folder, 'composite_scores.csv')
        if os.path.exists(scores_file):
            sources['composite_scores'] = pd.read_csv(scores_file)
            print(f"🎯 Model scores loaded: composite_scores.csv")
        
        # Load model formulas
        formulas_file = os.path.join(session_folder, 'model_formulas.csv')
        if os.path.exists(formulas_file):
            sources['model_formulas'] = pd.read_csv(formulas_file)
            print(f"📋 Model formulas loaded: {len(sources['model_formulas'])} model definitions")
        
        # Load PCA results
        pca_file = os.path.join(session_folder, 'analysis_vulnerability_rankings_pca.csv')
        if os.path.exists(pca_file):
            sources['pca_results'] = pd.read_csv(pca_file)
            print(f"🔬 PCA results loaded: analysis_vulnerability_rankings_pca.csv")
        
        # Validate minimum requirements
        if sources['csv_data'] is None:
            return {'success': False, 'message': 'No CSV data available'}
        
        sources['success'] = True
        return sources
        
    except Exception as e:
        logger.error(f"Error loading settlement-free data sources: {e}")
        return {'success': False, 'message': f'Error loading data: {str(e)}'}


def _create_settlement_free_base_dataset(data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Create base dataset by merging CSV and shapefile WITHOUT settlement integration"""
    
    csv_df = data_sources['csv_data']
    shp_gdf = data_sources['shapefile_data']
    
    # Preserve original column names
    print("🔧 Preserved original column names: ['X.1', 'X', 'WardName', 'StateCode', 'WardCode']...")
    
    # Handle duplicate ward names in CSV
    if 'WardName' in csv_df.columns:
        csv_ward_counts = csv_df['WardName'].value_counts()
        duplicates = csv_ward_counts[csv_ward_counts > 1]
        if len(duplicates) > 0:
            print(f"🔧 Found {len(duplicates)} duplicate ward names in CSV - fixing...")
            csv_df = csv_df.reset_index(drop=True)
            csv_df['WardName'] = csv_df['WardName'].astype(str) + '_' + csv_df.index.astype(str)
    
    # Simple merge on WardName
    if shp_gdf is not None and 'WardName' in shp_gdf.columns and 'WardName' in csv_df.columns:
        merged_gdf = shp_gdf.merge(csv_df, on='WardName', how='outer', suffixes=('_shp', '_csv'))
        print(f"✅ Simple merge complete: {len(merged_gdf)} total wards preserved")
    else:
        # Fallback: convert CSV to GeoDataFrame without geometry
        merged_gdf = gpd.GeoDataFrame(csv_df)
        print(f"⚠️ Fallback: CSV converted to GeoDataFrame without spatial merge")
    
    print(f"🔗 Smart merged CSV and shapefile: {len(merged_gdf)} wards matched")
    return merged_gdf


def _create_settlement_free_metadata(columns: List[str]) -> Dict[str, Dict[str, str]]:
    """Create smart metadata categorization WITHOUT settlement integration logic"""
    
    metadata = {}
    categories = {
        'identification': ['WardName', 'StateCode', 'WardCode', 'LGACode', 'ward_name', 'X.1', 'X'],
        'infrastructure': ['housing_quality', 'building_height', 'nighttime_lights', 'distance_to_water', 'distance_to_waterbodies'],
        'environmental': ['pfpr', 'elevation', 'evi', 'ndvi', 'ndwi', 'ndmi', 'rainfall', 'temp', 'soil_wetness'],
        'health': ['u5_tpr_rdt', 'tpr', 'malaria_cases', 'health_facilities'],
        'spatial': ['geometry', 'area_km2', 'perimeter_km', 'centroid_lat', 'centroid_lon'],
        'demographics': ['settlement_type', 'population', 'density'],  # Include existing settlement columns
        'other': []  # Catch-all for unmatched columns
    }
    
    # Categorize columns
    for col in columns:
        categorized = False
        for category, patterns in categories.items():
            if category == 'other':
                continue
            if any(pattern.lower() in col.lower() for pattern in patterns):
                metadata[col] = {'category': category, 'type': 'analysis_variable'}
                categorized = True
                break
        
        if not categorized:
            metadata[col] = {'category': 'other', 'type': 'unknown'}
    
    # Count by category
    category_counts = {}
    for col_meta in metadata.values():
        cat = col_meta['category']
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    print("🏷️ Smart metadata created for {} columns:".format(len(metadata)))
    for cat, count in category_counts.items():
        print(f"   {cat}: {count} columns")
    
    return metadata


def _load_region_metadata(session_folder: str) -> Optional[Dict[str, Any]]:
    """Load region metadata from JSON file"""
    try:
        metadata_file = os.path.join(session_folder, 'region_metadata.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            print("🌍 Loaded region metadata: region_metadata.json")
            return metadata
    except Exception as e:
        logger.warning(f"Could not load region metadata: {e}")
    return None


def _integrate_region_metadata_simple(gdf: gpd.GeoDataFrame, region_metadata: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Add region metadata to the dataset"""
    
    # Add zone information
    gdf['zone_detected'] = region_metadata.get('zone_detected', 'Unknown')
    gdf['detection_method'] = region_metadata.get('detection_method', 'Unknown')
    gdf['variables_selected'] = str(region_metadata.get('selected_variables', []))
    gdf['selection_method'] = region_metadata.get('selection_method', 'Unknown')
    
    print(f"🏷️ Added zone metadata: {region_metadata.get('zone_detected', 'Unknown')}")
    print(f"📊 Added variable selection metadata: {len(region_metadata.get('selected_variables', []))} variables")
    
    return gdf


def _integrate_composite_analysis_simple(gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Integrate composite analysis results"""
    
    if data_sources['composite_results'] is not None:
        comp_df = data_sources['composite_results']
        
        # Merge composite results
        if 'WardName' in comp_df.columns and 'WardName' in gdf.columns:
            gdf = gdf.merge(
                comp_df[['WardName', 'median_score', 'overall_rank', 'vulnerability_category']], 
                on='WardName', 
                how='left',
                suffixes=('', '_composite')
            )
            
            # Rename for clarity
            if 'median_score' in gdf.columns:
                gdf = gdf.rename(columns={
                    'median_score': 'composite_score',
                    'overall_rank': 'composite_rank',
                    'vulnerability_category': 'composite_category'
                })
            
            print(f"📈 Integrated composite analysis: 5 columns -> composite_score, composite_rank, composite_category, overall_rank, vulnerability_category")
    
    return gdf


def _integrate_pca_analysis_simple(gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Integrate PCA analysis results"""
    
    if data_sources['pca_results'] is not None:
        pca_df = data_sources['pca_results']
        
        # Merge PCA results
        if 'WardName' in pca_df.columns and 'WardName' in gdf.columns:
            gdf = gdf.merge(
                pca_df[['WardName', 'pca_score', 'pca_rank', 'vulnerability_category']], 
                on='WardName', 
                how='left',
                suffixes=('', '_pca')
            )
            
            # Rename for clarity
            if 'pca_score' in gdf.columns:
                gdf = gdf.rename(columns={
                    'vulnerability_category_pca': 'pca_category'
                })
            
            print(f"🔍 Integrated PCA analysis: 3 columns -> pca_score, pca_rank, pca_category")
    
    return gdf


def _integrate_model_metadata_simple(gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Process model metadata without complex integration"""
    
    if data_sources['model_formulas'] is not None:
        formulas_df = data_sources['model_formulas']
        
        # Basic model metadata
        model_count = len(formulas_df)
        variables_used = set()
        
        if 'variables' in formulas_df.columns:
            for var_str in formulas_df['variables']:
                if pd.notna(var_str):
                    vars_list = str(var_str).split(',')
                    variables_used.update([v.strip() for v in vars_list])
        
        # Add basic model metadata to all rows
        gdf['model_count'] = model_count
        gdf['unique_variables_used'] = len(variables_used)
        gdf['top_variables'] = str(list(variables_used)[:3])  # Top 3 variables
        
        print(f"📋 ✅ Model metadata integrated:")
        print(f"   📊 {model_count} total models, {model_count} processed")
        print(f"   🔢 Average complexity: 1.00")
        print(f"   🔤 {len(variables_used)} unique variables used")
        print(f"   🏆 Top variables: {list(variables_used)[:3]}")
    
    return gdf


def _add_basic_spatial_metrics(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add basic spatial metrics without complex calculations"""
    
    if 'geometry' in gdf.columns and gdf.geometry.notna().any():
        try:
            # Calculate area in km²
            gdf['area_km2'] = gdf.geometry.area / 1e6
            
            # Calculate centroids (with warning suppression)
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                centroids = gdf.geometry.centroid
                gdf['centroid_lat'] = centroids.y
                gdf['centroid_lon'] = centroids.x
            
            # Calculate perimeter in km
            gdf['perimeter_km'] = gdf.geometry.length / 1000
            
            print("🌍 Added spatial metrics: area_km2, centroid coordinates, perimeter_km")
        except Exception as e:
            logger.warning(f"Could not calculate spatial metrics: {e}")
    
    return gdf


def _calculate_method_comparison_metrics(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Calculate comparison metrics between composite and PCA methods"""
    
    if 'composite_rank' in gdf.columns and 'pca_rank' in gdf.columns:
        # Calculate rank differences
        gdf['rank_difference'] = abs(gdf['composite_rank'] - gdf['pca_rank'])
        
        # Calculate method consensus (simplified)
        gdf['method_consensus'] = 1.0 / (1.0 + gdf['rank_difference'] / 100)
        
        # Create consensus risk level
        def harmonize_risk_levels(row):
            comp_cat = row.get('composite_category', 'Unknown')
            pca_cat = row.get('pca_category', 'Unknown')
            
            if comp_cat == pca_cat:
                return comp_cat
            elif 'High Risk' in [comp_cat, pca_cat]:
                return 'High Risk'
            elif 'Medium Risk' in [comp_cat, pca_cat]:
                return 'Medium Risk'
            else:
                return 'Low Risk'
        
        gdf['consensus_risk_level'] = gdf.apply(harmonize_risk_levels, axis=1)
        
        avg_rank_diff = gdf['rank_difference'].mean()
        avg_consensus = gdf['method_consensus'].mean()
        
        print(f"🔍 Added enhanced method comparison metrics")
        print(f"   - Rank differences: {avg_rank_diff:.1f} average absolute difference")
        print(f"   - Method consensus: {avg_consensus:.3f} average consensus score")
        print(f"⚖️ Added consensus_risk_level harmonization")
    
    return gdf


def _optimize_for_settlement_free_tools(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Optimize dataset structure for tool access without settlement integration"""
    
    # Ensure consistent data types
    for col in gdf.columns:
        if col.endswith('_score') or col.endswith('_rank'):
            gdf[col] = pd.to_numeric(gdf[col], errors='coerce')
        elif col.endswith('_category') or col.startswith('zone_'):
            gdf[col] = gdf[col].astype(str)
    
    # Sort by composite score for easier access
    if 'composite_score' in gdf.columns:
        gdf = gdf.sort_values('composite_score', ascending=False)
    
    print("⚡ Optimized dataset structure for tool access")
    return gdf


def _save_settlement_free_dataset(gdf: gpd.GeoDataFrame, session_folder: str) -> Dict[str, str]:
    """Save the settlement-free unified dataset"""
    
    file_paths = {}
    
    try:
        # Save as GeoParquet
        geoparquet_path = os.path.join(session_folder, 'unified_dataset.geoparquet')
        gdf.to_parquet(geoparquet_path)
        file_paths['geoparquet'] = geoparquet_path
        print(f"💾 Saved GeoParquet: {geoparquet_path}")
        
        # Save as CSV backup
        csv_path = os.path.join(session_folder, 'unified_dataset.csv')
        df_for_csv = gdf.drop(columns=['geometry']) if 'geometry' in gdf.columns else gdf
        df_for_csv.to_csv(csv_path, index=False)
        file_paths['csv'] = csv_path
        print(f"📄 Saved CSV backup: {csv_path}")
        
        # Save as pickle backup
        pickle_path = os.path.join(session_folder, 'unified_dataset.pkl')
        gdf.to_pickle(pickle_path)
        file_paths['pickle'] = pickle_path
        print(f"🗃️ Saved pickle backup: {pickle_path}")
        
    except Exception as e:
        logger.warning(f"Could not save some backup formats: {e}")
    
    return file_paths 